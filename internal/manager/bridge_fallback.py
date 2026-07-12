#!/usr/bin/env python3
"""
MClaw Bridge (内置 fallback 版本)

此文件通过 go:embed 嵌入到 MClaw 二进制中，作为外部 scripts/bridge.py 的回退。
WS_URL 通过占位符由 Go PrepareBridgeCode 在运行时注入。

⚠️ 自动生成文件 — 请勿手动编辑！
   修改源码后运行: python3 scripts/build_fallback.py
"""
#!/usr/bin/env python3
import asyncio, websockets, httpx, json, os, traceback, base64, uuid
from datetime import datetime

def _load_mimo_config():
    for p in [os.path.expanduser("~/.openclaw/openclaw.json"), "/root/.openclaw/openclaw.json", "/opt/mimo-claw-seed/bundle/openclaw/openclaw.json"]:
        try:
            with open(p) as f: cfg = json.load(f)
            x = cfg.get("models",{}).get("providers",{}).get("xiaomi",{})
            b, k = x.get("baseUrl",""), x.get("apiKey","")
            k = os.path.expandvars(k) if k.startswith("${") else k
            if b.startswith("${"): b = os.path.expandvars(b)
            b = b.rstrip("/v1")
            if k and b: return k, b
        except: continue
    return "", ""

_cfg_key, _cfg_base = _load_mimo_config()
KEY = os.environ.get("MIMO_API_KEY") or _cfg_key
BASE = _cfg_base
WS_URL_B64 = "%s"
WS_URL = base64.b64decode(WS_URL_B64).decode() if WS_URL_B64 != "%s" else ""
SYSTEM_PREFIX = "You are a personal assistant running inside OpenClaw."

def strip_billing_header(text):

    if not text: return text
    lines = text.split('\n')
    result = []
    for line in lines:
        if line.strip().startswith('x-anthropic-billing-header:'):
            continue
        result.append(line)
    return '\n'.join(result).strip()

def clean_content_blocks(content):

    if not isinstance(content, list): return content
    cleaned = []
    for b in content:
        if not isinstance(b, dict): continue
        btype = b.get('type', '')
        if btype == 'text' and not b.get('text', '').strip():
            continue  # Skip empty text blocks
        if btype == 'thinking' and not b.get('thinking', '').strip():
            continue  # Skip empty thinking blocks
        cleaned.append(b)
    return cleaned if cleaned else content

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def safe_send(ws, lock, data):
    try:
        async with lock: await ws.send(json.dumps(data, ensure_ascii=False))
    except Exception as e: log(f"send err: {e}")

def _to_text(c):
    if isinstance(c, str): return c
    if isinstance(c, list): return "\n".join(b.get("text","") if isinstance(b,dict) else str(b) for b in c)
    if isinstance(c, dict): return c.get("text", json.dumps(c, ensure_ascii=False))
    return str(c)

def fix_system_message(messages):
    result = []
    has_system = False
    for m in messages:
        if m.get("role") == "system":
            has_system = True
            content = m.get("content", "")
            if isinstance(content, list): content = _to_text(content)
            if not content.startswith(SYSTEM_PREFIX):
                content = SYSTEM_PREFIX + "\n\n" + content
            result.append({"role": "system", "content": content})
        else:
            result.append(m)
    if not has_system:
        result.insert(0, {"role": "system", "content": SYSTEM_PREFIX})
    return result

def convert_anthropic_tools(tools):

    if not tools: return None
    result = []
    for t in tools:
        if t.get("type") == "function":
            result.append(t)
        else:
            func = {"name": t.get("name",""), "description": t.get("description","")}
            params = t.get("input_schema", t.get("parameters", {}))
            func["parameters"] = params
            result.append({"type": "function", "function": func})
    return result if result else None

def convert_messages(parsed, path):
    messages = []
    if "/anthropic/" in path:
        s = parsed.get("system","")
        if s:
            s = strip_billing_header(_to_text(s))
            if s:
                messages.append({"role": "system", "content": s})
        for m in parsed.get("messages",[]):
            role = m.get("role","user")
            content = m.get("content","")
            if isinstance(content, list):
                content = clean_content_blocks(content)
                has_tool_use = any(b.get("type") == "tool_use" for b in content if isinstance(b, dict))
                has_tool_result = any(b.get("type") == "tool_result" for b in content if isinstance(b, dict))
                if has_tool_use:
                    text_parts = []
                    tool_calls = []
                    for b in content:
                        if isinstance(b, dict):
                            if b.get("type") == "text":
                                text_parts.append(b.get("text",""))
                            elif b.get("type") == "tool_use":
                                tool_calls.append({
                                    "id": b.get("id",""),
                                    "type": "function",
                                    "function": {
                                        "name": b.get("name",""),
                                        "arguments": json.dumps(b.get("input",{}), ensure_ascii=False)
                                    }
                                })
                    msg = {"role": "assistant", "content": "\n".join(text_parts) if text_parts else None}
                    if tool_calls:
                        msg["tool_calls"] = tool_calls
                    messages.append(msg)
                    continue
                elif has_tool_result:
                    for b in content:
                        if isinstance(b, dict) and b.get("type") == "tool_result":
                            result_content = b.get("content","")
                            if isinstance(result_content, list):
                                result_content = "\n".join(
                                    c.get("text","") if isinstance(c,dict) else str(c) for c in result_content
                                )
                            messages.append({
                                "role": "tool",
                                "tool_call_id": b.get("tool_use_id",""),
                                "content": str(result_content)
                            })
                    continue
                else:
                    content = _to_text(content)
            if role in ("user","assistant","system"):
                messages.append({"role": role, "content": content})
    else:
        for m in parsed.get("messages",[]):
            role = m.get("role","user")
            c = _to_text(m.get("content",""))
            messages.append({"role": role, "content": c})
    return fix_system_message(messages)

def openai_to_anthropic(oai):
    choice = oai.get("choices",[{}])[0]
    msg = choice.get("message",{})
    reasoning = msg.get("reasoning_content","")
    content_text = msg.get("content","") or ""
    tool_calls = msg.get("tool_calls")
    finish = choice.get("finish_reason","stop")
    usage = oai.get("usage",{})
    msg_id = "msg_" + uuid.uuid4().hex[:24]
    blocks = []
    if reasoning:
        blocks.append({"type": "thinking", "thinking": reasoning})
    if tool_calls:
        for tc in tool_calls:
            func = tc.get("function",{})
            try:
                inp = json.loads(func.get("arguments","{}"))
            except:
                inp = {"raw": func.get("arguments","")}
            blocks.append({
                "type": "tool_use",
                "id": tc.get("id", "toolu_" + uuid.uuid4().hex[:24]),
                "name": func.get("name",""),
                "input": inp
            })
    elif content_text:
        blocks.append({"type": "text", "text": content_text})
    else:
        blocks.append({"type": "text", "text": ""})
    if finish == "tool_calls":
        sr = "tool_use"
    elif finish == "stop":
        sr = "end_turn"
    elif finish == "length":
        sr = "max_tokens"
    else:
        sr = finish
    return {"id": msg_id, "type": "message", "role": "assistant", "content": blocks,
            "model": oai.get("model","mimo-v2.5-pro"), "stop_reason": sr, "stop_sequence": None,
            "usage": {"input_tokens": usage.get("prompt_tokens",0), "output_tokens": usage.get("completion_tokens",0)}}

async def stream_convert(ws, lock, req_id, r):
    msg_id = "msg_" + uuid.uuid4().hex[:24]
    model = "mimo-v2.5-pro"
    def sse(et, data):
        data["type"] = et
        return "event: " + et + "\ndata: " + json.dumps(data, ensure_ascii=False) + "\n\n"
    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("message_start", {
        "message": {"id": msg_id, "type": "message", "role": "assistant", "content": [], "model": model, "stop_reason": None, "usage": {"input_tokens": 0, "output_tokens": 0}}})})
    th = False; co = False; tl = 0; cl = 0
    tool_calls_buf = []
    current_tool_call = None
    async for raw in r.aiter_lines():
        line = raw.strip()
        if not line.startswith("data: "): continue
        p = line[6:]
        if p == "[DONE]": break
        try: d = json.loads(p)
        except: continue
        chs = d.get("choices", [])
        if not chs: continue
        delta = chs[0].get("delta", {})
        finish = chs[0].get("finish_reason")
        reasoning = delta.get("reasoning_content")
        content = delta.get("content")
        tc_delta = delta.get("tool_calls")
        model = d.get("model", model)

        if tc_delta:
            for tc in tc_delta:
                idx = tc.get("index", 0)
                while len(tool_calls_buf) <= idx:
                    tool_calls_buf.append({"id": "", "function": {"name": "", "arguments": ""}})
                buf = tool_calls_buf[idx]
                if tc.get("id"): buf["id"] = tc["id"]
                func = tc.get("function", {})
                if func.get("name"): buf["function"]["name"] += func["name"]
                if func.get("arguments"): buf["function"]["arguments"] += func["arguments"]
            continue

        if reasoning is not None:
            if not th:
                th = True
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_start", {"index": 0, "content_block": {"type": "thinking", "thinking": ""}})})
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("ping", {})})
            tl += len(reasoning)
            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_delta", {"index": 0, "delta": {"type": "thinking_delta", "thinking": reasoning}})})

        if content is not None:
            ci = 1 if th else 0
            if th and not co:
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_stop", {"index": 0})})
            if not co:
                co = True
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_start", {"index": ci, "content_block": {"type": "text", "text": ""}})})
            cl += len(content)
            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_delta", {"index": ci, "delta": {"type": "text_delta", "text": content}})})

        if finish:
            if th and not co and not tool_calls_buf:
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_stop", {"index": 0})})
            if co:
                ci = 1 if th else 0
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_stop", {"index": ci})})
            if tool_calls_buf:
                if th:
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_stop", {"index": 0})})
                for i, tc in enumerate(tool_calls_buf):
                    idx = (1 if th else 0) + i
                    try:
                        inp = json.loads(tc["function"]["arguments"])
                    except:
                        inp = {"raw": tc["function"]["arguments"]}
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_start", {"index": idx, "content_block": {"type": "tool_use", "id": tc["id"], "name": tc["function"]["name"], "input": {}}})})
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_delta", {"index": idx, "delta": {"type": "input_json_delta", "partial_json": json.dumps(inp, ensure_ascii=False)}})})
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("content_block_stop", {"index": idx})})

            if finish == "tool_calls":
                sr = "tool_use"
            elif finish == "stop":
                sr = "end_turn"
            elif finish == "length":
                sr = "max_tokens"
            else:
                sr = finish
            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("message_delta", {"delta": {"stop_reason": sr, "stop_sequence": None}, "usage": {"output_tokens": cl}})})
            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": sse("message_stop", {})})
    log(f"[{req_id}] thinking={tl} content={cl} tools={len(tool_calls_buf)}")

async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id")
    try:
        body = req.get("body","")
        parsed = body if isinstance(body, dict) else json.loads(body)
        path = req.get("path","")
        is_anthropic = "/anthropic/" in path
        model = parsed.get("model","mimo-v2.5-pro")
        stream = parsed.get("stream", False)
        max_tokens = parsed.get("max_tokens", 4096)
        tools = parsed.get("tools")

        for field in ("context_management", "metadata", "output_config", "thinking"):
            parsed.pop(field, None)

        log(f"[{req_id}] path={path} model={model} stream={stream} tools={len(tools) if tools else 0}")

        if is_anthropic:
            messages = convert_messages(parsed, path)
            openai_tools = convert_anthropic_tools(tools)
        else:
            messages = []
            for m in parsed.get("messages",[]):
                messages.append(dict(m))
            messages = fix_system_message(messages)
            openai_tools = tools  # Already OpenAI format

        log(f"[{req_id}] msgs={len(messages)}")
        req_body = {"model": model, "messages": messages, "stream": stream}
        if max_tokens and max_tokens >= 100:
            req_body["max_tokens"] = max_tokens
        elif not max_tokens:
            req_body["max_tokens"] = 4096
        if openai_tools:
            req_body["tools"] = openai_tools
        req_body_str = json.dumps(req_body, ensure_ascii=False)
        url = f"{BASE}/v1/chat/completions"
        headers = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
        log(f"[{req_id}] -> {url} body_len={len(req_body_str)}")
        async with client.stream("POST", url, headers=headers, content=req_body_str) as r:
            log(f"[{req_id}] status={r.status_code}")
            await safe_send(ws, lock, {"req_id": req_id, "type": "start", "status": r.status_code, "headers": dict(r.headers)})
            if r.status_code == 200:
                if is_anthropic:
                    if stream:
                        await stream_convert(ws, lock, req_id, r)
                    else:
                        resp_text = ""
                        async for chunk in r.aiter_text(): resp_text += chunk
                        oai = json.loads(resp_text)
                        anthropic = openai_to_anthropic(oai)
                        await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": json.dumps(anthropic, ensure_ascii=False)})
                        log(f"[{req_id}] converted ok stop={anthropic.get('stop_reason')}")
                else:
                    total = 0
                    async for chunk in r.aiter_text():
                        if chunk:
                            total += len(chunk)
                            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": chunk})
                    log(f"[{req_id}] passthrough {total} bytes")
            else:
                err = ""
                async for chunk in r.aiter_text(): err += chunk
                log(f"[{req_id}] err: {err[:200]}")
                await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": err})
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})
            log(f"[{req_id}] done")
    except Exception as e:
        log(f"[{req_id}] ERROR: {e}")
        traceback.print_exc()
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

async def sync_models(ws, client):
    if not KEY: log("WARN: no key"); return
    try:
        resp = await client.get(f"{BASE}/v1/models", headers={"Authorization": f"Bearer {KEY}"}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            ids = [m.get("id","") for m in data.get("data",[])]
            await ws.send(json.dumps({"req_id":"__models__","type":"models","body":ids}))
            log(f"models: {len(ids)}")
    except Exception as e: log(f"sync err: {e}")

async def main():
    if not WS_URL: log("ERROR: no WS_URL"); return
    log(f"bridge start WS={WS_URL} API={BASE}")
    timeout = httpx.Timeout(connect=10, read=300, write=30, pool=10)
    async with httpx.AsyncClient(timeout=timeout) as client:
        retry = 0
        while True:
            try:
                retry += 1
                if retry > 1: log(f"reconnect #{retry}...")
                async with websockets.connect(WS_URL, max_size=10**8, open_timeout=15, close_timeout=5, ping_interval=20, ping_timeout=10) as ws:
                    retry = 0; log("WS connected")
                    await sync_models(ws, client)
                    lock = asyncio.Lock()
                    while True:
                        try:
                            raw = await ws.recv(decode=False)
                            data = json.loads(raw.decode("utf-8", errors="replace"))
                            log(f"recv type={data.get('type','?')} path={data.get('path','?')}")
                            asyncio.create_task(handle_request(ws, data, client, lock))
                        except websockets.exceptions.ConnectionClosed: log("WS closed"); break
                        except json.JSONDecodeError as e: log(f"json err: {e}")
                        except Exception as e: log(f"recv err: {e}")
            except Exception as e:
                log(f"WS err: {type(e).__name__}: {e}")
                if retry <= 3: traceback.print_exc()
            await asyncio.sleep(3)

if __name__ == "__main__": asyncio.run(main())
