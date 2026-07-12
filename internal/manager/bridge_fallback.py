#!/usr/bin/env python3
"""
MClaw Bridge (内置 fallback 版本)

此文件通过 go:embed 嵌入到 MClaw 二进制中，作为外部 scripts/bridge.py 的回退。
WS_URL 通过 %s 占位符由 Go fmt.Sprintf 在运行时注入。
"""
import asyncio, websockets, httpx, json, os, traceback, base64
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
KEY = _cfg_key or os.getenv("MIMO_API_KEY","")
BASE = _cfg_base
WS_URL_B64 = "%s"
WS_URL = base64.b64decode(WS_URL_B64).decode() if WS_URL_B64 != "__WS_URL_B64__" else ""

def log(msg):
    print(f"[{datetime.now().strftime('%%H:%%M:%%S')}] {msg}", flush=True)


async def safe_send(ws, lock, data):
    try:
        async with lock: await ws.send(json.dumps(data, ensure_ascii=False))
    except: pass

def anthropic_to_openai(parsed):
    openai = {}
    openai["model"] = parsed.get("model", "mimo-v2.5-pro")
    openai["max_tokens"] = parsed.get("max_tokens", 8192)
    if parsed.get("stream") is not None:
        openai["stream"] = parsed["stream"]
    messages = []
    system = parsed.get("system")
    if system:
        if isinstance(system, list):
            texts = []
            for s in system:
                if isinstance(s, dict) and s.get("type") == "text":
                    texts.append(s.get("text", ""))
                elif isinstance(s, str):
                    texts.append(s)
            system_text = "\n".join(texts)
        else:
            system_text = str(system)
        if system_text.strip():
            messages.append({"role": "system", "content": system_text})
    for msg in parsed.get("messages", []):
        role = msg.get("role", "user")
        content = msg.get("content")
        if isinstance(content, str):
            messages.append({"role": role, "content": content})
        elif isinstance(content, list):
            texts = []
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") == "text":
                        texts.append(block.get("text", ""))
                    elif block.get("type") == "image":
                        source = block.get("source", {})
                        if source.get("type") == "base64":
                            media_type = source.get("media_type", "image/png")
                            data = source.get("data", "")
                            texts.append({"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{data}"}})
                        elif source.get("type") == "url":
                            texts.append({"type": "image_url", "image_url": {"url": source.get("url", "")}})
                elif isinstance(block, str):
                    texts.append(block)
            if all(isinstance(t, str) for t in texts):
                messages.append({"role": role, "content": "\n".join(texts)})
            else:
                messages.append({"role": role, "content": [t for t in texts if isinstance(t, (str, dict))]})
        else:
            messages.append({"role": role, "content": str(content) if content is not None else ""})
    openai["messages"] = messages
    if parsed.get("tools"):
        openai_tools = []
        for tool in parsed["tools"]:
            openai_tools.append({"type": "function", "function": {"name": tool.get("name", ""), "description": tool.get("description", ""), "parameters": tool.get("input_schema", {})}})
        openai["tools"] = openai_tools
    return openai

def openai_chunk_to_anthropic_sse(openai_line, state):
    events = []
    if not openai_line.startswith("data: "):
        return events
    data_str = openai_line[6:].strip()
    if data_str == "[DONE]":
        events.append(f"event: message_stop\ndata: {{}}\n")
        return events
    try:
        chunk = json.loads(data_str)
    except json.JSONDecodeError:
        return events
    choice = (chunk.get("choices") or [{}])[0]
    delta = choice.get("delta", {})
    finish_reason = choice.get("finish_reason")
    if not state.get("started"):
        state["started"] = True
        model = chunk.get("model", "mimo-v2.5-pro")
        msg_id = chunk.get("id", "msg_" + model.replace(".", ""))
        state["msg_id"] = msg_id
        events.append(f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'model': model, 'content': [], 'stop_reason': None, 'usage': {'input_tokens': 0, 'output_tokens': 0}}})}\n")
    if delta.get("content") and not state.get("block_started"):
        state["block_started"] = True
        events.append(f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}})}\n")
    if delta.get("content"):
        events.append(f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': delta['content']}})}\n")
    if finish_reason:
        if state.get("block_started"):
            events.append(f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n")
        stop_reason = "end_turn"
        if finish_reason == "length":
            stop_reason = "max_tokens"
        elif finish_reason == "tool_calls":
            stop_reason = "tool_use"
        usage = chunk.get("usage", {})
        events.append(f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': stop_reason, 'stop_sequence': None}, 'usage': {'output_tokens': usage.get('completion_tokens', 0)}})}\n")
    return events

def openai_response_to_anthropic(openai_resp):
    choice = (openai_resp.get("choices") or [{}])[0]
    message = choice.get("message", {})
    finish_reason = choice.get("finish_reason", "stop")
    usage = openai_resp.get("usage", {})
    content = []
    text = message.get("content", "")
    if text:
        content.append({"type": "text", "text": text})
    stop_reason = "end_turn"
    if finish_reason == "length":
        stop_reason = "max_tokens"
    elif finish_reason == "tool_calls":
        stop_reason = "tool_use"
    return {"id": openai_resp.get("id", "msg_001"), "type": "message", "role": "assistant", "model": openai_resp.get("model", "mimo-v2.5-pro"), "content": content, "stop_reason": stop_reason, "stop_sequence": None, "usage": {"input_tokens": usage.get("prompt_tokens", 0), "output_tokens": usage.get("completion_tokens", 0)}}

async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id")
    try:
        body = req.get("body", "")
        log(f"[{req_id}] body_type={type(body).__name__} body_len={len(str(body))}")
        if isinstance(body, dict):
            parsed = body
        else:
            parsed = json.loads(body)
        log(f"[{req_id}] parsed_type={type(parsed).__name__} keys={list(parsed.keys()) if isinstance(parsed, dict) else 'N/A'}")
        path = req.get("path", "")
        is_anthropic = "/anthropic/" in path
        if is_anthropic:
            log(f"[{req_id}] Anthropic → OpenAI 格式转换")
            parsed = anthropic_to_openai(parsed)
        if "mimo-v2.5-pro" in parsed.get("model", ""):
            msgs = parsed.get("messages", [])
            if not any(m.get("role") == "system" for m in msgs):
                parsed["messages"] = [{"role": "system", "content": "You are a personal assistant running inside OpenClaw."}] + msgs
        if not parsed.get("max_tokens") and parsed.get("model") in ("mimo-v2.5-pro", "mimo-v2.5"):
            parsed["max_tokens"] = 131072
        body = json.dumps(parsed, ensure_ascii=False)
        log(f"[{req_id}] 发送MIMO body={body[:300]}")
        url = f"{BASE}/v1/chat/completions"
        auth_hdr = {"Authorization": f"Bearer {KEY}"}
        is_stream = parsed.get("stream", False)
        async with client.stream(method=req.get("method", "POST"), url=url,
            headers={**auth_hdr, "Content-Type": "application/json"}, content=body) as r:
            await safe_send(ws, lock, {"req_id": req_id, "type": "start", "status": r.status_code, "headers": dict(r.headers)})
            if is_anthropic and is_stream:
                state = {}
                async for chunk in r.aiter_text():
                    if not chunk: continue
                    for line in chunk.split("\n"):
                        line = line.strip()
                        if not line: continue
                        for event in openai_chunk_to_anthropic_sse(line, state):
                            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": event})
            elif is_anthropic and not is_stream:
                full_body = ""
                async for chunk in r.aiter_text():
                    if chunk: full_body += chunk
                try:
                    openai_resp = json.loads(full_body)
                    anthropic_resp = openai_response_to_anthropic(openai_resp)
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": json.dumps(anthropic_resp, ensure_ascii=False)})
                except json.JSONDecodeError:
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": full_body})
            else:
                async for chunk in r.aiter_text():
                    if chunk: await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": chunk})
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})
    except Exception as e:
        log(f"[{req_id}] 请求失败: {e}\n{traceback.format_exc()}")
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

async def sync_models(ws, client):
    if not KEY: return
    try:
        resp = await client.get(f"{BASE}/v1/models", headers={"Authorization":f"Bearer {KEY}"}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            model_ids = [m.get("id","") for m in data.get("data",[])]
            await ws.send(json.dumps({"req_id":"__models__","type":"models","body":model_ids}))
            log(f"模型同步: {len(model_ids)} 个")
    except Exception as e: log(f"模型同步异常: {e}")

async def main():
    if not WS_URL: log("错误: WS_URL 未设置"); return
    log(f"bridge 启动 WS={WS_URL} API={BASE}")
    timeout = httpx.Timeout(connect=10, read=300, write=30, pool=10)
    async with httpx.AsyncClient(timeout=timeout) as client:
        retry = 0
        while True:
            try:
                retry += 1
                if retry > 1: log(f"WS 重连 (第{retry}次)...")
                async with websockets.connect(WS_URL, max_size=10**8, open_timeout=15, close_timeout=5, ping_interval=20, ping_timeout=10) as ws:
                    retry = 0; log("WS 已连接")
                    await sync_models(ws, client)
                    lock = asyncio.Lock()
                    while True:
                        try:
                            raw_msg = await ws.recv(decode=False)
                            msg = raw_msg.decode("utf-8", errors="replace")
                            data = json.loads(msg)
                            log(f"收到消息 type={data.get('type','?')} path={data.get('path','?')}")
                            asyncio.create_task(handle_request(ws, data, client, lock))
                        except websockets.exceptions.ConnectionClosed:
                            log("WS 连接被关闭"); break
                        except json.JSONDecodeError as e:
                            log(f"消息解析失败: {e}")
                        except Exception as e:
                            log(f"接收异常: {type(e).__name__}: {e}")
            except Exception as e:
                log(f"WS 断开: {type(e).__name__}: {e}")
                if retry <= 3: traceback.print_exc()
                await asyncio.sleep(3)

if __name__ == "__main__": asyncio.run(main())
