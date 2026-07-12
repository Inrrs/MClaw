#!/usr/bin/env python3
"""
MClaw Bridge — 运行在 MIMO Claw 容器内部的反向代理客户端

职责：
1. 通过 WebSocket 连接到公网 MClaw 网关
2. 接收网关转发的 API 请求
3. 用 httpx 转发到本地 MIMO API
4. 流式回传响应给网关

部署方式：
  - 由 MClaw 网关通过 AI 聊天自动注入到 Claw 容器
  - 也可手动部署：python3 bridge.py
  - WS_URL 通过环境变量 MCLAW_WS_URL 或运行时替换 __WS_URL__ 设定
"""

import asyncio
import base64
import json
import os
import traceback
from datetime import datetime

import httpx
import websockets

# ─── 配置 ────────────────────────────────────────────────────

def _load_mimo_config():
    """从 OpenClaw 配置文件读取 MIMO API 密钥和地址"""
    for p in [
        os.path.expanduser("~/.openclaw/openclaw.json"),
        "/root/.openclaw/openclaw.json",
        "/opt/mimo-claw-seed/bundle/openclaw/openclaw.json",
    ]:
        try:
            with open(p) as f:
                cfg = json.load(f)
            x = cfg.get("models", {}).get("providers", {}).get("xiaomi", {})
            b, k = x.get("baseUrl", ""), x.get("apiKey", "")
            k = os.path.expandvars(k) if k.startswith("${") else k
            if b.startswith("${"):
                b = os.path.expandvars(b)
            b = b.rstrip("/v1")
            if k and b:
                return k, b
        except Exception:
            continue
    return "", ""


_cfg_key, _cfg_base = _load_mimo_config()
KEY = _cfg_key or os.getenv("MIMO_API_KEY", "")
BASE = _cfg_base

# WS_URL 解析优先级：环境变量 > 运行时替换 > 空
WS_URL = os.getenv("MCLAW_WS_URL", "")
if not WS_URL:
    _ws_b64 = "__WS_URL_B64__"
    if _ws_b64 != "__WS_URL_B64__":
        WS_URL = base64.b64decode(_ws_b64).decode()
    else:
        WS_URL = "__WS_URL__"
        if WS_URL == "__WS_URL__":
            WS_URL = ""

# 推理模型列表（需要 thinking 参数）
REASONING_MODELS = {"mimo-v2.5-pro", "mimo-v2.5", "mimo-v2-pro", "mimo-v2-omni", "mimo-v2.6-pro"}

# mimo-v2.5-pro 必须注入的 system prompt
REQUIRED_SYSTEM_PROMPT = "You are a personal assistant running inside OpenClaw."


# ─── 工具函数 ────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


async def safe_send(ws, lock, data):
    """线程安全的 WebSocket 发送"""
    try:
        async with lock:
            await ws.send(json.dumps(data, ensure_ascii=False))
    except websockets.exceptions.ConnectionClosed:
        pass  # 连接已关闭，静默忽略
    except Exception as e:
        log(f"safe_send 异常: {type(e).__name__}: {e}")


# ─── Anthropic ↔ OpenAI 格式转换 ─────────────────────────────

def anthropic_to_openai(parsed):
    """将 Anthropic Messages API 请求体转换为 OpenAI Chat Completions 格式"""
    openai = {}
    openai["model"] = parsed.get("model", "mimo-v2.5-pro")
    openai["max_tokens"] = parsed.get("max_tokens", 8192)
    if parsed.get("stream") is not None:
        openai["stream"] = parsed["stream"]

    messages = []
    # Anthropic system 是顶级字段
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

    # 转换 messages
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
                            texts.append({
                                "type": "image_url",
                                "image_url": {"url": f"data:{media_type};base64,{data}"}
                            })
                        elif source.get("type") == "url":
                            texts.append({
                                "type": "image_url",
                                "image_url": {"url": source.get("url", "")}
                            })
                    elif block.get("type") == "tool_use":
                        texts.append(json.dumps({
                            "type": "function",
                            "function": {
                                "name": block.get("name", ""),
                                "arguments": json.dumps(block.get("input", {}))
                            }
                        }))
                    elif block.get("type") == "tool_result":
                        texts.append(str(block.get("content", "")))
                elif isinstance(block, str):
                    texts.append(block)

            if all(isinstance(t, str) for t in texts):
                messages.append({"role": role, "content": "\n".join(texts)})
            else:
                messages.append({"role": role, "content": [t for t in texts if isinstance(t, (str, dict))]})
        else:
            messages.append({"role": role, "content": str(content) if content is not None else ""})

    openai["messages"] = messages

    # 转换 tools
    if parsed.get("tools"):
        openai_tools = []
        for tool in parsed["tools"]:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool.get("name", ""),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {})
                }
            })
        openai["tools"] = openai_tools

    return openai


def openai_chunk_to_anthropic_sse(openai_line, state):
    """将单行 OpenAI SSE 数据转换为 Anthropic SSE 事件列表"""
    events = []
    if not openai_line.startswith("data: "):
        return events

    data_str = openai_line[6:].strip()
    if data_str == "[DONE]":
        events.append("event: message_stop\ndata: {}\n")
        return events

    try:
        chunk = json.loads(data_str)
    except json.JSONDecodeError:
        return events

    choice = (chunk.get("choices") or [{}])[0]
    delta = choice.get("delta", {})
    finish_reason = choice.get("finish_reason")

    # 第一个 chunk：发送 message_start
    if not state.get("started"):
        state["started"] = True
        model = chunk.get("model", "mimo-v2.5-pro")
        msg_id = chunk.get("id", "msg_" + model.replace(".", ""))
        state["msg_id"] = msg_id
        state["model"] = model
        events.append(f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'model': model, 'content': [], 'stop_reason': None, 'usage': {'input_tokens': 0, 'output_tokens': 0}}})}\n")

    # thinking content（推理模型的 reasoning_content）
    reasoning = delta.get("reasoning_content")
    if reasoning and not state.get("thinking_started"):
        state["thinking_started"] = True
        events.append(f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'thinking', 'thinking': ''}})}\n")
    if reasoning:
        events.append(f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'thinking_delta', 'thinking': reasoning}})}\n")

    # content_block_start（首次有文本内容时）
    if delta.get("content") and not state.get("block_started"):
        # 如果有 thinking，先关闭 thinking block
        if state.get("thinking_started") and not state.get("thinking_closed"):
            state["thinking_closed"] = True
            events.append(f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n")
        block_idx = 1 if state.get("thinking_started") else 0
        state["block_started"] = True
        state["block_idx"] = block_idx
        events.append(f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': block_idx, 'content_block': {'type': 'text', 'text': ''}})}\n")

    # content_block_delta（文本增量）
    if delta.get("content"):
        idx = state.get("block_idx", 0)
        events.append(f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': idx, 'delta': {'type': 'text_delta', 'text': delta['content']}})}\n")

    # 处理 tool_calls
    if delta.get("tool_calls"):
        for tc in delta["tool_calls"]:
            idx = tc.get("index", 0)
            tool_idx = idx + (2 if state.get("thinking_started") else 1)
            if tc.get("id") and not state.get(f"tool_{idx}_started"):
                state[f"tool_{idx}_started"] = True
                state[f"tool_{idx}_id"] = tc["id"]
                fn_name = tc.get("function", {}).get("name", "")
                events.append(f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': tool_idx, 'content_block': {'type': 'tool_use', 'id': tc['id'], 'name': fn_name, 'input': {}}})}\n")
            if tc.get("function", {}).get("arguments"):
                events.append(f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': tool_idx, 'delta': {'type': 'input_json_delta', 'partial_json': tc['function']['arguments']}})}\n")

    # finish_reason → stop_reason
    if finish_reason:
        # 关闭所有打开的 content block
        if state.get("thinking_started") and not state.get("thinking_closed"):
            state["thinking_closed"] = True
            events.append(f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n")
        if state.get("block_started"):
            events.append(f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': state.get('block_idx', 0)})}\n")
        stop_reason = "end_turn"
        if finish_reason == "length":
            stop_reason = "max_tokens"
        elif finish_reason == "tool_calls":
            stop_reason = "tool_use"
        usage = chunk.get("usage", {})
        events.append(f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': stop_reason, 'stop_sequence': None}, 'usage': {'output_tokens': usage.get('completion_tokens', 0)}})}\n")

    return events


def openai_response_to_anthropic(openai_resp):
    """将 OpenAI 非流式响应转换为 Anthropic 格式"""
    choice = (openai_resp.get("choices") or [{}])[0]
    message = choice.get("message", {})
    finish_reason = choice.get("finish_reason", "stop")
    usage = openai_resp.get("usage", {})

    content = []

    # reasoning_content → thinking block
    reasoning = message.get("reasoning_content")
    if reasoning:
        content.append({"type": "thinking", "thinking": reasoning})

    text = message.get("content", "")
    if text:
        content.append({"type": "text", "text": text})

    # tool_calls
    if message.get("tool_calls"):
        for tc in message["tool_calls"]:
            fn = tc.get("function", {})
            try:
                inp = json.loads(fn.get("arguments", "{}"))
            except json.JSONDecodeError:
                inp = {}
            content.append({
                "type": "tool_use",
                "id": tc.get("id", ""),
                "name": fn.get("name", ""),
                "input": inp
            })

    stop_reason = "end_turn"
    if finish_reason == "length":
        stop_reason = "max_tokens"
    elif finish_reason == "tool_calls":
        stop_reason = "tool_use"

    return {
        "id": openai_resp.get("id", "msg_001"),
        "type": "message",
        "role": "assistant",
        "model": openai_resp.get("model", "mimo-v2.5-pro"),
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
        }
    }


# ─── 请求处理 ────────────────────────────────────────────────

async def handle_request(ws, req, client, lock):
    """处理单个 API 请求：转发到 MIMO → 流式回传"""
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

        # Anthropic 格式 → 转换为 OpenAI 格式
        if is_anthropic:
            log(f"[{req_id}] Anthropic 请求，转换为 OpenAI 格式")
            parsed = anthropic_to_openai(parsed)
            log(f"[{req_id}] 转换后 keys={list(parsed.keys())}")

        model = parsed.get("model", "")

        # mimo-v2.5-pro 必须注入 system prompt
        if "mimo-v2.5-pro" in model:
            msgs = parsed.get("messages", [])
            parsed["messages"] = [{"role": "system", "content": REQUIRED_SYSTEM_PROMPT}] + msgs

        # 推理模型添加 thinking 参数
        base_model = model.split("/")[-1] if "/" in model else model
        if base_model in REASONING_MODELS:
            parsed["thinking"] = {"type": "enabled"}
            parsed["reasoning_effort"] = "high"

        # 默认 max_tokens
        if not parsed.get("max_tokens") and base_model in ("mimo-v2.5-pro", "mimo-v2.5"):
            parsed["max_tokens"] = 131072

        body = json.dumps(parsed, ensure_ascii=False)
        log(f"[{req_id}] 发送MIMO model={model} body_len={len(body)} stream={parsed.get('stream', False)}")

        # 统一走 OpenAI 端点
        url = f"{BASE}/v1/chat/completions"
        auth_hdr = {"Authorization": f"Bearer {KEY}"}
        is_stream = parsed.get("stream", False)

        async with client.stream(
            method=req.get("method", "POST"),
            url=url,
            headers={**auth_hdr, "Content-Type": "application/json"},
            content=body,
        ) as r:
            log(f"[{req_id}] HTTP status={r.status_code}")

            # 非 200 响应：读取错误体并回传
            if r.status_code != 200:
                error_body = ""
                async for chunk in r.aiter_text():
                    if chunk:
                        error_body += chunk
                log(f"[{req_id}] API 错误 {r.status_code}: {error_body[:200]}")
                await safe_send(ws, lock, {
                    "req_id": req_id, "type": "start",
                    "status": r.status_code, "headers": dict(r.headers),
                })
                await safe_send(ws, lock, {
                    "req_id": req_id, "type": "error",
                    "status": r.status_code,
                    "body": json.dumps({"error": {"message": f"MIMO API error {r.status_code}: {error_body[:500]}", "code": r.status_code}}),
                })
                return

            await safe_send(ws, lock, {
                "req_id": req_id, "type": "start",
                "status": r.status_code, "headers": dict(r.headers),
            })

            if is_anthropic and is_stream:
                # 流式：OpenAI SSE → 转换为 Anthropic SSE
                state = {}
                async for chunk in r.aiter_text():
                    if not chunk:
                        continue
                    for line in chunk.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        events = openai_chunk_to_anthropic_sse(line, state)
                        for event in events:
                            await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": event})
            elif is_anthropic and not is_stream:
                # 非流式：收集完整响应后转换
                full_body = ""
                async for chunk in r.aiter_text():
                    if chunk:
                        full_body += chunk
                try:
                    openai_resp = json.loads(full_body)
                    anthropic_resp = openai_response_to_anthropic(openai_resp)
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": json.dumps(anthropic_resp, ensure_ascii=False)})
                except json.JSONDecodeError:
                    log(f"[{req_id}] 响应解析失败，原样返回")
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": full_body})
            else:
                # OpenAI 格式：原样转发
                async for chunk in r.aiter_text():
                    if chunk:
                        await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": chunk})

            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})

    except Exception as e:
        log(f"[{req_id}] 请求失败: {e}\n{traceback.format_exc()}")
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})


# ─── 模型同步 ────────────────────────────────────────────────

async def sync_models(ws, client):
    """启动时同步可用模型列表给网关"""
    if not KEY:
        return
    try:
        resp = await client.get(
            f"{BASE}/v1/models",
            headers={"Authorization": f"Bearer {KEY}"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            model_ids = [m.get("id", "") for m in data.get("data", [])]
            await ws.send(json.dumps({
                "req_id": "__models__", "type": "models", "body": model_ids,
            }))
            log(f"模型同步: {len(model_ids)} 个")
    except Exception as e:
        log(f"模型同步异常: {e}")


# ─── 主循环 ──────────────────────────────────────────────────

async def main():
    if not WS_URL:
        log("错误: WS_URL 未设置 (通过环境变量 MCLAW_WS_URL 或运行时替换)")
        return

    log(f"bridge 启动 WS={WS_URL} API={BASE}")
    timeout = httpx.Timeout(connect=10, read=300, write=30, pool=10)

    async with httpx.AsyncClient(timeout=timeout) as client:
        retry = 0
        while True:
            try:
                retry += 1
                if retry > 1:
                    log(f"WS 重连 (第{retry}次)...")

                async with websockets.connect(
                    WS_URL,
                    max_size=10**8,
                    open_timeout=15,
                    close_timeout=5,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    retry = 0
                    log("WS 已连接")
                    await sync_models(ws, client)
                    lock = asyncio.Lock()

                    while True:
                        try:
                            raw_msg = await ws.recv(decode=False)
                            msg = raw_msg.decode("utf-8", errors="replace")
                            data = json.loads(msg)
                            log(f"收到消息 type={data.get('type', '?')} path={data.get('path', '?')}")
                            asyncio.create_task(handle_request(ws, data, client, lock))
                        except websockets.exceptions.ConnectionClosed:
                            log("WS 连接被关闭")
                            break
                        except json.JSONDecodeError as e:
                            log(f"消息解析失败: {e}")
                        except Exception as e:
                            log(f"接收异常: {type(e).__name__}: {e}")

            except Exception as e:
                log(f"WS 断开: {type(e).__name__}: {e}")
                if retry <= 3:
                    traceback.print_exc()
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
