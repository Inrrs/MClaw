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
        if parsed.get("model") == "mimo-v2.5-pro":
            if "/anthropic/" in path:
                if not parsed.get("system"):
                    parsed["system"] = "You are a personal assistant running inside OpenClaw."
            else:
                msgs = parsed.get("messages", [])
                if not any(m.get("role") == "system" for m in msgs):
                    parsed["messages"] = [{"role": "system", "content": "You are a personal assistant running inside OpenClaw."}] + msgs
        if not parsed.get("max_tokens") and parsed.get("model") in ("mimo-v2.5-pro", "mimo-v2.5"):
            parsed["max_tokens"] = 131072
        body = json.dumps(parsed, ensure_ascii=False)
        log(f"[{req_id}] 发送MIMO body={body[:300]}")
        if "/anthropic/" in path:
            url, auth_hdr = f"{BASE}/anthropic/v1/messages", {"x-api-key": KEY}
        else:
            url, auth_hdr = f"{BASE}/v1/chat/completions", {"Authorization": f"Bearer {KEY}"}
        async with client.stream(method=req.get("method", "POST"), url=url,
            headers={**auth_hdr, "Content-Type": "application/json"}, content=body) as r:
            await safe_send(ws, lock, {"req_id": req_id, "type": "start", "status": r.status_code, "headers": dict(r.headers)})
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
