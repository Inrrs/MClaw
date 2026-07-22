#!/usr/bin/env python3
"""
MClaw Bridge (极简透传版)
对齐 mimi3 的纯透传思路，所有格式转换由网关处理。
bridge 只负责：连网关 → 收请求 → 透传给 MIMO → 回传结果。
"""
import asyncio, websockets, httpx, json, os

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
WS_URL = "__WS_URL__"
SYSTEM_PREFIX = "You are a personal assistant running inside OpenClaw."

def log(msg):
    print(f"[{__import__('datetime').datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def safe_send(ws, lock, data):
    try:
        async with lock: await ws.send(json.dumps(data, ensure_ascii=True))
    except: pass

def inject_system(body_str):
    """OpenAI 请求：注入 system prompt（仅 mimo-v2.5-pro）"""
    try: d = json.loads(body_str)
    except: return body_str
    model = d.get("model", "")
    if model and "mimo-v2.5-pro" not in model:
        return body_str
    msgs = d.get("messages", [])
    if any(m.get("role") == "system" for m in msgs):
        return body_str
    msgs.insert(0, {"role": "system", "content": SYSTEM_PREFIX})
    d["messages"] = msgs
    mt = d.get("max_tokens", 4096)
    if mt > 16384: d["max_tokens"] = 16384
    if not mt: d["max_tokens"] = 4096
    return json.dumps(d, ensure_ascii=True)

async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id")
    try:
        body_str = req.get("body", "")
        if not isinstance(body_str, str):
            body_str = json.dumps(body_str, ensure_ascii=True)
        # 仅在缺 system prompt 时注入，其余原样透传
        body_str = inject_system(body_str)
        url = f"{BASE}/v1/chat/completions"
        headers = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
        async with client.stream("POST", url, headers=headers, content=body_str) as r:
            await safe_send(ws, lock, {"req_id": req_id, "type": "start", "status": r.status_code})
            async for chunk in r.aiter_bytes():
                if chunk:
                    await safe_send(ws, lock, {"req_id": req_id, "type": "chunk", "body": chunk.decode("utf-8", errors="replace")})
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})
    except Exception as e:
        log(f"[{req_id}] err: {e}")
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

async def sync_models(ws, client, lock):
    if not KEY: return
    try:
        r = await client.get(f"{BASE}/v1/models", headers={"Authorization": f"Bearer {KEY}"}, timeout=15)
        if r.status_code == 200:
            ids = [m.get("id","") for m in r.json().get("data",[])]
            await safe_send(ws, lock, {"req_id":"__models__","type":"models","body":ids})
            log(f"models: {len(ids)}")
    except: pass

async def main():
    _placeholder = "__" + "WS_URL" + "__"
    if not WS_URL or WS_URL == _placeholder:
        log("ERROR: no WS_URL")
        return
    log(f"bridge start WS={WS_URL} API={BASE}")
    async with httpx.AsyncClient(timeout=None) as client:
        retry = 0
        while True:
            try:
                retry += 1
                if retry > 1: log(f"reconnect #{retry}...")
                async with websockets.connect(WS_URL, max_size=10**8, open_timeout=15, close_timeout=5, ping_interval=20, ping_timeout=10) as ws:
                    retry = 0; log("WS connected")
                    lock = asyncio.Lock()
                    await sync_models(ws, client, lock)
                    while True:
                        try:
                            raw = await ws.recv(decode=False)
                            data = json.loads(raw.decode("utf-8", errors="replace"))
                            asyncio.create_task(handle_request(ws, data, client, lock))
                        except websockets.exceptions.ConnectionClosed: log("WS closed"); break
                        except: pass
            except Exception as e: log(f"WS err: {type(e).__name__}: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__": asyncio.run(main())
