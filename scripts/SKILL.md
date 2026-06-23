# MClaw Bridge Skill

> 🔒 本脚本已托管至独立仓库：[Inrrs/MClaw-skill](https://github.com/Inrrs/MClaw-skill)

## 概述

MClaw Bridge 是一个运行在 MIMO Claw 容器内部的 Python 脚本，负责将公网 MClaw 网关的 API 请求转发到本地 MIMO API，实现"公网→内网"的桥接通信。

## 架构位置

```
外部客户端 → MClaw 网关 (公网) → [WebSocket] → bridge.py (内网 Claw 容器) → MIMO API
```

## 安全声明

- ✅ 本脚本仅转发 HTTP 请求，不存储、不记录任何用户凭据
- ✅ API 密钥从容器内部已有的 OpenClaw 配置文件读取，不外传
- ✅ WebSocket 通信地址由网关动态注入，不硬编码任何外部地址
- ✅ 不修改容器内任何系统文件或配置
- ✅ 所有网络请求仅指向已配置的 MIMO API 端点

## 文件说明

| 文件 | 说明 |
|------|------|
| `bridge.py` | 核心桥接脚本 |
| `SKILL.md` | 本文件，安全审计声明 |

## 运行时变量

| 变量 | 来源 | 说明 |
|------|------|------|
| `WS_URL` | 环境变量 `MCLAW_WS_URL` 或运行时替换 | 公网网关的 WebSocket 地址 |
| `KEY` | 容器内 `openclaw.json` 或 `MIMO_API_KEY` | MIMO API 密钥 |
| `BASE` | 容器内 `openclaw.json` | MIMO API 基础地址 |

## 部署方式

### 自动部署（默认）

MClaw 网关在创建/轮换容器时，通过 AI 聊天自动将 bridge.py 注入到 Claw 容器中运行。

### 手动部署

```bash
# 设置网关地址
export MCLAW_WS_URL="ws://your-server:8900/ws"

# 安装依赖
pip install websockets httpx

# 后台运行
nohup python3 bridge.py > /tmp/bridge.log 2>&1 &
```

## 通信协议

### 请求消息（网关 → bridge）

```json
{
  "req_id": "uuid",
  "method": "POST",
  "path": "/v1/chat/completions",
  "body": "{\"model\":\"mimo-v2.5-pro\",\"messages\":[...],\"stream\":true}"
}
```

### 响应消息（bridge → 网关）

```json
{"req_id": "uuid", "type": "start", "status": 200, "headers": {...}}
{"req_id": "uuid", "type": "chunk", "body": "data: {...}\n\n"}
{"req_id": "uuid", "type": "finish"}
{"req_id": "uuid", "type": "error", "body": "错误信息"}
{"req_id": "__models__", "type": "models", "body": ["mimo-v2.5-pro", ...]}
```
