# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MClaw (MIMO Protocol Gateway) 是一个 API 网关，将多个小米 AI Studio (MIMO) 容器实例聚合为兼容 OpenAI/Anthropic 格式的统一端点。Go 1.23 编写，WebUI 内嵌为单一 HTML 文件。

## Build & Run

```bash
# 本地运行
go run cmd/gateway/main.go -config data/config.json

# 构建二进制
go build -o mclaw cmd/gateway/main.go

# Docker
docker build -t cinrrs/mclaw:latest .
docker compose up -d

# 测试
go test ./...

# 运行单个测试
go test ./internal/manager/ -run TestFunctionName -v
```

CLI flags: `-config` (配置 JSON，默认 `data/config.json`), `-log-dir` (日志目录，默认 `logs`)

## Architecture

### 请求流程

```
Client (OpenAI/Anthropic 格式)
  -> HTTP API (/v1/chat/completions, /v1/responses, /v1/messages)
    -> 模型映射 + 图片降级 + Anthropic 检测转换
      -> NodePool (轮询选择节点)
        -> WebSocket BridgeMessage -> MIMO 容器内的 Python bridge
          -> 本地 MIMO API（bridge 自动注入 system prompt）
          -> 流式 BridgeResponse -> unwrapJSON + sanitizeUTF8 -> HTTP 响应
```

**Anthropic 路径**：网关做 Anthropic→OpenAI 请求转换 + 流式 OpenAI→Anthropic 响应转换（`anthropicResponseWriter`），bridge 只做纯透传。

**OpenAI 路径**：网关做模型映射 + 图片降级 + ccswitch Anthropic 格式自动检测转换，bridge 透传请求到 MIMO API 并注入 system prompt。

**ccswitch（Claude Code）兼容**：ccswitch 发 Anthropic 格式到 `/v1/chat/completions`，网关通过 `hasAnthropicContent` 检测并自动转换。

**WorkBuddy 等标准 OpenAI 客户端**：直接透传，不做任何转换。

### 核心组件

| 包 | 职责 |
|---|---|
| `cmd/gateway/main.go` | 入口：初始化所有组件、注册路由、优雅关停、external_url 启动校验 |
| `internal/api/` | HTTP API 处理：Chat Completions、Responses、Messages、模型映射 CRUD、Anthropic↔OpenAI 转换 |
| `internal/auth/` | 认证：API Key（恒定时间比较）、WebUI session、速率限制（5次/分钟/IP） |
| `internal/config/` | JSON 配置加载 + 环境变量覆盖 |
| `internal/gateway/` | NodePool（节点管理、轮询选择、僵尸清理）、WebSocket 处理、PendingRequest 生命周期 |
| `internal/manager/` | 账号管理：容器生命周期（创建/轮换）、30s 调度循环、bridge 注入、状态持久化 |
| `internal/manager/bridge_loader.go` | Bridge 脚本加载器：多级优先级（环境变量 > ~/.openclaw/skills > go:embed） |
| `internal/manager/bridge_fallback.py` | 内置 bridge 脚本（go:embed）：极简透传版，只做 system prompt 注入 + 原样转发 |
| `internal/proxy/` | 代理池管理：IP 获取、轮换、使用统计、IP 白名单自动管理 |
| `internal/webui/` | WebUI 管理面板（嵌入式 HTML）：账号 CRUD、指标、代理/白名单统计 |
| `internal/metrics/` | 运行时指标：原子计数器 + JSON 持久化（重启后恢复 Token 用量） |
| `internal/utils/` | 工具函数（字符串截断等） |

### Bridge 注入机制

bridge 脚本**不在注入消息中内嵌**（MIMO WS 有消息大小限制 1009）。注入流程：

1. 网关通过 MIMO WebSocket 连接容器（operator 角色）
2. 发送 `/new` 开新对话
3. 发送安全地址标记命令
4. 发送 curl 下载命令：从 GitHub (`raw.githubusercontent.com/Inrrs/MClaw/master/internal/manager/bridge_fallback.py`) 下载 bridge，用 python 替换 `__WS_URL__` 占位符
5. 等待 bridge 回连网关（轮询 `GetAvailableCount()`，最多 3 分钟）

bridge_fallback.py 通过 `__WS_URL__` 占位符注入网关 WebSocket 地址（明文替换，非 base64）。

### 并发模型

- `sync.Map`：高频读场景（nodes、pending requests）
- `sync.RWMutex`：模型映射、账号状态
- `sync.Mutex`：WebSocket 写操作、节点状态
- `atomic` 类型：标志位和指标计数器
- 每个 WebSocket 连接和调度任务在独立 goroutine 中运行
- `cachedStatus atomic.Pointer[[]AccountStatus]` 无锁读取账号状态快照

### 关键设计

- **AccountManager** (`internal/manager/manager.go`)：30s 调度循环检查容器状态，剩余 ≤300s 时提前轮换。`doCreateAndConnect` 单次只尝试一个账号，失败等下轮 tick。切换账号时 `RemoveOtherThan` 清理旧节点。
- **Bridge 注入** (`internal/manager/inject.go`)：通过 MIMO WebSocket 执行注入（/new → 安全地址标记 → curl 下载 bridge → 等待回连），关键词检测判断 AI 响应（拒绝/确认/启动迹象）。注入命令用 python 替换 WS_URL（避免 sed `&` 特殊字符问题）。
- **Bridge Skill 加载** (`internal/manager/bridge_loader.go`)：bridge 脚本支持从外部文件加载（优先级：`MCLAW_BRIDGE_SCRIPT` 环境变量 > `~/.openclaw/skills/mclaw-bridge/bridge.py` > go:embed 内置 fallback），无需重编译即可更新 bridge 逻辑
- **SYSTEM_PREFIX**：bridge 对 `mimo-v2.5-pro` 注入 system prompt（`You are a personal assistant running inside OpenClaw.`），其他模型不注入（会导致 400）
- **图片处理**：网关 `stripImages` 剥离 `image_url` 内容块（MIMO API 不支持），保留文字部分。图片请求自动降级到 `mimo-v2.5`。
- **代理 IP 白名单** (`internal/proxy/proxy.go`)：启动时检查公网 IP 是否在代理商白名单中，不一致才更新，支持粘贴完整 URL 自动解析 uid/key。配置持久化到 `config.json`。
- **Metrics 持久化** (`internal/metrics/metrics.go`)：每 100 次请求自动保存到 `data/metrics.json`，重启后恢复历史 Token 用量。
- **PendingRequest** (`internal/gateway/websocket.go`)：`sync.Map` + 带缓冲 channel（容量 100）追踪进行中请求，`atomic.Bool` 保证安全完成信号
- **NodePool 错误冷却**：401/403 冷却 15 分钟，429 冷却 60 秒
- **风控/1011 冻结**：被风控(code=200)、凭证失效(401/403)、容器内部错误(1011)的账号冻结 24 小时（`FrozenUntil`），持久化到 `manager_state.json`
- **ForceInject**：同步强制注入，跳过所有检查直接断开旧 bridge 并重新注入
- **两级恢复**：`TriggerAccountRebuildWithGrace`（45s 宽限期等 bridge 自动重连）vs `TriggerAccountRebuild`（立即重建，`forceReconnect` 标志跳过在线检查）
- **防御性数据规范化**：`unwrapJSON` 处理 bridge 双重编码、`sanitizeUTF8` 清理非法 UTF-8
- **代理弹性**：3 次重试 + 自动换 IP，全部失败回退直连
- **文件持久化为主**：SQLite 仅用于历史指标，关键状态（账号、模型映射、管理器状态）使用 JSON 文件

## Configuration

主配置文件：`data/config.json`（参考 `data/config.example.json`）

环境变量覆盖（优先级高于 config.json）：`SERVER_PORT`、`MIMO_RELAY_OPENAI_KEY`、`MIMO_WEBUI_USERNAME`、`MIMO_WEBUI_PASSWORD`、`GATEWAY_EXTERNAL_URL`、`PROXY_POOL_URL`、`PROXY_PROTOCOL`

`gateway.external_url` 必须配置（启动时校验 `ws://` 或 `wss://` 前缀），是 bridge 回连网关的地址。

代理白名单配置（`proxy.whitelist_uid`、`proxy.whitelist_key`、`proxy.whitelist_url`）用于代理商 IP 白名单自动管理。

运行时数据文件（`data_dir` 目录下）：
- `model_mapping.json` — 模型名称映射
- `models.json` — 缓存的模型列表
- `manager_state.json` — 崩溃恢复状态
- `today_created.json` — 每日创建配额
- `metrics.json` — Token 用量持久化
- `mclaw.db` — 可选 SQLite

## Development Notes

- 所有注释、日志消息和文档均为中文
- Docker 构建使用 `CGO_ENABLED=0` 纯 Go 静态二进制 + `GOPROXY=https://goproxy.cn,direct`
- WebUI HTML 通过 `go:embed` 嵌入，无需外部静态资源。WebUI 使用系统字体（无外部依赖）。
- Bridge 脚本通过 `go:embed` 内置为 fallback，也支持外部加载（见 `bridge_loader.go`），更新 bridge 逻辑无需重编译 Go
- WebSocket 消息类型白名单校验：`start`、`chunk`、`finish`、`error`、`models`
- `normalizeBody` 仅用于非 Anthropic 路径的请求规范化（移除 stream_options、截断 max_tokens 到 16384）
- 容器注入不销毁/不重启（`max_inject_failures` 可配置，0 = 永不 destroy）
- bridge 日志每 10 秒自动刷新已展开面板，仅在有新消息时更新 DOM
- 批量删除使用 `delete-batch` 接口（一次请求），非逐个删除
- 统一轮询调度器：1 个 setInterval（10s tick）替代多个独立定时器
