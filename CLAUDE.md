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
    -> 模型映射 + 图片降级
      -> NodePool (轮询选择节点)
        -> WebSocket BridgeMessage -> MIMO 容器内的 Python bridge
          -> 本地 MIMO API
          -> 流式 BridgeResponse -> PendingRequest channel -> HTTP 响应
```

### 核心组件

| 包 | 职责 |
|---|---|
| `cmd/gateway/main.go` | 入口：初始化所有组件、注册路由、优雅关停 |
| `internal/api/` | HTTP API 处理：Chat Completions、Responses、Messages、模型映射 CRUD |
| `internal/auth/` | 认证：API Key（恒定时间比较）、WebUI session、速率限制（5次/分钟/IP） |
| `internal/config/` | JSON 配置加载 + 环境变量覆盖 |
| `internal/gateway/` | NodePool（节点管理、轮询选择、僵尸清理）、WebSocket 处理、PendingRequest 生命周期 |
| `internal/manager/` | 账号管理：容器生命周期（创建/销毁/轮换）、30s 调度循环、bridge 注入、状态持久化 |
| `internal/persistence/` | 可选 SQLite（WAL 模式）存储历史指标、请求日志、token 统计 |
| `internal/proxy/` | 代理池管理：IP 获取、轮换、使用统计 |
| `internal/webui/` | WebUI 管理面板（嵌入式 HTML）：账号 CRUD、指标、代理统计 |

### 并发模型

- `sync.Map`：高频读场景（nodes、pending requests）
- `sync.RWMutex`：模型映射、账号状态
- `sync.Mutex`：WebSocket 写操作、节点状态
- `atomic` 类型：标志位和指标计数器
- 每个 WebSocket 连接和调度任务在独立 goroutine 中运行

### 关键设计

- **AccountManager** (`internal/manager/manager.go`)：30s 调度循环检查容器状态，剩余 ≤300s 时提前轮换
- **Bridge 注入** (`internal/manager/inject.go`)：最复杂的组件，通过 MIMO WebSocket 执行多步注入（reset → 检查/清除 soul → 注入 Python bridge），依赖关键词检测判断 AI 响应
- **PendingRequest** (`internal/gateway/websocket.go`)：`sync.Map` + 带缓冲 channel（容量 100）追踪进行中请求，`atomic.Bool` 保证安全完成信号
- **NodePool 错误冷却**：401/403 冷却 15 分钟，429 冷却 60 秒
- **风控/1011 冻结**：被风控或容器内部错误(1011)的账号冻结 24 小时
- **文件持久化为主**：SQLite 仅用于历史指标，关键状态（账号、模型映射、管理器状态）使用 JSON 文件

## Configuration

主配置文件：`data/config.json`（参考 `data/config.example.json`）

环境变量覆盖（优先级高于 config.json）：`SERVER_PORT`、`MIMO_RELAY_OPENAI_KEY`、`MIMO_WEBUI_USERNAME`、`MIMO_WEBUI_PASSWORD`、`GATEWAY_EXTERNAL_URL`、`PROXY_POOL_URL`、`PROXY_PROTOCOL`

运行时数据文件（`data_dir` 目录下）：
- `model_mapping.json` — 模型名称映射
- `models.json` — 缓存的模型列表
- `manager_state.json` — 崩溃恢复状态
- `today_created.json` — 每日创建配额
- `mclaw.db` — 可选 SQLite

## Development Notes

- 所有注释、日志消息和文档均为中文
- Docker 构建使用 `CGO_ENABLED=0` 纯 Go 静态二进制 + `GOPROXY=https://goproxy.cn,direct`
- WebUI HTML 通过 `go:embed` 嵌入，无需外部静态资源
- 测试文件位于 `internal/api/`、`internal/config/`、`internal/manager/`
- 详细开发文档见 `PROJECT_DOC.md`（1267 行，涵盖 19 个章节）
