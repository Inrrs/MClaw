# MClaw

MIMO Protocol Gateway — 将多个小米 AI Studio (MIMO) 容器实例聚合为兼容 OpenAI / Anthropic 的统一 API 网关。

## 功能

- ✅ 多账号管理（自动轮换、冷却机制、todayCreated 持久化）
- ✅ 自动容器创建与生命周期管理
- ✅ WebSocket 注入（SOUL 清理 + bridge 脚本部署）
- ✅ OpenAI Chat / Responses / Anthropic Messages 三种协议
- ✅ 代理池支持（自动轮换 IP）
- ✅ WebUI 管理面板（暗色主题、响应式、实时监控）
- ✅ 流式保活 + chunk 超时检测
- ✅ 僵尸/孤儿请求自动清理
- ✅ 模型映射（运行时可编辑）
- ✅ 图片自动降级
- ✅ 动态添加用户
- ✅ HTTP Server 优雅关闭
- ✅ 登录速率限制 + 时序攻击防护
- ✅ WebSocket 节点认证
- ✅ Context 传播（客户端断开自动清理）

## 快速开始

### 方式一：Docker Compose（推荐）

```bash
# 1. 创建配置
cp .env.example .env
# 编辑 .env 填入 API Key、WebUI 密码等

# 2. 启动
docker compose up -d

# 3. 访问 WebUI
open http://localhost:8900
```

### 方式二：本地运行

```bash
# 1. 配置
cp data/config.example.json data/config.json
# 编辑 config.json

# 2. 启动
go run cmd/gateway/main.go -config data/config.json

# 或编译后运行
go build -o mclaw cmd/gateway/main.go
./mclaw -config data/config.json -log-dir logs
```

### 方式三：环境变量

所有配置项均可通过环境变量覆盖（优先级高于 config.json）：

| 环境变量 | 说明 | 默认值 |
|----------|------|--------|
| `SERVER_PORT` | 监听端口 | `8900` |
| `MIMO_RELAY_OPENAI_KEY` | API Key | 无 |
| `MIMO_WEBUI_USERNAME` | WebUI 用户名 | `admin` |
| `MIMO_WEBUI_PASSWORD` | WebUI 密码 | 自动生成 |
| `GATEWAY_EXTERNAL_URL` | bridge 连接地址 | 无 |
| `PROXY_POOL_URL` | 代理池地址 | 无 |
| `PROXY_PROTOCOL` | 代理协议 | `http` |

## 导入账号

### WebUI 导入

访问 `http://localhost:8900`，在「账号管理」标签页点击「导入账号」，支持：
- 手动填写 userId / serviceToken / xiaomichatbot_ph
- 粘贴 curl 命令自动解析

### 手动创建

```json
// users/user_xxxx.json
{
  "userId": "1234567890",
  "serviceToken": "your_service_token",
  "xiaomichatbot_ph": "your_ph_value",
  "name": "账号名称",
  "group": "分组（可选）"
}
```

## API 使用

```bash
# Chat Completions（流式）
curl http://localhost:8900/v1/chat/completions \
  -H "Authorization: Bearer your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"stream":true}'

# 模型列表
curl http://localhost:8900/v1/models \
  -H "Authorization: Bearer your_api_key"
```

支持的模型名会自动映射到 MIMO 模型（`gpt-4o` → `mimo-v2.5-pro` 等），映射可通过 WebUI 或 API 动态修改。

## 配置说明

### config.json

```json
{
  "server": {
    "port": "8900",
    "host": "0.0.0.0"
  },
  "gateway": {
    "external_url": "ws://your-server:8900",
    "base_url": "https://aistudio.xiaomimimo.com"
  },
  "auth": {
    "api_key": "your_api_key",
    "webui_user": "admin",
    "webui_password": "your_password"
  },
  "proxy": {
    "pool_url": "",
    "protocol": "http",
    "interval": 60
  },
  "manager": {
    "switch_before_min": 5,
    "check_interval": 30
  },
  "data_dir": "data"
}
```

| 字段 | 说明 |
|------|------|
| `server.port` | 监听端口 |
| `gateway.external_url` | 外部 WebSocket 地址（bridge 连接用） |
| `gateway.base_url` | OpenClaw 平台地址（默认 `https://aistudio.xiaomimimo.com`） |
| `auth.api_key` | API 鉴权密钥（留空则无鉴权） |
| `auth.webui_password` | WebUI 密码（留空自动生成随机密码） |
| `proxy.pool_url` | 代理池地址（留空直连） |
| `data_dir` | 数据文件目录（默认 `data`） |

## 目录结构

```
MClaw/
├── cmd/gateway/main.go          # 入口 + 路由 + 启动流程
├── internal/
│   ├── api/
│   │   ├── handlers.go          # HTTP API + 模型映射 + 流式处理
│   │   └── handlers_test.go     # 单元测试
│   ├── auth/
│   │   ├── auth.go              # 鉴权 + 速率限制 + session 管理
│   │   └── static/login.html    # 登录页（go:embed）
│   ├── config/
│   │   ├── config.go            # 配置加载 + 环境变量覆盖
│   │   └── config_test.go       # 单元测试
│   ├── gateway/
│   │   ├── pool.go              # 节点池 + 请求路由 + 僵尸清理
│   │   ├── websocket.go         # WebSocket 处理 + 认证
│   │   └── pending.go           # 请求生命周期管理
│   ├── logger/logger.go         # 结构化日志（slog + 文件）
│   ├── manager/
│   │   ├── manager.go           # 账号管理 + 调度 + 容器生命周期
│   │   ├── manager_test.go      # 单元测试
│   │   ├── inject.go            # WebSocket 注入 + bridge 生成
│   │   ├── state.go             # 管理器状态持久化
│   │   ├── today.go             # 今日创建记录
│   │   └── logs.go              # 账号事件日志（环形缓冲区）
│   ├── metrics/metrics.go       # 运行时指标（原子计数器）
│   ├── persistence/sqlite.go    # SQLite 持久化（WAL 模式）
│   ├── proxy/proxy.go           # 代理池管理
│   └── webui/
│       ├── handler.go           # WebUI API + 页面渲染
│       ├── auth.go              # WebUI 路由注册
│       └── static/index.html    # 管理面板（go:embed）
├── scripts/
│   ├── bridge.py              # Bridge 脚本（独立仓库：github.com/Inrrs/MClaw-skill）
│   └── SKILL.md               # Bridge 安全审计声明
├── data/
│   └── config.example.json      # 配置示例
├── users/                       # 账号数据（运行时生成）
├── logs/                        # 日志文件（运行时生成）
├── .env.example                 # 环境变量模板
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
├── README.md
└── PROJECT_DOC.md               # 详细开发文档
```

## API 端点

### 公开端点（无需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/ws` | WebSocket bridge 连接 |
| GET | `/ping` | 健康检查 |
| GET | `/v1/models` | 模型列表 |
| GET | `/api/nodes` | 节点状态 |
| GET | `/api/models` | 可用模型 |
| GET | `/api/model_mapping` | 模型映射（只读） |

### API 鉴权端点（Bearer Token）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/chat/completions` | OpenAI Chat |
| POST | `/v1/responses` | OpenAI Responses |
| POST | `/v1/messages` | Anthropic Messages |

### 管理端点（需要 API Key）

| 方法 | 路径 | 说明 |
|------|------|------|
| PUT | `/api/model_mapping` | 更新模型映射 |
| DELETE | `/api/model_mapping` | 重置模型映射 |
| POST | `/api/rebuild_current` | 重建当前账号 |

### WebUI 端点（Session 认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 管理面板 |
| POST | `/api/auth/login` | 登录 |
| POST | `/api/auth/logout` | 登出 |
| GET | `/api/accounts` | 账号列表 |
| POST | `/api/accounts/import` | 导入账号 |
| POST | `/api/accounts/delete` | 删除账号 |
| POST | `/api/accounts/delete-batch` | 批量删除 |
| GET | `/api/metrics` | 系统指标 |
| GET | `/api/account_logs` | 账号日志 |
| GET | `/api/proxy_stats` | 代理统计 |
| POST | `/api/proxy` | 更新代理配置 |
| POST | `/api/test_account` | 测试账号连接 |

## Docker 部署

```bash
# 构建
docker build -t cinrrs/mclaw:latest .

# 运行
docker run -d \
  --name mclaw \
  -p 8900:8900 \
  -v ./data:/app/data \
  -v ./users:/app/users \
  -v ./logs:/app/logs \
  --env-file .env \
  cinrrs/mclaw:latest
```

或使用 Docker Compose：

```bash
docker compose up -d
```

## Bridge Skill 架构

Bridge 脚本（运行在 Claw 容器内的反向代理客户端）采用 **Skill 模式**，与网关主程序解耦：

```
scripts/bridge.py   ← 独立文件，可随时更新，无需重编译 Go
        ↓ (go:embed)
internal/manager/bridge_fallback.py   ← 内置 fallback，确保始终可用
        ↓
inject.go → 注入到 Claw 容器运行
```

**Bridge Skill 独立仓库**：[Inrrs/MClaw-skill](https://github.com/Inrrs/MClaw-skill)

**加载优先级**：

| 优先级 | 来源 | 场景 |
|:---:|------|------|
| 1 | 环境变量 `MCLAW_BRIDGE_SCRIPT` | 指定自定义脚本路径 |
| 2 | `scripts/bridge.py` | 项目目录下直接修改 |
| 3 | `~/.openclaw/skills/mclaw-bridge/bridge.py` | 从 MClaw-skill 仓库安装到 OpenClaw |
| 4 | 可执行文件同目录 `scripts/bridge.py` | Docker/二进制部署 |
| 5 | go:embed 内置 fallback | 无外部文件时自动回退 |

更新 bridge 逻辑只需修改 `scripts/bridge.py`，重启网关即可生效，无需重新编译。

## 安全特性

- API Key 时序攻击防护（`crypto/subtle.ConstantTimeCompare`）
- WebUI 密码自动生成（未配置时）
- 登录速率限制（每 IP 每分钟 5 次）
- Session Cookie `SameSite: Strict` + `HttpOnly`
- WebSocket 节点认证（API Key 查询参数）
- 管理写操作需 API Key 认证
- 请求体大小限制（10MB）
- 用户 ID 路径穿越防护
- WebUI XSS 转义

## 详细文档

参见 [PROJECT_DOC.md](./PROJECT_DOC.md)
