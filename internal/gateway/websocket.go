package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Node struct {
	ID            string
	Conn          *WebSocketConn
	Models        []string
	AccountID     string
	LastUsed      time.Time
	LastError     error
	ErrorCount    int
	CooldownUntil time.Time
	mu            sync.Mutex
}

func (n *Node) IsAvailable() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Now().After(n.CooldownUntil)
}

func (n *Node) SetCooldown(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CooldownUntil = time.Now().Add(d)
	slog.Warn("节点冷却", "id", n.ID, "until", n.CooldownUntil)
}

func (n *Node) RecordError(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastError = err
	n.ErrorCount++
}

func (n *Node) ResetErrors() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastError = nil
	n.ErrorCount = 0
}

func (n *Node) GetCooldownRemaining() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	remaining := time.Until(n.CooldownUntil)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func HandleWebSocket(pool *NodePool, wsToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Token 认证（如果配置了 wsToken）
		if wsToken != "" {
			token := r.URL.Query().Get("token")
			if token != wsToken {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("WebSocket 升级失败", "error", err)
			return
		}

		nodeID := r.URL.Query().Get("account")
		if nodeID == "" {
			nodeID = GenerateID()
		}

		wsConn := &WebSocketConn{Conn: conn}
		node := &Node{
			ID:        nodeID,
			Conn:      wsConn,
			AccountID: nodeID,
		}

		pool.Add(node)
		defer func() {
			pool.Remove(nodeID)
			conn.Close()
		}()

		conn.SetPongHandler(func(string) error {
			node.ResetErrors()
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		go pingLoop(pool, node)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
					slog.Error("WebSocket 错误", "node", nodeID, "error", err)
				}
				break
			}

			handleBridgeMessage(pool, node, message)
		}
	}
}

func pingLoop(pool *NodePool, node *Node) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, exists := pool.nodes.Load(node.ID); !exists {
				return
			}

			node.Conn.mu.Lock()
			err := node.Conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(10*time.Second),
			)
			node.Conn.mu.Unlock()

			if err != nil {
				slog.Warn("Ping 失败", "node", node.ID, "error", err)
				node.RecordError(err)
				return
			}
		}
	}
}

// validMessageTypes 白名单
var validMessageTypes = map[string]bool{
	"start": true, "chunk": true, "finish": true, "error": true, "models": true,
}

func handleBridgeMessage(pool *NodePool, node *Node, data []byte) {
	var msg BridgeResponse
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Error("解析消息失败", "error", err, "node", node.ID)
		return
	}

	// models 同步消息特殊处理
	if msg.ReqID == "__models__" {
		var models []string
		if err := json.Unmarshal(msg.Body, &models); err == nil {
			pool.UpdateModels(node.ID, models)
		}
		return
	}

	// 校验 type 字段
	if !validMessageTypes[msg.Type] {
		slog.Warn("未知消息类型", "type", msg.Type, "reqID", msg.ReqID, "node", node.ID)
		return
	}

	if pending, ok := pool.pendingRequests.Load(msg.ReqID); ok {
		pendingReq := pending.(*PendingRequest)

		if msg.Type == "error" {
			statusCode := msg.Status
			if statusCode == 0 {
				statusCode = 500
			}
			pool.HandleRequestError(node, statusCode)
		}

		if pendingReq.Done.Load() {
			return
		}

		select {
		case pendingReq.Response <- msg:
		default:
			slog.Warn("响应通道已满", "reqID", msg.ReqID)
		}
	}
}
