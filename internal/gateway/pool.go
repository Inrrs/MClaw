package gateway

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"mclaw/internal/metrics"
)

// Config 网关配置
type Config struct {
	StreamKeepaliveSec    int
	StreamChunkTimeoutSec int
	StaleQueueTTLSec      int
	Node401CooldownSec    int
}

var DefaultConfig = Config{
	StreamKeepaliveSec:    60,
	StreamChunkTimeoutSec: 600,
	StaleQueueTTLSec:      600,
	Node401CooldownSec:    900,
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type WebSocketConn struct {
	*websocket.Conn
	mu sync.Mutex
}

func (c *WebSocketConn) WriteJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteJSON(v)
}

type BridgeMessage struct {
	ReqID  string          `json:"req_id"`
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Body   json.RawMessage `json:"body"`
}

type BridgeResponse struct {
	ReqID  string          `json:"req_id"`
	Type   string          `json:"type"` // start, chunk, finish, error
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// PendingRequest 待处理请求
type PendingRequest struct {
	ReqID    string
	Response chan BridgeResponse
	Created  time.Time
	NodeID   string
	Done     atomic.Bool
}

// MarkDone 标记完成并关闭通道
func (p *PendingRequest) MarkDone() {
	if p.Done.CompareAndSwap(false, true) {
		close(p.Response)
	}
}

// IsExpired 检查是否超时
func (p *PendingRequest) IsExpired(ttl time.Duration) bool {
	return time.Since(p.Created) > ttl
}

// RequestError 请求级别错误日志
type RequestError struct {
	Time    time.Time `json:"time"`
	NodeID  string    `json:"node_id"`
	Status  int       `json:"status"`
	Path    string    `json:"path"`
	Message string    `json:"message"`
}

// RequestErrorStore 请求错误日志存储
type RequestErrorStore struct {
	mu     sync.Mutex
	errors []RequestError
	maxLen int
}

func NewRequestErrorStore(maxLen int) *RequestErrorStore {
	return &RequestErrorStore{maxLen: maxLen}
}

func (s *RequestErrorStore) Add(err RequestError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err.Time = time.Now()
	s.errors = append(s.errors, err)
	if len(s.errors) > s.maxLen {
		s.errors = s.errors[len(s.errors)-s.maxLen:]
	}
}

func (s *RequestErrorStore) GetAll(limit int) []RequestError {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]RequestError, len(s.errors))
	copy(result, s.errors)
	// 倒序（最新的在前）
	for i := 0; i < len(result)/2; i++ {
		j := len(result) - 1 - i
		result[i], result[j] = result[j], result[i]
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

// NodePool 节点池
type NodePool struct {
	nodes           sync.Map
	mu              sync.Mutex
	idx             int
	stopCh          chan struct{}
	config          Config
	onNodeDown      func(nodeID string)
	cachedModels    []string   // 缓存的模型列表
	modelsFile      string     // 模型列表持久化文件
	pendingRequests sync.Map   // 待处理请求存储
	ErrorStore      *RequestErrorStore // 请求错误日志
	availableMu     sync.RWMutex
	availableCache  []*Node    // 缓存的可用节点列表
	availableDirty  bool       // 缓存是否需要刷新
}

func NewNodePool() *NodePool {
	p := &NodePool{
		stopCh:     make(chan struct{}),
		config:     DefaultConfig,
		modelsFile: "data/models.json",
		ErrorStore: NewRequestErrorStore(200),
	}
	p.loadCachedModels()
	return p
}

func NewNodePoolWithConfig(cfg Config, modelsFile string) *NodePool {
	if modelsFile == "" {
		modelsFile = "data/models.json"
	}
	p := &NodePool{
		stopCh:     make(chan struct{}),
		config:     cfg,
		modelsFile: modelsFile,
		ErrorStore: NewRequestErrorStore(200),
	}
	p.loadCachedModels()
	return p
}

// loadCachedModels 从文件加载缓存的模型列表
func (p *NodePool) loadCachedModels() {
	data, err := os.ReadFile(p.modelsFile)
	if err != nil {
		return
	}
	var models []string
	if err := json.Unmarshal(data, &models); err == nil {
		p.cachedModels = models
		slog.Info("加载缓存模型列表", "count", len(models))
	}
}

// saveCachedModels 保存模型列表到文件
func (p *NodePool) saveCachedModels(models []string) {
	data, err := json.MarshalIndent(models, "", "  ")
	if err != nil {
		return
	}
	os.MkdirAll(filepath.Dir(p.modelsFile), 0755)
	os.WriteFile(p.modelsFile, data, 0644)
	p.cachedModels = models
}

func (p *NodePool) SetOnNodeDown(fn func(nodeID string)) {
	p.onNodeDown = fn
}

func (p *NodePool) Add(node *Node) {
	p.nodes.Store(node.ID, node)
	slog.Info("节点上线", "id", node.ID, "account", node.AccountID)
	// 更新节点指标
	total, available := p.countNodes()
	metrics.Get().UpdateNodes(total, available)
	// 标记缓存需要刷新
	p.availableMu.Lock()
	p.availableDirty = true
	p.availableMu.Unlock()
}

func (p *NodePool) Remove(id string) {
	if _, loaded := p.nodes.LoadAndDelete(id); loaded {
		slog.Info("节点下线", "id", id)
		// 孤儿请求清理
		p.CleanupOrphans(id)
		// 更新节点指标
		total, available := p.countNodes()
		metrics.Get().UpdateNodes(total, available)
		// 标记缓存需要刷新
		p.availableMu.Lock()
		p.availableDirty = true
		p.availableMu.Unlock()
		if p.onNodeDown != nil {
			p.onNodeDown(id)
		}
	}
}

// countNodes 统计节点数量
func (p *NodePool) countNodes() (total int, available int) {
	p.nodes.Range(func(_, v any) bool {
		total++
		node := v.(*Node)
		if node.IsAvailable() {
			available++
		}
		return true
	})
	return
}

func (p *NodePool) GetAvailable() *Node {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查缓存是否需要刷新
	p.availableMu.RLock()
	dirty := p.availableDirty
	p.availableMu.RUnlock()

	if dirty {
		var available []*Node
		p.nodes.Range(func(_, v any) bool {
			node := v.(*Node)
			if node.IsAvailable() {
				available = append(available, node)
			}
			return true
		})
		p.availableMu.Lock()
		p.availableCache = available
		p.availableDirty = false
		p.availableMu.Unlock()
	}

	p.availableMu.RLock()
	available := p.availableCache
	p.availableMu.RUnlock()

	if len(available) == 0 {
		return nil
	}

	node := available[p.idx%len(available)]
	p.idx++
	node.mu.Lock()
	node.LastUsed = time.Now()
	node.mu.Unlock()
	return node
}

func (p *NodePool) GetAvailableCount() int {
	count := 0
	p.nodes.Range(func(_, v any) bool {
		node := v.(*Node)
		if node.IsAvailable() {
			count++
		}
		return true
	})
	return count
}

func (p *NodePool) GetModels() []string {
	// 先从节点收集
	modelSet := make(map[string]bool)
	p.nodes.Range(func(_, v any) bool {
		node := v.(*Node)
		node.mu.Lock()
		for _, m := range node.Models {
			modelSet[m] = true
		}
		node.mu.Unlock()
		return true
	})

	if len(modelSet) > 0 {
		models := make([]string, 0, len(modelSet))
		for m := range modelSet {
			models = append(models, m)
		}
		return models
	}

	// 节点没有模型，返回缓存
	return p.cachedModels
}

// UpdateModels 更新模型列表（从 bridge 同步）
func (p *NodePool) UpdateModels(nodeID string, models []string) {
	// 查找节点并更新
	if v, ok := p.nodes.Load(nodeID); ok {
		node := v.(*Node)
		node.mu.Lock()
		node.Models = models
		node.mu.Unlock()
	}

	// 与缓存比较，如果有变化则保存
	if !modelsEqual(p.cachedModels, models) {
		p.saveCachedModels(models)
		slog.Info("模型列表已更新", "count", len(models), "node", nodeID)
	}
}

func modelsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool)
	for _, m := range a {
		set[m] = true
	}
	for _, m := range b {
		if !set[m] {
			return false
		}
	}
	return true
}

func (p *NodePool) Stop() {
	close(p.stopCh)
}

// SweepStale 清理僵尸请求
func (p *NodePool) SweepStale() {
	ttl := time.Duration(p.config.StaleQueueTTLSec) * time.Second
	cleaned := 0

	p.pendingRequests.Range(func(key, value any) bool {
		req := value.(*PendingRequest)
		if req.IsExpired(ttl) {
			req.MarkDone()
			p.pendingRequests.Delete(key)
			cleaned++
		}
		return true
	})

	if cleaned > 0 {
		slog.Info("清理僵尸请求", "count", cleaned)
	}
}

// CleanupOrphans 清理孤儿请求（节点断开时）
func (p *NodePool) CleanupOrphans(nodeID string) {
	cleaned := 0
	p.pendingRequests.Range(func(key, value any) bool {
		req := value.(*PendingRequest)
		if req.NodeID == nodeID {
			req.MarkDone()
			p.pendingRequests.Delete(key)
			cleaned++
		}
		return true
	})

	if cleaned > 0 {
		slog.Info("清理孤儿请求", "node", nodeID, "count", cleaned)
	}
}

// StartStaleSweeper 启动僵尸请求清理器
func (p *NodePool) StartStaleSweeper() {
	interval := time.Duration(p.config.StaleQueueTTLSec/2) * time.Second
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.SweepStale()
			case <-p.stopCh:
				return
			}
		}
	}()
}

// GetNodeCount 返回节点总数
func (p *NodePool) GetNodeCount() int {
	count := 0
	p.nodes.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// GetConfig 获取配置
func (p *NodePool) GetConfig() Config {
	return p.config
}

// pendingRequests 已移入 NodePool 结构体

func (p *NodePool) SendToNode(node *Node, reqID, method, path string, body any) (*PendingRequest, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	pending := &PendingRequest{
		ReqID:    reqID,
		Response: make(chan BridgeResponse, 100),
		Created:  time.Now(),
		NodeID:   node.ID,
	}
	p.pendingRequests.Store(reqID, pending)

	msg := BridgeMessage{
		ReqID:  reqID,
		Method: method,
		Path:   path,
		Body:   bodyBytes,
	}

	if err := node.Conn.WriteJSON(msg); err != nil {
		p.pendingRequests.Delete(reqID)
		return nil, err
	}

	return pending, nil
}

// HandleRequestError 处理请求错误，触发冷却
func (p *NodePool) HandleRequestError(node *Node, statusCode int) {
	switch statusCode {
	case 401, 403:
		node.SetCooldown(time.Duration(p.config.Node401CooldownSec) * time.Second)
		slog.Warn("凭证错误，冷却节点", "node", node.ID, "cooldown", p.config.Node401CooldownSec)
	case 429:
		node.SetCooldown(60 * time.Second)
		slog.Warn("限流，冷却节点", "node", node.ID)
	default:
		node.RecordError(nil)
	}
}

// GenerateID 生成唯一请求 ID
func GenerateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d-fallback", time.Now().UnixNano())
	}
	return fmt.Sprintf("%d-%x", time.Now().UnixNano(), b)
}
