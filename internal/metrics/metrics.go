package metrics

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// TokenUsage 单次请求的 token 用量
type TokenUsage struct {
	InputTokens  int
	OutputTokens int
	CacheTokens  int
}

type Metrics struct {
	// 请求统计
	TotalRequests   atomic.Int64
	SuccessRequests atomic.Int64
	FailedRequests  atomic.Int64

	// Token 统计（分类）
	TotalTokens  atomic.Int64
	InputTokens  atomic.Int64
	OutputTokens atomic.Int64
	CacheTokens  atomic.Int64

	// 账号统计
	TotalAccounts  atomic.Int64
	OnlineAccounts atomic.Int64

	// 节点统计
	TotalNodes  atomic.Int64
	ActiveNodes atomic.Int64

	// 延迟统计
	RequestDuration atomic.Int64 // 毫秒

	// 启动时间
	StartTime time.Time

	// 价格配置
	prices atomic.Pointer[PriceConfig]

	// 持久化
	savePath string
	saveMu   sync.Mutex
}

var global *Metrics
var once sync.Once

func Get() *Metrics {
	once.Do(func() {
		global = &Metrics{
			StartTime: time.Now(),
		}
	})
	return global
}

// SetSavePath 设置持久化文件路径，并从文件恢复历史数据
func SetSavePath(path string) {
	m := Get()
	m.savePath = path
	m.load()
}

func (m *Metrics) RecordRequest(success bool, usage TokenUsage, duration time.Duration) {
	m.TotalRequests.Add(1)
	if success {
		m.SuccessRequests.Add(1)
	} else {
		m.FailedRequests.Add(1)
	}
	m.InputTokens.Add(int64(usage.InputTokens))
	m.OutputTokens.Add(int64(usage.OutputTokens))
	m.CacheTokens.Add(int64(usage.CacheTokens))
	m.TotalTokens.Add(int64(usage.InputTokens + usage.OutputTokens + usage.CacheTokens))
	m.RequestDuration.Add(int64(duration.Milliseconds()))

	// 每 100 次请求自动保存一次
	if m.TotalRequests.Load()%100 == 0 {
		m.save()
	}
}

func (m *Metrics) UpdateAccounts(total, online int) {
	m.TotalAccounts.Store(int64(total))
	m.OnlineAccounts.Store(int64(online))
}

func (m *Metrics) UpdateNodes(total, active int) {
	m.TotalNodes.Store(int64(total))
	m.ActiveNodes.Store(int64(active))
}

func (m *Metrics) Uptime() time.Duration {
	return time.Since(m.StartTime)
}

// metricsSnapshot JSON 持久化快照
type metricsSnapshot struct {
	TotalRequests   int64 `json:"total_requests"`
	SuccessRequests int64 `json:"success_requests"`
	FailedRequests  int64 `json:"failed_requests"`
	TotalTokens     int64 `json:"total_tokens"`
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	CacheTokens     int64 `json:"cache_tokens"`
	TotalDuration   int64 `json:"total_duration_ms"`
	SavedAt         string `json:"saved_at"`
}

// save 将当前计数器持久化到 JSON 文件（不重置计数器，支持跨重启累加）
func (m *Metrics) save() {
	if m.savePath == "" {
		return
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	snap := metricsSnapshot{
		TotalRequests:   m.TotalRequests.Load(),
		SuccessRequests: m.SuccessRequests.Load(),
		FailedRequests:  m.FailedRequests.Load(),
		TotalTokens:     m.TotalTokens.Load(),
		InputTokens:     m.InputTokens.Load(),
		OutputTokens:    m.OutputTokens.Load(),
		CacheTokens:     m.CacheTokens.Load(),
		TotalDuration:   m.RequestDuration.Load(),
		SavedAt:         time.Now().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		slog.Error("序列化 metrics 失败", "error", err)
		return
	}
	os.MkdirAll(filepath.Dir(m.savePath), 0755)
	if err := os.WriteFile(m.savePath, data, 0644); err != nil {
		slog.Error("保存 metrics 失败", "error", err)
	}
}

// load 从 JSON 文件恢复历史计数器（累加到当前值）
func (m *Metrics) load() {
	if m.savePath == "" {
		return
	}
	data, err := os.ReadFile(m.savePath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("读取 metrics 失败", "error", err)
		}
		return
	}
	var snap metricsSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		slog.Warn("解析 metrics 失败", "error", err)
		return
	}
	// 累加历史值（原子操作，无需锁）
	m.TotalRequests.Add(snap.TotalRequests)
	m.SuccessRequests.Add(snap.SuccessRequests)
	m.FailedRequests.Add(snap.FailedRequests)
	m.TotalTokens.Add(snap.TotalTokens)
	m.InputTokens.Add(snap.InputTokens)
	m.OutputTokens.Add(snap.OutputTokens)
	m.CacheTokens.Add(snap.CacheTokens)
	m.RequestDuration.Add(snap.TotalDuration)
	slog.Info("恢复历史 metrics",
		"total_requests", snap.TotalRequests,
		"input_tokens", snap.InputTokens,
		"output_tokens", snap.OutputTokens,
		"saved_at", snap.SavedAt)
}

// Save 手动触发持久化（关闭时调用）
func (m *Metrics) Save() {
	m.save()
}

// PriceConfig 价格配置（元/百万 tokens）
type PriceConfig struct {
	InputPerM  float64 `json:"input_per_m"`  // 未命中缓存输入价格
	OutputPerM float64 `json:"output_per_m"` // 输出价格
	CachePerM  float64 `json:"cache_per_m"`  // 缓存命中价格
}

// DefaultPrices MIMO 官方定价（2026.5.27 降价后）
// 参考: https://platform.xiaomimimo.com/docs/price/pay-as-you-go
var DefaultPrices = PriceConfig{
	InputPerM:  2.0,   // mimo-v2.5-pro ¥3.00, mimo-v2.5 ¥1.00，取加权平均
	OutputPerM: 4.0,   // mimo-v2.5-pro ¥6.00, mimo-v2.5 ¥2.00
	CachePerM:  0.025, // mimo-v2.5-pro ¥0.025, mimo-v2.5 ¥0.02
}

func (m *Metrics) SetPrices(p PriceConfig) {
	m.prices.Store(&p)
}

func (m *Metrics) getPrices() PriceConfig {
	if p := m.prices.Load(); p != nil {
		return *p
	}
	return DefaultPrices
}

func (m *Metrics) EstimatedCost() float64 {
	p := m.getPrices()
	inputPrice := p.InputPerM / 1e6
	outputPrice := p.OutputPerM / 1e6
	cachePrice := p.CachePerM / 1e6

	input := float64(m.InputTokens.Load())
	output := float64(m.OutputTokens.Load())
	cache := float64(m.CacheTokens.Load())

	// 未命中缓存的输入 = 总输入 - 缓存命中
	uncachedInput := input - cache
	if uncachedInput < 0 {
		uncachedInput = 0
	}

	return uncachedInput*inputPrice + output*outputPrice + cache*cachePrice
}

// ToMap 导出为 map（用于 JSON 序列化）
func (m *Metrics) ToMap() map[string]any {
	uptime := m.Uptime()
	totalReqs := m.TotalRequests.Load()
	successReqs := m.SuccessRequests.Load()

	var successRate float64
	if totalReqs > 0 {
		successRate = float64(successReqs) / float64(totalReqs) * 100
	}

	var avgDuration int64
	if totalReqs > 0 {
		avgDuration = m.RequestDuration.Load() / totalReqs
	}

	return map[string]any{
		"uptime_seconds":    int(uptime.Seconds()),
		"total_requests":    totalReqs,
		"success_requests":  successReqs,
		"failed_requests":   m.FailedRequests.Load(),
		"success_rate":      successRate,
		"total_tokens":      m.TotalTokens.Load(),
		"input_tokens":      m.InputTokens.Load(),
		"output_tokens":     m.OutputTokens.Load(),
		"cache_tokens":      m.CacheTokens.Load(),
		"estimated_cost":    m.EstimatedCost(),
		"avg_duration_ms":   avgDuration,
		"total_accounts":    m.TotalAccounts.Load(),
		"online_accounts":   m.OnlineAccounts.Load(),
		"total_nodes":       m.TotalNodes.Load(),
		"active_nodes":      m.ActiveNodes.Load(),
	}
}
