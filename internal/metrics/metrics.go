package metrics

import (
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
	if p, ok := m.prices.Load().(*PriceConfig); ok && p != nil {
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
