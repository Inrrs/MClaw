package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Pool 代理池配置
type Pool struct {
	URL      string `json:"url"`
	Protocol string `json:"protocol"`
	Interval int    `json:"interval"` // 刷新间隔（秒）
}

// Stats 代理使用统计
type Stats struct {
	TotalUsed int       `json:"total_used"`
	TodayUsed int       `json:"today_used"`
	TodayDate string    `json:"today_date"`
	LastIP    string    `json:"last_ip"`
	LastUsed  time.Time `json:"last_used"`
}

// Manager 代理管理器
type Manager struct {
	pool      Pool
	saveFunc  func(url string) // 持久化回调

	mu        sync.RWMutex
	proxies   []string
	current   int
	expiresAt time.Time

	// 统计
	totalUsed int
	todayUsed int
	todayDate string
	lastIP    string
	lastUsed  time.Time

	stopCh chan struct{}
}

// NewManager 创建代理管理器
func NewManager(pool Pool, saveFunc func(string)) *Manager {
	return &Manager{
		pool:     pool,
		saveFunc: saveFunc,
		stopCh:   make(chan struct{}),
	}
}

// Start 启动代理管理器
func (m *Manager) Start() {
	if m.pool.URL == "" {
		return
	}
	slog.Info("代理管理器启动", "url", m.pool.URL)
	m.refresh()

	interval := m.pool.Interval
	if interval <= 0 {
		interval = 60
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.refresh()
		case <-m.stopCh:
			return
		}
	}
}

// Stop 停止代理管理器
func (m *Manager) Stop() {
	close(m.stopCh)
}

// refresh 从代理池 API 获取新 IP
func (m *Manager) refresh() {
	resp, err := http.Get(m.pool.URL)
	if err != nil {
		slog.Error("代理刷新失败", "error", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("代理读取响应失败", "error", err)
		return
	}

	raw := strings.TrimSpace(string(body))
	if raw == "" || strings.Contains(raw, "Error") || strings.Contains(raw, "error") {
		slog.Warn("代理 API 返回异常", "body", raw)
		return
	}

	// 解析 IP:PORT 格式（每行一个或逗号分隔）
	var proxies []string
	for _, line := range strings.Split(raw, "\n") {
		for _, p := range strings.Split(line, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				proxies = append(proxies, p)
			}
		}
	}

	if len(proxies) == 0 {
		slog.Warn("代理池为空")
		return
	}

	interval := m.pool.Interval
	if interval <= 0 {
		interval = 60
	}

	m.mu.Lock()
	m.proxies = proxies
	m.current = 0
	m.expiresAt = time.Now().Add(time.Duration(interval) * time.Second)
	m.mu.Unlock()

	slog.Info("代理刷新成功", "count", len(proxies))
}

// GetProxy 获取当前代理（不消耗）
func (m *Manager) GetProxy() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.proxies) == 0 {
		return ""
	}
	return m.proxies[m.current%len(m.proxies)]
}

// RotateProxy 换下一个代理 IP
func (m *Manager) RotateProxy() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.proxies) == 0 {
		return ""
	}
	m.current = (m.current + 1) % len(m.proxies)
	return m.proxies[m.current]
}

// MarkUsed 标记代理已使用
func (m *Manager) MarkUsed(ip string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	today := time.Now().Format("2006-01-02")
	if m.todayDate != today {
		m.todayUsed = 0
		m.todayDate = today
	}

	m.totalUsed++
	m.todayUsed++
	m.lastIP = ip
	m.lastUsed = time.Now()
}

// GetProxyURL 获取代理 URL
func (m *Manager) GetProxyURL() *url.URL {
	proxy := m.GetProxy()
	if proxy == "" {
		return nil
	}

	protocol := m.pool.Protocol
	if protocol == "" {
		protocol = "http"
	}

	if !strings.Contains(proxy, "://") {
		proxy = protocol + "://" + proxy
	}

	u, err := url.Parse(proxy)
	if err != nil {
		slog.Error("解析代理 URL 失败", "proxy", proxy, "error", err)
		return nil
	}

	return u
}

// GetTransport 获取带代理的 Transport
func (m *Manager) GetTransport() *http.Transport {
	proxyURL := m.GetProxyURL()
	if proxyURL == nil {
		return &http.Transport{}
	}
	return &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}
}

// GetStats 获取统计
func (m *Manager) GetStats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	today := time.Now().Format("2006-01-02")
	todayUsed := m.todayUsed
	if m.todayDate != today {
		todayUsed = 0
	}

	return Stats{
		TotalUsed: m.totalUsed,
		TodayUsed: todayUsed,
		TodayDate: today,
		LastIP:    m.lastIP,
		LastUsed:  m.lastUsed,
	}
}

// UpdateURL 更新代理池 URL（通过回调持久化）
func (m *Manager) UpdateURL(newURL string) {
	m.mu.Lock()
	m.pool.URL = newURL
	m.mu.Unlock()
	if newURL != "" {
		m.refresh()
	}
	if m.saveFunc != nil {
		m.saveFunc(newURL)
	}
}

// GetProxyCount 获取当前代理数量
func (m *Manager) GetProxyCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.proxies)
}
