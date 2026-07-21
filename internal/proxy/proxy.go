package proxy

import (
	"encoding/json"
	"fmt"
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

	// IP 白名单配置（代理商要求添加出口 IP 白名单才能使用代理）
	WhitelistUID  string `json:"whitelist_uid"`  // 代理商白名单 UID
	WhitelistKey  string `json:"whitelist_key"`  // 代理商白名单 Key
	WhitelistURL  string `json:"whitelist_url"`  // 白名单 API 基础 URL（默认 http://op.xiequ.cn）
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

	// 启动时先确保白名单包含当前 IP
	if err := m.EnsureWhitelist(); err != nil {
		slog.Warn("白名单初始化失败（继续启动）", "error", err)
	}

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
			// 每次刷新前检查白名单
			if err := m.EnsureWhitelist(); err != nil {
				slog.Warn("白名单检查失败", "error", err)
			}
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

	slog.Debug("代理刷新成功", "count", len(proxies))
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

// EnsureAvailable 确保当前代理可用，不可用则轮换，直到找到可用 IP 或代理池耗尽
func (m *Manager) EnsureAvailable() string {
	count := m.GetProxyCount()
	if count == 0 {
		return ""
	}
	// 第一轮：尝试当前所有 IP
	for i := 0; i < count; i++ {
		proxy := m.GetProxy()
		if m.testProxy(proxy) {
			return proxy
		}
		slog.Debug("代理不可用，切换下一个", "proxy", proxy)
		m.RotateProxy()
	}
	// 全部不可用，刷新代理池获取新 IP
	slog.Warn("当前代理池全部不可用，尝试刷新")
	m.refresh()
	newCount := m.GetProxyCount()
	if newCount == 0 {
		return ""
	}
	// 第二轮：尝试新 IP
	for i := 0; i < newCount; i++ {
		proxy := m.GetProxy()
		if m.testProxy(proxy) {
			return proxy
		}
		slog.Debug("新代理不可用，切换下一个", "proxy", proxy)
		m.RotateProxy()
	}
	// 刷新后仍全部不可用，代理池额度可能用完了
	slog.Warn("代理池额度可能已耗尽，所有代理均不可用")
	return ""
}

// testProxy 测试代理 IP 是否可用（GET http://httpbin.org/ip，10 秒超时）
func (m *Manager) testProxy(proxy string) bool {
	if proxy == "" {
		return false
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
		return false
	}
	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(u)},
		Timeout:   10 * time.Second,
	}
	resp, err := client.Get("http://httpbin.org/ip")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
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

// ─────────────────── IP 白名单管理 ───────────────────

// WhitelistInfo 白名单条目
type WhitelistInfo struct {
	IP   string `json:"ip"`
	Meno string `json:"meno"`
}

// GetPublicIP 获取当前服务器公网 IP
func GetPublicIP() (string, error) {
	for _, url := range []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	} {
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		ip := strings.TrimSpace(string(body))
		if ip != "" && !strings.Contains(ip, "<") {
			return ip, nil
		}
	}
	return "", fmt.Errorf("无法获取公网 IP")
}

// whitelistBaseURL 获取白名单 API 基础 URL
func (m *Manager) whitelistBaseURL() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.pool.WhitelistURL != "" {
		return m.pool.WhitelistURL
	}
	return "http://op.xiequ.cn"
}

// whitelistParams 获取白名单认证参数
func (m *Manager) whitelistParams() (uid, key string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pool.WhitelistUID, m.pool.WhitelistKey
}

// IsWhitelistConfigured 检查白名单是否已配置
func (m *Manager) IsWhitelistConfigured() bool {
	uid, key := m.whitelistParams()
	return uid != "" && key != ""
}

// GetWhitelist 获取当前白名单列表
func (m *Manager) GetWhitelist() ([]WhitelistInfo, error) {
	uid, key := m.whitelistParams()
	if uid == "" || key == "" {
		return nil, fmt.Errorf("白名单未配置")
	}
	base := m.whitelistBaseURL()
	url := fmt.Sprintf("%s/IpWhiteList.aspx?uid=%s&ukey=%s&act=getjson", base, uid, key)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("获取白名单失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	raw := strings.TrimSpace(string(body))
	if raw == "" {
		return []WhitelistInfo{}, nil
	}
	// 尝试 JSON 解析
	var items []WhitelistInfo
	if err := json.Unmarshal(body, &items); err == nil {
		return items, nil
	}
	// fallback: 按行解析 text 格式
	var result []WhitelistInfo
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.Contains(line, "error") && !strings.Contains(line, "Error") {
			result = append(result, WhitelistInfo{IP: line})
		}
	}
	return result, nil
}

// AddToWhitelist 添加 IP 到白名单
func (m *Manager) AddToWhitelist(ip, meno string) error {
	uid, key := m.whitelistParams()
	if uid == "" || key == "" {
		return fmt.Errorf("白名单未配置")
	}
	base := m.whitelistBaseURL()
	url := fmt.Sprintf("%s/IpWhiteList.aspx?uid=%s&ukey=%s&act=add&ip=%s&meno=%s", base, uid, key, url.QueryEscape(ip), url.QueryEscape(meno))
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("添加白名单失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	raw := strings.TrimSpace(string(body))
	if strings.Contains(raw, "error") || strings.Contains(raw, "Error") {
		return fmt.Errorf("添加白名单返回错误: %s", raw)
	}
	slog.Info("IP 白名单已添加", "ip", ip, "meno", meno, "response", raw)
	return nil
}

// DeleteFromWhitelist 从白名单删除 IP
func (m *Manager) DeleteFromWhitelist(ip string) error {
	uid, key := m.whitelistParams()
	if uid == "" || key == "" {
		return fmt.Errorf("白名单未配置")
	}
	base := m.whitelistBaseURL()
	var reqURL string
	if ip == "all" {
		reqURL = fmt.Sprintf("%s/IpWhiteList.aspx?uid=%s&ukey=%s&act=del&ip=all", base, uid, key)
	} else {
		reqURL = fmt.Sprintf("%s/IpWhiteList.aspx?uid=%s&ukey=%s&act=del&ip=%s", base, uid, key, url.QueryEscape(ip))
	}
	resp, err := http.Get(reqURL)
	if err != nil {
		return fmt.Errorf("删除白名单失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	slog.Info("IP 白名单已删除", "ip", ip, "response", strings.TrimSpace(string(body)))
	return nil
}

// EnsureWhitelist 确保当前公网 IP 在白名单中（不一致才更新）
func (m *Manager) EnsureWhitelist() error {
	if !m.IsWhitelistConfigured() {
		return nil // 未配置白名单，跳过
	}

	publicIP, err := GetPublicIP()
	if err != nil {
		return fmt.Errorf("获取公网 IP 失败: %w", err)
	}

	whitelist, err := m.GetWhitelist()
	if err != nil {
		slog.Warn("获取白名单失败，尝试直接添加", "error", err)
		return m.AddToWhitelist(publicIP, "mclaw-auto")
	}

	// 检查当前 IP 是否已在白名单中
	for _, item := range whitelist {
		if item.IP == publicIP {
			slog.Debug("公网 IP 已在白名单中", "ip", publicIP)
			return nil
		}
	}

	// 不一致：删除旧的，添加新的
	if len(whitelist) > 0 {
		slog.Info("白名单 IP 不一致，更新中", "public_ip", publicIP, "old_whitelist", len(whitelist))
		for _, item := range whitelist {
			m.DeleteFromWhitelist(item.IP)
		}
	} else {
		slog.Info("白名单为空，添加当前 IP", "ip", publicIP)
	}

	return m.AddToWhitelist(publicIP, "mclaw-auto")
}

// UpdateWhitelistConfig 更新白名单配置（WebUI 调用）
func (m *Manager) UpdateWhitelistConfig(uid, key, whitelistURL string) {
	m.mu.Lock()
	m.pool.WhitelistUID = uid
	m.pool.WhitelistKey = key
	if whitelistURL != "" {
		m.pool.WhitelistURL = whitelistURL
	}
	m.mu.Unlock()
	slog.Info("白名单配置已更新", "uid", uid, "url", m.pool.WhitelistURL)
}

// ParseWhitelistURL 从完整 URL 解析 uid 和 key
// 支持格式: http://op.xiequ.cn/IpWhiteList.aspx?uid=148379&ukey=47941D7F917D0E229BB53C1B3ECC6F28&act=get
func ParseWhitelistURL(rawURL string) (uid, key, baseURL string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", fmt.Errorf("URL 解析失败: %w", err)
	}
	uid = u.Query().Get("uid")
	ukey := u.Query().Get("ukey")
	if uid == "" || ukey == "" {
		return "", "", "", fmt.Errorf("URL 中缺少 uid 或 ukey 参数")
	}
	// 提取基础 URL（去掉路径和参数）
	baseURL = u.Scheme + "://" + u.Host
	return uid, ukey, baseURL, nil
}

// GetWhitelistStats 获取白名单状态（供 WebUI 展示）
func (m *Manager) GetWhitelistStats() map[string]any {
	result := map[string]any{
		"configured": m.IsWhitelistConfigured(),
	}
	if !m.IsWhitelistConfigured() {
		return result
	}
	publicIP, err := GetPublicIP()
	if err == nil {
		result["public_ip"] = publicIP
	}
	whitelist, err := m.GetWhitelist()
	if err == nil {
		result["whitelist"] = whitelist
		result["count"] = len(whitelist)
		// 检查当前 IP 是否在白名单中
		if publicIP != "" {
			matched := false
			for _, item := range whitelist {
				if item.IP == publicIP {
					matched = true
					break
				}
			}
			result["ip_in_whitelist"] = matched
		}
	}
	return result
}
