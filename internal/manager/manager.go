package manager

import (
	"encoding/json"
	"fmt"
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mclaw/internal/gateway"
	"mclaw/internal/metrics"
	"mclaw/internal/utils"
	"mclaw/internal/proxy"
)

const defaultBaseURL = "https://aistudio.xiaomimimo.com"

// tomorrowMidnight 返回明天零点的时间（冻结到第二天）
func tomorrowMidnight() time.Time {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, now.Location())
}

// Account 账号信息
type Account struct {
	UserID          string `json:"userId"`
	ServiceToken    string `json:"serviceToken"`
	XiaomiChatbotPH string `json:"xiaomichatbot_ph"`
	Name            string `json:"name,omitempty"`
	Group           string `json:"group,omitempty"`
	Region          string `json:"region,omitempty"`
	ImportedAt      string `json:"imported_at,omitempty"`
}

// AccountStatus 账号状态
type AccountStatus struct {
	Account      Account
	Status       string
	ExpireTime   time.Time
	RemainSec    int
	IsCurrent    bool
	LastFailTime time.Time // 上次失败时间（用于 4 小时冷却）
	FrozenUntil  time.Time // 冻结截止时间（到第二天零点）
	TimerStopCh  chan struct{}
}

// AccountManager 管理所有账号
type AccountManager struct {
	pool       *gateway.NodePool
	proxyMgr   *proxy.Manager
	gatewayURL string
	baseURL    string
	apiKey     string
	bridgeURL  string // bridge 代码 HTTP 下载地址
	accounts   []*Account
	statuses   map[string]*AccountStatus
	mu         sync.RWMutex
	stopCh     chan struct{}
	addUserCh  chan *Account
	httpCli    *http.Client
	creating   int32
	injecting  int32 // 注入中标志，防止 kill 旧 bridge 触发并发重建
	forceReconnect int32 // 强制重连标志，跳过 bridge 在线检查

	// cachedStatus 缓存 GetStatus 快照，实现无锁读取
	// 避免 GetStatus() 的 RLock 被写锁阻塞导致 API 超时
	cachedStatus atomic.Pointer[[]AccountStatus]
}

func NewAccountManager(pool *gateway.NodePool, proxyMgr *proxy.Manager, gatewayURL, baseURL, apiKey, bridgeURL string) *AccountManager {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &AccountManager{
		pool:       pool,
		proxyMgr:   proxyMgr,
		gatewayURL: gatewayURL,
		bridgeURL:  bridgeURL,
		baseURL:    baseURL,
		apiKey:     apiKey,
		statuses:   make(map[string]*AccountStatus),
		stopCh:     make(chan struct{}),
		addUserCh:  make(chan *Account, 10),
		httpCli:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (m *AccountManager) Start() {
	slog.Info("账号管理器启动")
	m.loadAccounts()

	state := LoadManagerState()
	if state != nil {
		m.mu.Lock()
		for uid, ps := range state.Statuses {
			if s, ok := m.statuses[uid]; ok {
				s.Status = ps.Status
				s.ExpireTime = ps.ExpireTime
				s.FrozenUntil = ps.FrozenUntil
				// 从 ExpireTime 重新计算 RemainSec（避免重启后使用过期值）
				if !ps.ExpireTime.IsZero() {
					s.RemainSec = max(0, int(time.Until(ps.ExpireTime).Seconds()))
				} else {
					s.RemainSec = ps.RemainSec
				}
			}
		}
		if state.CurrentUserID != "" {
			if s, ok := m.statuses[state.CurrentUserID]; ok {
				s.IsCurrent = true
				slog.Info("恢复当前账号", "userId", state.CurrentUserID)
			}
		}
		m.mu.Unlock()
		m.rebuildStatusCache()
	}

	if len(m.accounts) == 0 {
		slog.Warn("没有可用账号")
		return
	}

	go m.tryReuseOrConnect()
}

func (m *AccountManager) Stop() {
	close(m.stopCh)
}

func (m *AccountManager) tryReuseOrConnect() {
	m.mu.RLock()
	var savedCurrent *AccountStatus
	for _, s := range m.statuses {
		if s.IsCurrent {
			savedCurrent = s
			break
		}
	}
	m.mu.RUnlock()

	if savedCurrent != nil {
		// 快速路径：检查 bridge 节点是否已在 gateway 在线（重启后 bridge 可能仍在运行）
		if m.pool.GetAvailableCount() > 0 {
			status, remainSec, err := m.getContainerStatus(&savedCurrent.Account)
			if err == nil && status == "AVAILABLE" && remainSec > 300 {
				LogAccountInfo(savedCurrent.Account.UserID, "节点已在线且容器可用，直接复用（跳过注入）")
				m.setCurrentAccount(savedCurrent.Account.UserID, remainSec)
				m.startCountdown(savedCurrent.Account.UserID, time.Duration(remainSec)*time.Second)
				m.scheduleLoop()
				return
			}
		}

		// 慢路径：容器可用但 bridge 未在线，先等待 bridge 自动重连（bridge 脚本有 3 秒重连循环）
		if savedCurrent.Status == "AVAILABLE" {
			status, remainSec, err := m.getContainerStatus(&savedCurrent.Account)
			if err == nil && status == "AVAILABLE" && remainSec > 300 {
				LogAccountInfo(savedCurrent.Account.UserID, "容器可用，等待 bridge 自动重连... 剩余: %d秒", remainSec)
				if m.waitForBridgeReconnect(savedCurrent.Account.UserID, 5*time.Minute) {
					LogAccountInfo(savedCurrent.Account.UserID, "bridge 已自动重连，跳过注入")
					m.setCurrentAccount(savedCurrent.Account.UserID, remainSec)
					m.startCountdown(savedCurrent.Account.UserID, time.Duration(remainSec)*time.Second)
					m.scheduleLoop()
					return
				}
				// bridge 未自动重连，执行注入
				LogAccountWarn(savedCurrent.Account.UserID, "bridge 未自动重连，执行注入")
				ticket, err := m.getTicket(&savedCurrent.Account)
				if err == nil {
					if m.injectBridge(&savedCurrent.Account, ticket) {
						m.setCurrentAccount(savedCurrent.Account.UserID, remainSec)
						m.startCountdown(savedCurrent.Account.UserID, time.Duration(remainSec)*time.Second)
						m.scheduleLoop()
						return
					}
				}
				LogAccountWarn(savedCurrent.Account.UserID, "复用失败，重新走流程")
			}
		}
	}

	m.tryCreateAndConnect()
	m.scheduleLoop()
}

// waitForBridgeReconnect 在指定时间内轮询等待 bridge 节点连回 gateway
func (m *AccountManager) waitForBridgeReconnect(userID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	interval := 3 * time.Second
	start := time.Now()
	lastLog := time.Time{}
	for time.Now().Before(deadline) {
		select {
		case <-m.stopCh:
			return false
		case <-time.After(interval):
		}
		count := m.pool.GetAvailableCount()
		if count > 0 {
			LogAccountInfo(userID, "检测到可用节点: %d", count)
			return true
		}
		// 每 30 秒打一次进度日志，避免「卡死」错觉
		if time.Since(lastLog) >= 30*time.Second {
			LogAccountInfo(userID, "等待节点上线中... 已等待 %ds / 超时 %ds",
				int(time.Since(start).Seconds()), int(timeout.Seconds()))
			lastLog = time.Now()
		}
	}
	return false
}

func (m *AccountManager) loadAccounts() {
	pattern := filepath.Join("users", "user_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		slog.Error("加载账号失败", "error", err)
		return
	}

	for _, file := range files {
		if filepath.Ext(file) == ".example" {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			slog.Error("读取账号文件失败", "file", file, "error", err)
			continue
		}
		var account Account
		if err := json.Unmarshal(data, &account); err != nil {
			slog.Error("解析账号文件失败", "file", file, "error", err)
			continue
		}
		if account.UserID == "" || account.ServiceToken == "" || account.XiaomiChatbotPH == "" {
			slog.Warn("账号缺少必要字段", "file", file)
			continue
		}
		m.accounts = append(m.accounts, &account)
		m.statuses[account.UserID] = &AccountStatus{Account: account, Status: "UNKNOWN"}
		slog.Info("加载账号", "userId", account.UserID, "name", account.Name)
	}
	slog.Info("账号加载完成", "count", len(m.accounts))
}

func (m *AccountManager) scheduleLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.tick()
		case user := <-m.addUserCh:
			m.handleAddUser(user)
		case <-m.stopCh:
			return
		}
	}
}

func (m *AccountManager) handleAddUser(account *Account) {
	m.mu.Lock()
	for _, acc := range m.accounts {
		if acc.UserID == account.UserID {
			m.mu.Unlock()
			slog.Warn("账号已存在，跳过", "userId", account.UserID)
			return
		}
	}
	m.accounts = append(m.accounts, account)
	m.statuses[account.UserID] = &AccountStatus{Account: *account, Status: "UNKNOWN"}
	m.mu.Unlock()
	m.rebuildStatusCache()

	slog.Info("动态添加账号", "userId", account.UserID, "name", account.Name)
	go m.tryCreateAndConnect()
}

func (m *AccountManager) tick() {
	current := m.getCurrentAccount()

	// 已有创建/切换任务在执行，跳过本轮
	if atomic.LoadInt32(&m.creating) == 1 {
		slog.Debug("已有创建任务在执行，tick 跳过")
		return
	}

	if current == nil {
		go m.tryCreateAndConnect()
		return
	}

	m.mu.RLock()
	status := m.statuses[current.UserID]
	m.mu.RUnlock()

	// 容器已过期或不可用，必须切换（不管冷却状态）
	if status == nil || status.Status != "AVAILABLE" || status.RemainSec <= 0 {
		slog.Warn("当前账号不可用，切换", "userId", current.UserID)
		go m.tryCreateAndConnect()
		return
	}

	// 当前账号在冷却中（创建失败后 4 小时内），不要反复尝试切换
	if !status.LastFailTime.IsZero() && time.Since(status.LastFailTime) < 4*time.Hour {
		remain := int(4*time.Hour.Seconds() - time.Since(status.LastFailTime).Seconds())
		slog.Debug("当前账号在冷却中，跳过切换", "userId", current.UserID, "冷却剩余", remain)
		return
	}

	if status.RemainSec <= 300 {
		slog.Info("账号即将过期，切换", "userId", current.UserID, "剩余", status.RemainSec)
		go m.tryCreateAndConnect()
		return
	}

	slog.Debug("当前账号正常", "userId", current.UserID, "剩余", status.RemainSec)

	// 定期更新指标
	m.updateMetrics()
}

func (m *AccountManager) getCurrentAccount() *Account {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, status := range m.statuses {
		if status.IsCurrent {
			return &status.Account
		}
	}
	return nil
}

func (m *AccountManager) getCurrentUserID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, status := range m.statuses {
		if status.IsCurrent {
			return status.Account.UserID
		}
	}
	return ""
}

func (m *AccountManager) getNextAccount() *Account {
	m.mu.RLock()
	defer m.mu.RUnlock()

	currentIdx := -1
	for i, account := range m.accounts {
		if m.statuses[account.UserID] != nil && m.statuses[account.UserID].IsCurrent {
			currentIdx = i
			break
		}
	}

	for i := 1; i <= len(m.accounts); i++ {
		idx := (currentIdx + i) % len(m.accounts)
		account := m.accounts[idx]
		if m.statuses[account.UserID] != nil && m.statuses[account.UserID].IsCurrent {
			continue
		}
		return account
	}
	return nil
}

func (m *AccountManager) tryCreateAndConnect() {
	if !atomic.CompareAndSwapInt32(&m.creating, 0, 1) {
		slog.Debug("已有创建任务在执行，跳过")
		return
	}
	defer atomic.StoreInt32(&m.creating, 0)

	// 整体超时保护：最多 3 分钟
	done := make(chan struct{})
	go func() {
		m.doCreateAndConnect()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Minute):
		slog.Warn("tryCreateAndConnect 超时，强制结束")
	}
}

func (m *AccountManager) doCreateAndConnect() {
	m.mu.RLock()
	accounts := make([]*Account, len(m.accounts))
	copy(accounts, m.accounts)
	m.mu.RUnlock()

	if len(accounts) == 0 {
		slog.Error("没有可用账号")
		return
	}

	// 找到当前账号索引，优先尝试重连
	currentIdx := -1
	m.mu.RLock()
	for i, acc := range accounts {
		if m.statuses[acc.UserID] != nil && m.statuses[acc.UserID].IsCurrent {
			currentIdx = i
			break
		}
	}
	m.mu.RUnlock()

	// 构建遍历顺序：当前账号优先，然后其他账号
	var order []int
	if currentIdx >= 0 {
		order = append(order, currentIdx)
	}
	for i := 0; i < len(accounts); i++ {
		if i != currentIdx {
			order = append(order, i)
		}
	}

	for _, idx := range order {
		account := accounts[idx]

		m.mu.RLock()
		statusInfo := m.statuses[account.UserID]
		inCooldown := false
		if statusInfo != nil {
			// 风控冻结检查
			if !statusInfo.FrozenUntil.IsZero() && time.Now().Before(statusInfo.FrozenUntil) {
				inCooldown = true
				LogAccountInfo(account.UserID, "风控冻结中，解冻时间: %s", statusInfo.FrozenUntil.Format("2006-01-02 15:04"))
			} else if !statusInfo.LastFailTime.IsZero() && time.Since(statusInfo.LastFailTime) < 4*time.Hour {
				// FrozenUntil 已过期但 LastFailTime 在 4 小时内 → 保持冷却
				inCooldown = true
				remain := int(4*time.Hour.Seconds() - time.Since(statusInfo.LastFailTime).Seconds())
				LogAccountInfo(account.UserID, "4小时冷却中，剩余 %d 秒", remain)
			}
		}
		m.mu.RUnlock()

		if inCooldown {
			LogAccountInfo(account.UserID, "在冷却中，跳过")
			continue
		}

		status, remainSec, err := m.getContainerStatus(account)
		if err != nil {
			// 401/403 表示凭证失效，冻结 24 小时
			if strings.Contains(err.Error(), "code=401") || strings.Contains(err.Error(), "code=403") {
				LogAccountWarn(account.UserID, "凭证失效(401/403)，冻结到明天")
				m.mu.Lock()
				m.statuses[account.UserID].FrozenUntil = tomorrowMidnight()
				m.mu.Unlock()
				go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())
			} else {
				LogAccountError(account.UserID, "检查状态失败: %v", err)
			}
			continue
		}

		m.mu.Lock()
		m.statuses[account.UserID].Status = status
		m.statuses[account.UserID].RemainSec = remainSec
		if remainSec > 0 {
			m.statuses[account.UserID].ExpireTime = time.Now().Add(time.Duration(remainSec) * time.Second)
		}
		m.mu.Unlock()
		m.rebuildStatusCache()

		LogAccountInfo(account.UserID, "容器状态: %s, 剩余: %d秒", status, remainSec)

		if status == "AVAILABLE" && remainSec > 300 {
			// 容器可用，清除失败冷却标记
			m.mu.Lock()
			m.statuses[account.UserID].LastFailTime = time.Time{}
			m.mu.Unlock()

			// 注入进行中，跳过（防止并发注入干扰）
			if atomic.LoadInt32(&m.injecting) == 1 {
				LogAccountInfo(account.UserID, "注入进行中，跳过本次检查")
				return
			}

			// 快速路径：bridge 已在线且非强制重连时跳过注入
			if m.pool.GetAvailableCount() > 0 && atomic.CompareAndSwapInt32(&m.forceReconnect, 0, 0) {
				LogAccountInfo(account.UserID, "bridge 节点已在线，跳过注入")
				m.setCurrentAccount(account.UserID, remainSec)
				m.startCountdown(account.UserID, time.Duration(remainSec)*time.Second)
				return
			}
			// 清除强制重连标志
			atomic.StoreInt32(&m.forceReconnect, 0)
			// 容器已创建，尝试注入
			LogAccountInfo(account.UserID, "容器可用，尝试注入")
			ticket, err := m.getTicket(account)
			if err != nil {
				LogAccountError(account.UserID, "获取 ticket 失败: %v", err)
				continue
			}
			if m.injectBridge(account, ticket) {
				slog.Info("连接成功，切换为当前账号", "userId", account.UserID, "剩余", remainSec)
				LogAccountInfo(account.UserID, "连接成功，切换为当前账号")
				m.setCurrentAccount(account.UserID, remainSec)
				m.startCountdown(account.UserID, time.Duration(remainSec)*time.Second)
				return
			}
			// 1011 服务器内部错误 → 冻结 24h，切换下一个账号
			if wsCloseCode.Load() == 1011 {
				LogAccountWarn(account.UserID, "容器内部错误(1011)，冻结到明天")
				m.mu.Lock()
				m.statuses[account.UserID].FrozenUntil = tomorrowMidnight()
				m.mu.Unlock()
				go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())
			} else {
				LogAccountWarn(account.UserID, "注入失败，尝试下一个")
			}
			continue
		}

		if status == "CREATE_FAILED" || status == "DESTROYED" || status == "NOT_CREATED" {
			if IsTodayCreated(account.UserID) {
				ClearTodayCreated(account.UserID)
				LogAccountInfo(account.UserID, "状态 %s，清除今日标记，重新创建", status)
			}
		}

		if IsTodayCreated(account.UserID) {
			LogAccountInfo(account.UserID, "今日已创建，跳过")
			continue
		}

		// 容器正在创建中，等待完成而非重复创建
		if status == "CREATING" {
			LogAccountInfo(account.UserID, "容器正在创建中，等待完成")
			continue
		}

		if status == "NOT_CREATED" || status == "CREATE_FAILED" || status == "DESTROYED" || remainSec <= 0 {
			LogAccountInfo(account.UserID, "尝试创建容器")
			if m.tryCreateForAccount(account) {
				// 创建+注入成功，切换完成
				return
			}
			// 创建失败，继续尝试下一个账号
			LogAccountWarn(account.UserID, "创建/注入失败，尝试下一个账号")
			continue
		}

		LogAccountInfo(account.UserID, "状态 %s，跳过", status)
	}

	LogAccountWarn("", "所有账号不可用或在冷却中，等待下次检查")
}

func (m *AccountManager) tryCreateForAccount(account *Account) bool {
	status, remainSec, err := m.getContainerStatus(account)
	if err != nil {
		slog.Error("检查状态失败", "userId", account.UserID, "error", err)
		return false
	}

	m.mu.Lock()
	m.statuses[account.UserID].Status = status
	m.statuses[account.UserID].RemainSec = remainSec
	if remainSec > 0 {
		m.statuses[account.UserID].ExpireTime = time.Now().Add(time.Duration(remainSec) * time.Second)
	}
	m.mu.Unlock()
	m.rebuildStatusCache()

	go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())

	slog.Info("容器状态", "userId", account.UserID, "status", status, "剩余", remainSec)
	LogAccountInfo(account.UserID, "容器状态: %s, 剩余: %d秒", status, remainSec)

	// 容器正在创建中，等待完成而非重复创建
	if status == "CREATING" {
		LogAccountInfo(account.UserID, "容器正在创建中，等待完成")
		// 等待容器就绪（最多等待 2 分钟）
		for waitCount := 0; waitCount < 12; waitCount++ {
			time.Sleep(10 * time.Second)
			LogAccountInfo(account.UserID, "等待容器就绪... (%d/12)", waitCount+1)
			status, remainSec, err = m.getContainerStatus(account)
			if err != nil {
				LogAccountError(account.UserID, "检查状态失败: %v", err)
				continue
			}
			if status == "AVAILABLE" {
				LogAccountInfo(account.UserID, "容器就绪: %s, 剩余: %d秒", status, remainSec)
				break
			}
			if status != "CREATING" {
				LogAccountError(account.UserID, "容器状态异常: %s", status)
				return false
			}
		}
		if status != "AVAILABLE" {
			LogAccountError(account.UserID, "容器未就绪: %s", status)
			return false
		}
	}

	if status == "NOT_CREATED" || status == "DESTROYED" || status == "CREATE_FAILED" || remainSec <= 0 {
		ClearTodayCreated(account.UserID)

		if !m.createContainer(account) {
			m.mu.Lock()
			m.statuses[account.UserID].LastFailTime = time.Now()
			m.mu.Unlock()
			MarkTodayCreated(account.UserID)
			go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())
			LogAccountWarn(account.UserID, "创建失败，进入冷却")
			return false
		}

		// 创建成功，清除失败冷却标记
		m.mu.Lock()
		m.statuses[account.UserID].LastFailTime = time.Time{}
		m.mu.Unlock()

		MarkTodayCreated(account.UserID)
		LogAccountInfo(account.UserID, "容器创建成功，等待就绪...")

		// 等待容器就绪（最多等待 2 分钟）
		for waitCount := 0; waitCount < 12; waitCount++ {
			time.Sleep(10 * time.Second)
			LogAccountInfo(account.UserID, "检查容器就绪状态... (%d/12)", waitCount+1)
			status, remainSec, err = m.getContainerStatus(account)
			if err != nil {
				LogAccountError(account.UserID, "检查状态失败: %v", err)
				continue
			}
			if status == "AVAILABLE" {
				LogAccountInfo(account.UserID, "容器就绪: %s, 剩余: %d秒", status, remainSec)
				break
			}
			if status == "CREATING" {
				LogAccountInfo(account.UserID, "容器创建中，继续等待...")
				continue
			}
			// 其他状态（CREATE_FAILED, DESTROYED 等）
			LogAccountError(account.UserID, "容器状态异常: %s", status)
			return false
		}

		if status != "AVAILABLE" {
			slog.Error("容器未就绪", "userId", account.UserID, "status", status)
			LogAccountError(account.UserID, "容器未就绪: %s", status)
			return false
		}
		LogAccountInfo(account.UserID, "容器就绪: %s, 剩余: %d秒", status, remainSec)

		if remainSec > 0 {
			m.startCountdown(account.UserID, time.Duration(remainSec)*time.Second)
		}
	}

	LogAccountInfo(account.UserID, "获取 WebSocket ticket...")
	ticket, err := m.getTicket(account)
	if err != nil {
		slog.Error("获取 ticket 失败", "userId", account.UserID, "error", err)
		LogAccountError(account.UserID, "获取 ticket 失败: %v", err)
		return false
	}
	LogAccountInfo(account.UserID, "获取 ticket 成功")

	LogAccountInfo(account.UserID, "开始注入 bridge...")
	if !m.injectBridge(account, ticket) {
		if wsCloseCode.Load() == 1011 {
			LogAccountWarn(account.UserID, "新容器内部错误(1011)，冻结 24 小时")
			m.mu.Lock()
			m.statuses[account.UserID].FrozenUntil = tomorrowMidnight()
			m.mu.Unlock()
			go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())
		} else {
			LogAccountError(account.UserID, "注入 bridge 失败")
		}
		return false
	}

	slog.Info("注入成功，切换为当前账号", "userId", account.UserID, "剩余", remainSec)
	LogAccountInfo(account.UserID, "注入成功，切换为当前账号，剩余: %d秒", remainSec)
	m.setCurrentAccount(account.UserID, remainSec)
	return true
}

func (m *AccountManager) getContainerStatus(account *Account) (string, int, error) {
	statusURL := m.baseURL + "/open-apis/user/mimo-claw/status"
	req, err := http.NewRequest("GET", statusURL, nil)
	if err != nil {
		return "", 0, err
	}
	m.setHeaders(req, account)

	resp, err := m.httpCli.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("检查状态失败: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Status     string `json:"status"`
			ExpireTime int64  `json:"expireTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", 0, fmt.Errorf("解析状态响应失败: %w (body: %s)", err, utils.Truncate(string(body), 200))
	}

	if result.Code != 0 {
		return "", 0, fmt.Errorf("API error: code=%d, msg=%s", result.Code, result.Msg)
	}

	remainSec := 0
	if result.Data.ExpireTime > 0 {
		remainSec = max(0, int(result.Data.ExpireTime/1000-time.Now().Unix()))
	}

	return result.Data.Status, remainSec, nil
}

func (m *AccountManager) createContainer(account *Account) bool {
	slog.Info("创建容器", "userId", account.UserID)

	agreeURL := m.baseURL + "/open-apis/agreement/user/mimo-claw?xiaomichatbot_ph=" + url.QueryEscape(account.XiaomiChatbotPH)
	req, err := http.NewRequest("POST", agreeURL, nil)
	if err == nil {
		m.setHeaders(req, account)
		resp, err := m.doRequest(req)
		if err == nil {
			resp.Body.Close()
		}
	}

	createURL := m.baseURL + "/open-apis/user/mimo-claw/create?xiaomichatbot_ph=" + url.QueryEscape(account.XiaomiChatbotPH)

	// 午夜刷新：最多尝试3次确认是否风控
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err = http.NewRequest("POST", createURL, nil)
		if err != nil {
			slog.Error("创建请求失败", "error", err)
			return false
		}
		m.setHeaders(req, account)

		resp, err := m.doRequest(req)
		if err != nil {
			slog.Error("创建请求失败", "error", err)
			return false
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			slog.Error("解析创建响应失败", "error", err, "body", utils.Truncate(string(body), 200))
			return false
		}

		switch result.Code {
		case 0:
			slog.Info("容器创建成功", "userId", account.UserID, "attempt", attempt)
			LogAccountInfo(account.UserID, "容器创建成功 (第%d次尝试)", attempt)
			return true
		case 7001:
			slog.Warn("今日额度用完", "userId", account.UserID)
			LogAccountWarn(account.UserID, "今日额度用完")
			return false
		case 200:
			// 风控状态：午夜刷新时重试确认
			if attempt < maxRetries {
				LogAccountWarn(account.UserID, "疑似风控 (第%d次)，重试确认...", attempt)
				time.Sleep(5 * time.Second)
				continue
			}
			// 3次都返回风控，确认为风控账号
			slog.Warn("账号被风控，冻结 24 小时 (3次确认)", "userId", account.UserID)
			LogAccountWarn(account.UserID, "账号被风控，冻结到明天 (3次确认)")
			m.mu.Lock()
			if status, ok := m.statuses[account.UserID]; ok {
				status.FrozenUntil = tomorrowMidnight()
			}
			m.mu.Unlock()
			return false
		default:
			slog.Warn("创建失败", "userId", account.UserID, "code", result.Code, "msg", result.Msg)
			LogAccountWarn(account.UserID, "创建失败: code=%d, msg=%s", result.Code, result.Msg)
			return false
		}
	}

	return false
}

func (m *AccountManager) getTicket(account *Account) (string, error) {
	ticketURL := m.baseURL + "/open-apis/user/ws/ticket?xiaomichatbot_ph=" + url.QueryEscape(account.XiaomiChatbotPH)
	req, err := http.NewRequest("GET", ticketURL, nil)
	if err != nil {
		return "", err
	}
	m.setHeaders(req, account)

	resp, err := m.httpCli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Ticket string `json:"ticket"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("解析 ticket 响应失败: %w (body: %s)", err, utils.Truncate(string(body), 200))
	}

	if result.Code != 0 {
		return "", fmt.Errorf("API error: code=%d, msg=%s", result.Code, result.Msg)
	}

	return result.Data.Ticket, nil
}

func (m *AccountManager) injectBridge(account *Account, ticket string) bool {
	atomic.StoreInt32(&m.injecting, 1)
	defer atomic.StoreInt32(&m.injecting, 0)
	wsCloseCode.Store(0) // 重置关闭码

	if !injectBridgeImpl(m, account, ticket) {
		return false
	}
	// injectBridgeImpl 内部已处理节点上线等待
	return true
}

func (m *AccountManager) setCurrentAccount(userID string, remainSec int) {
	m.mu.Lock()

	for _, status := range m.statuses {
		status.IsCurrent = false
	}

	if status, ok := m.statuses[userID]; ok {
		status.IsCurrent = true
		status.Status = "AVAILABLE"
		status.RemainSec = remainSec
		status.ExpireTime = time.Now().Add(time.Duration(remainSec) * time.Second)
	}

	// 在锁内计算指标（避免调用 updateMetrics 导致 RLock 死锁）
	total := len(m.accounts)
	online := 0
	for _, s := range m.statuses {
		if s.IsCurrent && s.Status == "AVAILABLE" && s.RemainSec > 0 {
			online++
		}
	}

	m.mu.Unlock()

	slog.Info("切换账号", "userId", userID, "剩余", remainSec)
	LogAccountInfo(userID, "切换为当前账号，剩余: %d秒", remainSec)

	// 锁外操作，避免死锁
	metrics.Get().UpdateAccounts(total, online)
	go SaveManagerState(userID, m.snapshotStatuses())
	m.rebuildStatusCache()
}

func (m *AccountManager) setHeaders(req *http.Request, account *Account) {
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "system")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", m.baseURL)
	req.Header.Set("Referer", m.baseURL+"/")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0")
	req.Header.Set("x-timeZone", "Asia/Shanghai")
	req.Header.Set("Cookie", fmt.Sprintf(`serviceToken="%s"; userId=%s; xiaomichatbot_ph="%s"`, account.ServiceToken, account.UserID, account.XiaomiChatbotPH))
}

func (m *AccountManager) updateMetrics() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := len(m.accounts)
	online := 0
	for _, s := range m.statuses {
		if s.IsCurrent && s.Status == "AVAILABLE" && s.RemainSec > 0 {
			online++
		}
	}

	metrics.Get().UpdateAccounts(total, online)
	slog.Debug("更新指标", "total", total, "online", online)
}

// rebuildStatusCache 构建状态快照并原子存储，供 GetStatus 无锁读取
func (m *AccountManager) rebuildStatusCache() {
	m.mu.RLock()
	result := make([]AccountStatus, 0, len(m.statuses))
	for _, status := range m.statuses {
		s := *status
		result = append(result, s)
	}
	m.mu.RUnlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].Account.UserID < result[j].Account.UserID
	})
	m.cachedStatus.Store(&result)
}

// GetStatus 返回所有账号的状态快照（无锁读取，不会阻塞）
func (m *AccountManager) GetStatus() []AccountStatus {
	// 优先从原子缓存读取（无锁）
	if cached := m.cachedStatus.Load(); cached != nil {
		go m.rebuildStatusCache() // 后台刷新
		return *cached
	}

	// 首次调用或缓存未就绪：尝试获取锁，带超时保护
	done := make(chan struct{})
	go func() {
		m.rebuildStatusCache()
		close(done)
	}()

	select {
	case <-done:
		if cached := m.cachedStatus.Load(); cached != nil {
			return *cached
		}
		return nil
	case <-time.After(5 * time.Second):
		slog.Warn("GetStatus 获取锁超时，返回空结果")
		return nil
	}
}

// snapshotStatuses 创建 statuses 的快照（线程安全），供 goroutine 使用
func (m *AccountManager) snapshotStatuses() map[string]*AccountStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := make(map[string]*AccountStatus, len(m.statuses))
	for uid, s := range m.statuses {
		cp := *s
		snap[uid] = &cp
	}
	return snap
}

func (m *AccountManager) doRequest(req *http.Request) (*http.Response, error) {
	if m.proxyMgr != nil && m.proxyMgr.GetProxyCount() > 0 && strings.Contains(req.URL.Host, "xiaomimimo.com") {
		// 先确保代理可用
		proxy := m.proxyMgr.EnsureAvailable()
		if proxy == "" {
			slog.Warn("无可用代理，回退直连")
			return m.httpCli.Do(req)
		}

		var lastErr error
		for i := 0; i < 3; i++ {
			proxy = m.proxyMgr.GetProxy()
			transport := m.proxyMgr.GetTransport()
			client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

			// clone request body for retry
			var bodyCopy []byte
			if req.Body != nil {
				bodyCopy, _ = io.ReadAll(req.Body)
				req.Body.Close()
				req.Body = io.NopCloser(bytes.NewReader(bodyCopy))
			}

			resp, err := client.Do(req)
			if err == nil {
				m.proxyMgr.MarkUsed(proxy)
				return resp, nil
			}

			lastErr = err
			slog.Warn("代理请求失败，换 IP 重试", "attempt", i+1, "proxy", proxy, "error", err)
			m.proxyMgr.RotateProxy()

			// restore body for next attempt
			if bodyCopy != nil {
				req.Body = io.NopCloser(bytes.NewReader(bodyCopy))
			}
		}
		// 代理全部失败，回退直连
		slog.Warn("代理重试 3 次均失败，回退直连", "error", lastErr)
		return m.httpCli.Do(req)
	}
	return m.httpCli.Do(req)
}

func (m *AccountManager) TriggerAccountRebuild(userID string) {
	// 注入进行中不触发重建（注入命令会 kill 旧 bridge，导致 onNodeDown，但新 bridge 马上启动）
	if atomic.LoadInt32(&m.injecting) == 1 {
		slog.Info("注入进行中，忽略重建信号", "userId", userID)
		return
	}

	// 标记强制重连，跳过 bridge 在线检查
	atomic.StoreInt32(&m.forceReconnect, 1)

	// 先断开该账号的 bridge 节点
	m.pool.Remove(userID)
	LogAccountInfo(userID, "已断开 bridge 节点，准备重新注入")

	m.mu.Lock()
	if status, ok := m.statuses[userID]; ok {
		// 不清除 IsCurrent，让 doCreateAndConnect 优先尝试重连该账号
		status.Status = "REBUILDING"
	}
	m.mu.Unlock()
	m.rebuildStatusCache()

	slog.Info("触发重建（优先重连当前账号）", "userId", userID)
	go m.tryCreateAndConnect()
}

// ForceInject 强制注入：跳过所有检查（冻结/冷却/今日创建），直接创建+注入
func (m *AccountManager) ForceInject(userID string) map[string]any {
	account := m.getAccount(userID)
	if account == nil {
		return map[string]any{"success": false, "error": "账号不存在"}
	}

	// 断开旧 bridge
	m.pool.Remove(userID)
	LogAccountInfo(userID, "强制注入：已断开旧 bridge")

	// 清除所有限制标记
	m.mu.Lock()
	if s, ok := m.statuses[userID]; ok {
		s.FrozenUntil = time.Time{}
		s.LastFailTime = time.Time{}
	}
	m.mu.Unlock()
	ClearTodayCreated(userID)

	// 获取容器状态
	status, remainSec, err := m.getContainerStatus(account)
	if err != nil {
		return map[string]any{"success": false, "error": fmt.Sprintf("检查状态失败: %v", err)}
	}

	// 容器不存在，先创建
	if status != "AVAILABLE" || remainSec <= 0 {
		LogAccountInfo(userID, "强制注入：容器状态 %s，先创建", status)
		if !m.createContainer(account) {
			return map[string]any{"success": false, "error": "创建容器失败"}
		}
		// 等待容器就绪
		for i := 0; i < 12; i++ {
			time.Sleep(10 * time.Second)
			status, remainSec, err = m.getContainerStatus(account)
			if err != nil {
				continue
			}
			if status == "AVAILABLE" {
				break
			}
			LogAccountInfo(userID, "强制注入：等待容器就绪 (%d/12) status=%s", i+1, status)
		}
		if status != "AVAILABLE" {
			return map[string]any{"success": false, "error": fmt.Sprintf("容器未就绪: %s", status)}
		}
	}

	// 获取 ticket
	ticket, err := m.getTicket(account)
	if err != nil {
		return map[string]any{"success": false, "error": fmt.Sprintf("获取 ticket 失败: %v", err)}
	}

	// 强制注入
	LogAccountInfo(userID, "强制注入：开始注入 bridge")
	if !m.injectBridge(account, ticket) {
		code := wsCloseCode.Load()
		return map[string]any{"success": false, "error": fmt.Sprintf("注入失败 (ws_close=%d)", code)}
	}

	// 设置为当前账号
	m.setCurrentAccount(userID, remainSec)
	m.startCountdown(userID, time.Duration(remainSec)*time.Second)
	go SaveManagerState(m.getCurrentUserID(), m.snapshotStatuses())
	LogAccountInfo(userID, "强制注入成功，切换为当前账号，剩余: %d秒", remainSec)
	return map[string]any{"success": true, "remain_sec": remainSec}
}

func (m *AccountManager) getAccount(userID string) *Account {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, a := range m.accounts {
		if a.UserID == userID {
			return a
		}
	}
	return nil
}

// TriggerAccountRebuildWithGrace 带宽限期的重建触发
// bridge 脚本有自动重连机制，短暂断线不需要完整重建
// 等待 grace 秒后检查 bridge 是否已自动恢复，如果恢复则跳过重建
func (m *AccountManager) TriggerAccountRebuildWithGrace(userID string, grace time.Duration) {
	// 注入进行中不触发重建（注入命令会 kill 旧 bridge，导致节点下线，但新 bridge 马上启动）
	if atomic.LoadInt32(&m.injecting) == 1 {
		slog.Info("注入进行中，忽略节点下线信号", "userId", userID)
		return
	}
	go func() {
		slog.Info("节点下线，等待 bridge 自动重连...", "userId", userID, "grace", grace)
		time.Sleep(grace)

		// 再次检查是否注入中（可能 grace 期间开始了新的注入）
		if atomic.LoadInt32(&m.injecting) == 1 {
			slog.Info("注入进行中，跳过重建", "userId", userID)
			return
		}

		// 宽限期后检查 bridge 是否已自动重连
		if m.pool.GetAvailableCount() > 0 {
			slog.Info("bridge 已自动重连，跳过重建", "userId", userID)
			// 确保状态正确
			m.mu.Lock()
			if status, ok := m.statuses[userID]; ok && status.IsCurrent {
				status.Status = "AVAILABLE"
			}
			m.mu.Unlock()
			return
		}

		slog.Info("bridge 未自动重连，触发完整重建", "userId", userID)
		m.TriggerAccountRebuild(userID)
	}()
}

func (m *AccountManager) TriggerAddUser(account *Account) {
	m.addUserCh <- account
}

func (m *AccountManager) TestAccount(userID string) map[string]any {
	m.mu.RLock()
	statusInfo := m.statuses[userID]
	m.mu.RUnlock()

	if statusInfo == nil {
		return map[string]any{"success": false, "error": "账号不存在"}
	}

	// 检查 bridge 节点是否在线
	node := m.pool.GetAvailable()
	if node == nil {
		return map[string]any{
			"success": false,
			"error":   "bridge 未连接",
			"step":    "bridge",
		}
	}

	// 发送真实模型调用测试请求（非流式，最小 token 数）
	// mimo-v2.5-pro 必须包含 system prompt 才能正常调用
	testBody := map[string]any{
		"model":      "mimo-v2.5-pro",
		"max_tokens": 5,
		"stream":     false,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a personal assistant running inside OpenClaw."},
			{"role": "user", "content": "hi"},
		},
	}

	reqID := gateway.GenerateID()
	pending, err := m.pool.SendToNode(node, reqID, "POST", "/v1/chat/completions", testBody)
	if err != nil {
		return map[string]any{"success": false, "error": fmt.Sprintf("发送请求失败: %v", err), "step": "send"}
	}
	defer m.pool.CleanupPending(reqID)

	// 等待响应（30 秒超时）
	timeout := time.After(30 * time.Second)
	var httpStatus int
	var errBody string

	for {
		select {
		case msg, ok := <-pending.Response:
			if !ok {
				return map[string]any{"success": false, "error": "连接断开", "step": "response"}
			}
			switch msg.Type {
			case "start":
				httpStatus = msg.Status
			case "finish":
				return map[string]any{
					"success":    true,
					"step":       "ok",
					"status":     httpStatus,
					"remain_sec": statusInfo.RemainSec,
				}
			case "error":
				if msg.Body != nil {
					// 尝试解码 JSON 字符串
					var s string
					if json.Unmarshal(msg.Body, &s) == nil && s != "" {
						errBody = s
					} else {
						errBody = string(msg.Body)
					}
				}
				st := msg.Status
				if st == 0 {
					st = 502
				}
				return map[string]any{
					"success": false,
					"error":   errBody,
					"step":    "model",
					"status":  st,
				}
			}
		case <-timeout:
			return map[string]any{"success": false, "error": "调用超时 (30s)", "step": "timeout"}
		}
	}
}

func (m *AccountManager) startCountdown(userID string, duration time.Duration) {
	m.mu.Lock()
	status := m.statuses[userID]
	if status == nil {
		m.mu.Unlock()
		return
	}

	if status.TimerStopCh != nil {
		close(status.TimerStopCh)
	}

	status.TimerStopCh = make(chan struct{})
	status.ExpireTime = time.Now().Add(duration)
	status.RemainSec = int(duration.Seconds())
	stopCh := status.TimerStopCh
	m.mu.Unlock()

	slog.Info("启动倒计时", "userId", userID, "duration", duration)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				remain := int(time.Until(status.ExpireTime).Seconds())
				if remain < 0 {
					m.mu.Lock()
					s := m.statuses[userID]
					if s != nil {
						s.RemainSec = 0
						s.Status = "EXPIRED"
					}
					m.mu.Unlock()
					LogAccountInfo(userID, "容器已过期")
					return
				}
				// 原子更新 RemainSec，避免每秒获取写锁
				m.mu.RLock()
				if s, ok := m.statuses[userID]; ok {
					s.RemainSec = remain
				}
				m.mu.RUnlock()
			case <-stopCh:
				return
			}
		}
	}()
}

func (m *AccountManager) stopCountdown(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status, ok := m.statuses[userID]; ok && status.TimerStopCh != nil {
		close(status.TimerStopCh)
		status.TimerStopCh = nil
	}
}

func (m *AccountManager) ImportAccount(userID, serviceToken, ph, name, group, region string) error {
	account := &Account{
		UserID:          userID,
		ServiceToken:    serviceToken,
		XiaomiChatbotPH: ph,
		Name:            name,
		Group:           group,
		Region:          region,
		ImportedAt:      time.Now().Format(time.RFC3339),
	}

	// 先保存文件（在锁外做 I/O，减少锁竞争）
	if err := saveAccount(account); err != nil {
		return err
	}

	m.mu.Lock()
	for _, acc := range m.accounts {
		if acc.UserID == userID {
			m.mu.Unlock()
			return fmt.Errorf("账号已存在: %s", userID)
		}
	}
	m.accounts = append(m.accounts, account)
	m.statuses[userID] = &AccountStatus{Account: *account, Status: "UNKNOWN"}
	m.mu.Unlock()

	slog.Info("导入账号", "userId", userID, "name", name)
	go m.tryCreateAndConnect()
	return nil
}

// validateUserID 验证用户 ID 格式，防止路径穿越
func validateUserID(userID string) bool {
	if userID == "" || len(userID) > 128 {
		return false
	}
	for _, c := range userID {
		if c == '/' || c == '\\' || c == '.' {
			return false
		}
	}
	return true
}

func (m *AccountManager) DeleteAccount(userID string) error {
	if !validateUserID(userID) {
		return fmt.Errorf("无效的用户 ID: %s", userID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, acc := range m.accounts {
		if acc.UserID == userID {
			m.accounts = append(m.accounts[:i], m.accounts[i+1:]...)
			delete(m.statuses, userID)
			filePath := filepath.Join("users", fmt.Sprintf("user_%s.json", userID))
			os.Remove(filePath)
			slog.Info("删除账号", "userId", userID)
			return nil
		}
	}

	return fmt.Errorf("账号不存在: %s", userID)
}

func saveAccount(account *Account) error {
	if !validateUserID(account.UserID) {
		return fmt.Errorf("无效的用户 ID: %s", account.UserID)
	}
	if err := os.MkdirAll("users", 0755); err != nil {
		return err
	}
	filePath := filepath.Join("users", fmt.Sprintf("user_%s.json", account.UserID))
	data, err := json.MarshalIndent(account, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

