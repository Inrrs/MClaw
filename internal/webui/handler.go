package webui

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"mclaw/internal/manager"
	"mclaw/internal/metrics"
	"mclaw/internal/proxy"
)

//go:embed static/index.html
var indexHTML string

// logRequest 记录 WebUI API 请求（含响应时间）
func logRequest(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next(w, r)
		elapsed := time.Since(start)
		if elapsed > 500*time.Millisecond {
			slog.Warn("WebUI 请求慢", "method", r.Method, "path", r.URL.Path, "elapsed", elapsed.String())
		} else {
			slog.Debug("WebUI 请求", "method", r.Method, "path", r.URL.Path, "elapsed", elapsed.String())
		}
	}
}

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	if err := h.indexTmpl.Execute(w, nil); err != nil {
		slog.Error("渲染首页失败", "error", err)
	}
}

func (h *Handler) handleAccounts(w http.ResponseWriter, r *http.Request) {
	statuses := h.manager.GetStatus()
	slog.Debug("账号列表查询", "count", len(statuses))
	type AccountInfo struct {
		UserID    string `json:"user_id"`
		Name      string `json:"name"`
		Group     string `json:"group"`
		Status    string `json:"status"`
		RemainSec int    `json:"remain_sec"`
		IsCurrent bool   `json:"is_current"`
	}

	accounts := make([]AccountInfo, 0)
	for _, s := range statuses {
		accounts = append(accounts, AccountInfo{
			UserID:    s.Account.UserID,
			Name:      s.Account.Name,
			Group:     s.Account.Group,
			Status:    s.Status,
			RemainSec: s.RemainSec,
			IsCurrent: s.IsCurrent,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(accounts)
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := metrics.Get()

	// 获取实时数据
	statuses := h.manager.GetStatus()
	totalAccounts := len(statuses)
	onlineAccounts := 0
	for _, s := range statuses {
		if s.IsCurrent && s.Status == "AVAILABLE" && s.RemainSec > 0 {
			onlineAccounts++
		}
	}

	// 覆盖 metrics 中的数据
	data := m.ToMap()
	data["total_accounts"] = totalAccounts
	data["online_accounts"] = onlineAccounts

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) handleAccountLogs(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	limit := 100
	logs := manager.AccountLogs.GetByUser(userID, limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func (h *Handler) handleProxyStats(w http.ResponseWriter, r *http.Request) {
	if h.proxyMgr == nil {
		writeJSON(w, map[string]any{"enabled": false})
		return
	}
	stats := h.proxyMgr.GetStats()
	writeJSON(w, map[string]any{
		"enabled":     true,
		"total_used":  stats.TotalUsed,
		"today_used":  stats.TodayUsed,
		"today_date":  stats.TodayDate,
		"last_ip":     stats.LastIP,
		"last_used":   stats.LastUsed,
		"proxy_count": h.proxyMgr.GetProxyCount(),
	})
}

func (h *Handler) handleProxyUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(r.Body)
	slog.Info("收到代理更新请求", "body", string(body))
	var req struct {
		PoolURL  string `json:"pool_url"`
		Protocol string `json:"protocol"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		slog.Error("解析代理更新请求失败", "error", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if h.proxyMgr != nil {
		h.proxyMgr.UpdateURL(req.PoolURL)
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (h *Handler) handleWhitelistGet(w http.ResponseWriter, r *http.Request) {
	if h.proxyMgr == nil {
		writeJSON(w, map[string]any{"configured": false})
		return
	}
	writeJSON(w, h.proxyMgr.GetWhitelistStats())
}

func (h *Handler) handleWhitelistUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req struct {
		UID         string `json:"uid"`
		Key         string `json:"key"`
		URL         string `json:"url"`          // 完整 URL，自动解析 uid/key
		WhitelistURL string `json:"whitelist_url"` // 白名单 API 基础 URL
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": "Invalid JSON"})
		return
	}
	if h.proxyMgr == nil {
		writeJSON(w, map[string]any{"ok": false, "error": "代理管理器未初始化"})
		return
	}
	uid, key, whitelistURL := req.UID, req.Key, req.WhitelistURL
	// 支持粘贴完整 URL 自动解析
	if req.URL != "" {
		parsedUID, parsedKey, parsedBase, err := proxy.ParseWhitelistURL(req.URL)
		if err != nil {
			writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		uid, key, whitelistURL = parsedUID, parsedKey, parsedBase
	}
	if uid == "" || key == "" {
		writeJSON(w, map[string]any{"ok": false, "error": "uid 和 key 不能为空"})
		return
	}
	h.proxyMgr.UpdateWhitelistConfig(uid, key, whitelistURL)
	// 立即尝试同步白名单
	if err := h.proxyMgr.EnsureWhitelist(); err != nil {
		writeJSON(w, map[string]any{"ok": true, "warning": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (h *Handler) handleWhitelistParseURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeJSON(w, map[string]any{"ok": false, "error": "请提供 URL"})
		return
	}
	uid, key, baseURL, err := proxy.ParseWhitelistURL(req.URL)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "uid": uid, "key": key, "base_url": baseURL})
}

func (h *Handler) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		slog.Error("读取导入请求体失败", "error", err)
		writeJSON(w, map[string]any{"success": false, "error": "读取请求体失败"})
		return
	}
	r.Body.Close()
	slog.Info("导入请求", "size", len(rawBody))

	var jsonBody struct {
		RawText string `json:"raw_text"`
		Group   string `json:"group"`
	}

	var accounts []manager.Account

	if err := json.Unmarshal(rawBody, &jsonBody); err == nil && jsonBody.RawText != "" {
		acc, err := parseCookieString(jsonBody.RawText)
		if err != nil {
			writeJSON(w, map[string]any{"success": false, "error": err.Error()})
			return
		}
		if jsonBody.Group != "" {
			acc.Group = jsonBody.Group
		}
		accounts = append(accounts, *acc)
	} else if err := json.Unmarshal(rawBody, &accounts); err == nil {
		// JSON 数组
	} else {
		var single manager.Account
		if err := json.Unmarshal(rawBody, &single); err == nil {
			accounts = append(accounts, single)
		} else {
			acc, err := parseCookieString(string(rawBody))
			if err != nil {
				writeJSON(w, map[string]any{"success": false, "error": err.Error()})
				return
			}
			accounts = append(accounts, *acc)
		}
	}

	imported := 0
	for _, acc := range accounts {
		if acc.UserID == "" || acc.ServiceToken == "" || acc.XiaomiChatbotPH == "" {
			continue
		}
		if err := h.manager.ImportAccount(acc.UserID, acc.ServiceToken, acc.XiaomiChatbotPH, acc.Name, acc.Group, acc.Region); err != nil {
			slog.Error("导入失败", "userId", acc.UserID, "error", err)
			continue
		}
		imported++
	}

	writeJSON(w, map[string]any{"success": true, "imported": imported})
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		slog.Warn("删除请求参数无效", "error", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := h.manager.DeleteAccount(req.UserID); err != nil {
		slog.Error("删除账号失败", "userId", req.UserID, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

func (h *Handler) handleDeleteBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserIDs []string `json:"userIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// 限制批量删除数量
	if len(req.UserIDs) > 50 {
		writeJSON(w, map[string]any{"ok": false, "error": "最多批量删除 50 个账号"})
		return
	}

	deleted := 0
	for _, uid := range req.UserIDs {
		if err := h.manager.DeleteAccount(uid); err == nil {
			deleted++
		}
	}

	writeJSON(w, map[string]any{"ok": true, "deleted": deleted})
}

func (h *Handler) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeJSON(w, map[string]any{"success": false, "error": "userId required"})
		return
	}

	slog.Info("触发重新注入", "userId", req.UserID)
	h.manager.TriggerAccountRebuild(req.UserID)
	writeJSON(w, map[string]any{"success": true, "message": "重建信号已发送"})
}

func (h *Handler) handleForceInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeJSON(w, map[string]any{"success": false, "error": "userId required"})
		return
	}
	slog.Info("强制注入", "userId", req.UserID)
	result := h.manager.ForceInject(req.UserID)
	writeJSON(w, result)
}

func (h *Handler) handleTestAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		slog.Warn("测试账号请求参数无效", "error", err)
		writeJSON(w, map[string]any{"success": false, "error": "userId required"})
		return
	}

	slog.Info("测试账号连接", "userId", req.UserID)
	result := h.manager.TestAccount(req.UserID)
	slog.Info("测试结果", "userId", req.UserID, "success", result["success"], "step", result["step"])
	writeJSON(w, result)
}

func (h *Handler) handleErrorLogs(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if h.pool != nil && h.pool.ErrorStore != nil {
		errors := h.pool.ErrorStore.GetAll(limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(errors)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode([]any{})
}

// parseCookieString 从 curl 命令或 cookie 字符串中解析账号信息
func parseCookieString(s string) (*manager.Account, error) {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "curl ") {
		s = extractCookieFromCurl(s)
		if s == "" {
			return nil, fmt.Errorf("无法从 curl 命令中提取 cookie")
		}
	}

	cookies := make(map[string]string)
	pairs := strings.Split(s, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eqIdx := strings.Index(pair, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.TrimSpace(pair[eqIdx+1:])
		val = strings.Trim(val, `"' `)
		if key != "" && val != "" {
			cookies[key] = val
		}
	}

	uid := cookies["userId"]
	st := cookies["serviceToken"]
	ph := cookies["xiaomichatbot_ph"]

	if uid == "" || st == "" || ph == "" {
		return nil, fmt.Errorf("缺少必要字段 (userId=%s, serviceToken=%v, ph=%s)", uid, st != "", ph)
	}

	return &manager.Account{
		UserID:          uid,
		ServiceToken:    st,
		XiaomiChatbotPH: ph,
		Name:            "Imported_" + uid,
	}, nil
}

func extractCookieFromCurl(s string) string {
	s = joinCurlLines(s)

	var cookieStart int
	var quoteChar byte

	if idx := strings.Index(s, " -b '"); idx >= 0 {
		cookieStart = idx + 5
		quoteChar = '\''
	} else if idx := strings.Index(s, ` -b "`); idx >= 0 {
		cookieStart = idx + 5
		quoteChar = '"'
	} else if idx := strings.Index(s, " --cookie '"); idx >= 0 {
		cookieStart = idx + 11
		quoteChar = '\''
	} else if idx := strings.Index(s, ` --cookie "`); idx >= 0 {
		cookieStart = idx + 11
		quoteChar = '"'
	} else {
		return ""
	}

	end := cookieStart
	for end < len(s) {
		if s[end] == quoteChar {
			if end > 0 && s[end-1] == '\\' {
				end++
				continue
			}
			break
		}
		end++
	}

	if end >= len(s) {
		return ""
	}
	return s[cookieStart:end]
}

func joinCurlLines(s string) string {
	lines := strings.Split(s, "\n")
	var result strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		for i := 0; i < len(line); i++ {
			ch := line[i]
			if inQuotes {
				if ch == quoteChar {
					inQuotes = false
				}
			} else {
				if ch == '"' || ch == '\'' {
					quoteChar = ch
					inQuotes = true
				}
			}
		}

		if !inQuotes && strings.HasSuffix(line, "\\") {
			line = strings.TrimSuffix(line, "\\")
			line = strings.TrimSpace(line)
		}

		result.WriteString(line)
		if !inQuotes {
			result.WriteString(" ")
		}
	}
	return strings.TrimSpace(result.String())
}

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
