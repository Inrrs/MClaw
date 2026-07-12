package webui

import (
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/go-chi/chi/v5"

	"mclaw/internal/auth"
	"mclaw/internal/gateway"
	"mclaw/internal/manager"
	"mclaw/internal/proxy"
)

type Handler struct {
	manager   *manager.AccountManager
	proxyMgr  *proxy.Manager
	auth      *auth.Auth
	pool      *gateway.NodePool
	indexTmpl *template.Template
}

func NewHandler(mgr *manager.AccountManager, proxyMgr *proxy.Manager, authMgr *auth.Auth, pool *gateway.NodePool) *Handler {
	return &Handler{
		manager:   mgr,
		proxyMgr:  proxyMgr,
		auth:      authMgr,
		pool:      pool,
		indexTmpl: template.Must(template.New("index").Parse(indexHTML)),
	}
}

func (h *Handler) RegisterRoutes(r *chi.Mux) {
	// 公开
	r.Get("/api/status", h.handleStatus)

	// 认证
	r.Get("/api/auth/session", h.auth.HandleSessionCheck)
	r.Post("/api/auth/login", h.auth.HandleLogin)
	r.Post("/api/auth/logout", h.auth.HandleLogout)

	// 需要认证
	r.Get("/", h.auth.WebUIAuthMiddleware(h.handleIndex))
	r.Get("/api/accounts", h.auth.WebUIAuthMiddleware(logRequest(h.handleAccounts)))
	r.Post("/api/accounts/import", h.auth.WebUIAuthMiddleware(h.handleImport))
	r.Post("/api/accounts/delete", h.auth.WebUIAuthMiddleware(h.handleDelete))
	r.Post("/api/accounts/delete-batch", h.auth.WebUIAuthMiddleware(h.handleDeleteBatch))
	r.Post("/api/accounts/rebuild", h.auth.WebUIAuthMiddleware(h.handleRebuild))
	r.Post("/api/accounts/force_inject", h.auth.WebUIAuthMiddleware(h.handleForceInject))
	r.Get("/api/metrics", h.auth.WebUIAuthMiddleware(logRequest(h.handleMetrics)))
	r.Get("/api/account_logs", h.auth.WebUIAuthMiddleware(h.handleAccountLogs))
	r.Get("/api/proxy_stats", h.auth.WebUIAuthMiddleware(h.handleProxyStats))
	r.Post("/api/proxy", h.auth.WebUIAuthMiddleware(h.handleProxyUpdate))
	r.Post("/api/test_account", h.auth.WebUIAuthMiddleware(h.handleTestAccount))
	r.Get("/api/error_logs", h.auth.WebUIAuthMiddleware(h.handleErrorLogs))
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	statuses := h.manager.GetStatus()
	online := 0
	for _, s := range statuses {
		if s.Status == "AVAILABLE" && s.RemainSec > 0 {
			online++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"total": len(statuses), "online": online})
}
