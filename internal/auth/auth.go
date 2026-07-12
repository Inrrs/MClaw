package auth

import (
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Auth struct {
	apiKey      string
	webUIUser   string
	webUIPass   string
	secret      string
	sessions    map[string]time.Time
	mu          sync.RWMutex
	rateLimiter map[string]*rateEntry
	rateMu      sync.Mutex
	stopCh      chan struct{}
}

type rateEntry struct {
	count    int
	lastSeen time.Time
}

func New(apiKey, webUIUser, webUIPass, secret string) *Auth {
	if secret == "" {
		secret = generateRandomKey()
	}
	if webUIPass == "" {
		webUIPass = generateRandomKey()[:16]
		slog.Warn("WebUI 密码未配置，自动生成", "password", webUIPass)
	}

	a := &Auth{
		apiKey:      apiKey,
		webUIUser:   webUIUser,
		webUIPass:   webUIPass,
		secret:      secret,
		sessions:    make(map[string]time.Time),
		rateLimiter: make(map[string]*rateEntry),
	}

	a.stopCh = make(chan struct{})
	go a.cleanupSessions()
	return a
}

// APIAuthMiddleware API 鉴权中间件
func (a *Auth) APIAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.apiKey == "" {
			next.ServeHTTP(w, r)
			return
		}

		auth := r.Header.Get("Authorization")
		apiKey := r.Header.Get("X-API-Key")

		valid := false
		if strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == a.apiKey {
			valid = true
		}
		if apiKey == a.apiKey {
			valid = true
		}

		if !valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"message":"Unauthorized","type":"authentication_error"}}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// WebUIAuthMiddleware WebUI 鉴权中间件
func (a *Auth) WebUIAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("mclaw_session")
		if err == nil && a.validateSession(cookie.Value) {
			next(w, r)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"Unauthorized"}`))
			return
		}

		a.handleLoginPage(w, r)
	}
}

func (a *Auth) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(loginHTML))
}

func (a *Auth) HandleLogin(w http.ResponseWriter, r *http.Request) {
	// 速率限制：每 IP 每分钟最多 5 次尝试
	ip := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		ip = strings.Split(fwd, ",")[0]
	}
	if !a.checkRateLimit(ip) {
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	r.ParseForm()
	user := r.FormValue("username")
	pass := r.FormValue("password")

	if user != a.webUIUser || subtle.ConstantTimeCompare([]byte(pass), []byte(a.webUIPass)) != 1 {
		a.recordAttempt(ip)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// 登录成功，清除该 IP 的限制
	a.clearRateLimit(ip)

	sessionID := a.createSession()
	http.SetCookie(w, &http.Cookie{
		Name:     "mclaw_session",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Now().Add(24 * time.Hour),
	})

	http.Redirect(w, r, "/", http.StatusFound)
}

func (a *Auth) HandleLogout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("mclaw_session")
	if err == nil {
		a.mu.Lock()
		delete(a.sessions, cookie.Value)
		a.mu.Unlock()
	}

	http.SetCookie(w, &http.Cookie{
		Name:   "mclaw_session",
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (a *Auth) HandleSessionCheck(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("mclaw_session")
	valid := err == nil && a.validateSession(cookie.Value)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"authenticated": valid})
}

func (a *Auth) createSession() string {
	b := make([]byte, 32)
	rand.Read(b)
	sessionID := hex.EncodeToString(b)

	a.mu.Lock()
	a.sessions[sessionID] = time.Now().Add(24 * time.Hour)
	a.mu.Unlock()

	return sessionID
}

func (a *Auth) validateSession(sessionID string) bool {
	a.mu.RLock()
	expiry, ok := a.sessions[sessionID]
	a.mu.RUnlock()

	return ok && time.Now().Before(expiry)
}

// Stop 停止 auth 清理 goroutine
func (a *Auth) Stop() {
	if a.stopCh != nil {
		close(a.stopCh)
	}
}

func (a *Auth) cleanupSessions() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 清理过期 session
			a.mu.Lock()
			for sessionID, expiry := range a.sessions {
				if time.Now().After(expiry) {
					delete(a.sessions, sessionID)
				}
			}
			a.mu.Unlock()
			// 清理过期 rate limiter 条目
			a.rateMu.Lock()
			for ip, entry := range a.rateLimiter {
				if time.Since(entry.lastSeen) > 5*time.Minute {
					delete(a.rateLimiter, ip)
				}
			}
			a.rateMu.Unlock()
		case <-a.stopCh:
			return
		}
	}
}

func generateRandomKey() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// 速率限制：每 IP 每分钟最多 5 次
const maxAttemptsPerMinute = 5

func (a *Auth) checkRateLimit(ip string) bool {
	a.rateMu.Lock()
	defer a.rateMu.Unlock()

	entry, ok := a.rateLimiter[ip]
	if !ok {
		return true
	}
	// 超过 1 分钟重置
	if time.Since(entry.lastSeen) > time.Minute {
		delete(a.rateLimiter, ip)
		return true
	}
	return entry.count < maxAttemptsPerMinute
}

func (a *Auth) recordAttempt(ip string) {
	a.rateMu.Lock()
	defer a.rateMu.Unlock()

	entry, ok := a.rateLimiter[ip]
	if !ok || time.Since(entry.lastSeen) > time.Minute {
		a.rateLimiter[ip] = &rateEntry{count: 1, lastSeen: time.Now()}
	} else {
		entry.count++
		entry.lastSeen = time.Now()
	}
}

func (a *Auth) clearRateLimit(ip string) {
	a.rateMu.Lock()
	defer a.rateMu.Unlock()
	delete(a.rateLimiter, ip)
}

//go:embed static/login.html
var loginHTML string
