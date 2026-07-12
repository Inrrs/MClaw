package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mclaw/internal/api"
	"mclaw/internal/auth"
	"mclaw/internal/config"
	"mclaw/internal/gateway"
	"mclaw/internal/logger"
	"mclaw/internal/manager"
	"mclaw/internal/proxy"
	"mclaw/internal/webui"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	configPath := flag.String("config", "data/config.json", "配置文件路径")
	logDir := flag.String("log-dir", "logs", "日志目录")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("加载配置失败: %v\n", err)
		os.Exit(1)
	}

	log, err := logger.New(*logDir, slog.LevelInfo)
	if err != nil {
		fmt.Printf("初始化日志失败: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	go logger.CleanupOldLogs(*logDir, 7)

	// 加载模型映射
	api.LoadModelMapping(cfg.ModelMappingPath())

	// 初始化鉴权
	authMgr := auth.New(cfg.Auth.APIKey, cfg.Auth.WebUIUser, cfg.Auth.WebUIPass, "")

	proxyMgr := proxy.NewManager(proxy.Pool{
		URL:      cfg.Proxy.PoolURL,
		Protocol: cfg.Proxy.Protocol,
		Interval: cfg.Proxy.Interval,
	}, func(newURL string) {
		cfg.Proxy.PoolURL = newURL
		if err := cfg.Save(*configPath); err != nil {
			slog.Error("保存代理配置失败", "error", err)
		} else {
			slog.Info("代理池 URL 已保存", "url", newURL)
		}
	})

	poolCfg := gateway.Config{
		StreamKeepaliveSec:    60,
		StreamChunkTimeoutSec: 600,
		StaleQueueTTLSec:      600,
		Node401CooldownSec:    900,
	}

	nodePool := gateway.NewNodePoolWithConfig(poolCfg, cfg.ModelsFile())

	// 配置 manager 文件路径
	manager.SetStatePath(cfg.ManagerStatePath())
	manager.SetTodayCreatedPath(cfg.TodayCreatedPath())
	manager.LoadTodayCreated()

	accountMgr := manager.NewAccountManager(nodePool, proxyMgr, cfg.Gateway.ExternalURL, cfg.Gateway.BaseURL, cfg.Auth.APIKey)

	nodePool.SetOnNodeDown(func(nodeID string) {
		slog.Info("节点下线，等待 bridge 自动重连", "node", nodeID)
		accountMgr.TriggerAccountRebuildWithGrace(nodeID, 60*time.Second)
	})

	// 启动僵尸请求清理
	nodePool.StartStaleSweeper()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Heartbeat("/ping"))

	// WebSocket
	r.Get("/ws", gateway.HandleWebSocket(nodePool, cfg.Auth.APIKey))

	// API (OpenAI 兼容)
	r.Route("/v1", func(r chi.Router) {
		if cfg.Auth.APIKey != "" {
			r.Use(api.AuthMiddleware(cfg.Auth.APIKey))
		}
		r.Post("/chat/completions", api.HandleChatCompletions(nodePool))
		r.Post("/responses", api.HandleResponses(nodePool))
		r.Post("/messages", api.HandleMessages(nodePool))
		r.Get("/models", api.HandleModels(nodePool))
	})

	// API (Anthropic 兼容)
	r.Route("/anthropic/v1", func(r chi.Router) {
		if cfg.Auth.APIKey != "" {
			r.Use(api.AuthMiddleware(cfg.Auth.APIKey))
		}
		r.Post("/messages", api.HandleMessages(nodePool))
	})

	// 模型映射
	modelMappingPath := cfg.ModelMappingPath()
	r.Get("/api/model_mapping", api.HandleModelMappingGet())

	// 需要认证的管理操作
	r.Group(func(r chi.Router) {
		if cfg.Auth.APIKey != "" {
			r.Use(api.AuthMiddleware(cfg.Auth.APIKey))
		}
		r.Put("/api/model_mapping", api.HandleModelMappingPut(modelMappingPath))
		r.Delete("/api/model_mapping", api.HandleModelMappingDelete(modelMappingPath))
		r.Post("/api/rebuild_current", func(w http.ResponseWriter, r *http.Request) {
			statuses := accountMgr.GetStatus()
			for _, s := range statuses {
				if s.IsCurrent {
					accountMgr.TriggerAccountRebuild(s.Account.UserID)
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]bool{"ok": true})
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "没有当前账号"})
		})
	})

	// 只读状态端点（公开）
	r.Get("/api/nodes", api.HandleNodesStatus(nodePool))
	r.Get("/api/models", api.HandleAvailableModels(nodePool))

	// WebUI
	webuiHandler := webui.NewHandler(accountMgr, proxyMgr, authMgr, nodePool)
	webuiHandler.RegisterRoutes(r)

	slog.Info("MClaw 启动",
		"port", cfg.Server.Port,
		"apiKey", maskKey(cfg.Auth.APIKey),
		"proxy", cfg.Proxy.PoolURL != "",
	)

	go proxyMgr.Start()
	go accountMgr.Start()

	server := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: r,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("服务器错误", "error", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("正在关闭...")
	proxyMgr.Stop()
	nodePool.Stop()
	accountMgr.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("服务器关闭失败", "error", err)
	} else {
		slog.Info("服务器已关闭")
	}
}

func maskKey(key string) string {
	if key == "" {
		return "(无鉴权)"
	}
	if len(key) <= 8 {
		return "****"
	}
	return key[:4] + "****" + key[len(key)-4:]
}

func init() {
	fmt.Println(`
  __  __            _
 |  \/  | __ _  ___| | _____
 | |\/| |/ _' |/ __| |/ / _ \
 | |  | | (_| | (__|   <  __/
 |_|  |_|\__,_|\___|_|\_\___|

  MIMO Protocol Gateway v0.1`)
}
