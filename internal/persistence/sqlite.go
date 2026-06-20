package persistence

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct {
	db     *sql.DB
	stopCh chan struct{}
}

func NewDB(dbPath string) (*DB, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	d := &DB{db: db, stopCh: make(chan struct{})}
	if err := d.init(); err != nil {
		return nil, err
	}

	slog.Info("SQLite 数据库初始化", "path", dbPath)
	return d, nil
}

func (d *DB) init() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS status_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			user_id TEXT,
			status TEXT,
			remain_sec INTEGER,
			notes TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS request_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			method TEXT,
			path TEXT,
			model TEXT,
			status_code INTEGER,
			tokens_in INTEGER,
			tokens_out INTEGER,
			duration_ms INTEGER,
			node_id TEXT,
			client_ip TEXT,
			error TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS token_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			model TEXT,
			tokens_in INTEGER,
			tokens_out INTEGER,
			total_tokens INTEGER,
			request_count INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS route_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			route TEXT,
			request_count INTEGER,
			error_count INTEGER,
			avg_duration_ms INTEGER,
			p95_duration_ms INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_status_history_timestamp ON status_history(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_token_stats_timestamp ON token_stats(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_route_stats_timestamp ON route_stats(timestamp)`,
	}

	for _, q := range queries {
		if _, err := d.db.Exec(q); err != nil {
			return err
		}
	}

	// 启动数据清理协程
	go d.cleanupLoop()

	return nil
}

// SaveStatusHistory 保存状态历史
func (d *DB) SaveStatusHistory(userID, status string, remainSec int, notes string) error {
	_, err := d.db.Exec(
		"INSERT INTO status_history (user_id, status, remain_sec, notes) VALUES (?, ?, ?, ?)",
		userID, status, remainSec, notes,
	)
	return err
}

// SaveRequestLog 保存请求日志
func (d *DB) SaveRequestLog(method, path, model string, statusCode, tokensIn, tokensOut, durationMs int, nodeID, clientIP, errMsg string) error {
	_, err := d.db.Exec(
		"INSERT INTO request_logs (method, path, model, status_code, tokens_in, tokens_out, duration_ms, node_id, client_ip, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		method, path, model, statusCode, tokensIn, tokensOut, durationMs, nodeID, clientIP, errMsg,
	)
	return err
}

// SaveTokenStats 保存 Token 统计
func (d *DB) SaveTokenStats(model string, tokensIn, tokensOut, totalTokens, requestCount int) error {
	_, err := d.db.Exec(
		"INSERT INTO token_stats (model, tokens_in, tokens_out, total_tokens, request_count) VALUES (?, ?, ?, ?, ?)",
		model, tokensIn, tokensOut, totalTokens, requestCount,
	)
	return err
}

// SaveRouteStats 保存路由统计
func (d *DB) SaveRouteStats(route string, requestCount, errorCount, avgDurationMs, p95DurationMs int) error {
	_, err := d.db.Exec(
		"INSERT INTO route_stats (route, request_count, error_count, avg_duration_ms, p95_duration_ms) VALUES (?, ?, ?, ?, ?)",
		route, requestCount, errorCount, avgDurationMs, p95DurationMs,
	)
	return err
}

// GetStatusHistory 获取状态历史
func (d *DB) GetStatusHistory(userID string, hours int) ([]map[string]any, error) {
	rows, err := d.db.Query(
		"SELECT timestamp, user_id, status, remain_sec, notes FROM status_history WHERE timestamp > datetime('now', ?) ORDER BY timestamp DESC",
		fmt.Sprintf("-%d hours", hours),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var ts, uid, status, notes string
		var remainSec int
		if err := rows.Scan(&ts, &uid, &status, &remainSec, &notes); err != nil {
			continue
		}
		result = append(result, map[string]any{
			"timestamp":  ts,
			"user_id":    uid,
			"status":     status,
			"remain_sec": remainSec,
			"notes":      notes,
		})
	}
	return result, rows.Err()
}

// GetRequestLogs 获取请求日志
func (d *DB) GetRequestLogs(limit, offset int) ([]map[string]any, error) {
	rows, err := d.db.Query(
		"SELECT timestamp, method, path, model, status_code, tokens_in, tokens_out, duration_ms, node_id, client_ip, error FROM request_logs ORDER BY timestamp DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var ts, method, path, model, nodeID, clientIP, errMsg string
		var statusCode, tokensIn, tokensOut, durationMs int
		if err := rows.Scan(&ts, &method, &path, &model, &statusCode, &tokensIn, &tokensOut, &durationMs, &nodeID, &clientIP, &errMsg); err != nil {
			continue
		}
		result = append(result, map[string]any{
			"timestamp":    ts,
			"method":       method,
			"path":         path,
			"model":        model,
			"status_code":  statusCode,
			"tokens_in":    tokensIn,
			"tokens_out":   tokensOut,
			"duration_ms":  durationMs,
			"node_id":      nodeID,
			"client_ip":    clientIP,
			"error":        errMsg,
		})
	}
	return result, rows.Err()
}

// GetTokenStats 获取 Token 统计
func (d *DB) GetTokenStats(hours int) (map[string]any, error) {
	rows, err := d.db.Query(
		"SELECT model, SUM(tokens_in), SUM(tokens_out), SUM(total_tokens), SUM(request_count) FROM token_stats WHERE timestamp > datetime('now', ?) GROUP BY model",
		fmt.Sprintf("-%d hours", hours),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]any)
	for rows.Next() {
		var model string
		var tokensIn, tokensOut, totalTokens, requestCount int
		if err := rows.Scan(&model, &tokensIn, &tokensOut, &totalTokens, &requestCount); err != nil {
			continue
		}
		result[model] = map[string]any{
			"tokens_in":     tokensIn,
			"tokens_out":    tokensOut,
			"total_tokens":  totalTokens,
			"request_count": requestCount,
		}
	}
	return result, rows.Err()
}

// cleanupLoop 定期清理旧数据
func (d *DB) cleanupLoop() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.cleanup()
		case <-d.stopCh:
			return
		}
	}
}

func (d *DB) cleanup() {
	// 保留 90 天数据
	_, err := d.db.Exec("DELETE FROM status_history WHERE timestamp < datetime('now', '-90 days')")
	if err != nil {
		slog.Error("清理 status_history 失败", "error", err)
	}

	_, err = d.db.Exec("DELETE FROM request_logs WHERE timestamp < datetime('now', '-90 days')")
	if err != nil {
		slog.Error("清理 request_logs 失败", "error", err)
	}

	_, err = d.db.Exec("DELETE FROM token_stats WHERE timestamp < datetime('now', '-90 days')")
	if err != nil {
		slog.Error("清理 token_stats 失败", "error", err)
	}

	_, err = d.db.Exec("DELETE FROM route_stats WHERE timestamp < datetime('now', '-90 days')")
	if err != nil {
		slog.Error("清理 route_stats 失败", "error", err)
	}

	slog.Info("数据清理完成")
}

// Close 关闭数据库
func (d *DB) Close() error {
	close(d.stopCh)
	return d.db.Close()
}
