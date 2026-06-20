package manager

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TodayCreatedRecord 今日创建记录
type TodayCreatedRecord struct {
	Date     string            `json:"date"`     // YYYY-MM-DD
	Accounts map[string]bool   `json:"accounts"` // userId -> created
}

var (
	todayCreated     = &TodayCreatedRecord{Accounts: make(map[string]bool)}
	todayCreatedMu   sync.RWMutex
	todayCreatedPath = "data/today_created.json" // 默认值，可通过 SetTodayCreatedPath 覆盖
)

// SetTodayCreatedPath 设置今日创建记录文件路径（必须在使用前调用）
func SetTodayCreatedPath(path string) {
	todayCreatedMu.Lock()
	defer todayCreatedMu.Unlock()
	todayCreatedPath = path
}

// LoadTodayCreated 加载今日创建记录
func LoadTodayCreated() {
	loadTodayCreated()
}

func loadTodayCreated() {
	data, err := os.ReadFile(todayCreatedPath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("加载 todayCreated 失败", "error", err)
		}
		return
	}

	todayCreatedMu.Lock()
	defer todayCreatedMu.Unlock()

	if err := json.Unmarshal(data, todayCreated); err != nil {
		slog.Warn("解析 todayCreated 失败", "error", err)
		return
	}

	// 检查是否是今天
	today := time.Now().Format("2006-01-02")
	if todayCreated.Date != today {
		todayCreated.Date = today
		todayCreated.Accounts = make(map[string]bool)
		slog.Info("todayCreated 日期不匹配，重置", "stored", todayCreated.Date, "today", today)
	}

	slog.Info("加载 todayCreated", "date", todayCreated.Date, "count", len(todayCreated.Accounts))
}

func saveTodayCreated() {
	todayCreatedMu.RLock()
	data, err := json.MarshalIndent(todayCreated, "", "  ")
	todayCreatedMu.RUnlock()

	if err != nil {
		slog.Error("序列化 todayCreated 失败", "error", err)
		return
	}

	os.MkdirAll(filepath.Dir(todayCreatedPath), 0755)
	if err := os.WriteFile(todayCreatedPath, data, 0644); err != nil {
		slog.Error("保存 todayCreated 失败", "error", err)
	}
}

func IsTodayCreated(userID string) bool {
	todayCreatedMu.RLock()
	defer todayCreatedMu.RUnlock()

	today := time.Now().Format("2006-01-02")
	if todayCreated.Date != today {
		return false
	}
	return todayCreated.Accounts[userID]
}

func MarkTodayCreated(userID string) {
	todayCreatedMu.Lock()
	today := time.Now().Format("2006-01-02")
	if todayCreated.Date != today {
		todayCreated.Date = today
		todayCreated.Accounts = make(map[string]bool)
	}
	todayCreated.Accounts[userID] = true
	todayCreatedMu.Unlock()

	saveTodayCreated()
}

func ClearTodayCreated(userID string) {
	todayCreatedMu.Lock()
	delete(todayCreated.Accounts, userID)
	todayCreatedMu.Unlock()

	saveTodayCreated()
}

func GetTodayCreatedCount() int {
	todayCreatedMu.RLock()
	defer todayCreatedMu.RUnlock()

	today := time.Now().Format("2006-01-02")
	if todayCreated.Date != today {
		return 0
	}
	return len(todayCreated.Accounts)
}
