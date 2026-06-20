package logger

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

type Logger struct {
	*slog.Logger
	file *os.File
}

func New(logDir string, level slog.Level) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	// 日志文件名按日期
	logFile := filepath.Join(logDir, time.Now().Format("2006-01-02")+".log")
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// 同时输出到控制台和文件
	multiWriter := io.MultiWriter(os.Stdout, f)

	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
	})

	logger := &Logger{
		Logger: slog.New(handler),
		file:   f,
	}

	slog.SetDefault(logger.Logger)
	return logger, nil
}

func (l *Logger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

// CleanupOldLogs 清理旧日志
func CleanupOldLogs(logDir string, keepDays int) error {
	if keepDays <= 0 {
		return nil
}

	cutoff := time.Now().AddDate(0, 0, -keepDays)

	entries, err := os.ReadDir(logDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// 解析日期文件名
		name := entry.Name()
		if len(name) < 10 || name[len(name)-4:] != ".log" {
			continue
		}

		dateStr := name[:10]
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		if date.Before(cutoff) {
			os.Remove(filepath.Join(logDir, name))
		}
	}

	return nil
}
