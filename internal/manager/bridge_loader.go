package manager

import (
	_ "embed"
	"encoding/base64"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

//go:embed bridge_fallback.py
var embeddedBridgeCode string

// LoadBridgeCode 加载 bridge 脚本代码
//
// 优先级（从高到低）：
//  1. 环境变量 MCLAW_BRIDGE_SCRIPT 指定的文件路径
//  2. ~/.openclaw/skills/mclaw-bridge/bridge.py（从 MClaw-skill 仓库安装）
//  3. 可执行文件同目录下的 scripts/bridge.py
//  4. 内置 fallback 版本（go:embed）
func LoadBridgeCode() string {
	// 1. 环境变量指定
	if envPath := os.Getenv("MCLAW_BRIDGE_SCRIPT"); envPath != "" {
		if code, err := os.ReadFile(envPath); err == nil {
			slog.Info("从环境变量指定路径加载 bridge 脚本", "path", envPath)
			return string(code)
		}
		slog.Warn("环境变量指定的 bridge 脚本路径不可用，回退", "path", envPath)
	}

	// 2. ~/.openclaw/skills/mclaw-bridge/bridge.py
	if home, err := os.UserHomeDir(); err == nil {
		skillPath := filepath.Join(home, ".openclaw", "skills", "mclaw-bridge", "bridge.py")
		if code, err := os.ReadFile(skillPath); err == nil {
			slog.Warn("从外部路径加载 bridge 脚本（覆盖内置 fallback）", "path", skillPath, "priority", 2)
			return string(code)
		}
	}

	// 3. 可执行文件同目录 scripts/bridge.py
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		scriptPath := filepath.Join(exeDir, "scripts", "bridge.py")
		if code, err := os.ReadFile(scriptPath); err == nil {
			slog.Warn("从外部路径加载 bridge 脚本（覆盖内置 fallback）", "path", scriptPath, "priority", 3)
			return string(code)
		}
	}

	// 4. 内置 fallback
	slog.Info("使用内置 bridge 脚本 (go:embed)")
	return embeddedBridgeCode
}

// PrepareBridgeCode 准备最终注入的 bridge 代码
// 对齐 mimi3：把 __WS_URL__ 明文替换为网关地址（容器可直接 connect）
func PrepareBridgeCode(gatewayURL string) string {
	code := LoadBridgeCode()

	// 1) 首选：mimi3 / 当前 scripts/bridge.py 风格
	if strings.Contains(code, "__WS_URL__") {
		code = strings.ReplaceAll(code, "__WS_URL__", gatewayURL)
		slog.Info("bridge WS_URL 已注入", "gateway", gatewayURL, "placeholder", "__WS_URL__")
		return code
	}

	// 2) 兼容旧 base64 占位符
	encoded := base64.StdEncoding.EncodeToString([]byte(gatewayURL))
	if strings.Contains(code, "__WS_URL_B64__") {
		code = strings.Replace(code, "__WS_URL_B64__", encoded, 1)
		slog.Info("bridge WS_URL 已注入(base64)", "gateway", gatewayURL, "placeholder", "__WS_URL_B64__")
		return code
	}
	if strings.Contains(code, `WS_URL_B64 = "%s"`) {
		code = strings.Replace(code, `WS_URL_B64 = "%s"`, `WS_URL_B64 = "`+encoded+`"`, 1)
		slog.Info("bridge WS_URL 已注入(base64)", "gateway", gatewayURL, "placeholder", `WS_URL_B64="%s"`)
		return code
	}

	// 3) 兜底：仅当代码里确实是 WS_URL_B64 赋值时才替换第一个 %s
	if strings.Contains(code, "WS_URL_B64") && strings.Contains(code, "%s") {
		slog.Warn("bridge 使用兜底 %s 替换，请检查占位符格式")
		code = strings.Replace(code, "%s", encoded, 1)
		return code
	}

	slog.Error("bridge 代码中未找到 WS_URL 占位符，节点将无法连接网关")
	return code
}
