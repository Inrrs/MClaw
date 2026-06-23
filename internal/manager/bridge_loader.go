package manager

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

//go:embed bridge_fallback.py
var embeddedBridgeCode string

// LoadBridgeCode 加载 bridge 脚本代码
//
// 优先级（从高到低）：
//  1. 环境变量 MCLAW_BRIDGE_SCRIPT 指定的文件路径
//  2. 工作目录下的 scripts/bridge.py
//  3. ~/.openclaw/skills/mclaw-bridge/bridge.py（从 MClaw-skill 仓库安装）
//  4. 可执行文件同目录下的 scripts/bridge.py
//  5. 内置 fallback 版本（go:embed）
func LoadBridgeCode() string {
	// 1. 环境变量指定
	if envPath := os.Getenv("MCLAW_BRIDGE_SCRIPT"); envPath != "" {
		if code, err := os.ReadFile(envPath); err == nil {
			slog.Info("从环境变量指定路径加载 bridge 脚本", "path", envPath)
			return string(code)
		}
		slog.Warn("环境变量指定的 bridge 脚本路径不可用，回退", "path", envPath)
	}

	// 2. 工作目录 scripts/bridge.py
	if code, err := os.ReadFile("scripts/bridge.py"); err == nil {
		slog.Info("从 scripts/bridge.py 加载 bridge 脚本")
		return string(code)
	}

	// 3. ~/.openclaw/skills/mclaw-bridge/bridge.py
	if home, err := os.UserHomeDir(); err == nil {
		skillPath := filepath.Join(home, ".openclaw", "skills", "mclaw-bridge", "bridge.py")
		if code, err := os.ReadFile(skillPath); err == nil {
			slog.Info("从 OpenClaw skill 目录加载 bridge 脚本", "path", skillPath)
			return string(code)
		}
	}

	// 4. 可执行文件同目录 scripts/bridge.py
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		scriptPath := filepath.Join(exeDir, "scripts", "bridge.py")
		if code, err := os.ReadFile(scriptPath); err == nil {
			slog.Info("从可执行文件目录加载 bridge 脚本", "path", scriptPath)
			return string(code)
		}
	}

	// 5. 内置 fallback
	slog.Info("使用内置 bridge 脚本 (go:embed)")
	return embeddedBridgeCode
}

// PrepareBridgeCode 准备最终注入的 bridge 代码
// 替换 WS_URL 占位符为实际网关地址
func PrepareBridgeCode(gatewayURL string) string {
	code := LoadBridgeCode()
	// 用 base64 编码网关地址，避免特殊字符问题
	encoded := base64.StdEncoding.EncodeToString([]byte(gatewayURL))
	code = fmt.Sprintf(code, encoded)
	return code
}
