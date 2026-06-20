package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	Server struct {
		Port string `json:"port"`
		Host string `json:"host"`
	} `json:"server"`

	Gateway struct {
		ExternalURL string `json:"external_url"` // 外部访问地址，如 ws://svip.asia:9100
		BaseURL     string `json:"base_url"`      // OpenClaw 平台地址，默认 https://aistudio.xiaomimimo.com
	} `json:"gateway"`

	Auth struct {
		APIKey      string `json:"api_key"`
		WebUIUser   string `json:"webui_user"`
		WebUIPass   string `json:"webui_password"`
	} `json:"auth"`

	Proxy struct {
		PoolURL  string `json:"pool_url"`
		Protocol string `json:"protocol"`
		Interval int    `json:"interval"`
	} `json:"proxy"`

	Manager struct {
		SwitchBeforeMin int `json:"switch_before_min"` // 切换提前时间(分钟)
		CheckInterval   int `json:"check_interval"`    // 检查间隔(秒)
	} `json:"manager"`

	DataDir string `json:"data_dir"` // 数据目录，默认 "data"
}

// ModelMappingPath 模型映射文件路径
func (c *Config) ModelMappingPath() string {
	return filepath.Join(c.dataDir(), "model_mapping.json")
}

// ManagerStatePath 管理器状态文件路径
func (c *Config) ManagerStatePath() string {
	return filepath.Join(c.dataDir(), "manager_state.json")
}

// ModelsFile 模型列表缓存文件路径
func (c *Config) ModelsFile() string {
	return filepath.Join(c.dataDir(), "models.json")
}

// TodayCreatedPath 今日创建记录文件路径
func (c *Config) TodayCreatedPath() string {
	return filepath.Join(c.dataDir(), "today_created.json")
}

// DBPath SQLite 数据库文件路径
func (c *Config) DBPath() string {
	return filepath.Join(c.dataDir(), "mclaw.db")
}

func (c *Config) dataDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	return "data"
}

func Load(path string) (*Config, error) {
	cfg := &Config{}

	// 默认值
	cfg.Server.Port = "8900"
	cfg.Auth.WebUIUser = "admin"
	cfg.Manager.SwitchBeforeMin = 5
	cfg.Manager.CheckInterval = 30

	// 从文件加载
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		}
	}

	// 环境变量覆盖
	if v := os.Getenv("SERVER_PORT"); v != "" {
		cfg.Server.Port = v
	}
	if v := os.Getenv("MIMO_RELAY_OPENAI_KEY"); v != "" {
		cfg.Auth.APIKey = v
	}
	if v := os.Getenv("MIMO_WEBUI_USERNAME"); v != "" {
		cfg.Auth.WebUIUser = v
	}
	if v := os.Getenv("MIMO_WEBUI_PASSWORD"); v != "" {
		cfg.Auth.WebUIPass = v
	}
	if v := os.Getenv("PROXY_POOL_URL"); v != "" {
		cfg.Proxy.PoolURL = v
	}
	if v := os.Getenv("PROXY_PROTOCOL"); v != "" {
		cfg.Proxy.Protocol = v
	}
	if v := os.Getenv("GATEWAY_EXTERNAL_URL"); v != "" {
		cfg.Gateway.ExternalURL = v
	}

	return cfg, nil
}

func (c *Config) Save(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
