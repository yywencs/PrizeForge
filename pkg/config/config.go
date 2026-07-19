package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

var Conf *Config

func InitViperConfig() {
	v := viper.New()

	v.SetConfigName("config") // 文件名 config.yaml
	v.SetConfigType("yaml")

	v.AddConfigPath("configs")
	v.AddConfigPath(".")
	v.SetEnvPrefix("PRIZEFORGE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		panic(fmt.Errorf("decode config: %w", err))
	}
	Conf = &cfg
}
