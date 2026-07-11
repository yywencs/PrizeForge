package config

import (
	"fmt"
	"log/slog"

	"github.com/spf13/viper"
)

var Conf *Config

func InitViperConfig() {
	v := viper.New()

	v.SetConfigName("config") // 文件名 config.yaml
	v.SetConfigType("yaml")

	v.AddConfigPath("configs")
	v.AddConfigPath(".")

	// 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	if err := v.Unmarshal(&Conf); err != nil {
		slog.Error("Unable to decode into struct.", "err", err)
	}
}
