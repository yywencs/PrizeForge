package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

// Config 定义配置结构体
type Config struct {
	Filename   string
	MaxSize    int // MB
	MaxBackups int
	MaxAge     int    // Days
	Level      string // "debug", "info", "error"
}

// Init 初始化全局 Logger
func Init(cfg Config) {
	// 1. 配置日志切割 (lumberjack)
	writeSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		Compress:   true, // 默认开启压缩
	})

	// 2. 配置 Encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder   // 时间格式: 2023-01-01T00:00:00.000Z0700
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // 日志级别大写: INFO, ERROR

	// JSON 格式
	encoder := zapcore.NewJSONEncoder(encoderConfig)

	// 3. 配置 Core
	core := zapcore.NewCore(
		encoder,
		writeSyncer,
		parseLevel(cfg.Level),
	)

	// 4. 创建 Logger
	// AddCaller: 添加调用者信息 (文件名和行号)
	Log = zap.New(core, zap.AddCaller())

	// 5. 替换全局 logger (可选)
	zap.ReplaceGlobals(Log)
}

func parseLevel(lvl string) zapcore.Level {
	switch lvl {
	case "debug":
		return zap.DebugLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}
