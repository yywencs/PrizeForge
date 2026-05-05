package logger

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
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
	Compress   bool
}

// Init 初始化全局 Logger
func Init(cfg Config) {
	// 1. 配置日志切割 (lumberjack)
	writeSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		Compress:   cfg.Compress,
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

// KratosLogger 适配器
type KratosLogger struct {
	log *zap.Logger
}

func NewKratosLogger(z *zap.Logger) log.Logger {
	return &KratosLogger{log: z}
}

func (l *KratosLogger) Log(level log.Level, keyvals ...interface{}) error {
	if len(keyvals) == 0 || len(keyvals)%2 != 0 {
		l.log.Warn(fmt.Sprint("Keyvalues must appear in pairs: ", keyvals))
		return nil
	}

	var data []zap.Field
	for i := 0; i < len(keyvals); i += 2 {
		data = append(data, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
	}

	switch level {
	case log.LevelDebug:
		l.log.Debug("", data...)
	case log.LevelInfo:
		l.log.Info("", data...)
	case log.LevelWarn:
		l.log.Warn("", data...)
	case log.LevelError:
		l.log.Error("", data...)
	case log.LevelFatal:
		l.log.Fatal("", data...)
	}
	return nil
}

// ---------------- Helper functions to replace slog ----------------

func Info(msg string, args ...interface{}) {
	Log.Sugar().Infow(msg, args...)
}

func Warn(msg string, args ...interface{}) {
	Log.Sugar().Warnw(msg, args...)
}

func Error(msg string, args ...interface{}) {
	Log.Sugar().Errorw(msg, args...)
}

func Debug(msg string, args ...interface{}) {
	Log.Sugar().Debugw(msg, args...)
}

func InfoContext(ctx interface{}, msg string, args ...interface{}) {
	Log.Sugar().Infow(msg, args...)
}

func WarnContext(ctx interface{}, msg string, args ...interface{}) {
	Log.Sugar().Warnw(msg, args...)
}

func ErrorContext(ctx interface{}, msg string, args ...interface{}) {
	Log.Sugar().Errorw(msg, args...)
}
