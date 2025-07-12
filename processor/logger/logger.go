package logger

import (
	"go.uber.org/zap/zapcore"
	"log"

	"go.uber.org/zap"
)

func NewLog(level zapcore.Level, isDevel bool) {
	var l *zap.Logger
	var opts []zap.Option
	var config zap.Config
	if isDevel {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	opts = append(opts, zap.AddCallerSkip(1))
	config.Level.SetLevel(level)
	var err error
	if l, err = config.Build(opts...); err != nil {
		log.Fatal("failed initializing zap global logger", err)
	}
	zap.ReplaceGlobals(l)
}

func Debug(msg string, fields ...zap.Field) {
	zap.L().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	zap.L().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	zap.L().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	zap.L().Error(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	zap.L().Panic(msg, fields...)
}

func DPanic(msg string, fields ...zap.Field) {
	zap.L().DPanic(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	zap.L().Fatal(msg, fields...)
}

func Sync() error {
	return zap.L().Sync()
}
