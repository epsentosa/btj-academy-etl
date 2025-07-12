package main

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

type Environment int8

const (
	Development Environment = iota
	Staging
	Production
)

func (e Environment) String() string {
	switch e {
	case Development:
		return "development"
	case Staging:
		return "staging"
	case Production:
		return "production"
	default:
		return fmt.Sprintf("Env(%d)", e)
	}
}

func (e *Environment) UnmarshalText(text []byte) error {
	if e == nil {
		return errors.New("nil env")
	}
	str := string(text)
	switch strings.ToLower(str) {
	case "development", "devel", "dev":
		*e = Development
	case "staging", "stag":
		*e = Staging
	case "production", "prod":
		*e = Production
	default:
		return errors.Errorf("unknown environment %s", str)
	}
	return nil
}

type config struct {
	DBUrl       string        `env:"DB_URL,notEmpty"`
	RedisHost   string        `env:"REDIS_PM_HOST" envDefault:"redis"`
	RedisDB     string        `env:"REDIS_PM_DB" envDefault:"6"`
	LogLevel    zapcore.Level `env:"LOG_LEVEL" envDefault:"INFO"`
	Environment Environment   `env:"ENVIRONMENT" envDefault:"development"`
}
