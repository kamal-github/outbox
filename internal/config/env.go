package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type ENV struct {
	DBDriver     string        `envconfig:"DB_DRIVER"`
	DatabaseURL  string        `envconfig:"DB_URL"`
	OutboxTable  string        `envconfig:"OUTBOX_TABLE"`
	Backend      string        `envconfig:"BACKEND"`
	BackendURL   string        `envconfig:"BACKEND_URL"`
	MineInterval time.Duration `envconfig:"MINE_INTERVAL"`
}

func Process() (ENV, error) {
	var e ENV
	if err := envconfig.Process("", &e); err != nil {
		return ENV{}, err
	}

	return e, nil
}
