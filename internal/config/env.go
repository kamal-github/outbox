package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// ENV maps the Env vars.
type ENV struct {
	PostgresURI  string        `envconfig:"PG_URI"`
	MySQLURI     string        `envconfig:"MYSQL_URI"`
	RabbitMQURI  string        `envconfig:"RABBITMQ_URI"`
	SQSURI       string        `envconfig:"SQS_URI"`
	MineInterval time.Duration `envconfig:"MINE_INTERVAL"`
}

// Process loads Environment vars into ENV.
func Process() (ENV, error) {
	var e ENV
	if err := envconfig.Process("", &e); err != nil {
		return ENV{}, err
	}

	return e, nil
}
