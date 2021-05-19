package event

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// OutboxRow represents the outbox rows in DB
type OutboxRow struct {
	OutboxID int
	Metadata Metadata
	Payload  []byte
	Status   sql.NullInt64
}

// Metadata defines the various configuration for Messaging system.
type Metadata struct {
	RabbitCfg *RabbitCfg `json:"rabbitCfg,omitempty"`
	SQSCfg    *SQSCfg    `json:"sqsCfg,omitempty"`
}

// Scan overrides the behaviour to parse Metadata.
func (m *Metadata) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("Metadata.Scan: invalid datatype for column metadata")
	}

	return json.Unmarshal(b, m)
}

// Value overrides the behaviour to covert Metadata to driver.Value.
func (m *Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

// RabbitCfg defines all configuration required to publish to given Exchange.
type RabbitCfg struct {
	Exchange   string          `json:"exchange"`
	RoutingKey string          `json:"routingKey"`
	Mandatory  bool            `json:"mandatory"`
	Immediate  bool            `json:"immediate"`
	Publishing amqp.Publishing `json:"publishing"`
}

// SQSCfg defines all configuration required to publish to SQS queue.
type SQSCfg struct {
	*sqs.SendMessageInput
}
