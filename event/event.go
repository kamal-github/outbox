package event

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type OutboxRow struct {
	OutboxID int
	Metadata Metadata
	Payload  []byte
	Status   sql.NullInt64
}

type Metadata struct {
	RabbitCfg *RabbitCfg `json:"rabbitCfg,omitempty"`
	SQSCfg    *SQSCfg    `json:"sqsCfg,omitempty"`
}

func (m *Metadata) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("Metadata.Scan: invalid datatype for column metadata")
	}

	return json.Unmarshal(b, m)
}

func (m *Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

type RabbitCfg struct {
	Exchange   string          `json:"exchange"`
	RoutingKey string          `json:"routingKey"`
	Mandatory  bool            `json:"mandatory"`
	Immediate  bool            `json:"immediate"`
	Publishing amqp.Publishing `json:"publishing"`
}

type SQSCfg struct {
	*sqs.SendMessageInput
}
