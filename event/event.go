package event

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type OutboxRow struct {
	OutboxID int           `db:"id"`
	Metadata Metadata      `db:"metadata"`
	Payload  []byte        `db:"payload"`
	Status   sql.NullInt64 `db:"status"`
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
	Exchange   string     `json:"exchange"`
	RoutingKey string     `json:"routingKey"`
	Mandatory  bool       `json:"mandatory"`
	Immediate  bool       `json:"immediate"`
	Publishing Publishing `json:"publishing"`
}

type Publishing struct {
	Headers         map[string]interface{} `json:"headers"`
	ContentType     string                 `json:"contentType"`     // MIME content type
	ContentEncoding string                 `json:"contentEncoding"` // MIME content encoding
	DeliveryMode    uint8                  `json:"deliveryMode"`    // Transient (0 or 1) or Persistent (2)
	Priority        uint8                  `json:"priority"`        // 0 to 9
	CorrelationId   string                 `json:"correlationId"`   // correlation identifier
	ReplyTo         string                 `json:"replyTo"`         // address to to reply to (ex: RPC)
	Expiration      string                 `json:"expiration"`      // message expiration spec
	MessageId       string                 `json:"messageId"`       // message identifier
	Timestamp       *time.Time             `json:"timestamp"`       // message timestamp
	Type            string                 `json:"type"`            // message type name
	UserId          string                 `json:"userId"`          // creating user id - ex: "guest"
	AppId           string                 `json:"appId"`           // creating application id
}

type SQSCfg struct {
	*sqs.SendMessageInput
}
