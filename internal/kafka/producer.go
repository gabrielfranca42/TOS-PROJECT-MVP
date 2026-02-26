package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
)

// ProduceContainerEvent deve ser exportada (letra maiúscula) e sem func main
func ProduceContainerEvent(ctx context.Context, event domain.ContainerEvent) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "v1.port.movements",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(event.ShipID),
			Value: payload,
		},
	)
}
