package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"smartport-go/internal/core/domain"
	"smartport-go/internal/core/services"

	"github.com/segmentio/kafka-go"
)

type ContainerConsumer struct {
	reader  *kafka.Reader
	service *services.PortService
}

func NewContainerConsumer(brokers []string, service *services.PortService) *ContainerConsumer {
	return &ContainerConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    "v1.port.movements",
			GroupID:  "tos-processor-group",
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		service: service,
	}
}

func (c *ContainerConsumer) Start(ctx context.Context) {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("Erro ao ler mensagem: %v\n", err)
			break
		}

		var event domain.ContainerEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			continue
		}

		// AQUI CONECTA COM A LÓGICA DE NEGÓCIO (SERVICE -> DOMAIN)
		err = c.service.ProcessContainerMovement(event)
		if err != nil {
			fmt.Printf("Erro ao processar carga: %v\n", err)
		}
	}
}
