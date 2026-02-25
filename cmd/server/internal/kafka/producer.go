package main

import (
	"context"
	"encoding/json"
	"smartport-go/internal/core/domain"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Configura o Writer (Producer)
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "v1.port.movements",
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	// Simula o envio de um evento de carga
	event := domain.ContainerEvent{
		ContainerID: "CONT-123",
		ShipID:      "SHIP-ABC",
		Status:      "LOADED",
		Timestamp:   time.Now(),
	}

	payload, _ := json.Marshal(event)

	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.ShipID), // Particionamento por Navio
			Value: payload,
		},
	)
	// Tratar erro...
}
