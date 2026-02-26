package main

import (
	"context"
	"time"

	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"
)

func main() {
	for {
		event := domain.ContainerEvent{
			ContainerID: "CONT-IOT-001",
			ShipID:      "SHIP-123",
			Status:      "LOADED",
			Timestamp:   time.Now(),
		}

		kafka.ProduceContainerEvent(context.Background(), event)
		time.Sleep(5 * time.Second)
	}
}
