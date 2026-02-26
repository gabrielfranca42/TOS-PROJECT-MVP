package main

import (
	"context"
	"fmt"
	"time"

	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"
)

func main() {
	ctx := context.Background()

	fmt.Println("Iniciando TOS-PROJECT-MVP...")

	// 1. Inicializar o serviço de domínio
	// (Se o seu PortService tiver um construtor como NewPortService, use-o aqui)
	portService := &services.PortService{}

	// 2. Inicializar o consumidor do Kafka
	brokers := []string{"localhost:9092"}
	consumer := kafka.NewContainerConsumer(brokers, portService)

	// 3. Rodar o consumidor em uma Goroutine (em segundo plano)
	go func() {
		fmt.Println("Consumidor do Kafka iniciado e escutando o tópico...")
		consumer.Start(ctx)
	}()

	// Pequeno delay apenas para garantir que o consumidor subiu antes de produzir
	time.Sleep(2 * time.Second)

	// 4. Loop infinito do Produtor simulando eventos IoT
	fmt.Println("Iniciando a simulação de produção de eventos...")
	for {
		event := domain.ContainerEvent{
			ContainerID: "CONT-IOT-001",
			ShipID:      "SHIP-123",
			Status:      "LOADED",
			Timestamp:   time.Now(),
		}

		// Chama a função exportada do seu pacote kafka
		err := kafka.ProduceContainerEvent(ctx, event)
		if err != nil {
			fmt.Printf("Erro ao produzir evento no Kafka: %v\n", err)
		} else {
			fmt.Printf("Evento produzido: Container %s carregado no Navio %s\n", event.ContainerID, event.ShipID)
		}

		// Aguarda 5 segundos até o próximo evento
		time.Sleep(5 * time.Second)
	}
}
