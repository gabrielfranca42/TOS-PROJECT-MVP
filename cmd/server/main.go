package main

import (
	"context"
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"

	// MANTER APENAS O CAMINHO CORRETO. Exemplo:
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/handlers/repository"
)

func main() {
	ctx := context.Background()

	fmt.Println("Iniciando TOS-PROJECT-MVP...")

	dsn := "host=localhost user=user password=password dbname=smartport port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("falha de infraestrutura: impossível conectar ao banco de dados alerta de panico de gorilla:" + err.Error())

	}

	// 1. Inicializar o repositório (Requer a sua implementação real de banco de dados ou mock)
	// Exemplo: repo := repositories.NewShipRepository(dbConnection)
	repo := repository.NewPostgresRepository(db)

	// 2. Inicializar o serviço de domínio utilizando o construtor correto
	portService := services.NewPortService(repo)

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
