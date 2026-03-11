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
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/handlers/repository"
)

func main() {
    ctx := context.Background()

    fmt.Println("Iniciando TOS-PROJECT-MVP...")

    dsn := "host=127.0.0.1 user=user password=password dbname=smartport port=5433 sslmode=disable"
    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
    if err != nil {
        panic("falha de infraestrutura: impossível conectar ao banco de dados alerta de panico de gorilla:" + err.Error())
    }

    // --- MODIFICAÇÃO 1: Mapeamento Objeto-Relacional (DDL) ---
    err = db.AutoMigrate(&domain.Ship{})
    if err != nil {
        panic("falha de infraestrutura: impossível executar AutoMigrate no banco de dados: " + err.Error())
    }

    // --- MODIFICAÇÃO 2: Injeção de Estado Inicial (Seed) ---
    seedShip := domain.Ship{
        ID:            "SHIP-123",
        Name:          "Navio de Teste IOT",
        TotalCapacity: 100,
        LoadedCount:   0,
        Status:        "DOCKING",
    }
    // Garante a existência do registro base de forma idempotente
    db.FirstOrCreate(&seedShip, domain.Ship{ID: "SHIP-123"})

    // 1. Inicializar o repositório
    repo := repository.NewPostgresRepository(db)

    // 2. Inicializar o serviço de domínio utilizando o construtor correto
    portService := services.NewPortService(repo)

    // 3. Inicializar o consumidor do Kafka
    brokers := []string{"localhost:9092"}
    consumer := kafka.NewContainerConsumer(brokers, portService)

    // 4. Rodar o consumidor em uma Goroutine (em segundo plano)
    go func() {
        fmt.Println("Consumidor do Kafka iniciado e escutando o tópico...")
        consumer.Start(ctx)
    }()

    // Delay para garantia de subida do consumidor
    time.Sleep(2 * time.Second)

    // 5. Loop infinito do Produtor simulando eventos IoT
    fmt.Println("Iniciando a simulação de produção de eventos...")
    for {
        event := domain.ContainerEvent{
            ContainerID: "CONT-IOT-001",
            ShipID:      "SHIP-123",
            Status:      "LOADED",
            Timestamp:   time.Now(),
        }

        err := kafka.ProduceContainerEvent(ctx, event)
        if err != nil {
            fmt.Printf("Erro ao produzir evento no Kafka: %v\n", err)
        } else {
            fmt.Printf("Evento produzido: Container %s carregado no Navio %s\n", event.ContainerID, event.ShipID)
        }

        time.Sleep(5 * time.Second)
    }
}