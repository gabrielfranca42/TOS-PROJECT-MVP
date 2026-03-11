package main

import (
    "context"
    "fmt"
    "time"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"

    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/handlers/repository"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"
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
    // Adicionada a entidade ShipTelemetry para criar a tabela de séries temporais
    err = db.AutoMigrate(&domain.Ship{}, &domain.ShipTelemetry{})
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
    // Garante a existência do registro base de forma idempotente para chave estrangeira
    db.FirstOrCreate(&seedShip, domain.Ship{ID: "SHIP-123"})

    // 1. Inicializar o repositório
    repo := repository.NewPostgresRepository(db)

    // 2. Inicializar o serviço de domínio
    portService := services.NewPortService(repo)

    // 3. Inicializar o consumidor do Kafka
    brokers := []string{"localhost:9092"}
    consumer := kafka.NewTelemetryConsumer(brokers, portService)

    // 4. Rodar o consumidor em uma Goroutine (em segundo plano)
    go func() {
        fmt.Println("Consumidor do Kafka iniciado e escutando o tópico...")
        consumer.Start(ctx)
    }()

    // Delay para garantia de subida do consumidor
    time.Sleep(2 * time.Second)

    // --- MODIFICAÇÃO 3: Inicialização do Produtor IoT (Connection Pooling) ---
    producer := kafka.NewTelemetryProducer("localhost:9092", "v1.telemetry.draft")
    defer producer.Close() // Libera os descritores de arquivo (sockets) no encerramento

    // 5. Loop infinito do Produtor simulando eventos IoT
    fmt.Println("Iniciando a simulação IoT de telemetria contínua...")
    baseDraft := 10.0
    for {
        event := domain.TelemetryEvent{
            ShipID:    "SHIP-123",
            Draft:     baseDraft,
            Timestamp: time.Now(),
        }

        err := producer.Produce(ctx, event)
        if err != nil {
            fmt.Printf("Erro de I/O na produção: %v\n", err)
        } else {
            fmt.Printf("Telemetria enviada: Navio %s | Calado: %.2f metros\n", event.ShipID, event.Draft)
        }

        baseDraft += 0.5 // Simula variação de calado
        time.Sleep(2 * time.Second) // Cadência do sensor
    }
}