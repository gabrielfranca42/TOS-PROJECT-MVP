package main

import (
	"context"
	"encoding/json" // Adicionado para a API
	"fmt"
	"net/http" // Adicionado para o Servidor Web
	"time"

	"github.com/gorilla/mux"                                // Adicionado para Rotas
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/handlers/repository"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/mqtt" // Adicionado pacote MQTT
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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
	defer producer.Close()

	// ==========================================================
	// ADIÇÃO 1: INICIALIZAÇÃO DO MQTT (HARDWARE REAL)
	// ==========================================================
	mqttSub := mqtt.NewTelemetrySubscriber("tcp://localhost:1883", producer)
	go func() {
		fmt.Println("Subscriber MQTT iniciado (Pronto para receber dados do hardware)...")
		mqttSub.Start(ctx)
	}()

	// ==========================================================
	// ADIÇÃO 2: MOVER O MOCK PARA GOROUTINE (SIMULADOR)
	// ==========================================================
	go func() {
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

			baseDraft += 0.1
			time.Sleep(5 * time.Second) // Aumentado para 5s para não poluir o terminal
		}
	}()

	// ==========================================================
	// ADIÇÃO 3: SERVIDOR HTTP (INTERFACE WEB / API)
	// ==========================================================
	r := mux.NewRouter()

	// Endpoint para o Frontend buscar dados do navio
	r.HandleFunc("/ships/{id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ship, err := portService.GetShip(vars["id"])
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "Navio nao encontrado"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ship)
	}).Methods("GET")

	fmt.Println("Servidor HTTP iniciado em http://localhost:8080")
	// Este é o ponto que segura a aplicação ligada
	if err := http.ListenAndServe(":8080", r); err != nil {
		fmt.Printf("Falha no servidor HTTP: %v\n", err)
	}
}