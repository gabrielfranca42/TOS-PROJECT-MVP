package kafka

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/segmentio/kafka-go"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
)

// TelemetryConsumer substitui o consumidor de containers para atuar na ingestão IoT
type TelemetryConsumer struct {
    reader  *kafka.Reader
    service *services.PortService
}

// NewTelemetryConsumer inicializa a subscrição no tópico de telemetria
func NewTelemetryConsumer(brokers []string, service *services.PortService) *TelemetryConsumer {
    return &TelemetryConsumer{
        reader: kafka.NewReader(kafka.ReaderConfig{
            Brokers:  brokers,
            Topic:    "v1.telemetry.draft", // Alinhado com o TelemetryProducer
            GroupID:  "tos-telemetry-group", // Novo grupo para isolar offsets de Time-Series
            MinBytes: 10e3,
            MaxBytes: 10e6,
        }),
        service: service,
    }
}

func (c *TelemetryConsumer) Start(ctx context.Context) {
    for {
        m, err := c.reader.ReadMessage(ctx)
        if err != nil {
            fmt.Printf("[CONSUMER ERROR] Falha de I/O na leitura: %v\n", err)
            break
        }

        var event domain.TelemetryEvent
        if err := json.Unmarshal(m.Value, &event); err != nil {
            fmt.Printf("[CONSUMER WARN] Falha de desserialização na Partição %d, Offset %d. Payload: %s. Erro: %v\n", m.Partition, m.Offset, string(m.Value), err)
            continue
        }

        err = c.service.ProcessTelemetry(event)
        if err != nil {
            fmt.Printf("[CONSUMER ERROR] Erro na persistência de telemetria: %v\n", err)
        } else {
            fmt.Printf("[CONSUMER SUCCESS] Telemetria processada para Navio %s | Calado: %.2f metros\n", event.ShipID, event.Draft)
        }
    }
}