package kafka

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/segmentio/kafka-go"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"
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
            fmt.Printf("[CONSUMER ERROR] Falha de I/O na leitura: %v\n", err)
            break
        }

        var event domain.ContainerEvent
        if err := json.Unmarshal(m.Value, &event); err != nil {
            fmt.Printf("[CONSUMER WARN] Falha de desserialização na Partição %d, Offset %d. Payload: %s. Erro: %v\n", m.Partition, m.Offset, string(m.Value), err)
            continue
        }

        fmt.Printf("[CONSUMER INFO] Evento recebido: Container %s -> Navio %s\n", event.ContainerID, event.ShipID)

        err = c.service.ProcessContainerLoaded(event.ShipID)
        if err != nil {
            fmt.Printf("[CONSUMER ERROR] Erro na regra de negócio/DB: %v\n", err)
        } else {
            fmt.Printf("[CONSUMER SUCCESS] Carga processada no DB para o Navio %s\n", event.ShipID)
        }
    }
}