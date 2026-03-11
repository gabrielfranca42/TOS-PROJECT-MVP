package kafka

import (
    "context"
    "encoding/json"

    "github.com/segmentio/kafka-go"
    "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
)

// TelemetryProducer encapsula o writer para garantir reutilização de socket TCP.
type TelemetryProducer struct {
    writer *kafka.Writer
}

// NewTelemetryProducer inicializa a conexão persistente com o broker.
func NewTelemetryProducer(brokerAddress, topic string) *TelemetryProducer {
    return &TelemetryProducer{
        writer: &kafka.Writer{
            Addr:     kafka.TCP(brokerAddress),
            Topic:    topic,
            Balancer: &kafka.LeastBytes{},
            // Configurações assíncronas padrão para IoT (Batching) podem ser injetadas aqui
        },
    }
}

// Produce envia o evento utilizando a conexão TCP já estabelecida.
func (p *TelemetryProducer) Produce(ctx context.Context, event domain.TelemetryEvent) error {
    payload, err := json.Marshal(event)
    if err != nil {
        return err
    }

    return p.writer.WriteMessages(ctx,
        kafka.Message{
            Key:   []byte(event.ShipID), // Particiona ordenadamente pelo ID do navio
            Value: payload,
        },
    )
}

// Close encerra as conexões TCP de forma graciosa. Obrigatório no encerramento da aplicação.
func (p *TelemetryProducer) Close() error {
    return p.writer.Close()
}