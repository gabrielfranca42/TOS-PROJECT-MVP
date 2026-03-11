package mqtt

import (
	"context"
	"encoding/json"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/kafka"
)

type TelemetrySubscriber struct {
	client   mqtt.Client
	producer *kafka.TelemetryProducer
}

// NewTelemetrySubscriber configura o cliente MQTT.
func NewTelemetrySubscriber(broker string, producer *kafka.TelemetryProducer) *TelemetrySubscriber {
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID("tos_mvp_gateway")
	opts.SetAutoReconnect(true)
	
	return &TelemetrySubscriber{
		client:   mqtt.NewClient(opts),
		producer: producer,
	}
}

// Start inicializa a subscrição no tópico IoT.
func (s *TelemetrySubscriber) Start(ctx context.Context) {
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("[MQTT ERROR] Falha na conexão com o broker: %v\n", token.Error())
		return
	}

	// Subscrição no tópico v1/telemetry/draft
	s.client.Subscribe("v1/telemetry/draft", 1, func(client mqtt.Client, msg mqtt.Message) {
		var event domain.TelemetryEvent
		if err := json.Unmarshal(msg.Payload(), &event); err != nil {
			fmt.Printf("[MQTT WARN] Payload inválido: %s\n", string(msg.Payload()))
			return
		}

		// Encaminha para o Kafka (Reuso da infraestrutura existente)
		s.producer.Produce(ctx, event)
		fmt.Printf("[MQTT INFO] Telemetria de hardware recebida: %s\n", event.ShipID)
	})
}