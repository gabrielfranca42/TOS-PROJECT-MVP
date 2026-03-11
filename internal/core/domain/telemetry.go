package domain

import "time"

// ShipTelemetry representa a entidade Time-Series no banco de dados.
// Padrão Append-Only: Inserções contínuas, sem atualizações (UPDATE).
type ShipTelemetry struct {
    ID        uint      `gorm:"primaryKey;autoIncrement"`
    ShipID    string    `gorm:"column:ship_id;index;not null"`
    Draft     float64   `gorm:"column:draft;type:double precision;not null"` // Nível da água/calado
    Timestamp time.Time `gorm:"column:timestamp;index;not null"`             // Índice obrigatório para Time-Series
}

// TelemetryEvent representa o payload JSON emitido pelo microcontrolador IoT.
type TelemetryEvent struct {
    ShipID    string    `json:"ship_id"`
    Draft     float64   `json:"draft"`
    Timestamp time.Time `json:"timestamp"`
}