package domain

import (
	"errors"
	"time"
)

// Ship representa a entidade do seu domínio
type Ship struct {
	ID            string
	Name          string
	TotalCapacity int
	LoadedCount   int
	Status        string
}

// ContainerEvent representa o evento para o Kafka
type ContainerEvent struct {
	ContainerID string    `json:"container_id"`
	ShipID      string    `json:"ship_id"`
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
}

func (s *Ship) CanDepart() bool {
	return s.LoadedCount >= s.TotalCapacity
}

func (s *Ship) LoadContainer() error {
	if s.LoadedCount >= s.TotalCapacity {
		return errors.New("navio já está na capacidade máxima")
	}
	s.LoadedCount++
	if s.LoadedCount == s.TotalCapacity {
		s.Status = "READY_TO_DEPART"
	}
	return nil
}
