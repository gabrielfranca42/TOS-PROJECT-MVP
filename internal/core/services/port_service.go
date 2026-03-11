package services

import "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"

// Repository define o contrato de persistência esperado pelo Service
type Repository interface {
    GetShipByID(id string) (*domain.Ship, error)
    UpdateShip(ship *domain.Ship) error
    InsertTelemetry(telemetry *domain.ShipTelemetry) error // Contrato estendido para Time-Series
}

type PortService struct {
    repo Repository
}

func NewPortService(r Repository) *PortService {
    return &PortService{repo: r}
}

func (ps *PortService) GetShip(id string) (*domain.Ship, error) {
    return ps.repo.GetShipByID(id)
}

func (ps *PortService) ProcessContainerLoaded(shipID string) error {
    ship, err := ps.repo.GetShipByID(shipID)
    if err != nil {
        return err
    }

    err = ship.LoadContainer()
    if err != nil {
        return err
    }

    return ps.repo.UpdateShip(ship)
}

// ProcessTelemetry converte o evento IoT do broker para o modelo de persistência append-only
func (ps *PortService) ProcessTelemetry(event domain.TelemetryEvent) error {
    telemetry := &domain.ShipTelemetry{
        ShipID:    event.ShipID,
        Draft:     event.Draft,
        Timestamp: event.Timestamp,
    }
    return ps.repo.InsertTelemetry(telemetry)
}