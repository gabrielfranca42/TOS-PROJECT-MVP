package services

import "github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"

// Interface que o Service espera do Repository
type Repository interface {
	GetShipByID(id string) (*domain.Ship, error)
	UpdateShip(ship *domain.Ship) error
}

type PortService struct {
	repo Repository
}

func NewPortService(r Repository) *PortService {
	return &PortService{repo: r}
}

// ESTE É O MÉTODO QUE ESTAVA FALTANDO:
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
