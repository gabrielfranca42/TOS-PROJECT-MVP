package services

type PortService struct {
	repo Repository // Interface para o Postgres
}

func (ps *PortService) ProcessContainerLoaded(shipID string) error {
	// 1. Busca o navio do banco
	ship, _ := ps.repo.GetShipByID(shipID)

	// 2. USA A REGRA QUE VOCÊ CRIOU NO DOMAIN
	err := ship.LoadContainer()
	if err != nil {
		return err
	}

	// 3. Salva o novo estado (ex: de IN_PROGRESS para READY_TO_DEPART)
	return ps.repo.UpdateShip(ship)
}
