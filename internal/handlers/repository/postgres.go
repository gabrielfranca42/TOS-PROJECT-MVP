package repository

import (
	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/domain"

	"gorm.io/gorm"
)

type PostgresShipRepository struct {
	db *gorm.DB
}

func NewPostgresRepository(db *gorm.DB) *PostgresShipRepository {
	return &PostgresShipRepository{db: db}
}

func (r *PostgresShipRepository) GetShipByID(id string) (*domain.Ship, error) {
	var ship domain.Ship
	// Busca o navio pelo ID no Postgres
	result := r.db.First(&ship, "id = ?", id)
	return &ship, result.Error
}

func (r *PostgresShipRepository) UpdateShip(ship *domain.Ship) error {
	// Salva o novo estado (ex: de IN_PROGRESS para READY_TO_DEPART)
	return r.db.Save(ship).Error
}
