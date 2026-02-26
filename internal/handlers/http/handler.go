package http

import (
	"net/http"

	"github.com/seuusuario/TOS-PROJECT-MVP/internal/core/services"

	"github.com/gin-gonic/gin" // Exemplo usando o framework Gin
)

type ShipHandler struct {
	service *services.PortService
}

func NewShipHandler(s *services.PortService) *ShipHandler {
	return &ShipHandler{service: s}
}

// GetStatus retorna se o navio está pronto ou não
func (h *ShipHandler) GetStatus(c *gin.Context) {
	id := c.Param("id")
	ship, err := h.service.GetShip(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Navio não encontrado"})
		return
	}

	// Retorna o status que o Domain decidiu (ex: "READY_TO_DEPART")
	c.JSON(http.StatusOK, ship)
}
