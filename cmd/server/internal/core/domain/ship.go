package domain

import "errors"

// Ship representa a entidade principal do seu domínio
type Ship struct {
	ID            string
	Name          string
	TotalCapacity int
	LoadedCount   int
	Status        string
}

// Regra de Negócio: O navio pode ser liberado?
// Note que isso é puro Go, sem dependências externas.
func (s *Ship) CanDepart() bool {
	return s.LoadedCount >= s.TotalCapacity
}

// Incrementa a carga e valida
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
