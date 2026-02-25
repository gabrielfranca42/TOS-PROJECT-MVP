package main

import (
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupPostgres() *gorm.DB {
	dsn := "host=localhost user=postgres password=suasenha dbname=tos_db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Falha ao conectar no banco de dados:", err)
	}
	return db
}
