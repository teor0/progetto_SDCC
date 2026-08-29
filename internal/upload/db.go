package upload

import (
	"fmt"
	"os"
	model "photogallery/internal/upload/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewDB() (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
		os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME_UPLOAD"), os.Getenv("DB_PORT_UPLOAD"),
	)
	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	if err := conn.AutoMigrate(&model.Record{}); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return conn, nil
}
