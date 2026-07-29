package gallery

import (
	"fmt"
	"os"
	"photogallery/internal/gallery/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewDB() (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
		os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME_GALLERY"), os.Getenv("DB_PORT_GALLERY"),
	)
	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	if err := conn.AutoMigrate(&models.Gallery{}, &models.Member{}); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return conn, nil
}
