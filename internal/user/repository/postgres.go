package repository

import (
	"context"
	"fmt"
	"os"
	"photogallery/internal/user/models"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

//go:generate mockgen -source=postgres.go -destination=../mocks/user_mock.go -package=mocks
type Repository interface {
	CreateUser(context.Context, *models.User) (uuid.UUID, error)
	GetByEmail(context.Context, string) (*models.User, error)
	Close() error
}

type GormRepository struct{ db *gorm.DB }

func NewDB() (Repository, error) {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
		os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME_USER"), os.Getenv("DB_PORT_USER"),
	)
	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	if err := conn.AutoMigrate(&models.User{}); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &GormRepository{conn}, nil
}

func (r *GormRepository) Close() error {
	s, _ := r.db.DB()
	return s.Close()
}

func (r *GormRepository) CreateUser(ctx context.Context, user *models.User) (uuid.UUID, error) {
	result := r.db.WithContext(ctx).Create(&user)
	if result.Error != nil {
		return uuid.Nil, result.Error
	}
	return user.ID, nil
}

func (r *GormRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	var user models.User
	result := r.db.WithContext(ctx).Where("email = ?", email).First(&user)
	if result.Error != nil {
		return nil, result.Error
	}
	return &user, nil
}
