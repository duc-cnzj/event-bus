package bootstrapers

import (
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
	"mq/config"
)

type AppInterface interface {
	Boot()
	DB() *gorm.DB
	Config() *config.Config
	Redis() *redis.Client
}

type App struct {
	cfg          *config.Config
	redis        *redis.Client
	db           *gorm.DB
	Bootstrapers []Boot
}

func (app *App) Boot() {
	for _, bootstraper := range app.Bootstrapers {
		bootstraper.Boot(app)
	}
}

func (app *App) DB() *gorm.DB {
	return app.db
}

func (app *App) Redis() *redis.Client {
	return app.redis
}

func (app *App) Config() *config.Config {
	return app.cfg
}
