package bootstrapers

import (
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
	"mq/config"
)

var app = &Application{
	Bootstrapers: []Boot{
		&ConfigLoader{},
		&DBLoader{},
		&RedisLoader{},
	},
}

func App() AppInterface {
	return app
}

type AppInterface interface {
	Boot()
	DB() *gorm.DB
	Config() *config.Config
	Redis() *redis.Client
}

type Application struct {
	booted       bool
	cfg          *config.Config
	redis        *redis.Client
	db           *gorm.DB
	Bootstrapers []Boot
}

func (app *Application) Booted() bool {
	return app.booted
}

func (app *Application) Boot() {
	for _, bootstraper := range app.Bootstrapers {
		bootstraper.Boot(app)
	}

	app.booted = true
}

func (app *Application) DB() *gorm.DB {
	return app.db
}

func (app *Application) Redis() *redis.Client {
	return app.redis
}

func (app *Application) Config() *config.Config {
	return app.cfg
}
