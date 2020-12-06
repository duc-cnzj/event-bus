package bootstrapers

import "github.com/go-redis/redis/v8"

type RedisLoader struct {
}

func (r *RedisLoader) Boot(app *Application) {
	cfg := app.cfg
	app.redis = redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}
