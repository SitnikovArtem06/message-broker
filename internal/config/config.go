package config

type Config struct {
	GRPCAddr string
	DB       DBConfig
	Auth     AuthConfig
	Limits   LimitsConfig
}

func Load() (Config, error) {
	dbConfig, err := loadDBConfig()
	if err != nil {
		return Config{}, err
	}

	authConfig, err := loadAuthConfig()
	if err != nil {
		return Config{}, err
	}

	limitsConfig, err := loadLimitsConfig()
	if err != nil {
		return Config{}, err
	}

	return Config{
		GRPCAddr: getEnv("GRPC_ADDR", ":50051"),
		DB:       dbConfig,
		Auth:     authConfig,
		Limits:   limitsConfig,
	}, nil
}
