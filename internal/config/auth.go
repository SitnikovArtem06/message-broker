package config

import "fmt"

type AuthConfig struct {
	RootToken string
}

func loadAuthConfig() (AuthConfig, error) {
	rootToken, ok := lookupEnv("ROOT_TOKEN")
	if !ok || rootToken == "" {
		return AuthConfig{}, fmt.Errorf("ROOT_TOKEN is required")
	}

	return AuthConfig{RootToken: rootToken}, nil
}
