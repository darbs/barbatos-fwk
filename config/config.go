package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/tkanos/gonfig"
)

type Configuration struct {
	DbName     string
	DbEndpoint string
	MqEndpoint string
}

var config Configuration

func init() {
	env := os.Getenv("MODE")
	configPath := os.Getenv("CONFIG_PATH")

	filename := ""
	switch {
	case env == "production":
		filename = "prod.json"
	case env == "development":
		filename = "dev.json"
	case env == "test":
		filename = "test.json"
	}

	if configPath == "" {
		configPath = "configs"
	}

	fp := filepath.Join(configPath, filename)
	if filename != "" {
		log.Printf("Loading - %v", fp)
	}

	err := gonfig.GetConf(fp, &config)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
		panic(err)
	}
}

func GetConfig() Configuration {
	return config
}
