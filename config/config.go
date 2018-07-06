package config

import (
	"os"
	"path/filepath"

	"github.com/tkanos/gonfig"
	log "github.com/sirupsen/logrus"
)

type Configuration struct {
	DbName     string
	DbEndpoint string
	MqEndpoint string
}

var config Configuration

func init() {
	loglevel, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		panic(err)
	}
	log.SetLevel(loglevel)

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
		log.Infof("Loading - %v", fp)
	}

	err = gonfig.GetConf(fp, &config)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
		panic(err)
	}
}

func GetConfig() Configuration {
	return config
}
