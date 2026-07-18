package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Temporal struct {
	Address   string `yaml:"address"`
	Namespace string `yaml:"namespace"`
	TaskQueue string `yaml:"task_queue"`
	TLSCACert string `yaml:"tls_ca_cert"`
	TLSCert   string `yaml:"tls_cert"`
	TLSKey    string `yaml:"tls_key"`
}

type ControlPlane struct {
	BindAddr       string   `yaml:"bind_addr"`
	CatalogGlob    string   `yaml:"catalog_glob"`
	Namespace      string   `yaml:"namespace"`
	HostAgentURL   string   `yaml:"host_agent_url"`
	HostAgentToken string   `yaml:"host_agent_token"`
	Temporal       Temporal `yaml:"temporal"`
}

type HostAgent struct {
	BindAddr    string `yaml:"bind_addr"`
	AuthToken   string `yaml:"auth_token"`
	CatalogGlob string `yaml:"catalog_glob"`
	MLX         struct {
		Executable            string `yaml:"executable"`
		Host                  string `yaml:"host"`
		Port                  int    `yaml:"port"`
		StartupTimeoutSeconds int    `yaml:"startup_timeout_seconds"`
		RunDir                string `yaml:"run_dir"`
	} `yaml:"mlx"`
}

func Load[T any](path string) (T, error) {
	var config T
	contents, err := os.ReadFile(path)
	if err != nil {
		return config, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(contents, &config); err != nil {
		return config, fmt.Errorf("parse config %s: %w", path, err)
	}
	return config, nil
}
