package api

import (
	"fmt"
	"github.com/couchbaselabs/observability/config-svc/pkg/alertmanager"
	v1 "github.com/couchbaselabs/observability/config-svc/pkg/api/v1"
	"github.com/labstack/echo/v4"
	"gopkg.in/guregu/null.v4"
	"gopkg.in/yaml.v3"
	"net/http"
	"os"
)

const (
	defaultAlertmanagerConfigPath = "/etc/alertmanager/config.yml"
)

func (s *Server) GetAlertsConfiguration(ctx echo.Context) error {
	fd, err := os.OpenFile(defaultAlertmanagerConfigPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open alertmanager config: %w", err)
	}
	defer fd.Close()
	var cfg alertmanager.ConfigFile
	if err := yaml.NewDecoder(fd).Decode(&cfg); err != nil {
		return fmt.Errorf("failed to read alertmanager config: %w", err)
	}

	result := v1.AlertNotificationConfig{
		Email: nil,
		Slack: nil,
	}
	if cfg.Global.SlackAPIURLFile != "" {
		result.Slack = &v1.SlackAlertNotificationConfig{ConfiguredExternally: null.BoolFrom(true).Ptr()}
	} else if cfg.Global.SlackAPIURL != "" {
		result.Slack = &v1.SlackAlertNotificationConfig{WebhookURL: cfg.Global.SlackAPIURL}
	}

	if cfg.Global.SMTPFrom != "" && cfg.Global.SMTPHello != "" {
		result.Email = &v1.EmailAlertNotificationConfig{
			From:       cfg.Global.SMTPFrom,
			Host:       cfg.Global.SMTPSmarthost,
			Hello:      null.StringFrom(cfg.Global.SMTPHello).Ptr(),
			Identity:   null.StringFrom(cfg.Global.SMTPAuthIdentity).Ptr(),
			Password:   null.StringFrom(cfg.Global.SMTPAuthPassword).Ptr(),
			RequireTLS: null.BoolFrom(cfg.Global.SMTPRequireTLS).Ptr(),
			Secret:     null.StringFrom(cfg.Global.SMTPAuthSecret).Ptr(),
			Username:   null.StringFrom(cfg.Global.SMTPAuthUsername).Ptr(),
		}
	}

	return ctx.JSON(http.StatusOK, result)
}

func (s *Server) PutAlertsConfiguration(ctx echo.Context) error {
	return fmt.Errorf("NYI")
}
