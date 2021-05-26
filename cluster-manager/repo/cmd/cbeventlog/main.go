package main

import (
	"fmt"
	"os"

	"github.com/couchbaselabs/cbmultimanager/log/parsers"
	"github.com/couchbaselabs/cbmultimanager/log/scraper"
	"github.com/couchbaselabs/cbmultimanager/log/utilities"
	"github.com/couchbaselabs/cbmultimanager/log/values"
	"github.com/couchbaselabs/cbmultimanager/logger"

	cli "github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	app := &cli.App{
		Name:                 "Couchbase Event Log Creator",
		HelpName:             "cbeventlog",
		Usage:                "Runs the Couchbase Event Log Creator",
		Version:              "0.0.1",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:   "node",
				Usage:  "Create an events log using logs taken directly from a node",
				Action: runCluster,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "username",
						Aliases:  []string{"u"},
						Usage:    "Username to access cluster",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "password",
						Aliases:  []string{"p"},
						Usage:    "Password to access cluster",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "node",
						Aliases:  []string{"n"},
						Usage:    "Address of the node to produce events log for",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "node-name",
						Usage:    "An identifier for the node the events log is produced for",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "include-events",
						Usage: "A comma separated list of event types to include in the output file",
					},
					&cli.StringFlag{
						Name:  "exclude-events",
						Usage: "A comma separated list of event types to exclude from the output file",
					},
					&cli.StringFlag{
						Name:  "log-path",
						Usage: "The path to output the Couchbase logs to (defaults to working directory)",
					},
				},
			},
			{
				Name:   "cbcollect",
				Usage:  "Create an events log from a cbcollect",
				Action: runCbcollect,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "path",
						Usage:    "Path to the zipped cbcollect file",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "node-name",
						Usage:    "An identifier for the node the events log is produced for",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "include-events",
						Usage: "A comma separated list of event types to include in the output file",
					},
					&cli.StringFlag{
						Name:  "exclude-events",
						Usage: "A comma separated list of event types to exclude from the output file",
					},
					&cli.StringFlag{
						Name:  "log-path",
						Usage: "The path to output the Couchbase logs to (defaults to working directory)",
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		zap.S().Fatalw("Failed to produce events log", "error", err)
	}
}

func runCluster(c *cli.Context) error {
	cred := &values.Credentials{
		User:     c.String("username"),
		Password: c.String("password"),
		Cluster:  c.String("node"),
		NodeName: c.String("node-name"),
	}

	return run(c, cred, "")
}

func runCbcollect(c *cli.Context) error {
	cred := &values.Credentials{
		NodeName: c.String("node-name"),
	}

	cbcollectPath := c.String("path")

	return run(c, cred, cbcollectPath)
}

func run(c *cli.Context, cred *values.Credentials, cbcollectPath string) error {
	if c.String("include-events") != "" && c.String("exclude-events") != "" {
		return fmt.Errorf("cannot give both include-events and exclude-events flags")
	}

	if err := logger.Init(zapcore.WarnLevel, ""); err != nil {
		return fmt.Errorf("could not initialize logger: %w", err)
	}

	var (
		events  []values.EventType
		err     error
		include bool
	)

	if c.String("include-events") != "" {
		events, err = utilities.GetEventList(c.String("include-events"))
		include = true
	} else {
		events, err = utilities.GetEventList(c.String("exclude-events"))
	}

	if err != nil {
		return err
	}

	logPath := c.String("log-path")
	if logPath == "" {
		logPath, err = os.Getwd()
		if err != nil {
			return err
		}
	}

	zap.S().Info("Event Log creation started")

	for _, log := range parsers.ParserFunctions {
		err := scraper.RunParsers(log, cred, cbcollectPath, logPath)
		if err != nil {
			zap.S().Warnw("(SCRAPER) Failed to run parsers on log", "log", log.Name, "error", err)
		}
	}

	err = scraper.MergeEventLogs(cred)
	if err != nil {
		zap.S().Warnw("(SCRAPER) Failed to merge event logs", "error", err)
		return err
	}

	zap.S().Info("Event Log creation finished successfully")

	if events == nil {
		return nil
	}

	err = scraper.FilterEvents(cred, events, include)
	if err != nil {
		zap.S().Warnw("(SCRAPER) Failed to filter exclude events", "error", err)
	}

	return err
}