// Copyright 2023 Harness, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harness/gitness/profiler"
	"github.com/harness/gitness/types"
	"github.com/harness/gitness/version"

	"github.com/joho/godotenv"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"l7e.io/yama"
)

type command struct {
	envfile     string
	enableCI    bool
	initializer Initializer
}

// Initializer builds the system and the lifecycle that starts and stops it.
type Initializer func(
	ctx context.Context,
	config *types.Config,
	enableCI CIEnabled,
	fail FailFunc,
	opts ...yama.Option,
) (*System, yama.Lifecycle, error)

func (c *command) run(*kingpin.ParseContext) error {
	// load environment variables from file.
	// no error handling needed when file is not present
	_ = godotenv.Load(c.envfile)

	// create the system configuration store by loading
	// data from the environment.
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("encountered an error while loading configuration: %w", err)
	}

	// configure the log level
	SetupLogger(config)

	// configure profiler
	SetupProfiler(config)

	// add logger to context
	log := log.Logger.With().Logger()
	ctx := log.WithContext(context.Background())

	// ctx is canceled with the cause if a component fails while the system runs.
	ctx, fail := context.WithCancelCause(ctx)
	defer fail(nil)

	// appCtx is the context of the routines that the providers start.
	// The system cancels it first when it stops.
	appCtx, cancelApp := context.WithCancel(ctx)
	defer cancelApp()

	// Create context that listens for the interrupt signal from the OS.
	waitCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// initialize system
	_, lifecycle, err := c.initializer(appCtx, config, CIEnabled(c.enableCI), FailFunc(fail),
		yama.WithInterceptors(lifecycleLogger{fail: FailFunc(fail)}),
		yama.WithEndComponents(backgroundRoutines{cancel: cancelApp}),
	)
	if err != nil {
		return fmt.Errorf("encountered an error while wiring the system: %w", err)
	}

	// start the system in dependency order
	err = lifecycle.Start(ctx)
	if err != nil {
		// the cause names the component that failed first.
		return fmt.Errorf("encountered an error while starting the system: %w", errors.Join(err, context.Cause(ctx)))
	}

	log.Info().
		Str("host", config.HTTP.Host).
		Int("port", config.HTTP.Port).
		Str("revision", version.GitCommit).
		Str("repository", version.GitRepository).
		Stringer("version", version.Version).
		Msg("server started")

	// wait until a signal arrives or a component fails
	<-waitCtx.Done()

	// restore default behavior on the interrupt signal and notify user of shutdown.
	stop()
	log.Info().Msg("shutting down gracefully (press Ctrl+C again to force)")

	// stop the system in reverse dependency order
	lifecycle.Stop(ctx)

	return context.Cause(ctx)
}

// SetupLogger configures the global logger from the loaded configuration.
func SetupLogger(config *types.Config) {
	// configure the log level
	switch {
	case config.Trace:
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case config.Debug:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// configure time format (ignored if running in terminal)
	zerolog.TimeFieldFormat = time.RFC3339Nano

	// if the terminal is a tty we should output the
	// logs in pretty format
	if isatty.IsTerminal(os.Stdout.Fd()) {
		log.Logger = log.Output(
			zerolog.ConsoleWriter{
				Out:        os.Stderr,
				NoColor:    false,
				TimeFormat: "15:04:05.999",
			},
		)
	}
}

func SetupProfiler(config *types.Config) {
	profilerType, parsed := profiler.ParseType(config.Profiler.Type)
	if !parsed {
		log.Info().Msgf("No valid profiler so skipping profiling ['%s']", config.Profiler.Type)
		return
	}

	gitnessProfiler, _ := profiler.New(profilerType)
	gitnessProfiler.StartProfiling(config.Profiler.ServiceName, version.Version.String())
}

// Register the server command.
func Register(app *kingpin.Application, initializer Initializer) {
	c := new(command)
	c.initializer = initializer

	cmd := app.Command("server", "starts the server").
		Action(c.run)

	cmd.Arg("envfile", "load the environment variable file").
		Default("").
		StringVar(&c.envfile)

	cmd.Flag("enable-ci", "start ci runners for build executions").
		Default("true").
		Envar("ENABLE_CI").
		BoolVar(&c.enableCI)
}
