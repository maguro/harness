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
	"net"
	nethttp "net/http"
	"sync"
	"time"

	"github.com/harness/gitness/app/bootstrap"
	"github.com/harness/gitness/app/pipeline/logger"
	"github.com/harness/gitness/app/pipeline/resolver"
	"github.com/harness/gitness/app/server"
	"github.com/harness/gitness/app/services"
	"github.com/harness/gitness/app/services/instrument"
	"github.com/harness/gitness/http"
	"github.com/harness/gitness/ssh"
	"github.com/harness/gitness/types"

	"github.com/drone/runner-go/poller"
	gliderssh "github.com/gliderlabs/ssh"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// CIEnabled tells if the system runs the CI runners for build executions.
type CIEnabled bool

// FailFunc stops the system after a component fails while it runs.
type FailFunc func(error)

// backgroundRoutines cancels the context of the routines that the providers start.
// It quiesces before every graph component when it runs as an end component.
type backgroundRoutines struct {
	cancel context.CancelFunc
}

// Quiesce cancels the context of the background routines.
func (b backgroundRoutines) Quiesce(context.Context) {
	b.cancel()
}

// Bootstrapper bootstraps the system when the lifecycle starts.
// Components that must start after the bootstrap take it as a dependency.
type Bootstrapper struct {
	bootstrap bootstrap.Bootstrap
}

// ProvideBootstrapper provides the component that bootstraps the system.
func ProvideBootstrapper(bootstrap bootstrap.Bootstrap) *Bootstrapper {
	return &Bootstrapper{bootstrap: bootstrap}
}

// Start bootstraps the system.
func (b *Bootstrapper) Start(ctx context.Context) error {
	err := b.bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("encountered an error while bootstrapping the system: %w", err)
	}

	return nil
}

// Instrumentation closes the instrumentation service when the lifecycle stops.
// Components that use the instrumentation take it as a dependency.
type Instrumentation struct {
	service instrument.Service
	timeout time.Duration
}

// ProvideInstrumentation provides the component that closes the instrumentation service.
func ProvideInstrumentation(config *types.Config, service instrument.Service) *Instrumentation {
	return &Instrumentation{service: service, timeout: config.GracefulShutdownTime}
}

// Stop closes the instrumentation service.
func (i *Instrumentation) Stop(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.timeout)
	defer cancel()

	err := i.service.Close(ctx)
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("failed to close instrumentation gracefully")
	}
}

// HTTPServer serves the HTTP API and the web UI while the lifecycle runs.
type HTTPServer struct {
	listenServer
}

// ProvideHTTPServer provides the component that runs the HTTP server.
// The server starts after the bootstrap and stops before the instrumentation.
func ProvideHTTPServer(
	config *types.Config,
	server *server.Server,
	fail FailFunc,
	_ *Bootstrapper,
	_ *Instrumentation,
) *HTTPServer {
	return &HTTPServer{listenServer: newListenServer("http server", server, config.GracefulShutdownTime, fail)}
}

// MetricServer serves the metrics while the lifecycle runs.
type MetricServer struct {
	listenServer
}

// ProvideMetricServer provides the component that runs the metric server.
// The server starts after the bootstrap and stops before the instrumentation.
func ProvideMetricServer(
	config *types.Config,
	server http.ListenAndServeServer,
	fail FailFunc,
	_ *Bootstrapper,
	_ *Instrumentation,
) *MetricServer {
	return &MetricServer{listenServer: newListenServer("metric server", server, config.GracefulShutdownTime, fail)}
}

// listenServer runs a server that listens in the background.
// A server error other than a close stops the system.
type listenServer struct {
	name     string
	server   http.ListenAndServeServer
	timeout  time.Duration
	fail     FailFunc
	shutdown http.ShutdownFunction
	done     chan struct{}
}

func newListenServer(
	name string,
	server http.ListenAndServeServer,
	timeout time.Duration,
	fail FailFunc,
) listenServer {
	return listenServer{name: name, server: server, timeout: timeout, fail: fail}
}

// Start starts the server in the background.
func (s *listenServer) Start(context.Context) error {
	var group *errgroup.Group
	group, s.shutdown = s.server.ListenAndServe()

	s.done = make(chan struct{})
	go func() {
		defer close(s.done)

		err := group.Wait()
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			s.fail(fmt.Errorf("%s failed: %w", s.name, err))
		}
	}()

	return nil
}

// Quiesce shuts the server down and waits for the active requests to complete.
func (s *listenServer) Quiesce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
	defer cancel()

	err := s.shutdown(ctx)
	if err != nil {
		log.Ctx(ctx).Err(err).Msgf("failed to shutdown %s gracefully", s.name)
	}
}

// Stop waits for the server to exit.
func (s *listenServer) Stop(context.Context) {
	<-s.done
}

// SSHServer serves the git ssh protocol while the lifecycle runs.
// It does nothing when the configuration disables ssh.
type SSHServer struct {
	server  *ssh.Server
	enabled bool
	timeout time.Duration
	ln      net.Listener
	done    chan struct{}
}

// ProvideSSHServer provides the component that runs the ssh server.
// The server starts after the bootstrap and stops before the instrumentation.
func ProvideSSHServer(
	config *types.Config,
	server *ssh.Server,
	_ *Bootstrapper,
	_ *Instrumentation,
) *SSHServer {
	return &SSHServer{server: server, enabled: config.SSH.Enable, timeout: config.GracefulShutdownTime}
}

// Start binds the ssh address and serves it in the background.
func (s *SSHServer) Start(ctx context.Context) error {
	if !s.enabled {
		return nil
	}

	ln, err := s.server.Listen(ctx)
	if err != nil {
		return fmt.Errorf("failed to start ssh server: %w", err)
	}

	s.ln = ln
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)

		err := s.server.Serve(ln)
		if err != nil && !errors.Is(err, gliderssh.ErrServerClosed) {
			log.Err(err).Msg("ssh server failed")
		}
	}()

	return nil
}

// Quiesce shuts the ssh server down and waits for the active sessions to complete.
func (s *SSHServer) Quiesce(ctx context.Context) {
	if s.done == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("failed to shutdown ssh server gracefully")
	}

	// Shutdown does not close a listener that Serve has not taken yet.
	_ = s.ln.Close()
}

// Stop waits for the ssh server to exit.
func (s *SSHServer) Stop(context.Context) {
	if s.done == nil {
		return
	}

	<-s.done
}

// JobRunner registers the recurring jobs and runs the job scheduler while the lifecycle runs.
// A scheduler error stops the system.
type JobRunner struct {
	services services.Services
	timeout  time.Duration
	fail     FailFunc
	cancel   context.CancelFunc
	done     chan struct{}
}

// ProvideJobRunner provides the component that runs the job scheduler.
// The scheduler starts after the bootstrap and stops before the instrumentation.
func ProvideJobRunner(
	config *types.Config,
	services services.Services,
	fail FailFunc,
	_ *Bootstrapper,
	_ *Instrumentation,
) *JobRunner {
	return &JobRunner{services: services, timeout: config.GracefulShutdownTime, fail: fail}
}

// Start registers the recurring jobs and runs the job scheduler in the background.
func (r *JobRunner) Start(ctx context.Context) error {
	if r.services.MetricCollector != nil {
		err := r.services.MetricCollector.Register(ctx)
		if err != nil {
			return fmt.Errorf("failed to register metric collector: %w", err)
		}
	}

	if r.services.RepoSizeCalculator != nil {
		err := r.services.RepoSizeCalculator.Register(ctx)
		if err != nil {
			return fmt.Errorf("failed to register repo size calculator: %w", err)
		}
	}

	err := r.services.Cleanup.Register(ctx)
	if err != nil {
		return fmt.Errorf("failed to register cleanup service: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel
	r.done = make(chan struct{})

	go func() {
		defer close(r.done)

		err := r.services.JobScheduler.Run(runCtx)
		if err != nil && runCtx.Err() == nil {
			r.fail(fmt.Errorf("job scheduler failed: %w", err))
		}
	}()

	return nil
}

// Quiesce stops the job scheduler and waits for the running jobs to finish.
func (r *JobRunner) Quiesce(ctx context.Context) {
	r.cancel()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.timeout)
	defer cancel()

	r.services.JobScheduler.WaitJobsDone(ctx)
}

// Stop waits for the job scheduler to exit.
func (r *JobRunner) Stop(context.Context) {
	<-r.done
}

// CIRunner populates the plugins and polls for build executions while the lifecycle runs.
// It does nothing when CI is disabled.
type CIRunner struct {
	poller   *poller.Poller
	resolver *resolver.Manager
	enabled  bool
	workers  int
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// ProvideCIRunner provides the component that runs the CI runners.
// The runners start after the bootstrap.
func ProvideCIRunner(
	config *types.Config,
	enabled CIEnabled,
	poller *poller.Poller,
	resolver *resolver.Manager,
	_ *Bootstrapper,
) *CIRunner {
	return &CIRunner{
		poller:   poller,
		resolver: resolver,
		enabled:  bool(enabled),
		workers:  config.CI.ParallelWorkers,
	}
}

// Start populates the plugins and polls for build executions in the background.
func (r *CIRunner) Start(ctx context.Context) error {
	if !r.enabled {
		return nil
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel

	r.wg.Go(func() {
		err := r.resolver.Populate(runCtx)
		if err != nil {
			log.Ctx(runCtx).Err(err).Msg("could not populate plugins")
		}
	})

	r.wg.Go(func() {
		r.poller.Poll(logger.WithWrappedZerolog(runCtx), r.workers)
	})

	return nil
}

// Quiesce stops the plugin population and the polling for build executions.
func (r *CIRunner) Quiesce(context.Context) {
	if r.cancel == nil {
		return
	}

	r.cancel()
}

// Stop waits for the CI runners to exit.
func (r *CIRunner) Stop(context.Context) {
	r.wg.Wait()
}
