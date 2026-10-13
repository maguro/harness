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
	"github.com/harness/gitness/http"

	"github.com/google/wire"
)

// WireSet provides the system and the components of its lifecycle.
var WireSet = wire.NewSet(
	NewSystem,
	ProvideBootstrapper,
	ProvideInstrumentation,
	ProvideHTTPServer,
	ProvideMetricServer,
	ProvideSSHServer,
	ProvideJobRunner,
	ProvideCIRunner,
)

// System is the root of the dependency graph.
// It holds the components that the system lifecycle starts and stops.
type System struct {
	httpServer   *HTTPServer
	metricServer *MetricServer
	sshServer    *SSHServer
	jobRunner    *JobRunner
	ciRunner     *CIRunner
}

func ProvideNoOpMetricServer() http.ListenAndServeServer {
	return http.NoOpListenAndServeServer{}
}

func NewSystem(
	httpServer *HTTPServer,
	metricServer *MetricServer,
	sshServer *SSHServer,
	jobRunner *JobRunner,
	ciRunner *CIRunner,
) *System {
	return &System{
		httpServer:   httpServer,
		metricServer: metricServer,
		sshServer:    sshServer,
		jobRunner:    jobRunner,
		ciRunner:     ciRunner,
	}
}
