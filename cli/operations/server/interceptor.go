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
	"fmt"

	"github.com/rs/zerolog/log"
	"l7e.io/yama"
)

// lifecycleLogger logs each lifecycle call of each component.
// It logs the error of a component that fails to start and gives that error to fail.
type lifecycleLogger struct {
	fail FailFunc
}

func (l lifecycleLogger) Start(ctx context.Context, next yama.Starter) error {
	name := componentName(ctx)
	log.Ctx(ctx).Debug().Str("component", name).Msg("starting component")

	err := next.Start(ctx)
	if err != nil {
		log.Ctx(ctx).Err(err).Str("component", name).Msg("failed to start component")
		l.fail(fmt.Errorf("failed to start %s: %w", name, err))
	}

	return err
}

func (lifecycleLogger) Quiesce(ctx context.Context, next yama.Quiescer) {
	log.Ctx(ctx).Debug().Str("component", componentName(ctx)).Msg("quiescing component")

	next.Quiesce(ctx)
}

func (lifecycleLogger) Stop(ctx context.Context, next yama.Stopper) {
	log.Ctx(ctx).Debug().Str("component", componentName(ctx)).Msg("stopping component")

	next.Stop(ctx)
}

// componentName returns the type of the component under interception.
func componentName(ctx context.Context) string {
	component, _ := yama.FromContext[any](ctx)

	return fmt.Sprintf("%T", component)
}
