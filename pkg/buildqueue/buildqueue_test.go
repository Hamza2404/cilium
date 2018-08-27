// Copyright 2018 Authors of Cilium
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

package buildqueue

import (
	"fmt"
	"testing"
	"time"

	"github.com/cilium/cilium/pkg/lock"
	"github.com/cilium/cilium/pkg/testutils"
	"github.com/cilium/cilium/pkg/uuid"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type BuildQueueTestSuite struct{}

var _ = Suite(&BuildQueueTestSuite{})

type testBuilder struct {
	mutex       lock.Mutex
	uuid        string
	nbuilds     int
	building    bool
	finishBuild chan error
}

func newTestBuilder() *testBuilder {
	test := &testBuilder{
		uuid:        uuid.NewUUID().String(),
		finishBuild: make(chan error, 1),
	}

	fmt.Printf("Allocated new UUID %s\n", test.uuid)

	return test
}

func (t *testBuilder) GetUUID() string {
	return t.uuid
}

func (t *testBuilder) Build() error {
	t.mutex.Lock()
	t.building = true
	t.mutex.Unlock()

	err := <-t.finishBuild

	t.mutex.Lock()
	t.building = false
	t.nbuilds++
	t.mutex.Unlock()

	return err
}

func (t *testBuilder) getNumBuilds() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.nbuilds
}

func (t *testBuilder) isBuilding() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.building
}

func (t *testBuilder) makeBuildReturn(err error) {
	t.finishBuild <- err
}

func (s *BuildQueueTestSuite) TestBuildQueue(c *C) {
	bq := NewBuildQueue()
	c.Assert(bq, Not(IsNil))
	defer bq.Stop()

	// create builder1
	builder1 := newTestBuilder()
	c.Assert(builder1.getNumBuilds(), Equals, 0)

	// create builder2
	builder2 := newTestBuilder()
	c.Assert(builder2.getNumBuilds(), Equals, 0)

	// enqueue builder1
	bq.Enqueue(builder1)

	// enqueue two builds of builder2. The 2nd enqueue should be ignored
	// as it is definitely not building yet.
	bq.Enqueue(builder2)
	bq.Enqueue(builder2)

	// Wait for builder1 to start building
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.isBuilding()
	}, 2*time.Second), IsNil)

	// Enqueue a 2nd build of builder1
	bq.Enqueue(builder1)

	// Let the first build of builder1 complete
	builder1.makeBuildReturn(nil)

	// Wait for first build of builder1 to complete
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.getNumBuilds() == 1
	}, 2*time.Second), IsNil)

	// Check that the 2nd build of builder1 did not complete. Give it 100 millisecond
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.getNumBuilds() == 2
	}, 100*time.Millisecond), Not(IsNil))

	// Let the build 2 of builder1 complete
	builder1.makeBuildReturn(nil)

	// Build 2 of builder1 should complete
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.getNumBuilds() == 2
	}, 2*time.Second), IsNil)

	// Let the first build of builder 2 complete
	builder2.makeBuildReturn(nil)

	// Out of the two builds of builder 2, only one should have been
	// performed
	c.Assert(testutils.WaitUntil(func() bool {
		return builder2.getNumBuilds() == 1
	}, 2*time.Second), IsNil)
}

func (s *BuildQueueTestSuite) TestBuildStatus(c *C) {
	bq := NewBuildQueue()
	c.Assert(bq, Not(IsNil))
	defer bq.Stop()

	// create builder1
	builder1 := newTestBuilder()

	// enqueue builder1
	buildStatus := bq.Enqueue(builder1)

	// Let the build fail with a failure
	builder1.makeBuildReturn(fmt.Errorf("build error"))

	buildSuccessful := <-buildStatus
	c.Assert(buildSuccessful, Equals, false)
	c.Assert(builder1.getNumBuilds(), Equals, 1)

	// enqueue builder1
	buildStatus = bq.Enqueue(builder1)

	// Let the build succeed
	builder1.makeBuildReturn(nil)

	buildSuccessful = <-buildStatus
	c.Assert(builder1.getNumBuilds(), Equals, 2)
	c.Assert(buildSuccessful, Equals, true)
}

func (s *BuildQueueTestSuite) TestDrain(c *C) {
	bq := NewBuildQueue()
	c.Assert(bq, Not(IsNil))
	defer bq.Stop()

	builder1 := newTestBuilder()
	bq.Enqueue(builder1)

	// Wait for builder1 to start building
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.isBuilding()
	}, 2*time.Second), IsNil)

	// Drain in the background and wait
	drainDone := make(chan bool, 1)
	go func() {
		drainDone <- bq.Drain(builder1)
		close(drainDone)
	}()

	builder1.makeBuildReturn(nil)

	select {
	case hasWaited := <-drainDone:
		c.Assert(hasWaited, Equals, true)
	case <-time.After(2 * time.Second):
		c.Fatalf("timeout while waiting for build to drain")
	}
}

func (s *BuildQueueTestSuite) TestExclusiveBuild(c *C) {
	bq := NewBuildQueue()
	c.Assert(bq, Not(IsNil))
	defer bq.Stop()

	// enqueue builder1
	builder1 := newTestBuilder()
	bq.Enqueue(builder1)

	// Wait for builder1 to start building
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.isBuilding()
	}, 2*time.Second), IsNil)

	// enqueue a follow-up build of builder1
	bq.Enqueue(builder1)

	// enqueue builder2
	builder2 := newTestBuilder()
	bq.Enqueue(builder2)

	// Wait for builder2 to start building
	c.Assert(testutils.WaitUntil(func() bool {
		return builder2.isBuilding()
	}, 2*time.Second), IsNil)

	// enqueue a follow-up build of builder2
	bq.Enqueue(builder2)

	// create exclusive
	exclusive := newTestBuilder()

	bq.PreemptExclusive(exclusive)

	// Make sure exclusive build does not start
	c.Assert(testutils.WaitUntil(func() bool {
		return exclusive.isBuilding()
	}, 100*time.Millisecond), Not(IsNil))

	// Let builder1 and builder2 return
	builder1.makeBuildReturn(nil)
	builder2.makeBuildReturn(nil)

	// Wait for exclusive builder to start
	c.Assert(testutils.WaitUntil(func() bool {
		return exclusive.isBuilding()
	}, 2*time.Second), IsNil)

	// queue another exclusive build
	bq.PreemptExclusive(exclusive)

	// Make sure builder1 and builder2 did not start
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.isBuilding() || builder2.isBuilding()
	}, 100*time.Millisecond), Not(IsNil))

	// let both exclusive builder succeed
	exclusive.makeBuildReturn(nil)
	exclusive.makeBuildReturn(nil)

	// Wait for builder1 and builder2 to start
	c.Assert(testutils.WaitUntil(func() bool {
		return builder1.isBuilding() && builder2.isBuilding()
	}, 2*time.Second), IsNil)
}
