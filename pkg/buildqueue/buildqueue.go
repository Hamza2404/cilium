// Copyright 2016-2018 Authors of Cilium
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
	"runtime"
	"sync"

	"github.com/cilium/cilium/pkg/lock"
)

const (
	// minWorkerThreads is the minimum number of worker threads to use
	// regardless of the number of cores available in the system
	minWorkerThreads = 2

	// buildQueueSize is the maximum number of builds that can be queued
	// before Enqueue() starts blocking
	buildQueueSize = 4096
)

// buildStatusMap contains the build status of all queued builders
type buildStatusMap map[string]*buildStatus

type BuildQueue struct {
	// mutex protects the queued map
	mutex lock.Mutex

	// buildStatus contains the status of all builders separated by UUID
	buildStatus buildStatusMap

	// workerBuildQueue is a buffered channel that contains all scheduled
	// builds
	workerBuildQueue chan Builder

	// stopWorker is used to stop worker threads when the build queue is
	// shutdown. Workers will run until this channel is closed.
	stopWorker chan struct{}

	// ongoingExclusiveBuilds counts the ongoing exclusive builds and
	// allows waiting for all of them to complete
	ongoingExclusiveBuilds sync.WaitGroup

	// ongoingRegularBuilds counts the ongoing regular builds and allows
	// waiting for all of them to complete
	ongoingRegularBuilds sync.WaitGroup
}

// NewBuildQueue returns a new build queue
func NewBuildQueue() *BuildQueue {
	q := &BuildQueue{
		buildStatus:      buildStatusMap{},
		workerBuildQueue: make(chan Builder, buildQueueSize),
		stopWorker:       make(chan struct{}, 0),
	}

	nWorkers := numWorkerThreads()

	for w := 0; w < nWorkers; w++ {
		go q.runBuildQueue()
	}

	return q
}

// Stop stops the build queue and terminates all workers
func (q *BuildQueue) Stop() {
	close(q.stopWorker)
}

// numWorkerThreads returns the number of worker threads to use
func numWorkerThreads() int {
	ncpu := runtime.NumCPU()

	if ncpu < minWorkerThreads {
		return minWorkerThreads
	}
	return ncpu
}

// Buildable is an object that is buildable
type Buildable interface {
	// GetUUID must return a unique UUID of the object
	GetUUID() string
}

// Builder is an object that can build itself. A builder must also be Buildable
type Builder interface {
	Buildable

	// Build must build object
	Build() error
}

type BuildNotification chan bool

func newBuildNotification() BuildNotification {
	return make(BuildNotification, 1)
}

// build is what is returned by Enqueue() to provide information about the
// build
type build struct {
	builder Builder

	// notificationChannels is a list of a channels that should be notified
	// about the completion of the build
	notificationChannels []BuildNotification

	internalNotification BuildNotification
}

// addNotificationChannel adds channels to be notified. This function may only
// be called while the build is not yet in progress.
func (i *build) addNotificationChannel(n BuildNotification) {
	i.notificationChannels = append(i.notificationChannels, n)
}

// reportStatus reports the status of a build to all notification channels.
// This function may only be called if the build is the currentBuild. It may
// never be called while the build is still assigned to nextBuild.
func (i *build) reportStatus(success bool) {
	i.internalNotification <- success
	close(i.internalNotification)

	for _, notify := range i.notificationChannels {
		notify <- success
		close(notify)
	}
}

func newBuildInfo(b Builder) *build {
	return &build{
		builder:              b,
		notificationChannels: []BuildNotification{},
		internalNotification: make(BuildNotification, 1),
	}
}

// buildStatus is the current status of a build
type buildStatus struct {
	// curentBuild is the building information of any ongoing build
	currentBuild *build

	// nextBuild is the building information of the next build for this
	// UUID
	nextBuild *build
}

func (q *BuildQueue) enqueueBuild(b Builder) (BuildNotification, bool) {
	build, enqueued := q.serializeBuild(b)

	notify := newBuildNotification()
	build.addNotificationChannel(notify)

	return notify, enqueued
}

func (q *BuildQueue) serializeBuild(b Builder) (*build, bool) {
	build := newBuildInfo(b)
	uuid := b.GetUUID()

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if buildStatus, ok := q.buildStatus[uuid]; ok {
		// If the builder is currently being built, prepare the next
		// build and store it in the nextBuild field. When the current
		// build finishes, the next build will automatically be added
		// to the workerBuildQueue
		if buildStatus.currentBuild != nil && buildStatus.nextBuild == nil {
			buildStatus.nextBuild = build
			return build, false
		}

		// The builder is already in the queue but not being built, the
		// build request will be fulfilled when the queued build is
		// executed. The caller can be notified to skip the build.
		return buildStatus.nextBuild, false
	}

	q.buildStatus[uuid] = &buildStatus{nextBuild: build}

	return build, true
}

// Enqueue schedules Builder for building. A channel is returned to provide the
// ability to wait for the build to complete and check for success or failure.
func (q *BuildQueue) Enqueue(b Builder) BuildNotification {
	notify, enqueue := q.enqueueBuild(b)
	if enqueue {
		// This should be non-blocking unless there is contention beyond
		// queueBuildSize in which case this will block until a slot in the
		// queue becomes available.
		q.workerBuildQueue <- b
	}

	return notify
}

// Remove removes the builder from the queue.
func (q *BuildQueue) Remove(b Buildable) {
	q.mutex.Lock()
	delete(q.buildStatus, b.GetUUID())
	q.mutex.Unlock()
}

// Drain will drain the queue from any ongoing or scheduled builds of a
// Buildable. If a build is ongoing, the function will block for the build to
// complete. It is the responsibility of the caller that the Buildable does not
// get re-scheduled during the draining. Returns true if waiting was required.
func (q *BuildQueue) Drain(b Buildable) bool {
	uuid := b.GetUUID()

	q.mutex.Lock()
	status, ok := q.buildStatus[uuid]
	var currentBuild *build
	if ok {
		currentBuild = status.currentBuild
		delete(q.buildStatus, uuid)

	}

	q.mutex.Unlock()

	// If a build is onging, block until build is complete
	if currentBuild != nil {
		<-currentBuild.internalNotification
		return true
	}

	return false
}

func (q *BuildQueue) runExclusiveBuild(b Builder) {
	uuid := b.GetUUID()

	for {
		currentBuild := q.dequeueBuild(uuid)
		if currentBuild == nil {
			return
		}

		// Register need for an exclusive build
		q.ongoingExclusiveBuilds.Add(1)

		// Wait for all regular builds to complete
		q.ongoingRegularBuilds.Wait()

		err := b.Build()
		currentBuild.reportStatus(err == nil)

		q.ongoingExclusiveBuilds.Done()

		if !q.needToBuildAgain(uuid) {
			return
		}
	}
}

// PreemptExclusive enqueues a build at the front of the queue and provides
// exclusive build access. All regular builds enqueued via Enqueue() have to
// finish before the exclusive build is executed. Exclusive builds themselves
// can run in parallel unless their UUIDs match in which case they are being
// serialized.
//
// If an exclusive build for the same UUID is already enqueued but not yet
// running, the build will be folded into the already scheduled build but both
// notification channels will be notified.
func (q *BuildQueue) PreemptExclusive(b Builder) BuildNotification {
	notify, enqueue := q.enqueueBuild(b)
	if enqueue {
		go q.runExclusiveBuild(b)
	}

	return notify
}

func (q *BuildQueue) dequeueBuild(uuid string) *build {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	buildStatus, ok := q.buildStatus[uuid]
	if !ok {
		// The builder has been removed since the build was
		// scheduled. Cancel the build.
		return nil
	}

	// Mark the scheduled build as building
	currentBuild := buildStatus.nextBuild
	buildStatus.currentBuild = currentBuild
	buildStatus.nextBuild = nil

	return currentBuild
}

func (q *BuildQueue) needToBuildAgain(uuid string) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	buildStatus, ok := q.buildStatus[uuid]
	var nextBuild *build
	if ok {
		buildStatus.currentBuild = nil
		nextBuild = buildStatus.nextBuild

		// If no next build is scheduled, the builder can be
		// removed from the build status entirely
		if nextBuild == nil {
			delete(q.buildStatus, uuid)
		}
	}

	return nextBuild != nil
}

func (q *BuildQueue) runBuildQueue() {
	for b := range q.workerBuildQueue {
		uuid := b.GetUUID()

		// Cannot run regular builds while exclusive builds are ongoing
		q.ongoingExclusiveBuilds.Wait()

		currentBuild := q.dequeueBuild(uuid)
		if currentBuild == nil {
			return
		}

		// Start the build and wait for it to be complete
		q.ongoingRegularBuilds.Add(1)
		err := b.Build()
		q.ongoingRegularBuilds.Done()

		currentBuild.reportStatus(err == nil)

		// If another build for the same builder is scheduled, queue it
		if q.needToBuildAgain(uuid) {
			q.workerBuildQueue <- b
		}

		select {
		case <-q.stopWorker:
			return
		default:
		}
	}
}
