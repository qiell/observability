package janitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/couchbaselabs/cbmultimanager/storage"
	"github.com/couchbaselabs/cbmultimanager/values"

	"go.uber.org/zap"
)

// Janitor is in charge of cleaning up stale data periodically.
type Janitor struct {
	store storage.Store

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	shiftStart chan struct{}
}

func NewJanitor(store storage.Store) *Janitor {
	return &Janitor{
		store:      store,
		shiftStart: make(chan struct{}, 20),
	}
}

// Start starts the janitor cleanup duty.
func (j *Janitor) Start(frequency time.Duration) {
	if j.ctx != nil {
		return
	}

	j.ctx, j.cancel = context.WithCancel(context.Background())
	j.wg.Add(1)
	go j.periodicCleanUp(frequency)
}

func (j *Janitor) Stop() {
	if j.ctx == nil {
		return
	}

	j.cancel()
	j.wg.Wait()
	j.ctx, j.cancel = nil, nil
}

func (j *Janitor) ForceShift() {
	j.shiftStart <- struct{}{}
}

func (j *Janitor) periodicCleanUp(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer func() {
		ticker.Stop()
		j.wg.Done()
	}()

	for {
		select {
		case <-j.shiftStart:
			j.doCleanUp()
		case <-ticker.C:
			j.doCleanUp()
		case <-j.ctx.Done():
			return
		}
	}
}

func (j *Janitor) doCleanUp() {
	zap.S().Infow("(Janitor) Shift started")
	start := time.Now()
	j.cleanStore()
	zap.S().Debugw("(Janitor) Shift ended", "elapsed", time.Since(start).String())
}

func (j *Janitor) cleanStore() {
	j.cleanExpiredDismissals()

	clusters, err := j.store.GetClusters(true)
	if err != nil {
		zap.S().Errorw("(Janitor) Could not get clusters", "err", err)
		return
	}

	for _, cluster := range clusters {
		j.cleanCluster(cluster)
	}
}

// cleanCluster deletes stale data related to the given cluster. The data being removed is results/dismissals for
// nodes/buckets that are no longer part of the cluster.
func (j *Janitor) cleanCluster(cluster *values.CouchbaseCluster) {
	// delete any node level checkers where the node uuid is not a known value
	nodeUUIDS := make([]string, len(cluster.NodesSummary))
	for i, node := range cluster.NodesSummary {
		nodeUUIDS[i] = node.NodeUUID
	}

	removed, err := j.store.DeleteWhereNodesDoNotMatch(nodeUUIDS, cluster.UUID)
	reportCleanUp("unknown node results", removed, err)

	// delete any bucket level checkers where the node uuid is not a known value
	removed, err = j.store.DeleteWhereBucketsDoNotMatch(cluster.BucketsSummary.GetBucketNames(), cluster.UUID)
	reportCleanUp("unknown buckets results", removed, err)

	// delete dismissals for unknown nodes
	removed, err = j.store.DeleteDismissalForUnknownNodes(nodeUUIDS, cluster.UUID)
	reportCleanUp("unknown nodes dismissals", removed, err)

	// delete dismissals for unknown buckets
	removed, err = j.store.DeleteDismissalForUnknownBuckets(cluster.BucketsSummary.GetBucketNames(), cluster.UUID)
	reportCleanUp("unknown buckets dismissals", removed, err)
}

func (j *Janitor) cleanExpiredDismissals() {
	removed, err := j.store.DeleteExpiredDismissals()
	reportCleanUp("expired dismissals", removed, err)
}

func reportCleanUp(cleaning string, removed int64, err error) {
	if err != nil {
		zap.S().Errorw(fmt.Sprintf("(Janitor) Could not remove %s", cleaning), "err", err)
	} else {
		zap.S().Infow(fmt.Sprintf("(Janitor) Removed %s", cleaning), "count", removed)
	}
}