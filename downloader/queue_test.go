package downloader

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iawia002/lux/extractors"
)

func queueTestData(site string, size int64) *extractors.Data {
	return &extractors.Data{
		Site:  site,
		Title: site,
		Type:  extractors.DataTypeVideo,
		Streams: map[string]*extractors.Stream{
			"default": {
				ID:   "default",
				Size: size,
			},
		},
	}
}

// TestQueueOrdering verifies the scheduling order: user priority first,
// then site, then file size.
func TestQueueOrdering(t *testing.T) {
	qm := NewQueueManager(QueueOptions{MaxConcurrent: 1})
	qm.Add(queueTestData("b-site", 300), 0) // ID 1
	qm.Add(queueTestData("a-site", 100), 0) // ID 2
	qm.Add(queueTestData("a-site", 50), 0)  // ID 3
	qm.Add(queueTestData("z-site", 1), 10)  // ID 4, highest user priority

	var mu sync.Mutex
	order := make([]int, 0, 4)
	result := qm.Run(func(task *Task) error {
		mu.Lock()
		order = append(order, task.ID)
		mu.Unlock()
		return nil
	})

	want := []int{4, 3, 2, 1}
	if len(order) != len(want) {
		t.Fatalf("expected %d tasks to run, got %d", len(want), len(order))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("wrong scheduling order: got %v, want %v", order, want)
		}
	}
	if len(result.TaskErrors) != 0 || len(result.SystemErrors) != 0 {
		t.Fatalf("unexpected errors: %+v", result)
	}
	for _, task := range qm.Tasks() {
		if task.Status != TaskStatusDone {
			t.Errorf("task %d status = %s, want %s", task.ID, task.Status, TaskStatusDone)
		}
	}
}

// TestQueueConcurrencyLimit verifies the global concurrent download limit.
func TestQueueConcurrencyLimit(t *testing.T) {
	const limit = 2
	qm := NewQueueManager(QueueOptions{MaxConcurrent: limit})
	for i := 0; i < 6; i++ {
		qm.Add(queueTestData("site", int64(i)), 0)
	}

	var current, maxSeen int32
	qm.Run(func(task *Task) error {
		n := atomic.AddInt32(&current, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if n <= m || atomic.CompareAndSwapInt32(&maxSeen, m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil
	})

	if maxSeen > limit {
		t.Fatalf("concurrency limit exceeded: max concurrent = %d, limit = %d", maxSeen, limit)
	}
	if maxSeen < limit {
		t.Fatalf("expected to reach the concurrency limit %d, got %d", limit, maxSeen)
	}
}

// TestQueueTaskLevelRetry verifies that a failed task is put into the delay
// queue and rescheduled (task-level retry), and that a task which keeps
// failing ends up in Result.TaskErrors after retries are exhausted.
func TestQueueTaskLevelRetry(t *testing.T) {
	qm := NewQueueManager(QueueOptions{
		MaxConcurrent:  2,
		MaxTaskRetries: 2,
		RetryDelay:     10 * time.Millisecond,
	})
	flaky := qm.Add(queueTestData("flaky", 1), 0)
	broken := qm.Add(queueTestData("broken", 1), 0)

	var flakyCalls, brokenCalls int32
	result := qm.Run(func(task *Task) error {
		switch task {
		case flaky:
			if atomic.AddInt32(&flakyCalls, 1) < 3 {
				return errors.New("temporary error")
			}
			return nil
		case broken:
			atomic.AddInt32(&brokenCalls, 1)
			return errors.New("permanent error")
		}
		return nil
	})

	if got := atomic.LoadInt32(&flakyCalls); got != 3 {
		t.Errorf("flaky task attempts = %d, want 3", got)
	}
	// 1 initial run + 2 task-level retries
	if got := atomic.LoadInt32(&brokenCalls); got != 3 {
		t.Errorf("broken task attempts = %d, want 3", got)
	}
	if flaky.Status != TaskStatusDone {
		t.Errorf("flaky task status = %s, want %s", flaky.Status, TaskStatusDone)
	}
	if broken.Status != TaskStatusFailed {
		t.Errorf("broken task status = %s, want %s", broken.Status, TaskStatusFailed)
	}
	if len(result.TaskErrors) != 1 || result.TaskErrors[0].Task != broken {
		t.Fatalf("expected 1 task-level error for the broken task, got %+v", result.TaskErrors)
	}
	if len(result.SystemErrors) != 0 {
		t.Fatalf("unexpected system-level errors: %+v", result.SystemErrors)
	}
}

// TestQueuePauseResume verifies pausing and resuming a single pending task.
func TestQueuePauseResume(t *testing.T) {
	qm := NewQueueManager(QueueOptions{MaxConcurrent: 1})
	blocked := qm.Add(queueTestData("blocked", 1), 0)
	other := qm.Add(queueTestData("other", 1), 0)

	if !qm.Pause(blocked.ID) {
		t.Fatal("Pause should succeed for a pending task")
	}

	executed := make(map[int]bool)
	var mu sync.Mutex
	fn := func(task *Task) error {
		mu.Lock()
		executed[task.ID] = true
		mu.Unlock()
		return nil
	}

	// the paused task is skipped while the other one runs
	qm.Run(fn)
	mu.Lock()
	if executed[blocked.ID] {
		t.Error("paused task should not be executed")
	}
	if !executed[other.ID] {
		t.Error("the other task should be executed")
	}
	mu.Unlock()
	if blocked.Status != TaskStatusPaused {
		t.Fatalf("blocked task status = %s, want %s", blocked.Status, TaskStatusPaused)
	}

	if !qm.Resume(blocked.ID) {
		t.Fatal("Resume should succeed for a paused task")
	}
	qm.Run(fn)
	mu.Lock()
	if !executed[blocked.ID] {
		t.Error("resumed task should be executed")
	}
	mu.Unlock()
	if blocked.Status != TaskStatusDone {
		t.Fatalf("blocked task status = %s, want %s", blocked.Status, TaskStatusDone)
	}
}

// TestPauseController verifies the blocking behavior of Wait while paused.
func TestPauseController(t *testing.T) {
	pc := NewPauseController()
	pc.Wait() // not paused, should return immediately

	pc.Pause()
	if !pc.Paused() {
		t.Fatal("controller should be paused")
	}
	done := make(chan struct{})
	go func() {
		pc.Wait()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Wait should block while paused")
	case <-time.After(50 * time.Millisecond):
	}

	pc.Resume()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should return after Resume")
	}
	if pc.Paused() {
		t.Fatal("controller should not be paused after Resume")
	}
	pc.Wait() // should return immediately
}

// TestQueueSystemError verifies that a task which can not be executed at all
// is reported as a system-level error, not a task-level error.
func TestQueueSystemError(t *testing.T) {
	qm := NewQueueManager(QueueOptions{MaxConcurrent: 1})
	task := qm.Add(nil, 0)

	called := false
	result := qm.Run(func(task *Task) error {
		called = true
		return nil
	})

	if called {
		t.Fatal("download function should not be called for an empty task")
	}
	if task.Status != TaskStatusFailed {
		t.Errorf("task status = %s, want %s", task.Status, TaskStatusFailed)
	}
	if len(result.SystemErrors) != 1 {
		t.Fatalf("expected 1 system-level error, got %+v", result.SystemErrors)
	}
	if len(result.TaskErrors) != 0 {
		t.Fatalf("expected no task-level errors, got %+v", result.TaskErrors)
	}
}
