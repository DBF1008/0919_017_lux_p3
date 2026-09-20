package app

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, opt QueueOptions) *QueueManager {
	t.Helper()
	qm, err := NewQueueManager(opt)
	if err != nil {
		t.Fatalf("NewQueueManager() error = %v", err)
	}
	return qm
}

func TestNewQueueManagerInvalidOptions(t *testing.T) {
	if _, err := NewQueueManager(QueueOptions{MaxConcurrent: 0}); err == nil {
		t.Fatal("expected system-level error for max-concurrent < 1, got nil")
	}
	if _, err := NewQueueManager(QueueOptions{MaxConcurrent: 1, QueueRetryTimes: -1}); err == nil {
		t.Fatal("expected system-level error for negative queue retry times, got nil")
	}
}

func TestQueuePriorityOrdering(t *testing.T) {
	var mu sync.Mutex
	var order []int

	qm := newTestQueue(t, QueueOptions{
		MaxConcurrent: 1,
		RetryDelay:    time.Millisecond,
		Runner: func(task *Task) error {
			mu.Lock()
			order = append(order, task.ID)
			mu.Unlock()
			return nil
		},
	})

	// user-specified priority first, then site, then size
	qm.Enqueue(&Task{Site: "bilibili", Size: 300})               // ID 1
	qm.Enqueue(&Task{Site: "youtube", Size: 100, Priority: 1})   // ID 2
	qm.Enqueue(&Task{Site: "bilibili", Size: 100})               // ID 3
	qm.Enqueue(&Task{Site: "acfun", Size: 999})                  // ID 4
	qm.Enqueue(&Task{Site: "youtube", Size: 100, Priority: 2})   // ID 5
	qm.Enqueue(&Task{Site: "bilibili", Size: 100, Priority: -1}) // ID 6

	qm.Run()

	// priority: 5(2) > 2(1) > default(0): 4(acfun) < 3(bilibili,100) < 1(bilibili,300) > 6(-1)
	want := []int{5, 2, 4, 3, 1, 6}
	if len(order) != len(want) {
		t.Fatalf("expected %d tasks to run, got %d", len(want), len(order))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("execution order = %v, want %v", order, want)
		}
	}
}

func TestQueueConcurrencyLimit(t *testing.T) {
	const maxConcurrent = 2
	const taskCount = 10

	var mu sync.Mutex
	current := 0
	maxSeen := 0

	qm := newTestQueue(t, QueueOptions{
		MaxConcurrent: maxConcurrent,
		RetryDelay:    time.Millisecond,
		Runner: func(task *Task) error {
			mu.Lock()
			current++
			if current > maxSeen {
				maxSeen = current
			}
			mu.Unlock()

			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			current--
			mu.Unlock()
			return nil
		},
	})

	for i := 0; i < taskCount; i++ {
		qm.Enqueue(&Task{Site: "test"})
	}
	results := qm.Run()

	if len(results) != taskCount {
		t.Fatalf("expected %d results, got %d", taskCount, len(results))
	}
	if maxSeen > maxConcurrent {
		t.Fatalf("concurrency limit violated: saw %d concurrent tasks, limit %d", maxSeen, maxConcurrent)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("task %d failed: %v", r.Task.ID, r.Err)
		}
		if r.Task.Status() != TaskStatusDone {
			t.Fatalf("task %d status = %s, want done", r.Task.ID, r.Task.Status())
		}
	}
}

func TestQueueDelayedRetry(t *testing.T) {
	var mu sync.Mutex
	calls := make(map[int]int)

	qm := newTestQueue(t, QueueOptions{
		MaxConcurrent:   2,
		QueueRetryTimes: 2,
		RetryDelay:      10 * time.Millisecond,
		Runner: func(task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			calls[task.ID]++
			// task 1 fails once then succeeds; task 2 always succeeds
			if task.ID == 1 && calls[task.ID] == 1 {
				return errors.New("temporary failure")
			}
			return nil
		},
	})

	qm.Enqueue(&Task{Site: "test"})
	qm.Enqueue(&Task{Site: "test"})
	results := qm.Run()

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("task %d should have succeeded after queue retry: %v", r.Task.ID, r.Err)
		}
	}
	if calls[1] != 2 {
		t.Fatalf("task 1 should be attempted twice, got %d", calls[1])
	}
}

func TestQueueRetryExhausted(t *testing.T) {
	taskErr := errors.New("permanent failure")
	var mu sync.Mutex
	calls := 0

	qm := newTestQueue(t, QueueOptions{
		MaxConcurrent:   1,
		QueueRetryTimes: 2,
		RetryDelay:      time.Millisecond,
		Runner: func(task *Task) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return taskErr
		},
	})

	qm.Enqueue(&Task{Site: "test"})
	results := qm.Run()

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !errors.Is(results[0].Err, taskErr) {
		t.Fatalf("expected task-level error %v, got %v", taskErr, results[0].Err)
	}
	// 1 initial attempt + 2 queue retries
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
	if results[0].Task.Status() != TaskStatusFailed {
		t.Fatalf("task status = %s, want failed", results[0].Task.Status())
	}
}

func TestQueuePauseResumePendingTask(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	qm := newTestQueue(t, QueueOptions{
		MaxConcurrent: 1,
		RetryDelay:    time.Millisecond,
		Runner: func(task *Task) error {
			if task.ID == 1 {
				started <- struct{}{}
				<-release
			}
			return nil
		},
	})

	qm.Enqueue(&Task{Site: "test"})
	id2 := qm.Enqueue(&Task{Site: "test"})

	// pause task 2 before it starts; the scheduler must skip it
	if !qm.PauseTask(id2) {
		t.Fatal("PauseTask on pending task should succeed")
	}

	done := make(chan struct{})
	go func() {
		qm.Run()
		close(done)
	}()

	<-started // task 1 is running and blocking

	// task 2 is paused, so nothing else can start; Run must not finish
	select {
	case <-done:
		t.Fatal("Run returned while task 2 is still paused")
	case <-time.After(50 * time.Millisecond):
	}

	// resume task 2 and let task 1 finish
	if !qm.ResumeTask(id2) {
		t.Fatal("ResumeTask should succeed")
	}
	close(release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish after resuming the paused task")
	}

	for _, task := range qm.Tasks() {
		if task.Status() != TaskStatusDone {
			t.Fatalf("task %d status = %s, want done", task.ID, task.Status())
		}
	}
}

func TestQueuePauseUnknownTask(t *testing.T) {
	qm := newTestQueue(t, QueueOptions{MaxConcurrent: 1})
	if qm.PauseTask(999) {
		t.Fatal("PauseTask on unknown task should return false")
	}
	if qm.ResumeTask(999) {
		t.Fatal("ResumeTask on unknown task should return false")
	}
}

func TestTaskLess(t *testing.T) {
	tasks := []*Task{
		{ID: 1, Site: "b", Size: 10, Priority: 0},
		{ID: 2, Site: "a", Size: 10, Priority: 0},
		{ID: 3, Site: "a", Size: 5, Priority: 0},
		{ID: 4, Site: "z", Size: 1, Priority: 1},
	}
	// higher priority wins
	if !less(tasks[3], tasks[0]) {
		t.Fatal("higher priority task should be scheduled first")
	}
	// same priority: site name ascending
	if !less(tasks[1], tasks[0]) {
		t.Fatal("same priority: site a should come before site b")
	}
	// same priority and site: smaller size first
	if !less(tasks[2], tasks[1]) {
		t.Fatal("same priority and site: smaller size should come first")
	}
}
