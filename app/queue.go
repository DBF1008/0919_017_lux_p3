package app

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/iawia002/lux/downloader"
	"github.com/iawia002/lux/extractors"
)

// TaskStatus describes the lifecycle state of a queued download task.
type TaskStatus int

const (
	// TaskStatusPending means the task is ready and waiting for a free slot.
	TaskStatusPending TaskStatus = iota
	// TaskStatusRunning means the task is being downloaded.
	TaskStatusRunning
	// TaskStatusPaused means the task is paused by the user.
	TaskStatusPaused
	// TaskStatusDelayed means the task failed and is waiting in the delayed
	// queue for a task-level retry (re-scheduling).
	TaskStatusDelayed
	// TaskStatusDone means the task finished successfully.
	TaskStatusDone
	// TaskStatusFailed means the task failed and exhausted its queue retries.
	TaskStatusFailed
)

func (s TaskStatus) String() string {
	switch s {
	case TaskStatusPending:
		return "pending"
	case TaskStatusRunning:
		return "running"
	case TaskStatusPaused:
		return "paused"
	case TaskStatusDelayed:
		return "delayed"
	case TaskStatusDone:
		return "done"
	case TaskStatusFailed:
		return "failed"
	}
	return "unknown"
}

// Task is a single queued download job.
type Task struct {
	ID    int
	URL   string
	Title string
	// Site is the extractor site name, used for site-based scheduling.
	Site string
	// Size is the estimated download size in bytes, used for size-based scheduling.
	Size int64
	// Priority is the user-specified priority, higher values are scheduled first.
	Priority int

	Data       *extractors.Data
	Downloader *downloader.Downloader

	pause    *downloader.PauseController
	status   TaskStatus
	attempts int
	readyAt  time.Time
	err      error
}

// Status returns the current task status.
func (t *Task) Status() TaskStatus {
	return t.status
}

// Err returns the final error of a failed task, nil otherwise.
func (t *Task) Err() error {
	return t.err
}

// Attempts returns how many times the task has been executed.
func (t *Task) Attempts() int {
	return t.attempts
}

// TaskRunner executes one task. It is called by the queue manager workers.
type TaskRunner func(t *Task) error

// QueueOptions configures the QueueManager.
type QueueOptions struct {
	// MaxConcurrent is the global limit of simultaneously running downloads.
	MaxConcurrent int
	// QueueRetryTimes is how many times a failed task is re-scheduled by the
	// queue (task-level retry, distinct from the per-chunk retry inside the
	// downloader controlled by --retry).
	QueueRetryTimes int
	// RetryDelay is the base delay before a failed task is re-scheduled.
	// The actual delay grows linearly with the number of attempts.
	RetryDelay time.Duration
	// Runner executes a task. If nil, the task's own Downloader is used.
	Runner TaskRunner
}

// TaskResult is the final outcome of a finished task.
type TaskResult struct {
	Task *Task
	Err  error // task-level failure, nil on success
}

// QueueManager schedules download tasks with priority ordering, a global
// concurrency limit, per-task pause/resume and a delayed retry queue.
type QueueManager struct {
	opt QueueOptions

	mu      sync.Mutex
	notify  chan struct{}
	seq     int
	tasks   map[int]*Task
	pending []*Task
	delayed []*Task
	running map[int]*Task
	results []TaskResult
}

// NewQueueManager returns a new QueueManager.
func NewQueueManager(opt QueueOptions) (*QueueManager, error) {
	if opt.MaxConcurrent < 1 {
		return nil, fmt.Errorf("max-concurrent must be >= 1, got %d", opt.MaxConcurrent)
	}
	if opt.QueueRetryTimes < 0 {
		return nil, fmt.Errorf("queue retry times must be >= 0, got %d", opt.QueueRetryTimes)
	}
	if opt.RetryDelay <= 0 {
		opt.RetryDelay = 5 * time.Second
	}
	return &QueueManager{
		opt:     opt,
		notify:  make(chan struct{}, 1),
		tasks:   make(map[int]*Task),
		running: make(map[int]*Task),
	}, nil
}

// Enqueue adds a new task to the queue. It must be called before Run.
func (qm *QueueManager) Enqueue(task *Task) int {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.seq++
	task.ID = qm.seq
	task.status = TaskStatusPending
	if task.pause == nil {
		task.pause = downloader.NewPauseController()
	}
	qm.tasks[task.ID] = task
	qm.pending = append(qm.pending, task)
	return task.ID
}

// Len returns the total number of tasks ever enqueued.
func (qm *QueueManager) Len() int {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return len(qm.tasks)
}

// Tasks returns a snapshot of all tasks, ordered by ID.
func (qm *QueueManager) Tasks() []*Task {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	tasks := make([]*Task, 0, len(qm.tasks))
	for i := 1; i <= qm.seq; i++ {
		if t, ok := qm.tasks[i]; ok {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// TaskSnapshot is a consistent read-only view of a task's state, safe to use
// while the queue is running.
type TaskSnapshot struct {
	ID     int
	Status TaskStatus
	Site   string
	Title  string
}

// Snapshots returns the current state of all tasks, ordered by ID.
func (qm *QueueManager) Snapshots() []TaskSnapshot {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	snapshots := make([]TaskSnapshot, 0, len(qm.tasks))
	for i := 1; i <= qm.seq; i++ {
		if t, ok := qm.tasks[i]; ok {
			snapshots = append(snapshots, TaskSnapshot{
				ID:     t.ID,
				Status: t.status,
				Site:   t.Site,
				Title:  t.Title,
			})
		}
	}
	return snapshots
}

// PauseTask pauses a single task. Running tasks pause at the next chunk
// boundary; pending tasks are skipped by the scheduler until resumed.
func (qm *QueueManager) PauseTask(id int) bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	t, ok := qm.tasks[id]
	if !ok {
		return false
	}
	switch t.status {
	case TaskStatusRunning, TaskStatusPending:
		t.pause.Pause()
		t.status = TaskStatusPaused
		return true
	}
	return false
}

// ResumeTask resumes a previously paused task.
func (qm *QueueManager) ResumeTask(id int) bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	t, ok := qm.tasks[id]
	if !ok || t.status != TaskStatusPaused {
		return false
	}
	t.pause.Resume()
	if _, running := qm.running[id]; running {
		t.status = TaskStatusRunning
	} else {
		t.status = TaskStatusPending
	}
	qm.signalLocked()
	return true
}

// signalLocked wakes the scheduler. The caller must hold qm.mu.
func (qm *QueueManager) signalLocked() {
	select {
	case qm.notify <- struct{}{}:
	default:
	}
}

// less orders tasks for scheduling: user-specified priority first (higher
// first), then site type (grouped alphabetically), then file size (smaller
// first), then enqueue order.
func less(a, b *Task) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if a.Site != b.Site {
		return a.Site < b.Site
	}
	if a.Size != b.Size {
		return a.Size < b.Size
	}
	return a.ID < b.ID
}

// popNextLocked removes and returns the highest-priority runnable pending
// task, or nil if there is none. Paused tasks are skipped.
func (qm *QueueManager) popNextLocked() *Task {
	best := -1
	for i, t := range qm.pending {
		if t.status != TaskStatusPending {
			continue
		}
		if best == -1 || less(t, qm.pending[best]) {
			best = i
		}
	}
	if best == -1 {
		return nil
	}
	t := qm.pending[best]
	qm.pending = append(qm.pending[:best], qm.pending[best+1:]...)
	return t
}

// promoteDelayedLocked moves delayed tasks whose retry time has come back to
// the pending queue and returns the time the next delayed task becomes ready.
func (qm *QueueManager) promoteDelayedLocked(now time.Time) (nextReady time.Time, hasNext bool) {
	remaining := qm.delayed[:0]
	for _, t := range qm.delayed {
		if !t.readyAt.After(now) {
			t.status = TaskStatusPending
			qm.pending = append(qm.pending, t)
			continue
		}
		remaining = append(remaining, t)
		if !hasNext || t.readyAt.Before(nextReady) {
			nextReady, hasNext = t.readyAt, true
		}
	}
	qm.delayed = remaining
	return nextReady, hasNext
}

func (qm *QueueManager) runTask(t *Task) {
	var err error
	if qm.opt.Runner != nil {
		err = qm.opt.Runner(t)
	} else {
		err = t.Downloader.Download(t.Data)
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.running, t.ID)
	if err == nil {
		t.status = TaskStatusDone
		qm.results = append(qm.results, TaskResult{Task: t})
	} else {
		t.attempts++
		if t.attempts <= qm.opt.QueueRetryTimes {
			// Task-level retry: put the task into the delayed queue and
			// re-schedule it later. This is distinct from the per-chunk
			// retry inside the downloader (--retry).
			t.status = TaskStatusDelayed
			t.readyAt = time.Now().Add(qm.opt.RetryDelay * time.Duration(t.attempts))
			qm.delayed = append(qm.delayed, t)
		} else {
			t.status = TaskStatusFailed
			t.err = err
			qm.results = append(qm.results, TaskResult{Task: t, Err: err})
		}
	}
	qm.signalLocked()
}

// Run starts scheduling and blocks until all tasks are done or failed.
// Whenever a task finishes, the next highest-priority task is picked up
// automatically. It returns the results of all finished tasks.
func (qm *QueueManager) Run() []TaskResult {
	for {
		qm.mu.Lock()
		now := time.Now()
		nextReady, hasDelayed := qm.promoteDelayedLocked(now)

		// Start tasks while there are free slots. A finished task signals
		// the scheduler which then automatically picks the next one.
		for len(qm.running) < qm.opt.MaxConcurrent {
			t := qm.popNextLocked()
			if t == nil {
				break
			}
			t.status = TaskStatusRunning
			qm.running[t.ID] = t
			go qm.runTask(t)
		}

		finished := len(qm.pending) == 0 && len(qm.delayed) == 0 && len(qm.running) == 0
		if finished {
			results := qm.results
			qm.mu.Unlock()
			return results
		}

		var timer *time.Timer
		var timerC <-chan time.Time
		if hasDelayed {
			timer = time.NewTimer(time.Until(nextReady))
			timerC = timer.C
		}
		qm.mu.Unlock()

		select {
		case <-qm.notify:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// sortTasksByID sorts results by task ID for deterministic reporting.
func sortResultsByID(results []TaskResult) {
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Task.ID < results[j].Task.ID
	})
}
