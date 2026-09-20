package downloader

import (
	"errors"
	"sync"
	"time"

	"github.com/iawia002/lux/extractors"
)

// TaskStatus describes the lifecycle state of a download task in the queue.
type TaskStatus string

// Task statuses.
const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusPaused  TaskStatus = "paused"
	// TaskStatusDelayed means the task failed and is waiting in the delay
	// queue to be rescheduled (task-level retry, different from the
	// in-task retry of the downloader itself).
	TaskStatusDelayed TaskStatus = "delayed"
	TaskStatusDone    TaskStatus = "done"
	TaskStatusFailed  TaskStatus = "failed"
)

// errEmptyTaskData is a system-level error: the task itself is invalid.
var errEmptyTaskData = errors.New("download queue: task has no data")

// PauseController controls pause/resume of a single task.
// The zero value is ready to use.
type PauseController struct {
	mu      sync.Mutex
	paused  bool
	resumeC chan struct{}
}

// NewPauseController returns a new PauseController.
func NewPauseController() *PauseController {
	return &PauseController{}
}

// Pause marks the controller as paused, subsequent Wait calls block until Resume.
func (p *PauseController) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		return
	}
	p.paused = true
	p.resumeC = make(chan struct{})
}

// Resume wakes up all blocked Wait calls.
func (p *PauseController) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.paused = false
	close(p.resumeC)
}

// Paused reports whether the controller is paused.
func (p *PauseController) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// Wait blocks while the controller is paused.
func (p *PauseController) Wait() {
	p.mu.Lock()
	if !p.paused {
		p.mu.Unlock()
		return
	}
	ch := p.resumeC
	p.mu.Unlock()
	<-ch
}

// Task is a single download task managed by the QueueManager.
type Task struct {
	ID   int
	Data *extractors.Data
	// Priority is specified by the user, a larger value means scheduling earlier.
	Priority int
	Site     string
	Size     int64

	// Attempts is the number of times the task has been executed.
	Attempts int
	Status   TaskStatus
	Err      error

	pause *PauseController
}

// PauseController returns the pause controller bound to this task.
func (t *Task) PauseController() *PauseController {
	return t.pause
}

// QueueOptions defines options used by the QueueManager.
type QueueOptions struct {
	// MaxConcurrent is the global limit of concurrent download tasks.
	MaxConcurrent int
	// MaxTaskRetries is how many times a failed task is rescheduled by the
	// queue (task-level retry). It is different from Options.RetryTimes,
	// which retries inside a single task execution.
	MaxTaskRetries int
	// RetryDelay is how long a failed task stays in the delay queue before
	// being rescheduled.
	RetryDelay time.Duration
}

// TaskError records a task-level failure (retries exhausted).
type TaskError struct {
	Task *Task
	Err  error
}

// Result is the outcome of a QueueManager.Run call.
type Result struct {
	// TaskErrors are task-level failures: the task ran but failed after all
	// task-level retries.
	TaskErrors []TaskError
	// SystemErrors are system-level failures: the task could not even be
	// executed (eg: empty task data).
	SystemErrors []error
}

// DownloadFunc executes a single task, normally it calls Downloader.Download.
type DownloadFunc func(task *Task) error

// QueueManager schedules download tasks with priority ordering and a global
// concurrency limit, and supports pause/resume and task-level retry.
type QueueManager struct {
	option QueueOptions

	mu       sync.Mutex
	tasks    []*Task
	running  int
	notifyCh chan struct{}
	result   *Result
}

// NewQueueManager returns a new QueueManager.
func NewQueueManager(option QueueOptions) *QueueManager {
	if option.MaxConcurrent <= 0 {
		option.MaxConcurrent = 1
	}
	if option.RetryDelay <= 0 {
		option.RetryDelay = 5 * time.Second
	}
	return &QueueManager{
		option:   option,
		notifyCh: make(chan struct{}, 1),
		result:   &Result{},
	}
}

// Add creates a new task for the given data and puts it into the queue.
// priority is specified by the user, a larger value means scheduling earlier.
func (qm *QueueManager) Add(data *extractors.Data, priority int) *Task {
	task := &Task{
		Data:     data,
		Priority: priority,
		Status:   TaskStatusPending,
		pause:    NewPauseController(),
	}
	if data != nil {
		task.Site = data.Site
		task.Size = dataSize(data)
	}

	qm.mu.Lock()
	task.ID = len(qm.tasks) + 1
	qm.tasks = append(qm.tasks, task)
	qm.mu.Unlock()
	qm.notify()
	return task
}

// Tasks returns a snapshot of all tasks in the queue.
func (qm *QueueManager) Tasks() []*Task {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	tasks := make([]*Task, len(qm.tasks))
	copy(tasks, qm.tasks)
	return tasks
}

// Pause pauses the task with the given ID. A pending or delayed task will not
// be scheduled until resumed; a running task pauses at the next checkpoint of
// the downloader.
func (qm *QueueManager) Pause(id int) bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	task := qm.findLocked(id)
	if task == nil {
		return false
	}
	switch task.Status {
	case TaskStatusPending, TaskStatusDelayed:
		task.Status = TaskStatusPaused
		return true
	case TaskStatusRunning:
		task.pause.Pause()
		return true
	}
	return false
}

// Resume resumes the paused task with the given ID.
func (qm *QueueManager) Resume(id int) bool {
	qm.mu.Lock()
	task := qm.findLocked(id)
	if task == nil {
		qm.mu.Unlock()
		return false
	}
	if task.Status == TaskStatusPaused {
		task.Status = TaskStatusPending
	} else {
		task.pause.Resume()
	}
	qm.mu.Unlock()
	qm.notify()
	return true
}

// Run schedules and executes all tasks in the queue and returns the result.
// Tasks still paused when the queue drains are left untouched (skipped).
func (qm *QueueManager) Run(fn DownloadFunc) *Result {
	sem := make(chan struct{}, qm.option.MaxConcurrent)
	var wg sync.WaitGroup
	for {
		qm.mu.Lock()
		task := qm.nextTaskLocked()
		if task == nil {
			idle := qm.running == 0 && !qm.hasSchedulableLocked()
			qm.mu.Unlock()
			if idle {
				break
			}
			// wait for a task to finish, expire or be resumed
			<-qm.notifyCh
			continue
		}
		task.Status = TaskStatusRunning
		qm.running++
		qm.mu.Unlock()

		sem <- struct{}{} // block when the global concurrency limit is reached
		wg.Add(1)
		go func(t *Task) {
			defer wg.Done()
			defer func() { <-sem }()
			qm.execute(t, fn)
		}(task)
	}
	wg.Wait()
	return qm.result
}

func (qm *QueueManager) execute(task *Task, fn DownloadFunc) {
	if task.Data == nil {
		// system-level failure: the task can not be executed at all
		qm.mu.Lock()
		qm.running--
		task.Status = TaskStatusFailed
		task.Err = errEmptyTaskData
		qm.result.SystemErrors = append(qm.result.SystemErrors, errEmptyTaskData)
		qm.mu.Unlock()
		qm.notify()
		return
	}

	task.Attempts++
	err := fn(task)

	qm.mu.Lock()
	qm.running--
	switch {
	case err == nil:
		task.Status = TaskStatusDone
	case task.Attempts <= qm.option.MaxTaskRetries:
		// task-level retry: put the failed task into the delay queue and
		// reschedule it later
		task.Err = err
		task.Status = TaskStatusDelayed
		delay := qm.option.RetryDelay
		time.AfterFunc(delay, func() {
			qm.mu.Lock()
			if task.Status == TaskStatusDelayed {
				task.Status = TaskStatusPending
			}
			qm.mu.Unlock()
			qm.notify()
		})
	default:
		task.Status = TaskStatusFailed
		task.Err = err
		qm.result.TaskErrors = append(qm.result.TaskErrors, TaskError{Task: task, Err: err})
	}
	qm.mu.Unlock()
	qm.notify()
}

func (qm *QueueManager) findLocked(id int) *Task {
	for _, task := range qm.tasks {
		if task.ID == id {
			return task
		}
	}
	return nil
}

// nextTaskLocked returns the pending task that should be scheduled first:
// higher user priority first, then grouped by site, then smaller size first.
func (qm *QueueManager) nextTaskLocked() *Task {
	var best *Task
	for _, task := range qm.tasks {
		if task.Status != TaskStatusPending {
			continue
		}
		if best == nil || taskLess(task, best) {
			best = task
		}
	}
	return best
}

// hasSchedulableLocked reports whether there are tasks that may still run:
// pending tasks or delayed tasks waiting for task-level retry.
func (qm *QueueManager) hasSchedulableLocked() bool {
	for _, task := range qm.tasks {
		if task.Status == TaskStatusPending || task.Status == TaskStatusDelayed {
			return true
		}
	}
	return false
}

func (qm *QueueManager) notify() {
	select {
	case qm.notifyCh <- struct{}{}:
	default:
	}
}

func taskLess(a, b *Task) bool {
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

func dataSize(data *extractors.Data) int64 {
	var size int64
	for _, stream := range data.Streams {
		if stream.Size > size {
			size = stream.Size
		}
	}
	return size
}
