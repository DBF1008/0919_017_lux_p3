package downloader

import "sync"

// PauseController controls the pause/resume state of a single download task.
// It is safe for concurrent use. A paused controller blocks in Wait until
// Resume is called.
type PauseController struct {
	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
}

// NewPauseController returns a new PauseController in the resumed state.
func NewPauseController() *PauseController {
	p := &PauseController{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Pause marks the controller as paused. Subsequent Wait calls block until Resume.
func (p *PauseController) Pause() {
	p.mu.Lock()
	p.paused = true
	p.mu.Unlock()
}

// Resume marks the controller as running and wakes up all blocked Wait calls.
func (p *PauseController) Resume() {
	p.mu.Lock()
	p.paused = false
	p.cond.Broadcast()
	p.mu.Unlock()
}

// IsPaused reports whether the controller is currently paused.
func (p *PauseController) IsPaused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// Wait blocks while the controller is paused. It returns immediately when
// the controller is not paused.
func (p *PauseController) Wait() {
	p.mu.Lock()
	for p.paused {
		p.cond.Wait()
	}
	p.mu.Unlock()
}
