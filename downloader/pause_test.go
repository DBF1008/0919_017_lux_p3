package downloader

import (
	"testing"
	"time"
)

func TestPauseControllerBlocksWhilePaused(t *testing.T) {
	p := NewPauseController()
	if p.IsPaused() {
		t.Fatal("new controller should not be paused")
	}

	// Wait returns immediately when not paused
	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should return immediately when not paused")
	}

	// Wait blocks while paused
	p.Pause()
	if !p.IsPaused() {
		t.Fatal("controller should be paused")
	}
	blocked := make(chan struct{})
	go func() {
		p.Wait()
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("Wait should block while paused")
	case <-time.After(50 * time.Millisecond):
	}

	// Resume wakes up the blocked Wait
	p.Resume()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("Wait should return after Resume")
	}
	if p.IsPaused() {
		t.Fatal("controller should not be paused after Resume")
	}
}

func TestDownloaderWaitIfPausedNilSafe(t *testing.T) {
	d := New(Options{})
	// must not panic without a PauseController
	d.waitIfPaused()
}
