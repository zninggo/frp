package group

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLn is a controllable net.Listener for tests.
type fakeLn struct {
	connCh chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newFakeLn() *fakeLn {
	return &fakeLn{
		connCh: make(chan net.Conn, 8),
		closed: make(chan struct{}),
	}
}

func (f *fakeLn) Accept() (net.Conn, error) {
	select {
	case c := <-f.connCh:
		return c, nil
	case <-f.closed:
		return nil, net.ErrClosed
	}
}

func (f *fakeLn) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeLn) Addr() net.Addr { return fakeAddr("127.0.0.1:9999") }

func (f *fakeLn) inject(c net.Conn) {
	select {
	case f.connCh <- c:
	case <-f.closed:
	}
}

func TestBaseGroup_WorkerFanOut(t *testing.T) {
	fl := newFakeLn()
	var bg baseGroup
	bg.initBase("g", "key", fl, func() {})

	bg.mu.Lock()
	ln := bg.newListener(fl.Addr())
	bg.mu.Unlock()

	go bg.worker(fl)

	c1, c2 := net.Pipe()
	defer c2.Close()
	fl.inject(c1)

	select {
	case got := <-ln.ch:
		assert.Equal(t, c1, got)
		got.Close()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection on listener channel")
	}

	fl.Close()
}

func TestBaseGroup_WorkerStopsOnListenerClose(t *testing.T) {
	fl := newFakeLn()
	var bg baseGroup
	bg.initBase("g", "key", fl, func() {})

	done := make(chan struct{})
	go func() {
		bg.worker(fl)
		close(done)
	}()

	fl.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after listener close")
	}
}

func TestBaseGroup_WorkerDistributesRoundRobin(t *testing.T) {
	fl := newFakeLn()
	var bg baseGroup
	bg.initBase("g", "key", fl, func() {})

	bg.mu.Lock()
	ln1 := bg.newListener(fl.Addr())
	ln2 := bg.newListener(fl.Addr())
	bg.mu.Unlock()

	go bg.worker(fl)

	// Send connections one at a time so the worker can dispatch each
	// before the next arrives, ensuring deterministic round-robin.
	// Add(1) returns: 1%2=1->ln2, 2%2=0->ln1, 3%2=1->ln2, 4%2=0->ln1
	ln1Count := 0
	ln2Count := 0
	for range 4 {
		c1, c2 := net.Pipe()
		fl.inject(c1)

		select {
		case <-ln1.ch:
			ln1Count++
			c1.Close()
		case <-ln2.ch:
			ln2Count++
			c1.Close()
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for connection")
		}
		c2.Close()
	}
	assert.Equal(t, 2, ln1Count, "ln1 should receive 2 connections")
	assert.Equal(t, 2, ln2Count, "ln2 should receive 2 connections")

	fl.Close()
}

func TestBaseGroup_CloseLastListenerTriggersCleanup(t *testing.T) {
	fl := newFakeLn()
	var bg baseGroup
	cleanupCalled := 0
	bg.initBase("g", "key", fl, func() { cleanupCalled++ })

	bg.mu.Lock()
	ln1 := bg.newListener(fl.Addr())
	ln2 := bg.newListener(fl.Addr())
	bg.mu.Unlock()

	go bg.worker(fl)

	ln1.Close()
	assert.Equal(t, 0, cleanupCalled, "cleanup should not run while listeners remain")

	ln2.Close()
	assert.Equal(t, 1, cleanupCalled, "cleanup should run after last listener closed")
}

func TestBaseGroup_CloseOneOfTwoListeners(t *testing.T) {
	fl := newFakeLn()
	var bg baseGroup
	cleanupCalled := 0
	bg.initBase("g", "key", fl, func() { cleanupCalled++ })

	bg.mu.Lock()
	ln1 := bg.newListener(fl.Addr())
	ln2 := bg.newListener(fl.Addr())
	bg.mu.Unlock()

	go bg.worker(fl)

	ln1.Close()
	assert.Equal(t, 0, cleanupCalled)

	// ln2 should still receive connections.
	c1, c2 := net.Pipe()
	defer c2.Close()
	fl.inject(c1)

	got, err := ln2.Accept()
	require.NoError(t, err)
	assert.Equal(t, c1, got)
	got.Close()

	ln2.Close()
	assert.Equal(t, 1, cleanupCalled)
}
