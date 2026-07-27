package group

import (
	"net"
	"sync"
	"sync/atomic"
)

// baseGroup contains the shared plumbing for listener-based groups
// (TCP, HTTPS, TCPMux). Each concrete group embeds this and provides
// its own Listen method with protocol-specific validation.
type baseGroup struct {
	group    string
	groupKey string

	realLn    net.Listener
	lns       []*Listener
	mu        sync.Mutex
	cleanupFn func()
	index     atomic.Uint64
}

// initBase resets the baseGroup for a fresh listen cycle.
// Must be called under mu when len(lns) == 0.
func (bg *baseGroup) initBase(group, groupKey string, realLn net.Listener, cleanupFn func()) {
	bg.group = group
	bg.groupKey = groupKey
	bg.realLn = realLn
	bg.cleanupFn = cleanupFn
	bg.index.Store(0)
}

// worker reads from the real listener and distributes connections to
// virtual listeners using round-robin selection.
func (bg *baseGroup) worker(realLn net.Listener) {
	for {
		c, err := realLn.Accept()
		if err != nil {
			return
		}
		bg.dispatch(c)
	}
}

// dispatch sends a connection to one of the virtual listeners using
// round-robin. If the chosen listener's channel is full or closed, it
// retries once; on persistent failure the connection is closed.
func (bg *baseGroup) dispatch(c net.Conn) {
	for range 2 {
		bg.mu.Lock()
		n := len(bg.lns)
		var ln *Listener
		if n > 0 {
			ln = bg.lns[bg.index.Add(1)%uint64(n)]
		}
		bg.mu.Unlock()

		if ln == nil {
			c.Close()
			return
		}

		select {
		case ln.ch <- c:
			return
		default:
		}
	}
	c.Close()
}

// newListener creates a new Listener with its own channel, wired to this baseGroup.
// Must be called under mu.
func (bg *baseGroup) newListener(addr net.Addr) *Listener {
	ln := newListener(addr, bg.closeListener)
	bg.lns = append(bg.lns, ln)
	return ln
}

// closeListener removes ln from the list and closes its channel.
// When the last listener is removed, it closes the real listener and
// calls cleanupFn.
func (bg *baseGroup) closeListener(ln *Listener) {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	for i, l := range bg.lns {
		if l == ln {
			bg.lns = append(bg.lns[:i], bg.lns[i+1:]...)
			break
		}
	}
	close(ln.ch)
	if len(bg.lns) == 0 {
		bg.realLn.Close()
		bg.cleanupFn()
	}
}
