package group

import (
	"net"
	"sync"
)

// Listener is a per-proxy virtual listener that receives connections
// distributed by the group worker via round-robin. It implements net.Listener.
type Listener struct {
	ch      chan net.Conn
	addr    net.Addr
	closeCh chan struct{}
	onClose func(*Listener)
	once    sync.Once
}

func newListener(addr net.Addr, onClose func(*Listener)) *Listener {
	return &Listener{
		ch:      make(chan net.Conn, 1),
		addr:    addr,
		closeCh: make(chan struct{}),
		onClose: onClose,
	}
}

func (ln *Listener) Accept() (net.Conn, error) {
	select {
	case <-ln.closeCh:
		return nil, ErrListenerClosed
	case c, ok := <-ln.ch:
		if !ok {
			return nil, ErrListenerClosed
		}
		return c, nil
	}
}

func (ln *Listener) Addr() net.Addr {
	return ln.addr
}

func (ln *Listener) Close() error {
	ln.once.Do(func() {
		close(ln.closeCh)
		ln.onClose(ln)
	})
	return nil
}
