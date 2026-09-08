package ingest

import "sync"

// envelopeBufPool recycles the per-request []*Envelope scratch slice. Without
// it every export request allocates a slice header plus backing array sized to
// the number of distinct traces in the batch, which on the hot path is one of
// the larger remaining allocations.
var envelopeBufPool = sync.Pool{
	New: func() any {
		b := make([]*Envelope, 0, 64)
		return &b
	},
}

func envelopeBufGet() []*Envelope {
	return (*(envelopeBufPool.Get().(*[]*Envelope)))[:0]
}

func envelopeBufPut(b []*Envelope) {
	// Do not retain a slice that one pathological request grew without bound.
	const maxRetained = 4096
	if cap(b) > maxRetained {
		return
	}
	b = b[:0]
	envelopeBufPool.Put(&b)
}
