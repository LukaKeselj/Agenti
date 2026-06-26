package actorframework

import (
	"fmt"
	"sync/atomic"
)

// ─────────────────────────────────────────────
// MailboxStrategy controls overflow behaviour.
// ─────────────────────────────────────────────

// MailboxStrategy defines what happens when a bounded mailbox is full.
type MailboxStrategy int

const (
	// DropNewest silently drops the incoming message when the buffer is full.
	DropNewest MailboxStrategy = iota
	// DropOldest removes the oldest message to make room for the new one.
	DropOldest
	// BlockSender blocks the caller until there is room (back-pressure).
	BlockSender
)

// DefaultMailboxSize is used when no explicit capacity is requested.
const DefaultMailboxSize = 256

// ─────────────────────────────────────────────
// Mailbox
// ─────────────────────────────────────────────

// Mailbox is a FIFO queue backed by a buffered Go channel.
// It is the only data path between actors; no actor may read
// another actor's mailbox directly.
//
// Two modes:
//   - Bounded  (capacity > 0): fixed-size buffer; overflow handled by Strategy.
//   - Unbounded (capacity == 0): uses an internal growable ring buffer
//     so Enqueue never blocks and never drops (memory is the only limit).
type Mailbox struct {
	ch       chan Message
	strategy MailboxStrategy
	bounded  bool

	// Unbounded mode: a simple goroutine-safe forwarding pump.
	unboundedIn  chan Message
	unboundedBuf []Message

	// Counters – accessed atomically.
	enqueued uint64
	dropped  uint64
}

// NewMailbox creates a bounded mailbox with the given capacity and strategy.
func NewMailbox(capacity int, strategy MailboxStrategy) *Mailbox {
	mb := &Mailbox{
		ch:       make(chan Message, capacity),
		strategy: strategy,
		bounded:  true,
	}
	return mb
}

// NewUnboundedMailbox creates a mailbox that never blocks or drops messages.
// It uses a background goroutine to drain an input channel into a growable
// slice and then forward messages onto the consumer channel.
func NewUnboundedMailbox() *Mailbox {
	mb := &Mailbox{
		ch:          make(chan Message, DefaultMailboxSize),
		unboundedIn: make(chan Message, DefaultMailboxSize),
		bounded:     false,
	}
	go mb.pump()
	return mb
}

// pump runs the unbounded relay goroutine.
func (mb *Mailbox) pump() {
	for {
		// If the buffer is empty, block waiting for the first message.
		if len(mb.unboundedBuf) == 0 {
			msg, ok := <-mb.unboundedIn
			if !ok {
				return // mailbox closed
			}
			mb.unboundedBuf = append(mb.unboundedBuf, msg)
		}

		// Try to forward the head of the buffer without blocking.
		select {
		case mb.ch <- mb.unboundedBuf[0]:
			mb.unboundedBuf = mb.unboundedBuf[1:]
		case msg, ok := <-mb.unboundedIn:
			if !ok {
				return
			}
			mb.unboundedBuf = append(mb.unboundedBuf, msg)
		}
	}
}

// ─────────────────────────────────────────────
// Enqueue
// ─────────────────────────────────────────────

// Enqueue adds msg to the mailbox according to the configured strategy.
func (mb *Mailbox) Enqueue(msg Message) {
	atomic.AddUint64(&mb.enqueued, 1)

	if !mb.bounded {
		mb.unboundedIn <- msg
		return
	}

	switch mb.strategy {
	case BlockSender:
		mb.ch <- msg // blocks if full – intentional back-pressure

	case DropNewest:
		select {
		case mb.ch <- msg:
		default:
			atomic.AddUint64(&mb.dropped, 1)
			fmt.Printf("[mailbox] WARN: bounded mailbox full – dropped newest message %q\n", msg.MsgType)
		}

	case DropOldest:
		for {
			select {
			case mb.ch <- msg:
				return
			default:
				// Drain one old message to make room.
				select {
				case <-mb.ch:
					atomic.AddUint64(&mb.dropped, 1)
				default:
				}
			}
		}
	}
}

// ─────────────────────────────────────────────
// Dequeue / channel access
// ─────────────────────────────────────────────

// C returns the receive-only channel that the actor's run loop reads from.
// Only the owning actor's goroutine should read from this channel.
func (mb *Mailbox) C() <-chan Message {
	return mb.ch
}

// Close shuts down the mailbox.  After Close, Enqueue is a no-op and
// the consumer channel will be drained and then closed.
func (mb *Mailbox) Close() {
	if !mb.bounded {
		close(mb.unboundedIn)
	} else {
		close(mb.ch)
	}
}

// ─────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────

// Stats returns a snapshot of mailbox counters.
type MailboxStats struct {
	Enqueued uint64
	Dropped  uint64
	Pending  int
}

func (mb *Mailbox) Stats() MailboxStats {
	return MailboxStats{
		Enqueued: atomic.LoadUint64(&mb.enqueued),
		Dropped:  atomic.LoadUint64(&mb.dropped),
		Pending:  len(mb.ch),
	}
}
