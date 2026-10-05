// Package agent is minilog's vlagent: a durable, replicating forwarder that
// sits in front of storage nodes and guarantees delivery across outages and
// its own restarts.
//
// ---------------------------------------------------------------------------
// STAGE 11 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go test ./harness -run TestStage11 -v
//
// Reference: app/vlagent/remotewrite/remotewrite.go (one context per URL,
// pushToRemoteStorages), client.go (runWorker, sendBlockHTTP -- read this one
// for the 400 rule and the backoff), pendinglogrows.go (the in-memory block
// in front of the queue).
// ---------------------------------------------------------------------------
package agent

import (
	"time"

	"github.com/niladrix719/minilog/minilog"
)

// Where this sits, and why it is a separate process in the real system:
//
//	app  ->  agent  ->  storage node(s)
//
// netinsert (stage 7) lives inside the insert node and buffers in memory.
// The agent lives next to the APPLICATION and buffers on disk. That is the
// difference between "the cluster is down for two minutes and we lost two
// minutes of logs" and "the cluster is down for two minutes and the logs
// arrive two minutes late." The second one is what people mean by a log
// pipeline.
//
// The agent speaks the internal insert protocol directly (InsertPath, with
// the version arg) -- the same thing vlagent's -remoteWrite.format=native
// does, and the same wire format stage 6 already built. Nothing new crosses
// the network; what is new is what happens on THIS side of it.

// DefaultMaxBlockSize is the encoded size at which the in-memory block is
// pushed to the queues without waiting for the flush ticker.
//
// Same knob as netinsert's MaxInsertBlockSize and for the same reason: a
// block is a request, and a request has a fixed cost.
const DefaultMaxBlockSize = 2 * 1024 * 1024

// DefaultFlushInterval bounds how long rows sit in memory before reaching
// the queue.
//
// This is the only window left in which process death loses rows, and it is
// the number to say out loud when someone asks "is it durable": rows are on
// disk within this interval, and MustFlush makes it now.
const DefaultFlushInterval = time.Second

// DefaultRetryMinInterval and DefaultRetryMaxInterval bound the backoff.
//
// A failed send waits Min, then 2*Min, then 4*Min ... capped at Max, then
// tries again, forever. Forever is correct: the block is on disk, the
// destination will come back, and the only question is how often to check.
// Retrying at a fixed fast rate turns a dead node into a busy-loop; retrying
// at a fixed slow rate turns a one-second blip into a minute of latency.
// Exponential-with-cap is the standard answer and the real one uses it.
const (
	DefaultRetryMinInterval = 100 * time.Millisecond
	DefaultRetryMaxInterval = 10 * time.Second
)

// Config configures an Agent.
type Config struct {
	// Addrs are the destinations, e.g. "127.0.0.1:9001". Every block goes to
	// EVERY address. One address is a durable forwarder; two or more is
	// replication -- the same rows in two places -- which is how vlagent
	// gives you HA without the cluster itself knowing the word.
	//
	// Each address gets its own queue and its own sender. This is the
	// property stage 11 tests hardest: a dead destination must not slow a
	// live one by a single millisecond.
	Addrs []string

	// DataPath is the directory for the on-disk queues. One subdirectory per
	// destination, named so that the same Addrs reopen the same queues.
	// Reopening with a NEW address list must not silently pick up another
	// destination's backlog -- name the subdirectory after the address.
	DataPath string

	// MaxPendingBytes bounds each destination's queue. 0 means unbounded,
	// which means "until the disk is full", which is a choice too.
	MaxPendingBytes int64

	// MaxBlockSize is the in-memory block size. 0 means DefaultMaxBlockSize.
	MaxBlockSize int

	// FlushInterval bounds in-memory residency. 0 means DefaultFlushInterval.
	FlushInterval time.Duration

	// RetryMinInterval and RetryMaxInterval bound the send backoff.
	// 0 means the defaults. Tests set these low.
	RetryMinInterval time.Duration
	RetryMaxInterval time.Duration
}

// DestStats is one destination's view of the world.
type DestStats struct {
	Addr string

	// Queue is the on-disk state. Queue.PendingBytes == 0 means everything
	// this agent ever accepted for this destination has been acknowledged.
	Queue QueueStats

	BlocksSent int64
	BytesSent  int64

	// Retries counts sends that failed and were tried again. During an outage
	// this climbs; after it, it stops. If it climbs while the destination is
	// healthy, something is wrong that a retry will not fix.
	Retries int64

	// BlocksRejected counts blocks the destination refused with a 4xx. They
	// were dropped, not retried, and the reason is in the log. See
	// sendBlock for why retrying them would be worse than dropping them.
	BlocksRejected int64
}

// Agent accepts rows and guarantees their delivery to every destination.
//
// You will need: a []*destination, the resolved config, the in-memory block
// under construction (encoded bytes, a row count, a mutex, and the time it
// was started), a stop channel, and a WaitGroup for the senders and the
// flush ticker.
type Agent struct {
	// TODO stage 11
}

// destination is one address: its queue, its sender, its counters.
//
// You will need: the address, the *queue, a mutex that serializes queue
// access between MustFlush (writer) and the sender (reader), a way for the
// sender to WAIT when the queue is empty rather than spin -- sync.Cond over
// that mutex is the standard tool, and the real one uses exactly that -- an
// *http.Client with its own Transport (stage 9 taught you why), and the
// counters behind DestStats.
type destination struct {
	// TODO stage 11
}

// NewAgent opens (or creates) the queues under cfg.DataPath and starts one
// sender per destination and one flush ticker.
//
// Opening must succeed with a backlog present: that is the restart case, and
// the senders should start draining it before the first MustAddRows.
func NewAgent(cfg *Config) *Agent {
	panic("TODO stage 11: implement NewAgent")
}

// MustAddRows encodes rows into the in-memory block and returns.
//
// Returns when the rows are in MEMORY. They reach disk at the next flush --
// MaxBlockSize or FlushInterval, whichever first -- or when MustFlush is
// called. So the durability contract is:
//
//	MustAddRows returns  ->  rows survive anything except process death
//	                         within FlushInterval
//	MustFlush returns    ->  rows survive process death
//
// Compare stage 7's table. The window did not go away; it got small, bounded,
// and named. You could close it entirely by writing to the queue inside this
// call -- one fsync per MustAddRows. Try it, measure the throughput, and then
// decide. That measurement is stage 11's first number.
//
// Encode with internalapi.MarshalRowBatch, once, and append the encoded
// bytes to EVERY destination's queue at flush time. Encoding once and
// writing N times is the cheap kind of replication.
func (a *Agent) MustAddRows(rows []minilog.Row) {
	panic("TODO stage 11: implement Agent.MustAddRows")
}

// MustFlush pushes the in-memory block to every destination's queue and
// returns once it is on disk. It does NOT wait for delivery.
//
// This is the call that makes "on disk" true right now instead of within
// FlushInterval. The senders pick it up from there.
func (a *Agent) MustFlush() {
	panic("TODO stage 11: implement Agent.MustFlush")
}

// WaitDrained flushes, then waits until every destination's queue is empty
// -- every block acknowledged -- or timeout passes. Returns whether it
// drained.
//
// Test hook, and the honest definition of "delivered". A false return with a
// destination down is correct behaviour, not a failure: the rows are on disk
// and will go when the destination returns.
func (a *Agent) WaitDrained(timeout time.Duration) bool {
	panic("TODO stage 11: implement Agent.WaitDrained")
}

// Stats returns per-destination counters, by Addrs index.
func (a *Agent) Stats() []DestStats {
	panic("TODO stage 11: implement Agent.Stats")
}

// MustClose flushes, stops the senders, and closes the queues.
//
// Stopping a sender mid-send: give it a moment to finish (the real one waits
// 5s), then stop anyway. The block it was sending is still at the head of
// the queue -- it was peeked, not acked -- so nothing is lost; at worst it is
// sent twice. Note the difference from netinsert.MustClose, which had to
// choose between hanging and dropping. The queue is what removed that
// choice.
func (a *Agent) MustClose() {
	panic("TODO stage 11: implement Agent.MustClose")
}

// sendResult is what one attempt to deliver a block came back with.
//
// Same three-way split as stage 9's sendInsertRequest, plus one: a 4xx is
// its own outcome here because the queue changes what it means. In stage 9
// a rejected block failed one request. Here a rejected block that is not
// dropped sits at the HEAD of the queue and is retried forever, and every
// block behind it waits forever. One bad block stops the pipeline. That is
// head-of-line blocking, and dropping the block is the only cure that does
// not involve a human.
type sendResult int

const (
	sendStored      sendResult = iota // 2xx: ack it
	sendUnavailable                   // no response: back off, retry
	sendServerError                   // 5xx: back off, retry
	sendRejected                      // 4xx: log it, drop it, move on
)

// sendBlock POSTs one block to d and classifies the outcome.
//
// Same request shape as netinsert.sendInsertRequest. Read the body on a
// non-2xx so the log line says WHY -- a rejected block with no reason in the
// log is a block somebody deletes from the queue by hand at 3am.
func (d *destination) sendBlock(block []byte) (sendResult, error) {
	panic("TODO stage 11: implement destination.sendBlock")
}

// runSender is the loop: wait for a block, send it until it is stored or
// rejected, ack it, repeat. Exits when stopCh closes.
//
// The shape:
//
//	for {
//	    block := peek, blocking (cond.Wait) while the queue is empty
//	    attempt := 0
//	    for {
//	        result := sendBlock(block)
//	        stored or rejected -> break
//	        sleep backoff(attempt), or return if stopCh closes first
//	        attempt++
//	        block = peek again   // see below
//	    }
//	    ack
//	}
//
// The peek and the ack take the destination's mutex; the send does NOT.
// Holding the lock across a network round-trip would stop MustFlush from
// writing for as long as the destination takes to answer, and during an
// outage that is forever.
//
// Peek AGAIN before every retry. While you were backing off, MustWriteBlock
// may have hit maxPendingBytes and dropped the head -- the very block you
// are holding. Sending it anyway delivers a block the queue has already
// counted as dropped, and the "oldest" that survives is no longer the
// oldest. The disk-bound test checks exactly which rows arrive.
func (d *destination) runSender(stopCh <-chan struct{}) {
	panic("TODO stage 11: implement destination.runSender")
}

// backoff returns how long to wait before retry number attempt (0-based):
// minInterval doubled attempt times, capped at maxInterval.
//
// Add jitter if you like. Think about what happens when a thousand agents
// all lose the same destination at the same second and all retry at exactly
// Min, 2*Min, 4*Min. The real one does not jitter here; vmagent's does.
func backoff(attempt int, minInterval, maxInterval time.Duration) time.Duration {
	panic("TODO stage 11: implement backoff")
}
