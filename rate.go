// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"log"
	"math"
	"sync/atomic"
	"time"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

const timeShift = 19
const sentinalShift = timeShift - 1
const tokensMask = (1 << sentinalShift) - 1
const maxTokens = tokensMask

type packedState uint64

func newPackedState(newSinceBase int64, tokens int32) packedState {
	if newSinceBase < 0 {
		newSinceBase = 0
	}
	if tokens < 0 {
		tokens = 0
	}
	return packedState((uint64(newSinceBase) << timeShift) | (0x1 << sentinalShift) | (uint64(tokens) & tokensMask))
}

func (ps packedState) tokens() int32 {
	return int32(ps & tokensMask)
}

func (ps packedState) timeMicros() int64 {
	return int64(ps >> timeShift)
}

func (ps packedState) ok() bool {
	return ((ps >> sentinalShift) & 0x1) == 0x1
}

func (ps packedState) Unpack() (sinceBase time.Duration, tokens int32, ok bool) {
	return time.Duration(ps.timeMicros()) * time.Microsecond, ps.tokens(), ps.ok()
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has three main methods, Allow, Reserve, and Wait.
// Most callers should use Wait.
//
// Each of the three methods consumes a single token.
// They differ in their behavior when no token is available.
// If no token is available, Allow returns false.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it.
// If no token is available, Wait blocks until one can be obtained
// or its associated context.Context is canceled.
//
// The methods AllowN, ReserveN, and WaitN consume n tokens.
type Limiter struct {
	// static config
	limit Limit

	base time.Time

	// mostly static, unless limit == 0 then atomically decremented
	burst int64

	state uint64 // packedState

	// pad this Limiter struct to 64-bytes to avoid false sharing
	_padding [64 - 8 - 24 - 8 - 8]byte
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	return lim.limit
}

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless limit == Inf.
func (lim *Limiter) Burst() int {
	burst := atomic.LoadInt64(&lim.burst)
	return int(burst)
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(r Limit, b int) *Limiter {
	if b > maxTokens {
		log.Printf("TODO: warn about this or something")
		b = maxTokens
	}
	return &Limiter{
		limit: r,
		base:  time.Now(),
		burst: int64(b),
		state: uint64(newPackedState(0, int32(b))),
	}
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow() bool {
	return lim.reserve(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(1<<63 - 1)

func (lim *Limiter) loadState() packedState {
	return packedState(atomic.LoadUint64(&lim.state))
}

// binnedNow is time.Now() with 1 microsecond precision (on linux its usually higher)
func (lim *Limiter) binnedTime(now time.Time) int64 {
	return now.UnixMicro()
}

// reserve is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserve returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
func (lim *Limiter) reserve(now time.Time) bool {
	if lim.limit == Inf {
		return true
	} else if lim.limit == 0 {
		var ok bool
		if lim.burst >= 1 {
			newBurst := atomic.AddInt64(&lim.burst, -1)
			ok = newBurst >= 0
		}
		return ok
	}

	binnedNow := lim.binnedTime(now)
	for i := 0; ; i++ {
		currState := lim.loadState()
		sinceBase, tokens, ok := currState.Unpack()
		if !ok {
			// TODO: packed state is bad: either we had an underflow or first time
			//       through and need to initialize
			log.Printf("TODO: packed state is bad: either we had an underflow or first time through and need to initialize\n")
			return false
		}
		stateTime := lim.base.Add(sinceBase).UnixMicro()

		// check if we are in the current epoch (or the state's epoch
		// is in the 'future', which we treat as == current)
		if binnedNow == stateTime {
			if tokens <= 0 {
				// fail early to scale "obviously rate limited" traffic
				return false
			} else {
				// there are tokens, and we're in the same epoch as currState
				newPackedState := packedState(atomic.AddUint64(&lim.state, ^uint64(0)))
				// TODO: is there sanity checking we should do around checking writeTime?
				_, writeTokens, writeOk := newPackedState.Unpack()
				if writeOk && writeTokens >= 0 {
					return true
				}
			}
		} else {
			// slow path

			newNowMicros, tokens := lim.advance(binnedNow, stateTime, int64(tokens))
			if tokens < 1 {
				// if there are no tokens available, return
				return false
			}

			// consume 1 token
			tokens--

			newSinceBase := newNowMicros - lim.base.UnixMicro()
			nextState := newPackedState(newSinceBase, tokens)
			if ok := atomic.CompareAndSwapUint64(&lim.state, uint64(currState), uint64(nextState)); ok {
				// CAS worked (we won the race)
				return true
			}

			// CAS failed (someone else won the race), fallthrough.  That means
			// with high probability lim.state's time epoch is likely to be equal
			// to our epoch in the next iteration, avoiding doing CAS two iterations
			// in a row
		}
	}
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func (lim *Limiter) advance(nowMicros, lastMicros int64, oldTokens int64) (newNowMicros int64, newTokens int32) {
	now := time.UnixMicro(nowMicros)
	last := time.UnixMicro(lastMicros)
	if now.Before(last) {
		last = now
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := now.Sub(last)
	delta := lim.limit.tokensFromDuration(elapsed)

	tokens := float64(oldTokens) + delta
	if burst := float64(atomic.LoadInt64(&lim.burst)); tokens > burst {
		tokens = burst
	}

	wholeTokens := int64(tokens)
	if wholeTokens > maxTokens {
		wholeTokens = maxTokens
	} else {
		// if we don't adjust "now" we lose fractional tokens and rate limit
		// at a substantially different rate than users specified.
		remaining := tokens - float64(wholeTokens)
		adjust := lim.limit.durationFromTokens(remaining)
		adjustedNow := now.Add(-adjust)
		// this should never happen be triggered, but just in case ensure
		// at least that the time tracked in lim.state doesn't go backwards.
		if !adjustedNow.Before(last) {
			nowMicros = adjustedNow.UnixMicro()
		}
	}
	return nowMicros, int32(wholeTokens)
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	if limit <= 0 {
		return InfDuration
	}
	seconds := tokens / float64(limit)
	return time.Duration(float64(time.Second) * seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	if limit <= 0 {
		return 0
	}
	return d.Seconds() * float64(limit)
}
