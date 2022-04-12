// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"log"
	"math"
	"sync"
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

func (ps packedState) Unpack() (sinceBaseMicros int64, tokens int32, ok bool) {
	return ps.timeMicros(), ps.tokens(), ps.ok()
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

	// to ensure we can pack larger burst values into state, split `lastUpdated`
	// between baseMicros and a smaller microsecond-based difference packed into
	// `state`.  Effectively `lastUpdate := lim.baseMicros + lim.state.timeMicros()`.
	// We keep a mutex here so that if there is a _large_ time correction we
	// synchronize while rebasing baseMicros.
	baseWriteMu sync.Mutex
	baseMicros  int64

	// mostly static, unless limit == 0 then atomically decremented
	burst int64

	state uint64 // packedState

	// pad this Limiter struct to 64-bytes to avoid false sharing
	_padding [64 - 8 - 8 - 8 - 8 - 8]byte
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
	l := &Limiter{
		limit: r,
		burst: int64(b),
	}

	l.reinit(time.Now().UnixMicro())

	return l
}

//go:noinline
func (lim *Limiter) reinit(nowMicros int64) {
	lim.baseWriteMu.Lock()
	defer lim.baseWriteMu.Unlock()

	base := atomic.LoadInt64(&lim.baseMicros)
	state := lim.loadState()
	// recheck that things look wonky with the lock held -- someone else could have
	// won the lock race and fixed it for us.
	if !state.ok() || base == 0 || nowMicros < base {
		// poison the state: try to get other threads we are racing with to call reinit
		atomic.StoreUint64(&lim.state, 0)

		// this is pretty wishy-washy -- I think this store can be observed without
		// observing the state poisoning above.  The point/hope of the lock here is
		// to serialize with other threads also observing the wonkiness.
		newBase := time.UnixMicro(nowMicros).Add(-(7 * 24 * time.Hour)).UnixMicro()
		atomic.StoreInt64(&lim.baseMicros, newBase)

		sinceBase := nowMicros - newBase

		atomic.StoreUint64(&lim.state, uint64(newPackedState(sinceBase, int32(lim.Burst()))))
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

	nowMicros := now.UnixMicro()
	baseMicros := atomic.LoadInt64(&lim.baseMicros)

	for i := 0; i < 256; i++ {
		currState := lim.loadState()
		sinceBase, tokens, ok := currState.Unpack()
		if !ok || nowMicros < baseMicros {
			// time jumped backwards a lot or our state was corrupted; do our best
			lim.reinit(nowMicros)
			// reinit may reset baseMicros, so reload it
			baseMicros = atomic.LoadInt64(&lim.baseMicros)
			continue
		}

		stateTime := baseMicros + sinceBase

		if nowMicros == stateTime {
			if tokens <= 0 {
				// fail early to scale "obviously rate limited" traffic.  Under load this
				// is the main branch taken
				return false
			} else {
				// there are tokens, and we're in the same epoch as currState
				newPackedState := packedState(atomic.AddUint64(&lim.state, ^uint64(0)))
				_, writeTokens, writeOk := newPackedState.Unpack()
				if writeOk && writeTokens >= 0 {
					// fmt.Printf("bonus\n")
					return true
				}
				// if we failed, start the loop over
				continue
			}
		}

		newNowMicros, tokens := lim.advance(nowMicros, stateTime, int64(tokens))
		if tokens < 1 {
			// if there are no tokens available, return
			return false
		}

		// consume 1 token
		tokens--

		newSinceBase := newNowMicros - baseMicros
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

	// the above loop should be executed 1-2 times, meaning we should never reach here.
	// (never seen in tests!).  If we do reach here, we're in a Weird situation, so fail open.
	return true
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func (lim *Limiter) advance(nowMicros, lastMicros int64, oldTokens int64) (newNowMicros int64, newTokens int32) {
	if nowMicros < lastMicros {
		lastMicros = nowMicros
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := time.Duration((nowMicros - lastMicros) * 1000)
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
		adjustMicros := lim.limit.durationFromTokens(remaining) / time.Microsecond
		adjustedNow := nowMicros - int64(adjustMicros)
		// this should never happen be triggered, but just in case ensure
		// at least that the time tracked in lim.state doesn't go backwards.
		if adjustedNow >= lastMicros {
			nowMicros = adjustedNow
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
