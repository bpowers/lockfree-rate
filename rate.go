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

const timeShift = 20
const sentinelShift = timeShift - 1
const catchUnderflowShift = sentinelShift - 1
const tokensMask = (1 << catchUnderflowShift) - 1
const maxTokens = (1 << (catchUnderflowShift - 1)) - 1

type packedState uint64

func newPackedState(newSinceBase int64, tokens int32) packedState {
	if newSinceBase < 0 {
		newSinceBase = 0
	}
	if tokens < 0 {
		tokens = 0
	}
	return packedState((uint64(newSinceBase) << timeShift) | (0x1 << sentinelShift) | (0x1 << catchUnderflowShift) | (uint64(tokens) & tokensMask))
}

func (ps packedState) tokens() int32 {
	tokens := int32(ps & tokensMask)
	// do this shift dance after we've masked to left-fill 1s in the case of a
	// transition from 0 -> -1.  Check out TestUnderflow for details.
	return (tokens << (32 - catchUnderflowShift)) >> (32 - catchUnderflowShift)
}

func (ps packedState) timeMicros() int64 {
	return int64(ps >> timeShift)
}

func (ps packedState) ok() bool {
	return ((ps >> sentinelShift) & 0x1) == 0x1
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
		log.Printf("rate.NewLimiter: burst %d bigger than max %d, using %d", b, maxTokens, maxTokens)
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

	limit := lim.limit
	maxBurst := atomic.LoadInt64(&lim.burst)
	baseMicros := atomic.LoadInt64(&lim.baseMicros)

	for i := 0; i < 1024; i++ {
		currState := lim.loadState()
		sinceBase, tokens, ok := currState.Unpack()
		if !ok || nowMicros < baseMicros {
			// time jumped backwards a lot or our state was corrupted; do our best
			lim.reinit(nowMicros)
			// reinit may reset baseMicros, so reload it
			baseMicros = atomic.LoadInt64(&lim.baseMicros)
			continue
		}

		lastUpdateMicros := baseMicros + sinceBase

		// CPUs are fast, so "binning" time to microseconds (1,000 nanoseconds,
		// 1/1,000 of a millisecond) leaves us with a pretty coarse grained measure
		// of advancing time (more like a staircase than a sloped hill).  Consequently,
		// if there is a lot of traffic for this rate limiter we expect last update to
		// be equal to now micros most of the time (especially if we are on the second
		// iteration of this loop, having just lost the CAS race to another goroutine).
		// Handling this case separately, first, gives us a fast path that reduces
		// contention and helps us scale.
		if nowMicros == lastUpdateMicros {
			if tokens <= 0 {
				// fail early to scale "obviously rate limited" traffic.  Under load this
				// is the main branch taken and happens in the first iteration of the loop.
				return false
			} else {
				// there are tokens, and we're in the same epoch as currState.  race-ily
				// decrement the state and check if we won the race.  Because we treat
				// token count as a signed integer and always set an extra `1` bit just
				// to the left of token count, if we drop below 0 tokens here when racing
				// it is fine.
				//
				// This branch is mostly hit if we have burst capacity and a bunch of
				// concurrent requests coming in at once.
				newPackedState := packedState(atomic.AddUint64(&lim.state, ^uint64(0)))
				_, writeTokens, writeOk := newPackedState.Unpack()
				// if write tokens is 0, it means that our write was the one that
				// decremented tokens from 1 to 0.  We count that as a win!
				return writeOk && writeTokens >= 0
			}
		}

		newNowMicros, tokens := advance(limit, nowMicros, lastUpdateMicros, int64(tokens), maxBurst)
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

	// The above loop should be executed 1-2 times before one of the return statements is triggered.
	// We've never seen in tests this fallthrough get hit!  But to avoid some degenerate/unicorn case
	// that causes the logic above to loop forever, we limit the times through the loop.  If we do
	// reach here, we're in a Super Weird situation, so fail open.
	return true
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func advance(lim Limit, nowMicros, lastMicros int64, oldTokens int64, maxBurst int64) (newNowMicros int64, newTokens int32) {
	// in the event of a time jump _or_ another goroutine winning the CAS race,
	// lastMicros may be in the future!
	if nowMicros < lastMicros {
		lastMicros = nowMicros
	}

	// we may observe a race-y underflow from the non-CAS atomic decrement in the loop above,
	// correct it here.
	if oldTokens < 0 {
		oldTokens = 0
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := time.Duration(nowMicros-lastMicros) * time.Microsecond
	delta := lim.tokensFromDuration(elapsed)

	// if a long time has passed we may have a lot of tokens available -- clamp
	// it down to the max burst we've configured. The constructor ensures
	// `maxBurst <= maxTokens`
	tokens := float64(oldTokens) + delta
	if burst := float64(maxBurst); tokens > burst {
		tokens = burst
	}

	wholeTokens := int32(tokens)

	// if we don't adjust "now" we lose fractional tokens and rate limit
	// at a substantially different rate than users specified.
	remaining := tokens - float64(wholeTokens)
	adjustMicros := lim.durationFromTokens(remaining) / time.Microsecond
	adjustedNow := nowMicros - int64(adjustMicros)

	// this should always be true, but just in case ensure that the time
	// tracked in lim.state doesn't go backwards.  An always-taken branch
	// is ~ free.
	if adjustedNow >= lastMicros {
		nowMicros = adjustedNow
	}

	return nowMicros, wholeTokens
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
