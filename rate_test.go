// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.7
// +build go1.7

package rate

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestSize(t *testing.T) {
	if unsafe.Sizeof(Limiter{}) != 64 {
		t.Errorf("expected limiter to by 64 bytes in size")
	}
}

func TestShifting(t *testing.T) {
	// half year + 1 microsecond in microseconds
	const yearMicros = uint64((180*24*time.Hour + 1000) / time.Microsecond)

	if (yearMicros<<timeShift)>>timeShift != yearMicros {
		t.Errorf("timeShift too big to losslessly deal with our duration")
	}
}

func TestUnderflow(t *testing.T) {
	s := uint64(newPackedState(0, 0))

	// -1 in uint64-speak
	atomic.AddUint64(&s, ^uint64(0))

	lastUpdate, toks, ok := packedState(s).Unpack()

	if toks != -1 {
		t.Fatalf("expected toks to be -1 not %d", toks)
	}

	if !ok {
		t.Fatalf("expected undeflow to be ok")
	}

	if lastUpdate != 0 {
		t.Fatalf("expected lastUpdate to be un-affected")
	}
}

func TestLimit(t *testing.T) {
	if Limit(10) == Inf {
		t.Errorf("Limit(10) == Inf should be false")
	}
}

func closeEnough(a, b Limit) bool {
	return (math.Abs(float64(a)/float64(b)) - 1.0) < 1e-9
}

func TestEvery(t *testing.T) {
	cases := []struct {
		interval time.Duration
		lim      Limit
	}{
		{0, Inf},
		{-1, Inf},
		{1 * time.Nanosecond, Limit(1e9)},
		{1 * time.Microsecond, Limit(1e6)},
		{1 * time.Millisecond, Limit(1e3)},
		{10 * time.Millisecond, Limit(100)},
		{100 * time.Millisecond, Limit(10)},
		{1 * time.Second, Limit(1)},
		{2 * time.Second, Limit(0.5)},
		{time.Duration(2.5 * float64(time.Second)), Limit(0.4)},
		{4 * time.Second, Limit(0.25)},
		{10 * time.Second, Limit(0.1)},
		{time.Duration(math.MaxInt64), Limit(1e9 / float64(math.MaxInt64))},
	}
	for _, tc := range cases {
		lim := Every(tc.interval)
		if !closeEnough(lim, tc.lim) {
			t.Errorf("Every(%v) = %v want %v", tc.interval, lim, tc.lim)
		}
	}
}

const (
	d = 100 * time.Millisecond
)

var (
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t9 = t0.Add(time.Duration(9) * d)
)

type allow struct {
	t  time.Time
	n  int
	ok bool
}

func run(t *testing.T, lim *Limiter, allows []allow) {
	t.Helper()
	for i, allow := range allows {
		sawDivergence := false
		for j := 0; j < allow.n; j++ {
			ok := lim.reserve(allow.t)
			t.Logf("step %d: lim.AllowN(%v, %v) = %v want %v",
				i, allow.t, allow.n, ok, allow.ok)

			if ok != allow.ok {
				if allow.n > 1 && j != allow.n-1 {
					sawDivergence = true
					continue
				}
				t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v",
					i, allow.t, allow.n, ok, allow.ok)
			}
		}
		if allow.ok && sawDivergence {
			t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v",
				i, allow.t, allow.n, false, allow.ok)
		}
	}
}

func TestLimiterBurst1(t *testing.T) {
	run(t, NewLimiter(10, 1), []allow{
		{t0, 1, true},
		{t0, 1, false},
		{t0, 1, false},
		{t1, 1, true},
		{t1, 1, false},
		{t1, 1, false},
		{t2, 1, true},
		{t2, 1, false},
	})
}

func TestLimiterBurst3(t *testing.T) {
	run(t, NewLimiter(10, 3), []allow{
		{t0, 2, true},
		{t0, 1, true},
		{t0, 1, false},
		{t2, 1, true},
		{t3, 1, true},
		{t4, 1, true},
		{t4, 1, true},
		{t4, 1, false},
		{t4, 1, false},
		{t9, 3, true},
		{t9, 0, true},
	})
}

func TestLimiterJumpBackwards(t *testing.T) {
	run(t, NewLimiter(10, 3), []allow{
		{t1, 1, true}, // start at t1
		{t0, 1, true}, // jump back to t0, two tokens remain
		{t0, 1, true},
		{t0, 1, false},
		{t0, 1, false},
		{t1, 1, true}, // got a token
		{t1, 1, false},
		{t1, 1, false},
		{t2, 1, true}, // got another token
		{t2, 1, false},
		{t2, 1, false},
	})
}

// Ensure that tokensFromDuration doesn't produce
// rounding errors by truncating nanoseconds.
// See golang.org/issues/34861.
func TestLimiter_noTruncationErrors(t *testing.T) {
	if !NewLimiter(0.7692307692307693, 1).Allow() {
		t.Fatal("expected true")
	}
}

func TestSimultaneousRequests(t *testing.T) {
	const (
		limit       = 1
		burst       = 5
		numRequests = 15
	)
	var (
		wg    sync.WaitGroup
		numOK = uint32(0)
	)

	// Very slow replenishing bucket.
	lim := NewLimiter(limit, burst)

	// Tries to take a token, atomically updates the counter and decreases the wait
	// group counter.
	f := func() {
		defer wg.Done()
		if ok := lim.Allow(); ok {
			atomic.AddUint32(&numOK, 1)
		}
	}

	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go f()
	}
	wg.Wait()
	if numOK != burst {
		t.Errorf("numOK = %d, want %d", numOK, burst)
	}
}

func TestLongRunningQPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if runtime.GOOS == "openbsd" {
		t.Skip("low resolution time.Sleep invalidates test (golang.org/issue/14183)")
		return
	}

	// The test runs for a few seconds executing many requests and then checks
	// that overall number of requests is reasonable.
	const (
		limit = 100
		burst = 100
	)
	var numOK = int32(0)

	lim := NewLimiter(limit, burst)

	var wg sync.WaitGroup
	f := func() {
		if ok := lim.Allow(); ok {
			atomic.AddInt32(&numOK, 1)
		}
		wg.Done()
	}

	start := time.Now()
	end := start.Add(5 * time.Second)
	for time.Now().Before(end) {
		wg.Add(1)
		go f()

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()
	elapsed := time.Since(start)
	ideal := burst + (limit * float64(elapsed) / float64(time.Second))

	// We should never get more requests than allowed.
	if want := int32(ideal + 1); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

type request struct {
	t   time.Time
	n   int
	act time.Time
	ok  bool
}

// dFromDuration converts a duration to the nearest multiple of the global constant d.
func dFromDuration(dur time.Duration) int {
	// Add d/2 to dur so that integer division will round to
	// the nearest multiple instead of truncating.
	// (We don't care about small inaccuracies.)
	return int((dur + (d / 2)) / d)
}

// dSince returns multiples of d since t0
func dSince(t time.Time) int {
	return dFromDuration(t.Sub(t0))
}

func BenchmarkAllowN(b *testing.B) {
	var numOK = uint64(0)

	lim := NewLimiter(2000, 50)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if lim.reserve(now) {
				atomic.AddUint64(&numOK, 1)
			}
		}
	})
	b.Logf("allowed %d requests through", atomic.LoadUint64(&numOK))
}

func Benchmark10RPS(b *testing.B) {
	benchmarkRPS(b, 10, 1)
}

func Benchmark100RPS(b *testing.B) {
	benchmarkRPS(b, 100, 1)
}

func Benchmark1000RPS(b *testing.B) {
	benchmarkRPS(b, 1000, 1)
}

func Benchmark10000RPS(b *testing.B) {
	benchmarkRPS(b, 10000, 1)
}

func Benchmark100000RPS(b *testing.B) {
	benchmarkRPS(b, 100000, 1)
}

func benchmarkRPS(b *testing.B, rate Limit, burst int) {
	b.Helper()

	var total = uint64(0)
	var numOK = uint64(0)

	lim := NewLimiter(rate, burst)

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		localOK := uint64(0)
		localTotal := uint64(0)

		for pb.Next() {
			if lim.Allow() {
				localOK++
			}
			localTotal++
		}

		atomic.AddUint64(&numOK, localOK)
		atomic.AddUint64(&total, localTotal)
	})

	d := time.Now().Sub(start)
	if d > 1*time.Second {
		ok := atomic.LoadUint64(&numOK)
		tot := atomic.LoadUint64(&total)
		allowedRPS := float64(ok) / (float64(d) / float64(time.Second))
		overallRPS := float64(tot) / (float64(d) / float64(time.Second))
		b.Logf("%.1f RPS allowed, %.1f RPS overall. %.3f%% of %d requests in %v", allowedRPS, overallRPS, 100*float64(ok)/float64(tot), tot, d)
	}
}

func BenchmarkBurstRPS(b *testing.B) {
	b.Helper()

	var total = uint64(0)
	var numOK = uint64(0)

	lim := NewLimiter(100000, 100000)

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		localOK := uint64(0)
		localTotal := uint64(0)

		for pb.Next() {
			if lim.Allow() {
				localOK++
			}
			localTotal++
		}

		atomic.AddUint64(&numOK, localOK)
		atomic.AddUint64(&total, localTotal)
	})

	d := time.Now().Sub(start)
	if d > 1*time.Second {
		ok := atomic.LoadUint64(&numOK)
		tot := atomic.LoadUint64(&total)
		allowedRPS := float64(ok) / (float64(d) / float64(time.Second))
		overallRPS := float64(tot) / (float64(d) / float64(time.Second))
		b.Logf("%.1f RPS allowed, %.1f RPS overall. %.3f%% of %d requests in %v", allowedRPS, overallRPS, 100*float64(ok)/float64(tot), tot, d)
	}
}

// global var so the comparison below can't be optimized away
var Then int64

// this tests the absolute minimum time a call to Accept() (which internally
// calls time.Now) could take.
func BenchmarkTimeNow(b *testing.B) {
	var total = uint64(0)
	var numOK = uint64(0)

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		localOK := uint64(0)
		localTotal := uint64(0)

		for pb.Next() {
			if time.Now().UnixMicro() > Then {
				localOK++
			}
			localTotal++
		}

		atomic.AddUint64(&numOK, localOK)
		atomic.AddUint64(&total, localTotal)
	})

	d := time.Now().Sub(start)
	if d > 1*time.Second {
		ok := atomic.LoadUint64(&numOK)
		tot := atomic.LoadUint64(&total)
		allowedRPS := float64(ok) / (float64(d) / float64(time.Second))
		overallRPS := float64(tot) / (float64(d) / float64(time.Second))
		b.Logf("%.1f RPS allowed, %.1f RPS overall. %.3f%% of %d requests in %v", allowedRPS, overallRPS, 100*float64(ok)/float64(tot), tot, d)
	}
}

func TestZeroLimit(t *testing.T) {
	r := NewLimiter(0, 1)
	if !r.Allow() {
		t.Errorf("Limit(0, 1) want true when first used")
	}
	if r.Allow() {
		t.Errorf("Limit(0, 1) want false when already used")
	}
}
