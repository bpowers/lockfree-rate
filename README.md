# Lockfree version of the golang.org/x/time/rate.Limiter struct

[![Go Reference](https://pkg.go.dev/badge/github.com/bpowers/lockfree-rate.svg)](https://pkg.go.dev/github.com/bpowers/lockfree-rate)

This package provides a token bucket rate limiter supporting a subset of the [`rate.Limiter`](https://pkg.go.dev/golang.org/x/time/rate#Limiter) API that is up to 100x faster under contention.
To accomplish this we only support the `Allow()` method.

While performance of the original Limiter degraded under contention, this version scales linearly to at least 8 cores:

```
$ benchstat old.txt new.txt
name      old time/op    new time/op    delta
AllowN      32.3ns ± 0%     3.4ns ± 0%  -89.41%  (p=0.002 n=6+6)
AllowN-2    84.0ns ± 1%     1.8ns ± 0%  -97.90%  (p=0.002 n=6+6)
AllowN-4     126ns ± 3%       1ns ± 0%  -99.28%  (p=0.004 n=5+6)
AllowN-8     136ns ± 1%       0ns ± 0%  -99.66%  (p=0.004 n=5+6)
```


