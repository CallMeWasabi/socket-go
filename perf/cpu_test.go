package perf

import (
	"testing"
)

var cache = make(map[int]int)

func fib(n int) int {
	if n <= 1 {
		return n
	} else if _, ok := cache[n]; ok {
		return cache[n]
	}
	cache[n] = fib(n-1) + fib(n-2)
	return cache[n]
}

// The benchmark test used to capture profiling data
func BenchmarkFlameGraphSample(b *testing.B) {
	fib(100)
}
