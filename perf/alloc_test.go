package perf

import (
	"testing"
)

func BenchmarkAppendNoPrealloc(b *testing.B) {
	for b.Loop() {
		var s []int
		for j := range 10000 {
			s = append(s, j)
		}
	}
}

func BenchmarkAppendWithPrealloc(b *testing.B) {
	for b.Loop() {
		s := make([]int, 0, 10000)
		for j := range 10000 {
			s = append(s, j)
		}
	}
}
