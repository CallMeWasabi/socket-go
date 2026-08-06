# socket-go

go test -bench=. -cpuprofile=cpu.prof ./perf/cpu_test.go
go tool pprof -http=:8080 cpu.prof

go test -bench=. -benchmem -memprofile=mem.out ./perf/alloc_test.go
