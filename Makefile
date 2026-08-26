.PHONY: help stage1 stage2 stage3 stage4 test race crash vet clean

help:
	@echo "minilog -- a 1500-line VictoriaLogs, built by you"
	@echo ""
	@echo "  make stage1   encoding + block + part round-trip, compression ratio"
	@echo "  make stage2   bloom filter FP rate vs theory, block skip ratio"
	@echo "  make stage3   merge correctness, LSM write-amplification table"
	@echo "  make stage4   partition pruning, retention, sharding sweep"
	@echo ""
	@echo "  make test     all stages"
	@echo "  make race     all stages under the race detector"
	@echo "  make crash    200 kill -9 / recover iterations"
	@echo ""
	@echo "Every stage fails loudly until you implement it. That is the point."

stage1:
	go test ./harness -run TestStage1 -v

stage2:
	go test ./harness -run TestStage2 -v

stage3:
	go test ./harness -run TestStage3 -v -timeout 30m

stage4:
	go test ./harness -run TestStage4 -v -timeout 30m

test:
	go test ./harness -v -timeout 60m

race:
	go test ./harness -race -timeout 90m

crash:
	go run ./cmd/crashtest -iters 200

vet:
	go vet ./...

clean:
	go clean -testcache
