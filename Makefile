.PHONY: $(MAKECMDGOALS)

build:
	@mkdir -p bin/
	go build -o bin/dagcargo_cron ./cmd/cron
