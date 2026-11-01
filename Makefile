.PHONY: init up down test integration lint security web helm swagger check
init:
	@set -eu; if [ ! -e .env ]; then \
		umask 077; \
		printf 'ADMIN_TOKEN=%s\nSIGNING_KEY=%s\nCOOKIE_SECURE=false\n' "$$(openssl rand -hex 24)" "$$(openssl rand -hex 32)" > .env; \
	fi
up: init
	docker compose up --build -d
down:
	docker compose down
test:
	go test -race ./...
integration:
	TEST_DATABASE_URL='postgres://voting:local-dev-only@localhost:15432/voting?sslmode=disable' go test -race ./internal/repository -count=1
lint:
	golangci-lint run ./...
security:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
web:
	cd web && npm ci && npm run build
helm:
	helm lint deploy/helm/voting --strict --set image.tag=check
	helm template voting deploy/helm/voting --set image.tag=check > /dev/null
swagger:
	go run github.com/swaggo/swag/cmd/swag@v1.16.6 init -g main.go -d cmd/voting,internal/controller,internal/entity --parseInternal --outputTypes yaml -o api
	mv api/swagger.yaml api/swagger.yml
check: test lint security web helm swagger
