.PHONY: init up down test integration web helm check
init:
	./scripts/init-env.sh
up: init
	docker compose up --build -d
down:
	docker compose down
test:
	go test -race ./...
integration:
	TEST_DATABASE_URL='postgres://voting:local-dev-only@localhost:15432/voting?sslmode=disable' go test -race ./internal/store -count=1
web:
	cd web && npm ci && npm run build
helm:
	helm lint deploy/helm/voting --strict --set image.tag=check
	helm template voting deploy/helm/voting --set image.tag=check > /dev/null
check: test web helm
