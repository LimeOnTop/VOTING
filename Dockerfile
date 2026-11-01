FROM node:26-alpine AS web
WORKDIR /web
COPY web/package*.json ./
RUN npm ci --ignore-scripts
COPY web/ ./
RUN npm run build
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY migrations/ migrations/
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /voting ./cmd/voting
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /voting /app/voting
COPY --from=web /web/dist /app/web/dist
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/app/voting"]
