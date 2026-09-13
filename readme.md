# PhotoGallery

A distributed notification system for a photo gallery app, built as Go microservices communicating over gRPC.

## Services

| Service | Description | Port |
|---|---|---|
| gateway | HTTP/REST entrypoint, proxies to internal gRPC services | 8080 |
| user-service | Registration, login, JWT issuing | 8081 |
| gallery-service | Gallery CRUD, membership, moderator actions | 8082 |
| upload-service | Photo upload to MinIO, publishes events | 8083 |
| notification-service | Fans out notifications to subscribed users | 8084 |
| frontend | React SPA served by nginx | 80 |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Go 1.26+ needed if you want to run tests or build outside Docker

## 1. Instruction to run 
Clone repository with
```bash
git clone https://github.com/teor0/progetto_sdcc ~/photogallery
```

A `.env` file is already included at the project root with working defaults for local use.

**If deploying to a server like Amazon EC2:**
- Set in `.env` `MINIO_PUBLIC_URL=http://<PUBLIC_IP>:9000` so uploaded photo URLs are reachable by browsers.

From the project root:

```bash
docker compose up -d --build
```

Check everything is healthy and up and running:

```bash
docker compose ps
```

## 2. Use it

Open http://localhost, or `http://<PUBLIC_IP>` register a **Moderator** account to create galleries, and a **User** account to join galleries and upload photos.

## Common commands

```bash
# scale a service 
docker compose up -d --build --scale upload-service=3

# stop everything
docker compose down

# stop and wipe all data (databases, MinIO, RabbitMQ)
docker compose down -v
```

## Running tests

Unit tests (no Docker needed):

```bash
go test ./...
```

Integration tests requires the full stack running via `docker compose up` locally:

```bash
go test -tags=integration ./test/integration/... -v
```

Point integration tests at a remote deployment instead of localhost:

```bash
GATEWAY_URL=http://<PUBLIC_IP>:8080 go test -tags=integration ./test/integration/... -v
```

Check also the comments inside the test files.

## Frontend only (dev mode)

To run the frontend with hot reload against an already-running backend:

```bash
cd frontend
npm install
npm run dev
```