# Observability — стенд курсовой работы

Объединённый стенд для запуска связки `outbox-payments-service` (CDC) → Kafka → `inbox-service`.

На текущем этапе здесь только инфраструктура (Postgres x2, Kafka, Kafka Connect/Debezium, Kafka UI). Prometheus, Grafana и экспортёры будут добавлены позже.

## Топология

| Компонент | Контейнер | Порт хоста | Назначение |
|---|---|---|---|
| Postgres (outbox) | `postgres` | 5432 | БД сервиса outbox, `wal_level=logical` для Debezium |
| Postgres (inbox) | `postgres-inbox` | 5433 | БД сервиса inbox |
| Goose | `inbox-migrate` | — | Прокатывает миграции inbox при старте |
| Kafka (KRaft) | `kafka` | 9092 (внутри), 29092 (с хоста) | Брокер |
| Kafka UI | `kafka-ui` | 8081 | Просмотр топиков |
| Kafka Connect | `kafka-connect` | 8083 | Debezium PostgreSQL connector |

Сами Go-сервисы запускаются отдельно через `go run` — так их удобнее отлаживать и инструментировать.

## Запуск связки

### 1. Поднять инфраструктуру

```bash
cd observability
docker compose up -d
```

Дождаться, пока контейнеры пройдут healthcheck:

```bash
docker compose ps
```

Миграции inbox прокатываются автоматически контейнером `inbox-migrate`. Проверить можно:

```bash
docker compose logs inbox-migrate
```

### 2. Запустить outbox-сервис

В другом терминале:

```bash
cd outbox-payments-service
go run cmd/main.go
```

Сервис применит миграции outbox (создаст таблицы `accounts`, `transfers`, `outbox`, publication `outbox_pub`) и начнёт слушать на `localhost:8080`.

### 3. Зарегистрировать Debezium-коннектор

```bash
cd outbox-payments-service
make kafka-connect-create-outbox-connector
```

Проверить:

```bash
curl -s localhost:8083/connectors/outbox-connector/status | jq
```

Должно быть `state: RUNNING`.

### 4. Запустить inbox-сервис

В третьем терминале:

```bash
cd Inbox
go run cmd/main.go
```

В логах появится `inbox service started`.

### 5. Сделать тестовый перевод

```bash
curl -s -X POST localhost:8080/api/v1/accounts/transfer-money \
  -H "Content-Type: application/json" \
  -d '{
    "from_account": "22222222-2222-2222-2222-222222222222",
    "to_account":   "11111111-1111-1111-1111-111111111111",
    "amount": 100
  }'
```

Ожидаемая цепочка:

1. Outbox создаёт `transfers` запись и `outbox` запись в одной транзакции.
2. Debezium читает WAL, публикует событие в Kafka-топик `accounts.money.transferred`.
3. Inbox потребляет из топика, пишет в `inbox_order` (статус `RECEIVED` → `PROCESSED`) и в `processed_payment`.

### 6. Проверить результат

```bash
# Inbox: запись о принятом сообщении
docker exec -it postgres-inbox psql -U postgres -d inbox \
  -c "SELECT transfer_id, status FROM inbox_order;"

# Inbox: бизнес-результат
docker exec -it postgres-inbox psql -U postgres -d inbox \
  -c "SELECT * FROM processed_payment;"

# Outbox: проверить что событие было в outbox-таблице
docker exec -it postgres psql -U admin -d payments-service-db \
  -c "SELECT id, event_type, created_at FROM outbox ORDER BY created_at DESC LIMIT 5;"
```

Либо открыть Kafka UI: <http://localhost:8081>.

## Остановка

```bash
docker compose down            # сохранить volumes
docker compose down -v         # стереть данные Postgres и Kafka
```

## Известные тонкости

- **Брокер для клиентов с хоста:** `localhost:29092` (а не `9092` — это внутренний listener для контейнеров).
- **Имя контейнера `postgres`** оставлено как в исходном `outbox-payments-service/docker-compose.yaml`, чтобы не править `outbox-payments-service/migrations/connect/outbox.json` (там `database.hostname: postgres`).
- **Inbox-овский `docker-compose.yaml`** в этом сетапе не используется — он остался для соло-разработки inbox без общей инфры.
- **Outbox-овский `docker-compose.yaml`** дублирует часть сервисов из этого. Когда работаем с полной связкой, его поднимать не нужно.
