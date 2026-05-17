# observability — стенд наблюдаемости системы гарантированной доставки

Подсистема наблюдаемости (Prometheus + Grafana + Alertmanager + экспортёры)
поверх системы доставки сообщений Outbox → Kafka → Inbox → DLQ.

Стенд запускается из docker compose и включает одновременно:

- инфраструктуру: PostgreSQL × 3, Kafka, Kafka Connect (Debezium 2.5);
- микросервисы: `outbox-payments-service` (Go), `Inbox` (Go), `dlq-service` (Spring Boot);
- слой наблюдаемости: Prometheus с recording и alerting rules, Grafana
  с четырьмя дашбордами, Alertmanager, postgres_exporter × 2, kafka_exporter;
- Go-генератор нагрузки и тестовых сценариев.

Java-метрики DLQ-сервиса не подключаются; наблюдаемость DLQ-потока
строится по стороне продюсера (Inbox, `inbox_dlq_messages_total`). Сам
DLQ-сервис запускается, чтобы у топика `dead-letter-queue` был реальный
потребитель.

---

## Запуск

```bash
make start
```

Поднимает весь стек, дожидается готовности outbox/inbox, регистрирует
Debezium-коннектор и печатает адреса сервисов.

| Сервис | URL |
|---|---|
| Outbox API | http://localhost:8080 |
| Outbox /metrics | http://localhost:8080/metrics |
| Inbox /metrics | http://localhost:8082/metrics |
| DLQ REST API | http://localhost:8084/dlq |
| Kafka UI | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 (admin / admin) |

`make help` — список всех команд.

---

## Структура

```
observability/
├── docker-compose.yaml
├── Makefile
├── prometheus/
│   ├── prometheus.yml
│   ├── recording_rules.yml
│   └── alerting_rules.yml
├── alertmanager/
│   └── alertmanager.yml
├── grafana/
│   └── provisioning/
│       ├── datasources/prometheus.yml
│       └── dashboards/
│           ├── dashboards.yml
│           ├── overview.json
│           ├── outbox.json
│           ├── inbox.json
│           └── infra.json
├── loadgen/
│   ├── go.mod
│   └── main.go
└── tests/
    ├── baseline.sh
    ├── spike.sh
    ├── dlq.sh
    ├── cdc-fail.sh
    ├── inbox-stall.sh
    └── duplicates.sh
```

---

## Метрики

### Outbox (Go, namespace `outbox_`)

| Метрика | Тип | Где меряется | Назначение |
|---|---|---|---|
| `outbox_http_requests_total{method,route,status}` | counter | middleware | RED.R + RED.E |
| `outbox_http_request_duration_seconds` | histogram | middleware | RED.D |
| `outbox_transfer_attempts_total{outcome}` | counter | usecase MoneyTransfer | бизнес-исход (ok / insufficient_funds / invalid_amount / account_not_found / db_error) |
| `outbox_transfer_duration_seconds{outcome}` | histogram | usecase | длительность бизнес-операции (без HTTP) |
| `outbox_events_inserted_total{event_type}` | counter | repository.accounts | количество вставленных в outbox событий |
| `outbox_pgx_pool_*` | gauge | фоновый коллектор | acquired / idle / total / max / wait_count |

«Глубина недоставленного» при CDC не считается в самом приложении — её
исчерпывающе отражает `pg_replication_slots_pg_wal_lsn_diff` (см. ниже).
Запись в outbox-таблицу хранится после прочтения Debezium-ом, поэтому
`COUNT(*)` по ней не показывает очередь.

### Inbox (Go, namespace `inbox_`)

| Метрика | Тип | Где меряется | Назначение |
|---|---|---|---|
| `inbox_kafka_messages_read_total{topic,outcome}` | counter | Listener | ok / read_error / bad_envelope / bad_payload |
| `inbox_processing_duration_seconds{outcome}` | histogram | PaymentUseCase | RED.D шага обработки |
| `inbox_messages_processed_total{outcome}` | counter | PaymentUseCase | processed / duplicate / validation_error / processing_error |
| `inbox_duplicates_total{topic}` | counter | PaymentUseCase | срабатывает при existing.Status == PROCESSED |
| `inbox_validation_errors_total{field}` | counter | PaymentUseCase | какое поле упало на валидации |
| `inbox_dlq_messages_total{error_type,outcome}` | counter | DLQProducer | публикации в `dead-letter-queue` |
| `inbox_dlq_produce_duration_seconds{outcome}` | histogram | DLQProducer | длительность Kafka WriteMessages |
| `inbox_delivery_e2e_latency_seconds` | histogram | PaymentUseCase | `now() - outbox.event_time` |
| `inbox_table_rows{status}` | gauge | фоновый коллектор | строки `inbox_order` по статусам |
| `inbox_pgx_pool_*` | gauge | фоновый коллектор | acquired / idle / total / max / wait_count |

### Инфраструктура

| Источник | Что отдаёт |
|---|---|
| `postgres-exporter-outbox` | `pg_stat_user_tables_*`, `pg_stat_database_*`, `pg_replication_slots_*` (WAL lag слота `outbox_slot`) для БД `payments-service-db` |
| `postgres-exporter-inbox` | то же для БД `inbox` |
| `kafka-exporter` | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `kafka_topic_partitions` |

---

## Дашборды Grafana

**Delivery Overview** — открывается по умолчанию. Четыре stat-панели сверху
(E2E p99, доля 5xx, DLQ rate, consumer lag), под ними четыре графика:
HTTP-трафик, E2E p50/p95/p99, WAL slot lag, разбивка Inbox-исходов.

**Outbox Service** — четыре секции:

1. HTTP (RED) — rate, errors ratio, latency-квантили
2. Money transfers — бизнес-исходы и длительность usecase
3. Outbox table — события записанные и WAL slot lag
4. pgxpool & runtime — пул соединений, goroutines, heap, `pg_stat_user_tables`

**Inbox Service** — пять секций:

1. Kafka consumer & throughput — чтение и lag
2. Processing pipeline — исходы и длительность ProcessPayment
3. Dedup, validation, DLQ — три ключевые метрики
4. End-to-end latency — квантили и heatmap
5. inbox_order table — строки по статусам и pgxpool

**Infrastructure** — Postgres (commits/rollbacks, dead/live tuples, replication
slot lag, активные коннекты) и Kafka (offsets, lag по группам).

---

## Алерты

`prometheus/alerting_rules.yml`. Alertmanager сконфигурирован на null-receiver:
firing-состояния видны в Prometheus UI и Alertmanager UI, но не пересылаются.

| Alert | Условие | Смысл |
|---|---|---|
| ServiceDown | `up == 0` 1m | контейнер не отдаёт `/metrics` |
| InboxConsumerLagHigh | `kafka_consumergroup_lag{...inbox-service} > 100` 2m | Inbox отстал от потока |
| DLQRateHigh | `rate(inbox_dlq_messages_total) > 1/s` 1m | поток битых данных |
| E2ELatencyP99High | `inbox:e2e_latency:p99_5m > 30` 2m | нарушение SLO p99 ≤ 30s |
| OutboxDeadTuplesHigh | `pg_stat_user_tables_n_dead_tup{outbox} > 50k` 5m | autovacuum не успевает |
| ReplicationSlotLagBytes | `pg_replication_slots_pg_wal_lsn_diff > 256MB` 2m | Debezium стоит, WAL копится |

---

## Тестовые сценарии

Шесть независимых команд, каждая воспроизводит одну ситуацию и оставляет
систему в рабочем состоянии. Запускать можно в любом порядке. Перед прогоном
стенд должен быть поднят (`make start`).

### `make test-baseline` — ровный поток 5 RPS на 30 секунд

| Дашборд | Панель | Что показывает |
|---|---|---|
| Overview | Transfer requests / s by status | Ровная линия ≈ 5 ops/s, только статус 204 |
| Overview | E2E delivery latency | p99 < 1 сек |
| Overview | Replication slot WAL lag | Околонулевая линия |
| Overview | Inbox consumer lag (stat) | 0 |
| Outbox | Transfer attempts by outcome | только `outcome=ok` |
| Inbox | Processing outcomes / s | только `outcome=processed` |

### `make test-spike` — трапеция 5 → 50 → 5 RPS за 60 секунд

| Дашборд | Панель | Что показывает |
|---|---|---|
| Overview | Transfer requests / s | Трапеция: рамп вверх, плато 50 ops/s, рамп вниз |
| Outbox | HTTP latency p50/p95/p99 | p99 поднимается в зоне плато |
| Outbox | pgxpool connections | `acquired` растёт, `idle` снижается |
| Outbox | pgxpool wait_count | Растёт, если упёрлись в `pool_max_conns=10` |
| Inbox | Processing duration p99 | Подскок на сотни мс |
| Inbox | E2E latency heatmap | Сдвиг плотности в сторону больших задержек |

### `make test-dlq` — 20 битых сообщений напрямую в Kafka

Loadgen в режиме `invalid-payload` шлёт пять разновидностей сломанных
payload'ов (пустой transfer_id, отрицательный amount, пустые from/to, полностью
пустой объект) прямо в `accounts.money.transferred`, минуя outbox.

| Дашборд | Панель | Что показывает |
|---|---|---|
| Inbox | Validation errors by field | Столбики по `transfer_id` / `amount` / `from_account` / `to_account` |
| Inbox | DLQ produce rate by error_type | Спайк `error_type=VALIDATION_ERROR` |
| Inbox | Processing outcomes / s | Полоса `outcome=validation_error` |
| Inbox | inbox_order rows by status | Рост `status=FAILED` на 20 |
| Overview | DLQ rate (stat) | Кратковременно не нулевой |

`make check-dlq` покажет записи на стороне DLQ-сервиса
(таблица `dead_letter_queue` и REST `GET /dlq`).

### `make test-cdc-fail` — Kafka Connect выключен 45 секунд при нагрузке

Outbox продолжает писать события в БД, но Debezium их не публикует.
После DOWNTIME Connect возвращается, коннектор регистрируется и догоняет
накопленный WAL.

| Дашборд | Панель | Что показывает |
|---|---|---|
| Infra | Replication slot WAL lag (bytes) | Растёт во время простоя, резко обнуляется после рестарта |
| Outbox | Replication slot WAL lag (bytes) | То же значение (та же метрика) |
| Outbox | Events inserted / s | Не меняется (события всё ещё пишутся) |
| Infra | outbox dead vs live tuples | Кратковременный рост dead tuples после рестарта (CDC прочитал блок) |
| Inbox | Processing outcomes | Во время простоя пропадает, после восстановления — короткий «catch-up» |
| Prometheus / Alerts | `ReplicationSlotLagBytes` | Перейдёт в `firing`, если порог в 256 МБ превышен; на стенде с лёгким трафиком обычно не успевает — порог имеет смысл понизить или подавать побольше WAL'а |

Длительность простоя настраивается: `DOWNTIME=120s make test-cdc-fail`.

### `make test-inbox-stall` — Inbox выключен 45 секунд при нагрузке

Outbox пишет, Debezium публикует, но Inbox не читает — копится lag.

| Дашборд | Панель | Что показывает |
|---|---|---|
| Inbox | Kafka consumer lag | Линейный рост, после рестарта быстрое схлопывание |
| Inbox | Kafka messages read / s | Падает в 0, после старта поднимается выше базового (catch-up) |
| Inbox | Processing outcomes | Пропадает, потом «горка» обработки |
| Overview | Inbox consumer lag (stat) | Жёлтый/красный во время простоя |
| Outbox | Replication slot WAL lag | Не растёт — CDC продолжает публиковать, не Inbox держит WAL |
| Prometheus / Alerts | `InboxConsumerLagHigh` | После 2 минут отставания > 100 переходит в `firing` |

### `make test-duplicates` — сброс offsets группы → дубликаты

Останавливаем Inbox, делаем `kafka-consumer-groups --reset-offsets --to-earliest`
по группе `inbox-service`, запускаем Inbox обратно. Inbox перечитывает топик,
но почти все сообщения уже PROCESSED — срабатывает дедупликация.

| Дашборд | Панель | Что показывает |
|---|---|---|
| Inbox | Duplicates rate / s | Спайк, в десятки раз выше базового потока |
| Inbox | Processing outcomes / s | Жирная полоса `outcome=duplicate` |
| Inbox | E2E latency | Не растёт — наблюдения для дубликатов не пишутся в гистограмму |
| Inbox | inbox_order rows by status | Не меняется |

---

## Ограничения

- Стенд развёртывается в docker-compose на одной машине. Цифры из
  тестовых сценариев — порядки величин, а не production-нагрузка.
- Java-метрики DLQ-сервиса не подключены: наблюдаемость DLQ-потока
  ограничена стороной продюсера (`inbox_dlq_messages_total`).
- JMX-метрики Kafka Connect (`MilliSecondsBehindSource` и т. д.) не
  подключены. Их роль закрывают `pg_replication_slots_pg_wal_lsn_diff`
  (Postgres-сторона CDC) и `kafka_consumergroup_lag` (Kafka-сторона).
- Alertmanager на null-receiver: уведомления никуда не уходят, видны
  только в UI.
- Outbox-сервис должен быть на ветке `cdc-impl` (или содержать ту же
  схему и Debezium-конфиг). На ветке `main` (worker polling) часть
  инфраструктурных метрик (`pg_replication_slots_*`) будет нулевой.
