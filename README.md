# observability — стенд наблюдаемости системы гарантированной доставки

Этот репозиторий — индивидуальная часть курсовой работы
Он добавляет подсистему наблюдаемости (Prometheus + Grafana + Alertmanager +
экспортёры) поверх системы доставки сообщений Outbox -> Kafka -> Inbox -> DLQ,
разработанной командой курсовой.

Стенд запускается из docker compose и включает в себя одновременно:

- **инфраструктуру** (PostgreSQL × 2, Kafka, Kafka Connect/Debezium 2.5);
- **микросервисы команды** (`outbox-payments-service` на Go, `Inbox` на Go),
  с собственной инструментацией метрик;
- **слой наблюдаемости** — Prometheus (recording + alerting rules), Grafana
  (4 дашборда), Alertmanager, postgres_exporter × 2, kafka_exporter;
- **генератор нагрузки и chaos-сценариев** на Go.

DLQ-сервис (Spring Boot) в курсовой делает другой участник команды; в этом
репозитории он не запускается — наблюдаемость DLQ-потока строится по
метрикам Inbox-сервиса (`inbox_dlq_messages_total`).

---

## Быстрый старт

```bash
make start # поднять стек, дождаться готовности, зарегистрировать Debezium
```

| Сервис | URL |
|---|---|
| Outbox API | http://localhost:8080 |
| Outbox /metrics | http://localhost:8080/metrics |
| Inbox /metrics | http://localhost:8082/metrics |
| Kafka UI | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 (admin / admin) |

---

## Структура

```
observability/
├── docker-compose.yaml        infra + сервисы + наблюдаемость
├── Makefile                   команды жизненного цикла (start/load/chaos/demo)
├── demo.sh                    четырёхфазовый сценарий для защиты
├── prometheus/
│   ├── prometheus.yml         scrape_configs, подключение Alertmanager
│   ├── recording_rules.yml    производные SLI (rate'ы, ratio, p95/p99)
│   └── alerting_rules.yml     алерты (queue depth, WAL lag, DLQ rate, E2E p99)
├── alertmanager/
│   └── alertmanager.yml       null-receiver (демонстрационный)
├── postgres-exporter/
│   └── queries.yaml           кастомные запросы для outbox-БД
├── grafana/
│   └── provisioning/
│       ├── datasources/prometheus.yml
│       └── dashboards/
│           ├── dashboards.yml        file-provisioner для всех JSON ниже
│           ├── overview.json         сводный дашборд (домашний)
│           ├── outbox.json           Outbox: RED + бизнес + БД + pgxpool
│           ├── inbox.json            Inbox: Kafka + дедупликация + DLQ + E2E
│           └── infra.json            PostgreSQL и Kafka в одном месте
└── loadgen/
    ├── go.mod
    └── main.go                нагрузочный генератор + chaos сценарии
```

---

## Метрики, которые собираются

### Outbox (Go, namespace `outbox_`)

| Метрика | Тип | Где меряется | Назначение |
|---|---|---|---|
| `outbox_http_requests_total{method,route,status}` | counter | middleware | RED.R + RED.E |
| `outbox_http_request_duration_seconds` | histogram | middleware | RED.D |
| `outbox_transfer_attempts_total{outcome}` | counter | usecase MoneyTransfer | бизнес-исход (ok / insufficient_funds / invalid_amount / account_not_found / db_error) |
| `outbox_transfer_duration_seconds{outcome}` | histogram | usecase | длительность чисто бизнес-операции, без HTTP |
| `outbox_events_inserted_total{event_type}` | counter | repository.accounts | количество фактически записанных в outbox событий |
| `outbox_table_rows` | gauge | фоновый коллектор | глубина очереди outbox |
| `outbox_oldest_event_age_seconds` | gauge | фоновый коллектор | возраст самого старого события |
| `outbox_pgx_pool_*` | gauge | фоновый коллектор | USE для pool: acquired/idle/total/max + wait_count |

### Inbox (Go, namespace `inbox_`)

| Метрика | Тип | Где меряется | Назначение |
|---|---|---|---|
| `inbox_kafka_messages_read_total{topic,outcome}` | counter | Listener | ok / read_error / bad_envelope / bad_payload |
| `inbox_processing_duration_seconds{outcome}` | histogram | PaymentUseCase | RED.D для шага обработки |
| `inbox_messages_processed_total{outcome}` | counter | PaymentUseCase | processed / duplicate / validation_error / processing_error |
| `inbox_duplicates_total{topic}` | counter | PaymentUseCase | срабатывает, когда existing.Status == PROCESSED |
| `inbox_validation_errors_total{field}` | counter | PaymentUseCase | какое поле упало валидацию (transfer_id / amount / from_account / to_account) |
| `inbox_dlq_messages_total{error_type,outcome}` | counter | DLQProducer | публикации в dead-letter-queue |
| `inbox_dlq_produce_duration_seconds{outcome}` | histogram | DLQProducer | сколько занял Kafka WriteMessages |
| `inbox_delivery_e2e_latency_seconds` | histogram | PaymentUseCase | `now() - outbox.event_time` |
| `inbox_table_rows{status}` | gauge | фоновый коллектор | строки inbox_order по статусам |
| `inbox_pgx_pool_*` | gauge | фоновый коллектор | USE для pool |

### Инфраструктура

| Источник | Что отдаёт |
|---|---|
| `postgres-exporter-outbox` | `pg_stat_user_tables_*` (n_dead/n_live tup), `pg_stat_database_*` (xact_commit/rollback), `pg_replication_slots_*` (WAL lag слота `outbox_slot`), `pg_outbox_age_*` (кастомный запрос) |
| `postgres-exporter-inbox` | то же для БД `inbox` |
| `kafka-exporter` | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `kafka_topic_partitions` |

---

## Какой дашборд что показывает

**Delivery Overview** — открывается по умолчанию. Четыре stat-панели сверху (E2E
p99, доля 5xx, DLQ rate, consumer lag) дают мгновенный ответ на «всё ли в порядке».
Ниже — четыре графика: HTTP-трафик, E2E p50/p95/p99, состояние таблицы outbox,
разбивка Inbox-исходов.

**Outbox Service** — детальная разборка по сервису outbox. Четыре секции:

1. HTTP (RED) — rate, errors ratio, latency-квантили
2. Money transfers — бизнес-исходы и длительность usecase
3. Outbox table — события записанные / глубина / возраст
4. pgxpool & runtime — пул соединений, go_goroutines, heap, `pg_stat_user_tables` для таблицы `outbox`

**Inbox Service** — четыре секции:

1. Kafka consumer & throughput — чтение и lag
2. Processing pipeline — исходы и длительность ProcessPayment
3. Dedup, validation, DLQ — три ключевые метрики
4. End-to-end latency — квантили + heatmap
5. inbox_order table — строки по статусам и pgxpool

**Infrastructure** — Postgres-сторона (commits/rollbacks, dead/live tuples,
replication slot lag, активные коннекты) и Kafka-сторона (offsets, lag по
группам).

---

## Алерты

Файл: `prometheus/alerting_rules.yml`. Алерты подаются в Alertmanager, который
сконфигурирован на null-receiver (нотификации не уходят) — но в Prometheus UI
вкладка Alerts честно показывает все firing-состояния.

| Alert | Условие | Что значит на стенде |
|---|---|---|
| ServiceDown | `up == 0` 1m | контейнер упал или не отдаёт /metrics |
| OutboxQueueDepthHigh | `outbox_table_rows > 1000` 2m | Debezium не успевает |
| OutboxOldestEventStale | `outbox_oldest_event_age_seconds > 60` 2m | в outbox висит старая запись |
| InboxConsumerLagHigh | `kafka_consumergroup_lag{...inbox-service} > 100` 2m | Inbox отстал |
| DLQRateHigh | `rate(inbox_dlq_messages_total) > 1/s` 1m | поток битых данных |
| E2ELatencyP99High | `inbox:e2e_latency:p99_5m > 30` 2m | нарушение SLO p99≤30s |
| OutboxDeadTuplesHigh | `pg_stat_user_tables_n_dead_tup{outbox} > 50k` 5m | autovacuum не успевает |
| ReplicationSlotLagBytes | `pg_replication_slots_pg_wal_lsn_diff > 256MB` 2m | Debezium стоит, WAL копится |

## Ограничения и допущения

- Стенд развёртывается в docker-compose на одной машине. Нагрузочные тесты
  показывают порядки величин, а не реальные production-числа.
- DLQ-сервис команды не запускается; обозреваемость DLQ-потока строится по
  стороне продюсера (Inbox). Это сознательное упрощение индивидуальной части.
- JMX-метрики Kafka Connect/Debezium (`MilliSecondsBehindSource`,
  `TotalNumberOfEventsSeen`) не подключены — их роль на стенде закрывают
  `pg_replication_slots_pg_wal_lsn_diff` (Postgres-сторона) и
  `kafka_consumergroup_lag` (Kafka-сторона), что достаточно для всех
  алертов и SLI.
- Alertmanager сконфигурирован с null-receiver: алерты видны в UI, но
  никуда не уходят. Это намеренно — стенд автономный.
- Outbox-сервис должен быть на ветке `cdc-impl` (или содержать те же
  таблицы и Debezium-конфиг). На ветке `main` (worker polling) часть
  метрик инфраструктуры (`pg_replication_slots_*`) будет нулевой —
  это ожидаемое поведение.