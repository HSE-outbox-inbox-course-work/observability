# observability

Репозиторий представляет собой стенд функционирующей гарантированной доставки, собранный при помощи docker compose с наличием метрик Prometheus и мониторинга при помощи Grafana .

## Запуск

Сервисы собираются из соседних директорий (`../outbox-payments-service`, `../Inbox`)

```bash
make start
```

| Сервис | Адрес |
|---|---|
| Outbox API | http://localhost:8080 |
| Kafka UI | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 — `admin / admin` |

## Команды

```bash
make start          # старт работы сервисов и инфраструктуры
make stop           # остановить и удалить volumes (данные сбросятся)
make restart        # stop + start

make transfer       # один тестовый перевод
make load           # непрерывная нагрузка ~2 RPS, Ctrl+C чтобы остановить

make check-inbox    # последние 10 записей в inbox_order
make check-outbox   # последние 10 записей в outbox таблице
make logs           # Логи сервисов 
```

## Запуск

Debezium-коннектор регистрируется через REST API уже после старта контейнеров — это делает `make start` через `make register-connector`. Makefile ждёт пока task перейдёт в `RUNNING`, только после этого считает стек готовым.

Топики Kafka (`accounts.money.transferred`, `dead-letter-queue`) создаёт отдельный контейнер `kafka-init`, который запускается один раз и завершается. `inbox` зависит от него через `service_completed_successfully` — то есть стартует только когда топики уже есть. `auto.create.topics.enable` выключен намеренно, чтобы не было ситуации «топик создался с дефолтными настройками потому что кто-то написал в него первым».

## Метрики

Prometheus scrape-ит outbox каждые 15 секунд по адресу `outbox:8080/metrics`. Grafana стартует с уже подключённым Prometheus, дашборд загружается из `grafana/provisioning/dashboards/`.

## Структура

```
docker-compose.yaml
prometheus.yml
Makefile
grafana/
  provisioning/
    datasources/prometheus.yml    — подключение к Prometheus
    dashboards/dashboards.yml
    dashboards/outbox-service.json
```
