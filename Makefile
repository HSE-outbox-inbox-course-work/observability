OUTBOX_DIR    := ../outbox-payments-service
INBOX_DIR     := ../Inbox

OUTBOX_API    := http://localhost:8080
INBOX_METRICS := http://localhost:8082/metrics
KAFKA_BROKER  := localhost:29092

KAFKA_TOPIC   := accounts.money.transferred

.PHONY: help start stop restart ps urls logs \
        register-connector unregister-connector wait-connect wait-outbox wait-inbox \
        load load-spike \
        chaos-dlq chaos-duplicates chaos-debezium-off chaos-debezium-on \
        demo demo-step \
        check-outbox check-inbox check-alerts \
        loadgen-build

help: ## Показать список команд
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-24s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Запуск / остановка

start: ## Поднять весь стек (compose up + регистрация Debezium-коннектора)
	docker compose up -d --build
	@$(MAKE) -s wait-outbox
	@$(MAKE) -s wait-inbox
	@$(MAKE) -s register-connector
	@$(MAKE) -s urls

stop: ## Остановить стек, удалить volumes (данные очищаются)
	docker compose down -v
	@echo "stack stopped, volumes removed"

restart: stop start ## stop + start

urls: ## Напечатать адреса сервисов
	@echo
	@echo "Services:"
	@echo "  Outbox API       $(OUTBOX_API)"
	@echo "  Outbox /metrics  $(OUTBOX_API)/metrics"
	@echo "  Inbox /metrics   $(INBOX_METRICS)"
	@echo "  Kafka UI         http://localhost:8081"
	@echo "  Kafka Connect    http://localhost:8083"
	@echo "  Prometheus       http://localhost:9090"
	@echo "  Alertmanager     http://localhost:9093"
	@echo "  Grafana          http://localhost:3000  (admin / admin)"
	@echo

logs: ## Логи. Указать сервис: make logs c=outbox
	@docker compose logs -f $(if $(c),$(c),outbox inbox)

# Ожидание готовности 

wait-outbox:
	@printf "waiting for outbox /metrics..."
	@until curl -sf $(OUTBOX_API)/metrics >/dev/null 2>&1; do printf "."; sleep 1; done
	@echo " ok"

wait-inbox:
	@printf "waiting for inbox /metrics..."
	@until curl -sf $(INBOX_METRICS) >/dev/null 2>&1; do printf "."; sleep 1; done
	@echo " ok"

wait-connect:
	@printf "waiting for kafka connect..."
	@until curl -sf localhost:8083/connectors >/dev/null 2>&1; do printf "."; sleep 1; done
	@echo " ok"

# Debezium-коннектор

register-connector: wait-connect ## Зарегистрировать Debezium коннектор и дождаться RUNNING
	@if curl -sf localhost:8083/connectors/outbox-connector >/dev/null 2>&1; then \
		echo "outbox-connector already registered"; \
	else \
		echo "registering outbox-connector..."; \
		curl -sS -X POST localhost:8083/connectors \
			-H "Content-Type: application/json" \
			-d @$(OUTBOX_DIR)/migrations/connect/outbox.json >/dev/null; \
	fi
	@printf "waiting for connector task RUNNING..."
	@until curl -sf localhost:8083/connectors/outbox-connector/status 2>/dev/null \
		| python3 -c "import sys,json; s=json.load(sys.stdin); exit(0 if s.get('tasks') and s['tasks'][0]['state']=='RUNNING' else 1)" \
		2>/dev/null; do \
		printf "."; sleep 1; \
	done
	@echo " ok"

unregister-connector: ## Удалить Debezium коннектор (slot останется в Postgres)
	@curl -sS -X DELETE localhost:8083/connectors/outbox-connector >/dev/null || true
	@echo "outbox-connector removed"

check-outbox: ## Последние 10 строк таблицы outbox
	@docker exec postgres psql -U admin -d payments-service-db \
		-c "SELECT id, event_type, created_at FROM outbox ORDER BY created_at DESC LIMIT 10;"

check-inbox: ## Последние 10 строк inbox_order и распределение по статусам
	@docker exec postgres-inbox psql -U postgres -d inbox \
		-c "SELECT transfer_id, status, created_at FROM inbox_order ORDER BY created_at DESC LIMIT 10;"
	@docker exec postgres-inbox psql -U postgres -d inbox \
		-c "SELECT status, count(*) FROM inbox_order GROUP BY status;"

check-alerts: ## Текущие active alerts в Alertmanager
	@curl -s localhost:9093/api/v2/alerts | python3 -m json.tool || true
