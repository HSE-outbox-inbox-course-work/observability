OUTBOX_DIR := ../outbox-payments-service

.PHONY: start stop restart register-connector transfer check check-outbox logs logs-outbox logs-inbox status

# Start

start:
	docker compose up -d --build
	@$(MAKE) -s _wait-outbox
	@$(MAKE) -s register-connector
	@echo ""
	@echo "Стек запущен:"
	@echo "  Outbox API:  http://localhost:8080"
	@echo "  Kafka UI:    http://localhost:8081"
	@echo "  Kafka Conn:  http://localhost:8083"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  Grafana:     http://localhost:3000  (admin/admin)"

stop:
	docker compose down -v
	@echo "◼ Всё остановлено, данные очищены"

restart: stop start

# Waitin for ready

_wait-connect:
	@echo "Waiting for kafka connect..."
	@until curl -sf localhost:8083/connectors > /dev/null 2>&1; do \
		printf "."; sleep 3; \
	done
	@echo " готов"

_wait-outbox:
	@echo "Waiting for healthcheck..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' outbox 2>/dev/null)" = "healthy" ]; do \
		printf "."; sleep 2; \
	done
	@echo " готов"

# Debezium-connector

register-connector:
	@$(MAKE) -s _wait-connect
	@echo "Registring Debezium-connector..."
	@cd $(OUTBOX_DIR) && make -s kafka-connect-create-outbox-connector > /dev/null
	@echo "Waiting task RUNNING..."
	@until curl -sf localhost:8083/connectors/outbox-connector/status 2>/dev/null \
		| python3 -q -c \
		  "import sys,json; s=json.load(sys.stdin); exit(0 if s.get('tasks') and s['tasks'][0]['state']=='RUNNING' else 1)" \
		2>/dev/null; do \
		printf "."; sleep 2; \
	done
	@echo "ready"

# test transfer

transfer:
	@curl -s -X POST localhost:8080/api/v1/accounts/transfer-money \
		-H "Content-Type: application/json" \
		-d '{"from_account":"22222222-2222-2222-2222-222222222222","to_account":"11111111-1111-1111-1111-111111111111","amount":1}'
	@echo ""

# Making transfers in a circle RPS ~2/сек

load:
	@echo "Making transfers..."
	@i=0; while true; do \
		case $$((i % 4)) in \
			0) FROM="11111111-1111-1111-1111-111111111111"; TO="22222222-2222-2222-2222-222222222222" ;; \
			1) FROM="22222222-2222-2222-2222-222222222222"; TO="33333333-3333-3333-3333-333333333333" ;; \
			2) FROM="33333333-3333-3333-3333-333333333333"; TO="44444444-4444-4444-4444-444444444444" ;; \
			3) FROM="44444444-4444-4444-4444-444444444444"; TO="11111111-1111-1111-1111-111111111111" ;; \
		esac; \
		curl -s -X POST localhost:8080/api/v1/accounts/transfer-money \
			-H "Content-Type: application/json" \
			-d "{\"from_account\":\"$$FROM\",\"to_account\":\"$$TO\",\"amount\":1}" > /dev/null; \
		i=$$((i+1)); \
		sleep 0.5; \
	done

check-inbox:
	@echo "=== inbox_order ==="
	@docker exec postgres-inbox psql -U postgres -d inbox \
		-c "SELECT transfer_id, status, created_at FROM inbox_order ORDER BY created_at DESC LIMIT 10;"

check-outbox:
	@echo "=== outbox ==="
	@docker exec postgres psql -U admin -d payments-service-db \
		-c "SELECT id, event_type, created_at FROM outbox ORDER BY created_at DESC LIMIT 10;"

status:
	@docker compose ps

# logs

logs:
	@docker compose logs -f outbox inbox

logs-outbox:
	@docker compose logs -f outbox

logs-inbox:
	@docker compose logs -f inbox
