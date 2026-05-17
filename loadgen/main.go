// loadgen — генератор нагрузки и тестовых сценариев.
//
// Сценарии:
//
//	transfer         POST /api/v1/accounts/transfer-money между seed-аккаунтами с заданным RPS.
//	invalid-payload  Битые сообщения напрямую в Kafka-топик accounts.money.transferred,
//	                 минуя outbox-сервис. Использует тот же envelope-формат, что и Debezium.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// seedAccounts — UUID из миграции outbox-сервиса. Перебираем по кругу,
// чтобы балансы не уходили в 0.
var seedAccounts = []string{
	"11111111-1111-1111-1111-111111111111",
	"22222222-2222-2222-2222-222222222222",
	"33333333-3333-3333-3333-333333333333",
	"44444444-4444-4444-4444-444444444444",
}

func main() {
	scenario := flag.String("scenario", "transfer", "transfer | invalid-payload")
	target := flag.String("target", "http://localhost:8080", "outbox-service base URL")
	broker := flag.String("broker", "localhost:29092", "Kafka broker для invalid-payload")
	topic := flag.String("topic", "accounts.money.transferred", "Kafka topic для invalid-payload")
	rps := flag.Int("rps", 5, "RPS для transfer")
	duration := flag.Duration("duration", 0, "сколько работать, 0 = до Ctrl+C")
	count := flag.Int("count", 20, "сколько сообщений отправить для invalid-payload")
	amount := flag.Int("amount", 1, "сумма перевода")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch *scenario {
	case "transfer":
		runTransfer(ctx, *target, *rps, *duration, *amount)
	case "invalid-payload":
		runInvalidPayload(ctx, *broker, *topic, *count)
	default:
		log.Fatalf("unknown scenario: %q", *scenario)
	}
}

// ─────────────────────────── transfer ───────────────────────────

func runTransfer(ctx context.Context, target string, rps int, dur time.Duration, amount int) {
	log.Printf("loadgen: transfer rps=%d duration=%s target=%s", rps, fmtDur(dur), target)

	if rps <= 0 {
		log.Fatalf("rps must be > 0")
	}

	tick := time.NewTicker(time.Second / time.Duration(rps))
	defer tick.Stop()

	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()

	var deadline <-chan time.Time
	if dur > 0 {
		deadline = time.After(dur)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	var sent, ok atomic.Int64
	i := 0

	for {
		select {
		case <-ctx.Done():
			report(sent.Load(), ok.Load())
			return
		case <-deadline:
			report(sent.Load(), ok.Load())
			return
		case <-logEvery.C:
			log.Printf("loadgen: sent=%d ok=%d", sent.Load(), ok.Load())
		case <-tick.C:
			from := seedAccounts[i%len(seedAccounts)]
			to := seedAccounts[(i+1)%len(seedAccounts)]
			i++
			go func() {
				sent.Add(1)
				if err := postTransfer(ctx, client, target, from, to, amount); err == nil {
					ok.Add(1)
				}
			}()
		}
	}
}

func postTransfer(ctx context.Context, c *http.Client, target, from, to string, amount int) error {
	body, _ := json.Marshal(map[string]any{
		"from_account": from,
		"to_account":   to,
		"amount":       amount,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		target+"/api/v1/accounts/transfer-money", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// runInvalidPayload публикует битые сообщения напрямую в Kafka, минуя
// outbox-сервис. Inbox прогонит их через validate() и отправит в DLQ.
func runInvalidPayload(ctx context.Context, broker, topic string, count int) {
	log.Printf("loadgen: invalid-payload broker=%s topic=%s count=%d", broker, topic, count)

	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	// Пять разновидностей битых payload'ов, по одной на ошибку.
	badPayloads := []map[string]any{
		{"transfer_id": "", "amount": 1, "from_account": "a", "to_account": "b"},
		{"transfer_id": randID(), "amount": -100, "from_account": "a", "to_account": "b"},
		{"transfer_id": randID(), "amount": 1, "from_account": "", "to_account": "b"},
		{"transfer_id": randID(), "amount": 1, "from_account": "a", "to_account": ""},
		{},
	}

	tick := time.NewTicker(200 * time.Millisecond) // ~5 msg/sec
	defer tick.Stop()

	for sent := 0; sent < count; {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			payload := badPayloads[sent%len(badPayloads)]
			if err := writeEnvelope(ctx, w, payload); err != nil {
				log.Printf("kafka write error: %v", err)
				continue
			}
			sent++
		}
	}
	log.Printf("loadgen: invalid-payload done")
}

// writeEnvelope воспроизводит обёртку {"payload":"<json string>"}, в
// которой Debezium EventRouter публикует события — чтобы Inbox.Listener
// проходил по обычной ветке десериализации.
func writeEnvelope(ctx context.Context, w *kafka.Writer, payload map[string]any) error {
	inner, _ := json.Marshal(payload)
	outer, _ := json.Marshal(map[string]string{"payload": string(inner)})
	return w.WriteMessages(ctx, kafka.Message{Value: outer})
}

func report(sent, ok int64) {
	log.Printf("loadgen: done sent=%d ok=%d", sent, ok)
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "∞"
	}
	return d.String()
}

// randID возвращает строку UUID-формата (Inbox проверяет лишь то, что
// поле не пустое, версия UUID для него не важна).
func randID() string {
	const hex = "0123456789abcdef"
	b := make([]byte, 36)
	for i := range b {
		switch i {
		case 8, 13, 18, 23:
			b[i] = '-'
		default:
			b[i] = hex[rand.Intn(16)]
		}
	}
	return string(b)
}
