// loadgen — генератор нагрузки и тестовых сценариев.
//
// Сценарии:
//
//	transfer         POST /api/v1/accounts/transfer-money между seed-аккаунтами с заданным RPS.
//	spike            Трапеция RPS: --rps → --spike-rps → --rps за --duration.
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
	scenario := flag.String("scenario", "transfer", "transfer | spike | invalid-payload")
	target := flag.String("target", "http://localhost:8080", "outbox-service base URL (для scenario=transfer/spike)")
	broker := flag.String("broker", "localhost:29092", "Kafka broker (для scenario=invalid-payload)")
	topic := flag.String("topic", "accounts.money.transferred", "Kafka topic для invalid-payload")
	rps := flag.Int("rps", 5, "целевой RPS для transfer/spike")
	spikeRPS := flag.Int("spike-rps", 50, "пиковый RPS для spike")
	duration := flag.Duration("duration", 0, "сколько работать, 0 = бесконечно")
	count := flag.Int("count", 0, "количество запросов (для invalid-payload); 0 = по duration")
	amount := flag.Int("amount", 1, "сумма перевода для transfer")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch *scenario {
	case "transfer":
		runTransferSteady(ctx, *target, *rps, *duration, *amount)
	case "spike":
		runSpike(ctx, *target, *rps, *spikeRPS, *duration, *amount)
	case "invalid-payload":
		runInvalidPayload(ctx, *broker, *topic, *count, *duration)
	default:
		log.Fatalf("unknown scenario: %q", *scenario)
	}
}

// transfer

func runTransferSteady(ctx context.Context, target string, rps int, dur time.Duration, amount int) {
	log.Printf("loadgen: transfer rps=%d duration=%s target=%s", rps, fmtDur(dur), target)

	tick := newRateTicker(ctx, rps)
	defer tick.Stop()

	var sent, ok atomic.Int64
	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()

	deadline := neverDeadline(ctx, dur)
	client := &http.Client{Timeout: 5 * time.Second}

	i := 0
	for {
		select {
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
				if err := postTransfer(ctx, client, target, from, to, amount); err != nil {
					return
				}
				ok.Add(1)
			}()
		}
	}
}

func runSpike(ctx context.Context, target string, baseRPS, peakRPS int, dur time.Duration, amount int) {
	if dur == 0 {
		dur = 30 * time.Second
	}
	log.Printf("loadgen: spike base=%d peak=%d duration=%s", baseRPS, peakRPS, dur)

	// Ramp-up 25% времени, плато 50%, ramp-down 25%. На графиках это даёт
	// узнаваемый профиль «трапеция», по которому легко видеть отклик SLI.
	rampUp := dur / 4
	plateau := dur / 2
	rampDown := dur - rampUp - plateau

	runRamp(ctx, target, baseRPS, peakRPS, rampUp, amount)
	if ctx.Err() == nil {
		runTransferSteady(ctx, target, peakRPS, plateau, amount)
	}
	if ctx.Err() == nil {
		runRamp(ctx, target, peakRPS, baseRPS, rampDown, amount)
	}
}

func runRamp(ctx context.Context, target string, fromRPS, toRPS int, dur time.Duration, amount int) {
	steps := 10
	step := dur / time.Duration(steps)
	for i := 0; i < steps; i++ {
		if ctx.Err() != nil {
			return
		}
		rate := fromRPS + (toRPS-fromRPS)*i/(steps-1)
		runTransferSteady(ctx, target, rate, step, amount)
	}
}

func postTransfer(ctx context.Context, c *http.Client, target, from, to string, amount int) error {
	body, _ := json.Marshal(map[string]any{
		"from_account": from,
		"to_account":   to,
		"amount":       amount,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, target+"/api/v1/accounts/transfer-money", bytes.NewReader(body))
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

// runInvalidPayload публикует пять разновидностей битых сообщений в
// accounts.money.transferred, минуя outbox-сервис. Inbox прогонит их
// через validate() и отправит в DLQ.
func runInvalidPayload(ctx context.Context, broker, topic string, count int, dur time.Duration) {
	if count == 0 && dur == 0 {
		count = 20
	}
	log.Printf("loadgen: invalid-payload broker=%s topic=%s count=%d duration=%s", broker, topic, count, fmtDur(dur))

	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	badPayloads := []map[string]any{
		// Пустой transfer_id → validation_errors_total{field="transfer_id"}
		{"transfer_id": "", "amount": 1, "from_account": "a", "to_account": "b"},
		// Отрицательная сумма
		{"transfer_id": randID(), "amount": -100, "from_account": "a", "to_account": "b"},
		// Пустой from_account
		{"transfer_id": randID(), "amount": 1, "from_account": "", "to_account": "b"},
		// Пустой to_account
		{"transfer_id": randID(), "amount": 1, "from_account": "a", "to_account": ""},
		// «Дырявый» payload — нет полей вообще, упадёт на transfer_id
		{},
	}

	deadline := neverDeadline(ctx, dur)
	sent := 0
	tick := time.NewTicker(200 * time.Millisecond) // ~5 msg/sec
	defer tick.Stop()

	for {
		if count > 0 && sent >= count {
			log.Printf("loadgen: invalid-payload done, sent=%d", sent)
			return
		}
		select {
		case <-deadline:
			log.Printf("loadgen: invalid-payload duration elapsed, sent=%d", sent)
			return
		case <-ctx.Done():
			return
		case <-tick.C:
			payload := badPayloads[sent%len(badPayloads)]
			inner, _ := json.Marshal(payload)
			// Debezium EventRouter оборачивает payload в {"payload":"<json string>"}.
			// Воспроизводим эту обёртку, чтобы Inbox.Listener шёл по обычной ветке.
			outer, _ := json.Marshal(map[string]string{"payload": string(inner)})
			if err := w.WriteMessages(ctx, kafka.Message{Value: outer}); err != nil {
				log.Printf("kafka write error: %v", err)
				continue
			}
			sent++
		}
	}
}

// helpers

// rateTicker генерирует тики с заданной частотой. При rps<=0 тиков не будет
type rateTicker struct {
	C <-chan time.Time
	t *time.Ticker
}

func newRateTicker(_ context.Context, rps int) *rateTicker {
	if rps <= 0 {
		return &rateTicker{C: make(chan time.Time)}
	}
	t := time.NewTicker(time.Second / time.Duration(rps))
	return &rateTicker{C: t.C, t: t}
}

func (r *rateTicker) Stop() {
	if r.t != nil {
		r.t.Stop()
	}
}

// neverDeadline возвращает канал, который никогда не сработает при dur=0
// (бесконечный режим), и таймер на dur иначе. Это упрощает основной
// select без if/else по dur внутри цикла. ctx уже наблюдается отдельным
// case'ом, так что dur=0 = «до Ctrl+C / SIGTERM».
func neverDeadline(_ context.Context, dur time.Duration) <-chan time.Time {
	if dur == 0 {
		return nil
	}
	return time.After(dur)
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
