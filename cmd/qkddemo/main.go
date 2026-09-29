// Command qkddemo 在单机上演示一次完整的模拟 QKD 协商与密钥池流程，
// 不依赖任何真实量子设备、外部服务或凭据。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/h3yow/qkd-key-pool-go/internal/pool"
	"github.com/h3yow/qkd-key-pool-go/internal/qkd"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) { log.Printf(format, args...) }

func runNegotiation(name string, mutate func(*qkd.SessionConfig)) {
	log.Printf("=== scenario: %s ===", name)
	cfg := qkd.SessionConfig{
		Version:       qkd.ProtocolVersion1,
		PhotonCount:   2048,
		QBERThreshold: 0.15,
		Link: qkd.LinkConfig{
			LossRate:  0.15,
			NoiseRate: 0.05,
			Seed:      100,
		},
		MessageTTL: 5 * time.Second,
		Seed:       100,
		Now:        time.Now,
		Log:        stdLogger{},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s := qkd.NewSession("demo-"+name, cfg)
	res, err := s.Run(context.Background())
	if err != nil {
		var f *qkd.Failure
		if errors.As(err, &f) {
			// 跨边界只暴露脱敏后的信息。
			pub := f.Public()
			log.Printf("[%s] STOPPED state=%s code=%s step=%s detail=%q",
				name, res.State, pub.Code, pub.Step, pub.Detail)
			return
		}
		log.Printf("[%s] unexpected error: %v", name, err)
		return
	}
	log.Printf("[%s] ESTABLISHED qber=%.4f keyBytes=%d events=%d",
		name, res.QBER, len(res.SharedKey), len(res.Events))
}

func demoPool() {
	log.Printf("=== scenario: key pool concurrent contention ===")
	p := pool.New(pool.Config{
		DefaultTTL:   200 * time.Millisecond,
		ReapInterval: 50 * time.Millisecond,
		Log:          stdLogger{},
	})
	p.AddKey("k-only", []byte("demo-key-material-32B-padding-xx"), time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	go p.RunReaper(ctx)
	defer cancel()

	const contenders = 8
	var wg sync.WaitGroup
	var winnerMu sync.Mutex
	winners := 0
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l, err := p.Lease(fmt.Sprintf("session-%d", i), time.Now())
			if err != nil {
				log.Printf("contender %d: lease rejected (%v)", i, err)
				return
			}
			winnerMu.Lock()
			winners++
			winnerMu.Unlock()
			log.Printf("contender %d: WON lease=%s key=%s", i, l.ID, l.KeyID)
			if err := p.Confirm(l.ID, time.Now()); err != nil {
				log.Printf("contender %d: confirm error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	log.Printf("contention result: %d contenders, %d winner (must be exactly 1)", contenders, winners)

	time.Sleep(300 * time.Millisecond)
}

func main() {
	runNegotiation("healthy", nil)
	runNegotiation("high-qber", func(c *qkd.SessionConfig) { c.Link.NoiseRate = 0.6 })
	runNegotiation("peer-abort", func(c *qkd.SessionConfig) { c.PeerAbortAfter = qkd.StateSifting })
	runNegotiation("bad-version", func(c *qkd.SessionConfig) { c.PeerVersion = "QKD/0.9" })
	demoPool()
	log.Printf("demo finished")
}
