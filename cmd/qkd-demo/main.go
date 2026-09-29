// Command qkd-demo runs a fully local end-to-end demonstration: a QKD
// negotiation over the simulated link, followed (only on success) by a key
// lease/confirm against the in-memory pool. No network, quantum device or real
// credential is used.
//
// Everything is seeded/configurable so a run is reproducible.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/h3yow/qkd-key-pool-go/link"
	"github.com/h3yow/qkd-key-pool-go/pool"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
	"github.com/h3yow/qkd-key-pool-go/session"
)

func main() {
	var (
		loss        = flag.Float64("loss", 0.0, "classical frame loss rate 0..1")
		disturb     = flag.Float64("disturb", 0.01, "channel disturbance rate (drives QBER)")
		noise       = flag.Float64("noise", 0.02, "detector noise rate")
		baseQBER    = flag.Float64("base-qber", 0.02, "intrinsic QBER floor")
		threshold   = flag.Float64("qber", 0.11, "QBER abort threshold")
		seed        = flag.Int64("seed", 1, "PRNG seed for reproducibility")
		peerAbort   = flag.Bool("peer-abort", false, "peer aborts during sifting")
		peerNoConf  = flag.Bool("peer-noconfirm", false, "peer fails confirmation")
		peerVersion = flag.String("peer-version", "", "override peer protocol version")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags)
	cfg := session.Config{
		SessionID:       "demo-session",
		QBERThreshold:   *threshold,
		StepTimeout:     3 * time.Second,
		PeerForceAbort:  *peerAbort,
		PeerFailConfirm: *peerNoConf,
		Link: link.Config{
			LossRate:      *loss,
			Disturbance:   *disturb,
			DetectorNoise: *noise,
			BaseQBER:      *baseQBER,
			Seed:          *seed,
			MaxAttempts:   4,
		},
	}
	if *peerVersion != "" {
		cfg.PeerVersion = protoVersion(*peerVersion)
	}

	n := session.New(cfg, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := n.Run(ctx)
	fmt.Printf("\n=== negotiation result ===\nstate: %s\n", n.State())
	if err != nil {
		fmt.Printf("reason: [%s] %v\n", qkderr.KindOf(err), err)
		if n.State() != session.StateEstablished {
			fmt.Println("session did not establish; no key will be issued.")
			os.Exit(1)
		}
	}

	// Only an established session reaches the key pool.
	p := pool.New(pool.Config{LeaseTTL: 30 * time.Second})
	if err := p.Generate(1, 32, time.Hour); err != nil {
		logger.Fatalf("seed pool: %v", err)
	}
	lease, err := p.Lease(ctx)
	if err != nil {
		logger.Fatalf("lease: [%s] %v", qkderr.KindOf(err), err)
	}
	fmt.Printf("\n=== key pool ===\nleased key id: %s (lease %s, ttl %s)\n",
		lease.KeyID, lease.ID, time.Until(lease.Expires()).Round(time.Second))

	if err := p.Confirm(ctx, lease.ID); err != nil {
		logger.Fatalf("confirm: [%s] %v", qkderr.KindOf(err), err)
	}
	avail, leased, consumed, expired := p.Len()
	fmt.Printf("consumed key. pool: available=%d leased=%d consumed=%d expired=%d\n",
		avail, leased, consumed, expired)
}
