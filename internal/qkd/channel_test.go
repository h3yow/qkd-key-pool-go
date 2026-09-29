package qkd

import "testing"

func TestChannelReproducibleAndCounts(t *testing.T) {
	cfg := LinkConfig{LossRate: 0.3, NoiseRate: 0.2, Seed: 42}
	in := make([]Photon, 2000)
	for i := range in {
		in[i] = Photon{Bit: byte(i % 2), Basis: byte((i / 2) % 2)}
	}

	c1 := NewChannel(cfg)
	out1, r1 := c1.Transmit(in)
	c2 := NewChannel(cfg)
	out2, idx2, r2 := c2.TransmitWithIndices(in)

	if r1 != r2 {
		t.Fatalf("same seed must give same report: %+v vs %+v", r1, r2)
	}
	if len(out1) != len(out2) {
		t.Fatalf("same seed must give same delivered count")
	}
	for i := range out1 {
		if out1[i] != out2[i] {
			t.Fatalf("delivered photon %d diverges: %+v vs %+v", i, out1[i], out2[i])
		}
	}
	if r1.Sent != 2000 || r1.Delivered+r1.Lost != 2000 {
		t.Fatalf("accounting broken: %+v", r1)
	}
	if r1.Delivered < 1000 || r1.Delivered > 1800 {
		t.Fatalf("loss ~0.3 expected ~1400 delivered, got %d", r1.Delivered)
	}
	if r1.Flipped < 150 || r1.Flipped > 600 {
		t.Fatalf("noise ~0.2 on delivered expected ~%d flips, got %d", r1.Delivered/5, r1.Flipped)
	}
	if len(idx2) != r2.Delivered {
		t.Fatalf("indices length must equal delivered")
	}
	t.Logf("report sent=%d delivered=%d lost=%d flipped=%d", r1.Sent, r1.Delivered, r1.Lost, r1.Flipped)
}

func TestChannelTotalLoss(t *testing.T) {
	ch := NewChannel(LinkConfig{LossRate: 1, Seed: 1})
	out, r := ch.Transmit([]Photon{{Bit: 1, Basis: 0}, {Bit: 0, Basis: 1}})
	if len(out) != 0 || r.Lost != 2 {
		t.Fatalf("total loss expected, got out=%v report=%+v", out, r)
	}
}
