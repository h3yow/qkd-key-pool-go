package qkd

import "math/rand"

// LinkConfig 配置模拟量子链路的扰动，全部可本地复现。
type LinkConfig struct {
	LossRate  float64 // 单光子被丢弃的概率 [0,1]
	NoiseRate float64 // 存活光子被翻转的概率 [0,1]，贡献误码
	Seed      int64
}

// Photon 是一个模拟光子：Bit 为编码比特，Basis 为编码基（0/1）。
type Photon struct {
	Bit   byte
	Basis byte
}

// TransmitReport 记录一次传输的关键计数，用于日志与判定依据。
type TransmitReport struct {
	Sent      int
	Delivered int
	Lost      int
	Flipped   int
}

// Channel 是确定性可复现的模拟量子链路：
// 先按 LossRate 丢光子，再对存活光子按 NoiseRate 翻转比特。
type Channel struct {
	cfg LinkConfig
	rng *rand.Rand
}

func NewChannel(cfg LinkConfig) *Channel {
	if cfg.LossRate < 0 || cfg.LossRate > 1 || cfg.NoiseRate < 0 || cfg.NoiseRate > 1 {
		// 配置非法时退化为无扰动链路，保证可运行；调用方应在配置校验层拦截。
		cfg.LossRate, cfg.NoiseRate = 0, 0
	}
	return &Channel{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed))}
}

// Transmit 返回按到达顺序排列的存活光子（已按噪声翻转）。
func (c *Channel) Transmit(in []Photon) (out []Photon, report TransmitReport) {
	out, _, report = c.TransmitWithIndices(in)
	return out, report
}

// TransmitWithIndices 是规范实现：返回存活光子（已翻转）及其在输入中的原始下标。
// 每个光子依次消耗两次随机数（丢包判定、翻转判定），序列固定、可复现。
func (c *Channel) TransmitWithIndices(in []Photon) (out []Photon, indices []int, report TransmitReport) {
	report.Sent = len(in)
	out = make([]Photon, 0, len(in))
	indices = make([]int, 0, len(in))
	for i, ph := range in {
		if c.rng.Float64() < c.cfg.LossRate {
			report.Lost++
			continue
		}
		if c.rng.Float64() < c.cfg.NoiseRate {
			ph.Bit ^= 1
			report.Flipped++
		}
		out = append(out, ph)
		indices = append(indices, i)
	}
	report.Delivered = len(out)
	return out, indices, report
}
