package qkd

import "fmt"

// FailureCode 标识协商失败的可区分原因。
type FailureCode string

const (
	FailureQBERTooHigh     FailureCode = "qber_too_high"       // 误码率超阈值
	FailureConfirmation    FailureCode = "confirmation_failed" // 确认阶段失败
	FailurePeerAborted     FailureCode = "peer_aborted"        // 对端主动中止
	FailureVersionIncompat FailureCode = "version_incompatible"
	FailureNoKeyAgreed     FailureCode = "no_key_agreed" // 筛选后无可用比特
	FailureInternal        FailureCode = "internal"      // 仅内部使用，对外脱敏
)

// Failure 表示协商停在某个明确状态。
// Error() 面向内部日志（含 Step/Detail/cause）；
// Public() 得到可跨信任边界暴露的副本（剥离内部 cause 与内部细节）。
type Failure struct {
	Code   FailureCode
	Step   State
	Detail string
	cause  error
}

func NewFailure(code FailureCode, step State, detail string, cause error) *Failure {
	return &Failure{Code: code, Step: step, Detail: detail, cause: cause}
}

func (f *Failure) Error() string {
	if f.cause != nil {
		return fmt.Sprintf("qkd negotiation failed: code=%s step=%s detail=%s cause=%v",
			f.Code, f.Step, f.Detail, f.cause)
	}
	return fmt.Sprintf("qkd negotiation failed: code=%s step=%s detail=%s",
		f.Code, f.Step, f.Detail)
}

func (f *Failure) Unwrap() error { return f.cause }

// Public 返回可跨边界暴露的副本：剥离内部 cause，且 internal 类原因被泛化。
func (f *Failure) Public() *Failure {
	pub := &Failure{Code: f.Code, Step: f.Step, Detail: f.Detail}
	if pub.Code == FailureInternal {
		pub.Code = FailureInternal
		pub.Detail = "negotiation aborted due to an internal error"
	}
	return pub
}
