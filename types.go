// Package workflow 提供进程内的工作流执行能力：
// 有序步骤、每步独立超时、失败重试、执行器退出跟踪，全部状态保存在内存。
package workflow

import (
	"context"
	"time"
)

// DefaultStepTimeout 是步骤未指定时限时的默认执行时限。
const DefaultStepTimeout = 120 * time.Second

// MaxAttempts 是每一步允许的最大尝试次数。
const MaxAttempts = 3

// RetryBackoff 是第 i 次尝试被判失败后，到下一次尝试的最短等待。
// 下标 0、1 分别对应第 1、2 次失败后。
var RetryBackoff = [MaxAttempts - 1]time.Duration{
	1 * time.Second,
	2 * time.Second,
}

// CallInfo 标识一次执行器调用，每次调用都能关联到所属工作流、步骤与尝试序号。
type CallInfo struct {
	WorkflowID string
	StepIndex  int
	StepName   string
	Attempt    int // 从 1 开始
}

// Executor 是调用方提供的进程内执行函数。
// ctx 到达时限或被取消时表示请求执行器停止；执行器允许不响应并在任意晚的时刻返回。
// 返回 nil 表示成功，非 nil 表示失败。
type Executor func(ctx context.Context, call CallInfo) error

// StepSpec 描述工作流中的一个步骤。
type StepSpec struct {
	Name    string
	Timeout time.Duration // 为零时使用 DefaultStepTimeout
	Exec    Executor
}

// WorkflowStatus 是工作流的业务终态/进行中状态。
type WorkflowStatus string

const (
	StatusRunning   WorkflowStatus = "Running"
	StatusSucceeded WorkflowStatus = "Succeeded"
	StatusFailed    WorkflowStatus = "Failed"
)

// AttemptResult 是一次尝试的结算结果。
type AttemptResult string

const (
	AttemptPending AttemptResult = ""        // 尚未结算
	AttemptSuccess AttemptResult = "Success" // 截止前返回成功
	AttemptFailure AttemptResult = "Failure" // 截止前返回失败
	AttemptTimeout AttemptResult = "Timeout" // 到达时限仍未返回，判超时
)

// LateResult 记录超时结算后执行器迟到返回的结果。
type LateResult string

const (
	LateNone    LateResult = ""
	LateSuccess LateResult = "LateSuccess"
	LateFailure LateResult = "LateFailure"
)

// AttemptRecord 是一次尝试的完整记录。
type AttemptRecord struct {
	Attempt        int
	Result         AttemptResult
	LateResult     LateResult // 仅超时后可能为非空
	StartedAt      time.Time  // 实际调用执行器的时刻，超时从此刻起算
	Deadline       time.Time
	SettledAt      time.Time // 被判出结果的时刻
	ExecutorExited bool      // 执行器调用是否已真正退出
	ExitedAt       time.Time
}

// StepRecord 是一个步骤的执行记录。
type StepRecord struct {
	Index    int
	Name     string
	Timeout  time.Duration
	Attempts []AttemptRecord
}

// Report 是查询返回的工作流执行情况快照。
type Report struct {
	WorkflowID string
	Status     WorkflowStatus
	// ExecutorActive 为 true 表示仍有执行器调用未退出；
	// 与 Status 组合可区分“业务已失败/成功”与“执行器尚未退出”。
	ExecutorActive bool
	Steps          []StepRecord
}
