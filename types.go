package workflow

import (
	"context"
	"time"
)

// Executor 是调用方提供的进程内执行函数。
// 返回 nil 表示成功，非 nil 表示失败。
// ctx 在到达执行时限时会被取消，用于请求执行器停止；执行器可以不响应。
type Executor func(ctx context.Context) error

// Step 是工作流中的一个有序步骤。
type Step struct {
	Name    string
	Timeout time.Duration // 单次尝试的执行时限，<=0 时使用默认 120 秒
	Exec    Executor
}

// Workflow 是由有序步骤组成的任务链。
type Workflow struct {
	Name  string
	Steps []Step
}

// AttemptState 是一次尝试的结算状态。
type AttemptState string

const (
	AttemptRunning   AttemptState = "Running"   // 已结算前：执行中或已超时但执行器未退出
	AttemptSucceeded AttemptState = "Succeeded" // 截止时刻前返回成功
	AttemptFailed    AttemptState = "Failed"    // 截止时刻前返回失败
	AttemptTimedOut  AttemptState = "TimedOut"  // 到达时限仍未返回有效结果
)

// LateResult 记录结算之后执行器迟到的返回。
type LateResult struct {
	Err        error
	ReturnedAt time.Time
}

// Attempt 是一次执行器调用的完整记录。
type Attempt struct {
	Number         int
	State          AttemptState
	StartedAt      time.Time
	Deadline       time.Time
	SettledAt      time.Time // 被判成功/失败/超时的时刻
	Err            error     // 结算时执行器返回的错误（成功为 nil，超时为 nil）
	ExecutorExited bool      // 执行器函数是否已真正返回
	LateResult     *LateResult
}

// StepState 是步骤的状态。
type StepState string

const (
	StepPending   StepState = "Pending"
	StepRunning   StepState = "Running"
	StepSucceeded StepState = "Succeeded"
	StepFailed    StepState = "Failed"
)

// StepStatus 是步骤的执行情况快照。
type StepStatus struct {
	Name     string
	State    StepState
	Attempts []Attempt
}

// WorkflowState 是工作流的业务状态。
type WorkflowState string

const (
	WorkflowRunning   WorkflowState = "Running"
	WorkflowSucceeded WorkflowState = "Succeeded"
	WorkflowFailed    WorkflowState = "Failed"
)

// Status 是工作流执行情况快照，可区分业务终态与执行器是否已退出。
type Status struct {
	ID             string
	Name           string
	State          WorkflowState
	ExecutorActive bool // 是否存在尚未退出的执行器调用
	CurrentStep    int  // 正在执行的步骤下标，终态时为 -1
	Steps          []StepStatus
}
