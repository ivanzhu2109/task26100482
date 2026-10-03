package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotFound 表示查询的工作流不存在。
var ErrNotFound = errors.New("workflow: not found")

// ErrInvalidSpec 表示提交的工作流规格不合法。
var ErrInvalidSpec = errors.New("workflow: invalid spec")

// Engine 是工作流执行引擎，全部状态保存在进程内存中。
type Engine struct {
	mu      sync.Mutex
	seq     int
	impl    map[string]*workflowState
	backoff [MaxAttempts - 1]time.Duration
}

// New 创建一个引擎实例，重试等待取自包级默认 RetryBackoff。
func New() *Engine {
	return &Engine{impl: make(map[string]*workflowState), backoff: RetryBackoff}
}

// Submit 提交一个工作流，返回工作流 ID。工作流在后台异步执行。
func (e *Engine) Submit(steps []StepSpec) (string, error) {
	if len(steps) == 0 {
		return "", fmt.Errorf("%w: no steps", ErrInvalidSpec)
	}
	for i, s := range steps {
		if s.Exec == nil {
			return "", fmt.Errorf("%w: step %d has no executor", ErrInvalidSpec, i)
		}
		if s.Timeout < 0 {
			return "", fmt.Errorf("%w: step %d has negative timeout", ErrInvalidSpec, i)
		}
	}

	e.mu.Lock()
	e.seq++
	id := fmt.Sprintf("wf-%d", e.seq)
	ws := &workflowState{
		id:      id,
		specs:   append([]StepSpec(nil), steps...),
		done:    make(chan struct{}),
		backoff: e.backoff,
	}
	ws.status = StatusRunning
	ws.steps = make([]StepRecord, len(steps))
	for i, s := range steps {
		to := s.Timeout
		if to == 0 {
			to = DefaultStepTimeout
		}
		ws.steps[i] = StepRecord{Index: i, Name: s.Name, Timeout: to}
	}
	e.impl[id] = ws
	e.mu.Unlock()

	go ws.run()
	return id, nil
}

// Query 查询工作流执行情况快照。
func (e *Engine) Query(id string) (Report, error) {
	e.mu.Lock()
	ws, ok := e.impl[id]
	e.mu.Unlock()
	if !ok {
		return Report{}, ErrNotFound
	}
	return ws.snapshot(), nil
}

// execResult 是执行器一次调用的返回通知。
type execResult struct {
	err      error
	exitedAt time.Time
}

// workflowState 是单个工作流的内部运行状态。
// 由一个 run goroutine 驱动，记录字段受 mu 保护，供 Query 读取。
type workflowState struct {
	id      string
	specs   []StepSpec
	backoff [MaxAttempts - 1]time.Duration

	mu             sync.Mutex
	status         WorkflowStatus
	steps          []StepRecord
	executorActive bool
	done           chan struct{} // 业务终态时关闭
}

func (ws *workflowState) snapshot() Report {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	steps := make([]StepRecord, len(ws.steps))
	for i, s := range ws.steps {
		steps[i] = s
		steps[i].Attempts = append([]AttemptRecord(nil), s.Attempts...)
	}
	return Report{
		WorkflowID:     ws.id,
		Status:         ws.status,
		ExecutorActive: ws.executorActive,
		Steps:          steps,
	}
}

// run 是工作流的主驱动循环：按顺序执行各步骤。
func (ws *workflowState) run() {
	for i := range ws.specs {
		if !ws.runStep(i) {
			return // 工作流已失败
		}
	}
	ws.mu.Lock()
	ws.status = StatusSucceeded
	ws.mu.Unlock()
	close(ws.done)
}

// runStep 执行单个步骤，返回步骤是否成功。
func (ws *workflowState) runStep(stepIdx int) bool {
	spec := ws.specs[stepIdx]
	timeout := ws.steps[stepIdx].Timeout

	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		res := ws.runAttempt(stepIdx, attempt, spec, timeout)

		ws.mu.Lock()
		rec := &ws.steps[stepIdx].Attempts[len(ws.steps[stepIdx].Attempts)-1]
		rec.SettledAt = res.settledAt
		switch {
		case res.valid && res.err == nil:
			rec.Result = AttemptSuccess
		case res.valid:
			rec.Result = AttemptFailure
		default:
			rec.Result = AttemptTimeout
		}
		result := rec.Result
		ws.mu.Unlock()

		if result == AttemptSuccess {
			return true
		}
		if attempt == MaxAttempts {
			ws.fail()
			ws.watchLateExit(stepIdx, attempt, res)
			return false
		}
		// 重试必须同时满足：最短等待已结束，且上一次执行器调用已退出。
		waitUntil := res.settledAt.Add(ws.backoff[attempt-1])
		ws.awaitRetry(waitUntil, stepIdx, attempt, res)
	}
	return false
}

// attemptOutcome 是一次尝试结算后传递给调度逻辑的信息。
type attemptOutcome struct {
	valid     bool  // 是否在截止前返回（有效结果）
	err       error // 有效结果的错误；迟到结果不计入
	settledAt time.Time
	done      chan execResult // 执行器退出通知（若尚未收到）
	received  bool            // done 是否已收到
}

// runAttempt 发起一次执行器调用并等待结算（成功/失败/超时三者之一）。
func (ws *workflowState) runAttempt(stepIdx, attempt int, spec StepSpec, timeout time.Duration) attemptOutcome {
	started := time.Now()
	deadline := started.Add(timeout)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	done := make(chan execResult, 1)
	call := CallInfo{
		WorkflowID: ws.id,
		StepIndex:  stepIdx,
		StepName:   spec.Name,
		Attempt:    attempt,
	}

	ws.mu.Lock()
	ws.executorActive = true
	ws.steps[stepIdx].Attempts = append(ws.steps[stepIdx].Attempts, AttemptRecord{
		Attempt:   attempt,
		StartedAt: started,
		Deadline:  deadline,
	})
	ws.mu.Unlock()

	go func() {
		err := spec.Exec(ctx, call)
		done <- execResult{err: err, exitedAt: time.Now()}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case r := <-done:
			ws.markExited(stepIdx, attempt, r.exitedAt)
			if r.exitedAt.Before(deadline) {
				// 严格早于截止时刻：有效结果。
				return attemptOutcome{valid: true, err: r.err, settledAt: r.exitedAt, received: true}
			}
			// 截止时刻或之后返回：只能作为迟到结果，按超时结算。
			ws.markLate(stepIdx, attempt, r.err)
			return attemptOutcome{valid: false, settledAt: deadline, received: true}
		case <-timer.C:
			// 到达时限：判超时。若执行器恰好同时返回，以其退出时刻判定有效性。
			select {
			case r := <-done:
				ws.markExited(stepIdx, attempt, r.exitedAt)
				if r.exitedAt.Before(deadline) {
					return attemptOutcome{valid: true, err: r.err, settledAt: r.exitedAt, received: true}
				}
				ws.markLate(stepIdx, attempt, r.err)
				return attemptOutcome{valid: false, settledAt: deadline, received: true}
			default:
			}
			return attemptOutcome{valid: false, settledAt: deadline, done: done}
		}
	}
}

// awaitRetry 等待重试条件满足：最短等待结束，且上一次执行器调用已退出。
// attempt 为 1 基尝试号。
func (ws *workflowState) awaitRetry(waitUntil time.Time, stepIdx, attempt int, res attemptOutcome) {
	if d := time.Until(waitUntil); d > 0 {
		timer := time.NewTimer(d)
		<-timer.C
	}
	if res.received {
		return
	}
	r := <-res.done
	ws.markExited(stepIdx, attempt, r.exitedAt)
	ws.markLate(stepIdx, attempt, r.err)
}

// watchLateExit 在工作流已终态后，继续跟踪尚未退出的执行器，补记退出与迟到结果。
// attempt 为 1 基尝试号。
func (ws *workflowState) watchLateExit(stepIdx, attempt int, res attemptOutcome) {
	if res.received {
		return
	}
	go func() {
		r := <-res.done
		ws.markExited(stepIdx, attempt, r.exitedAt)
		ws.markLate(stepIdx, attempt, r.err)
	}()
}

// fail 将工作流置为失败终态。
func (ws *workflowState) fail() {
	ws.mu.Lock()
	ws.status = StatusFailed
	ws.mu.Unlock()
	close(ws.done)
}

// markExited 记录某次尝试的执行器已退出。
func (ws *workflowState) markExited(stepIdx, attempt int, exitedAt time.Time) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	rec := &ws.steps[stepIdx].Attempts[attempt-1]
	if rec.ExecutorExited {
		return
	}
	rec.ExecutorExited = true
	rec.ExitedAt = exitedAt
	ws.executorActive = false
}

// markLate 在超时结算后补记迟到结果，不改变已确定的结算。
func (ws *workflowState) markLate(stepIdx, attempt int, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	rec := &ws.steps[stepIdx].Attempts[attempt-1]
	if rec.Result != AttemptTimeout || rec.LateResult != LateNone {
		return
	}
	if err == nil {
		rec.LateResult = LateSuccess
	} else {
		rec.LateResult = LateFailure
	}
}
