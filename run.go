package workflow

import (
	"context"
	"sync"
	"time"
)

// attemptResult 是执行器一次返回的结果，at 为执行器函数真正返回的时刻。
type attemptResult struct {
	err error
	at  time.Time
}

// run 是单个工作流的运行时状态。除快照读取外，所有状态变更都发生在
// loop  goroutine 或 waitExit goroutine 内，统一由 mu 保护。
type run struct {
	mu         sync.Mutex
	id         string
	name       string
	steps      []Step
	retryWaits [MaxAttempts - 1]time.Duration

	state          WorkflowState
	currentStep    int
	stepStatuses   []StepStatus
	executorActive bool
	exitCh         chan struct{} // 当前执行器退出时关闭
}

func newRun(id string, wf Workflow, retryWaits [MaxAttempts - 1]time.Duration) *run {
	r := &run{
		id:           id,
		name:         wf.Name,
		steps:        wf.Steps,
		retryWaits:   retryWaits,
		state:        WorkflowRunning,
		currentStep:  -1,
		stepStatuses: make([]StepStatus, len(wf.Steps)),
	}
	for i, s := range wf.Steps {
		r.stepStatuses[i].Name = s.Name
		r.stepStatuses[i].State = StepPending
	}
	return r
}

// loop 驱动工作流按序执行各步骤。
func (r *run) loop() {
	for i, step := range r.steps {
		timeout := step.Timeout
		if timeout <= 0 {
			timeout = DefaultStepTimeout
		}
		r.mu.Lock()
		r.currentStep = i
		r.stepStatuses[i].State = StepRunning
		r.mu.Unlock()

		if !r.runStep(i, step.Exec, timeout) {
			r.mu.Lock()
			r.stepStatuses[i].State = StepFailed
			r.state = WorkflowFailed
			r.currentStep = -1
			r.mu.Unlock()
			return
		}
		r.mu.Lock()
		r.stepStatuses[i].State = StepSucceeded
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.state = WorkflowSucceeded
	r.currentStep = -1
	r.mu.Unlock()
}

// runStep 执行单个步骤，返回该步最终是否成功。
func (r *run) runStep(stepIdx int, exec Executor, timeout time.Duration) bool {
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		if attempt > 1 {
			r.waitRetry(attempt)
		}

		resCh := make(chan attemptResult, 1)
		r.mu.Lock()
		r.executorActive = true
		r.exitCh = make(chan struct{})
		exitCh := r.exitCh
		now := time.Now()
		r.stepStatuses[stepIdx].Attempts = append(r.stepStatuses[stepIdx].Attempts, Attempt{
			Number:    attempt,
			State:     AttemptRunning,
			StartedAt: now,
			Deadline:  now.Add(timeout),
		})
		deadline := r.stepStatuses[stepIdx].Attempts[attempt-1].Deadline
		r.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			resCh <- attemptResult{err: exec(ctx), at: time.Now()}
		}()

		timer := time.NewTimer(timeout)
		settled := false
		for !settled {
			select {
			case res := <-resCh:
				if res.at.Before(deadline) {
					// 严格早于截止时刻：结果有效。
					r.settle(stepIdx, attempt, res.err, res.at)
				} else {
					// 截止时刻或之后返回：只能作为迟到结果，按超时结算。
					cancel()
					r.settleTimeout(stepIdx, attempt, time.Now())
					r.recordLateResult(stepIdx, attempt, res)
				}
				settled = true
			case <-timer.C:
				// 到达时限：请求执行器停止，但执行器允许不响应。
				cancel()
				// 结果可能恰好在截止前已送入通道但尚未被接收，优先按有效结果结算。
				select {
				case res := <-resCh:
					if res.at.Before(deadline) {
						r.settle(stepIdx, attempt, res.err, res.at)
					} else {
						r.settleTimeout(stepIdx, attempt, time.Now())
						r.recordLateResult(stepIdx, attempt, res)
					}
				default:
					r.settleTimeout(stepIdx, attempt, time.Now())
				}
				settled = true
			}
		}
		timer.Stop()

		r.mu.Lock()
		exited := !r.executorActive
		r.mu.Unlock()
		if !exited {
			// 执行器尚未退出：后台等待其返回，只记录退出与迟到结果。
			go r.waitExit(stepIdx, attempt, resCh, exitCh)
		} else {
			close(exitCh)
		}
		cancel()

		r.mu.Lock()
		state := r.stepStatuses[stepIdx].Attempts[attempt-1].State
		r.mu.Unlock()
		if state == AttemptSucceeded {
			return true
		}
		if attempt == MaxAttempts {
			return false
		}
	}
	return false
}

// settle 按执行器有效返回结算一次尝试。
func (r *run) settle(stepIdx, attempt int, err error, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := &r.stepStatuses[stepIdx].Attempts[attempt-1]
	if err == nil {
		a.State = AttemptSucceeded
	} else {
		a.State = AttemptFailed
		a.Err = err
	}
	a.SettledAt = at
	a.ExecutorExited = true
	r.executorActive = false
}

// settleTimeout 将一次尝试按超时结算。
func (r *run) settleTimeout(stepIdx, attempt int, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := &r.stepStatuses[stepIdx].Attempts[attempt-1]
	a.State = AttemptTimedOut
	a.SettledAt = at
}

// recordLateResult 记录结算后执行器的迟到返回。
func (r *run) recordLateResult(stepIdx, attempt int, res attemptResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := &r.stepStatuses[stepIdx].Attempts[attempt-1]
	a.LateResult = &LateResult{Err: res.err, ReturnedAt: res.at}
	a.ExecutorExited = true
	r.executorActive = false
}

// waitExit 等待已超时结算的执行器真正退出。
func (r *run) waitExit(stepIdx, attempt int, resCh <-chan attemptResult, exitCh chan struct{}) {
	res := <-resCh
	r.recordLateResult(stepIdx, attempt, res)
	close(exitCh)
}

// waitRetry 阻塞到重试条件满足：等待已结束且上一次执行器调用已退出。
func (r *run) waitRetry(attempt int) {
	r.mu.Lock()
	attempts := r.stepStatuses[r.currentStep].Attempts
	last := attempts[len(attempts)-1]
	earliest := last.SettledAt.Add(r.retryWaits[attempt-2])
	active := r.executorActive
	exitCh := r.exitCh
	r.mu.Unlock()

	if d := time.Until(earliest); d > 0 {
		time.Sleep(d)
	}
	if active {
		<-exitCh
	}
}

// snapshot 导出当前状态快照。
func (r *run) snapshot() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Status{
		ID:             r.id,
		Name:           r.name,
		State:          r.state,
		ExecutorActive: r.executorActive,
		CurrentStep:    r.currentStep,
		Steps:          make([]StepStatus, len(r.stepStatuses)),
	}
	for i, ss := range r.stepStatuses {
		s.Steps[i].Name = ss.Name
		s.Steps[i].State = ss.State
		s.Steps[i].Attempts = append([]Attempt(nil), ss.Attempts...)
	}
	return s
}
