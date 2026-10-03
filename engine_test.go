package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// 截止边界与迟到成功：超时后执行器不退出，重试不得造成调用重叠；
// 迟到成功不得翻转已结算的超时结果。
func TestLateSuccessAndNoOverlap(t *testing.T) {
	const stepTimeout = 50 * time.Millisecond
	eng := NewEngine(WithRetryWaits(40*time.Millisecond, 80*time.Millisecond))

	release := [MaxAttempts]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	started := make(chan int, 10)
	var calls, active, maxActive int32

	exec := func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		cur := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxActive)
			if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
				break
			}
		}
		started <- int(n)
		<-release[n-1] // 忽略 ctx，模拟不响应停止请求的执行器
		atomic.AddInt32(&active, -1)
		return nil // 迟到成功
	}

	id := eng.Submit(Workflow{
		Name:  "late-success",
		Steps: []Step{{Name: "op", Timeout: stepTimeout, Exec: exec}},
	})

	statusOf := func() Status {
		st, ok := eng.Status(id)
		if !ok {
			t.Fatal("workflow not found")
		}
		return st
	}

	// 第 1 次尝试启动并超时。
	<-started
	waitFor(t, "attempt 1 timed out", func() bool {
		st := statusOf()
		return len(st.Steps[0].Attempts) == 1 && st.Steps[0].Attempts[0].State == AttemptTimedOut
	})

	// 重试等待（40ms）已过，但执行器未退出：不得发起第 2 次调用。
	time.Sleep(150 * time.Millisecond)
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("executor called %d times while previous call still active", n)
	}
	if st := statusOf(); !st.ExecutorActive {
		t.Fatal("expected ExecutorActive=true while executor has not exited")
	}

	// 放行第 1 次（迟到成功），随后应开始第 2 次尝试。
	close(release[0])
	<-started
	waitFor(t, "attempt 2 timed out", func() bool {
		st := statusOf()
		return len(st.Steps[0].Attempts) == 2 && st.Steps[0].Attempts[1].State == AttemptTimedOut
	})

	// 第 1 次尝试：迟到成功已记录，但结算结果保持超时，不得翻转。
	st := statusOf()
	a1 := st.Steps[0].Attempts[0]
	if a1.State != AttemptTimedOut {
		t.Fatalf("attempt 1 state flipped to %s", a1.State)
	}
	if a1.LateResult == nil || a1.LateResult.Err != nil {
		t.Fatalf("attempt 1 late result = %+v, want success", a1.LateResult)
	}
	if !a1.ExecutorExited {
		t.Fatal("attempt 1 executor should have exited")
	}

	// 第 2、3 次尝试同样超时。
	close(release[1])
	<-started
	waitFor(t, "workflow failed", func() bool {
		return statusOf().State == WorkflowFailed
	})

	// 第 3 次执行器仍未退出：业务已失败与执行器未退出可同时查询到。
	st = statusOf()
	if st.State != WorkflowFailed {
		t.Fatalf("state = %s, want Failed", st.State)
	}
	if !st.ExecutorActive {
		t.Fatal("expected ExecutorActive=true with last executor still running")
	}

	// 放行第 3 次：迟到成功不得改变终态。
	close(release[2])
	waitFor(t, "executor exited", func() bool {
		return !statusOf().ExecutorActive
	})
	st = statusOf()
	if st.State != WorkflowFailed {
		t.Fatalf("terminal state flipped to %s", st.State)
	}
	if len(st.Steps[0].Attempts) != MaxAttempts {
		t.Fatalf("attempts = %d, want %d", len(st.Steps[0].Attempts), MaxAttempts)
	}
	for i, a := range st.Steps[0].Attempts {
		if a.State != AttemptTimedOut {
			t.Fatalf("attempt %d state = %s, want TimedOut", i+1, a.State)
		}
		if a.LateResult == nil {
			t.Fatalf("attempt %d missing late result", i+1)
		}
	}
	if n := atomic.LoadInt32(&calls); n != MaxAttempts {
		t.Fatalf("executor calls = %d, want %d", n, MaxAttempts)
	}
	if m := atomic.LoadInt32(&maxActive); m != 1 {
		t.Fatalf("max concurrent executor calls = %d, want 1", m)
	}
}

// 截止前返回的结果有效：严格早于截止时刻返回成功，一次通过。
func TestResultBeforeDeadlineIsValid(t *testing.T) {
	eng := NewEngine(WithRetryWaits(20*time.Millisecond, 40*time.Millisecond))
	id := eng.Submit(Workflow{
		Name: "before-deadline",
		Steps: []Step{{
			Name:    "op",
			Timeout: 200 * time.Millisecond,
			Exec: func(ctx context.Context) error {
				time.Sleep(50 * time.Millisecond)
				return nil
			},
		}},
	})
	waitFor(t, "workflow succeeded", func() bool {
		st, _ := eng.Status(id)
		return st.State == WorkflowSucceeded
	})
	st, _ := eng.Status(id)
	a := st.Steps[0].Attempts[0]
	if a.State != AttemptSucceeded || a.LateResult != nil {
		t.Fatalf("attempt = %+v, want clean success", a)
	}
	if !a.SettledAt.Before(a.Deadline) {
		t.Fatal("settled after deadline")
	}
}

// 多个工作流混合：成功、失败后重试成功、三次失败终败，检查尝试记录与最终状态。
func TestMixedWorkflows(t *testing.T) {
	const (
		firstWait  = 60 * time.Millisecond
		secondWait = 120 * time.Millisecond
	)
	eng := NewEngine(WithRetryWaits(firstWait, secondWait))

	// wf-A：两步直接成功。
	idA := eng.Submit(Workflow{
		Name: "all-success",
		Steps: []Step{
			{Name: "s1", Timeout: time.Second, Exec: func(ctx context.Context) error { return nil }},
			{Name: "s2", Timeout: time.Second, Exec: func(ctx context.Context) error { return nil }},
		},
	})

	// wf-B：第 1、2 次失败，第 3 次成功。
	var callsB int32
	idB := eng.Submit(Workflow{
		Name: "retry-then-success",
		Steps: []Step{{
			Name:    "flaky",
			Timeout: time.Second,
			Exec: func(ctx context.Context) error {
				if atomic.AddInt32(&callsB, 1) < 3 {
					return errors.New("transient")
				}
				return nil
			},
		}},
	})

	// wf-C：第一步三次失败，第二步永远不应执行。
	var callsC2 int32
	idC := eng.Submit(Workflow{
		Name: "always-fail",
		Steps: []Step{
			{Name: "bad", Timeout: time.Second, Exec: func(ctx context.Context) error {
				return errors.New("boom")
			}},
			{Name: "never", Timeout: time.Second, Exec: func(ctx context.Context) error {
				atomic.AddInt32(&callsC2, 1)
				return nil
			}},
		},
	})

	waitFor(t, "wf-A succeeded", func() bool {
		st, _ := eng.Status(idA)
		return st.State == WorkflowSucceeded
	})
	waitFor(t, "wf-B succeeded", func() bool {
		st, _ := eng.Status(idB)
		return st.State == WorkflowSucceeded
	})
	waitFor(t, "wf-C failed", func() bool {
		st, _ := eng.Status(idC)
		return st.State == WorkflowFailed
	})

	// wf-A：每步恰好 1 次成功尝试。
	stA, _ := eng.Status(idA)
	for i, s := range stA.Steps {
		if s.State != StepSucceeded || len(s.Attempts) != 1 || s.Attempts[0].State != AttemptSucceeded {
			t.Fatalf("wf-A step %d = %+v", i, s)
		}
	}

	// wf-B：3 次尝试，失败、失败、成功；重试间隔满足最短等待。
	stB, _ := eng.Status(idB)
	atts := stB.Steps[0].Attempts
	if len(atts) != 3 {
		t.Fatalf("wf-B attempts = %d, want 3", len(atts))
	}
	if atts[0].State != AttemptFailed || atts[1].State != AttemptFailed || atts[2].State != AttemptSucceeded {
		t.Fatalf("wf-B attempt states = %s,%s,%s", atts[0].State, atts[1].State, atts[2].State)
	}
	if atts[1].StartedAt.Before(atts[0].SettledAt.Add(firstWait)) {
		t.Fatalf("retry 2 started too early: %v < %v", atts[1].StartedAt, atts[0].SettledAt.Add(firstWait))
	}
	if atts[2].StartedAt.Before(atts[1].SettledAt.Add(secondWait)) {
		t.Fatalf("retry 3 started too early: %v < %v", atts[2].StartedAt, atts[1].SettledAt.Add(secondWait))
	}

	// wf-C：第一步 3 次失败，第二步未执行，终态失败。
	stC, _ := eng.Status(idC)
	if len(stC.Steps[0].Attempts) != 3 {
		t.Fatalf("wf-C attempts = %d, want 3", len(stC.Steps[0].Attempts))
	}
	for _, a := range stC.Steps[0].Attempts {
		if a.State != AttemptFailed {
			t.Fatalf("wf-C attempt state = %s, want Failed", a.State)
		}
	}
	if stC.Steps[1].State != StepPending || len(stC.Steps[1].Attempts) != 0 {
		t.Fatalf("wf-C step 2 = %+v, want untouched", stC.Steps[1])
	}
	if n := atomic.LoadInt32(&callsC2); n != 0 {
		t.Fatalf("wf-C step 2 executed %d times", n)
	}
	if stC.ExecutorActive {
		t.Fatal("wf-C executor should have exited")
	}
}

// 查询不存在的工作流。
func TestStatusNotFound(t *testing.T) {
	eng := NewEngine()
	if _, ok := eng.Status("nope"); ok {
		t.Fatal("expected not found")
	}
}
