package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 缩短引擎的重试等待，加速测试。
func fastBackoff(t *testing.T, eng *Engine, first, second time.Duration) {
	t.Helper()
	eng.backoff = [MaxAttempts - 1]time.Duration{first, second}
}

// waitFor 轮询直到条件满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func mustSubmit(t *testing.T, eng *Engine, steps ...StepSpec) string {
	t.Helper()
	id, err := eng.Submit(steps)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return id
}

func query(t *testing.T, eng *Engine, id string) Report {
	t.Helper()
	rep, err := eng.Query(id)
	if err != nil {
		t.Fatalf("Query(%s): %v", id, err)
	}
	return rep
}

// TestLateSuccessDoesNotFlip 覆盖截止边界与迟到成功：
// 超时结算不被迟到成功翻转，且同一工作流不存在调用重叠。
func TestLateSuccessDoesNotFlip(t *testing.T) {
	eng := New()
	fastBackoff(t, eng, 40*time.Millisecond, 80*time.Millisecond)

	var inflight atomic.Int32
	var maxInflight atomic.Int32
	enter := func() {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
	}
	leave := func() { inflight.Add(-1) }

	var calls atomic.Int32
	exec := func(ctx context.Context, call CallInfo) error {
		enter()
		defer leave()
		n := calls.Add(1)
		if n == 1 {
			// 不响应停止请求，超时时限之后很久才返回成功。
			time.Sleep(250 * time.Millisecond)
			return nil
		}
		return nil
	}

	id := mustSubmit(t, eng, StepSpec{Name: "s1", Timeout: 100 * time.Millisecond, Exec: exec})
	waitFor(t, "工作流终态", func() bool {
		return query(t, eng, id).Status == StatusSucceeded
	})
	// 等迟到返回被记录。
	waitFor(t, "迟到结果记录", func() bool {
		rep := query(t, eng, id)
		return rep.Steps[0].Attempts[0].LateResult != LateNone
	})

	rep := query(t, eng, id)
	atts := rep.Steps[0].Attempts
	if len(atts) != 2 {
		t.Fatalf("尝试次数 = %d, 期望 2", len(atts))
	}
	if atts[0].Result != AttemptTimeout {
		t.Errorf("第1次结果 = %s, 期望 Timeout（不被迟到成功翻转）", atts[0].Result)
	}
	if atts[0].LateResult != LateSuccess {
		t.Errorf("第1次迟到结果 = %q, 期望 LateSuccess", atts[0].LateResult)
	}
	if atts[1].Result != AttemptSuccess {
		t.Errorf("第2次结果 = %s, 期望 Success", atts[1].Result)
	}
	if got := maxInflight.Load(); got != 1 {
		t.Errorf("最大并发执行器调用 = %d, 期望 1（同工作流无调用重叠）", got)
	}
	// 重试必须在上一次执行器退出之后发起。
	if atts[1].StartedAt.Before(atts[0].ExitedAt) {
		t.Errorf("第2次开始 %v 早于第1次执行器退出 %v", atts[1].StartedAt, atts[0].ExitedAt)
	}
	// 重试必须满足最短等待（从判失败时起算）。
	if atts[1].StartedAt.Before(atts[0].SettledAt.Add(40 * time.Millisecond)) {
		t.Errorf("第2次开始早于最短等待结束: settled=%v started=%v", atts[0].SettledAt, atts[1].StartedAt)
	}
	// 超时从实际调用时起算。
	if d := atts[0].Deadline.Sub(atts[0].StartedAt); d != 100*time.Millisecond {
		t.Errorf("时限 = %v, 期望 100ms", d)
	}
}

// TestDeadlineBoundary 验证严格早于截止时刻返回有效，截止时刻或之后返回为迟到。
func TestDeadlineBoundary(t *testing.T) {
	eng := New()
	fastBackoff(t, eng, 20*time.Millisecond, 40*time.Millisecond)

	// 步骤1：截止前 返回 → 有效成功。
	early := StepSpec{
		Name:    "early",
		Timeout: 200 * time.Millisecond,
		Exec: func(ctx context.Context, call CallInfo) error {
			time.Sleep(20 * time.Millisecond)
			return nil
		},
	}
	// 步骤2：第1次等 ctx 到期通知后才返回 → 退出时刻不早于截止时刻 → 迟到；
	// 后续尝试立即成功。
	var lateCalls atomic.Int32
	late := StepSpec{
		Name:    "late",
		Timeout: 100 * time.Millisecond,
		Exec: func(ctx context.Context, call CallInfo) error {
			if lateCalls.Add(1) == 1 {
				<-ctx.Done()
			}
			return nil
		},
	}
	id := mustSubmit(t, eng, early, late)
	waitFor(t, "工作流终态", func() bool {
		return query(t, eng, id).Status == StatusSucceeded
	})
	waitFor(t, "迟到结果记录", func() bool {
		return query(t, eng, id).Steps[1].Attempts[0].LateResult != LateNone
	})

	rep := query(t, eng, id)
	if got := rep.Steps[0].Attempts[0].Result; got != AttemptSuccess {
		t.Errorf("截止前返回: 结果 = %s, 期望 Success", got)
	}
	a := rep.Steps[1].Attempts[0]
	if a.Result != AttemptTimeout || a.LateResult != LateSuccess {
		t.Errorf("截止时刻后返回: 结果 = %s 迟到 = %q, 期望 Timeout+LateSuccess", a.Result, a.LateResult)
	}
	if rep.Steps[1].Attempts[1].Result != AttemptSuccess {
		t.Errorf("重试应成功, 实际 %s", rep.Steps[1].Attempts[1].Result)
	}
}

// TestMixedWorkflows 覆盖多工作流混合：成功、失败、重试等待与尝试记录。
func TestMixedWorkflows(t *testing.T) {
	eng := New()
	fastBackoff(t, eng, 50*time.Millisecond, 100*time.Millisecond)

	// wfOK：两步均一次成功。
	okID := mustSubmit(t, eng,
		StepSpec{Name: "a", Timeout: time.Second, Exec: func(context.Context, CallInfo) error { return nil }},
		StepSpec{Name: "b", Timeout: time.Second, Exec: func(context.Context, CallInfo) error { return nil }},
	)

	// wfFail：第一步连续业务失败 3 次 → 工作流失败，第二步不应执行。
	var failStep2 atomic.Bool
	failID := mustSubmit(t, eng,
		StepSpec{
			Name:    "flaky",
			Timeout: time.Second,
			Exec:    func(context.Context, CallInfo) error { return errors.New("bad") },
		},
		StepSpec{
			Name:    "never",
			Timeout: time.Second,
			Exec:    func(context.Context, CallInfo) error { failStep2.Store(true); return nil },
		},
	)

	// wfStuck：执行器不响应停止，3 次全部超时；
	// 前两次迟到退出使重试得以发起，最后一次迟迟不退出。
	stuckRelease := make(chan struct{})
	var stuckCalls atomic.Int32
	stuckID := mustSubmit(t, eng, StepSpec{
		Name:    "stuck",
		Timeout: 60 * time.Millisecond,
		Exec: func(ctx context.Context, call CallInfo) error {
			if stuckCalls.Add(1) < 3 {
				time.Sleep(150 * time.Millisecond) // 超时后才退出
				return errors.New("late failure")
			}
			<-stuckRelease // 永不响应 ctx，直到测试放行
			return errors.New("late failure")
		},
	})

	waitFor(t, "wfOK 成功", func() bool { return query(t, eng, okID).Status == StatusSucceeded })
	waitFor(t, "wfFail 失败", func() bool { return query(t, eng, failID).Status == StatusFailed })
	// wfStuck 第3次超时后应立即失败，即使执行器尚未退出。
	waitFor(t, "wfStuck 失败", func() bool { return query(t, eng, stuckID).Status == StatusFailed })

	// wfOK：每步恰好 1 次成功尝试。
	repOK := query(t, eng, okID)
	if repOK.ExecutorActive {
		t.Error("wfOK 不应有未退出执行器")
	}
	for i, s := range repOK.Steps {
		if len(s.Attempts) != 1 || s.Attempts[0].Result != AttemptSuccess {
			t.Errorf("wfOK 步骤%d 尝试记录不符: %+v", i, s.Attempts)
		}
	}

	// wfFail：3 次 Failure，间隔满足最短等待；第二步未执行。
	repFail := query(t, eng, failID)
	atts := repFail.Steps[0].Attempts
	if len(atts) != 3 {
		t.Fatalf("wfFail 尝试次数 = %d, 期望 3", len(atts))
	}
	for i, a := range atts {
		if a.Result != AttemptFailure {
			t.Errorf("wfFail 尝试%d 结果 = %s, 期望 Failure", i+1, a.Result)
		}
		if !a.ExecutorExited {
			t.Errorf("wfFail 尝试%d 执行器应已退出", i+1)
		}
	}
	backoffs := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}
	for i := 0; i < 2; i++ {
		if atts[i+1].StartedAt.Before(atts[i].SettledAt.Add(backoffs[i])) {
			t.Errorf("第%d次重试早于最短等待: settled=%v started=%v",
				i+2, atts[i].SettledAt, atts[i+1].StartedAt)
		}
	}
	if len(repFail.Steps[1].Attempts) != 0 {
		t.Error("wfFail 后续步骤不应执行")
	}
	if failStep2.Load() {
		t.Error("wfFail 第二步执行器不应被调用")
	}

	// wfStuck：业务已失败但执行器尚未退出，查询必须能区分。
	repStuck := query(t, eng, stuckID)
	if repStuck.Status != StatusFailed {
		t.Fatalf("wfStuck 状态 = %s, 期望 Failed", repStuck.Status)
	}
	if !repStuck.ExecutorActive {
		t.Error("wfStuck 执行器尚未退出, ExecutorActive 应为 true")
	}
	satts := repStuck.Steps[0].Attempts
	if len(satts) != 3 {
		t.Fatalf("wfStuck 尝试次数 = %d, 期望 3", len(satts))
	}
	for i, a := range satts {
		if a.Result != AttemptTimeout {
			t.Errorf("wfStuck 尝试%d 结果 = %s, 期望 Timeout", i+1, a.Result)
		}
	}
	if got := stuckCalls.Load(); got != 3 {
		t.Errorf("wfStuck 执行器调用次数 = %d, 期望 3（迟到返回未额外消耗尝试）", got)
	}

	// 放行执行器：迟到失败被记录，终态不翻转。
	close(stuckRelease)
	waitFor(t, "wfStuck 迟到结果记录", func() bool {
		rep := query(t, eng, stuckID)
		return rep.Steps[0].Attempts[2].LateResult != LateNone
	})
	repStuck = query(t, eng, stuckID)
	if repStuck.Status != StatusFailed {
		t.Errorf("wfStuck 终态被翻转: %s", repStuck.Status)
	}
	if repStuck.ExecutorActive {
		t.Error("wfStuck 执行器已退出, ExecutorActive 应为 false")
	}
	for i, a := range repStuck.Steps[0].Attempts {
		if a.LateResult != LateFailure {
			t.Errorf("wfStuck 尝试%d 迟到结果 = %q, 期望 LateFailure", i+1, a.LateResult)
		}
		if !a.ExecutorExited {
			t.Errorf("wfStuck 尝试%d 应已退出", i+1)
		}
	}
}

// TestQueryUnknown 与提交校验。
func TestQueryUnknownAndInvalidSpec(t *testing.T) {
	eng := New()
	if _, err := eng.Query("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Query 未知ID: err = %v, 期望 ErrNotFound", err)
	}
	if _, err := eng.Submit(nil); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("空步骤: err = %v, 期望 ErrInvalidSpec", err)
	}
	if _, err := eng.Submit([]StepSpec{{Name: "x"}}); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("缺少执行器: err = %v, 期望 ErrInvalidSpec", err)
	}
}

// TestConcurrentSubmit 并发提交多个工作流，各自独立推进。
func TestConcurrentSubmit(t *testing.T) {
	eng := New()
	fastBackoff(t, eng, 20*time.Millisecond, 40*time.Millisecond)
	const n = 8
	ids := make([]string, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := eng.Submit([]StepSpec{{
				Name:    "s",
				Timeout: time.Second,
				Exec:    func(context.Context, CallInfo) error { return nil },
			}})
			if err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(ids) != n {
		t.Fatalf("提交成功数 = %d, 期望 %d", len(ids), n)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("重复ID: %s", id)
		}
		seen[id] = true
		waitFor(t, "终态 "+id, func() bool {
			return query(t, eng, id).Status == StatusSucceeded
		})
	}
}
