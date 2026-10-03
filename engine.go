package workflow

import (
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultStepTimeout 是步骤单次尝试的默认执行时限。
	DefaultStepTimeout = 120 * time.Second
	// MaxAttempts 是每步的最大尝试次数。
	MaxAttempts = 3
)

// Engine 是工作流执行引擎，全部状态保存在进程内存。
type Engine struct {
	mu        sync.Mutex
	workflows map[string]*run
	seq       int

	retryWaits [MaxAttempts - 1]time.Duration // 第 1、2 次失败后的最短等待
}

// Option 用于覆盖引擎默认参数（主要用于测试）。
type Option func(*Engine)

// WithRetryWaits 覆盖第 1、2 次失败后的最短重试等待。
func WithRetryWaits(first, second time.Duration) Option {
	return func(e *Engine) {
		e.retryWaits[0] = first
		e.retryWaits[1] = second
	}
}

// NewEngine 创建引擎。
func NewEngine(opts ...Option) *Engine {
	e := &Engine{
		workflows:  make(map[string]*run),
		retryWaits: [MaxAttempts - 1]time.Duration{time.Second, 2 * time.Second},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Submit 提交工作流并异步开始执行，返回工作流 ID。
func (e *Engine) Submit(wf Workflow) string {
	e.mu.Lock()
	e.seq++
	id := fmt.Sprintf("wf-%d", e.seq)
	r := newRun(id, wf, e.retryWaits)
	e.workflows[id] = r
	e.mu.Unlock()
	go r.loop()
	return id
}

// Status 查询工作流执行情况，可区分业务终态与执行器是否已退出。
func (e *Engine) Status(id string) (Status, bool) {
	e.mu.Lock()
	r, ok := e.workflows[id]
	e.mu.Unlock()
	if !ok {
		return Status{}, false
	}
	return r.snapshot(), true
}
