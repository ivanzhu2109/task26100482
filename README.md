# workflow

进程内工作流执行库：有序步骤、每步独立超时、失败重试、执行器退出跟踪，全部状态保存在内存。

## 能力

- `Submit([]StepSpec) (id, error)`：提交工作流，后台异步执行。
- `Query(id) (Report, error)`：查询执行情况快照，可区分“业务已终态”与“执行器尚未退出”（`Report.ExecutorActive`），并查看每次尝试的结果（含超时后的迟到结果）。

## 规则

- 步骤有序，前一步成功才执行下一步；每步独立时限（默认 120s），从实际调用执行器时起算。
- 严格早于截止时刻返回的结果有效；截止时刻或之后返回一律视为迟到结果，不改变已结算结果。
- 到达时限即通过 `ctx` 请求执行器停止；执行器允许不响应并在任意晚的时刻返回。
- 每步最多 3 次尝试，失败与超时均消耗一次；第 1、2 次失败后最短等待 1s、2s（`RetryBackoff`）。
- 重试需同时满足：等待已结束，且上一次执行器调用已退出；同一工作流不会有两个未退出的调用。
- 全部步骤成功则工作流成功；某步第 3 次失败/超时则工作流立即失败，终态不可改变。

## 使用

```go
eng := workflow.New()
id, _ := eng.Submit([]workflow.StepSpec{
    {Name: "升级固件", Timeout: 30 * time.Second, Exec: func(ctx context.Context, call workflow.CallInfo) error {
        // call 含 WorkflowID / StepIndex / StepName / Attempt
        return nil
    }},
})
rep, _ := eng.Query(id)
```

完整示例见 `example/main.go`（`go run ./example`）。

## 测试

```sh
go test -race ./...
```
