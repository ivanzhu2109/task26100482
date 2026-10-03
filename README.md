# task26100482

进程内工作流执行库（Go），面向设备维护的有序任务链场景。全部状态保存在进程内存，
由上层直接调用，提供「提交工作流」与「查询执行情况」两类能力。

## 能力

- 有序步骤：前一步成功后才开始下一步；每步独立执行时限（默认 120 秒）。
- 执行器为调用方提供的进程内函数 `func(ctx context.Context) error`，
  超时通过 ctx 取消请求停止，但执行器可以不响应、任意晚返回。
- 超时从实际调用执行器时起算；严格早于截止时刻返回的结果有效，
  截止时刻或之后返回的一律记为迟到结果，按超时结算。
- 每步最多 3 次尝试；第 1、2 次失败后最短等待 1 秒、2 秒（从判失败时起算），
  且必须等上一次执行器调用真正退出才重试——同一工作流不会有两个未退出的调用。
- 每次尝试只结算一次；迟到的成功/失败只记录在 `LateResult`，不翻转已确定的结果。
- 全部步骤成功则工作流成功；某步第 3 次失败/超时则工作流立即失败，终态不可变。
  即使执行器尚未退出，也能查询到业务终态（`State`）与 `ExecutorActive`。

## 使用

```go
eng := workflow.NewEngine()
id := eng.Submit(workflow.Workflow{
    Name: "device-42-maintenance",
    Steps: []workflow.Step{
        {Name: "停机检查", Timeout: 2 * time.Second, Exec: func(ctx context.Context) error {
            return nil
        }},
    },
})
st, ok := eng.Status(id) // st.State / st.ExecutorActive / st.Steps[].Attempts[]
```

## 运行

```sh
go run ./examples/basic   # 调用示例
go test ./...             # 自动化测试
```
