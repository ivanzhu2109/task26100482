// 示例：提交一个设备维护工作流并轮询执行情况。
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"workflow"
)

func main() {
	eng := workflow.NewEngine()

	id := eng.Submit(workflow.Workflow{
		Name: "device-42-maintenance",
		Steps: []workflow.Step{
			{
				Name:    "停机检查",
				Timeout: 2 * time.Second,
				Exec: func(ctx context.Context) error {
					time.Sleep(100 * time.Millisecond)
					return nil
				},
			},
			{
				Name:    "固件升级",
				Timeout: 500 * time.Millisecond,
				Exec: func(ctx context.Context) error {
					// 第一次尝试失败，演示重试。
					return errors.New("flash verify failed")
				},
			},
			{
				Name:    "恢复上线",
				Timeout: 2 * time.Second,
				Exec: func(ctx context.Context) error {
					return nil
				},
			},
		},
	})

	for {
		st, _ := eng.Status(id)
		fmt.Printf("workflow=%s state=%s executorActive=%v\n", st.ID, st.State, st.ExecutorActive)
		for _, s := range st.Steps {
			fmt.Printf("  step=%s state=%s\n", s.Name, s.State)
			for _, a := range s.Attempts {
				fmt.Printf("    attempt=%d state=%s exited=%v err=%v\n",
					a.Number, a.State, a.ExecutorExited, a.Err)
			}
		}
		if st.State != workflow.WorkflowRunning {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
