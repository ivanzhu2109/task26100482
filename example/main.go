// 调用示例：运维人员用有序任务链执行设备维护。
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"workflow"
)

func main() {
	eng := workflow.New()

	// 步骤 1：升级固件。第 1 次失败，重试后成功。
	// 步骤 2：校验版本。执行器超时后仍不退出，很晚才返回成功（迟到结果）。
	var firmwareCalls, verifyCalls int
	steps := []workflow.StepSpec{
		{
			Name:    "升级固件",
			Timeout: 500 * time.Millisecond,
			Exec: func(ctx context.Context, call workflow.CallInfo) error {
				firmwareCalls++
				fmt.Printf("[%s] 步骤%d %q 第%d次尝试\n", call.WorkflowID, call.StepIndex, call.StepName, call.Attempt)
				if firmwareCalls == 1 {
					return errors.New("设备忙")
				}
				return nil
			},
		},
		{
			Name:    "校验版本",
			Timeout: 300 * time.Millisecond,
			Exec: func(ctx context.Context, call workflow.CallInfo) error {
				verifyCalls++
				fmt.Printf("[%s] 步骤%d %q 第%d次尝试\n", call.WorkflowID, call.StepIndex, call.StepName, call.Attempt)
				if verifyCalls == 1 {
					// 不响应停止请求，超时时限之后很久才返回成功。
					time.Sleep(800 * time.Millisecond)
					return nil
				}
				return nil
			},
		},
	}

	id, err := eng.Submit(steps)
	if err != nil {
		panic(err)
	}
	fmt.Println("已提交工作流:", id)

	// 轮询查询执行情况，直到业务终态。
	for {
		rep, err := eng.Query(id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("状态=%s 执行器未退出=%v\n", rep.Status, rep.ExecutorActive)
		for _, s := range rep.Steps {
			for _, a := range s.Attempts {
				fmt.Printf("  步骤%q 尝试%d: 结果=%s 迟到=%q 执行器已退出=%v\n",
					s.Name, a.Attempt, a.Result, a.LateResult, a.ExecutorExited)
			}
		}
		if rep.Status != workflow.StatusRunning {
			break
		}
		time.Sleep(700 * time.Millisecond)
	}
}
