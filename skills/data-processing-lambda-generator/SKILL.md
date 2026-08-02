---
name: data-processing-lambda-generator
description: 根据 IF定義書（接口定义书）与 IF変換説明書（接口转换说明书）自动生成数据处理 Lambda。当用户提供 IF 定义/转换文档，并要求为各个 IF 实现、生成或更新数据处理 Lambda（SQS 消费者、字段校验、字段转换、幂等写入、作业台账、失败重试），或要求参照 data-processing-sample 与 data_processing_framework 的模式生成数据处理代码时，使用本 skill。
---

# 数据处理 Lambda 自动生成

## 概述

根据 IF定義書 与 IF変換説明書，为每个 IF 生成遵循本仓库 `data-processing-sample` 模式的数据处理 Lambda：SQS 触发、校验先行、字段转换、幂等写入、作业台账、失败重试（ReportBatchItemFailures）。

## 输入与前置条件

- 用户提供：IF定義書、IF変換説明書 的文件路径（Excel/CSV/Markdown/Word），输出目录（未指定时默认 `data-processing/<if-id>/`）。
- 模板项目：`assets/sample-project/`（源自 `data-processing-sample`，生成时复制并定制）。
- 框架层：`data_processing_framework`（本仓库 `data-processing-layer/layer/python`）。本地测试加入 PYTHONPATH；部署时使用 LayerVersionArn。

## 工作流程

1. **收集输入**：确认文档路径、输出目录、输出表/DB。
2. **解析文档**：按 `references/if-docs.md` 为每个 IF 提取输入字段、输出字段、校验规则、转换规则。
3. **逐 IF 生成**：按 `references/generation-workflow.md` 复制 `assets/sample-project/` 并定制：
   - `src/<if-id>_processor.py`：`Processor.validate()` / `Processor.process()`（业务校验与转换独立成文件）
   - `src/app.py`：`lambda_handler` 与管线组装，从处理器模块导入
   - `template.yaml`：SQS 队列 + DLQ + DynamoDB 表 + 函数（ReportBatchItemFailures）
   - `events/<if-id>.json`：示例消息
   - `tests/test_app.py`：由文档规则导出的单元测试
4. **验证**：运行生成的测试（命令见 `references/generation-workflow.md`），修复失败直至全部通过。
5. **总结**：列出每个 IF 的生成结果、输出字段与待确认事项。

## 关键约束

- 只使用 `data_processing_framework` 导出的 API；签名与示例见 `references/framework-api.md`（生成代码前必读）。
- `validate()` 只校验、不产生副作用；`process()` 才执行转换与写入。
- 幂等：框架按 `f"{processor.name}#{event.event_id}"` 去重；业务写入本身也要用 `ConditionExpression="attribute_not_exists(event_id)"`。
- 金额/数值用 `Decimal` 计算；输出统一转为 `str`，保证 JSON 可序列化。
- 日志用 `get_logger` / `log_context`；作业台账用 `DynamoDbJobRunManager.start/complete/fail`。
- `lambda_handler` 只重试失败记录，返回 `{"batchItemFailures": [...]}`。
- 环境变量：输出表、幂等表、作业表（命名参照 sample 的 template.yaml）。

## 资源导航

- `references/framework-api.md` — 框架 API 签名、示例与注意事项（生成前必读）
- `references/if-docs.md` — IF 文档常见格式与解析规则（收到文档后必读）
- `references/generation-workflow.md` — 逐 IF 生成步骤、命名、转换逻辑、测试命令（生成时必读）
- `references/example-if-docs.md` — 示例文档与期望产物对照
- `assets/sample-project/` — 可复制的模板项目（processor、app.py、template.yaml、events、tests）
