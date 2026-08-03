# Serverless Data Processor

AWS 上基于 SAM 的无服务器数据处理框架与示例。以「SQS 触发 → 校验先行 → 字段转换 → 幂等写入 → 作业台账」为统一范式，支持按接口（IF）快速开发数据处理 Lambda。

## 仓库结构

| 目录 | 说明 |
|---|---|
| `data-processing-layer` | 可复用 Lambda Layer（Python 3.12）：事件标准化、校验管线、幂等、作业台账、消息解析、JSON 日志、DB 连接池与 `BaseRepository` 仓储基类 |
| `data-processing-sample` | SQS 数据处理示例（POS 交易行），新接口开发时以此为模板 |
| `skills/data-processing-lambda-generator` | 根据 IF定義書 / IF変換説明書 自动生成数据处理 Lambda 的 Codex skill |
| `docs` | 开发文档与整体方案设计 |

## 快速开始

### 本地测试

先安装 Layer 与示例（均为可编辑模式；Sample 依赖 Layer，需先安装）：

```bash
pip install -e ./data-processing-layer
pip install -e ./data-processing-sample
```

```bash
# Layer 测试
PYTHONPATH=data-processing-layer/layer/python python3 -m unittest discover -s data-processing-layer/tests -v

# 示例 Lambda 测试
PYTHONPATH=data-processing-layer/layer/python:data-processing-sample/src python3 -m unittest discover -s data-processing-sample/tests -v
```

两个项目都安装后，上述命令可省略 `PYTHONPATH=` 前缀。

### 部署

```bash
# 1. 部署 Layer，取得 LayerVersionArn
cd data-processing-layer
sam build --use-container
sam deploy --guided

# 2. 部署示例 Lambda
cd ../data-processing-sample
sam build
sam deploy --guided --parameter-overrides DataProcessingLayerArn=<layer-version-arn>
```

## 核心特性

- **标准事件信封**：消息统一为 `event_id` / `source_system` / `payload` 结构，业务载荷保留在 `payload`
- **校验先行**：`validate()` 只校验、不产生副作用，`process()` 才执行转换与写入
- **双重幂等**：框架幂等表（`idempotency_key`）＋ 业务写入自身幂等（示例使用 PostgreSQL `ON CONFLICT DO NOTHING`，也支持 DynamoDB 条件写入）
- **失败重试**：`ReportBatchItemFailures` 只重试失败记录，重试三次后进入 DLQ
- **作业台账**：每个任务登记 `RUNNING → SUCCEEDED / FAILED`，全程可追溯
- **JSON 日志**：日志携带 `event_id` / `trace_id` / `job_id`，便于排查

## 文档

- [数据处理 Lambda 开发文档](docs/data-processing-lambda-dev-guide.md)：开发新 IF 的完整指南（框架 API、代码示例、测试与部署）
- [整体方案设计](docs/solution-design.md)：平台架构与数据流设计

## 使用 skill 自动生成

将 skill 链接到 Codex 用户级 skills 目录后，提供 IF定義書 / IF変換説明書 即可自动为每个 IF 生成数据处理 Lambda：

```bash
ln -s "$(pwd)/skills/data-processing-lambda-generator" ~/.codex/skills/
```

生成的代码与 `data-processing-sample` 保持同一规范，详见开发文档第 8 章。
