# Data Processing Sample

一个 POS 交易行数据的 SQS 消费者示例。它通过 Layer 标准化事件、校验载荷并使用 DynamoDB 条件写入处理幂等；处理开始时写入 `JobRuns` 表的 `RUNNING` 记录，成功后更新为 `SUCCEEDED`，失败时更新为 `FAILED`。SQS 启用了 `ReportBatchItemFailures`，仅重试失败记录，三次失败后进入 DLQ。

## 部署

先部署相邻的 `data-processing-layer` 项目，并获得其 `LayerVersionArn`。随后在本目录执行：

```bash
sam build
sam deploy --guided --parameter-overrides DataProcessingLayerArn=<layer-version-arn>
```

## 本地测试

在仓库根目录执行：

```bash
PYTHONPATH=data-processing-layer/layer/python python3 -m unittest discover -s data-processing-layer/tests -v
PYTHONPATH=data-processing-layer/layer/python:data-processing-sample/src python3 -m unittest discover -s data-processing-sample/tests -v
```

示例输入见 `events/transaction.json`。生产环境应为数据质量错误增加 quarantine S3 路径与错误报告，再由人工或重放任务处置。
