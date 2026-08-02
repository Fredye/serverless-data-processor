# Data Processing Framework Layer

可复用的 Python 3.12 Lambda Layer，提供：

- 标准事件信封 `DataEvent`
- 处理器约定 `DataProcessor`
- DynamoDB 条件写入实现的幂等防护
- 校验优先、执行后输出的处理管线
- `MessageParser`：统一解析 SQS、SNS 包装的 SQS、EventBridge 及直接调用事件
- `get_logger` / `log_context`：输出带 `event_id`、`trace_id` 等关联字段的 JSON 日志
- `ConnectionPool` / `DatabaseRepository`：DB-API 连接池与事务、查询、写入共通访问层
- `DynamoDbJobRunManager`：作业开始时创建 `RUNNING` 运行记录，结束时更新为 `SUCCEEDED` 或 `FAILED`

数据库驱动由业务 Lambda 自行提供（例如 psycopg）；Layer 只提供驱动无关的池与仓储抽象。将连接池定义为 Lambda 模块级单例，以复用 warm environment 中的连接，并优先通过 RDS Proxy 访问 Aurora/RDS。

构建及部署：

```bash
# psycopg binary package must be built for the Lambda Linux runtime.
sam build --use-container
sam deploy --guided
```

构建规则位于 `layer/Makefile`：它会把框架代码复制到产物的 `python/` 目录，并执行 `pip install --requirement requirements.txt --target <artifact>/python`。所有 Layer 运行时依赖及版本固定在 `layer/requirements.txt`；更新依赖时应同步修改该文件并重新构建 Layer 版本。

将输出的 `LayerVersionArn` 传给使用方项目的 `DataProcessingLayerArn` 参数。运行单元测试时，将 `layer/python` 加入 `PYTHONPATH`。
