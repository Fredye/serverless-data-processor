# data_processing_framework API 参考

框架位于 `data-processing-layer/layer/python/data_processing_framework`，以下对象均从 `__init__.py` 导出。

## 数据模型（models.py）

### DataEvent

`DataEvent.from_message(message: dict) -> DataEvent`，必需信封字段：

- `event_id`：全局唯一事件 ID（str）
- `source_system`：来源系统（str）
- `payload`：业务载荷（dict）

可选字段：`event_type`（默认 `"data.received"`）、`event_version`（默认 `"1.0"`）、`trace_id`、`occurred_at`、`object_uri`。

### ProcessingContext

`ProcessingContext(job_id: str, trace_id: str, attempt: int = 1)`，另有自动生成的 `started_at`（UTC ISO 字符串）。

### ProcessingResult

`ProcessingResult(event_id, status, output=None, duplicate=False)`。

## 处理器与管线（pipeline.py）

```python
class DataProcessor(Protocol):
    name: str
    def validate(self, event: DataEvent) -> None: ...
    def process(self, event: DataEvent, context: ProcessingContext) -> dict: ...

ProcessingPipeline(processor, idempotency_store).execute(event, context) -> ProcessingResult
```

`execute()` 顺序：先 `processor.validate(event)`，再按 `f"{processor.name}#{event.event_id}"` 做幂等获取；重复消息返回 `ProcessingResult(status="SUCCEEDED", duplicate=True)`，且不会调用 `process()`。

## 幂等（idempotency.py）

`DynamoDbIdempotencyStore(table_name: str, client=None)`，`acquire(key: str) -> bool`（首次返回 True，重复返回 False）。DynamoDB 表需要 HASH 键 `idempotency_key`（S）。

## 作业台账（job_management.py）

`DynamoDbJobRunManager(table_name: str, client=None)`：

- `start(job_id, *, event_id, source_system, trace_id=None) -> bool`：写入 `RUNNING`，重复启动返回 False
- `complete(job_id, *, metrics: dict | None = None)`：更新为 `SUCCEEDED`
- `fail(job_id, *, reason: str)`：更新为 `FAILED`

DynamoDB 表需要 HASH 键 `job_id`（S）。

## 消息解析（message_parser.py）

`MessageParser`：

- `parse_records(event)` -> `list[ParsedMessage]`：SQS `Records` 或直接/EventBridge 事件
- `parse_sqs_record(record)` -> `ParsedMessage`：支持 SNS 包裹的 SQS body
- `parse_direct_event(event)` -> `ParsedMessage`：EventBridge 事件取 `detail`

`ParsedMessage` 字段：`message_id`、`data_event`、`raw_message`。

## 日志（logging.py）

- `get_logger(name)`：JSON 日志输出到 stdout
- `log_context(**fields)`：上下文管理器，为日志附加 `event_id`、`trace_id` 等字段
- `bind_log_context(**fields)`：模块级绑定

## 数据库访问（connection_pool.py / data_access.py）

- `ConnectionPool(factory, max_size=4, timeout_seconds=5)`：DB-API 连接池；`connection()` 上下文管理器
- `DatabaseRepository(pool)`：`transaction()`、`execute(sql, params)`、`fetch_all(sql, params)`

驱动（如 psycopg）不打包在 Layer 中，由业务 Lambda 自行引入；连接池定义为模块级单例以便 warm 环境复用。

## 注意事项

- boto3 由 Lambda 运行时提供；单测中用延迟导入的 `_table()` 避免依赖 AWS SDK。
- DynamoDB 条件写入异常：资源 API 用 `self._table.meta.client.exceptions.ConditionalCheckFailedException`，client API 用 `self._client.exceptions.ConditionalCheckFailedException`。
- `Decimal` 不能直接 JSON 序列化，输出前转为 `str`。
- 不要在 `validate()` 中产生副作用（写库、发消息）。
