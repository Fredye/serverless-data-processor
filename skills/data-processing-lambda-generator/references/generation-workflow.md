# 生成工作流（逐 IF）

## 1. 确定 IF 标识与输出目录

- IF 标识：取 IF定義書 的接口 ID/名称，规范化为小写 kebab-case（如 `POS取引明細` → `pos-transaction`）。
- 输出目录：默认 `<用户指定根目录>/data-processing/<if-id>/`。

## 2. 复制模板

```bash
cp -r <skill>/assets/sample-project <输出根目录>/data-processing/<if-id>
```

将 `<if-id>` 代入事件文件与命名（如 `events/<if-id>.json`）。

## 3. 拆分实现：处理器文件与入口文件

保持 sample 的整体结构：业务逻辑放独立处理器文件，入口只做组装。

- `src/<if-id>_processor.py`：Processor 类（`name`、`validate()`、`process()`）与表/仓库访问辅助函数；命名示例 `pos-transaction` → `src/pos_transaction_processor.py`。
- `src/app.py`：`lambda_handler`、`build_pipeline`、`build_job_run_manager`，从处理器模块导入类。
- 参照 `assets/sample-project/src/transaction_processor.py` 与 `src/app.py`。

### validate() 规则实现

- 必填校验：
  ```python
  required = ("field_a", "field_b")
  missing = [f for f in required if payload.get(f) in (None, "")]
  if missing:
      raise ValueError(f"payload missing: {', '.join(missing)}")
  ```
- 类型校验：`int(str(payload["x"]))`、`Decimal(str(payload["x"]))`，失败抛 `ValueError`。
- 范围/正值：`if int(payload["x"]) <= 0: raise ValueError(...)`。
- 格式校验：正则或 `datetime.strptime(value, "%Y%m%d")`。
- 枚举校验：`if payload["x"] not in {...}: raise ValueError(...)`。

### process() 转换实现

- 按输出字段逐个转换，统一构造 `item` dict。
- 金额计算用 `Decimal`：`gross_amount = quantity * unit_price`。
- 字符串规范化：`.strip()`、`.upper()`、`.zfill(n)`、日期 `strftime`。
- 默认值 / 固定值 / 映射表按文档实现。
- 幂等业务写入：
  ```python
  try:
      self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(event_id)")
  except self._table.meta.client.exceptions.ConditionalCheckFailedException:
      pass
  ```
- 返回摘要：`{"event_id": ..., <关键输出>: str(...)}`。

### 数据库输出（若转换目标为 Aurora/DB）

- 模块级单例：`_POOL = ConnectionPool(factory, max_size=...)`。
- `process()` 内用 `DatabaseRepository(pool)` 的 `transaction()` / `execute()`。
- 数据库驱动（如 psycopg）由业务函数自行引入，template.yaml 中配置对应 Secret/网络策略。

## 4. 定制 template.yaml

- 复制 sample 的资源结构：DLQ → 主队列（`VisibilityTimeout: 180`、`maxReceiveCount: 3`）→ 输出表 → IdempotencyLedger → JobRuns → 函数。
- 替换：函数逻辑 ID/名称、表名、环境变量（`IDEMPOTENCY_TABLE`、`JOB_RUNS_TABLE`、输出表变量）。
- 保留：`ReportBatchItemFailures`、`BatchSize: 10`、`DynamoDBCrudPolicy`。

## 5. 生成 events/<if-id>.json

按 IF定義書 的字段示例构造一条 SQS Records 消息（body 为信封 JSON，`payload` 含输入字段）。

## 6. 编写 tests/test_app.py

- 从 `src/<if-id>_processor.py` 导入 Processor 类。
- 每个必填字段缺失 → 断言 `ValueError` 且消息含字段名。
- 每个类型/范围/格式/枚举规则 → 断言拒绝。
- 每个转换规则 → 构造输入，断言输出字段值。
- 幂等：同一 `event_id` 第二次执行 `duplicate=True`。
- 参照 sample 测试写法（`table=object()` 注入，避免 boto3）。

## 7. 运行测试

在仓库根目录执行：

```bash
PYTHONPATH=data-processing-layer/layer/python:<输出目录>/<if-id>/src \
  python3 -m unittest discover -s <输出目录>/<if-id>/tests -v
```

生成项目的测试必须全部通过后才能交付。

## 常见坑

- `Decimal` 输出前转 `str`。
- 只使用 `__init__.py` 导出的框架 API，不要改框架签名。
- 测试中环境变量用 `os.environ.setdefault(...)`。
- 不要在 `validate()` 中写库/发消息。
- 输出字段名与文档物理名一致，不要臆造。
