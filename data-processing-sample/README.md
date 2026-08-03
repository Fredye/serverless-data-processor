# Data Processing Sample

一个 POS 交易行数据的 SQS 消费者示例。它通过 Layer 标准化事件、校验载荷，并把处理后的交易行写入 PostgreSQL；写入使用 `INSERT ... ON CONFLICT (event_id) DO NOTHING`，业务写入自身幂等，配合框架幂等表形成双重防护。处理开始时写入 `JobRuns` 表的 `RUNNING` 记录，成功后更新为 `SUCCEEDED`，失败时更新为 `FAILED`。SQS 启用了 `ReportBatchItemFailures`，仅重试失败记录，三次失败后进入 DLQ。

数据库访问复用 Layer 的 `BaseRepository`（内部基于 `ConnectionPool` 与 `DatabaseRepository`，见 `src/transaction_repository.py`）：psycopg3 已随 Layer 提供，`BaseRepository` 内置 psycopg 连接工厂（读取 `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD`），连接池由 `DB_POOL_MAX_SIZE` / `DB_POOL_TIMEOUT_SECONDS` 驱动并以模块级单例存在，warm Lambda 可直接复用连接；生产环境建议通过 RDS Proxy 访问 Aurora/RDS。

## 数据库准备

先准备一个 PostgreSQL 数据库（RDS/Aurora 或自建均可），并执行建表脚本：

```bash
psql "$DATABASE_URL" -f db/schema.sql
```

建表脚本会创建 `processed_transactions` 表，`event_id` 为主键，重复插入被 `ON CONFLICT DO NOTHING` 忽略。

## 部署

先部署相邻的 `data-processing-layer` 项目，并获得其 `LayerVersionArn`。若数据库位于 VPC 私有子网，还需提供 Lambda 使用的子网与安全组（需放行到 RDS 5432 端口的出站访问，并保证 Lambda 可访问 DynamoDB/SQS）。随后在本目录执行：

```bash
sam build
sam deploy --guided --parameter-overrides \
  DataProcessingLayerArn=<layer-version-arn> \
  DatabaseHost=<rds-endpoint> \
  DatabasePort=5432 \
  DatabaseName=<db-name> \
  DatabaseUser=<db-user> \
  DatabasePassword=<db-password> \
  VpcSubnetIds=<subnet-1,subnet-2> \
  VpcSecurityGroupIds=<security-group-id>
```

生产环境建议将数据库口令放入 AWS Secrets Manager，由 Lambda 运行时读取，而不是作为 CloudFormation 参数明文传入。

## 本地测试

先安装 Layer 与示例（均为可编辑模式；Sample 依赖 Layer，需先安装）：

```bash
pip install -e ./data-processing-layer
pip install -e ./data-processing-sample
```

随后在仓库根目录执行（安装后无需设置 `PYTHONPATH`）：

```bash
python3 -m unittest discover -s data-processing-layer/tests -v
python3 -m unittest discover -s data-processing-sample/tests -v
```

未安装时，也可以把 `layer/python` 与 `src` 加入 `PYTHONPATH` 后按原方式运行。单元测试不依赖真实数据库：仓储层通过 fake 的 `DatabaseRepository` 验证 SQL 与参数，处理器通过 fake 仓储验证写入内容。示例输入见 `events/transaction.json`。生产环境应为数据质量错误增加 quarantine S3 路径与错误报告，再由人工或重放任务处置。
