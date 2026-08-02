# 示例：POS 取引明細 IF

展示 IF定義書 / IF変換説明書 的典型写法与期望产物（对应仓库内 `data-processing-sample` 的实现，也即 `assets/sample-project/`）。

## IF定義書（POS 取引明細）

| 項番 | 項目ID | 項目名 | 型 | 桁数 | 必須 | 形式/パターン | 例 |
|---|---|---|---|---|---|---|---|
| 1 | transaction_id | 取引ID | 文字列 | 20 | ○ | - | T-1001 |
| 2 | line_id | 行番号 | 文字列 | 5 | ○ | 半角数字 | 1 |
| 3 | store_id | 店舗ID | 文字列 | 10 | ○ | - | 1001 |
| 4 | quantity | 数量 | 整数 | 9 | ○ | 正の整数 | 2 |
| 5 | unit_price | 単価 | 数値 | 12 | ○ | 数値 | 1980 |
| 6 | currency_code | 通貨コード | 文字列 | 3 | ○ | 英字 | JPY |

## IF変換説明書（変換先: transactions テーブル）

| 変換元項目 | 変換先項目 | 変換ロジック | 変換先型 | 必須 |
|---|---|---|---|---|
| transaction_id | transaction_id | そのまま | S | ○ |
| line_id | line_id | そのまま | S | ○ |
| store_id | store_id | そのまま | S | ○ |
| quantity | quantity | 型変換 (Decimal) | N | ○ |
| unit_price | unit_price | 型変換 (Decimal) | N | ○ |
| quantity, unit_price | gross_amount | 計算: 数量×単価 | N | ○ |
| currency_code | currency_code | 大文字化 | S | ○ |
| （固定） | event_id | 固定: event.event_id | S | ○ |
| （固定） | source_system | 固定: event.source_system | S | ○ |
| （固定） | trace_id | 固定: context.trace_id | S | ○ |

## 期望产物

即 `assets/sample-project/src/transaction_processor.py` 与 `src/app.py`：

- `transaction_processor.py`：`validate()` 校验必填字段、quantity 为正整数、unit_price 可解析为 Decimal；`process()` 用 Decimal 计算 `gross_amount = quantity * unit_price`，`currency_code` 大写，写入 `event_id` 条件写入的表。
- `app.py`：`lambda_handler` 组装管线，SQS 逐条处理、作业台账、失败记录返回 `batchItemFailures`。
