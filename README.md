# Serverless Data Processor

本仓库包含两个独立的 AWS SAM 子项目：

- `data-processing-layer`：供 Lambda 复用的数据处理框架 Layer。
- `data-processing-sample`：引用 Layer 的 SQS 数据处理示例。

详见各子项目的 README。整体方案说明位于 `solution-design/solution-design.md`。
