# 设计文档 — Customer 360 湖仓平台

> Final Project · ID2221 Data-Intensive Computing
> **状态：已开始实现。** 运行环境已确定，湖仓栈已跑通；pipeline 还没在 Iceberg 上跑过。分步计划和分工见第 9 节，剩余待办见第 10 节。

---

## 0. 如何使用本文档

这是**内部设计文档**——比你要提交审批的一页 proposal 更详细。proposal 获批后，本文档指导具体实现。要提交的一页 proposal 是第 1–4 节的压缩版（骨架见文末第 10 节）。

---

## 1. 问题描述（Problem Description）

**问题。** 在大多数面向客户的机构里，客户数据分散在互不相通的系统中：交易在一个系统、CRM/互动记录在另一个、产品目录在第三个、Web/App 行为在第四个。各系统 schema、格式、更新节奏都不同。由于没有统一视图，业务团队无法回答跨系统的基本问题——*哪些客户有流失风险、原因是什么？*——除非做缓慢的跨团队手工提数。

**我们要构建的。** 一个可复用的数据平台（湖仓），它摄取这些异构源、清洗并整合成一个受治理的 **Customer 360** 统一视图，并对外提供分析能力。在整合视图之上，回答具体业务问题（客户分群、流失/风险客户识别）。

**为什么这是一个合格的大数据问题。** 这是经典的"打破数据孤岛"数据工程用例（Databricks、Dremio 等厂商都用它作标准演示场景）。它覆盖了本课程针对的完整技术栈：异构摄取、分布式处理、对象存储上的开放表格式、数据质量治理、分析服务——数据量也大到足以证明使用分布式引擎（Spark）的合理性。

**范围边界。** 项目重点是**平台与数据工程管道**，而非最大化预测精度。分析方法（及可选的 ML 模型）位于其上，用于证明整合后的平台能支撑下游分析。

---

## 2. 工具（技术栈）

| 层 | 技术 | 作用 | 选型理由 |
|---|---|---|---|
| 对象存储 | **MinIO** | 所有数据的 S3 兼容存储 | 开源、S3 API、HDFS 的现代等价物、轻量 |
| 表格式 | **Apache Iceberg** | ACID 表、schema 演进、时间旅行、隐藏分区 | 开放表格式、引擎无关、湖仓标准 |
| catalog | **Iceberg REST catalog** | 管理 Iceberg 表元数据 | 最轻量；官方 quickstart 默认；免去单独部署 Hive Metastore+数据库 |
| 处理引擎 | **Apache Spark (PySpark)** | 摄取、清洗、整合、特征构建、分析 | 课程核心；要求的大数据处理框架 |
| 查询（可选） | **Trino** | Iceberg 上的交互式 SQL | 仅在需要 BI/交互层时使用；MPP 引擎（等价于 Impala） |
| 可视化 | **Superset** | 基于 Gold 表的 dashboard | 作业要求的可视化交付；经 Trino/Spark SQL 连 Iceberg |
| ML（可选） | **Spark MLlib** | 流失分类 | 仅在加预测组件时使用 |
| 运行环境 | **Docker Compose** | 容器编排 | 任何装了 Docker 的机器都能跑；避免 K8s 复杂度 |

**运行位置。** 队员的笔记本电脑，跑在 WSL2（Ubuntu 24.04）里，Docker Engine 29.8.1、Compose v5.5.1。仓库和源 CSV 放在 Linux 文件系统的 `~/customer360-lakehouse`；Iceberg 表放在栈内的 MinIO。

学校的共享 Linux 服务器已经先行排查过，跑不了这套栈：`brute.aalto.fi` 上没有 Docker、没有 Podman，也没有分配 subordinate UID 段，因此没有管理员配合就装不了任何容器运行时；模块里只有 `apptainer`，它能跑单个镜像，但不支持 Compose 编排。kthcloud 提供免费虚拟机，但默认账号等级没有 `useVms` 权限、磁盘配额只有 20 GB，两项都需要申请升级。用 WSL2 零成本，而且因为环境完全由 `infra/docker-compose.yml` 定义，将来如果需要共享环境，同一套栈可以原样搬到云主机（CSC Pouta 或 kthcloud）上。

**版本固定。** `tabulario/spark-iceberg:3.5.5_1.8.1`（Spark 3.5.5、Iceberg 1.8.1、Python 3.10、Java 17）和 `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`。`requirements.txt` 固定 `pyspark==3.5.5`，保证本地运行和容器内版本一致。

**起点。** Apache Iceberg 官方 *Spark and Iceberg Quickstart*（`tabular-io/docker-spark-iceberg`）提供一个验证过的 compose 文件（Spark + REST catalog + MinIO）。我们在其基础上扩展，而非从零搭建。

---

## 3. 数据（Data）

**数据集（已核实）。** 五个 CSV 文件，代表五个异构源系统，共约 88 万行 / 约 190 MB，均通过 `customer_id`（及 `product_id`、`device_id`）关联。多个字段**有意做脏**——这正是该数据集契合本项目的核心原因：它直接锻炼清洗与数据质量治理能力。

| 源文件 | 行数 | 大小 | 源系统 | 角色 | Schema（列） |
|---|---|---|---|---|---|
| `clickstream_500k_events.csv` | 500,000 | 122 MB | Web/App 行为分析 | 事实表（最大） | event_id, session_id, customer_id, event_type, page_url, device_id, timestamp, ingest_run_id |
| `orders_300k_dirty.csv` | 300,000 | 35 MB | 订单/交易系统 | 事实表 | order_id, customer_id, product_id, order_amount, order_date, payment_method, status, quantity |
| `crm_50000_customers_dirty_v3.csv` | 50,000 | 11 MB | CRM | 客户主维表 | customer_id, first_name, last_name, email, phone_number, gender, dob, signup_date, address, city, state, country, device_id(s), source |
| `support_tickets_30000_dirty.csv` | 30,000 | 4.2 MB | 客服系统 | 事实表 | ticket_id, customer_id, issue_type, ticket_created, ticket_resolved, resolution_time_hours, sentiment, support_agent |
| `product_catalog_dirty_30pct.csv` | 500 | 19 KB | 产品目录 | 小维表（lookup） | product_id, product_name, category, price |

**关联键。** `customer_id` 串起 CRM ↔ orders ↔ tickets ↔ clickstream；`product_id` 串起 orders ↔ product_catalog；`device_id` 把 clickstream 与 CRM 的多值字段 `device_id(s)` 关联。

**数据量说明。** 约 88 万行对单机 Spark 是很合适的规模：足够大到能体现分布式处理和分区裁剪实验的价值，又不会大到服务器/笔记本跑不动。

**数据获取。** 静态 CSV 文件，一次性下载到 `data/` 目录，该目录以**只读**方式挂载进 Spark 容器的 `/home/iceberg/data`，作为源系统落地区。CSV **不上传 MinIO**：MinIO 只承载 Bronze 及以上的 Iceberg 表。只读挂载从物理上保证 pipeline 不会回写源数据。无实时摄取。

**已核实的脏数据清单（驱动 Silver/DQ 设计，见 4.2）。**

| 源 | 观察到的脏数据形态 | 对应的清洗/DQ 技能 |
|---|---|---|
| clickstream | 空 `customer_id`；非法时间戳如 `2024/31/01 25:61:00`；CRLF 换行符 | 缺失外键检查；时间戳校验；格式规范化 |
| orders | `payment_method` 脏值（`wall-et` vs `wallet`）；坏/空 `order_date`；`order_amount` 尾部空格；异常大金额 | 类别值标准化；时间戳校验；trim+类型转换；异常值检测 |
| CRM | 大小写混乱（`mARIA`、`KKeevvin`）；前导空格；电话格式多样；多值 `device_id(s)` 用 `;` 分隔 | 字符串标准化；trim；电话规范化；多值字段拆分 |
| product_catalog | `category` 截断/乱码（`clo`、`3l3ctronics`）；负价格（`-100`）；前导空格 | 类别清洗；无效数值校验；trim |
| support_tickets | 空 `ticket_created`；非法时间戳（`2025-12-12T28:77:10`）；逻辑矛盾（resolved 早于 created）；`sentiment` 大小写不一致；客服名噪声 | 缺失值检查；时间戳校验；**跨字段逻辑校验**；标准化 |

**数据来源。** Kaggle 的 "Customer 360" 数据集（`https://www.kaggle.com/datasets/vinaykandimalla/customer-360`）。`【待确认：在数据集页面上核对 License 字段是否允许课程使用，并在 proposal 和报告里引用】`

**关于数据真实性（写进报告的定调）。** 该数据集是"仿真但贴近真实"、且有意做脏的。我们把项目定调在**平台能力**（整合 + 治理）而非"真实商业洞察"上——因此其合成性质是优点（可控、可复现、富含数据质量挑战），而非缺点。

---

## 4. 方法与算法（Methodology and Algorithm）

采用 **Medallion 架构**（Bronze → Silver → Gold）这一标准分层湖仓模式，并在其上叠加分析方法。所有方法均不依赖 ML；ML 是可选附加项。

### 4.1 Bronze — 原始落地
- 将每个源文件原样摄取为 Iceberg 表，按源对齐，不做转换。
- 记录摄取元数据（行数、摄取时间戳、源名、schema 版本）。
- **方法：** 通用 PySpark 摄取（格式无关的 reader → Iceberg writer）。

### 4.2 Silver — 清洗与标准化
基于第 3 节已核实的脏数据清单，具体清洗任务：
- **字符串标准化：** 姓名/情绪/类别大小写归一；`trim` 前后空格（CRM 姓名、order_amount、price、客服名）。
- **时间戳校验：** 拒绝/修复非法时间戳（`2024/31/01 25:61:00`、`2025-12-12T28:77:10`）；统一为 ISO-8601 UTC 格式。
- **类别值标准化：** 把脏值映射到规范集合（`wall-et`→`wallet`；修复 `clo`/`3l3ctronics`）。
- **数值校验：** 拒绝负价格（`-100`）；把 trim 后的字符串转数值；标记异常订单金额。
- **多值处理：** 拆开 CRM 的 `device_id(s)`（`;` 分隔）为规范化的"客户-设备"关联。
- **缺失/外键处理：** 空 `customer_id`（clickstream）、空日期（orders/tickets）。
- **跨字段逻辑校验：** 工单 `resolved` 必须 ≥ `created`。
- 无效记录**隔离进 reject 表、上报、并从 Gold 层排除**——不静默丢弃。
- **方法：** 规则化校验框架——一组可复用、可组合的检查函数（通用检查如空值/重复/类型，加上按源的专属规则），使新增规则时改动最小。

### 4.3 Gold — 整合成 Customer 360
- 把清洗后的各源 join 成一张 **Customer 360 表**：每行一个客户，附上交易汇总、互动历史、产品偏好、行为聚合。
- 构建分析集市（分群、流失特征）。
- **方法：**
  - **聚合与特征构建**（按客户汇总：recency、frequency、monetary；互动次数；在网时长）。
  - **客户分群**——如 RFM 式分群或规则分层。`【依据实际数据字段确定分群维度】`
  - **流失/风险分析**——在整合特征上做规则打分（无需 ML 即可满足要求）。

### 4.4 可选 — 预测组件（ML）
*仅在时间允许时加入；是加分项，非必需。*
- **任务：** 二分类——预测客户是否会流失。
- **方法：** Spark MLlib `Pipeline`——`VectorAssembler` → `StringIndexer`/`OneHotEncoder` → `StandardScaler` → 分类器（如 `LogisticRegression` 或 `RandomForestClassifier`）→ `Evaluator`（AUC）。
- 复用 Gold 层的 Customer 360 特征作为训练表。

### 4.5 服务/呈现
- 分析结果通过 Spark SQL（及可选 Trino 交互式查询）对外提供。
- **Superset dashboard**，呈现：客户分群、流失风险分布、关键驱动因素。

---

## 5. 架构（Architecture）

### 5.1 数据流

```
 5 个源 CSV 文件（多源、脏数据）
   放在 data/，以只读方式挂载进 Spark 容器
        │  仅 Bronze 读取
        ▼
 ┌──────────────┐
 │    MinIO     │  ← S3 兼容对象存储（Bronze 及以上都存这里）
 └──────┬───────┘
        │  Spark 读写
        ▼
 ┌─────────────────────────────────────────────┐
 │            Apache Spark (PySpark)            │
 │  Bronze → Silver → Gold  +  DQ + (可选) MLlib │
 └──────┬──────────────────────────────────────┘
        │  Iceberg 表（元数据经 REST catalog）
        ▼
 ┌──────────────┐        ┌───────────────────────┐
 │ REST Catalog │        │  查询 / 呈现层         │
 │ (Iceberg 元数据)│      │  Spark SQL / Trino     │
 └──────────────┘        │  → Superset dashboard  │
                         └───────────────────────┘
```

### 5.2 范围决策

为贴合项目范围与运行环境：源数据从只读落地目录读取，而非实时源数据库；调度用一个 shell 脚本（`run_pipeline.sh`），而非专门的编排器。小维表（product_catalog）不分区；大事实表按日期分区。

最终容器构成：MinIO + `mc`（建桶）+ Spark/Jupyter + REST catalog，加上第 9 节步骤 4、5 完成后的 Trino 和 Superset。

### 5.3 参考来源

- Apache Iceberg — *Spark and Iceberg Quickstart*（`tabular-io/docker-spark-iceberg`）。
- Medallion（Bronze/Silver/Gold）分层湖仓架构模式。
- Customer 360 / churn 参考用例（见 Databricks、Dremio 相关材料）。
- `【补充：具体参考的开源项目/教程链接】`

## 6. 存储与命名规范

- **桶：** MinIO 里的 `s3://warehouse/`，沿用 Iceberg quickstart 的默认值。
- **表命名：** catalog 为 `demo`，每层一个 namespace：`demo.bronze.orders`、`demo.silver.orders`、`demo.quarantine.orders`、`demo.gold.customer_360`、`demo.metadata.ingestion`。namespace 由 `lakehouse_io.ensure_namespaces()` 自动创建。
- **分区：** 大事实表/行为表按日期分区（如 `order_date` 按月）；小维表（product_catalog、customers）**不分区**——基数低，分区会造成小文件开销。
- **公共数据模型：** ISO-8601 UTC 时间戳；snake_case 列名；显式类型；文档化的空值处理规则。

---

## 7. 仓库结构

```
customer360-lakehouse/          # 位于 WSL 内的 ~/customer360-lakehouse
├── README.md                 # 如何运行（满足交付要求）
├── requirements.txt          # pyspark==3.5.5，与容器内版本一致
├── run_pipeline.sh           # Bronze → Silver → Gold
├── .gitignore                # 忽略 data/*.csv、warehouse/、.env
├── .env.example              # 配置模板，不含密钥
├── docs/                     # 本设计文档
├── infra/
│   ├── docker-compose.yml    # MinIO + mc + Spark + REST catalog（Trino、Superset 待加）
│   ├── LICENSE.upstream      # 所扩展的 quickstart 的 Apache-2.0 许可证
│   └── README.md             # 对上游的改动、端口、启动方式
├── src/
│   ├── spark_session.py      # Spark 配置，local / iceberg 双模式
│   ├── lakehouse_io.py       # 两种模式的统一读写层
│   ├── bronze_ingest.py      # 通用摄取 → Bronze
│   ├── silver_clean.py       # 清洗 + DQ + 隔离
│   ├── gold_metrics.py       # join → Customer 360 + 6 张分析表
│   ├── dq/framework.py       # 可复用校验规则
│   └── ml/ (可选)            # MLlib 流失 pipeline
├── notebooks/                # 探索、分析、图表
├── sql/                      # 分析查询
└── data/                     # ❌ CSV 已 gitignore（仅 clickstream 就 122 MB）
```


---

## 8. 评估计划

为让报告用实测支撑（而非凭直觉），测量：
- 每个源的摄取时间；总管道时间。
- 每层存储大小；生成文件数。
- 关键分析查询的延迟。
- DQ 统计：每个源被拒绝/隔离的行数。
- （若对事实表比较两种分区策略）摄取时间、存储、查询延迟、文件数。
- （若做 ML）在留出测试集上的模型 AUC。

---

## 9. 实施计划

七个步骤，每一步都能独立验证，这样出问题能定位到具体是哪一步。状态截至本文档最后一次修改。

### 步骤 1 — 环境 · 已完成

笔记本上的 WSL2（Ubuntu 24.04），Docker Engine 29.8.1、Compose v5.5.1。`cd infra && docker compose up -d` 启动四个服务：`spark-iceberg`、`rest`、`minio`、`mc`。

已验证：四个容器正常运行；Spark 容器上四个挂载都在，其中 `data/` 是只读；Spark 3.5.5 配 Iceberg 1.8.1 运行时；`mc` 已创建 `warehouse` 桶；REST catalog 在 8181 端口正常响应；宿主机浏览器能打开 Jupyter（8888）、MinIO 控制台（9001）和 Spark UI（8080）。

### 步骤 2 — 在 Iceberg 上跑通 pipeline · 下一步

```bash
cd infra && docker compose up -d
docker compose exec spark-iceberg bash
cd /home/iceberg/project
export LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data
./run_pipeline.sh
```

完成标准：`demo.bronze.*`、`demo.silver.*`、`demo.quarantine.*`、`demo.gold.*` 都已生成，七张 Gold 表都有数据。

跑的时候要记录下面这些数字，它们直接写进报告：每个源的隔离比例和原因分布、每一层的行数、以及 `demo.metadata.ingestion` 里记录的每个源摄取耗时。

### 步骤 3 — 与本地基线交叉验证

用 `LAKEHOUSE_MODE=local` 和 `LOCAL_OUT=./warehouse` 再跑一遍，同一套代码改写 Parquet。完成标准：行数和 DQ 比例与步骤 2 完全一致。对不上说明存储抽象层有 bug，而不是业务逻辑有问题。

### 步骤 4 — 加 Trino

在 `infra/docker-compose.yml` 里加一个 `trino` 服务，配置 Iceberg catalog 指向 REST catalog（`http://rest:8181`）和 MinIO（`http://minio:9000`）。完成标准：在 Trino CLI 里执行 `SELECT * FROM iceberg.gold.customer_360 LIMIT 10` 能返回数据。新增的服务要写进 compose 顶部注释的改动清单，这是上游 Apache-2.0 许可证的要求。

### 步骤 5 — 加 Superset，做 dashboard

在 compose 里加 `superset`，安装 Trino 驱动，连上 Trino。然后按 dashboard 蓝本做六个面板：客户概览、RFM 分群、流失风险与营销优先级、产品渗透、行为漏斗、数据质量。

完成标准：每个 dashboard 都能从 Gold 表渲染出来。做完要把 dashboard 从 Superset 导出到仓库里，这样别人部署一套新的也能导入——dashboard 存在 Superset 自己的数据库里，不导出就丢了。

### 步骤 6 — 流失分类器（可选）

只在步骤 1 到 5 都完成且还有时间时做。在 Gold 特征表上跑 Spark MLlib pipeline，用 AUC 汇报效果。详见 4.4。

### 步骤 7 — 整合与交付

全新 clone 一份，执行 `docker compose up -d` 和 `./run_pipeline.sh`，必须能复现报告里的每一个数字。然后写 README、两页报告和运行说明。

### 分工

步骤 2、3 必须在步骤 4、5 之前完成，因为 Trino 和 Superset 要有 Gold 表才能读。步骤 3 之后，查询和可视化这条线可以和可选的模型并行推进。

| 负责内容 | 负责人 | 依赖 |
|---|---|---|
| 摄取与 Bronze | `[姓名]` | — |
| 清洗、DQ 规则、隔离 | `[姓名]` | Bronze |
| Gold 整合与指标 | `[姓名]` | Silver |
| Trino、Superset、dashboard | `[姓名]` | Gold |
| 报告与可复现性 | `[姓名]` | 全部 |

---

## 10. 待办项

1. **数据集许可证** — 在 Kaggle 页面上确认是否允许课程使用，并在文档中引用。表、行数、schema 已核实。
2. `【待确认】` **分工** — 把上面表格里的负责人填上。
3. `【待确认】` **具体分析问题** — Gold 层要回答的具体业务问题（分群维度、流失定义）。
4. `【待确认】` **examiner 确认** — 确认「扩展官方 Iceberg quickstart」的做法可接受，以及如何注明。改动清单已经记在 compose 顶部注释和 `infra/README.md` 里。
5. **可选组件** — ML 分类器等步骤 5 完成后再定。Trino 和 Superset 属于查询/可视化要求范围内，保留。

---

## 11. 一页 Proposal 骨架（用于提交）

> 把以上压缩成约 1 页，按要求的四个标题：

**1. Problem Description.** 客户数据分散在各系统（CRM/交易/行为），缺乏统一视图，团队无法识别风险客户或理解流失动因。我们构建一个湖仓平台，把异构客户数据整合成受治理的 Customer 360 视图，并做客户分群与流失/风险分析。

**2. Tools.** MinIO（S3 存储）· Apache Iceberg（表格式）· Apache Spark/PySpark（处理）· Iceberg REST catalog · Trino（查询）· Superset（可视化）· Docker Compose。

**3. Data.** 五个异构源文件（约 88 万行 / 190 MB），代表不同系统：Web/App clickstream（50 万）、orders（30 万）、CRM 客户（5 万）、support tickets（3 万）、product catalog（500）。通过 customer_id / product_id / device_id 关联。有意做脏（非法时间戳、大小写不一致、坏类别值、负价格、多值字段、跨字段矛盾），用于锻炼清洗与数据质量组件。静态 CSV 放在只读落地目录，Bronze 及以上为 Iceberg 表。`【确认数据集许可证是否允许课程使用】`

**4. Methodology and Algorithm.** Medallion 架构（Bronze→Silver→Gold）：通用摄取；规则化清洗 + 数据质量校验 + 坏记录隔离；多源整合成 Customer 360；聚合 + 分群（RFM 式）+ 规则化流失打分。可选：Spark MLlib 流失分类器（`Pipeline` → LogisticRegression/RandomForest，AUC）。结果经 Spark SQL/Trino 提供，并用 Superset dashboard 可视化。
