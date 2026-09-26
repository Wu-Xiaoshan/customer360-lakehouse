# 设计文档 — Customer 360 湖仓平台

> Final Project · ID2221 Data-Intensive Computing
> **状态：实现中。** pipeline 已在 Iceberg 上端到端运行，产出每个客户一行的 Customer 360 表。下一步是第 9 节的步骤 3。计划和分工见第 9 节，每次运行的数字见 `run_log.md`，待办见第 10 节。

---

## 0. 如何使用本文档

这是**内部设计文档**——比要提交审批的一页 proposal 更详细。proposal 获批后，本文档指导具体实现。一页 proposal 是第 1–4 节的压缩版（骨架见文末第 11 节）。

---

## 1. 问题描述（Problem Description）

**问题。** 在大多数面向客户的机构里，客户数据分散在互不相通的系统中：交易在一个系统、CRM/互动记录在另一个、产品目录在第三个、Web/App 行为在第四个。各系统 schema、格式、更新节奏都不同。由于没有统一视图，业务团队无法回答跨系统的基本问题——*哪些客户有流失风险、原因是什么？*——除非做缓慢的跨团队手工提数。

**我们要构建的。** 一个可复用的数据平台（湖仓），它摄取这些异构源、清洗并整合成一个受治理的 **Customer 360** 统一视图，并对外提供分析能力。在整合视图之上，回答具体业务问题（客户分群、流失/风险客户识别）。

**为什么这是一个合格的大数据问题。** 这是经典的"打破数据孤岛"数据工程用例（Databricks、Dremio 等厂商都用它作标准演示场景）。它覆盖了本课程针对的完整技术栈：异构摄取、分布式处理、对象存储上的开放表格式、数据质量治理、分析服务——数据量也大到足以证明使用分布式引擎（Spark）的合理性。

**范围边界。** 项目重点是**平台与数据工程管道**，而非最大化预测精度。分析方法（及可选的 ML 模型）位于其上，用于证明整合后的平台能支撑下游分析。

**Customer 360 需要满足什么。** 三条性质，每条都在第 9 节验证：每个真实客户恰好一行；每个数据源都为这一行贡献属性；所有数据源以同一个参考日期衡量。

---

## 2. 工具（技术栈）

| 层 | 技术 | 作用 | 选型理由 |
|---|---|---|---|
| 对象存储 | **MinIO** | 所有数据的 S3 兼容存储 | 开源、S3 API、HDFS 的现代等价物、轻量 |
| 表格式 | **Apache Iceberg** | ACID 表、schema 演进、时间旅行、隐藏分区 | 开放表格式、引擎无关、湖仓标准 |
| catalog | **Iceberg REST catalog** | 管理 Iceberg 表元数据 | 最轻量；官方 quickstart 默认；免去单独部署 Hive Metastore+数据库 |
| 处理引擎 | **Apache Spark (PySpark)** | 摄取、清洗、整合、特征构建、分析 | 课程核心；要求的大数据处理框架 |
| 查询 | **Trino** | Iceberg 上的交互式 SQL | 为 Superset 提供查询；MPP 引擎（等价于 Impala） |
| 可视化 | **Superset** | 基于 Gold 表的 dashboard | 作业要求的可视化交付；经 Trino 连 Iceberg |
| ML（可选） | **Spark MLlib** | 流失分类 | 仅在加预测组件时使用 |
| 运行环境 | **Docker Compose** | 容器编排 | 任何装了 Docker 的机器都能跑；避免 K8s 复杂度 |

**运行位置。** 队员的笔记本电脑，跑在 WSL2（Ubuntu 24.04）里，Docker Engine 29.8.1、Compose v5.5.1。仓库和源 CSV 放在 Linux 文件系统的 `~/customer360-lakehouse`；Iceberg 表放在栈内的 MinIO。

学校的共享 Linux 服务器已经先行排查过，跑不了这套栈：`brute.aalto.fi` 上没有 Docker、没有 Podman，也没有分配 subordinate UID 段，因此没有管理员配合就装不了任何容器运行时；模块里只有 `apptainer`，它能跑单个镜像，但不支持 Compose 编排。kthcloud 提供免费虚拟机，但默认账号等级没有 `useVms` 权限、磁盘配额只有 20 GB，两项都需要申请升级。用 WSL2 零成本，而且因为环境完全由 `infra/docker-compose.yml` 定义，将来如果需要共享环境，同一套栈可以原样搬到云主机（CSC Pouta 或 kthcloud）上。

**版本固定。** `tabulario/spark-iceberg:3.5.5_1.8.1`（Spark 3.5.5、Iceberg 1.8.1、Python 3.10、Java 17）和 `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`。`requirements.txt` 固定 `pyspark==3.5.5`，保证本地运行和容器内版本一致。

**持久化。** 上游 quickstart 每次启动都会清空 `warehouse` 桶，而且 REST catalog 只存在内存里。我们的 compose 去掉了清空操作，把 catalog 放进 SQLite 文件，并给 MinIO 和 catalog 各配一个具名卷，所以 `docker compose down` 和重启之后表都还在。

**起点。** Apache Iceberg 官方 *Spark and Iceberg Quickstart*（`tabular-io/docker-spark-iceberg`）提供一个验证过的 compose 文件（Spark + REST catalog + MinIO）。我们在其基础上扩展，而非从零搭建。

---

## 3. 数据（Data）

**数据集。** 五个 CSV 文件，代表五个异构源系统，共 880,500 行、179 MB，来自 Kaggle 公开数据集 "Customer 360"（`https://www.kaggle.com/datasets/vinaykandimalla/customer-360`）。`【待确认：在数据集页面上核对 License 字段是否允许课程使用，并在 proposal 和报告里引用】`

| 源文件 | 行数 | 大小 | 源系统 | 角色 | Schema（列） |
|---|---|---|---|---|---|
| `clickstream_500k_events.csv` | 500,000 | 122 MB | Web/App 行为分析 | 事实表（最大） | event_id, session_id, customer_id, event_type, page_url, device_id, timestamp, ingest_run_id |
| `orders_300k_dirty.csv` | 300,000 | 35 MB | 订单/交易系统 | 事实表 | order_id, customer_id, product_id, order_amount, order_date, payment_method, status, quantity |
| `crm_50000_customers_dirty_v3.csv` | 50,000 | 11 MB | CRM | 客户主维表 | customer_id, first_name, last_name, email, phone_number, gender, dob, signup_date, address, city, state, country, device_id(s), source |
| `support_tickets_30000_dirty.csv` | 30,000 | 4.2 MB | 客服系统 | 事实表 | ticket_id, customer_id, issue_type, ticket_created, ticket_resolved, resolution_time_hours, sentiment, support_agent |
| `product_catalog_dirty_30pct.csv` | 500 | 19 KB | 产品目录 | 小维表（lookup） | product_id, product_name, category, price |

**关联键。** `customer_id` 把 CRM 和订单、工单、clickstream 连起来；`product_id` 把订单和商品目录连起来。这两个键在源数据里都是完整的：没有任何订单、工单或事件引用了 Bronze 父表中不存在的客户或商品。`device_id` 和 `session_id` 看起来像关联键，实际不是（见 3.2）。

**数据获取。** 静态 CSV 文件，一次性下载到 `data/` 目录，该目录以**只读**方式挂载进 Spark 容器的 `/home/iceberg/data`，作为源系统落地区。CSV **不上传 MinIO**：MinIO 只承载 Bronze 及以上的 Iceberg 表。只读挂载从物理上保证 pipeline 不会回写源数据。无实时摄取。

**时间覆盖与参考日期。** 各数据源覆盖的时间段不同：

| 数据源 | 原始数据的时间跨度 |
|---|---|
| 订单 | 2023-12-03 → 2025-12-01 |
| clickstream | 2025-09-02 → 2028-02-10，其中 95.8% 在 2025 年最后四个月 |
| 工单 | 2023 → 2027 |
| CRM 注册日期 | 2013-12-02 → 2025-12-01 |

因此 pipeline 统一使用一个**参考日期 `AS_OF_DATE` = 2025-12-01**，即最后一笔订单的日期。晚于它的记录会被拒绝：23,189 条 clickstream 事件和 1,845 张工单。行为指标只看**它之前的 90 天**（2025-09-02 到 2025-12-01），这是浏览数据和购买数据唯一重叠的时段。两个值都在 `spark_session.py` 里设定，可用环境变量覆盖。

**数据量说明。** 88 万行对单机 Spark 很合适：足够大到体现分布式处理的价值，又能在笔记本上跑。

### 3.1 脏数据清单

以下数字都是在数据上实测的，直接驱动 4.2 的规则。清单按「在哪个层级才能看到这个缺陷」来组织，因为逐列检查只能发现前两个层级。

| 层级 | 数据源 | 缺陷 | 数量 |
|---|---|---|---|
| 字段级 | clickstream | 无法解析的时间戳（`2024/31/01 25:61:00`） | 40,171 |
| 字段级 | 订单 | 无法解析的 `order_date` | 90,000 |
| 字段级 | 订单 | `order_amount` 带空格或逗号；转换后为 NaN | NaN 12,830 |
| 字段级 | 订单 | `payment_method` 脏值（`wall-et`、`crad`）中规则无法识别的 | 13,016 |
| 字段级 | 商品 | `category` 截断或乱码（`clo`、`3l3ctronics`） | 无法识别 30 |
| 字段级 | CRM | 姓名含标点（`Jamie!`） | 每个姓名字段约 3,100 |
| 字段级 | CRM | 姓名字符重复（`KKeevvin`、`Ppaarrrish`） | 每个姓名字段约 1,500 |
| 记录级 | 订单 | `order_amount` 非正 | 25,798 |
| 记录级 | 订单 | `quantity` 缺失或非正 | 45,651 |
| 记录级 | 商品 | `price` 非正或 NaN | 71 |
| 记录级 | 工单 | 无法解析的 `ticket_created` | 7,179 |
| 记录级 | 工单 | 解决时间早于创建时间 | 1,323 |
| 记录级 | 工单 | `resolution_time_hours` 与时间戳相差超过 1 小时 | 1,310 |
| 记录级 | clickstream | `customer_id` 缺失 | 150,553 |
| 实体级 | CRM | `customer_id` 重复：1,734 个 ID、1,800 条多余行，其中 481 个 ID 内容冲突 | 1,800 |
| 跨源 | clickstream、工单 | 晚于参考日期 | 23,189 / 1,845 |
| 跨源 | 订单、工单 | 早于客户注册日期 | 14,922 / 1,711 |
| 跨源 | 工单 | 解决时间晚于参考日期 | 1,414 |

### 3.2 已在数据上验证的结论

以下都是结论，不是待定问题，每一条都改变了 pipeline 做或不做的事。

- **共用联系方式的是不同的人，所以不合并 ID。** 有 450 个邮箱、500 个电话被多个 `customer_id` 共用。同一个地址下是互不相关的人：`shared1459@hotmail.com` 下有 Julie Woods、Robert Kelly、Jamie Black、Andrea Hale、Todd Harding，电话各不相同，注册日期从 2015 年到 2025 年。按邮箱合并会把他们合成一个人，所以 pipeline 只按 `customer_id` 去重。
- **源数据外键完整。** Bronze 层没有任何订单、工单或事件引用了不存在的客户或商品。
- **我们自己的隔离会制造孤儿。** 500 个商品里隔离了 90 个，导致 33,022 条订单没有有效商品，这些订单属于 42.3% 的购买客户。Gold 层把它们计入 `unknown` 类别，而不是丢弃。
- **`device_id` 不是关联键。** clickstream 的 305,114 个 device_id 中，出现在 CRM 里的是 0 个。
- **`session_id` 不代表会话。** 305,114 条事件只有 8,000 个不同的 session_id；每个平均被 38 个客户共用，没有一个只属于单个客户。它无法把一个客户的事件归组，所以没有任何指标使用它。

**关于数据真实性（写进报告的定调）。** 该数据集是"仿真但贴近真实"、且有意做脏的。我们把项目定调在**平台能力**（整合 + 治理）而非"真实商业洞察"上——因此其合成性质是优点（可控、可复现、富含数据质量挑战），而非缺点。

---

## 4. 方法与算法（Methodology and Algorithm）

采用 **Medallion 架构**（Bronze → Silver → Gold）这一标准分层湖仓模式，并在其上叠加分析方法。所有方法均不依赖 ML；ML 是可选附加项。

### 4.1 Bronze — 原始落地
- 将每个源文件原样摄取为 Iceberg 表，按源对齐，所有列保持字符串。
- 只做两件事：列名规范成 snake_case（`device_id(s)` → `device_ids`），加审计列 `_source`、`_ingest_ts`、`_ingest_run_id`。
- 每个源的行数和耗时追加到 `metadata.ingestion`，一次运行的三个阶段共用同一个 `run_id`。

### 4.2 Silver — 清洗、校验、去重

检查分四个层级，与 3.1 对应。

| 层级 | 检查什么 | 例子 |
|---|---|---|
| 字段级 | 单个单元格里的值：解析、规范化 | 用 `try_*` 解析时间戳和数字；类别、状态、情绪映射到标准值；姓名去标点并首字母大写；`device_ids` 拆成 `silver.customer_device` |
| 记录级 | 单独一行自身 | 主键非空；金额和价格为正；NaN；解决时间不早于创建时间；数量 |
| 实体级 | 描述同一对象的多行 | 每个源按主键一行；每个 CRM 客户一条黄金记录 |
| 跨源 | 一行与另一个数据源对照 | 晚于参考日期；早于客户注册；各关联关系的孤儿数 |

**两个严重级别。** 规则要么**拒绝**，把这一行连同 `_err` 里的原因移入 `quarantine.<源>`；要么**警告**，保留这一行，把原因记在 `_warn` 里。会导致指标出错的行被拒绝；不影响任何指标、或者数据本身说不清哪边有错的缺陷只警告。

| 拒绝 | 警告 |
|---|---|
| 主键缺失、日期和金额无法解析、NaN、金额和价格非正、解决早于创建、晚于参考日期、主键重复 | 数量缺失或非正（随后置为 null）、订单或工单早于注册、支付方式无法识别、姓名字符重复、工单在参考日期之后才解决、解决时长不一致 |

订单和工单早于注册只警告，因为游客下单后再注册很常见，出错的可能是注册日期那一边。数量只警告，因为没有任何指标用到它，为此拒绝四分之一的订单会扭曲所有购买指标。

**黄金记录。** 同一个 `customer_id` 有多条 CRM 记录时，保留警告最少的那条；并列时取资料最完整的；再并列时取业务列哈希值最小的。最后这条保证重跑时选中同一行。实际效果是保留 `Parrish` 而不是 `Ppaarrrish`，因为损坏的姓名会触发警告。

**框架。** 一条规则是一个 `(名称, 条件)` 对；每个数据源声明自己的拒绝规则列表和警告规则列表。`dedup(df, key, order_by)` 用窗口函数实现实体级检查。跨源检查先把另一个数据源需要的列 join 进来，这样它仍然是普通的单表规则。

**产出。** `silver.<源>` 存有效行及其 `_warn` 数组。`metadata.dq_summary` 按运行、数据源、严重级别和原因各一行。`metadata.integrity` 按每条关联关系，分别记录相对 Bronze 父表和相对 Silver 父表的孤儿数；两者之差就是我们自己的隔离造成的孤儿。

### 4.3 Gold — 整合成 Customer 360

所有与时间有关的指标都从 `AS_OF_DATE` 算起；行为指标覆盖它之前的 90 天。

**`customer_360`，每个客户一行（48,200 行）。**

| 分组 | 列 | 来源 |
|---|---|---|
| 画像 | customer_id, full_name_masked, state, country, gender, source, tenure_days | CRM |
| 购买 | recency_days, frequency, monetary, segment, bad_orders, favourite_category | 订单、商品 |
| 客服 | tickets_total, open_tickets, negative_tickets, last_issue_type | 工单 |
| 近 90 天行为 | events_90d, cart_events_90d, last_event_time | clickstream |
| 派生 | risk_score, risk_level, priority | 以上全部 |

邮箱、电话、地址不会进入这张表。没有订单的客户保留，分群记为 `No Purchase`；计数列缺失时为 0。

**分析表。**

- **RFM 分群**：按成功订单计算每个客户的 recency、frequency、monetary，各用 `NTILE(5)` 窗口函数打成五分位，再按组合映射到五个分群。
- **流失风险**：距上次下单的时长、退款或失败订单、负面工单三项各 0 到 2 分，总分映射成四个等级。
- **营销优先级**：消费金额五分位与风险分组合。
- **品类渗透率**：每个品类的购买客户数，商品被隔离的订单计入 `unknown`。
- **转化漏斗**：90 天窗口内 page_view → search → add_to_cart → purchase。每一步与上一步的客户集合取交集，所以人数必然递减，步间差值就是流失。
- **加购未下单**：窗口内加过购物车、但窗口内没有成功订单的客户。

### 4.4 可选 — 预测组件（ML）
*仅在时间允许时加入；是加分项，非必需。*
- **任务：** 二分类——预测客户是否会流失。
- **标签：** 数据本身没有标签。参考日期为 2025-12-01、预测窗口 90 天，所以截止日期是 2025-09-02；截止日到参考日期之间没有下单的客户记为流失。
- **特征：** 只用截止日之前的数据：订单、工单和 CRM。clickstream 恰好从截止日才开始，无法提供特征。
- **方法：** Spark MLlib `Pipeline`——`VectorAssembler` → `StringIndexer`/`OneHotEncoder` → `StandardScaler` → 分类器（如 `LogisticRegression` 或 `RandomForestClassifier`）→ `Evaluator`（AUC）。

### 4.5 服务/呈现
- 分析结果通过 Spark SQL 和 Trino 对外提供。
- 六个 Superset dashboard，规格见 `dashboard_blueprint_CN.md`。

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

## 6. 存储与命名规范

- **桶：** MinIO 里的 `s3://warehouse/`，沿用 Iceberg quickstart 的默认值。
- **表命名：** catalog 为 `demo`，每层一个 namespace：`demo.bronze.orders`、`demo.silver.orders`、`demo.quarantine.orders`、`demo.gold.customer_360`。namespace 由 `lakehouse_io.ensure_namespaces()` 自动创建。
- **运行元数据** 放在 `demo.metadata`，每次运行追加一批，以 `run_id` 区分：`ingestion`（每个源的行数和耗时）、`dq_summary`（每个源、严重级别、原因的行数）、`integrity`（每条关联关系的孤儿数）。
- **分区：** 大事实表/行为表按日期分区（如 `order_date` 按月）；小维表（product_catalog、customers）**不分区**——基数低，分区会造成小文件开销。
- **公共数据模型：** ISO-8601 UTC 时间戳；snake_case 列名；显式类型；规则结果存为 `_err` 和 `_warn` 数组。

---

## 7. 仓库结构

```
customer360-lakehouse/          # 位于 WSL 内的 ~/customer360-lakehouse
├── README.md                 # 如何运行（满足交付要求）
├── requirements.txt          # pyspark==3.5.5，与容器内版本一致
├── run_pipeline.sh           # Bronze → Silver → Gold，共用一个 RUN_ID
├── .gitignore                # 忽略 data/*.csv、warehouse/、.env
├── .env.example              # 可识别的环境变量，含 AS_OF_DATE
├── docs/
│   ├── design_document.md    # 本文档（英文版，另有中文版）
│   ├── dashboard_blueprint.md# dashboard 搭建规格（另有中文版）
│   └── run_log.md            # 每次 pipeline 运行的计划和结果
├── infra/
│   ├── docker-compose.yml    # MinIO + mc + Spark + REST catalog（Trino、Superset 待加）
│   ├── LICENSE.upstream      # 所扩展的 quickstart 的 Apache-2.0 许可证
│   └── README.md             # 对上游的改动、端口、启动方式
├── src/
│   ├── spark_session.py      # Spark 配置、AS_OF_DATE、行为窗口、RUN_ID
│   ├── lakehouse_io.py       # 两种模式的统一读写层
│   ├── bronze_ingest.py      # 通用摄取 → Bronze
│   ├── silver_clean.py       # 四层清洗、黄金记录、隔离、运行元数据
│   ├── gold_metrics.py       # Customer 360 + 6 张分析表
│   ├── dq/framework.py       # 规则、警告、去重、汇总
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
- 按数据源、严重级别和原因统计的 DQ 数字（`metadata.dq_summary`）。
- 清洗前后各关联关系的孤儿数（`metadata.integrity`）。
- （若对事实表比较两种分区策略）摄取时间、存储、查询延迟、文件数。
- （若做 ML）在留出测试集上的模型 AUC。

---

## 9. 实施计划

每一步都能独立验证，这样出问题能定位到具体是哪一步。状态截至本文档最后一次修改。

### 步骤 1 — 环境 · 已完成

笔记本上的 WSL2（Ubuntu 24.04），Docker Engine 29.8.1、Compose v5.5.1。`cd infra && docker compose up -d` 启动四个服务：`spark-iceberg`、`rest`、`minio`、`mc`。重启后表不会丢失（见第 2 节）。

### 步骤 2 — 在 Iceberg 上跑通 pipeline · 已完成

```bash
cd infra && docker compose up -d
docker compose exec spark-iceberg bash
cd /home/iceberg/project
export LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data
./run_pipeline.sh
```

第一次运行暴露了三个缺陷，在 Run 2 修复：NaN 金额同时绕过了解析检查和正数检查；商品类别在 8 个类别的目录里残留了 23 个不同取值；漏斗的步骤之间没有包含关系。

### 步骤 2b — 补齐 Customer 360 · 已完成

对照第 1 节的三条性质做了一次审计，结论是 pipeline 一条都没做到：50,000 行对应 48,200 个客户；`customer_360` 里没有任何行为或客服列；没有参考日期，漏斗拿三个月的浏览数据去比两年的订单。Run 3 补上了 4.2 的实体级和跨源检查、参考日期与窗口，以及完整的 `customer_360`。

Run 3 耗时 4 分 18 秒。880,500 行源数据产出有效行 557,646、隔离行 322,854（36.7%）。

| 数据源 | 有效 | 隔离 | 隔离率 |
|---|---|---|---|
| 订单 | 182,946 | 117,054 | 39.0% |
| clickstream | 305,114 | 194,886 | 39.0% |
| 工单 | 20,976 | 9,024 | 30.1% |
| 商品 | 410 | 90 | 18.0% |
| CRM | 48,200 | 1,800 | 3.6% |

13 项检查全部通过：`customer_360` 行数 = 不同 ID 数 = Silver CRM 行数（48,200）；黄金记录保留了 `Parrish`；Silver 中没有晚于参考日期的数据；每个源 Silver + 隔离 = Bronze；漏斗递减；品类渗透率有 `unknown` 类；99.8% 的客户在窗口内有行为数据。完整数字见 `run_log.md`。

### 步骤 3 — 与本地基线交叉验证 · 下一步

用 `LAKEHOUSE_MODE=local` 和 `LOCAL_OUT=./warehouse` 再跑一遍，同一套代码改写 Parquet。完成标准：行数和 DQ 比例与步骤 2b 完全一致。对不上说明存储抽象层有 bug，而不是业务逻辑有问题。

### 步骤 4 — 加 Trino

在 `infra/docker-compose.yml` 里加一个 `trino` 服务，配置 Iceberg catalog 指向 REST catalog（`http://rest:8181`）和 MinIO（`http://minio:9000`）。完成标准：在 Trino CLI 里执行 `SELECT * FROM iceberg.gold.customer_360 LIMIT 10` 能返回数据。新增的服务要写进 compose 顶部注释的改动清单，这是上游 Apache-2.0 许可证的要求。用 Trino 的文件式访问控制隐藏 `silver.crm` 和 `quarantine.crm`，因为它们存有未脱敏的邮箱、电话和地址；其他 Silver 表不含个人信息，部分 dashboard 需要读取它们。

### 步骤 5 — 加 Superset，做 dashboard

在 compose 里加 `superset`，安装 Trino 驱动，连上 Trino。然后按 `dashboard_blueprint_CN.md` 的规格做六个面板，那份文档为每张图指明了数据源表和字段。

完成标准：每个 dashboard 都能渲染出来。做完要把 dashboard 从 Superset 导出到仓库里，这样别人部署一套新的也能导入——dashboard 存在 Superset 自己的数据库里，不导出就丢了。

### 步骤 6 — 流失分类器（可选）

只在步骤 1 到 5 都完成且还有时间时做。标签和特征窗口见 4.4，用 AUC 汇报效果。

### 步骤 7 — 整合与交付

全新 clone 一份，执行 `docker compose up -d` 和 `./run_pipeline.sh`，必须能复现报告里的每一个数字。然后写 README、两页报告和运行说明。

### 分工

步骤 2、3 必须在步骤 4、5 之前完成，因为 Trino 和 Superset 要有 Gold 表才能读。步骤 3 之后，查询和可视化这条线可以和可选的模型并行推进。

| 负责内容 | 负责人 | 依赖 |
|---|---|---|
| 摄取与 Bronze | `[姓名]` | — |
| 清洗、DQ 规则、隔离、黄金记录 | `[姓名]` | Bronze |
| Gold 整合与指标 | `[姓名]` | Silver |
| Trino、Superset、dashboard | `[姓名]` | Gold |
| 报告与可复现性 | `[姓名]` | 全部 |

---

## 10. 待办项

1. **数据集许可证** — 在 Kaggle 页面上确认是否允许课程使用，并在文档中引用。
2. `【待确认】` **分工** — 把上面表格里的负责人填上。
3. `【待确认】` **examiner 确认** — 确认「扩展官方 Iceberg quickstart」的做法可接受，以及如何注明。改动清单已经记在 compose 顶部注释和 `infra/README.md` 里。
4. **会话重建（可选）** — `session_id` 无法归组事件（见 3.2）。可以按每个客户的事件时间重建会话：两次事件间隔超过 30 分钟就算新会话。这是一个标准的 Spark 窗口任务，能让 Dashboard 5 有一张真正的会话深度图。
5. **扩容测试（可选）** — 用新 ID 复制出几 GB 的数据，测量摄取和清洗的扩展性。
6. **可选组件** — ML 分类器等步骤 5 完成后再定。

---

## 11. 一页 Proposal 骨架（用于提交）

> 把以上压缩成约 1 页，按要求的四个标题：

**1. Problem Description.** 客户数据分散在各系统（CRM/交易/行为），缺乏统一视图，团队无法识别风险客户或理解流失动因。我们构建一个湖仓平台，把异构客户数据整合成受治理的 Customer 360 视图，并做客户分群与流失/风险分析。

**2. Tools.** MinIO（S3 存储）· Apache Iceberg（表格式）· Apache Spark/PySpark（处理）· Iceberg REST catalog · Trino（查询）· Superset（可视化）· Docker Compose。

**3. Data.** 五个异构源文件（约 88 万行 / 180 MB），代表不同系统：Web/App clickstream（50 万）、orders（30 万）、CRM 客户（5 万）、support tickets（3 万）、product catalog（500），通过 customer_id 和 product_id 关联。脏数据既有值层面的（非法时间戳、非标准类别、负价格），也有跨记录、跨源层面的（重复客户记录、各源覆盖的时间段不同）。静态 CSV 放在只读落地目录，Bronze 及以上为 Iceberg 表。`【确认数据集许可证是否允许课程使用】`

**4. Methodology and Algorithm.** Medallion 架构（Bronze→Silver→Gold）：通用摄取；以统一参考日期为基准，在字段、记录、实体三个层级清洗，被拒绝的行进入隔离表并报告原因；把五个源整合成每个客户一行的 Customer 360 表；RFM 分群、规则化流失打分，以及在浏览和购买数据重叠的时段内计算漏斗和加购未下单。可选：Spark MLlib 流失分类器，标签定义为参考日期之前 90 天内是否下单，用 AUC 评估。结果经 Trino 提供，在 Superset 中可视化。
