# Dashboard 搭建规格

六个 dashboard 的搭建说明。每张图注明数据来源表、字段、算法和验收标准。
全部为 SQL 聚合，不涉及 ML。

数字取自 2026-09-25 的 Run 2，完整记录见 `run_log.md`。

---

## 开始之前

**前置条件（尚未完成）**

| 条件 | 状态 | 说明 |
|---|---|---|
| Gold 表已生成 | 完成 | 七张表都在 MinIO 上，见下方数据源清单 |
| Trino 服务 | **未完成** | 设计文档 §9 步骤 4，Superset 通过它查 Iceberg |
| Superset 服务 | **未完成** | 设计文档 §9 步骤 5 |
| `gold.dq_summary` 表 | **不存在** | Dashboard 6 需要，见该节说明 |

**可用数据源**

| 表 | 行数 | 主要字段 |
|---|---|---|
| `gold.customer_360` | 50,000 | customer_id, full_name_masked, state, country, gender, source, recency_days, frequency, monetary, segment, risk_level, risk_score, priority |
| `gold.customer_rfm` | 44,937 | customer_id, recency_days, frequency, monetary, R, Fs, Ms, segment |
| `gold.customer_risk` | 44,937 | customer_id, recency_days, segment, bad_orders, neg_tickets, risk_score, risk_level |
| `gold.campaign_priority` | 44,937 | customer_id, segment, Ms, risk_score, priority |
| `gold.category_penetration` | 8 | category, customers, penetration_pct |
| `gold.behavior_funnel` | 4 | step_no, step, customers |
| `gold.cart_abandoners` | 2,032 | customer_id, cart_events |
| `silver.orders` | 182,946 | 清洗后的订单，含 status, payment_method, order_date, order_amount |
| `silver.clickstream` | 321,343 | 清洗后的行为事件，含 event_type, event_time, session_id |
| `silver.crm` | 50,000 | 清洗后的客户主数据，含 signup_date, dob |
| `silver.product` | 410 | 清洗后的商品，含规范化后的 category, price |
| `metadata.ingestion` | 每次运行 5 行 | source, rows, seconds, ingested_at |

优先用 Gold 表。只有 Gold 层没覆盖的指标才去查 Silver 层，并在图表说明里注明原因。

**命名约定**：Superset 里的 dataset 名称与表名一致，例如 `gold_customer_360`。

---

## Dashboard 1 — 客户概览

回答「我们有哪些客户」。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 客户总数 | 数字卡片 | `gold.customer_360` | `count(*)`，预期 50,000 |
| 有成交客户数 | 数字卡片 | `gold.customer_360` | `count(*) where segment <> 'No Purchase'`，预期 44,937 |
| 注册渠道分布 | 饼图 | `gold.customer_360` | 按 `source` 分组计数 |
| 性别分布 | 饼图 | `gold.customer_360` | 按 `gender` 分组计数 |
| 国家分布 | 柱状图，取前 15 | `gold.customer_360` | 按 `country` 分组计数降序 |
| 注册趋势 | 折线图 | `silver.crm` | `signup_date` 按月聚合。Gold 层没有这个字段 |

**验收**：前两个卡片的数字与上表一致；没有任何图表显示 email、phone 或 address。

---

## Dashboard 2 — RFM 分群

回答「客户价值怎么分层」。

R、F、M 三个维度各按五分位打 1 到 5 分（`R`、`Fs`、`Ms` 列），再按组合规则划入五个分群。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 分群人数 | 柱状图 | `gold.customer_rfm` | 按 `segment` 分组计数降序 |
| 各分群平均消费 | 柱状图 | `gold.customer_rfm` | 按 `segment` 分组取 `avg(monetary)` |
| 各分群平均购买频次 | 柱状图 | `gold.customer_rfm` | 按 `segment` 分组取 `avg(frequency)` |
| 消费金额分布 | 直方图 | `gold.customer_rfm` | `monetary` 分箱 |
| 高价值客户占比 | 数字卡片 | `gold.customer_rfm` | `Ms = 5` 的客户占比，按定义为 20% |

Run 2 的分群人数：Champions 11,858、Hibernating 11,603、Loyal 8,730、Others 6,373、At Risk 6,373。

**验收**：五个分群人数相加等于 44,937；`monetary` 列没有 NaN。

---

## Dashboard 3 — 流失风险与营销优先级

回答「该先联系谁」。

风险分 = 沉默时长 + 退款或失败订单数 + 负面情绪工单数，三项各 0 到 2 分，合计 0 到 6 分，再映射成四个等级。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 风险等级分布 | 柱状图 | `gold.customer_risk` | 按 `risk_level` 分组计数 |
| 分群 × 风险交叉 | 堆叠柱状图 | `gold.customer_risk` | 按 `segment` 分组，`risk_level` 堆叠 |
| 风险分构成 | 柱状图 | `gold.customer_risk` | `avg(bad_orders)` 和 `avg(neg_tickets)` 按 `risk_level` 分组，看哪个信号在推高风险 |
| 营销优先级分布 | 柱状图 | `gold.campaign_priority` | 按 `priority` 分组计数 |
| 高优先级客户名单 | 表格 | `gold.customer_360` | `priority = 'HIGH'`，按 `monetary` 降序，列出脱敏姓名、分群、风险等级、金额 |

Run 2：风险等级 Medium 28,429、High 10,025、Low 4,750、Very Low 1,733；优先级 MEDIUM 28,763、HIGH 14,496、LOW 1,678。

**验收**：名单表格只出现 `full_name_masked`，不出现任何真实姓名。

---

## Dashboard 4 — 产品与品类渗透

回答「哪些品类卖得动，哪些客户还没被覆盖」。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 品类渗透率 | 柱状图 | `gold.category_penetration` | `penetration_pct` 按 `category` 降序 |
| 品类客户数 | 柱状图 | `gold.category_penetration` | `customers` 按 `category` 降序 |
| 品类销售额 | 柱状图 | `silver.orders` join `silver.product` | 按 `category` 汇总 `order_amount`，仅 `status = 'success'`。Gold 层没有这张表 |
| 支付方式分布 | 饼图 | `silver.orders` | 按 `payment_method` 分组计数，清洗后为 card / wallet / upi / cash / unknown |
| 复购与单次客户 | 饼图 | `gold.customer_rfm` | `frequency = 1` 与 `frequency > 1` 两类占比 |

Run 2 渗透率：clothing 30.8%、kitchen 29.6%、beauty 28.3%、home 26.7%、sports 25.0%、automotive 24.7%、electronics 24.5%、toys 23.0%。

**验收**：品类恰好八个。如果出现 `clo`、`clothing-` 这类值，说明用的是旧数据，需要重跑 pipeline。

---

## Dashboard 5 — 行为与转化

回答「客户从浏览到下单流失在哪一步」。

漏斗是嵌套的：每一步的客户集合都是上一步的子集，所以人数必然逐层递减，步与步之间的差就是流失。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 转化漏斗 | 漏斗图 | `gold.behavior_funnel` | `customers` 按 `step_no` 升序。**必须按 `step_no` 排序**，按名称排会乱 |
| 各步流失率 | 柱状图 | `gold.behavior_funnel` | 相邻两步的差值除以前一步 |
| 加购未下单客户 | 表格 | `gold.cart_abandoners` | 按 `cart_events` 降序，营销机会名单 |
| 活跃时段 | 热力图 | `silver.clickstream` | `event_time` 按星期 × 小时 聚合计数 |
| 会话深度 | 直方图 | `silver.clickstream` | 按 `session_id` 分组计数，再对事件数分箱 |

Run 2 漏斗：page_view 47,260 → search 29,643 → add_to_cart 18,745 → purchase 17,538，流失率 37.3%、36.8%、6.4%。

**验收**：漏斗四根柱子从左到右递减；加购未下单 2,032 人。

---

## Dashboard 6 — 数据质量

回答「清洗拦下了什么」。这是项目治理能力的直接展示。

**前置任务：Gold 层目前没有汇总表。** `quarantine.*` 表里的原因存在 `_err` 数组列中，Superset 无法直接分组。需要在 `silver_clean.py` 里把 `dq_summary()` 的结果写成一张 `gold.dq_summary` 表，字段为 `run_id, source, reason, count`。这项工作应在本 dashboard 之前完成。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 各源隔离率 | 柱状图 | `gold.dq_summary` | 每个源的隔离行数除以 Bronze 行数 |
| 隔离原因分布 | 堆叠柱状图 | `gold.dq_summary` | 按 `source` 分组，`reason` 堆叠 |
| 各层行数 | 漏斗图或柱状图 | 各层表 | Bronze 880,500 → Silver 576,197，被隔离 304,303 |
| 摄取耗时 | 柱状图 | `metadata.ingestion` | `seconds` 按 `source` 分组，多次运行取最新 `run_id` |

Run 2 隔离率：orders 39.0%、clickstream 35.7%、tickets 28.3%、product 18.0%、crm 0%。

**验收**：各源有效行数与隔离行数相加等于该源的 Bronze 行数。

---

## 客户钻取

所有 dashboard 共用的最后一层，数据源统一为 `gold.customer_360`。

从任意聚合图表点进去，看单个客户的明细：脱敏姓名、地区、注册渠道、RFM 三值、分群、风险等级和分数、营销优先级。

**只使用脱敏字段。** `gold.customer_360` 在生成时就已排除 email、phone、address，因此不存在误配置泄露 PII 的可能。这一点写进报告。

---

## 搭建顺序与分工

| 顺序 | 内容 | 依赖 | 负责人 |
|---|---|---|---|
| 1 | 加 Trino 服务，验证能查到 Gold 表 | Gold 表已生成 | `[姓名]` |
| 2 | 加 Superset，连上 Trino，建 dataset | Trino | `[姓名]` |
| 3 | Dashboard 2、3 | dataset | `[姓名]` |
| 4 | Dashboard 1、4 | dataset | `[姓名]` |
| 5 | 补 `gold.dq_summary` 表，做 Dashboard 6 | 改 silver_clean.py 后重跑 | `[姓名]` |
| 6 | Dashboard 5 | dataset | `[姓名]` |

Dashboard 2、3 排在最前，因为它们完全基于 Gold 表，不需要额外开发，能最快验证 Superset 到 Iceberg 这条链路是通的。

**做完必须导出。** Superset 的 dashboard 存在它自己的元数据库里，不在 Iceberg 中。导出成文件提交进仓库，否则换一台机器部署就全没了，交付要求的「可复现」也不成立。
