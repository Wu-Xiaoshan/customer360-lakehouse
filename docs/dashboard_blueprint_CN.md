# Dashboard 搭建规格

六个 dashboard 的搭建说明。每张图注明数据来源表、字段、算法和验收标准。
全部为 SQL 聚合，不涉及 ML。

数字取自 2026-09-26 的 Run 3，完整记录见 `run_log.md`。与时间有关的数字都从参考日期
2025-12-01 算起，行为数字覆盖它之前的 90 天。

---

## 开始之前

**前置条件**

| 条件 | 状态 | 说明 |
|---|---|---|
| Gold 表和元数据表 | 完成 | 见下方数据源清单 |
| Trino 服务 | **未完成** | 设计文档 §9 步骤 4，Superset 通过它查 Iceberg |
| Superset 服务 | **未完成** | 设计文档 §9 步骤 5 |
| 在 Trino 里隐藏 `silver.crm` 和 `quarantine.crm` | **未完成** | 这两张表存有未脱敏的邮箱、电话和地址 |

**可用数据源**

| 表 | 行数 | 主要字段 |
|---|---|---|
| `gold.customer_360` | 48,200 | customer_id, full_name_masked, state, country, gender, source, tenure_days, recency_days, frequency, monetary, segment, bad_orders, favourite_category, tickets_total, open_tickets, negative_tickets, last_issue_type, events_90d, cart_events_90d, last_event_time, risk_score, risk_level, priority |
| `gold.customer_rfm` | 44,937 | customer_id, recency_days, frequency, monetary, R, Fs, Ms, segment |
| `gold.customer_risk` | 44,937 | customer_id, recency_days, segment, bad_orders, neg_tickets, risk_score, risk_level |
| `gold.campaign_priority` | 44,937 | customer_id, segment, Ms, risk_score, priority |
| `gold.category_penetration` | 9 | category, customers, penetration_pct |
| `gold.behavior_funnel` | 4 | step_no, step, customers |
| `gold.cart_abandoners` | 20,652 | customer_id, cart_events |
| `silver.orders` | 182,946 | status, payment_method, order_date, order_amount, product_id |
| `silver.product` | 410 | product_id, category, price |
| `silver.clickstream` | 305,114 | event_type, event_time |
| `metadata.dq_summary` | 每次运行一批 | run_id, source, severity, reason, rows, source_rows |
| `metadata.integrity` | 每次运行一批 | run_id, relationship, child_rows, orphans_vs_bronze, orphans_vs_silver |
| `metadata.ingestion` | 每次运行一批 | run_id, source, rows, seconds, ingested_at |

优先用 Gold 表。只有 Gold 层没覆盖的指标才去查 Silver 层，并在图表说明里注明原因。
元数据表每次运行追加一批：一律按最新的 `run_id` 过滤。

clickstream 里的 `session_id` 和 `device_id` 都不是可用的关联键（见设计文档 3.2），
所以没有任何图表按它们分组。

**命名约定**：Superset 里的 dataset 名称与表名一致，例如 `gold_customer_360`。

---

## Dashboard 1 — 客户概览

回答「我们有哪些客户」。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 客户数 | 数字卡片 | `gold.customer_360` | `count(*)`，预期 48,200 |
| 有成交客户数 | 数字卡片 | `gold.customer_360` | `count(*) where segment <> 'No Purchase'`，预期 44,937 |
| 注册渠道分布 | 饼图 | `gold.customer_360` | 按 `source` 分组计数 |
| 性别分布 | 饼图 | `gold.customer_360` | 按 `gender` 分组计数 |
| 国家分布 | 柱状图，取前 15 | `gold.customer_360` | 按 `country` 分组计数降序 |
| 在网时长 | 直方图 | `gold.customer_360` | `tenure_days` 按年分箱 |

**验收**：两个数字卡片与上表一致；没有任何图表显示 email、phone 或 address。

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

Run 3 的分群人数：Champions 11,858、Hibernating 11,604、Loyal 8,731、Others 6,372、At Risk 6,372。

**验收**：五个分群人数相加等于 44,937；`monetary` 列没有 NaN。

---

## Dashboard 3 — 流失风险与营销优先级

回答「该先联系谁」。

风险分 = 距上次下单的时长 + 退款或失败订单数 + 负面情绪工单数，三项各 0 到 2 分，合计 0 到 6 分，再映射成四个等级。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 风险等级分布 | 柱状图 | `gold.customer_risk` | 按 `risk_level` 分组计数 |
| 分群 × 风险交叉 | 堆叠柱状图 | `gold.customer_risk` | 按 `segment` 分组，`risk_level` 堆叠 |
| 风险分构成 | 柱状图 | `gold.customer_risk` | `avg(bad_orders)` 和 `avg(neg_tickets)` 按 `risk_level` 分组 |
| 营销优先级分布 | 柱状图 | `gold.campaign_priority` | 按 `priority` 分组计数 |
| 高优先级客户名单 | 表格 | `gold.customer_360` | `priority = 'HIGH'`，按 `monetary` 降序：脱敏姓名、分群、风险等级、问题订单数、负面工单数、消费金额 |

Run 3：风险等级 Medium 28,462、High 9,976、Low 4,759、Very Low 1,740；优先级 MEDIUM 28,770、HIGH 14,486、LOW 1,681。

**验收**：名单表格只出现 `full_name_masked`，不出现任何真实姓名。

---

## Dashboard 4 — 产品与品类渗透

回答「哪些品类卖得动，覆盖了哪些客户」。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 品类渗透率 | 柱状图 | `gold.category_penetration` | `penetration_pct` 降序，`unknown` 单独显示，比如用灰色 |
| 最常购买品类 | 柱状图 | `gold.customer_360` | 按 `favourite_category` 分组计数 |
| 品类销售额 | 柱状图 | `silver.orders` left join `silver.product` | 按 `category` 汇总 `order_amount`，仅 `status = 'success'`，匹配不上的归 `unknown`；Gold 层没有销售额表 |
| 支付方式分布 | 饼图 | `silver.orders` | 按 `payment_method` 分组计数，清洗后为 card / wallet / upi / cash / unknown |
| 复购与单次客户 | 饼图 | `gold.customer_rfm` | `frequency = 1` 与 `frequency > 1` 两类占比 |

Run 3 渗透率：unknown 42.3%、clothing 30.8%、kitchen 29.6%、beauty 28.3%、home 26.7%、sports 25.0%、automotive 24.7%、electronics 24.5%、toys 23.0%。

`unknown` 是商品被隔离的那些订单（500 个商品中隔离了 90 个）。它覆盖了 42.3% 的购买客户，必须保留在图上：去掉它，就看不出商品目录里的一次拒绝会波及多远。

**验收**：恰好出现八个具名类别加一个 `unknown`。

---

## Dashboard 5 — 行为与转化

回答「客户从浏览到下单流失在哪一步」。

这里的所有图表都只覆盖 2025-09-02 到 2025-12-01，即参考日期之前的 90 天，也是浏览数据和购买数据唯一重叠的时段。漏斗是嵌套的：每一步的客户集合都是上一步的子集，所以人数必然逐层递减，步与步之间的差就是流失。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 转化漏斗 | 漏斗图 | `gold.behavior_funnel` | `customers` 按 `step_no` 升序。**必须按 `step_no` 排序**，按名称排会乱 |
| 各步流失率 | 柱状图 | `gold.behavior_funnel` | 相邻两步的差值除以前一步 |
| 加购未下单客户 | 表格 | `gold.cart_abandoners` | 按 `cart_events` 降序 |
| 活跃时段 | 热力图 | `silver.clickstream` | 2025-09-02 之后的 `event_time`，按星期 × 小时聚合计数 |
| 每位客户的事件数 | 直方图 | `gold.customer_360` | `events_90d` 分箱 |

Run 3 漏斗：page_view 47,055 → search 28,644 → add_to_cart 17,568 → purchase 5,320，流失率 39.1%、38.7%、69.7%。20,652 位客户加购后没有下单。

**验收**：漏斗四根柱子从左到右递减；加购未下单表格有 20,652 行。

---

## Dashboard 6 — 数据质量

回答「清洗层拒绝了什么、标记了什么、对下游造成了什么影响」。

每张图都按最新的 `run_id` 过滤。

| 图表 | 类型 | 数据源 | 字段与算法 |
|---|---|---|---|
| 各源隔离率 | 柱状图 | `metadata.dq_summary` | `severity = 'reject'` 的 `rows` 之和除以 `source_rows` |
| 拒绝原因分布 | 堆叠柱状图 | `metadata.dq_summary` | `severity = 'reject'`，按 `source` 分组，`reason` 堆叠 |
| 警告分布 | 堆叠柱状图 | `metadata.dq_summary` | `severity = 'warn'`，按 `source` 分组，`reason` 堆叠 |
| 清洗前后的孤儿数 | 分组柱状图 | `metadata.integrity` | 每条 `relationship` 的 `orphans_vs_bronze` 与 `orphans_vs_silver` 并列 |
| 摄取耗时 | 柱状图 | `metadata.ingestion` | `seconds` 按 `source` 分组 |

Run 3 隔离率：orders 39.0%、clickstream 39.0%、tickets 30.1%、product 18.0%、CRM 3.6%（全部是重复客户记录）。最多的几类警告：数量缺失 27,521、数量非正 18,130、下单早于注册 14,922。

孤儿数那张图应该放在最前面：每条关联关系相对 Bronze 都是 0 孤儿，但 orders → product 相对 Silver 有 33,022 条。源数据是完整的，这些孤儿是我们隔离 90 个商品之后对订单造成的影响。

**验收**：每个源被拒绝的行数与对应隔离表的行数一致。

---

## 客户钻取

所有 dashboard 共用的最后一层，数据源统一为 `gold.customer_360`。

从任意聚合图表点进去，看单个客户：脱敏姓名、地区、注册渠道、在网时长、RFM 三值和分群、问题订单数、最常购买品类、工单数和最近问题类型、近 90 天事件数和加购次数、风险等级和分数、营销优先级。

**只使用脱敏字段。** `gold.customer_360` 在生成时就已排除 email、phone、address，因此不存在误配置泄露 PII 的可能。这一点写进报告。

---

## 搭建顺序与分工

| 顺序 | 内容 | 依赖 | 负责人 |
|---|---|---|---|
| 1 | 加 Trino，隐藏 CRM 两张表，验证能查到 Gold 和元数据表 | Gold 表已生成 | `[姓名]` |
| 2 | 加 Superset，连上 Trino，建 dataset | Trino | `[姓名]` |
| 3 | Dashboard 2、3 | dataset | `[姓名]` |
| 4 | Dashboard 1、4 | dataset | `[姓名]` |
| 5 | Dashboard 6 | dataset | `[姓名]` |
| 6 | Dashboard 5 | dataset | `[姓名]` |

Dashboard 2、3 排在最前，因为它们直接读现成的 Gold 表，能最快验证 Superset 到 Iceberg 这条链路是通的。

**做完必须导出。** Superset 的 dashboard 存在它自己的元数据库里，不在 Iceberg 中。导出成文件提交进仓库，否则换一台机器部署就全没了，交付要求的「可复现」也不成立。
