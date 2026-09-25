# Customer 360 Dashboard 蓝本（基于你的真实数据）

> 完全基于上传的五个文件的真实字段。每个图表都注明：用哪些字段、怎么算、放在哪个 dashboard。
> 全部为 SQL/规则聚合，**不需要 ML**。银行专属概念（AUM、分行）已替换为你数据支持的电商概念。

---

## 你的数据能算什么（字段核实后）

| 文件 | 可用于计算的关键字段 | 已确认的取值 |
|---|---|---|
| orders (30万) | order_amount, order_date, status, payment_method, product_id, quantity | 日期范围 2023-12 ~ 2025-12；status=success/refunded/failed（多种脏写法）；4 种支付方式 |
| clickstream (50万) | event_type, timestamp, customer_id, session_id | event_type = page_view / search / add_to_cart / login |
| tickets (3万) | issue_type, sentiment, ticket_created | issue_type = product/refund/delay/payment；sentiment = positive/negative/neutral |
| CRM (5万) | state, country, gender, source, signup_date, dob | source = referral/app/web；gender = F/M/O |
| product (500) | category, price | category = clothing/kitchen/beauty/automotive/toys/sports/home/electronics（含脏值） |

**关联键**：customer_id 串起 orders/clickstream/tickets/CRM；product_id 串 orders/product。

---

## Dashboard 1 — 客户概览（Customer Overview）

对应银行 repo 的"客户群 / 近期活跃"。

| 图表 | 类型 | 用什么字段 · 怎么算 |
|---|---|---|
| 总客户数 | 数字卡片 | CRM 去重 count(customer_id) |
| 近30天活跃客户 | 数字卡片 | orders/clickstream 中 max(date) 往前 30 天内有行为的 distinct customer_id |
| 客户按注册渠道 | 饼图 | CRM `source`（referral/app/web）分组计数 |
| 客户按性别 | 饼图 | CRM `gender`（F/M/O）分组计数 |
| 客户地理分布 | 地图 / 柱状图 | CRM `state` 或 `country` 分组计数（替代银行的"按分行"）|
| 注册趋势 | 折线图 | CRM `signup_date` 按月聚合 |

---

## Dashboard 2 — RFM 分群与客户价值（Segmentation）

对应银行的"RFM segments"。这是核心分析，纯 SQL 可做。

**先算每个客户的 R/F/M（Gold 层）：**
- **Recency** = 数据集最新日期 − 该客户最后一次成功下单日期（orders，status=success）
- **Frequency** = 该客户成功订单数（count）
- **Monetary** = 该客户成功订单总额（sum order_amount）

**再打分分群**（每个维度按分位数打 1-5 分，组合成分群）：

| 图表 | 类型 | 说明 |
|---|---|---|
| RFM 分群分布 | 柱状图/饼图 | Champions / Loyal / At Risk / Hibernating（按 RFM 组合规则划分）|
| 各分群平均消费 | 对比柱状图 | 每个分群的平均 Monetary |
| 各分群客户数 | 数字卡片组 | 每个分群多少人 |
| 高价值客户占比 | 数字卡片 | Monetary Top 20% 客户占比 |

> RFM 分群规则示例：R高+F高+M高 = Champions；R低+F高 = At Risk；R低+F低 = Hibernating。规则你们自己定，写进报告。

---

## Dashboard 3 — 流失风险与营销优先级（Churn Risk & Campaign）

对应银行的"Churn risk / Campaign priority"。规则打分，**不用 ML**。

**流失风险规则**（组合多个信号）：
- Recency 很大（很久没买）→ 风险↑
- 有 refund/failed 订单 → 风险↑
- 有 negative sentiment 工单（tickets）→ 风险↑
- 近期无 clickstream 活动 → 风险↑

| 图表 | 类型 | 用什么字段 · 怎么算 |
|---|---|---|
| 流失风险分布 | 柱状图 | 按规则打分后分 Very Low/Low/Medium/High 计数 |
| 各分群流失风险 | 堆叠柱状图 | RFM 分群 × 风险等级交叉 |
| 负面情绪工单占比 | 数字卡片 | tickets 中 sentiment=negative 的客户数（流失预警信号）|
| 营销优先级 | 柱状图 | 综合 RFM+风险，规则打 HIGH/MEDIUM/LOW（谁该优先联系）|
| 退款率高的客户 | 表格 | 按 refund 订单占比排序 |

---

## Dashboard 4 — 产品分析与渗透率（Product & Penetration）

对应银行的"Recommended product mix / Product penetration"。

| 图表 | 类型 | 用什么字段 · 怎么算 |
|---|---|---|
| 各品类销售额 | 柱状图 | orders join product → 按 category 汇总 order_amount（替代银行"按分行AUM"）|
| 各品类订单量 | 柱状图 | 按 category 计数订单 |
| 支付方式分布 | 饼图 | orders `payment_method` 分组（清洗后：cash/wallet/card/upi）|
| 品类渗透率 | 柱状图 | 每个品类"买过的客户数 / 总客户数" |
| 品类空白（推荐机会） | 表格 | 找"活跃但从没买过 X 品类"的客户 → 推荐目标 |
| 复购 vs 单次客户 | 饼图 | Frequency=1 vs >1 的客户占比 |

---

## Dashboard 5 — 行为分析（Behavioral，你独有的优势）

**这是银行 repo 没有的**——你有 clickstream 行为数据，能做它做不了的漏斗分析。

| 图表 | 类型 | 用什么字段 · 怎么算 |
|---|---|---|
| 行为事件分布 | 柱状图 | clickstream `event_type`（page_view/search/add_to_cart/login）计数 |
| 转化漏斗 | 漏斗图 | login → search → page_view → add_to_cart → （下单）逐层客户数 |
| 加购未下单客户 | 表格 | 有 add_to_cart 事件但无对应 success 订单的客户（营销机会）|
| 活跃时段热力 | 热力图 | clickstream `timestamp` 按小时/星期聚合 |
| 会话深度 | 柱状图 | 每个 session_id 的事件数分布 |

---

## Dashboard 6 — 数据质量（Data Quality，展示你的治理成果）

**强烈建议做**——直接可视化你 Silver 层 DQ 框架的成果，是很强的工程展示点。

| 图表 | 类型 | 用什么字段 · 怎么算 |
|---|---|---|
| 各源被隔离记录数 | 柱状图 | 各源 quarantine 表的行数 |
| 脏数据类型分布 | 柱状图 | 坏时间戳 / 负价格 / 缺失键 / 类别脏值 各多少 |
| 清洗前后对比 | 对比柱状图 | 每个字段清洗前脏值数 vs 清洗后 |
| 参照完整性缺口 | 数字卡片 | orders 中 product_id 不在 product 表的记录数 |

---

## 客户钻取（所有 dashboard 通用的最后一层）

对应银行的"Customer drill-down + 脱敏"。

- 从聚合指标点进去看**单个客户明细**
- 只用**脱敏字段**：`full_name_masked`（对 CRM first_name+last_name 做脱敏，如 `M*** D**`）、customer_id、分群、RFM 值、风险等级
- **不暴露** email、phone、address 等 PII —— 呼应你项目的"数据治理"主题

---

## 实现顺序建议

1. **先把 Gold 层的 customer_360 表和几张指标表算出来**（RFM、风险、渗透）——这是所有 dashboard 的数据源
2. Superset 连上 Gold 表 → 定义 dataset（写 SQL 一次）
3. 拖拽做图表 → 拼成上面 6 个 dashboard
4. 优先做 Dashboard 1/2/3（核心 Customer 360 故事），行为(5)和数据质量(6)是你的差异化亮点

**注意**：dashboard 是最后一公里，前面"清洗→整合→算指标"才是主体工作量。这些指标全部是 SQL 聚合，不需要 ML。
