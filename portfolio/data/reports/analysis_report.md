# Analytics Report

**Generated:** 2026-04-08 14:49:42 UTC
**Records analysed:** 9,701

---

## Summary KPIs

| Metric | Value |
| --- | --- |
| Total Orders | 9,701 |
| Total Revenue | $98,251,001.52 |
| Avg Order Value | $10,127.93 |
| Max Order Value | $46,393.96 |
| Unique Customers | 999 |
| Unique Products | 8 |

## Revenue by Region

| region | order_count | total_revenue | avg_order_value | total_units |
| --- | --- | --- | --- | --- |
South | 1971 | 20038938.0 | 10166.888888888889 | 49082.0
West | 1973 | 19845192.0 | 10058.384186517993 | 49386.0
East | 1926 | 19663389.94 | 10209.444413291798 | 48690.0
North | 1961 | 19500894.89 | 9944.362514023458 | 48946.0
Central | 1870 | 19202586.69 | 10268.762935828878 | 46832.0

## Top Products

| product | order_count | total_revenue | avg_discount |
| --- | --- | --- | --- |
Bundle-2 | 1248 | 12844059.04 | 0.2014551282051282
Service-Pro | 1213 | 12623413.28 | 0.20104204451772464
Bundle-1 | 1227 | 12512453.03 | 0.19723064384678077
Widget-B | 1254 | 12495794.26 | 0.20341626794258374
Service-Lite | 1220 | 12233827.36 | 0.19928688524590163
Gadget-X | 1182 | 12052798.29 | 0.199414551607445
Widget-A | 1162 | 11916887.42 | 0.20214113597246128
Gadget-Y | 1195 | 11571768.84 | 0.20712133891213388

## Revenue by Channel

| channel | total_revenue | order_count |
| --- | --- | --- |
Direct | 25043829.83 | 2426
Online | 24954979.46 | 2468
Partner | 24830711.46 | 2455
Retail | 23421480.77 | 2352

## Monthly Revenue Trend (first 12 months)

| year_month | monthly_revenue | order_count |
| --- | --- | --- |
2022-01 | 6915134.13 | 712
2022-02 | 6559058.5 | 661
2022-03 | 7736218.62 | 746
2022-04 | 7240429.49 | 713
2022-05 | 8038671.89 | 738
2022-06 | 7037505.8 | 674
2022-07 | 7797544.59 | 775
2022-08 | 7813733.82 | 740
2022-09 | 6965744.4799999995 | 672
2022-10 | 6863215.61 | 696

## Revenue Anomalies (top 20)

| order_id | date | region | product | channel | quantity | unit_price | discount_pct | customer_id | revenue | year_month |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
ORD-004559 | 2022-09-28 03:00:00 | West | Service-Lite | Online | 49.0 | 953.49 | 0.007 | CUST-0425 | 46393.96 | 2022-09
ORD-002665 | 2022-08-12 12:00:00 | East | Gadget-Y | Direct | 48.0 | 961.02 | 0.0 | CUST-0868 | 46128.96 | 2022-08
ORD-006599 | 2022-01-12 00:00:00 | South | Service-Lite | Online | 48.0 | 963.97 | 0.004 | CUST-0926 | 46085.48 | 2022-01
ORD-001333 | 2022-12-03 10:00:00 | South | Gadget-X | Partner | 49.0 | 941.09 | 0.012 | CUST-0802 | 45560.05 | 2022-12
ORD-005023 | 2022-06-24 21:00:00 | East | Widget-B | Online | 48.0 | 978.13 | 0.032 | CUST-0894 | 45447.83 | 2022-06
ORD-008850 | 2022-04-16 23:00:00 | Central | Bundle-1 | Retail | 45.0 | 994.76 | 0.008 | CUST-0802 | 44406.09 | 2022-04
ORD-009550 | 2022-11-25 15:00:00 | North | Bundle-2 | Retail | 49.0 | 939.36 | 0.038 | CUST-0511 | 44279.55 | 2022-11
ORD-007501 | 2022-02-05 08:00:00 | West | Service-Pro | Retail | 48.0 | 937.93 | 0.024 | CUST-0407 | 43940.14 | 2022-02
ORD-001001 | 2022-11-03 23:00:00 | North | Gadget-X | Partner | 48.0 | 980.01 | 0.068 | CUST-0510 | 43841.73 | 2022-11
ORD-005584 | 2022-07-11 08:00:00 | Central | Widget-B | Partner | 49.0 | 998.91 | 0.115 | CUST-0576 | 43317.73 | 2022-07

## Pipeline Efficiency Benchmark

| Run | Baseline (s) | Optimized (s) |
| --- | --- | --- |
| 1 | 0.7756 | 0.0084 |
| 2 | 0.7641 | 0.0066 |
| 3 | 0.7587 | 0.0066 |

**Mean baseline:** 0.7661s
**Mean optimized:** 0.0072s
**Improvement: 99.1%** (✓ ≥30%)
