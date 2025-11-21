# Monthly Workbook Automation

## Description
This project automates the creation of the monthly performance workbook for all AKG sites.  
It aggregates and calculates key metrics, generates presentation tables, and feeds data into Power BI for visualisation.  
The solution replaces a manual 2–3 day process with a fully automated pipeline that now runs in under 1 minute.

## Current Business Problem
Previously, compiling the monthly workbook required:  
- Manual aggregation of site performance data from a raw data source   
- Calculation of metrics and targets based on DWP starts waterfall  
- Visualisation in Excel and copied to Powerpoint  
- The process took **2–3 days** and was prone to delays and human error  

## Solution
The project provides a **fully automated SQL pipeline** that:  

1. **Aggregates Data**  
   - Aggregates performance data from all AKG sites across multiple metrics and stores in a single table.
<details> 
<summary><strong>Aggregation Script</strong></summary>
   
```sql
create or replace task IN_MONTH_AGGREGATED_MILESTONES_TASK
    warehouse = compute_wh
    schedule = 'USING CRON 30 8 * * MON-FRI UTC'
    as
create or replace table DW.BI.IN_MONTH_AGGREGATED_MILESTONES
as(
select
site_name as Site,
YEAR(start_date) as Year,
MONTH(start_date) as Month,
case when PO_DATE >= '2024-07-01' then 'EX' else 'OG' end as Contract,
'Starts' as Milestone_Type,
COUNT(DS.participant_id) as Count

from DW.STAGING.DR

left join DW.STAGING.DS on DS.participant_id = DR.participant_id

where  DR.participant_id is not null and site_name is not null and start_date is not null and actual_start = 'Start'
and site_name not in ('Restricted Area', 'TEST LOCATION ONLY','AKG Atherstone','CSC Cold Referrals')
group by 1,2,3,4

UNION ALL

select
site_name as Site,
YEAR(MILESTONE_JOB_OUT_DATE) as Year,
MONTH(MILESTONE_JOB_OUT_DATE) as Month,
case when PO_DATE >= '2024-07-01' then 'EX' else 'OG' end as Contract,
'JO' as Milestone_Type,
COUNT(DS.participant_id) as Count

from DW.STAGING.DR

left join DW.STAGING.DS on DS.participant_id = DR.participant_id

where MILESTONE_JOB_OUT = 'Yes' and DR.participant_id is not null and site_name is not null and start_date is not null and actual_start = 'Start'
and site_name not in ('Restricted Area', 'TEST LOCATION ONLY','AKG Atherstone','CSC Cold Referrals')

group by 1,2,3,4

UNION ALL

select
site_name as Site,
YEAR(MILESTONE_COMM_EARN_DATE) as Year,
MONTH(MILESTONE_COMM_EARN_DATE) as Month,
case when PO_DATE >= '2024-07-01' then 'EX' else 'OG' end as Contract,
'FE' as Milestone_Type,
COUNT(DS.participant_id) as Count

from DW.STAGING.DR

left join DW.STAGING.DS on DS.participant_id = DR.participant_id

where milestone_comm_earn = 'Yes' and DR.participant_id is not null and site_name is not null and start_date is not null and actual_start = 'Start'
and site_name not in ('Restricted Area', 'TEST LOCATION ONLY','AKG Atherstone','CSC Cold Referrals')

group by 1,2,3,4

UNION ALL

select
site_name as Site,
YEAR(first_time_job_start_date) as Year,
MONTH(first_time_job_start_date) as Month,
case when PO_DATE >= '2024-07-01' then 'EX' else 'OG' end as Contract,
'JS' as Milestone_Type,
COUNT(DS.participant_id) as Count

from DW.STAGING.DR

left join DW.STAGING.DS on DS.participant_id = DR.participant_id

where first_time_job_start = 'Yes' and DR.participant_id is not null and site_name is not null and start_date is not null and actual_start = 'Start'
and site_name not in ('Restricted Area', 'TEST LOCATION ONLY','AKG Atherstone','CSC Cold Referrals')

group by 1,2,3,4
order by 1,2,3,5
);
```

</details>  

2. **Calculates Targets**  
   - Generates targets using the DWP starts waterfall methodology across two distinct contracts (OG and EX).
<details> 
<summary><strong>Target Methodology Script</strong></summary>
   
```sql
create or replace table DW.BI.OG_SITE_WATERFALL
as(
select
Site,
s.Year,
s.Month,
og.in_month_year,
og.in_month_month,
s.Count * og.value as Value
from OG_WATERFALL og
left join IN_MONTH_AGGREGATED_MILESTONES s on og.cohort_year = s.Year and og.cohort_month = s.Month and s.Contract = 'OG' and s.Milestone_Type = 'Starts'
where Contract = 'OG' 
order by 1,2,3
);

create or replace table DW.BI.EX_SITE_WATERFALL
AS(
select
Site,
s.Year,
s.Month,
ex.in_month_year,
ex.in_month_month,
s.Count * ex.value as "Value"
from EX_WATERFALL ex
left join (
select 
Site,
Year,
Month,
Count,
from IN_MONTH_AGGREGATED_MILESTONES 
where Milestone_Type = 'Starts' and contract = 'EX' and DATE_FROM_PARTS(Year, Month, day(current_date)) < current_date
union
select 
site,
year as Year,
month as Month,
"Target" as Count
from SITE_FUTURE_STARTS_TARGETS
where DATE_FROM_PARTS(year, month, day(current_date)) >= current_date
order by 1,2,3
) as s on ex.cohort_year = s.Year and ex.cohort_month = s.Month 
order by 1,2,3,4,5
);


create or replace table DW.BI.OG_IN_MONTH_STATS
as(
with Pivoted_Milestones AS (
  SELECT
    Site,
    Year,
    Month,
    Contract,
    MAX(CASE WHEN Milestone_Type = 'Starts' THEN Count END)        AS Scheme_Starts,
    MAX(CASE WHEN Milestone_Type = 'JS'     THEN Count END)        AS JS_Actual,
    MAX(CASE WHEN Milestone_Type = 'FE'     THEN Count END)        AS FE_Actual,
    MAX(CASE WHEN Milestone_Type = 'JO'     THEN Count END)        AS Outcome_Actual
  FROM DW.BI.IN_MONTH_AGGREGATED_MILESTONES
  WHERE Contract = 'OG' 
  GROUP BY Site, Year, Month, Contract
)
select
c.Site,
c.year,
c.month,
p.Scheme_Starts as Scheme_Starts,
p.JS_Actual as JS_Actual,
((sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) = DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) <= DATE_FROM_PARTS(c.Year, c.month, 1) then w.Value else 0 end)
+
sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) < DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) = DATE_FROM_PARTS(c.Year, c.month, 1) then w.Value else 0 end))/0.64)/0.9 as JS_Target, 
p.FE_Actual as FE_Actual,
(sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) = DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) <= DATE_FROM_PARTS(c.Year, c.month, 1) then w.Value else 0 end)
+
sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) < DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) = DATE_FROM_PARTS(c.Year, c.month, 1) then w.Value else 0 end))/0.64 as FE_Target, 
p.Outcome_Actual as Outcome_Actual,
sum(case when w.in_month_year = c.Year  and w.in_month_month = c.month then w.Value else null end) as Outcome_TRNO,
(sum(case when w.in_month_year = c.Year  and w.in_month_month = c.month then w.Value else null end)) * (0.25/0.34) as Outcome_MRNO,
'OG' as Contract

from DW.BI.CALENDAR c
LEFT JOIN Pivoted_Milestones p ON p.Site  = c.Site AND p.Year  = c.Year AND p.Month = c.Month
left join OG_SITE_WATERFALL w on c.Site = w.Site
where c.date >= '2021-01-07' and c.date < TO_CHAR(DATE_TRUNC('month', CURRENT_DATE), 'YYYY-DD-MM')
group by 1,2,3,4,5,7,9,12
order by 1,2,3
);

create or replace table DW.BI.EX_IN_MONTH_STATS
as(
WITH Pivoted_Milestones AS (
  SELECT
    Site,
    Year,
    Month,
    Contract,
    MAX(CASE WHEN Milestone_Type = 'Starts' THEN Count END)        AS Scheme_Starts,
    MAX(CASE WHEN Milestone_Type = 'JS'     THEN Count END)        AS JS_Actual,
    MAX(CASE WHEN Milestone_Type = 'FE'     THEN Count END)        AS FE_Actual,
    MAX(CASE WHEN Milestone_Type = 'JO'     THEN Count END)        AS Outcome_Actual
  FROM DW.BI.IN_MONTH_AGGREGATED_MILESTONES
  WHERE Contract = 'EX' 
  GROUP BY Site, Year, Month, Contract
)
select
c.Site,
c.year,
c.month,
p.Scheme_Starts as Scheme_Starts,
p.JS_Actual as JS_Actual,
((sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) = DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) <= DATE_FROM_PARTS(c.Year, c.month, 1) then w."Value" else 0 end)
+
sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) < DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) = DATE_FROM_PARTS(c.Year, c.month, 1) then w."Value" else 0 end))/0.64)/0.9 as JS_Target,  
p.FE_Actual as FE_Actual,
(sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) = DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) <= DATE_FROM_PARTS(c.Year, c.month, 1) then w."Value" else 0 end)
+
sum(case when DATE_FROM_PARTS(w.in_month_year, w.in_month_month, 1) < DATEADD(MONTH, 4, DATE_FROM_PARTS(c.Year, c.month, 1)) 
and DATE_FROM_PARTS(w.Year, w.Month, 1) = DATE_FROM_PARTS(c.Year, c.month, 1) then w."Value" else 0 end))/0.64 as FE_Target, 
p.Outcome_Actual as Outcome_Actual,
sum(case when w.in_month_year = c.Year  and w.in_month_month = c.month then w."Value" else null end) as Outcome_TRNO,
(sum(case when w.in_month_year = c.Year  and w.in_month_month = c.month then w."Value" else null end)) * (0.25/0.34) as Outcome_MRNO,
'EX' as "Contract"

from DW.BI.CALENDAR c
LEFT JOIN Pivoted_Milestones p ON p.Site  = c.Site AND p.Year  = c.Year AND p.Month = c.Month
left join EX_SITE_WATERFALL w on c.Site = w.Site
where c.date >= '2024-01-07' and c.date < TO_CHAR(DATE_TRUNC('month', CURRENT_DATE), 'YYYY-DD-MM')
group by 1,2,3,4,5,7,9,12
order by 1,2,3
);



create or replace table DW.BI.IN_MONTH_STATS_UNION
as(
select 
case when Site like '%AKG%' then 'AKG' else 'Delivery Partners' end as "Region",
case when Site is not null then 'CPA1B' else null end as "CPA1B",
case when site like '%Acorn%' then 'Acorn'
     when site like '%ACIS%' then 'Acis'
     when site like '%Business%' then 'B2B'
     when site like '%EADS%' then 'EADS'
     when site like '%First College%' then 'First College'
     when site like '%AKG%' then 'AKG'
     when site like '%PETXI%' then 'PETXI'
     when site like '%Twin%' then 'Twin'
     when site like '%Steadfast%' then 'Steadfast'
     when site like '%Workpays%' then 'Workpays'
     when site like '%Stoke Jets%' then 'Stoke Jets'
     when site like '%TCHC%' then 'TCHC'
     else 'ERROR' end as "Provider",
*
from OG_IN_MONTH_STATS
union
select 
case when Site like '%AKG%' then 'AKG' else 'Delivery Partners' end as "Region",
case when Site is not null then 'CPA1B' else null end as "CPA1B",
case when site like '%Acorn%' then 'Acorn'
     when site like '%Acis%' then 'Acis'
     when site like '%Business%' then 'B2B'
     when site like '%EADS%' then 'EADS'
     when site like '%First College%' then 'First College'
     when site like '%AKG%' then 'AKG'
     when site like '%PETXI%' then 'PETXI'
     when site like '%Twin%' then 'Twin'
     when site like '%Steadfast%' then 'Steadfast'
     when site like '%Workpays%' then 'Workpays'
     when site like '%Stoke Jets%' then 'Stoke Jets'
     when site like '%TCHC%' then 'TCHC'
     else 'ERROR' end as "Provider",
* 
from EX_IN_MONTH_STATS
);
```

</details>  

3. **Creates Presentation Tables**  
   - Populates tables specifically designed for Power BI visualisation.  
<details> 
<summary><strong>Presentation Script & Dashboard Screenshots</strong></summary>
   
```sql
create or replace table DW.BI.TEAM_VIEWER
as(
WITH AggregatedStats AS (
    SELECT
        s.site as Provider,
        DATE_FROM_PARTS(s.year, s.month, 1) AS "Date",
        s.year,
        s.month,
        SUM(Scheme_Starts) AS "Scheme_Starts",
        SUM(JS_ACTUAL) AS "JS_Actual",
        SUM(JS_TARGET) AS "JS_Target",
        SUM(FE_ACTUAL) AS "FE_Actual",
        SUM(FE_TARGET) AS "FE_Target",
        SUM(OUTCOME_ACTUAL) AS "Outcome_Actual",
        SUM(OUTCOME_TRNO) AS "Outcome_TRNO",
        SUM(OUTCOME_MRNO) AS "Outcome_MRNO",
        -- Precompute contributions
        SUM(JS_TARGET) / NULLIF(SUM(SUM(JS_TARGET)) OVER (PARTITION BY year, month), 0) AS "Expected_JS_Contribution",
        SUM(JS_ACTUAL) / NULLIF(SUM(SUM(JS_TARGET)) OVER (PARTITION BY year, month), 0) AS "Actual_JS_Contribution",
        
        SUM(FE_TARGET) / NULLIF(SUM(SUM(FE_TARGET)) OVER (PARTITION BY year, month), 0) AS "Expected_FE_Contribution",
        SUM(FE_ACTUAL) / NULLIF(SUM(SUM(FE_TARGET)) OVER (PARTITION BY year, month), 0) AS "Actual_FE_Contribution",
        
        SUM(OUTCOME_TRNO) / NULLIF(SUM(SUM(OUTCOME_TRNO)) OVER (PARTITION BY year, month), 0) AS "Expected_TRNO_Contribution",
        SUM(OUTCOME_ACTUAL) / NULLIF(SUM(SUM(OUTCOME_TRNO)) OVER (PARTITION BY year, month), 0) AS "Actual_TRNO_Contribution",
        
        -- Precompute performance metrics
        COALESCE(DIV0(SUM(JS_ACTUAL), SUM(JS_TARGET)), 0) AS "JS_Performance",
        COALESCE(DIV0(SUM(FE_ACTUAL), SUM(FE_TARGET)), 0) AS "FE_Performance",
        COALESCE(DIV0(SUM(OUTCOME_ACTUAL), SUM(OUTCOME_TRNO)), 0) AS "TRNO_Performance",
        COALESCE(DIV0(SUM(OUTCOME_ACTUAL), SUM(OUTCOME_MRNO)), 0) AS "MRNO_Performance"
    FROM
        IN_MONTH_STATS_UNION s
    GROUP BY
        s.site, s.year, s.month
),
RankedStats AS (
    SELECT
        *,
        -- JS Rolling Actual and Target
        SUM("JS_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "JS_Rolling_Three_Actual",
        SUM("JS_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "JS_Rolling_Three_Target",
        SUM("JS_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "JS_Rolling_Six_Actual",
        SUM("JS_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "JS_Rolling_Six_Target",

        -- FE Rolling Actual and Target
        SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Three_Actual",
        SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Three_Target",
        SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Six_Actual",
        SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Six_Target",
        SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Twelve_Actual",
        SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "FE_Rolling_Twelve_Target",

        -- Outcome Rolling Actual and Target
        SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Three_Actual",
        SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Three_Target",
        SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Six_Actual",
        SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Six_Target",
        SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Twelve_Actual",
        SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "Outcome_Rolling_Twelve_Target",

        -- JS Rolling Performance
        COALESCE(DIV0(SUM("JS_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
             SUM("JS_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)), 0) AS "JS_Rolling_Three",
        COALESCE(DIV0(SUM("JS_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
             SUM("JS_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)), 0) AS "JS_Rolling_Six",

        -- FE Rolling Performance
        COALESCE(DIV0(SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
             SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)), 0) AS "FE_Rolling_Three",
        COALESCE(DIV0(SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
             SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)), 0) AS "FE_Rolling_Six",
        COALESCE(DIV0(SUM("FE_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
             SUM("FE_Target") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)), 0) AS "FE_Rolling_Twelve",

        -- Outcome Rolling Performance
        COALESCE(DIV0(SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
             SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)), 0) AS "Outcome_Rolling_Three",
        COALESCE(DIV0(SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
             SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)), 0) AS "Outcome_Rolling_Six",
        COALESCE(DIV0(SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
             SUM("Outcome_TRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)), 0) AS "Outcome_Rolling_Twelve",

        -- MRNO Rolling Performance
        COALESCE(DIV0(
            SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
            SUM("Outcome_MRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
            ), 0) AS "MRNO_Rolling_Three",

        COALESCE(DIV0(
            SUM("Outcome_Actual") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
            SUM("Outcome_MRNO") OVER (PARTITION BY Provider ORDER BY year, month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
            ), 0) AS "MRNO_Rolling_Six"
    FROM
        AggregatedStats
)
SELECT
    *,
    -- JS, FE, and TRNO Performance Ranks
    RANK() OVER (PARTITION BY year, month ORDER BY "JS_Performance" DESC) AS "JS_Performance_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "FE_Performance" DESC) AS "FE_Performance_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "TRNO_Performance" DESC) AS "TRNO_Performance_Rank",

    -- Final ranks for rolling values
    RANK() OVER (PARTITION BY year, month ORDER BY "JS_Rolling_Three" DESC) AS "JS_Rolling_Three_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "JS_Rolling_Six" DESC) AS "JS_Rolling_Six_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "FE_Rolling_Three" DESC) AS "FE_Rolling_Three_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "FE_Rolling_Six" DESC) AS "FE_Rolling_Six_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "FE_Rolling_Twelve" DESC) AS "FE_Rolling_Twelve_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "Outcome_Rolling_Three" DESC) AS "Outcome_Rolling_Three_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "Outcome_Rolling_Six" DESC) AS "Outcome_Rolling_Six_Rank",
    RANK() OVER (PARTITION BY year, month ORDER BY "Outcome_Rolling_Twelve" DESC) AS "Outcome_Rolling_Twelve_Rank"
FROM
    RankedStats
ORDER BY
    Provider, year, month
);
```
<img width="1425" height="783" alt="{0F70C861-151C-4679-AE4C-20B5D0FE8F7B}" src="https://github.com/user-attachments/assets/434fe807-b70a-47e8-a3d6-020b6c0a5e9f" />  
<img width="1423" height="778" alt="{C6474689-DBA9-4872-ABD4-635A1E772849}" src="https://github.com/user-attachments/assets/86232866-79ed-4c1b-8219-094ec852277a" />  
<img width="1416" height="784" alt="{62E14A35-179A-4C75-99C7-07393E956CD3}" src="https://github.com/user-attachments/assets/00522e90-ae67-461e-a173-f8b702503bf1" />  

</details>  

4. **Automation**  
   - Uses **Snowflake tasks** to run the pipeline automatically.  
   - Stores all historical workbooks back to **July 2021**, allowing the business to access past reports in one place.  

## Impact
- Reduced workbook compilation time from 2–3 days to **under 1 minute**  
- Fully automated, reliable, and consistent reporting  
- Easy access to historical data   
- Enables faster business decisions with up-to-date insights  
- Eliminates manual errors and saves significant analyst time 
