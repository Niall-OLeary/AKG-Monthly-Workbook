# Automate Monthly Workbook

## Description
This project automates the creation of the monthly performance workbook for all AKG sites.  
It aggregates and calculates key metrics, generates presentation tables, and feeds data into Power BI for visualization.  
The solution replaces a manual 2–3 day process with a fully automated pipeline that now runs in just 2.5 minutes.

## Current Business Problem
Previously, compiling the monthly workbook required:  
- Manual aggregation of site performance data  
- Calculation of metrics and targets based on DWP starts waterfall  
- Visualization in Power BI  
- The process took **2–3 days** and was prone to delays and human error  

## Solution
The project provides a **fully automated SQL pipeline** that:  

1. **Aggregates Data**  
   - Collects performance data from all AKG sites across multiple metrics.

2. **Calculates Targets**  
   - Generates targets using the DWP starts waterfall methodology.

3. **Creates Presentation Tables**  
   - Populates tables specifically designed for Power BI visualization.  

4. **Automation**  
   - Uses **Snowflake tasks** to run the pipeline automatically at the beginning of each month.  
   - Stores all historical workbooks back to **July 2021**, allowing the business to access past reports.  

5. **Power BI Integration**  
   - Presentation tables are fed into Power BI for dashboards accessible across the business.

## Impact
- Reduced workbook compilation time from 2–3 days to **2.5 minutes**  
- Fully automated, reliable, and consistent reporting  
- Easy access to historical data since July 2021  
- Enables faster business decisions with up-to-date insights  
- Eliminates manual errors and saves significant analyst time 
