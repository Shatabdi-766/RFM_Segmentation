-- Database Setup
CREATE DATABASE IF NOT EXISTS RFM_SALES;
USE RFM_SALES;
-- Data Exploration
SELECT 
    *
FROM
    sales_data_for_rfm_segmentation
LIMIT 5;
-- NO OF RECORDS
SELECT COUNT(ORDERNUMBER) as Total_Records
FROM sales_data_for_rfm_segmentation; 
-- NO OF DISTINCT CUSTOMER
SELECT COUNT(DISTINCT ORDERNUMBER) as Unique_customer
FROM sales_data_for_rfm_segmentation; 
-- Types of Status
select distinct status from sales_data_for_rfm_segmentation;
-- Number of Years
select distinct year_id from sales_data_for_rfm_segmentation;
-- Number of Products
select distinct PRODUCTLINE from sales_data_for_rfm_segmentation;
-- Number of Countries
select distinct COUNTRY from sales_data_for_rfm_segmentation;
-- Types of Dealsizw
select distinct DEALSIZE from sales_data_for_rfm_segmentation;
-- Types of Territories
select distinct TERRITORY from sales_data_for_rfm_segmentation;
-- First 5 order dates
SELECT str_to_date(`ORDERDATE_1`,'%d/%m/%y') AS DATE 
FROM sales_data_for_rfm_segmentation
LIMIT 5; 
-- Last Order Date
SELECT max(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS LASTDATE 
FROM sales_data_for_rfm_segmentation;
-- Earliest Order Date
SELECT MIN(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS EARLIESTDATE 
FROM sales_data_for_rfm_segmentation;
-- Date Range (Duration of Sales Data)
SELECT MIN(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS EARLIESTDATE 
FROM sales_data_for_rfm_segmentation;
-- Checking Year & Month
WITH CTE AS ( SELECT  
YEAR_ID, 
COUNT(distinct MONTH_ID) as Distinct_Month_Count
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 1 DESC)

SELECT * FROM CTE;

-- Max Revenue for Yearly Sales
WITH CTE AS ( SELECT  
YEAR_ID, 
COUNT(distinct MONTH_ID) as Distinct_Month_Count
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 1 DESC)

SELECT * FROM CTE;
-- Yearly Sales
SELECT YEAR_ID, ROUND(SUM(SALES),2) AS YEARLY_SALES
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 2 DESC;
