# **RFM Segmentation**
RFM Segmentation with SQL This repository contains a project for customer segmentation using RFM (Recency, Frequency, Monetary) analysis. The project leverages SQL for data querying and manipulation, making it efficient for handling large datasets and performing complex calculations. 

### Key Features :
- **Data Preprocessing**: Clean and prepare transaction data for RFM analysis.
- **RFM Scoring**: Calculate Recency, Frequency, and Monetary scores for each customer.
- **Customer Segmentation**: Group customers into segments (e.g.,'Churned Customer', 'Slipping Away, Cannot Lose', 'New Customers', 'Potential Churners', 'Active', and 'Loyal'.).
- **Actionable Insights**: Provide recommendations for targeted marketing campaigns.

### Technologies Used :
- SQL
- MySQL
- Python

### Dataset :
The dataset used in this project is available in CSV format. You can download it [here](./Sales_Data_for_RFM_Segmentation.csv).

### Database Setup :
- Create a database named `RFM_SALES`.
  
To set up the database for this project, run the following SQL commands:

```sql
CREATE DATABASE IF NOT EXISTS RFM_SALES;
USE RFM_SALES;

```
- The dataset is imported and set up using Python :

 ```python
  
  # Importing Libraries
  import pandas as pd
  import mysql.connector
  import os

  # Load the dataset
  csv_files = [('Sales_Data_for_RFM_Segmentation.csv','Sales_Data_for_RFM_Segmentation')]

  # Connect to the MySQL database
  conn = mysql.connector.connect(
    host='localhost',
    user='your_username',
    password='your_pasword',
    database='RFM_SALES'
     )
   
   cursor = conn.cursor()

  # Folder containing the CSV files
  folder_path = 'Your_Folder_Path'

  def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

  for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path,encoding='iso-8859-1')
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

  # Close the connection
  conn.close()
  
  import mysql.connector

  db = mysql.connector.connect(host = 'localhost',
                            username = 'Your_username',
                            password = 'Your_password',
                            database = 'RFM_SALES')
  cur = db.cursor()
  ```

  ## Dataset Exploration :

  ```sql
  SELECT 
    *
  FROM
    sales_data_for_rfm_segmentation
  LIMIT 5;

  ```
 -- OUTPUT --
| ORDERNUMBER | QUANTITYORDERED | PRICEEACH | ORDERLINENUMBER | SALES  | ORDERDATE_1 | STATUS  | QTR_ID | MONTH_ID | YEAR_ID | PRODUCTLINE | MSRP | PRODUCTCODE | CUSTOMERNAME           | PHONE         | ADDRESSLINE1           | ADDRESSLINE2 | CITY        | STATE | POSTALCODE | COUNTRY | TERRITORY | CONTACTLASTNAME | CONTACTFIRSTNAME | DEALSIZE |
|-------------|-----------------|-----------|-----------------|--------|-------------|---------|--------|----------|---------|-------------|------|-------------|------------------------|---------------|------------------------|--------------|-------------|-------|------------|---------|-----------|-----------------|------------------|----------|
| 10107       | 30              | 95.7      | 2               | 2871   | 24/2/03     | Shipped | 1      | 2        | 2003    | Motorcycles | 95   | S10_1678    | Land of Toys Inc.      | 2125557818    | 897 Long Airport Avenue | NULL         | NYC         | NY    | 10022      | USA     | NULL      | Yu              | Kwai             | Small    |
| 10121       | 34              | 81.35     | 5               | 2765.9 | 7/5/2003    | Shipped | 2      | 5        | 2003    | Motorcycles | 95   | S10_1678    | Reims Collectables     | 26.47.1555    | 59 rue de l'Abbaye     | NULL         | Reims       | NULL  | 51100      | France  | EMEA      | Henriot         | Paul             | Small    |
| 10134       | 41              | 94.74     | 2               | 3884.34| 1/7/2003    | Shipped | 3      | 7        | 2003    | Motorcycles | 95   | S10_1678    | Lyon Souveniers        | +33 1 46 62 7555 | 27 rue du Colonel Pierre Avia | NULL | Paris       | NULL  | 75508      | France  | EMEA      | Da Cunha        | Daniel           | Medium   |
| 10145       | 45              | 83.26     | 6               | 3746.7 | 25/8/03     | Shipped | 3      | 8        | 2003    | Motorcycles | 95   | S10_1678    | Toys4GrownUps.com      | 6265557265    | 78934 Hillside Dr.     | NULL         | Pasadena    | CA    | 90003      | USA     | NULL      | Young           | Julie            | Medium   |
| 10159       | 49              | 100       | 14              | 5205.27| 10/10/2003  | Shipped | 4      | 10       | 2003    | Motorcycles | 95   | S10_1678    | Corporate Gift Ideas Co. | 6505551386 | 7734 Strong St.        | NULL         | San Francisco | CA    | NULL       | USA     | NULL      | Brown           | Julie            | Medium   |

### Dataset Description :
- **ORDERNUMBER**: Unique identifier for each order.
- **QUANTITYORDERED**: Number of units ordered.
- **PRICEEACH**: Price per unit.
- **SALES**: Total sales amount (QUANTITYORDERED * PRICEEACH).
- **ORDERDATE_1**: Date of the order.
- **STATUS**: Current status of the order (e.g., Shipped).
- **PRODUCTLINE**: Category of the product (e.g., Motorcycles).
- **CUSTOMERNAME**: Name of the customer.
- **DEALSIZE**: Size of the deal (e.g., Small, Medium).

  
## Total Number of Records :
  ```sql
  -- NO OF RECORDS
  SELECT COUNT(ORDERNUMBER) as Total_Records
  FROM sales_data_for_rfm_segmentation; 
   ```
  --Output--
  | Total_Records |
  |----------|
  | 2823     |
  
- The dataset contains a total of 2,823 records based on the ORDERNUMBER field. This indicates there are 2,823 orders in the sales data, which can be used as the foundation for further RFM (Recency, Frequency, Monetary) segmentation analysis.

## Checking unique values :

## Number of Unique Customers :
```sql
-- NO OF DISTINCT CUSTOMER
SELECT COUNT(DISTINCT ORDERNUMBER) as Unique_customer
FROM sales_data_for_rfm_segmentation; 
```
--Output--
| Unique_customer |
|----------|
| 307    |

- There are 307 unique customers in the dataset, as identified by distinct ORDERNUMBER values. This suggests that the sales data represents transactions from 307 individual customers, which is a key starting point for understanding customer behavior and segmentation.

  
## Types of Status :
```sql
select distinct status from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| status     |
|------------|
| Shipped    |
| Disputed   |
| In Process |
| Cancelled  |
| On Hold    |
| Resolved   |

## Number of Years :
```sql
select distinct year_id from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| year_id |
|---------|
| 2003    |
| 2004    |
| 2005    |

- The dataset spans 3 distinct years: 2003, 2004, and 2005. This provides a multi-year view of sales data, which is useful for analyzing trends, seasonality, and year-over-year performance.


## Number of Products :
```sql
select distinct PRODUCTLINE from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| PRODUCTLINE      |
|------------------|
| Motorcycles      |
| Classic Cars     |
| Trucks and Buses |
| Vintage Cars     |
| Planes           |
| Ships            |
| Trains           |

- The dataset includes 7 distinct product lines. This diversity in product lines offers a comprehensive view of the business's offerings.
  
## Number of Countries :
```sql
select distinct COUNTRY from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| COUNTRY     |
|-------------|
| USA         |
| France      |
| Norway      |
| Australia   |
| Finland     |
| Austria     |
| UK          |
| Spain       |
| Sweden      |
| Singapore   |
| Canada      |
| Japan       |
| Italy       |
| Denmark     |
| Belgium     |
| Philippines |
| Germany     |
| Switzerland |
| Ireland     |

- The dataset covers sales across 19 distinct countries, including major markets like the USA, France, UK, Germany, and Japan, as well as smaller markets like Norway, Singapore, and Philippines. This global presence provides an opportunity to analyze regional performance, identify high-performing markets, and explore potential growth areas.

 ## Types of Dealsize: 

```sql
select distinct DEALSIZE from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| DEALSIZE |
|----------|
| Small    |
| Medium   |
| Large    |

- The dataset categorizes deals into 3 distinct sizes. This segmentation allows for analysis of sales performance based on deal size, helping to identify trends, profitability, and customer preferences across different transaction scales.

 ## Types of Territories : 

```sql
select distinct TERRITORY from sales_data_for_rfm_segmentation;
```
-- OUTPUT --
| TERRITORY |
|-----------|
| NA        |
| EMEA      |
| APAC      |
| Japan     |

- The dataset is segmented into 4 distinct territories:
1. NA (North America)

2. EMEA (Europe, Middle East, and Africa)

3. APAC (Asia-Pacific)

4. Japan

- This territorial breakdown provides a clear structure for analyzing regional sales performance and identifying growth opportunities in specific markets.

## First 5 Order Dates :
```sql
SELECT str_to_date(`ORDERDATE_1`,'%d/%m/%y') AS DATE 
FROM sales_data_for_rfm_segmentation
LIMIT 5; 

```
--Output--
|  DATE      |
|------------|
| 2003-02-24 |
| 2003-05-07 |
| 2003-07-01 |
| 2003-08-25 |
| 2003-10-10 |

- The first 5 order dates in the dataset provide a snapshot of the earliest transactions recorded. These dates can help identify the starting point of the sales data and set the context for analyzing trends over time.

  ## Last Order Date :
  ```sql
  SELECT max(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS LASTDATE 
  FROM sales_data_for_rfm_segmentation;
  ```
 --Output--
| LASTDATE   |
|------------|
| 2005-05-31 |
 
## Earliest Order Date :
```sql
SELECT MIN(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS EARLIESTDATE 
FROM sales_data_for_rfm_segmentation;
```
--Output--
| EARLIESTDATE  |
| ----------    |
| 2003-01-06    |

- The earliest order date in the dataset marks the beginning of the recorded sales history. This date is essential for understanding the time span of the data and calculating the overall duration of sales activity.

## Date Range (Duration of Sales Data) :
```sql
SELECT 
datediff(max(str_to_date(`ORDERDATE_1`,'%d/%m/%y')), min(str_to_date(`ORDERDATE_1`,'%d/%m/%y'))) AS `RANGE`
FROM sales_data_for_rfm_segmentation;
```
--Output--
|  RANGE  |
| --------|
| 876     |

- The sales data spans a total duration of 876 days, which is approximately 2.4 years. This time range provides a substantial period for analyzing sales trends, customer behavior, and performance metrics over time.

## Checking Year & Month :
```sql
WITH CTE AS ( SELECT  
YEAR_ID, 
COUNT(distinct MONTH_ID) as Distinct_Month_Count
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 1 DESC)

SELECT * FROM CTE;
```
--Output--
| YEAR_ID | Distinct_Month_Count |
|---------|----------------------|
| 2005    | 5                    |
| 2004    | 12                   |
| 2003    | 12                   |

- The dataset shows a variation in the number of months with recorded sales across different years:

1. 2003 and 2004 have data for 12 distinct months, indicating a full year of sales activity.

2. 2005 has data for only 5 distinct months, suggesting incomplete data or a potential decline in sales activity for that year.

- This discrepancy could indicate a data collection issue, a business-specific event, or a shift in sales strategy.

## MAX REVENUE FOR YEARLY SALES :
```sql
WITH CTE AS (SELECT YEAR_ID, ROUND(SUM(SALES),2) AS REV
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 2 DESC)

SELECT MAX(REV) AS MAX_REV FROM CTE;
```
--Output--
|  MAX_REV   |
| --------   |
| 4724162.59 |

- The maximum revenue recorded in the dataset is $4,724,162.59. This represents the highest single revenue value, which could be tied to a large transaction, a high-value customer, or a significant deal.

## Yearly Sales :
```sql
SELECT YEAR_ID, ROUND(SUM(SALES),2) AS YEARLY_SALES
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 2 DESC;
```
--Output--
| YEAR_ID | YEARLY_SALES  |
|---------|---------------|
| 2004    | 4724162.59    |
| 2003    | 3516979.55    |
| 2005    | 1791486.71    |

- We can see that we get maximum revenue in 2004.
- The yearly sales data reveals the following trends:

1. 2004 had the highest sales, totaling $4,724,162.59, indicating a peak in revenue generation.

2. 2003 followed with $3,516,979.55, showing strong performance but slightly lower than 2004.

3. 2005 recorded the lowest sales at $1,791,486.71, which is significantly lower compared to the previous years. This could indicate a decline in sales, incomplete data for the year, or changes in business strategy.

#RFM Segmentation Analysis :

## RFM Value :
**This SQL query calculates the RFM (Recency, Frequency, Monetary) values for each customer in the dataset**
```sql
-- RFM SEGMENTATION : SEGMENTING CUSTOMERS BASED ON RECENCY(R), FREQUENCY(F), MONETARY(M) SCORES
-- MAX(STR_TO_DATE(ORDERDATE,' %d/%m/%y')) AS EACH_CUSTOMERS_LAST_TRANSACTION_DATE,
-- (SELECT MAX(STR_TO_DATE(ORDERDATE, '%d/%m/%y')) FROM SALES_SAMPLE_DATA) AS BUSINESS_LAST_TRANSACTION_DATE,
SELECT 
    CUSTOMERNAME,
    ROUND(SUM(SALES),0) AS MONETARY_VALUE,
    COUNT(ORDERNUMBER) AS FREQUENCY,
    DATEDIFF(
        (SELECT MAX(STR_TO_DATE(`ORDERDATE_1`, '%d/%m/%y')) FROM sales_data_for_rfm_segmentation),
        MAX(STR_TO_DATE(`ORDERDATE_1`, '%d/%m/%y'))
        
    ) AS RECENCY
FROM `sales_data_for_rfm_segmentation`
GROUP BY CUSTOMERNAME
order by 2 desc;

```
--Output--
| CUSTOMERNAME                     | MONETARY_VALUE | FREQUENCY | RECENCY |
|----------------------------------|----------------|-----------|---------|
| Euro Shopping Channel            | 912294         | 259       | 0       |
| Mini Gifts Distributors Ltd.     | 654858         | 180       | 2       |
| Australian Collectors, Co.       | 200995         | 55        | 183     |
| Muscle Machine Inc               | 197737         | 48        | 181     |
| La Rochelle Gifts                | 180125         | 53        | 0       |
| Dragon Souveniers, Ltd.          | 172990         | 43        | 90      |
| Land of Toys Inc.                | 164069         | 49        | 197     |
| The Sharp Gifts Warehouse        | 160010         | 40        | 39      |
| AV Stores, Co.                   | 157808         | 51        | 195     |
| Anna's Decorations, Ltd          | 153996         | 46        | 83      |
| Souveniers And Things Co.        | 151571         | 46        | 2       |
| Corporate Gift Ideas Co.         | 149883         | 41        | 97      |
| Salzburg Collectables            | 149799         | 40        | 14      |

- **Top Customer:** Euro Shopping Channel leads with the highest Monetary Value ($912,294) and Frequency (259 orders), making them the most valuable and active customer.

- **Recent Activity:** Euro Shopping Channel and La Rochelle Gifts have the lowest Recency (0 days), indicating very recent engagement.

- **High-Value Customers:** Mini Gifts Distributors Ltd. and Australian Collectors, Co. also show strong performance with high Monetary Value and Frequency.

## Which month had the highest sales in 2003 :
```sql
SELECT PRODUCTLINE, COUNT(ORDERNUMBER) AS FREQUENCY, ROUND(SUM(SALES),2) AS REVENUE, MONTH_ID
FROM sales_data_for_rfm_segmentation
WHERE YEAR_ID = 2003
GROUP BY 1,4
ORDER BY 3 DESC;
```
--Output--
| PRODUCTLINE      | FREQUENCY | REVENUE    | MONTH_ID |
|------------------|-----------|------------|----------|
| Classic Cars     | 114       | 452924.37  | 11       |
| Classic Cars     | 62        | 241145.43  | 10       |
| Vintage Cars     | 66        | 184673.4   | 11       |
| Classic Cars     | 37        | 137666.87  | 9        |
| Classic Cars     | 32        | 135593.69  | 12       |
| Trucks and Buses | 33        | 127062.92  | 11       |
| Motorcycles      | 31        | 109345.5   | 11       |
| Classic Cars     | 26        | 105026.68  | 3        |
| Vintage Cars     | 31        | 100603.25  | 10       |
| Classic Cars     | 28        | 98179.48   | 5        |
| Classic Cars     | 22        | 94055.58   | 7        |
| Ships            | 27        | 79174.8    | 11       |
| Planes           | 20        | 69180.74   | 10       |
| Motorcycles      | 19        | 64235.65   | 10       |
| Classic Cars     | 12        | 59873.6    | 4        |

-  November had the highest sales in 2003.
-  **Top Performer:** Classic Cars dominate with the highest Frequency (114 orders) and Revenue ($452,924.37) in Month 11 (November), making it the most popular and profitable product 
    line.
-  **Seasonal Trends:** Month 11 (November) shows peak performance across multiple product lines, including Vintage Cars, Trucks and Buses, and Motorcycles, suggesting a potential 
    seasonal spike in sales.
-  **Consistent Revenue:** Classic Cars consistently generate high revenue across multiple months, indicating strong customer demand.

## Average Customer :
```sql
SELECT
    CUSTOMERNAME,
    ROUND(AVG(SALES),0) AS AVG_MONETARY_VALUE,
    COUNT(*) AS FREQUENCY,
    MAX(`ORDERDATE_1`) AS RECENCY,
    DATEDIFF(MAX(`ORDERDATE_1`), MIN(`ORDERDATE_1`)) AS RECENCYDAYS
FROM `sales_data_for_rfm_segmentation`
GROUP BY 1
order by 5 desc;
```
--Output--
| CUSTOMERNAME                     | AVG_MONETARY_VALUE | FREQUENCY | RECENCY      | RECENCYDAYS |
|----------------------------------|--------------------|-----------|--------------|-------------|
| La Corne D'abondance, Co.        | 4226               | 23        | 28/8/04      | 740470      |
| West Coast Collectables Co.      | 3545               | 13        | 29/1/04      | 740379      |
| Corrida Auto Replicas, Ltd       | 3769               | 32        | 28/5/03      | 740162      |
| Saveley & Henriot, Co.           | 3485               | 41        | 25/11/03     | 739189      |
| Mini Wheels Co.                  | 3546               | 21        | 25/3/03      | 738976      |
| The Sharp Gifts Warehouse        | 4000               | 40        | 22/4/05      | 738155      |
| Royal Canadian Collectables, Ltd.| 2871               | 26        | 20/8/04      | 737182      |
| giftsbymail.co.uk                | 3009               | 26        | 20/3/04      | 737180      |
| Oulu Toy Supplies, Inc.          | 3262               | 32        | 31/1/05      | 6850        |
| Marta's Replicas Co.             | 3818               | 27        | 27/8/04      | 5052        |
| Toys4GrownUps.com                | 3485               | 30        | 25/8/03      | 4959        |
| Souveniers And Things Co.        | 3295               | 46        | 29/5/05      | 4689        |
| Royale Belge                     | 4180               | 8         | 22/11/04     | 4686        |

-  **High-Value Customers:** La Corne D'abondance, Co. has the highest Average Monetary Value ($4,226), indicating high spending per transaction.

-  **Frequent Buyers:** Saveley & Henriot, Co. and Souveniers And Things Co. have the highest Frequency (41 and 46 orders), showing strong customer loyalty.

-  **Recent Activity:** Souveniers And Things Co. has the lowest Recency Days (4,689), indicating the most recent engagement among the listed customers.

-  **Inactive Customers:** Corrida Auto Replicas, Ltd and Mini Wheels Co. have the highest Recency Days (740,162 and 738,976), suggesting they haven’t made recent purchases.

## Which date has the most orders :
```sql
WITH CTE AS (
SELECT ORDERDATE_1, COUNT(ORDERNUMBER) AS FREQUENCY, ROUND(SUM(SALES),2) AS REVENUE
FROM sales_data_for_rfm_segmentation
GROUP BY 1
ORDER BY 3 DESC)

SELECT * FROM CTE
LIMIT 1;
```
--Output--
| ORDERDATE_1 | FREQUENCY | REVENUE   |
|-------------|-----------|-----------|
| 24/11/04   | 35        | 137644.72  |

- On 24th November 2004, there were 35 orders generating $137,644.72 in revenue, indicating a high-activity day with significant sales. This could be tied to a promotional event, 
  holiday shopping, or seasonal demand.

## In 2004 did the highest sales occur in November :
```sql
SELECT  PRODUCTLINE, COUNT(ORDERNUMBER) AS FREQUENCY , ROUND(SUM(SALES),2) AS REVENUE , MONTH_ID
FROM sales_data_for_rfm_segmentation
WHERE YEAR_ID = 2004 AND MONTH_ID = 11
GROUP BY 1
ORDER BY 2 DESC;
```
--Output--
| PRODUCTLINE      | FREQUENCY | REVENUE    | MONTH_ID |
|------------------|-----------|------------|----------|
| Classic Cars     | 105       | 372231.89  | 11       |
| Vintage Cars     | 65        | 233990.34  | 11       |
| Motorcycles      | 39        | 151711.86  | 11       |
| Planes           | 36        | 121130.7   | 11       |
| Trucks and Buses | 29        | 123811.14  | 11       |
| Ships            | 21        | 63900.85   | 11       |
| Trains           | 6         | 22271.23   | 11       |

- Yes, in 2004, the highest sales occurred in November (Month 11). Key highlights include:

1. **Classic Cars** dominated with 105 orders and $372,231.89 in revenue.

2. **Vintage Cars** and **Motorcycles** also performed exceptionally well, contributing 233,990.34 ∗∗and∗∗ 151,711.86 respectively.

3. All product lines saw significant activity, indicating a strong seasonal peak in November, likely driven by holiday shopping or year-end promotions.


## RFM Category Combination :
**This SQL code creates a view named RFM_SEGMENT, which calculates the RFM (Recency, Frequency, Monetary) scores and combines them into a single RFM category combination for each customer.**
```sql
CREATE or Replace VIEW RFM_SEGMENT AS
WITH RFM_INITIAL_CALCULATION AS  (
SELECT 
    CUSTOMERNAME,
    ROUND(SUM(SALES),0) AS MONETARY_VALUE,
    COUNT(ORDERNUMBER) AS FREQUENCY,
    DATEDIFF(
        (SELECT MAX(STR_TO_DATE(`ORDERDATE_1`, '%d/%m/%y')) FROM sales_data_for_rfm_segmentation),
        MAX(STR_TO_DATE(`ORDERDATE_1`, '%d/%m/%y'))
        
    ) AS RECENCY
FROM `sales_data_for_rfm_segmentation`
GROUP BY CUSTOMERNAME), 
-- RFM SCORE
-- SELECT 
-- C.*,
-- NTILE(4) OVER (ORDER BY C.RECENCY DESC) AS RFM_RECENCY_SCORE,
-- NTILE(4) OVER (ORDER BY C.FREQUENCY DESC) AS RFM_FREQUENCY_SCORE,
-- NTILE(4) OVER (ORDER BY C.MONETARY_VALUE DESC) AS RFM_MONETARY_SCORE
-- FROM RFM_INITIAL_CALCULATION AS C ; 

RFM_SCORE_CALCULATION AS (
SELECT 
C.*,
NTILE(4) OVER (ORDER BY C.RECENCY DESC) AS RFM_RECENCY_SCORE,
NTILE(4) OVER (ORDER BY C.FREQUENCY DESC) AS RFM_FREQUENCY_SCORE,
NTILE(4) OVER (ORDER BY C.MONETARY_VALUE DESC) AS RFM_MONETARY_SCORE
FROM RFM_INITIAL_CALCULATION AS C)

SELECT 
    R.CUSTOMERNAME,
    (R.RFM_RECENCY_SCORE + R.RFM_FREQUENCY_SCORE + R.RFM_MONETARY_SCORE) AS RFM_TOTAL_SCORE,
    CONCAT_WS(R.RFM_RECENCY_SCORE,
            R.RFM_FREQUENCY_SCORE,
            R.RFM_MONETARY_SCORE) AS RFM_CATEGORY_COMBINATION
FROM
    RFM_SCORE_CALCULATION AS R;

SELECT * FROM RFM_SEGMENT
order by 3 desc;
```
--Output--
| CUSTOMERNAME                     | RFM_TOTAL_SCORE | RFM_CATEGORY_COMBINATION |
|----------------------------------|-----------------|--------------------------|
| Mini Caravy                      | 11              | 443                      |
| Boards & Toys Co.                | 11              | 434                      |
| Atelier graphique                | 10              | 424                      |
| Volvo Model Replicas, Co         | 9               | 423                      |
| Bavarian Collectables Imports, Co.| 9               | 414                     |
| Australian Collectables, Ltd     | 11              | 344                      |
| Alpha Cognac                     | 10              | 343                      |
| Tokyo Collectables, Ltd          | 8               | 242                      |
| Diecast Classics Inc.            | 7               | 241                      |
| Signal Gift Stores               | 8               | 233                      |
| Suominen Souveniers              | 7               | 232                      |
| Royal Canadian Collectables, Ltd.| 6               | 213                      |
| Vida Sport, Ltd                  | 5               | 212                      |
| Herkku Gifts                     | 5               | 212                      |
| Marta's Replicas Co.             | 5               | 212                      |
| Amica Models & Co.               | 5               | 212                      |
| Handji Gifts& Co                 | 7               | 142                      |
| Mini Gifts Distributors Ltd.     | 6               | 141                      |
| Australian Collectors, Co.       | 5               | 131                      |
| AV Stores, Co.                   | 4               | 121                      |
| Rovelli Gifts                    | 4               | 121                      |
| Land of Toys Inc.                | 4               | 121                      |
| Online Diecast Creations Co.     | 4               | 121                      |
| Saveley & Henriot, Co.           | 3               | 111                      |

- The RFM (Recency, Frequency, Monetary) analysis categorizes customers based on their engagement and value:

1. **Top Customers:** Mini Caravy and Boards & Toys Co. have the highest RFM Total Score (11) and are categorized as 443 and 434, indicating high recency, frequency, and monetary value. These are your most valuable and loyal customers.

2. **Mid-Tier Customers:** Atelier graphique and Volvo Model Replicas, Co have RFM Total Scores of 10 and 9, respectively, showing good engagement but with room for improvement.

3. **Low Engagement:** Saveley & Henriot, Co. and AV Stores, Co. have the lowest RFM Total Scores (3 and 4), indicating infrequent and low-value transactions. These customers may need re-engagement strategies.

```sql
SELECT DISTINCT RFM_CATEGORY_COMBINATION 
    FROM RFM_SEGMENT
ORDER BY 1;
```
--Output--
| RFM_CATEGORY_COMBINATION |
|--------------------------|
| 111                      |
| 121                      |
| 131                      |
| 132                      |
| 141                      |
| 142                      |
| 212                      |
| 213                      |
| 221                      |
| 222                      |
| 223                      |
| 232                      |
| 233                      |
| 241                      |
| 242                      |
| 243                      |
| 312                      |
| 313                      |
| 314                      |
| 322                      |
| 323                      |
| 324                      |
| 333                      |
| 342                      |
| 343                      |
| 344                      |
| 413                      |
| 414                      |
| 423                      |
| 424                      |
| 434                      |
| 443                      |

## RFM Customer Segmentation :

**This SQL code segment assigns a customer segment label based on their RFM category combination**

```sql

WITH RFM_INITIAL_CALCULATION AS (
    SELECT
        CUSTOMERNAME,
        ROUND(SUM(SALES), 0) AS MONETARY_VALUE,
        COUNT(ORDERNUMBER) AS FREQUENCY,
        MAX(STR_TO_DATE(`ORDERDATE_1`, '%d/%m/%y')) AS RECENT_ORDER_DATE
    FROM `sales_data_for_rfm_segmentation`
    GROUP BY CUSTOMERNAME
),
RFM_SCORE_CALCULATION AS (
    SELECT
        C.*,
        NTILE(4) OVER (ORDER BY RECENT_ORDER_DATE DESC) AS RFM_RECENCY_SCORE,
        NTILE(4) OVER (ORDER BY FREQUENCY DESC) AS RFM_FREQUENCY_SCORE,
        NTILE(4) OVER (ORDER BY MONETARY_VALUE DESC) AS RFM_MONETARY_SCORE
    FROM RFM_INITIAL_CALCULATION AS C
)
    
   SELECT 
    CUSTOMERNAME,
    CASE
        WHEN RFM_CATEGORY_COMBINATION IN (111, 112, 121, 123, 132, 211, 211, 212, 114, 141) THEN 'CHURNED CUSTOMER'
        WHEN RFM_CATEGORY_COMBINATION IN (133, 134, 143, 244, 334, 343, 344, 144) THEN 'SLIPPING AWAY, CANNOT LOSE'
        WHEN RFM_CATEGORY_COMBINATION IN (311, 411, 331) THEN 'NEW CUSTOMERS'
        WHEN RFM_CATEGORY_COMBINATION IN (222, 231, 221,  223, 233, 322) THEN 'POTENTIAL CHURNERS'
        WHEN RFM_CATEGORY_COMBINATION IN (323, 333,321, 341, 422, 332, 432) THEN 'ACTIVE'
        WHEN RFM_CATEGORY_COMBINATION IN (433, 434, 443, 444) THEN 'LOYAL'
    ELSE 'CANNOT BE DEFINED'
    END AS CUSTOMER_SEGMENT

FROM RFM_SEGMENT;
```
--Output--
| CUSTOMERNAME                     | CUSTOMER_SEGMENT             |
|----------------------------------|------------------------------|
| Euro Shopping Channel            | CHURNED CUSTOMER             |
| Mini Gifts Distributors Ltd.     | CHURNED CUSTOMER             |
| Australian Collectors, Co.       | CANNOT BE DEFINED            |
| Muscle Machine Inc               | CANNOT BE DEFINED            |
| La Rochelle Gifts                | CHURNED CUSTOMER             |
| Dragon Souveniers, Ltd.          | CANNOT BE DEFINED            |
| Land of Toys Inc.                | CHURNED CUSTOMER             |
| The Sharp Gifts Warehouse        | CHURNED CUSTOMER             |
| AV Stores, Co.                   | CHURNED CUSTOMER             |
| Anna's Decorations, Ltd          | CANNOT BE DEFINED            |
| Souveniers And Things Co.        | CHURNED CUSTOMER             |
| Corporate Gift Ideas Co.         | CANNOT BE DEFINED            |
| Salzburg Collectables            | CHURNED CUSTOMER             |
| Danish Wholesale Imports         | CHURNED CUSTOMER             |
| Saveley & Henriot, Co.           | CHURNED CUSTOMER             |
| L'ordine Souveniers              | CHURNED CUSTOMER             |
| Rovelli Gifts                    | CHURNED CUSTOMER             |
| Reims Collectables               | CHURNED CUSTOMER             |
| Scandinavian Gift Ideas          | CANNOT BE DEFINED            |
| Online Diecast Creations Co.     | CHURNED CUSTOMER             |
| Diecast Classics Inc.            | CANNOT BE DEFINED            |
| Technics Stores Inc.             | CANNOT BE DEFINED            |
| Corrida Auto Replicas, Ltd       | POTENTIAL CHURNERS           |
| Tokyo Collectables, Ltd          | CANNOT BE DEFINED            |
| UK Collectables, Ltd.            | CANNOT BE DEFINED            |
| Vida Sport, Ltd                  | CHURNED CUSTOMER             |
| Baane Mini Imports               | POTENTIAL CHURNERS           |
| Handji Gifts& Co                 | CANNOT BE DEFINED            |
| Suominen Souveniers              | CANNOT BE DEFINED            |
| Herkku Gifts                     | CHURNED CUSTOMER             |
| Toys of Finland, Co.             | CANNOT BE DEFINED            |
| Mini Creations Ltd.              | CHURNED CUSTOMER             |
| Toys4GrownUps.com                | CANNOT BE DEFINED            |
| Oulu Toy Supplies, Inc.          | CANNOT BE DEFINED            |
| Marta's Replicas Co.             | CHURNED CUSTOMER             |
| Gift Depot Inc.                  | CANNOT BE DEFINED            |
| Heintze Collectables             | POTENTIAL CHURNERS           |
| Toms Spezialitten, Ltd           | POTENTIAL CHURNERS           |
| FunGiftIdeas.com                 | CANNOT BE DEFINED            |
| La Corne D'abondance, Co.        | POTENTIAL CHURNERS           |
| Amica Models & Co.               | CHURNED CUSTOMER             |
| Cruz & Sons Co.                  | POTENTIAL CHURNERS           |
| Auto Canal Petit                 | CANNOT BE DEFINED            |
| Stylish Desk Decors, Co.         | CANNOT BE DEFINED            |
| Vitachrome Inc.                  | POTENTIAL CHURNERS           |
| Collectable Mini Designs Co.     | CANNOT BE DEFINED            |
| Mini Classics                    | POTENTIAL CHURNERS           |
| Blauer See Auto, Co.             | ACTIVE                       |
| Motor Mint Distributors Inc.     | ACTIVE                       |
| Tekni Collectables Inc.          | SLIPPING AWAY, CANNOT LOSE   |
| Gifts4AllAges.com                | CANNOT BE DEFINED            |
| Signal Gift Stores               | POTENTIAL CHURNERS           |
| Collectables For Less Inc.       | ACTIVE                       |
| Mini Caravy                      | LOYAL                       |
| Super Scale Inc.                 | CANNOT BE DEFINED            |
| Norway Gifts By Mail, Co.        | CANNOT BE DEFINED            |
| Lyon Souveniers                  | SLIPPING AWAY, CANNOT LOSE   |
| Enaco Distributors               | ACTIVE                       |
| giftsbymail.co.uk                | POTENTIAL CHURNERS           |
| Classic Legends Inc.             | ACTIVE                       |
| Volvo Model Replicas, Co         | CANNOT BE DEFINED            |
| Canadian Gift Exchange Network   | ACTIVE                       |
| Petit Auto                       | SLIPPING AWAY, CANNOT LOSE   |
| Marseille Mini Autos             | ACTIVE                       |
| Royal Canadian Collectables, Ltd.| CANNOT BE DEFINED            |
| Mini Wheels Co.                  | ACTIVE                       |
| Quebec Home Shopping Network     | SLIPPING AWAY, CANNOT LOSE   |
| Diecast Collectables             | CANNOT BE DEFINED            |
| Alpha Cognac                     | SLIPPING AWAY, CANNOT LOSE   |
| Daedalus Designs Imports         | CANNOT BE DEFINED            |
| Osaka Souveniers Co.             | CANNOT BE DEFINED            |
| Classic Gift Ideas, Inc          | CANNOT BE DEFINED            |
| Auto Assoc. & Cie.               | CANNOT BE DEFINED            |
| Australian Collectables, Ltd     | SLIPPING AWAY, CANNOT LOSE   |
| Australian Gift Network, Co      | LOYAL                       |
| Clover Collections, Co.          | CANNOT BE DEFINED            |
| Gift Ideas Corp.                 | LOYAL                       |
| Online Mini Collectables         | CANNOT BE DEFINED            |
| Iberia Gift Imports, Corp.       | CANNOT BE DEFINED            |
| Mini Auto Werke                  | LOYAL                       |
| Signal Collectibles Ltd.         | CANNOT BE DEFINED            |
| CAF Imports                      | CANNOT BE DEFINED            |
| Men 'R' US Retailers, Ltd.       | CANNOT BE DEFINED            |
| West Coast Collectables Co.      | CANNOT BE DEFINED            |
| Cambridge Collectables Co.       | CANNOT BE DEFINED            |
| Double Decker Gift Stores, Ltd   | CANNOT BE DEFINED            |
| Bavarian Collectables Imports, Co.| CANNOT BE DEFINED           |
| Royale Belge                     | LOYAL                       |
| Microscale Inc.                  | CANNOT BE DEFINED            |
| Auto-Moto Classics Inc.          | LOYAL                       |
| Atelier graphique                | CANNOT BE DEFINED            |
| Boards & Toys Co.                | LOYAL                       |

- The customer segmentation reveals distinct groups based on engagement and loyalty:

1. **Churned Customers:** A significant portion, including Euro Shopping Channel, Mini Gifts Distributors Ltd., and La Rochelle Gifts, are marked as Churned, indicating they have 
   stopped engaging with the business.

2.  **Potential Churners:** Customers like Corrida Auto Replicas, Ltd and Vitachrome Inc. are at risk of churning and need immediate attention to retain them.

3.  **Active & Loyal Customers:** Blauer See Auto, Co., Motor Mint Distributors Inc., and Mini Caravy are Active or Loyal, representing high-value, engaged customers.

4.  **Slipping Away:** Customers like Tekni Collectables Inc. and Lyon Souveniers are Slipping Away but are still valuable and should not be lost.

-   **Actionable Steps :**

1. **Re-engage Churned Customers:** Offer incentives or personalized campaigns to win them back.

2.  **Retain Potential Churners:** Provide targeted offers or improved customer service to prevent them from leaving.

3.  **Reward Loyal Customers:** Strengthen relationships with exclusive perks or loyalty programs.

4.  **Monitor Slipping Away Customers:** Address their concerns and rekindle their interest.


## Numbers of Customer Segment :
```sql
WITH CTE1 AS (
SELECT 
    CUSTOMERNAME,
    CASE
        WHEN RFM_CATEGORY_COMBINATION IN (111, 112, 121, 123, 132, 211, 211, 212, 114, 141) THEN 'CHURNED CUSTOMER'
        WHEN RFM_CATEGORY_COMBINATION IN (133, 134, 143, 244, 334, 343, 344, 144) THEN 'SLIPPING AWAY, CANNOT LOSE'
        WHEN RFM_CATEGORY_COMBINATION IN (311, 411, 331) THEN 'NEW CUSTOMERS'
        WHEN RFM_CATEGORY_COMBINATION IN (222, 231, 221,  223, 233, 322) THEN 'POTENTIAL CHURNERS'
        WHEN RFM_CATEGORY_COMBINATION IN (323, 333,321, 341, 422, 332, 432) THEN 'ACTIVE'
        WHEN RFM_CATEGORY_COMBINATION IN (433, 434, 443, 444) THEN 'LOYAL'
    ELSE 'CANNOT BE DEFINED'
    END AS CUSTOMER_SEGMENT

FROM RFM_SEGMENT)

SELECT CUSTOMER_SEGMENT , COUNT(*) AS NUMBER_OF_CUSTOMERS
FROM CTE1
GROUP BY 1
ORDER BY 2 DESC;
```
--Output--
| CUSTOMER_SEGMENT             | NUMBER_OF_CUSTOMERS |
|------------------------------|---------------------|
| CANNOT BE DEFINED            | 42                  |
| CHURNED CUSTOMER             | 19                  |
| POTENTIAL CHURNERS           | 10                  |
| ACTIVE                       | 8                   |
| LOYAL                        | 7                   |
| SLIPPING AWAY, CANNOT LOSE   | 6                   |


- The customer segmentation breakdown highlights the following distribution:

1. **Cannot Be Defined (42 customers):** A large portion of customers lack clear segmentation, indicating a need for better data collection or analysis to understand their behavior.

2. **Churned Customers (19 customers):** Nearly 20 customers have stopped engaging, representing a significant loss in potential revenue.

3. **Potential Churners (10 customers):** 10 customers are at risk of leaving, requiring immediate retention efforts.

4. **Active (8 customers):** A small group of customers are actively engaging with the business.

5. **Loyal (7 customers):** 7 customers are highly loyal and valuable, contributing consistently to revenue.

6. **Slipping Away, Cannot Lose (6 customers):** 6 customers are disengaging but are still valuable and should be prioritized for retention.

-  **Actionable Steps :**

1. **Focus on Retention:** Prioritize Potential Churners and Slipping Away customers with targeted campaigns to prevent further loss.

2. **Re-engage Churned Customers:** Develop win-back strategies to revive relationships with Churned Customers.

3. **Strengthen Loyalty:** Reward Loyal and Active customers to maintain their engagement and increase lifetime value.

4. **Improve Segmentation:** Address the Cannot Be Defined segment by enhancing data collection and analysis for better insights.

## how does the query classify customers into different risk levels based on their recency of purchase :
```sql
WITH CUSTOMERRFM AS (
    SELECT
        CUSTOMERNAME,
        ROUND(AVG(SALES), 0) AS AVG_MONETARY_VALUE,
        COUNT(*) AS FREQUENCY,
        MAX(`ORDERDATE_1`) AS RECENCY,
        abs(DATEDIFF(CURDATE(), MAX(`ORDERDATE_1`))) AS RECENCYDAYS
    FROM `sales_data_for_rfm_segmentation`
    GROUP BY CUSTOMERNAME
),

RFM_SEGMENT AS (
    SELECT
        CUSTOMERNAME,
        NTILE(4) OVER (ORDER BY RECENCY DESC) AS RFM_RECENCY,
        NTILE(4) OVER (ORDER BY FREQUENCY DESC) AS RFM_FREQUENCY,
        NTILE(4) OVER (ORDER BY AVG_MONETARY_VALUE DESC) AS RFM_MONETARY_VALUE,
        RECENCYDAYS,
        CASE
            WHEN RECENCYDAYS > 7000 THEN 'High Risk'
            WHEN RECENCYDAYS > 300 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS RiskLevel
    FROM CUSTOMERRFM
)

select CUSTOMERNAME,
RECENCYDAYS,
RiskLevel
 from rfm_segment;
```
--Output--
| CUSTOMERNAME                     | RECENCYDAYS | RiskLevel   |
|----------------------------------|-------------|-------------|
| Super Scale Inc.                 | 738085      | High Risk   |
| Mini Caravy                      | 738054      | High Risk   |
| La Corne D'abondance, Co.        | 1258        | Medium Risk |
| Royale Belge                     | 842         | Medium Risk |
| Muscle Machine Inc               | 736929      | High Risk   |
| Gift Depot Inc.                  | 737719      | High Risk   |
| UK Collectables, Ltd.            | 736653      | High Risk   |
| Danish Wholesale Imports         | 736289      | High Risk   |
| Dragon Souveniers, Ltd.          | 737993      | High Risk   |
| The Sharp Gifts Warehouse        | 1055        | Medium Risk |
| Volvo Model Replicas, Co         | 736075      | High Risk   |
| Australian Gift Network, Co      | 737172      | High Risk   |
| Tekni Collectables Inc.          | 738480      | High Risk   |
| Diecast Classics Inc.            | 1897        | Medium Risk |
| Diecast Collectables             | 736046      | High Risk   |
| Lyon Souveniers                  | 736045      | High Risk   |
| Classic Legends Inc.             | 738116      | High Risk   |
| Online Diecast Creations Co.     | 737476      | High Risk   |
| Blauer See Auto, Co.             | 736380      | High Risk   |
| Toms Spezialitten, Ltd           | 1573        | Medium Risk |
| Herkku Gifts                     | 738328      | High Risk   |
| CAF Imports                      | 736807      | High Risk   |
| Marta's Replicas Co.             | 892         | Medium Risk |
| Online Mini Collectables         | 738724      | High Risk   |
| FunGiftIdeas.com                 | 737901      | High Risk   |
| Suominen Souveniers              | 737474      | High Risk   |
| Vida Sport, Ltd                  | 1988        | Medium Risk |
| Corrida Auto Replicas, Ltd       | 1165        | Medium Risk |
| Tokyo Collectables, Ltd          | 316         | Medium Risk |
| Salzburg Collectables            | 1135        | Medium Risk |
| Heintze Collectables             | 873         | Medium Risk |
| Toys of Finland, Co.             | 736347      | High Risk   |
| L'ordine Souveniers              | 737294      | High Risk   |
| Corporate Gift Ideas Co.         | 749         | Medium Risk |
| Australian Collectors, Co.       | 1500        | Medium Risk |
| Iberia Gift Imports, Corp.       | 737202      | High Risk   |
| Baane Mini Imports               | 737536      | High Risk   |
| Mini Gifts Distributors Ltd.     | 736533      | High Risk   |
| Motor Mint Distributors Inc.     | 738481      | High Risk   |
| Amica Models & Co.               | 736136      | High Risk   |
| Cruz & Sons Co.                  | 738513      | High Risk   |
| Clover Collections, Co.          | 3094        | Medium Risk |
| Auto Assoc. & Cie.               | 738905      | High Risk   |
| Technics Stores Inc.             | 737839      | High Risk   |
| Mini Wheels Co.                  | 8           | Low Risk    |
| West Coast Collectables Co.      | 1411        | Medium Risk |
| Scandinavian Gift Ideas          | 736501      | High Risk   |
| Alpha Cognac                     | 736441      | High Risk   |
| Euro Shopping Channel            | 736167      | High Risk   |
| Vitachrome Inc.                  | 737750      | High Risk   |
| Collectable Mini Designs Co.     | 346         | Medium Risk |
| Toys4GrownUps.com                | 161         | Low Risk    |
| Saveley & Henriot, Co.           | 253         | Low Risk    |
| Mini Auto Werke                  | 737902      | High Risk   |
| Atelier graphique                | 923         | Medium Risk |
| Daedalus Designs Imports         | 1480        | Medium Risk |
| Auto Canal Petit                 | 737019      | High Risk   |
| Men 'R' US Retailers, Ltd.       | 736379      | High Risk   |
| Canadian Gift Exchange Network   | 737507      | High Risk   |
| Stylish Desk Decors, Co.         | 737172      | High Risk   |
| Enaco Distributors               | 618         | Medium Risk |
| Collectables For Less Inc.       | 737598      | High Risk   |
| La Rochelle Gifts                | 2262        | Medium Risk |
| Osaka Souveniers Co.             | 4343        | Medium Risk |
| Quebec Home Shopping Network     | 737537      | High Risk   |
| Land of Toys Inc.                | 736501      | High Risk   |
| Anna's Decorations, Ltd          | 736319      | High Risk   |
| Signal Collectibles Ltd.         | 4556        | Medium Risk |
| Microscale Inc.                  | 738267      | High Risk   |
| Auto-Moto Classics Inc.          | 736867      | High Risk   |
| Norway Gifts By Mail, Co.        | 737568      | High Risk   |
| Souveniers And Things Co.        | 1532        | Medium Risk |
| Reims Collectables               | 736991      | High Risk   |
| Mini Classics                    | 253         | Low Risk    |
| Cambridge Collectables Co.       | 736624      | High Risk   |
| Oulu Toy Supplies, Inc.          | 2142        | Medium Risk |
| Classic Gift Ideas, Inc          | 3795        | Medium Risk |
| Handji Gifts& Co                 | 738175      | High Risk   |
| Gifts4AllAges.com                | 737354      | High Risk   |
| Mini Creations Ltd.              | 737109      | High Risk   |
| AV Stores, Co.                   | 2549        | Medium Risk |
| Boards & Toys Co.                | 736713      | High Risk   |
| Gift Ideas Corp.                 | 737871      | High Risk   |
| giftsbymail.co.uk                | 1817        | Medium Risk |
| Double Decker Gift Stores, Ltd   | 1146        | Medium Risk |
| Petit Auto                       | 1897        | Medium Risk |
| Marseille Mini Autos             | 736625      | High Risk   |
| Rovelli Gifts                    | 737537      | High Risk   |
| Royal Canadian Collectables, Ltd.| 1664        | Medium Risk |
| Signal Gift Stores               | 737263      | High Risk   |
| Australian Collectables, Ltd     | 736258      | High Risk   |
| Bavarian Collectables Imports, Co.| 3460       | Medium Risk |

## Identify RFM segments with a higher risk of customer churn :
```sql
WITH CUSTOMERRFM AS (
    SELECT
        CUSTOMERNAME,
        ROUND(AVG(SALES), 0) AS AVG_MONETARY_VALUE,
        COUNT(*) AS FREQUENCY,
        MAX(`ORDERDATE_1`) AS RECENCY,
        DATEDIFF(CURDATE(), MAX(`ORDERDATE_1`)) AS RECENCYDAYS
    FROM `sales_data_for_rfm_segmentation`
    GROUP BY CUSTOMERNAME
),

RFM_SEGMENT AS (
    SELECT
        CUSTOMERNAME,
        NTILE(4) OVER (ORDER BY RECENCY DESC) AS RFM_RECENCY,
        NTILE(4) OVER (ORDER BY FREQUENCY DESC) AS RFM_FREQUENCY,
        NTILE(4) OVER (ORDER BY AVG_MONETARY_VALUE DESC) AS RFM_MONETARY_VALUE,
        RECENCYDAYS,
        CASE
            WHEN RECENCYDAYS > 7000 THEN 'High Risk'
            WHEN RECENCYDAYS > 300 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS RiskLevel
    FROM CUSTOMERRFM
),

CTE1 AS (
    SELECT 
        CUSTOMERNAME,
        CASE
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('111', '112', '121', '123', '132', '211', '212', '114', '141') THEN 'CHURNED CUSTOMER'
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('133', '134', '143', '244', '334', '343', '344', '144') THEN 'SLIPPING AWAY, CANNOT LOSE'
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('311', '411', '331') THEN 'NEW CUSTOMERS'
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('222', '231', '221', '223', '233', '322') THEN 'POTENTIAL CHURNERS'
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('323', '333', '321', '341', '422', '332', '432') THEN 'ACTIVE'
            WHEN CONCAT(RFM_RECENCY, RFM_FREQUENCY, RFM_MONETARY_VALUE) IN ('433', '434', '443', '444') THEN 'LOYAL'
            ELSE 'CANNOT BE DEFINED'
        END AS CUSTOMER_SEGMENT,
        RiskLevel
    FROM RFM_SEGMENT
)

SELECT 
    
    CUSTOMERNAME,
    RiskLevel,
    CUSTOMER_SEGMENT
FROM
    CTE1
GROUP BY CUSTOMER_SEGMENT , CUSTOMERNAME , RiskLevel ;

```

--Output--
| CUSTOMERNAME                     | RiskLevel   | CUSTOMER_SEGMENT             |
|----------------------------------|-------------|------------------------------|
| Super Scale Inc.                 | High Risk   | CANNOT BE DEFINED            |
| Mini Caravy                      | High Risk   | CANNOT BE DEFINED            |
| La Corne D'abondance, Co.        | Low Risk    | NEW CUSTOMERS                |
| Royale Belge                     | Medium Risk | CANNOT BE DEFINED            |
| Muscle Machine Inc               | High Risk   | CHURNED CUSTOMER             |
| Gift Depot Inc.                  | High Risk   | POTENTIAL CHURNERS           |
| UK Collectables, Ltd.            | High Risk   | CHURNED CUSTOMER             |
| Danish Wholesale Imports         | High Risk   | CHURNED CUSTOMER             |
| Dragon Souveniers, Ltd.          | High Risk   | CHURNED CUSTOMER             |
| The Sharp Gifts Warehouse        | Medium Risk | NEW CUSTOMERS                |
| Volvo Model Replicas, Co         | High Risk   | CHURNED CUSTOMER             |
| Australian Gift Network, Co      | High Risk   | CANNOT BE DEFINED            |
| Tekni Collectables Inc.          | High Risk   | NEW CUSTOMERS                |
| Diecast Classics Inc.            | Low Risk    | ACTIVE                       |
| Diecast Collectables             | High Risk   | CHURNED CUSTOMER             |
| Lyon Souveniers                  | High Risk   | CANNOT BE DEFINED            |
| Classic Legends Inc.             | High Risk   | POTENTIAL CHURNERS           |
| Online Diecast Creations Co.     | High Risk   | CHURNED CUSTOMER             |
| Blauer See Auto, Co.             | High Risk   | CANNOT BE DEFINED            |
| Toms Spezialitten, Ltd           | Medium Risk | CANNOT BE DEFINED            |
| Herkku Gifts                     | High Risk   | ACTIVE                       |
| CAF Imports                      | High Risk   | CANNOT BE DEFINED            |
| Marta's Replicas Co.             | Low Risk    | ACTIVE                       |
| Online Mini Collectables         | High Risk   | CANNOT BE DEFINED            |
| FunGiftIdeas.com                 | High Risk   | POTENTIAL CHURNERS           |
| Suominen Souveniers              | High Risk   | POTENTIAL CHURNERS           |
| Vida Sport, Ltd                  | Low Risk    | POTENTIAL CHURNERS           |
| Corrida Auto Replicas, Ltd       | Low Risk    | POTENTIAL CHURNERS           |
| Tokyo Collectables, Ltd          | Low Risk    | ACTIVE                       |
| Salzburg Collectables            | Low Risk    | CANNOT BE DEFINED            |
| Heintze Collectables             | Medium Risk | ACTIVE                       |
| Toys of Finland, Co.             | High Risk   | CANNOT BE DEFINED            |
| L'ordine Souveniers              | High Risk   | CHURNED CUSTOMER             |
| Corporate Gift Ideas Co.         | Medium Risk | CANNOT BE DEFINED            |
| Australian Collectors, Co.       | Low Risk    | CANNOT BE DEFINED            |
| Iberia Gift Imports, Corp.       | High Risk   | CANNOT BE DEFINED            |
| Baane Mini Imports               | High Risk   | POTENTIAL CHURNERS           |
| Mini Gifts Distributors Ltd.     | High Risk   | CHURNED CUSTOMER             |
| Motor Mint Distributors Inc.     | High Risk   | ACTIVE                       |
| Amica Models & Co.               | High Risk   | CANNOT BE DEFINED            |
| Cruz & Sons Co.                  | High Risk   | POTENTIAL CHURNERS           |
| Clover Collections, Co.          | Medium Risk | CANNOT BE DEFINED            |
| Auto Assoc. & Cie.               | High Risk   | CANNOT BE DEFINED            |
| Technics Stores Inc.             | High Risk   | CHURNED CUSTOMER             |
| Mini Wheels Co.                  | Low Risk    | ACTIVE                       |
| West Coast Collectables Co.      | Low Risk    | CANNOT BE DEFINED            |
| Scandinavian Gift Ideas          | High Risk   | CANNOT BE DEFINED            |
| Alpha Cognac                     | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Euro Shopping Channel            | High Risk   | CANNOT BE DEFINED            |
| Vitachrome Inc.                  | High Risk   | POTENTIAL CHURNERS           |
| Collectable Mini Designs Co.     | Low Risk    | ACTIVE                       |
| Toys4GrownUps.com                | Low Risk    | CANNOT BE DEFINED            |
| Saveley & Henriot, Co.           | Low Risk    | CANNOT BE DEFINED            |
| Mini Auto Werke                  | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Atelier graphique                | Low Risk    | SLIPPING AWAY, CANNOT LOSE   |
| Daedalus Designs Imports         | Medium Risk | LOYAL                        |
| Auto Canal Petit                 | High Risk   | CHURNED CUSTOMER             |
| Men 'R' US Retailers, Ltd.       | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Canadian Gift Exchange Network   | High Risk   | POTENTIAL CHURNERS           |
| Stylish Desk Decors, Co.         | High Risk   | POTENTIAL CHURNERS           |
| Enaco Distributors               | Low Risk    | LOYAL                        |
| Collectables For Less Inc.       | High Risk   | POTENTIAL CHURNERS           |
| La Rochelle Gifts                | Low Risk    | CANNOT BE DEFINED            |
| Osaka Souveniers Co.             | Medium Risk | LOYAL                        |
| Quebec Home Shopping Network     | High Risk   | POTENTIAL CHURNERS           |
| Land of Toys Inc.                | High Risk   | CANNOT BE DEFINED            |
| Anna's Decorations, Ltd          | High Risk   | CANNOT BE DEFINED            |
| Signal Collectibles Ltd.         | Medium Risk | LOYAL                        |
| Microscale Inc.                  | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Auto-Moto Classics Inc.          | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Norway Gifts By Mail, Co.        | High Risk   | CANNOT BE DEFINED            |
| Souveniers And Things Co.        | Low Risk    | CANNOT BE DEFINED            |
| Reims Collectables               | High Risk   | CHURNED CUSTOMER             |
| Mini Classics                    | Low Risk    | CANNOT BE DEFINED            |
| Cambridge Collectables Co.       | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Oulu Toy Supplies, Inc.          | Low Risk    | CANNOT BE DEFINED            |
| Classic Gift Ideas, Inc          | Medium Risk | LOYAL                        |
| Handji Gifts& Co                 | High Risk   | CANNOT BE DEFINED            |
| Gifts4AllAges.com                | High Risk   | CANNOT BE DEFINED            |
| Mini Creations Ltd.              | High Risk   | CANNOT BE DEFINED            |
| AV Stores, Co.                   | Medium Risk | CANNOT BE DEFINED            |
| Boards & Toys Co.                | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Gift Ideas Corp.                 | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| giftsbymail.co.uk                | Medium Risk | CANNOT BE DEFINED            |
| Double Decker Gift Stores, Ltd   | Medium Risk | LOYAL                        |
| Petit Auto                       | Low Risk    | SLIPPING AWAY, CANNOT LOSE   |
| Marseille Mini Autos             | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Rovelli Gifts                    | High Risk   | CANNOT BE DEFINED            |
| Royal Canadian Collectables, Ltd.| Medium Risk | CANNOT BE DEFINED            |
| Signal Gift Stores               | High Risk   | CANNOT BE DEFINED            |
| Australian Collectables, Ltd     | High Risk   | SLIPPING AWAY, CANNOT LOSE   |
| Bavarian Collectables Imports, Co.| Medium Risk | LOYAL                       |    
   

## Number of Risk Level :
```sql
WITH CUSTOMERRFM AS (
    SELECT
        CUSTOMERNAME,
        ROUND(AVG(SALES), 0) AS AVG_MONETARY_VALUE,
        COUNT(*) AS FREQUENCY,
        MAX(`ORDERDATE_1`) AS RECENCY,
        abs(DATEDIFF(CURDATE(), MAX(`ORDERDATE_1`))) AS RECENCYDAYS
    FROM `sales_data_for_rfm_segmentation`
    GROUP BY CUSTOMERNAME
),

RFM_SEGMENT AS (
    SELECT
        CUSTOMERNAME,
        NTILE(4) OVER (ORDER BY RECENCY DESC) AS RFM_RECENCY,
        NTILE(4) OVER (ORDER BY FREQUENCY DESC) AS RFM_FREQUENCY,
        NTILE(4) OVER (ORDER BY AVG_MONETARY_VALUE DESC) AS RFM_MONETARY_VALUE,
        RECENCYDAYS,
        CASE
            WHEN RECENCYDAYS > 7000 THEN 'High Risk'
            WHEN RECENCYDAYS > 300 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS RiskLevel
    FROM CUSTOMERRFM
)

select RiskLevel, count(RiskLevel) as Number_of_Risk_level from rfm_segment
group by 1
order by 2 desc;
```
--Output--
| RiskLevel   | Number_of_Risk_level |
|-------------|------------------    |
| High Risk   | 57                   |
| Medium Risk | 31                   |
| Low Risk    | 4                    |


-  The analysis reveals a clear distribution of customer risk levels and their corresponding segments:

 -  **Risk Level Distribution:**

1. **High Risk (57 Customers):**
   
    The majority of customers fall into this category, indicating a significant portion of the customer base is at risk of churning or has already 
    disengaged.
   
**Examples** include UK Collectables, Ltd., Dragon Souveniers, Ltd., and Mini Gifts Distributors Ltd., many of whom are marked as Churned Customers or Cannot Be Defined.

- **Action :**  Immediate retention strategies, such as personalized offers, win-back campaigns, or surveys to understand their concerns, are critical.

2. **Medium Risk (31 Customers):**

   These customers, such as The Sharp Gifts Warehouse and Corporate Gift Ideas Co., show moderate engagement but are at risk of slipping further.

- **Action :** Targeted efforts to improve their experience, such as exclusive deals or improved customer service, can help retain them.

3. **Low Risk (4 Customers):**

  A small group of customers, including Diecast Classics Inc. and Marta's Replicas Co., are Active or Loyal and represent stable, low-risk relationships.

- **Action :** Strengthen loyalty through rewards, exclusive perks, or early access to new products.

#  **Key Observations :**
- **Churned Customers (19) :** High-risk customers who have already disengaged. These represent lost revenue and require win-back campaigns.

- **Potential Churners (10) :** High-risk customers showing signs of disengagement. Immediate retention strategies are needed to prevent further loss.

- **Cannot Be Defined (42) :** A large portion of customers lack clear segmentation, highlighting the need for better data collection and analysis.

#  **Actionable Steps :**

1.  **High-Risk Customers :** Focus on re-engagement and retention through personalized outreach and incentives.

2.  **Medium-Risk Customers :** Improve their experience with targeted offers and enhanced support.

3.  **Low-Risk Customers :** Reward and nurture these loyal customers to maintain their engagement.

4.  **Cannot Be Defined :** Enhance data collection and analysis to better understand and segment these customers.



