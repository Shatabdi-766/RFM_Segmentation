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

### Dataset
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

## 
