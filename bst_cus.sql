-- who is our best customer?
SELECT `ORDERDATE_1` 
FROM sales_data_for_rfm_segmentation
LIMIT 5;

SELECT str_to_date(`ORDERDATE_1`,'%d/%m/%y') AS DATE 
FROM sales_data_for_rfm_segmentation
LIMIT 5; 

SELECT max(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS LASTDATE 
FROM sales_data_for_rfm_segmentation;

SELECT MIN(str_to_date(`ORDERDATE_1`,'%d/%m/%y')) AS EARLIESTDATE 
FROM sales_data_for_rfm_segmentation;

SELECT 
datediff(max(str_to_date(`ORDERDATE_1`,'%d/%m/%y')), min(str_to_date(`ORDERDATE_1`,'%d/%m/%y'))) AS `RANGE`
FROM sales_data_for_rfm_segmentation;




