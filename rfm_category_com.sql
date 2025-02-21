CREATE VIEW RFM_SEGMENT AS
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

SELECT * FROM RFM_SEGMENT;





