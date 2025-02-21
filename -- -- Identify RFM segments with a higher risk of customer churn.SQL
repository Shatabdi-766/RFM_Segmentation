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
    CUSTOMER_SEGMENT,
    CUSTOMERNAME,
    RiskLevel,
    COUNT(*) AS NUMBER_OF_CUSTOMERS
FROM
    CTE1
GROUP BY CUSTOMER_SEGMENT , CUSTOMERNAME , RiskLevel
ORDER BY NUMBER_OF_CUSTOMERS DESC


    
   
