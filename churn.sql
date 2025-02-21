-- Ensure that these indexes exist for optimized performance
-- CREATE INDEX idx_activity_pretest_userid_dt ON Activity_pretest (userid, dt);
-- CREATE INDEX idx_assignments_dt_conversion_userid_dt ON assignments_dt_conversion (userid, dt);

WITH cte_table1 AS (
    SELECT userid, dt, activity_level 
    FROM Activity_pretest
),
cte_table2 AS (
    SELECT userid, dt, group_id 
    FROM assignments_dt_conversion 
),
cte_table1_combined AS (
    SELECT 
        userid, 
        dt, 
        activity_level, 
        0 AS group_id 
    FROM 
        cte_table1
),
cte_table2_combined AS (
    SELECT 
        userid, 
        dt, 
        0 AS activity_level, 
        group_id 
    FROM 
        cte_table2
)
SELECT 
    COALESCE(t1.userid, t2.userid) AS userid,
    COALESCE(t1.dt, t2.dt) AS dt,
    COALESCE(t1.activity_level, 0) AS activity_level,
    COALESCE(t2.group_id, 0) AS group_id
FROM 
    cte_table1_combined AS t1
LEFT JOIN 
    cte_table2_combined AS t2
ON 
    t1.userid = t2.userid
AND 
    t1.dt = t2.dt
UNION ALL
SELECT 
    COALESCE(t1.userid, t2.userid) AS userid,
    COALESCE(t1.dt, t2.dt) AS dt,
    COALESCE(t1.activity_level, 0) AS activity_level,
    COALESCE(t2.group_id, 0) AS group_id
FROM 
    cte_table2_combined AS t2
LEFT JOIN 
    cte_table1_combined AS t1
ON 
    t2.userid = t1.userid
AND 
    t2.dt = t1.dt
WHERE 
    t1.userid IS NULL;
