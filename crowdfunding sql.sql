
# location 
create table location(
id text ,
displayable_name text,
type text,
name text,
state text,
short_name text,
is_root text,
country text,
localized_name text);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Crowdfunding_Location.csv'
INTO TABLE location
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
alter table location modify column id int primary key;
select count(*) from location; 


# creators 
CREATE TABLE creators (
    creator_id TEXT,
    name TEXT,
    chosen_currency TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Crowdfunding_Creator.csv'
INTO TABLE creators
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
alter table creators modify column creator_id int primary key;
desc creators;
select count(*) from creators;

# 1 converting epoch -> date format
CREATE OR REPLACE VIEW vw_projects AS 
SELECT     
    p.ProjectID,     
    p.state,                         
    p.name,     
    p.country,     
    p.creator_id,     
    p.location_id,     
    p.category_id,      

    -- epoch -> DATETIME (UTC)    
    FROM_UNIXTIME(NULLIF(p.created_at, 0))        AS created_dt,     
    FROM_UNIXTIME(NULLIF(p.deadline, 0))          AS deadline_dt,     
    FROM_UNIXTIME(NULLIF(p.updated_at, 0))        AS updated_dt,     
    FROM_UNIXTIME(NULLIF(p.state_changed_at, 0))  AS state_changed_dt,     
    FROM_UNIXTIME(NULLIF(p.successful_at, 0))     AS successful_dt,     
    FROM_UNIXTIME(NULLIF(p.launched_at, 0))       AS launched_dt,      

    -- keeping raw epoch values     
    p.created_at, 
    p.deadline, 
    p.updated_at, 
    p.state_changed_at, 
    p.successful_at, 
    p.launched_at,  

    -- money & backers     
    p.goal,     
    p.pledged,     
    p.currency,     
    p.usd_pledged,     
    p.static_usd_rate,     
    p.backers_count,      

    -- 4 converting money fields (using static USD rate)     
    ROUND(p.goal * p.static_usd_rate, 2)    AS goal_usd,     
    ROUND(p.pledged * p.static_usd_rate, 2) AS pledged_usd 

FROM projects AS p;
SELECT * FROM vw_projects LIMIT 10;

-- Allow deeper recursion for CTE
SET SESSION cte_max_recursion_depth = 1000000;
# 2 calendar table
CREATE TABLE dim_calendar (
    dt DATE PRIMARY KEY,
    year_num INT,
    month_no TINYINT,
    month_name VARCHAR(20),
    quarter_label CHAR(2),
    weekday_no TINYINT,
    weekday_name VARCHAR(10),
    financial_month_label CHAR(4),
    financial_quarter_label CHAR(4)
);

INSERT INTO dim_calendar (
    dt, year_num, month_no, month_name, quarter_label, weekday_no, weekday_name,
    financial_month_label, financial_quarter_label
)
WITH RECURSIVE dates AS (
    SELECT DATE(MIN(created_dt)) AS dt, DATE(MAX(created_dt)) AS dmax
    FROM vw_projects
    UNION ALL
    SELECT DATE_ADD(dt, INTERVAL 1 DAY), dmax
    FROM dates
    WHERE dt < dmax
)
SELECT
    dt,
    YEAR(dt) AS year_num,
    MONTH(dt) AS month_no,
    DATE_FORMAT(dt, '%M') AS month_name,
    CONCAT('Q', QUARTER(dt)) AS quarter_label,
    WEEKDAY(dt) + 1 AS weekday_no,
    DATE_FORMAT(dt, '%W') AS weekday_name,

    -- Financial Month (April = FM1, ..., March = FM12)
    CONCAT('FM', ((MONTH(dt) + 9) % 12) + 1) AS financial_month_label,

    -- Financial Quarter based on financial month
    CONCAT('FQ', ((MONTH(dt) + 8) DIV 3) % 4 + 1) AS financial_quarter_label
FROM dates;
SELECT * FROM dim_calendar LIMIT 10;

# 5-a.Total number of projects by outcome
SELECT
  state AS outcome,
  COUNT(*) AS total_projects
FROM vw_projects
GROUP BY state
ORDER BY total_projects DESC;

# 5-b Total number of projects by location country level
SELECT
  country,
  COUNT(*) AS total_projects
FROM vw_projects
GROUP BY country
ORDER BY total_projects DESC;

# 5-b Total number of projects by location city /area level
SELECT
  COALESCE(l.displayable_name, l.name, CONCAT(l.localized_name, '')) AS location_label,
  COUNT(*) AS total_projects
FROM vw_projects p
LEFT JOIN location l ON p.location_id = l.id
GROUP BY location_label
ORDER BY total_projects DESC;

# 5-c Total number of projects by category
SELECT
  c.name AS category,
  COUNT(*) AS total_projects
FROM vw_projects p
INNER JOIN category c ON p.category_id = c.id
GROUP BY c.name
ORDER BY total_projects DESC;


# 5-d Total projects created by Year / Quarter / Month
-- By Year
SELECT dc.year_num AS year, COUNT(*) AS total_projects
FROM vw_projects p
JOIN dim_calendar dc ON DATE(p.created_dt) = dc.dt
GROUP BY dc.year_num
ORDER BY dc.year_num;

-- By Year + Quarter
SELECT dc.year_num AS year, dc.quarter_label AS quarter, COUNT(*) AS total_projects
FROM vw_projects p
JOIN dim_calendar dc ON DATE(p.created_dt) = dc.dt
GROUP BY dc.year_num, dc.quarter_label
ORDER BY dc.year_num, dc.quarter_label;

-- By Year + Month 
SELECT dc.month_name,dc.year_num, COUNT(*) AS total_projects
FROM vw_projects p
JOIN dim_calendar dc ON DATE(p.created_dt) = dc.dt
GROUP BY dc.year_num,dc.month_name
ORDER BY MIN(dc.dt);

# 6 succesccful projects - 
# 6-a amount raised (usd)
SELECT
  CONCAT('$', FORMAT(SUM(p.pledged_usd), 2)) AS amount_raised_usd
FROM vw_projects p
WHERE p.state = 'successful';

# 6-b number of backers
SELECT
  COALESCE(SUM(p.backers_count), 0) AS total_backers
FROM vw_projects p
WHERE p.state = 'successful';

# 6-c avg no of days for successful projects from launch to success
SELECT
  ROUND(AVG(DATEDIFF(p.successful_dt, p.launched_dt)), 2) AS avg_days_to_success
FROM vw_projects p
WHERE p.state = 'successful'
  AND p.successful_dt IS NOT NULL
  AND p.launched_dt IS NOT NULL;

# 7 top sucessful projects
# 7-a by no of backers
SELECT
ProjectId,name, backers_count, pledged_usd, goal_usd
FROM vw_projects 
WHERE state = 'successful'
ORDER BY backers_count DESC
LIMIT 10;  

# 7-b based on amount raised (usd)
SELECT
  projectid, p.name, p.pledged_usd, p.backers_count, p.goal_usd
FROM vw_projects p
WHERE p.state = 'successful'
ORDER BY p.pledged_usd DESC
LIMIT 10;

# 8-a Percentage of Successful Projects overall
SELECT
  ROUND(100.0 * SUM(state = 'successful') / COUNT(*), 2) AS pct_success_overall
FROM vw_projects;

# 8-b Percentage of Successful Projects  by Category
SELECT
  c.name AS category,
  COUNT(*) AS total_projects,
  SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) AS successful_projects,
  ROUND(100.0 * SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_success
FROM vw_projects p
LEFT JOIN category c ON p.category_id = c.id
GROUP BY c.name
ORDER BY pct_success DESC, total_projects DESC;

# 8-c Percentage of Successful Projects by Year , Month etc..
-- By Year
SELECT
  dc.year_num AS year,
  COUNT(*) AS total_projects,
  SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) AS successful_projects,
  ROUND(100.0 * SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_success
FROM vw_projects p
JOIN dim_calendar dc ON DATE(p.created_dt) = dc.dt
GROUP BY dc.year_num
ORDER BY dc.year_num;

-- By Year-Month
SELECT
 dc.year_num, dc.month_name,
  COUNT(*) AS total_projects,
  SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) AS successful_projects,
  ROUND(100.0 * SUM(CASE WHEN p.state = 'successful' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_success
FROM vw_projects p
JOIN dim_calendar dc ON DATE(p.created_dt) = dc.dt
GROUP BY dc.month_name ,year_num
ORDER BY MIN(dc.dt);

# 8-d Percentage of Successful projects by Goal Range 
SELECT
  CASE
    WHEN goal_usd <    5000 THEN 'USD 0–4.9k'
    WHEN goal_usd <   10000 THEN 'USD 5k–9.9k'
    WHEN goal_usd <   50000 THEN 'USD 10k–49.9k'
    WHEN goal_usd <  100000 THEN 'USD 50k–99.9k'
    ELSE                    'USD 100k+'
  END AS goal_band,
  COUNT(*) AS total_projects,
  SUM(state = 'successful') AS successful_projects,
  ROUND(100.0 * SUM(state = 'successful') / COUNT(*), 2) AS pct_success
FROM vw_projects
GROUP BY goal_band
ORDER BY
  CASE goal_band
    WHEN 'USD 0–4.9k'    THEN 1
    WHEN 'USD 5k–9.9k'   THEN 2
    WHEN 'USD 10k–49.9k' THEN 3
    WHEN 'USD 50k–99.9k' THEN 4
    WHEN 'USD 100k+'     THEN 5
  END;