-- ============================================================================
-- Snowflake Cortex Code Snowsight Daily Limit Framework
-- File: cortex_snowsight_rate_limit_tasks_commented.sql
-- Purpose:
--   1) Maintain a per-user daily USD limit table for Cortex Code Snowsight usage.
--   2) Auto-enroll users who have CORTEX_ACCESS_ROLE.
--   3) Revoke CORTEX_ACCESS_ROLE when the user's estimated daily Snowsight cost
--      exceeds the configured limit.
--   4) Restore revoked users daily at midnight UTC.
--   5) Schedule recurring sync / enforce / restore tasks.
--
-- Run as: ACCOUNTADMIN
-- Database / schema used: ADMIN.COST_MANAGEMENT
-- Usage source:
--   SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
-- Cost logic used in this script:
--   estimated_cost_usd = SUM(token_credits) * 2
--
-- Important implementation detail:
--   This framework revokes the shared CORTEX_ACCESS_ROLE. Because CLI and
--   Snowsight use the same revocable role in this implementation, revocation by
--   one framework removes access until a restore grants the role back.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ADMIN;
CREATE SCHEMA IF NOT EXISTS COST_MANAGEMENT;
USE SCHEMA COST_MANAGEMENT;

-- ----------------------------------------------------------------------------
-- Step 1: Control table for Snowsight daily limits
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CORTEX_SNOWSIGHT_DAILY_LIMITS (
    USER_NAME       VARCHAR PRIMARY KEY,
    DAILY_USD_LIMIT NUMBER(10,2) DEFAULT 1.00,
    IS_REVOKED      BOOLEAN DEFAULT FALSE,
    REVOKED_AT      TIMESTAMP_LTZ
);

-- ----------------------------------------------------------------------------
-- Step 2: Auto-enroll users who currently hold CORTEX_ACCESS_ROLE.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SYNC_CORTEX_SNOWSIGHT_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_count NUMBER DEFAULT 0;
BEGIN
    SHOW GRANTS OF ROLE CORTEX_ACCESS_ROLE;

    MERGE INTO CORTEX_SNOWSIGHT_DAILY_LIMITS AS t
    USING (
        SELECT "grantee_name" AS USER_NAME
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "granted_to" = 'USER'
    ) AS s
    ON t.USER_NAME = s.USER_NAME
    WHEN NOT MATCHED THEN
        INSERT (USER_NAME)
        VALUES (s.USER_NAME);

    v_count := SQLROWCOUNT;
    RETURN 'Inserted ' || v_count || ' new user(s)';
END;
$$;

-- ----------------------------------------------------------------------------
-- Step 3: Enforcement procedure
-- Logic:
--   - Look up each governed user.
--   - Read today's Snowsight usage from ACCOUNT_USAGE.
--   - Estimate USD cost as SUM(token_credits) * 2.
--   - Revoke CORTEX_ACCESS_ROLE for users who exceed DAILY_USD_LIMIT.
--   - Mark the user as revoked in the tracking table.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ENFORCE_CORTEX_SNOWSIGHT_DAILY_LIMITS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var count = 0;
    var rs = snowflake.execute({sqlText: `
        SELECT l.USER_NAME
        FROM ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS l
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
            ON u.name = l.USER_NAME
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY c
            ON c.user_id = u.user_id
           AND DATE(c.usage_time) = CURRENT_DATE()
        WHERE l.IS_REVOKED = FALSE
        GROUP BY l.USER_NAME, l.DAILY_USD_LIMIT
        HAVING COALESCE(SUM(c.token_credits), 0) * 2 > l.DAILY_USD_LIMIT
    `});

    while (rs.next()) {
        var user = rs.getColumnValue(1);

        snowflake.execute({sqlText: 'REVOKE ROLE CORTEX_ACCESS_ROLE FROM USER "' + user + '"'});
        snowflake.execute({sqlText: `
            UPDATE ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS
            SET IS_REVOKED = TRUE,
                REVOKED_AT = CURRENT_TIMESTAMP()
            WHERE USER_NAME = '` + user + `'
        `});
        count++;
    }

    return 'Revoked ' + count + ' user(s)';
$$;

-- ----------------------------------------------------------------------------
-- Step 4: Restore procedure
-- Re-grants CORTEX_ACCESS_ROLE to all users currently marked as revoked in the
-- Snowsight control table, then clears the revoke flags.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE RESTORE_CORTEX_SNOWSIGHT_ACCESS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var count = 0;
    var rs = snowflake.execute({sqlText: `
        SELECT USER_NAME
        FROM ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS
        WHERE IS_REVOKED = TRUE
    `});

    while (rs.next()) {
        var user = rs.getColumnValue(1);

        snowflake.execute({sqlText: 'GRANT ROLE CORTEX_ACCESS_ROLE TO USER "' + user + '"'});
        snowflake.execute({sqlText: `
            UPDATE ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS
            SET IS_REVOKED = FALSE,
                REVOKED_AT = NULL
            WHERE USER_NAME = '` + user + `'
        `});
        count++;
    }

    return 'Restored ' + count + ' user(s)';
$$;

-- ----------------------------------------------------------------------------
-- Step 5: Scheduled tasks
-- CHECK_CORTEX_SNOWSIGHT_LIMITS        : enforce every 15 minutes
-- SYNC_CORTEX_SNOWSIGHT_USERS_TASK     : add newly granted users every 15 minutes
-- RESTORE_CORTEX_SNOWSIGHT_ACCESS_DAILY: restore all revoked users at midnight UTC
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK CHECK_CORTEX_SNOWSIGHT_LIMITS
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON */15 * * * * UTC'
AS
    CALL ENFORCE_CORTEX_SNOWSIGHT_DAILY_LIMITS();

CREATE OR REPLACE TASK SYNC_CORTEX_SNOWSIGHT_USERS_TASK
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON */15 * * * * UTC'
AS
    CALL SYNC_CORTEX_SNOWSIGHT_USERS();

CREATE OR REPLACE TASK RESTORE_CORTEX_SNOWSIGHT_ACCESS_DAILY
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON 0 0 * * * UTC'
AS
    CALL RESTORE_CORTEX_SNOWSIGHT_ACCESS();

ALTER TASK CHECK_CORTEX_SNOWSIGHT_LIMITS RESUME;
ALTER TASK RESTORE_CORTEX_SNOWSIGHT_ACCESS_DAILY RESUME;
ALTER TASK SYNC_CORTEX_SNOWSIGHT_USERS_TASK RESUME;

-- ----------------------------------------------------------------------------
-- Helper query: current Snowsight daily spend estimate per user
-- Note: the original file also included helper examples using 3.36x in comments.
-- This commented version aligns helper examples to the active enforcement logic
-- in the procedure above, which uses 2x.
-- ----------------------------------------------------------------------------
-- SELECT
--     l.USER_NAME,
--     l.DAILY_USD_LIMIT,
--     l.IS_REVOKED,
--     COALESCE(SUM(c.token_credits), 0) * 2 AS estimated_cost_usd_today
-- FROM ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS l
-- LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
--     ON u.name = l.USER_NAME
-- LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY c
--     ON c.user_id = u.user_id
--    AND DATE(c.usage_time) = CURRENT_DATE()
-- GROUP BY l.USER_NAME, l.DAILY_USD_LIMIT, l.IS_REVOKED
-- ORDER BY estimated_cost_usd_today DESC, l.USER_NAME;

-- ----------------------------------------------------------------------------
-- Helper query: per-user usage by date for the last 30 days
-- ----------------------------------------------------------------------------
-- SELECT
--     DATE(c.usage_time) AS usage_date,
--     u.name AS user_name,
--     COUNT(c.request_id) AS total_requests,
--     SUM(c.tokens) AS total_tokens,
--     ROUND(SUM(c.token_credits), 4) AS total_credits,
--     ROUND(SUM(c.token_credits) * 2, 2) AS estimated_cost_usd
-- FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY c
-- LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
--     ON c.user_id = u.user_id
-- WHERE c.usage_time >= DATEADD('month', -1, CURRENT_DATE())
-- GROUP BY DATE(c.usage_time), u.name
-- ORDER BY usage_date DESC, total_credits DESC;

-- ----------------------------------------------------------------------------
-- Suggested setup order
--   1) Run cortex_permissions_commented.sql
--   2) Run cortex_rate_limit_tasks_commented.sql
--   3) Run this file to create Snowsight governance objects
--   4) Validate helper query and task status
-- ----------------------------------------------------------------------------
-- SHOW TASKS LIKE 'CHECK_CORTEX_SNOWSIGHT_LIMITS' IN SCHEMA ADMIN.COST_MANAGEMENT;
-- SHOW TASKS LIKE 'SYNC_CORTEX_SNOWSIGHT_USERS_TASK' IN SCHEMA ADMIN.COST_MANAGEMENT;
-- SHOW TASKS LIKE 'RESTORE_CORTEX_SNOWSIGHT_ACCESS_DAILY' IN SCHEMA ADMIN.COST_MANAGEMENT;
