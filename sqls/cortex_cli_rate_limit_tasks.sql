-- ============================================================================
-- Snowflake Cortex Code CLI Daily Limit Framework
-- File: cortex_rate_limit_tasks_commented.sql
-- Purpose:
--   1) Maintain a per-user daily USD limit table for Cortex Code CLI usage.
--   2) Auto-enroll users who have CORTEX_ACCESS_ROLE.
--   3) Revoke CORTEX_ACCESS_ROLE when the user's estimated daily CLI cost
--      exceeds the configured limit.
--   4) Restore revoked users daily at midnight UTC.
--   5) Schedule recurring sync / enforce / restore tasks.
--
-- Run as: ACCOUNTADMIN
-- Database / schema used: ADMIN.COST_MANAGEMENT
-- Usage source:
--   SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
-- Cost logic used in this script:
--   estimated_cost_usd = SUM(token_credits) * 3
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
-- Step 1: Control table for CLI daily limits
-- DAILY_USD_LIMIT : configured max estimated CLI spend per day for the user
-- IS_REVOKED      : whether the role was revoked by this framework
-- REVOKED_AT      : timestamp of the last revoke event
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CORTEX_CLI_DAILY_LIMITS (
    USER_NAME       VARCHAR PRIMARY KEY,
    DAILY_USD_LIMIT NUMBER(10,2) DEFAULT 1.00,
    IS_REVOKED      BOOLEAN DEFAULT FALSE,
    REVOKED_AT      TIMESTAMP_LTZ
);

-- ----------------------------------------------------------------------------
-- Step 2: Auto-enroll users who currently hold CORTEX_ACCESS_ROLE.
-- This procedure inserts only missing users and leaves existing limits intact.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SYNC_CORTEX_CLI_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_count NUMBER DEFAULT 0;
BEGIN
    SHOW GRANTS OF ROLE CORTEX_ACCESS_ROLE;

    MERGE INTO CORTEX_CLI_DAILY_LIMITS AS t
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
--   - Read today's CLI usage from ACCOUNT_USAGE.
--   - Estimate USD cost as SUM(token_credits) * 3.
--   - Revoke CORTEX_ACCESS_ROLE for users who exceed DAILY_USD_LIMIT.
--   - Mark the user as revoked in the tracking table.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ENFORCE_CORTEX_CLI_DAILY_LIMITS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var count = 0;
    var rs = snowflake.execute({sqlText: `
        SELECT l.USER_NAME
        FROM ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS l
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
            ON u.name = l.USER_NAME
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY c
            ON c.user_id = u.user_id
           AND DATE(c.usage_time) = CURRENT_DATE()
        WHERE l.IS_REVOKED = FALSE
        GROUP BY l.USER_NAME, l.DAILY_USD_LIMIT
        HAVING COALESCE(SUM(c.token_credits), 0) * 3 > l.DAILY_USD_LIMIT
    `});

    while (rs.next()) {
        var user = rs.getColumnValue(1);

        snowflake.execute({sqlText: 'REVOKE ROLE CORTEX_ACCESS_ROLE FROM USER "' + user + '"'});
        snowflake.execute({sqlText: `
            UPDATE ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS
            SET IS_REVOKED = TRUE,
                REVOKED_AT = CURRENT_TIMESTAMP()
            WHERE USER_NAME = '` + user + `'
        `});
        count++;
    }

    return 'Revoked ' + count + ' user(s)';
$$;

-- ----------------------------------------------------------------------------
-- Optional manual test statements used during validation.
-- Keep commented unless you intentionally want to simulate a revoke or trigger
-- the procedure manually.
-- ----------------------------------------------------------------------------
-- REVOKE ROLE CORTEX_ACCESS_ROLE FROM USER "UMANGI.PATEL@GRIMCO.COM";
-- UPDATE ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS
-- SET IS_REVOKED = TRUE,
--     REVOKED_AT = CURRENT_TIMESTAMP()
-- WHERE USER_NAME = 'UMANGI.PATEL@GRIMCO.COM';
-- CALL ENFORCE_CORTEX_CLI_DAILY_LIMITS();

-- ----------------------------------------------------------------------------
-- Step 4: Restore procedure
-- Re-grants CORTEX_ACCESS_ROLE to all users currently marked as revoked in the
-- CLI control table, then clears the revoke flags.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE RESTORE_CORTEX_CLI_ACCESS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var count = 0;
    var rs = snowflake.execute({sqlText: `
        SELECT USER_NAME
        FROM ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS
        WHERE IS_REVOKED = TRUE
    `});

    while (rs.next()) {
        var user = rs.getColumnValue(1);

        snowflake.execute({sqlText: 'GRANT ROLE CORTEX_ACCESS_ROLE TO USER "' + user + '"'});
        snowflake.execute({sqlText: `
            UPDATE ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS
            SET IS_REVOKED = FALSE,
                REVOKED_AT = NULL
            WHERE USER_NAME = '` + user + `'
        `});
        count++;
    }

    return 'Restored ' + count + ' user(s)';
$$;

-- Optional manual run
-- CALL RESTORE_CORTEX_CLI_ACCESS();

-- ----------------------------------------------------------------------------
-- Step 5: Scheduled tasks
-- CHECK_CORTEX_CLI_LIMITS        : enforce every 15 minutes
-- SYNC_CORTEX_CLI_USERS_TASK     : add newly granted users every 15 minutes
-- RESTORE_CORTEX_CLI_ACCESS_DAILY: restore all revoked users at midnight UTC
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK CHECK_CORTEX_CLI_LIMITS
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON */15 * * * * UTC'
AS
    CALL ENFORCE_CORTEX_CLI_DAILY_LIMITS();

CREATE OR REPLACE TASK SYNC_CORTEX_CLI_USERS_TASK
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON */15 * * * * UTC'
AS
    CALL SYNC_CORTEX_CLI_USERS();

CREATE OR REPLACE TASK RESTORE_CORTEX_CLI_ACCESS_DAILY
    WAREHOUSE = RAW_EDW_WH_S
    SCHEDULE  = 'USING CRON 0 0 * * * UTC'
AS
    CALL RESTORE_CORTEX_CLI_ACCESS();

ALTER TASK CHECK_CORTEX_CLI_LIMITS RESUME;
ALTER TASK RESTORE_CORTEX_CLI_ACCESS_DAILY RESUME;
ALTER TASK SYNC_CORTEX_CLI_USERS_TASK RESUME;

-- ----------------------------------------------------------------------------
-- Helper query: current CLI daily spend estimate per user
-- ----------------------------------------------------------------------------
-- SELECT
--     l.USER_NAME,
--     l.DAILY_USD_LIMIT,
--     l.IS_REVOKED,
--     COALESCE(SUM(c.token_credits), 0) * 3 AS estimated_cost_usd_today
-- FROM ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS l
-- LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
--     ON u.name = l.USER_NAME
-- LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY c
--     ON c.user_id = u.user_id
--    AND DATE(c.usage_time) = CURRENT_DATE()
-- GROUP BY l.USER_NAME, l.DAILY_USD_LIMIT, l.IS_REVOKED
-- ORDER BY estimated_cost_usd_today DESC, l.USER_NAME;

-- ----------------------------------------------------------------------------
-- Suggested setup order
--   1) Run cortex_permissions_commented.sql
--   2) Run this file to create CLI governance objects
--   3) Validate helper query and task status
-- ----------------------------------------------------------------------------
-- SHOW TASKS LIKE 'CHECK_CORTEX_CLI_LIMITS' IN SCHEMA ADMIN.COST_MANAGEMENT;
-- SHOW TASKS LIKE 'SYNC_CORTEX_CLI_USERS_TASK' IN SCHEMA ADMIN.COST_MANAGEMENT;
-- SHOW TASKS LIKE 'RESTORE_CORTEX_CLI_ACCESS_DAILY' IN SCHEMA ADMIN.COST_MANAGEMENT;
