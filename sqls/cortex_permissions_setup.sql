-- ============================================================================
-- Snowflake Cortex Code Access Bootstrap
-- File: cortex_permissions_commented.sql
-- Purpose:
--   1) Remove inherited / default Cortex-related database role access.
--   2) Create a dedicated revocable role for Cortex Code access.
--   3) Grant that role directly to allowed users.
--
-- Run as: ACCOUNTADMIN
-- Notes:
--   - This script is intended as a one-time or occasional access bootstrap.
--   - It restructures access so user-level revocation is possible later from the
--     rate-limit procedures and Streamlit dashboard.
--   - Keep the user grant section aligned with your actual allow-list.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Optional hardening for specific users:
-- Clears default secondary roles for the listed users before role restructuring.
-- Keep or remove these statements based on your account-level access model.
-- ----------------------------------------------------------------------------
ALTER USER "MARMIK.PATEL@GRIMCO.COM" SET DEFAULT_SECONDARY_ROLES = ();
ALTER USER "UMANGI.PATEL@GRIMCO.COM" SET DEFAULT_SECONDARY_ROLES = ();

-- ----------------------------------------------------------------------------
-- Step 1: Remove Cortex-related database roles from broad admin / inherited roles
-- This prevents users from retaining access through inherited paths after a
-- direct user-level revoke of CORTEX_ACCESS_ROLE.
-- ----------------------------------------------------------------------------
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER       FROM ROLE ACCOUNTADMIN;
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER FROM ROLE ACCOUNTADMIN;
REVOKE DATABASE ROLE SNOWFLAKE.COPILOT_USER      FROM ROLE ACCOUNTADMIN;

REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER       FROM ROLE "CSG_SNOWFLAKE-SYSADMIN";
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER FROM ROLE "CSG_SNOWFLAKE-SYSADMIN";
REVOKE DATABASE ROLE SNOWFLAKE.COPILOT_USER      FROM ROLE "CSG_SNOWFLAKE-SYSADMIN";

REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER       FROM ROLE "ADF_INTEGRATION_ROLE";
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER FROM ROLE "ADF_INTEGRATION_ROLE";
REVOKE DATABASE ROLE SNOWFLAKE.COPILOT_USER      FROM ROLE "ADF_INTEGRATION_ROLE";

-- ----------------------------------------------------------------------------
-- Step 2: Remove broad grants from PUBLIC and SYSADMIN.
-- These are the main inherited access paths this implementation is closing.
-- ----------------------------------------------------------------------------
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER  FROM ROLE PUBLIC;
REVOKE DATABASE ROLE SNOWFLAKE.COPILOT_USER FROM ROLE PUBLIC;

REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER  FROM ROLE SYSADMIN;
REVOKE DATABASE ROLE SNOWFLAKE.COPILOT_USER FROM ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- Step 3: Create a dedicated revocable role used by the governance framework.
-- Rate-limit procedures later grant / revoke this role directly at user level.
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS CORTEX_ACCESS_ROLE;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER       TO ROLE CORTEX_ACCESS_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE CORTEX_ACCESS_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER      TO ROLE CORTEX_ACCESS_ROLE;

-- ----------------------------------------------------------------------------
-- Step 4: Grant CORTEX_ACCESS_ROLE to approved users.
-- You can either:
--   A) Generate statements dynamically from SHOW USERS, or
--   B) Run the explicit grant list below.
--
-- Important:
--   - Review the generated output before executing it.
--   - Do not blindly grant every active user unless that is intended.
-- ----------------------------------------------------------------------------

-- Option A: Generate GRANT statements for active users.
SHOW USERS;
SELECT 'GRANT ROLE CORTEX_ACCESS_ROLE TO USER "' || "name" || '";'
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "disabled" = 'false';

-- Option B: Explicit allow-list.
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "ANDREW.ROBY@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "CROUTLEDGE@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "JON.HELLER@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "KKUNKEL@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "KSPELLAZZA@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "MARK.FOWLER@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "MARMIK.PATEL@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "MATTHEW.GELESKE@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "MATTHEW.MCGUIRK@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "MFOWLER";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "NCLARK@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "OPENFLOW_USER";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "SVC_AZURE_DATAFACTORY";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "SVC_AZURE_DEVOPS";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "THOMAS.STARR@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "UMANGI.PATEL@GRIMCO.COM";
GRANT ROLE CORTEX_ACCESS_ROLE TO USER "VFUGMAN@GRIMCO.COM";

-- ----------------------------------------------------------------------------
-- Suggested validation queries
-- ----------------------------------------------------------------------------
-- SHOW GRANTS OF ROLE CORTEX_ACCESS_ROLE;
-- SHOW GRANTS TO USER "MARMIK.PATEL@GRIMCO.COM";
-- SHOW GRANTS TO USER "UMANGI.PATEL@GRIMCO.COM";
