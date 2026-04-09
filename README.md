# Cortex Code Credit Manager

A Snowflake-native solution to control **Snowflake Cortex Code** usage at a **per-user level**.

This project provides:
- per-user daily limits for **Cortex Code CLI**
- per-user daily limits for **Cortex Code Snowsight**
- automated revoke / restore workflows
- scheduled monitoring and enforcement
- a Snowflake Streamlit dashboard for administration and usage visibility

## Why this exists

Most Snowflake Cortex Code setups stop at enabling access. That creates a cost-control gap.

This project adds governance and enforcement by:
- tracking usage by user
- comparing usage against configured daily limits
- revoking access when limits are exceeded
- restoring access on the next daily reset
- exposing controls and usage history through a Snowflake-hosted Streamlit app

## Features

- **User-level daily limits**
  - Separate limits for:
    - Cortex Code CLI
    - Cortex Code Snowsight

- **Automated enforcement**
  - Revokes access when a user exceeds the configured daily limit
  - Uses a dedicated `CORTEX_ACCESS_ROLE`

- **Daily restore**
  - Revoked users are restored automatically at midnight

- **Scheduled monitoring**
  - Tasks run every 15 minutes to:
    - sync eligible users
    - monitor usage
    - apply enforcement

- **Snowflake-native dashboard**
  - View current user status
  - Update user limits
  - Revoke / restore access
  - Perform bulk limit updates
  - View 30-day usage history and per-user breakdown

- **Auto-onboarding**
  - Newly eligible users are inserted automatically with default limits

## Important limitation

Usage monitoring depends on Snowflake `ACCOUNT_USAGE` views, which can have a latency of roughly **2 to 3 hours**.

Because of that, enforcement is **not real-time**. A user can temporarily exceed the configured daily limit before the monitoring task catches up and revokes access.

## Source tables used

This solution reads usage and user metadata from:

- `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY`
- `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY`
- `SNOWFLAKE.ACCOUNT_USAGE.USERS`

These sources are used for:
- per-user usage tracking
- credit consumption tracking
- interface-specific enforcement
- user sync logic

## Repository structure

```text
.
├── screenshots/
│   ├── main.png
│   ├── manage_accountadmin_only.png
│   └── usage_history.png
├── sqls/
│   ├── cortex_permissions.sql
│   ├── cortex_cli_rate_limit_tasks.sql
│   └── cortex_snowsight_rate_limit_tasks.sql
├── python/
│   └── cortex_code_credit_manager.py
└── README.md
