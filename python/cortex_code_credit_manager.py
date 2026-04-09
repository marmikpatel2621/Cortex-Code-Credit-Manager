import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Cortex Code Credit Manager", layout="wide")

session = get_active_session()

if "active_tab" not in st.session_state:
    st.session_state.active_tab = None

def refresh_data():
    st.cache_data.clear()

CLI_TABLE = "ADMIN.COST_MANAGEMENT.CORTEX_CLI_DAILY_LIMITS"
SNOWSIGHT_TABLE = "ADMIN.COST_MANAGEMENT.CORTEX_SNOWSIGHT_DAILY_LIMITS"
CLI_USAGE = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY"
SNOWSIGHT_USAGE = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY"
USERS_VIEW = "SNOWFLAKE.ACCOUNT_USAGE.USERS"
CREDIT_COST_USD = 2
CLI_MULTIPLIER = CREDIT_COST_USD
SNOWSIGHT_MULTIPLIER = CREDIT_COST_USD
ADMIN_ROLES = ["ACCOUNTADMIN"]

current_role = session.get_current_role().replace('"', '')
is_admin = current_role in ADMIN_ROLES


@st.cache_data(ttl=60)
def load_accountadmin_users():
    query = """
    SELECT "grantee_name" AS USER_NAME
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "granted_to" = 'USER'
    """
    session.sql("SHOW GRANTS OF ROLE ACCOUNTADMIN").collect()
    return set(session.sql(query).to_pandas()["USER_NAME"].tolist())


@st.cache_data(ttl=60)
def load_combined_status():
    query = f"""
    WITH cli_spend AS (
        SELECT u.name AS USER_NAME,
               COALESCE(SUM(c.token_credits), 0) * {CLI_MULTIPLIER} AS spend_usd
        FROM {USERS_VIEW} u
        LEFT JOIN {CLI_USAGE} c
            ON c.user_id = u.user_id AND DATE(c.usage_time) = CURRENT_DATE()
        GROUP BY u.name
    ),
    snowsight_spend AS (
        SELECT u.name AS USER_NAME,
               COALESCE(SUM(c.token_credits), 0) * {SNOWSIGHT_MULTIPLIER} AS spend_usd
        FROM {USERS_VIEW} u
        LEFT JOIN {SNOWSIGHT_USAGE} c
            ON c.user_id = u.user_id AND DATE(c.usage_time) = CURRENT_DATE()
        GROUP BY u.name
    )
    SELECT
        COALESCE(cl.USER_NAME, sl.USER_NAME) AS USER_NAME,
        cl.DAILY_USD_LIMIT AS CLI_LIMIT,
        cl.IS_REVOKED AS CLI_REVOKED,
        cl.REVOKED_AT AS CLI_REVOKED_AT,
        ROUND(COALESCE(cs.spend_usd, 0), 4) AS CLI_SPEND_TODAY,
        ROUND(COALESCE(cs.spend_usd, 0) / {CREDIT_COST_USD}, 4) AS CLI_CREDITS_TODAY,
        sl.DAILY_USD_LIMIT AS SNOWSIGHT_LIMIT,
        sl.IS_REVOKED AS SNOWSIGHT_REVOKED,
        sl.REVOKED_AT AS SNOWSIGHT_REVOKED_AT,
        ROUND(COALESCE(ss.spend_usd, 0), 4) AS SNOWSIGHT_SPEND_TODAY,
        ROUND(COALESCE(ss.spend_usd, 0) / {CREDIT_COST_USD}, 4) AS SNOWSIGHT_CREDITS_TODAY
    FROM {CLI_TABLE} cl
    FULL OUTER JOIN {SNOWSIGHT_TABLE} sl ON cl.USER_NAME = sl.USER_NAME
    LEFT JOIN cli_spend cs ON cs.USER_NAME = COALESCE(cl.USER_NAME, sl.USER_NAME)
    LEFT JOIN snowsight_spend ss ON ss.USER_NAME = COALESCE(cl.USER_NAME, sl.USER_NAME)
    ORDER BY COALESCE(cl.USER_NAME, sl.USER_NAME)
    """
    return session.sql(query).to_pandas()


@st.cache_data(ttl=60)
def load_history(days=30):
    query = f"""
    SELECT DATE(c.usage_time) AS USAGE_DATE, u.name AS USER_NAME,
           'CLI' AS SOURCE,
           ROUND(SUM(c.token_credits) * {CLI_MULTIPLIER}, 2) AS COST_USD,
           ROUND(SUM(c.token_credits), 4) AS CREDITS_USED
    FROM {CLI_USAGE} c
    JOIN {USERS_VIEW} u ON c.user_id = u.user_id
    WHERE c.usage_time >= DATEADD('day', -{days}, CURRENT_DATE())
    GROUP BY 1,2
    UNION ALL
    SELECT DATE(c.usage_time) AS USAGE_DATE, u.name AS USER_NAME,
           'Snowsight' AS SOURCE,
           ROUND(SUM(c.token_credits) * {SNOWSIGHT_MULTIPLIER}, 2) AS COST_USD,
           ROUND(SUM(c.token_credits), 4) AS CREDITS_USED
    FROM {SNOWSIGHT_USAGE} c
    JOIN {USERS_VIEW} u ON c.user_id = u.user_id
    WHERE c.usage_time >= DATEADD('day', -{days}, CURRENT_DATE())
    GROUP BY 1,2
    ORDER BY 1 DESC, 4 DESC
    """
    return session.sql(query).to_pandas()


def update_limit(table, user, new_limit):
    session.sql(f"""
        UPDATE {table}
        SET DAILY_USD_LIMIT = {new_limit}
        WHERE USER_NAME = '{user}'
    """).collect()


def reset_access(table, user):
    session.sql(f"""
        UPDATE {table}
        SET IS_REVOKED = FALSE, REVOKED_AT = NULL
        WHERE USER_NAME = '{user}'
    """).collect()
    session.sql(f'GRANT ROLE CORTEX_ACCESS_ROLE TO USER "{user}"').collect()


def revoke_access(table, user):
    session.sql(f"""
        UPDATE {table}
        SET IS_REVOKED = TRUE, REVOKED_AT = CURRENT_TIMESTAMP()
        WHERE USER_NAME = '{user}'
    """).collect()
    session.sql(f'REVOKE ROLE CORTEX_ACCESS_ROLE FROM USER "{user}"').collect()


st.title("Cortex Credit Manager")
if not is_admin:
    st.caption(f"Viewing as **{current_role}** (read-only)")

df = load_combined_status()
admin_users = load_accountadmin_users()
df["IS_ACCOUNTADMIN"] = df["USER_NAME"].isin(admin_users)

col1, col2, col3, col4, col5 = st.columns(5)
total_users = len(df)
cli_revoked = int(df["CLI_REVOKED"].sum()) if "CLI_REVOKED" in df.columns else 0
ss_revoked = int(df["SNOWSIGHT_REVOKED"].sum()) if "SNOWSIGHT_REVOKED" in df.columns else 0
total_spend = df["CLI_SPEND_TODAY"].sum() + df["SNOWSIGHT_SPEND_TODAY"].sum()
total_credits = df["CLI_CREDITS_TODAY"].sum() + df["SNOWSIGHT_CREDITS_TODAY"].sum()
col1.metric("Total Users", total_users)
col2.metric("CLI Revoked", cli_revoked)
col3.metric("Snowsight Revoked", ss_revoked)
col4.metric("Total Credit Today", f"{total_credits:,.2f}")
col5.metric("Total Spend Today($)", f"${total_spend:,.2f}")
st.divider()

if is_admin:
    tab_overview, tab_manage, tab_history = st.tabs(["Status Overview", "Manage Access", "Usage History"])
else:
    tab_overview, tab_history = st.tabs(["Status Overview", "Usage History"])

with tab_overview:
    col_hdr, col_btn = st.columns([8, 1])
    col_hdr.subheader("All Users — Current Status")
    if col_btn.button("Refresh", key="refresh_overview"):
        refresh_data()
        st.rerun()

    display_df = df.copy()
    display_df["CLI_STATUS"] = display_df.apply(
        lambda r: "EXCLUDED" if r["IS_ACCOUNTADMIN"] else ("REVOKED" if r["CLI_REVOKED"] else "ACTIVE"), axis=1)
    display_df["SNOWSIGHT_STATUS"] = display_df.apply(
        lambda r: "EXCLUDED" if r["IS_ACCOUNTADMIN"] else ("REVOKED" if r["SNOWSIGHT_REVOKED"] else "ACTIVE"), axis=1)
    display_df["NOTE"] = display_df["IS_ACCOUNTADMIN"].apply(
        lambda x: "ACCOUNTADMIN — excluded from limits" if x else "")

    filter_status = st.selectbox("Filter by status", ["All", "Any Revoked", "All Active"], key="filter_overview")
    if filter_status == "Any Revoked":
        display_df = display_df[(display_df["CLI_REVOKED"] == True) | (display_df["SNOWSIGHT_REVOKED"] == True)]
    elif filter_status == "All Active":
        display_df = display_df[(display_df["CLI_REVOKED"] == False) & (display_df["SNOWSIGHT_REVOKED"] == False)]

    def highlight_admin(row):
        if row.get("NOTE", "") != "":
            return ["background-color: #e0e0e0; color: #888"] * len(row)
        return [""] * len(row)

    styled_df = display_df[["USER_NAME", "CLI_LIMIT", "CLI_SPEND_TODAY", "CLI_CREDITS_TODAY", "CLI_STATUS",
                     "SNOWSIGHT_LIMIT", "SNOWSIGHT_SPEND_TODAY", "SNOWSIGHT_CREDITS_TODAY", "SNOWSIGHT_STATUS", "NOTE"]]
    st.dataframe(
        styled_df.style.apply(highlight_admin, axis=1),
        use_container_width=True,
        hide_index=True,
        column_config={
            "USER_NAME": "User",
            "CLI_LIMIT": st.column_config.NumberColumn("CLI Limit ($)", format="$%.2f"),
            "CLI_SPEND_TODAY": st.column_config.NumberColumn("CLI Spend ($)", format="$%.4f"),
            "CLI_CREDITS_TODAY": st.column_config.NumberColumn("CLI Credits", format="%.4f"),
            "CLI_STATUS": "CLI Status",
            "SNOWSIGHT_LIMIT": st.column_config.NumberColumn("Snowsight Limit ($)", format="$%.2f"),
            "SNOWSIGHT_SPEND_TODAY": st.column_config.NumberColumn("Snowsight Spend ($)", format="$%.4f"),
            "SNOWSIGHT_CREDITS_TODAY": st.column_config.NumberColumn("Snowsight Credits", format="%.4f"),
            "SNOWSIGHT_STATUS": "Snowsight Status",
            "NOTE": "Note",
        }
    )

if is_admin:
    with tab_manage:
        st.subheader("Update Limits & Reset Access")

        user_list = sorted(df["USER_NAME"].tolist())
        selected_user = st.selectbox("Select User", user_list, key="manage_user")

        if selected_user:
            user_row = df[df["USER_NAME"] == selected_user].iloc[0]

            if user_row["IS_ACCOUNTADMIN"]:
                st.warning(f"{selected_user} has ACCOUNTADMIN role and is excluded from rate limits. Changes here will not affect their access.")

            left, right = st.columns(2)

            with left:
                st.markdown("**Cortex CLI**")
                st.write(f"Daily limit: **${user_row['CLI_LIMIT']:.2f}**")
                st.write(f"Today's spend: **${user_row['CLI_SPEND_TODAY']:.4f}** ({user_row['CLI_CREDITS_TODAY']:.4f} credits)")
                st.write(f"Status: **{'REVOKED' if user_row['CLI_REVOKED'] else 'ACTIVE'}**")
                if user_row["CLI_REVOKED"]:
                    st.write(f"Revoked at: {user_row['CLI_REVOKED_AT']}")

                new_cli_limit = st.number_input("New CLI Daily Limit ($)", min_value=0.0, value=float(user_row["CLI_LIMIT"]), step=1.0, key="cli_limit_input", help=f"1 Snowflake credit = ${CREDIT_COST_USD}")
                c1, c2, c3 = st.columns(3)
                if c1.button("Update CLI Limit", key="update_cli"):
                    update_limit(CLI_TABLE, selected_user, new_cli_limit)
                    st.success(f"CLI limit updated to ${new_cli_limit:.2f}")
                    refresh_data()
                if c2.button("Reset CLI Access", key="reset_cli", disabled=not user_row["CLI_REVOKED"]):
                    reset_access(CLI_TABLE, selected_user)
                    st.success(f"CLI access restored for {selected_user}")
                    refresh_data()
                if c3.button("Revoke CLI", key="revoke_cli", disabled=bool(user_row["CLI_REVOKED"])):
                    revoke_access(CLI_TABLE, selected_user)
                    st.success(f"CLI access revoked for {selected_user}")
                    refresh_data()

            with right:
                st.markdown("**Cortex Snowsight**")
                st.write(f"Daily limit: **${user_row['SNOWSIGHT_LIMIT']:.2f}**")
                st.write(f"Today's spend: **${user_row['SNOWSIGHT_SPEND_TODAY']:.4f}** ({user_row['SNOWSIGHT_CREDITS_TODAY']:.4f} credits)")
                st.write(f"Status: **{'REVOKED' if user_row['SNOWSIGHT_REVOKED'] else 'ACTIVE'}**")
                if user_row["SNOWSIGHT_REVOKED"]:
                    st.write(f"Revoked at: {user_row['SNOWSIGHT_REVOKED_AT']}")

                new_ss_limit = st.number_input("New Snowsight Daily Limit ($)", min_value=0.0, value=float(user_row["SNOWSIGHT_LIMIT"]), step=1.0, key="ss_limit_input", help=f"1 Snowflake credit = ${CREDIT_COST_USD}")
                c4, c5, c6 = st.columns(3)
                if c4.button("Update Snowsight Limit", key="update_ss"):
                    update_limit(SNOWSIGHT_TABLE, selected_user, new_ss_limit)
                    st.success(f"Snowsight limit updated to ${new_ss_limit:.2f}")
                    refresh_data()
                if c5.button("Reset Snowsight Access", key="reset_ss", disabled=not user_row["SNOWSIGHT_REVOKED"]):
                    reset_access(SNOWSIGHT_TABLE, selected_user)
                    st.success(f"Snowsight access restored for {selected_user}")
                    refresh_data()
                if c6.button("Revoke Snowsight", key="revoke_ss", disabled=bool(user_row["SNOWSIGHT_REVOKED"])):
                    revoke_access(SNOWSIGHT_TABLE, selected_user)
                    st.success(f"Snowsight access revoked for {selected_user}")
                    refresh_data()

        st.divider()
        st.subheader("Bulk Update")
        bulk_col1, bulk_col2 = st.columns(2)
        with bulk_col1:
            bulk_target = st.selectbox("Target", ["CLI", "Snowsight", "Both"], key="bulk_target")
            bulk_limit = st.number_input("New Daily Limit ($) for All Users", min_value=0.0, value=1.0, step=1.0, key="bulk_limit", help=f"1 Snowflake credit = ${CREDIT_COST_USD}")
        with bulk_col2:
            st.write("")
            st.write("")
            if st.button("Apply Bulk Limit Update", key="bulk_update"):
                if bulk_target in ("CLI", "Both"):
                    session.sql(f"UPDATE {CLI_TABLE} SET DAILY_USD_LIMIT = {bulk_limit}").collect()
                if bulk_target in ("Snowsight", "Both"):
                    session.sql(f"UPDATE {SNOWSIGHT_TABLE} SET DAILY_USD_LIMIT = {bulk_limit}").collect()
                st.success(f"Updated {bulk_target} limit to ${bulk_limit:.2f} for all users")
                refresh_data()

            if st.button("Restore All Revoked Users", key="restore_all"):
                session.sql(f"CALL ADMIN.COST_MANAGEMENT.RESTORE_CORTEX_CLI_ACCESS()").collect()
                session.sql(f"CALL ADMIN.COST_MANAGEMENT.RESTORE_CORTEX_SNOWSIGHT_ACCESS()").collect()
                st.success("All revoked users have been restored")
                refresh_data()

with tab_history:
    st.subheader("Usage History (Last 30 Days)")

    history_df = load_history(30)

    if not history_df.empty:
        daily_totals = history_df.groupby(["USAGE_DATE", "SOURCE"])["COST_USD"].sum().reset_index()
        st.line_chart(daily_totals, x="USAGE_DATE", y="COST_USD", color="SOURCE")

        st.subheader("Per-User Breakdown")
        user_filter = st.multiselect("Filter by User", sorted(history_df["USER_NAME"].unique().tolist()), key="history_user_filter")
        if user_filter:
            history_df = history_df[history_df["USER_NAME"].isin(user_filter)]

        st.dataframe(
            history_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "USAGE_DATE": "Date",
                "USER_NAME": "User",
                "SOURCE": "Source",
                "COST_USD": st.column_config.NumberColumn("Cost ($)", format="$%.2f"),
                "CREDITS_USED": st.column_config.NumberColumn("Credits Used", format="%.4f"),
            }
        )
    else:
        st.info("No usage data found for the last 30 days.")
