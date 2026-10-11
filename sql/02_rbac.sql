/* ============================================================================
   02 — Roles and grants
   ----------------------------------------------------------------------------
   Two functional roles. The analyst can read MARTS only; the engineer owns the
   pipeline. Both roll up to SYSADMIN so account admins retain visibility.

   Snowflake inherits privileges UPWARD through role grants: granting
   MARKET_ANALYST to MARKET_ENGINEER gives the engineer everything the analyst
   has, not the other way round.
   ========================================================================= */

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS MARKET_ENGINEER;
CREATE ROLE IF NOT EXISTS MARKET_ANALYST;

USE ROLE SECURITYADMIN;
GRANT ROLE MARKET_ANALYST  TO ROLE MARKET_ENGINEER;
GRANT ROLE MARKET_ENGINEER TO ROLE SYSADMIN;

-- Warehouse access is separate from data access
GRANT USAGE ON WAREHOUSE LOAD_WH  TO ROLE MARKET_ENGINEER;
GRANT USAGE ON WAREHOUSE QUERY_WH TO ROLE MARKET_ANALYST;

-- Engineer: full control of the pipeline
GRANT USAGE ON DATABASE MARKET_DW TO ROLE MARKET_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MARKET_DW TO ROLE MARKET_ENGINEER;
GRANT ALL   ON SCHEMA MARKET_DW.RAW     TO ROLE MARKET_ENGINEER;
GRANT ALL   ON SCHEMA MARKET_DW.STAGING TO ROLE MARKET_ENGINEER;
GRANT ALL   ON SCHEMA MARKET_DW.MARTS   TO ROLE MARKET_ENGINEER;

/* Analyst: read-only, MARTS only.
   FUTURE GRANTS cover objects created later — ALL is a one-time snapshot. */
GRANT USAGE  ON SCHEMA MARKET_DW.MARTS TO ROLE MARKET_ANALYST;
GRANT SELECT ON ALL    TABLES IN SCHEMA MARKET_DW.MARTS TO ROLE MARKET_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARKET_DW.MARTS TO ROLE MARKET_ANALYST;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA MARKET_DW.MARTS TO ROLE MARKET_ANALYST;

GRANT ROLE MARKET_ENGINEER TO USER <YOUR_USERNAME>;
GRANT ROLE MARKET_ANALYST  TO USER <YOUR_USERNAME>;

-- Required for scheduled tasks
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE MARKET_ENGINEER;

/* ---------------------------------------------------------------------------
   Verify the boundary actually holds. This SHOULD fail with an
   "insufficient privileges" error — testing a grant matters more than making
   one.
   ------------------------------------------------------------------------ */
-- USE ROLE MARKET_ANALYST;
-- USE WAREHOUSE QUERY_WH;
-- SELECT * FROM MARKET_DW.RAW.PRICES_RAW LIMIT 1;
