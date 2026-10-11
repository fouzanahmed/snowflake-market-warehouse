/* ============================================================================
   11 — Dynamic data masking                                 [NOT YET RUN]
   ----------------------------------------------------------------------------
   Column-level, applied AT QUERY TIME — data on disk is unchanged. A masking
   policy is a schema-level object, so one policy is reusable across many
   tables; that is the point versus building a view per role.

   Sibling feature: ROW ACCESS POLICY for row-level filtering.
   Both are Enterprise edition and above.
   ========================================================================= */

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE MASKING POLICY MARKET_DW.MARTS.MASK_TICKER
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('MARKET_ENGINEER','ACCOUNTADMIN') THEN val
    ELSE '***MASKED***'
  END;

ALTER TABLE MARKET_DW.MARTS.PRICE_FEATURES
  MODIFY COLUMN TICKER SET MASKING POLICY MARKET_DW.MARTS.MASK_TICKER;

-- Test BOTH directions. A policy you haven't tested isn't a control.
USE ROLE MARKET_ANALYST;  USE WAREHOUSE QUERY_WH;
SELECT TICKER, CLOSE FROM MARKET_DW.MARTS.PRICE_FEATURES LIMIT 5;  -- masked

USE ROLE MARKET_ENGINEER;
SELECT TICKER, CLOSE FROM MARKET_DW.MARTS.PRICE_FEATURES LIMIT 5;  -- clear
