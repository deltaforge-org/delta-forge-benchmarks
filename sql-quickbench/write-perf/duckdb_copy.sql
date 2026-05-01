-- DuckDB head-to-head: same 5M-row x 43-col dim_customer shape as the
-- DeltaForge perf scripts, written to a single parquet file via COPY TO.
--
-- This is the reference baseline for "what a production-quality parquet
-- writer produces from inline cyclic-lookup data." Compare against
-- df_ctas.sql and df_insert.sql to spot DeltaForge writer regressions.
-- Current numbers live in BASELINE.md; this file no longer hardcodes them.

.shell rm -f B:/odbc_df/df-demo/perf_test/duck_dim_customer.parquet

.timer on

COPY (
  WITH src AS (
    SELECT (range + 1) AS rn FROM range(5000000)
  )
  SELECT
    rn                                                                  AS customer_id,
    'CUST-' || lpad(CAST(rn AS VARCHAR), 8, '0')                        AS customer_code,
    ['Mr.','Mrs.','Ms.','Dr.','Prof.'][CAST(((rn-1) % 5)+1 AS INT)]     AS salutation,
    ['Alice','Bob','Carol','David','Emma','Frank','Grace','Henry','Iris','Jack','Karen','Leo','Maya','Noah','Olivia','Peter','Quinn','Rachel','Steve','Tina','Uma','Victor','Wendy','Xander','Yara','Zoe','Aaron','Beth','Chris','Diana','Ethan','Fiona','George','Hannah','Ian','Julia','Kevin','Laura','Mike','Nina','Oscar','Paula','Quentin','Rose','Sam','Tara','Umar','Vera','Will','Xenia'][CAST(((rn-1) % 50)+1 AS INT)] AS first_name,
    ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'][CAST(((rn-1) % 26)+1 AS INT)] AS middle_initial,
    ['Smith','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez'][CAST(((rn*7-1) % 50)+1 AS INT)] AS last_name,
    ['Alice','Bob','Carol','David','Emma','Frank','Grace','Henry','Iris','Jack','Karen','Leo','Maya','Noah','Olivia','Peter','Quinn','Rachel','Steve','Tina','Uma','Victor','Wendy','Xander','Yara','Zoe','Aaron','Beth','Chris','Diana','Ethan','Fiona','George','Hannah','Ian','Julia','Kevin','Laura','Mike','Nina','Oscar','Paula','Quentin','Rose','Sam','Tara','Umar','Vera','Will','Xenia'][CAST(((rn-1) % 50)+1 AS INT)]
      || ' '
      || ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'][CAST(((rn-1) % 26)+1 AS INT)]
      || '. '
      || ['Smith','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez'][CAST(((rn*7-1) % 50)+1 AS INT)] AS full_name,
    'cust' || CAST(rn AS VARCHAR) || '@' || ['example','retail','demo','test','sample'][CAST(((rn-1) % 5)+1 AS INT)] || '.com' AS email,
    '+1' || lpad(CAST(rn % 10000000000 AS VARCHAR), 10, '0')            AS phone_e164,
    ['M','F','X','U'][CAST(((rn-1) % 4)+1 AS INT)]                      AS gender,
    DATE '1940-01-01' + CAST((rn * 13) % 25000 AS INT)                  AS birth_date,
    ['18-24','25-34','35-44','45-54','55+'][CAST(((rn-1) % 5)+1 AS INT)] AS age_band,
    ['Single','Married','Divorced','Widowed','Partner'][CAST(((rn-1) % 5)+1 AS INT)] AS marital_status,
    ['High School','Associate','Bachelor','Master','Doctorate'][CAST(((rn-1) % 5)+1 AS INT)] AS education_level,
    ['Engineer','Teacher','Manager','Sales','Healthcare','Retail','Service','Technician','Analyst','Professional'][CAST(((rn-1) % 10)+1 AS INT)] AS occupation,
    ['Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka'][CAST(((rn-1) % 10)+1 AS INT)] || ' Industries' AS employer_name,
    CAST(25000 + (rn % 475000) AS DECIMAL(18,2))                        AS annual_income_usd,
    ['Under $25K','$25K-$50K','$50K-$100K','$100K-$200K','$200K+'][CAST(((rn-1) % 5)+1 AS INT)] AS income_band,
    CAST(1 + (rn % 6) AS INTEGER)                                       AS household_size,
    CAST(rn % 5 AS INTEGER)                                             AS number_of_children,
    CAST(rn AS VARCHAR) || ' ' || ['Main St','Oak Ave','Maple Dr','Cedar Ln','Elm St','Pine Rd','Birch Way','Walnut Ct','Spruce Pl','Willow Ln','Cherry St','Park Ave','Lake Dr','River Rd','Hill St','Forest Ave','Meadow Ln','Sunset Blvd','Highland Dr','Valley View'][CAST(((rn-1) % 20)+1 AS INT)] AS address_line_1,
    CASE WHEN rn % 3 = 0 THEN 'Apt #' || CAST((rn % 999 + 1) AS VARCHAR) ELSE NULL END AS address_line_2,
    ['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle','Denver','Washington','Boston','El Paso','Detroit','Nashville','Memphis','Portland','Oklahoma City','Las Vegas','Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Sacramento','Mesa','Kansas City','Atlanta','Miami','Raleigh','Omaha','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa','Arlington','Tampa','New Orleans','Wichita'][CAST(((rn-1) % 50)+1 AS INT)] AS city,
    ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'][CAST(((rn-1) % 50)+1 AS INT)] AS state_code,
    ['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming'][CAST(((rn-1) % 50)+1 AS INT)] AS state_name,
    lpad(CAST(rn % 99999 AS VARCHAR), 5, '0')                           AS postal_code,
    ['US','CA','MX','UK','DE','FR','JP','AU','BR','IN'][CAST(((rn-1) % 10)+1 AS INT)] AS country_code,
    ['United States','Canada','Mexico','United Kingdom','Germany','France','Japan','Australia','Brazil','India'][CAST(((rn-1) % 10)+1 AS INT)] AS country_name,
    ['NA','EU','APAC','LATAM','MEA'][CAST(((rn-1) % 5)+1 AS INT)]       AS region,
    -90.0 + CAST(rn % 18000 AS DOUBLE) / 100.0                          AS latitude,
    -180.0 + CAST(rn % 36000 AS DOUBLE) / 100.0                         AS longitude,
    DATE '2018-01-01' + CAST((rn * 7) % 2192 AS INT)                    AS signup_date,
    ['Web','Mobile','Store','Phone','Referral','Social','Email','Partner'][CAST(((rn-1) % 8)+1 AS INT)] AS signup_channel,
    ['Email','SMS','Phone','Mail'][CAST(((rn-1) % 4)+1 AS INT)]         AS preferred_contact_channel,
    rn % 4 = 0                                                          AS marketing_opt_in,
    rn % 5 = 0                                                          AS sms_opt_in,
    ['Bronze','Silver','Gold','Platinum','Diamond'][CAST(((rn-1) % 5)+1 AS INT)] AS loyalty_tier,
    CAST((rn * 11) % 50000 AS INTEGER)                                  AS loyalty_points_balance,
    CAST(rn % 100 AS INTEGER)                                           AS lifetime_orders,
    CAST(rn % 100000 AS DECIMAL(18,2)) + CAST(0.99 AS DECIMAL(18,2))    AS lifetime_revenue_usd,
    DATE '2024-01-01' + CAST((rn * 3) % 365 AS INT)                     AS last_purchase_date,
    CAST(rn % 100 AS DOUBLE) / 100.0                                    AS churn_risk_score,
    ['New','Active','At Risk','VIP','Inactive'][CAST(((rn-1) % 5)+1 AS INT)] AS segment
  FROM src
)
TO 'B:/odbc_df/df-demo/perf_test/duck_dim_customer.parquet'
(FORMAT PARQUET, COMPRESSION 'snappy');

SELECT COUNT(*) AS rows FROM read_parquet('B:/odbc_df/df-demo/perf_test/duck_dim_customer.parquet');
