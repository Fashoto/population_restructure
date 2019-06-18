-- HOW SCRIPT WORKS
-- 1. creates zone, zone_states table and functions to aid 
--    processing the population data.
--    PRE-REQUISITES:
--     a. Table: vts_popestimate_adj (table containing just the
--        adjusted records which are records with source:
--        'WorldPop ORNL / Adjusted' and 'CDC Adjusted')
-- 2. STEP 2:creates a restructured table: vts_settlement_pop
-- 3. STEP 3: lastly, create the pop tables by region.

-- ANONYMOUS BLOCK TO
--  * CREATE ZONES TABLE

CREATE SCHEMA IF NOT EXISTS pop_estimate_temp;
SET search_path=pop_estimate_temp;


-- STEP 1: create functions
DO $STEP1$
DECLARE
  ZCOUNT CONSTANT INTEGER := 6;         -- zone count
  ZSCOUNT CONSTANT INTEGER := 37;       -- zone_state count
  rvalue INTEGER := 0;
BEGIN

  -- TABLE: zones
  RAISE NOTICE 'Creating Table: zones';
  CREATE TABLE IF NOT EXISTS zones (
      code VARCHAR(2) PRIMARY KEY
    , name VARCHAR(15) UNIQUE NOT NULL
  );

  -- TABLE: zone_states table
  RAISE NOTICE 'Creating Table: zone_states';
  CREATE TABLE IF NOT EXISTS zone_states (
      zone_code VARCHAR(2) REFERENCES zones(code)
    , state_code VARCHAR(2)
    , PRIMARY KEY (zone_code, state_code)
  );

  -- INSERT: zones & zone_state values
  SELECT INTO rvalue COUNT(*) FROM zones;
  IF rvalue != ZCOUNT THEN
    TRUNCATE TABLE zones CASCADE;

    RAISE NOTICE 'Inserting records for zones';
    INSERT INTO zones (code, name)
    VALUES ('NE', 'North East'), ('NW', 'North West'), ('NC', 'North Central'),
           ('SE', 'South East'), ('SW', 'South West'), ('SS', 'South South');
  END IF;

  SELECT INTO rvalue COUNT(*) FROM zone_states;
  IF rvalue != ZSCOUNT THEN
    TRUNCATE TABLE zone_states;

    RAISE NOTICE 'Inserting records for zone_states';
    INSERT INTO zone_states (zone_code, state_code)
    VALUES ('NE', 'AD'), ('NE', 'BA'), ('NE', 'BR'), ('NE', 'GO'), ('NE', 'TA'), ('NE', 'YO'),
           ('NW', 'JI'), ('NW', 'KD'), ('NW', 'KN'), ('NW', 'KT'), ('NW', 'KB'), ('NW', 'SO'), ('NW', 'ZA'),
           ('NC', 'BE'), ('NC', 'KO'), ('NC', 'KW'), ('NC', 'NA'), ('NC', 'NI'), ('NC', 'PL'), ('NC', 'FC'),
           ('SE', 'AB'), ('SE', 'AN'), ('SE', 'EB'), ('SE', 'EN'), ('SE', 'IM'),
           ('SW', 'EK'), ('SW', 'LA'), ('SW', 'OG'), ('SW', 'ON'), ('SW', 'OS'), ('SW', 'OY'),
           ('SS', 'RI'), ('SS', 'CR'), ('SS', 'BY'), ('SS', 'DE'), ('SS', 'ED'),('SS','AK');
  END IF;

  -- FUNCTION: fn_GetZoneWards
  RAISE NOTICE 'Creating Function: fn_GetZoneWards ...';
  CREATE OR REPLACE FUNCTION fn_GetZoneWards (zoneCode VARCHAR)
  RETURNS VARCHAR ARRAY AS $FN1$
  DECLARE
    wards VARCHAR ARRAY;
  BEGIN
    SELECT ARRAY(
      SELECT code as ward_code
      FROM nigeria_master.boundaries
      WHERE state_code IN (
        SELECT state_code
        FROM zone_states
        WHERE zone_code = zoneCode
      ) 
    ) INTO wards;
    RETURN wards;
  END $FN1$
  LANGUAGE 'plpgsql';

  -- FUNCTION: fnGetWardPopSummary
  RAISE NOTICE 'Creating Function: fn_GetWardPopSummary ...';
  CREATE OR REPLACE FUNCTION fn_GetWardPopSummary (ageFrom INTEGER, ageTo INTEGER)
  RETURNS TABLE (
    global_id uuid,
    ward_code VARCHAR,
    source VARCHAR,
    gender VARCHAR,
    settlement_name VARCHAR,
    popvalue INTEGER
  ) AS $FN2$
  BEGIN
    IF ageFROM = 0 AND ageTO = 4 THEN
      -- RAISE NOTICE 'GetWardPopSummary for 0-4';
      RETURN QUERY SELECT
          vp.global_id,
          vp.ward_code,
          vp.source::varchar,
          vp.gender,
          fe.name as settlement_name,
          CAST((SUM(CASE WHEN vp.gender='M' THEN value ELSE 0 END) +
                SUM(CASE WHEN vp.gender='F' THEN value ELSE 0 END)
               ) AS INTEGER) as "popvalue"
      FROM pop_estimate_adj vp
      JOIN nigeria_master.settlement_areas fe
        ON vp.global_id = fe.global_id
      WHERE vp.age_group_to <= ageTo and vp.gender <> 'MF'
      GROUP BY (vp.global_id, vp.featureidentifier, vp.ward_code, 
                vp.source, vp.gender,settlement_name);
    ELSE
      -- RAISE NOTICE 'GetWardPopSummary for 5+';
      RETURN QUERY SELECT
          vp.global_id,
          vp.ward_code,
          vp.source:: VARCHAR,
          vp.gender,
          fe.name as settlement_name,
          CAST((SUM(CASE WHEN vp.gender='M' THEN value ELSE 0 END) +
                SUM(CASE WHEN vp.gender='F' THEN value ELSE 0 END)
               ) AS INTEGER) as "popvalue"
      FROM pop_estimate_adj vp
      JOIN nigeria_master.settlement_areas fe
        ON vp.global_id = fe.global_id
      WHERE vp.age_group_from = ageFROM 
      AND vp.gender <> 'MF'
      AND vp.age_group_to = ageTo
      GROUP BY (vp.global_id, vp.featureidentifier, vp.ward_code,
                vp.source, vp.gender,settlement_name);
    END IF;
  END $FN2$
  LANGUAGE 'plpgsql';

   -- FUNCTON: getSettlementPopByZone
  RAISE NOTICE 'Creating Function: fn_GetSettlementPopByZone ...';
  CREATE OR REPLACE FUNCTION fn_GetSettlementPopByZone(zoneName VARCHAR)
  RETURNS TABLE (
    global_id uuid, settlement_name VARCHAR, ward_code VARCHAR,
    pop1_4 INTEGER,   pop5_9 INTEGER,   pop10_14 INTEGER, pop15_19 INTEGER,
    pop20_24 INTEGER, pop25_29 INTEGER, pop30_34 INTEGER, pop35_39 INTEGER,
    pop40_44 INTEGER, pop45_49 INTEGER, pop50_54 INTEGER, pop55_59 INTEGER,
    pop60_64 INTEGER, pop65_69 INTEGER, pop70_100 INTEGER, pop_total INTEGER,
    geom public.GEOMETRY(MultiPolygon,4326)
  ) AS $FN3$
  BEGIN
    RETURN QUERY SELECT
        fe.global_id, fe.name as settlement_name, fe.ward_code,
        pp.age1_4,   pp.age5_9,   pp.age10_14, pp.age15_19,
        pp.age20_24, pp.age25_29, pp.age30_34, pp.age35_39,
        pp.age40_44, pp.age45_49, pp.age50_54, pp.age55_59,
        pp.age60_64, pp.age65_69, pp.age70_100, pp.pop_total,
        pp.geom
    FROM vts_settlement_pop as pp
    JOIN nigeria_master.settlement_areas as fe
      ON pp.global_id = fe.global_id
    WHERE ARRAY[pp.ward_code] <@ fn_GetZoneWards(zoneName);
  END $FN3$
  LANGUAGE 'plpgsql';

  -- TABLE: vts_settlement_pop
  RAISE NOTICE 'Creating Function: fn_CreateSettlementPopTable ...';
  CREATE OR REPLACE FUNCTION fn_CreateSettlementPopTable()
  RETURNS INTEGER AS $FN4$
  BEGIN
    RAISE NOTICE 'Creating the vts_settement_pop table ...';
    CREATE TABLE IF NOT EXISTS vts_settlement_pop AS
    SELECT
        tbl1_4.global_id,
        tbl1_4.settlement_name,
        tbl1_4.ward_code,
        tbl1_4.source,
        tbl1_4.gender,
        NOW() as "timestamp",
        tbl1_4.popvalue as "pop1_4",
        tbl5_9.popvalue as "pop5_9",
        tbl10_14.popvalue as "pop10_14",
        tbl15_19.popvalue as "pop15_19",
        tbl20_24.popvalue as "pop20_24",
        tbl25_29.popvalue as "pop25_29",
        tbl30_34.popvalue as "pop30_34",
        tbl35_39.popvalue as "pop35_39",
        tbl40_44.popvalue as "pop40_44",
        tbl45_49.popvalue as "pop45_49",
        tbl50_54.popvalue as "pop50_54",
        tbl55_59.popvalue as "pop55_59",
        tbl60_64.popvalue as "pop60_64",
        tbl65_69.popvalue as "pop65_69",
        tbl70_74.popvalue as "pop70_74",
        tbl75_100.popvalue as "pop75_100",
        (COALESCE(tbl1_4.popvalue, 0.0)   + COALESCE(tbl5_9.popvalue, 0.0) +
        COALESCE(tbl10_14.popvalue, 0.0) + COALESCE(tbl15_19.popvalue, 0.0) +
        COALESCE(tbl20_24.popvalue, 0.0) + COALESCE(tbl25_29.popvalue, 0.0) +
        COALESCE(tbl30_34.popvalue, 0.0) + COALESCE(tbl35_39.popvalue, 0.0) +
        COALESCE(tbl40_44.popvalue, 0.0) + COALESCE(tbl45_49.popvalue, 0.0) +
        COALESCE(tbl50_54.popvalue, 0.0) + COALESCE(tbl55_59.popvalue, 0.0) +
        COALESCE(tbl60_64.popvalue, 0.0) + COALESCE(tbl65_69.popvalue, 0.0) +
        COALESCE(tbl70_74.popvalue, 0.0) + COALESCE(tbl75_100.popvalue, 0.0)
        ) as "pop_total"
    FROM fn_GetWardPopSummary(0, 4) as tbl1_4
    LEFT JOIN fn_GetWardPopSummary(5, 9) as tbl5_9
      ON tbl1_4.global_id = tbl5_9.global_id

    LEFT JOIN fn_GetWardPopSummary(10, 14) as tbl10_14
      ON tbl5_9.global_id = tbl10_14.global_id
    LEFT JOIN fn_GetWardPopSummary(15, 19) as tbl15_19
      ON tbl10_14.global_id = tbl15_19.global_id

    LEFT JOIN fn_GetWardPopSummary(20, 24) as tbl20_24
      ON tbl15_19.global_id = tbl20_24.global_id
    LEFT JOIN fn_GetWardPopSummary(25, 29) as tbl25_29
      ON tbl20_24.global_id = tbl25_29.global_id

    LEFT JOIN fn_GetWardPopSummary(30, 34) as tbl30_34
      ON tbl25_29.global_id = tbl30_34.global_id
    LEFT JOIN fn_GetWardPopSummary(35, 39) as tbl35_39
      ON tbl30_34.global_id = tbl35_39.global_id

    LEFT JOIN fn_GetWardPopSummary(40, 44) as tbl40_44
      ON tbl35_39.global_id = tbl40_44.global_id
    LEFT JOIN fn_GetWardPopSummary(45, 49) as tbl45_49
      ON tbl40_44.global_id = tbl45_49.global_id

    LEFT JOIN fn_GetWardPopSummary(50, 54) as tbl50_54
      ON tbl45_49.global_id = tbl50_54.global_id
    LEFT JOIN fn_GetWardPopSummary(55, 59) as tbl55_59
      ON tbl50_54.global_id = tbl55_59.global_id

    LEFT JOIN fn_GetWardPopSummary(60, 64) as tbl60_64
      ON tbl55_59.global_id = tbl60_64.global_id
    LEFT JOIN fn_GetWardPopSummary(65, 69) as tbl65_69
      ON tbl60_64.global_id = tbl65_69.global_id

    LEFT JOIN fn_GetWardPopSummary(70, 74) as tbl70_74
      ON tbl65_69.global_id = tbl70_74.global_id
    LEFT JOIN fn_GetWardPopSummary(75, 100) as tbl75_100
      ON tbl70_74.global_id = tbl75_100.global_id;

    RAISE NOTICE 'done!';
    RETURN 0;
  END $FN4$
  LANGUAGE 'plpgsql';

  -- FUNCTION: fn_CreatePopTablesPerZone
  RAISE NOTICE 'Creating Function: fn_CreatePopTablesPerZone ...';
  CREATE OR REPLACE FUNCTION fn_CreatePopTablesPerZone()
  RETURNS INTEGER
  AS $FN5$
  DECLARE
    table_name VARCHAR;
    current_zone VARCHAR;
    -- target_zones VARCHAR ARRAY := ARRAY['NE'];
    target_zones VARCHAR ARRAY := ARRAY['NE', 'NC', 'NW', 'SW', 'SE', 'SS'];
    colproj VARCHAR := '
      SUM(pop1_4) "pop1_4", SUM(pop5_9) "pop5_9", SUM(pop10_14) "pop10_14", SUM(pop15_19) "pop15_19",
      SUM(pop20_24) "pop20_24", SUM(pop25_29) "pop25_29", SUM(pop30_34) "pop30_34", SUM(pop35_39) "pop35_39",
      SUM(pop40_44) "pop40_44", SUM(pop45_49) "pop45_49", SUM(pop50_54) "pop50_54", SUM(pop55_59) "pop55_59",
      SUM(pop60_64) "pop60_64", SUM(pop65_69) "pop65_69", SUM(pop70_74) "pop70_74", SUM(pop75_100) "pop75_100",
      SUM(pop_total) "pop_total"';
  BEGIN
    FOREACH current_zone IN ARRAY target_zones
    LOOP
      RAISE NOTICE 'About creating table for %', current_zone;
      
      -- get zone name
      SELECT INTO table_name LOWER(REPLACE(name, ' ', ''))
      FROM zones
      WHERE code = current_zone;
      RAISE NOTICE 'Zone full name: %', table_name;

      -- create table
      RAISE NOTICE 'Creating Table: %', (table_name || '_pop_settlement');
      EXECUTE FORMAT('
      CREATE TABLE IF NOT EXISTS %s AS
          SELECT sp.*, fe.category,fe.geom
          FROM vts_settlement_pop as sp
          JOIN nigeria_master.settlement_areas fe
            ON sp.global_id = fe.global_id
          WHERE ARRAY[sp.ward_code] <@ fn_GetZoneWards(''%s'')
        ', (table_name || '_pop_settlement'), current_zone
      );
      RAISE NOTICE 'done';

      -- create views: ??_pop_ward
      RAISE NOTICE 'Creating Table: %', (table_name || '_pop_wards');
      EXECUTE FORMAT('
        CREATE TABLE IF NOT EXISTS %s AS
          SELECT b.ward_code, b.ward_name, b.lga_code,%s,b.geom
          FROM %s as "pt" JOIN wards as "b"
            ON pt.ward_code = b.ward_code
          GROUP BY b.ward_code,b.ward_name,b.lga_code,b.geom;
      ', (table_name || '_pop_wards') , colproj, table_name || '_pop_settlement');
      RAISE NOTICE 'done!';

      -- create views: ??_pop_lga
      RAISE NOTICE 'Creating Table: %', (table_name || '_pop_lga');
      EXECUTE FORMAT('
        CREATE TABLE IF NOT EXISTS %s AS
          SELECT b.lga_code, b.lga_name, b.state_code,
          %s, b.geom
          FROM %s as "pt" JOIN local_government_areas as "b"
            ON pt.lga_code = b.lga_code
          GROUP BY b.lga_code,b.lga_name, b.state_code, b.geom;
      ', (table_name || '_pop_lga') , colproj, table_name || '_pop_wards');
      RAISE NOTICE 'done!';

      -- create views: ??_pop_state
      RAISE NOTICE 'Creating Table: %', (table_name || '_pop_state');
      EXECUTE FORMAT('
        CREATE TABLE IF NOT EXISTS %s AS
          SELECT b.state_code, b.state_name,
          %s, b.geom
          FROM %s as "pt" JOIN states as "b"
            ON pt.state_code = b.state_code
          GROUP BY b.state_code,b.state_name,b.geom;
      ', (table_name || '_pop_state') , colproj, table_name || '_pop_lga');
      RAISE NOTICE 'done!';

    END LOOP;
    RETURN 0;
  END $FN5$
  LANGUAGE 'plpgsql';

END $STEP1$;

DO $STEP1_1$
BEGIN
  -- REPLICATE BOUNDARY TABLES
  RAISE NOTICE 'Replicating Table: boundary_vaccstates ...';
  CREATE TABLE IF NOT EXISTS states
  AS SELECT name as state_name,code as state_code, geom
  FROM nigeria_master.states;

  RAISE NOTICE 'Replicating Table: boundary_vacclgas ...';
  CREATE TABLE IF NOT EXISTS local_government_areas
  AS SELECT name as lga_name,code as lga_code, state_code, geom
  FROM nigeria_master.local_government_areas;

  RAISE NOTICE 'Replicating Table: boundary_vaccwards ...';
  CREATE TABLE IF NOT EXISTS wards
  AS SELECT name as ward_name,code as ward_code, lga_code, geom 
  FROM nigeria_master.wards;
  RAISE NOTICE 'Replications complete ...';
END $STEP1_1$;

-- POPULATION DATA PROCESSING
DO $STEP2$
DECLARE
  rvalue2 INTEGER := 0;
BEGIN
  -- STEP 2: create restructured table
  SELECT INTO rvalue2 * FROM fn_CreateSettlementPopTable();
END $STEP2$;

DO $STEP3$
DECLARE
  rvalue3 INTEGER := 0;
BEGIN
  -- STEP 3: final step: create pop tables per zone
  SELECT INTO rvalue3 * FROM fn_CreatePopTablesPerZone();
END $STEP3$

-- ## script snippet
-- CREATE SCHEMA vts_pop_temp;
-- CREATE TABLE vts_pop_temp.vts_popestimate_adj
-- AS SELECT * 
--    FROM vts_pop.vts_populationestimates_july
--    WHERE source='Worldpop / ORNL Adjusted';
--C:\Program Files\PostgreSQL\9.5\bin>psql -d nigeria_master -U postgres -h 10.11.52.58 -f 'C:\Users\fashoto.busayo\Desktop\pop\popps_update.sql'

-- ## script snippet: purger
-- SET search_path=vts_pop_temp;
-- DROP TABLE zones, zone_states, vts_settlement_pop CASCADE;
-- DROP FUNCTION fn_GetZoneWards(VARCHAR); 
-- DROP FUNCTION fn_GetWardPopSummary(INTEGER, INTEGER);
-- DROP FUNCTION fn_GetSettlementPopByZone(VARCHAR);
-- DROP FUNCTION fn_CreateSettlementPopTable();
-- DROP FUNCTION fn_CreatePopTablesPerZone();