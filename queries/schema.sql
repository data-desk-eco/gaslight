INSTALL spatial; LOAD spatial;

-- SWR 32 exception permits
CREATE TABLE IF NOT EXISTS permits (
    excep_seq       VARCHAR,
    submittal_dt    VARCHAR,
    filing_no       VARCHAR,
    status          VARCHAR,
    filing_type     VARCHAR,
    operator_no     VARCHAR,
    operator_name   VARCHAR,
    property        VARCHAR,
    effective_dt    VARCHAR,
    expiration_dt   VARCHAR,
    fv_district     VARCHAR,
    -- parsed from property field
    property_type   VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR
);

-- RRC wells (Permian only, with locations)
CREATE TABLE IF NOT EXISTS wells (
    api             VARCHAR,
    oil_gas_code    VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR,
    well_number     VARCHAR,
    operator_no     VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geom            GEOMETRY
);

-- RRC operators
CREATE TABLE IF NOT EXISTS operators (
    operator_number VARCHAR,
    operator_name   VARCHAR,
    status          VARCHAR
);

-- Permitted flare/vent locations (scraped from SWR 32 detail pages)
CREATE TABLE IF NOT EXISTS flare_locations (
    filing_no       VARCHAR,
    name            VARCHAR,
    county          VARCHAR,
    district        VARCHAR,
    release_type    VARCHAR,
    release_height_ft VARCHAR,
    gps_datum       VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    h2s_area        VARCHAR,
    h2s_concentration_ppm VARCHAR,
    h2s_distance_ft VARCHAR,
    h2s_public_area_type VARCHAR,
    geom            GEOMETRY
);

-- VNF daily flare detections (Permian)
CREATE TABLE IF NOT EXISTS vnf (
    flare_id    INTEGER,
    lat         DOUBLE,
    lon         DOUBLE,
    date        DATE,
    clear       BOOLEAN,
    detected    BOOLEAN,
    rh_mw       DOUBLE,
    temp_k      DOUBLE,
    n_passes    INTEGER,
    geom        GEOMETRY
);

-- Methane plume detections (Carbon Mapper + IMEO)
CREATE TABLE IF NOT EXISTS plumes (
    plume_id        VARCHAR,
    source          VARCHAR,
    satellite       VARCHAR,
    date            DATE,
    latitude        DOUBLE,
    longitude       DOUBLE,
    emission_rate   DOUBLE,
    emission_uncertainty DOUBLE,
    sector          VARCHAR,
    geom            GEOMETRY
);
