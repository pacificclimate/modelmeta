--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: pcic_meta; Type: SCHEMA; Schema: -; Owner: pcic_meta
--

CREATE SCHEMA pcic_meta;


ALTER SCHEMA pcic_meta OWNER TO pcic_meta;

SET search_path = pcic_meta, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: times; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE climatological_times (
    time_start timestamp without time zone NOT NULL,
    time_end timestamp without time zone NOT NULL,
    time_idx integer NOT NULL,
    time_set_id integer NOT NULL
);


ALTER TABLE pcic_meta.climatological_times OWNER TO pcic_meta;


--
-- Name: data_file_variables; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE data_file_variables (
    data_file_variable_id integer NOT NULL,
    data_file_id integer NOT NULL,
    variable_alias_id integer NOT NULL,
    derivation_method character varying(255),
    variable_cell_methods character varying(255),
    level_set_id integer,
    grid_id integer NOT NULL,
    netcdf_variable_name character varying(32) NOT NULL,
    disabled boolean DEFAULT false,
    range_min real NOT NULL,
    range_max real NOT NULL
);


ALTER TABLE pcic_meta.data_file_variables OWNER TO pcic_meta;

--
-- Name: data_file_variables_data_file_variable_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE data_file_variables_data_file_variable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.data_file_variables_data_file_variable_id_seq OWNER TO pcic_meta;

--
-- Name: data_file_variables_data_file_variable_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE data_file_variables_data_file_variable_id_seq OWNED BY data_file_variables.data_file_variable_id;

--
-- Name: data_file_variables_qc_flags; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE data_file_variables_qc_flags (
    data_file_variable_id integer NOT NULL,
    qc_flag_id integer NOT NULL
);


ALTER TABLE pcic_meta.data_file_variables_qc_flags OWNER TO pcic_meta;


--
-- Name: data_files; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE data_files (
    data_file_id integer NOT NULL,
    filename character varying(2048) NOT NULL,
    run_id integer,
    first_1mib_md5sum character(32) NOT NULL,
    unique_id character varying(255) NOT NULL,
    time_set_id integer,
    x_dim_name character varying(32) NOT NULL,
    y_dim_name character varying(32) NOT NULL,
    z_dim_name character varying(32),
    t_dim_name character varying(32),
    index_time timestamp without time zone NOT NULL DEFAULT now()
);


ALTER TABLE pcic_meta.data_files OWNER TO pcic_meta;

--
-- Name: data_files_data_file_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE data_files_data_file_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.data_files_data_file_id_seq OWNER TO pcic_meta;

--
-- Name: data_files_data_file_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE data_files_data_file_id_seq OWNED BY data_files.data_file_id;


--
-- Name: emissions; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE emissions (
    emission_id integer NOT NULL,
    emission_long_name character varying(255),
    emission_short_name character varying(255) NOT NULL
);


ALTER TABLE pcic_meta.emissions OWNER TO pcic_meta;

--
-- Name: ensemble_data_file_variables; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE ensemble_data_file_variables (
    ensemble_id integer NOT NULL,
    data_file_variable_id integer NOT NULL
);


ALTER TABLE pcic_meta.ensemble_data_file_variables OWNER TO pcic_meta;

--
-- Name: ensembles; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE ensembles (
    ensemble_id integer NOT NULL,
    ensemble_name character varying(32) NOT NULL,
    ensemble_description character varying(255),
    version real NOT NULL,
    changes text NOT NULL
);


ALTER TABLE pcic_meta.ensembles OWNER TO pcic_meta;

--
-- Name: ensembles_ensemble_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE ensembles_ensemble_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.ensembles_ensemble_id_seq OWNER TO pcic_meta;

--
-- Name: ensembles_ensemble_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE ensembles_ensemble_id_seq OWNED BY ensembles.ensemble_id;


--
-- Name: grids; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE grids (
    grid_id integer NOT NULL,
    srid integer,
    grid_name character varying(255),
    xc_grid_step real NOT NULL,
    yc_grid_step real NOT NULL,
    xc_origin real NOT NULL,
    yc_origin real NOT NULL,
    xc_count integer NOT NULL,
    yc_count integer NOT NULL,
    cell_avg_area_sq_km real,
    evenly_spaced_y boolean NOT NULL,
    xc_units character varying(64) NOT NULL,
    yc_units character varying(64) NOT NULL
);


ALTER TABLE pcic_meta.grids OWNER TO pcic_meta;

--
-- Name: grid_grid_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE grid_grid_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.grid_grid_id_seq OWNER TO pcic_meta;

--
-- Name: grid_grid_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE grid_grid_id_seq OWNED BY grids.grid_id;


--
-- Name: level_sets; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE level_sets (
    level_set_id integer NOT NULL,
    level_units character varying(32) NOT NULL
);


ALTER TABLE pcic_meta.level_sets OWNER TO pcic_meta;

--
-- Name: level_sets_level_set_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE level_sets_level_set_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.level_sets_level_set_id_seq OWNER TO pcic_meta;

--
-- Name: level_sets_level_set_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE level_sets_level_set_id_seq OWNED BY level_sets.level_set_id;


--
-- Name: levels; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE levels (
    vertical_level real NOT NULL,
    level_start real,
    level_end real,
    level_idx integer NOT NULL,
    level_set_id integer NOT NULL
);


ALTER TABLE pcic_meta.levels OWNER TO pcic_meta;

--
-- Name: models; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE models (
    model_id integer NOT NULL,
    model_long_name character varying(255),
    model_short_name character varying(32) NOT NULL,
    type character varying(32) NOT NULL DEFAULT 'GCM',
    model_organization character varying(64)
);


ALTER TABLE pcic_meta.models OWNER TO pcic_meta;

--
-- Name: models_model_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE models_model_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.models_model_id_seq OWNER TO pcic_meta;

--
-- Name: models_model_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE models_model_id_seq OWNED BY models.model_id;

--
-- Name: qc_flags; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE qc_flags (
    qc_flag_id integer NOT NULL,
    qc_flag_name character varying(32) NOT NULL,
    qc_flag_description character varying(2048)
);


ALTER TABLE pcic_meta.qc_flags OWNER TO pcic_meta;

--
-- Name: qc_flags_qc_flag_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE qc_flags_qc_flag_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.qc_flags_qc_flag_id_seq OWNER TO pcic_meta;

--
-- Name: qc_flags_qc_flag_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE qc_flags_qc_flag_id_seq OWNED BY qc_flags.qc_flag_id;


--
-- Name: runs; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE runs (
    run_id integer NOT NULL,
    run_name character varying(32) NOT NULL,
    model_id integer NOT NULL,
    emission_id integer NOT NULL,
    driving_run integer,
    initialized_from integer,
    project character varying(64)
);


ALTER TABLE pcic_meta.runs OWNER TO pcic_meta;

--
-- Name: runs_run_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE runs_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.runs_run_id_seq OWNER TO pcic_meta;

--
-- Name: runs_run_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE runs_run_id_seq OWNED BY runs.run_id;


--
-- Name: scenarios_scenario_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE scenarios_scenario_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.scenarios_scenario_id_seq OWNER TO pcic_meta;

--
-- Name: scenarios_scenario_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE scenarios_scenario_id_seq OWNED BY emissions.emission_id;


--
-- Name: time_sets; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE time_sets (
    time_set_id integer NOT NULL,
    calendar character varying(32) NOT NULL DEFAULT 'gregorian',
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone NOT NULL,
    time_resolution public.timescale NOT NULL,
    multi_year_mean boolean NOT NULL DEFAULT false,
    num_times integer NOT NULL
);


ALTER TABLE pcic_meta.time_sets OWNER TO pcic_meta;

--
-- Name: time_sets_time_set_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE time_sets_time_set_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.time_sets_time_set_id_seq OWNER TO pcic_meta;

--
-- Name: time_sets_time_set_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE time_sets_time_set_id_seq OWNED BY time_sets.time_set_id;


--
-- Name: times; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE times (
    timestep timestamp without time zone NOT NULL,
    time_idx integer NOT NULL,
    time_set_id integer NOT NULL
);


ALTER TABLE pcic_meta.times OWNER TO pcic_meta;

--
-- Name: variable_aliases; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE variable_aliases (
    variable_alias_id integer NOT NULL,
    variable_long_name character varying(255) NOT NULL,
    variable_standard_name character varying(64) NOT NULL,
    variable_units character varying(32) NOT NULL
);


ALTER TABLE pcic_meta.variable_aliases OWNER TO pcic_meta;

--
-- Name: variable_aliases_variable_alias_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE variable_aliases_variable_alias_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.variable_aliases_variable_alias_id_seq OWNER TO pcic_meta;

--
-- Name: variable_aliases_variable_alias_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE variable_aliases_variable_alias_id_seq OWNED BY variable_aliases.variable_alias_id;


--
-- Name: variables; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE variables (
    variable_id integer NOT NULL,
    variable_alias_id integer NOT NULL,
    variable_name character varying(64) NOT NULL,
    variable_description character varying(255) NOT NULL
);


ALTER TABLE pcic_meta.variables OWNER TO pcic_meta;


--
-- Name: variables_variable_id_seq; Type: SEQUENCE; Schema: pcic_meta; Owner: pcic_meta
--

CREATE SEQUENCE variables_variable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pcic_meta.variables_variable_id_seq OWNER TO pcic_meta;

--
-- Name: variables_variable_id_seq; Type: SEQUENCE OWNED BY; Schema: pcic_meta; Owner: pcic_meta
--

ALTER SEQUENCE variables_variable_id_seq OWNED BY variables.variable_id;


--
-- Name: y_cell_bounds; Type: TABLE; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE TABLE y_cell_bounds (
    grid_id integer NOT NULL,
    top_bnd real,
    y_center real NOT NULL,
    bottom_bnd real
);


ALTER TABLE pcic_meta.y_cell_bounds OWNER TO pcic_meta;

--
-- Name: data_file_variable_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables ALTER COLUMN data_file_variable_id SET DEFAULT nextval('data_file_variables_data_file_variable_id_seq'::regclass);


--
-- Name: data_file_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_files ALTER COLUMN data_file_id SET DEFAULT nextval('data_files_data_file_id_seq'::regclass);


--
-- Name: emission_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY emissions ALTER COLUMN emission_id SET DEFAULT nextval('scenarios_scenario_id_seq'::regclass);


--
-- Name: ensemble_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY ensembles ALTER COLUMN ensemble_id SET DEFAULT nextval('ensembles_ensemble_id_seq'::regclass);


--
-- Name: grid_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY grids ALTER COLUMN grid_id SET DEFAULT nextval('grid_grid_id_seq'::regclass);


--
-- Name: level_set_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY level_sets ALTER COLUMN level_set_id SET DEFAULT nextval('level_sets_level_set_id_seq'::regclass);


--
-- Name: model_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY models ALTER COLUMN model_id SET DEFAULT nextval('models_model_id_seq'::regclass);


--
-- Name: qc_flag; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY qc_flags ALTER COLUMN qc_flag_id SET DEFAULT nextval('qc_flags_qc_flag_id_seq'::regclass);


--
-- Name: run_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY runs ALTER COLUMN run_id SET DEFAULT nextval('runs_run_id_seq'::regclass);


--
-- Name: time_set_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY time_sets ALTER COLUMN time_set_id SET DEFAULT nextval('time_sets_time_set_id_seq'::regclass);


--
-- Name: variable_alias_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY variable_aliases ALTER COLUMN variable_alias_id SET DEFAULT nextval('variable_aliases_variable_alias_id_seq'::regclass);


--
-- Name: variable_id; Type: DEFAULT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY variables ALTER COLUMN variable_id SET DEFAULT nextval('variables_variable_id_seq'::regclass);


--
-- Name: data_file_variables_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY data_file_variables
    ADD CONSTRAINT data_file_variables_pkey PRIMARY KEY (data_file_variable_id);


--
-- Name: data_file_variables_qc_flags_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY data_file_variables_qc_flags
    ADD CONSTRAINT data_file_variables_qc_flags_pkey PRIMARY KEY (data_file_variable_id, qc_flag_id);


--
-- Name: data_files_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY data_files
    ADD CONSTRAINT data_files_pkey PRIMARY KEY (data_file_id);


--
-- Name: data_files_unique_id_key; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY data_files
    ADD CONSTRAINT data_files_unique_id_key UNIQUE (unique_id);


ALTER TABLE ONLY data_files 
    ADD CONSTRAINT data_files_time_dim_check CHECK(NOT (t_dim_name IS NULL != time_set_id IS NULL));

--
-- Name: ensemble_name_version_key; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY ensembles
    ADD CONSTRAINT ensemble_name_version_key UNIQUE (ensemble_name, version);


--
-- Name: ensemble_data_file_variables_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY ensemble_data_file_variables
    ADD CONSTRAINT ensemble_data_file_variables_pkey PRIMARY KEY (ensemble_id, data_file_variable_id);


--
-- Name: ensembles_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY ensembles
    ADD CONSTRAINT ensembles_pkey PRIMARY KEY (ensemble_id);


--
-- Name: grid_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY grids
    ADD CONSTRAINT grid_pkey PRIMARY KEY (grid_id);


--
-- Name: level_sets_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY level_sets
    ADD CONSTRAINT level_sets_pkey PRIMARY KEY (level_set_id);


--
-- Name: levels_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY levels
    ADD CONSTRAINT levels_pkey PRIMARY KEY (level_idx, level_set_id);


--
-- Name: models_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY models
    ADD CONSTRAINT models_pkey PRIMARY KEY (model_id);


--
-- Name: qc_flags_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY qc_flags
    ADD CONSTRAINT qc_flags_pkey PRIMARY KEY (qc_flag_id);


--
-- Name: runs_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (run_id);
ALTER TABLE ONLY runs
    ADD CONSTRAINT unique_run_model_emissions_constraint UNIQUE(run_name, model_id, emission_id);


--
-- Name: scenarios_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY emissions
    ADD CONSTRAINT scenarios_pkey PRIMARY KEY (emission_id);


--
-- Name: time_sets_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY time_sets
    ADD CONSTRAINT time_sets_pkey PRIMARY KEY (time_set_id);


--
-- Name: variable_aliases_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY variable_aliases
    ADD CONSTRAINT variable_aliases_pkey PRIMARY KEY (variable_alias_id);


--
-- Name: variables_pkey; Type: CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

ALTER TABLE ONLY variables
    ADD CONSTRAINT variables_pkey PRIMARY KEY (variable_id);


--
-- Name: time_set_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX climatological_times_time_set_id_key ON climatological_times USING btree (time_set_id);


--
-- Name: data_files_run_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX data_files_run_id_key ON data_files USING btree (run_id);


--
-- Name: runs_emission_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX runs_emission_id_key ON runs USING btree (emission_id);


--
-- Name: runs_model_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX runs_model_id_key ON runs USING btree (model_id);


--
-- Name: time_set_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX time_set_id_key ON times USING btree (time_set_id);


--
-- Name: y_c_b_grid_id_key; Type: INDEX; Schema: pcic_meta; Owner: pcic_meta; Tablespace: 
--

CREATE INDEX y_c_b_grid_id_key ON y_cell_bounds USING btree (grid_id);


--
-- Name: times_time_set_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY climatological_times
    ADD CONSTRAINT climatological_times_time_set_id_fkey FOREIGN KEY (time_set_id) REFERENCES time_sets(time_set_id);


--
-- Name: data_file_variables_data_file_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables
    ADD CONSTRAINT data_file_variables_data_file_id_fkey FOREIGN KEY (data_file_id) REFERENCES data_files(data_file_id);


--
-- Name: data_file_variables_grid_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables
    ADD CONSTRAINT data_file_variables_grid_id_fkey FOREIGN KEY (grid_id) REFERENCES grids(grid_id);


--
-- Name: data_file_variables_level_set_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables
    ADD CONSTRAINT data_file_variables_level_set_id_fkey FOREIGN KEY (level_set_id) REFERENCES level_sets(level_set_id);


--
-- Name: data_file_variables_variable_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables
    ADD CONSTRAINT data_file_variable_aliases_variable_alias_id_fkey FOREIGN KEY (variable_alias_id) REFERENCES variable_aliases(variable_alias_id);


--
-- Name: data_file_variables_qc_flags_ensemble_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables_qc_flags
    ADD CONSTRAINT data_file_variables_qc_flags_qc_flag_id_fkey FOREIGN KEY (qc_flag_id) REFERENCES qc_flags(qc_flag_id);


--
-- Name: data_file_variables_qc_flags_run_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_file_variables_qc_flags
    ADD CONSTRAINT data_file_variables_qc_flags_run_id_fkey FOREIGN KEY (data_file_variable_id) REFERENCES data_file_variables(data_file_variable_id);


--
-- Name: data_files_run_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_files
    ADD CONSTRAINT data_files_run_id_fkey FOREIGN KEY (run_id) REFERENCES runs(run_id);


--
-- Name: data_files_time_set_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY data_files
    ADD CONSTRAINT data_files_time_set_id_fkey FOREIGN KEY (time_set_id) REFERENCES time_sets(time_set_id);


--
-- Name: ensemble_data_file_variables_ensemble_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY ensemble_data_file_variables
    ADD CONSTRAINT ensemble_data_file_variables_ensemble_id_fkey FOREIGN KEY (ensemble_id) REFERENCES ensembles(ensemble_id);


--
-- Name: ensemble_data_file_variables_run_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY ensemble_data_file_variables
    ADD CONSTRAINT ensemble_data_file_variables_data_file_variable_id_fkey FOREIGN KEY (data_file_variable_id) REFERENCES data_file_variables(data_file_variable_id);


--
-- Name: grid_srid_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY grids
    ADD CONSTRAINT grid_srid_fkey FOREIGN KEY (srid) REFERENCES public.spatial_ref_sys(srid);


--
-- Name: levels_level_set_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY levels
    ADD CONSTRAINT levels_level_set_id_fkey FOREIGN KEY (level_set_id) REFERENCES level_sets(level_set_id);


--
-- Name: runs_driving_run_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY runs
    ADD CONSTRAINT runs_driving_run_fkey FOREIGN KEY (driving_run) REFERENCES runs(run_id);


--
-- Name: runs_emission_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY runs
    ADD CONSTRAINT runs_emission_id_fkey FOREIGN KEY (emission_id) REFERENCES emissions(emission_id);


--
-- Name: runs_initialized_from_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY runs
    ADD CONSTRAINT runs_initialized_from_fkey FOREIGN KEY (initialized_from) REFERENCES runs(run_id);


--
-- Name: runs_model_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY runs
    ADD CONSTRAINT runs_model_id_fkey FOREIGN KEY (model_id) REFERENCES models(model_id);


--
-- Name: times_time_set_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY times
    ADD CONSTRAINT times_time_set_id_fkey FOREIGN KEY (time_set_id) REFERENCES time_sets(time_set_id);


--
-- Name: variables_variable_alias_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY variables
    ADD CONSTRAINT variables_variable_alias_id_fkey FOREIGN KEY (variable_alias_id) REFERENCES variable_aliases(variable_alias_id);


--
-- Name: y_cell_bounds_grid_id_fkey; Type: FK CONSTRAINT; Schema: pcic_meta; Owner: pcic_meta
--

ALTER TABLE ONLY y_cell_bounds
    ADD CONSTRAINT y_cell_bounds_grid_id_fkey FOREIGN KEY (grid_id) REFERENCES grids(grid_id);


--
-- Name: pcic_meta; Type: ACL; Schema: -; Owner: pcic_meta
--

REVOKE ALL ON SCHEMA pcic_meta FROM PUBLIC;
REVOKE ALL ON SCHEMA pcic_meta FROM pcic_meta;
GRANT ALL ON SCHEMA pcic_meta TO pcic_meta;
GRANT USAGE ON SCHEMA pcic_meta TO viewer;
GRANT USAGE ON SCHEMA pcic_meta TO httpd_meta;


--
-- Name: climatological_times; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE climatological_times FROM PUBLIC;
REVOKE ALL ON TABLE climatological_times FROM pcic_meta;
GRANT ALL ON TABLE climatological_times TO pcic_meta;
GRANT SELECT ON TABLE climatological_times TO steward;


--
-- Name: data_file_variables; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE data_file_variables FROM PUBLIC;
REVOKE ALL ON TABLE data_file_variables FROM pcic_meta;
GRANT ALL ON TABLE data_file_variables TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE data_file_variables TO steward;
GRANT SELECT ON TABLE data_file_variables TO httpd_meta;


--
-- Name: data_file_variables_data_file_variable_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE data_file_variables_data_file_variable_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE data_file_variables_data_file_variable_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE data_file_variables_data_file_variable_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE data_file_variables_data_file_variable_id_seq TO steward;


--
-- Name: data_file_variables_qc_flags; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE data_file_variables_qc_flags FROM PUBLIC;
REVOKE ALL ON TABLE data_file_variables_qc_flags FROM pcic_meta;
GRANT ALL ON TABLE data_file_variables_qc_flags TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE data_file_variables_qc_flags TO steward;
GRANT SELECT ON TABLE data_file_variables_qc_flags TO httpd_meta;


--
-- Name: data_files; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE data_files FROM PUBLIC;
REVOKE ALL ON TABLE data_files FROM pcic_meta;
GRANT ALL ON TABLE data_files TO pcic_meta;
GRANT SELECT,INSERT ON TABLE data_files TO steward;
GRANT SELECT ON TABLE data_files TO httpd_meta;


--
-- Name: data_files_data_file_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE data_files_data_file_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE data_files_data_file_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE data_files_data_file_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE data_files_data_file_id_seq TO steward;


--
-- Name: emissions; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE emissions FROM PUBLIC;
REVOKE ALL ON TABLE emissions FROM pcic_meta;
GRANT ALL ON TABLE emissions TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE emissions TO steward;
GRANT SELECT ON TABLE emissions TO httpd_meta;


--
-- Name: ensemble_data_file_variables; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE ensemble_data_file_variables FROM PUBLIC;
REVOKE ALL ON TABLE ensemble_data_file_variables FROM pcic_meta;
GRANT ALL ON TABLE ensemble_data_file_variables TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE ensemble_data_file_variables TO steward;
GRANT SELECT ON TABLE ensemble_data_file_variables TO httpd_meta;


--
-- Name: ensembles; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE ensembles FROM PUBLIC;
REVOKE ALL ON TABLE ensembles FROM pcic_meta;
GRANT ALL ON TABLE ensembles TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE ensembles TO steward;
GRANT SELECT ON TABLE ensembles TO httpd_meta;


--
-- Name: ensembles_ensemble_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE ensembles_ensemble_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE ensembles_ensemble_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE ensembles_ensemble_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE ensembles_ensemble_id_seq TO steward;


--
-- Name: grids; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE grids FROM PUBLIC;
REVOKE ALL ON TABLE grids FROM pcic_meta;
GRANT ALL ON TABLE grids TO pcic_meta;
GRANT SELECT ON TABLE grids TO steward;


--
-- Name: grid_grid_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE grid_grid_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE grid_grid_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE grid_grid_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE grid_grid_id_seq TO steward;


--
-- Name: level_sets; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE level_sets FROM PUBLIC;
REVOKE ALL ON TABLE level_sets FROM pcic_meta;
GRANT ALL ON TABLE level_sets TO pcic_meta;
GRANT SELECT ON TABLE level_sets TO steward;


--
-- Name: level_sets_level_set_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE level_sets_level_set_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE level_sets_level_set_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE level_sets_level_set_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE level_sets_level_set_id_seq TO steward;


--
-- Name: levels; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE levels FROM PUBLIC;
REVOKE ALL ON TABLE levels FROM pcic_meta;
GRANT ALL ON TABLE levels TO pcic_meta;
GRANT SELECT ON TABLE levels TO steward;


--
-- Name: models; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE models FROM PUBLIC;
REVOKE ALL ON TABLE models FROM pcic_meta;
GRANT ALL ON TABLE models TO pcic_meta;
GRANT SELECT,UPDATE ON TABLE models TO steward;
GRANT SELECT ON TABLE models TO httpd_meta;


--
-- Name: models_model_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE models_model_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE models_model_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE models_model_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE models_model_id_seq TO steward;


--
-- Name: qc_flags; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE qc_flags FROM PUBLIC;
REVOKE ALL ON TABLE qc_flags FROM pcic_meta;
GRANT ALL ON TABLE qc_flags TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE qc_flags TO steward;


--
-- Name: qc_flags_qc_flag_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE qc_flags_qc_flag_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE qc_flags_qc_flag_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE qc_flags_qc_flag_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE qc_flags_qc_flag_id_seq TO steward;


--
-- Name: runs; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE runs FROM PUBLIC;
REVOKE ALL ON TABLE runs FROM pcic_meta;
GRANT ALL ON TABLE runs TO pcic_meta;
GRANT SELECT,INSERT,UPDATE ON TABLE runs TO steward;
GRANT SELECT ON TABLE runs TO httpd_meta;


--
-- Name: runs_run_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE runs_run_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE runs_run_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE runs_run_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE runs_run_id_seq TO steward;


--
-- Name: scenarios_scenario_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE scenarios_scenario_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE scenarios_scenario_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE scenarios_scenario_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE scenarios_scenario_id_seq TO steward;


--
-- Name: time_sets; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE time_sets FROM PUBLIC;
REVOKE ALL ON TABLE time_sets FROM pcic_meta;
GRANT ALL ON TABLE time_sets TO pcic_meta;
GRANT SELECT ON TABLE time_sets TO steward;


--
-- Name: time_sets_time_set_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE time_sets_time_set_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE time_sets_time_set_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE time_sets_time_set_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE time_sets_time_set_id_seq TO steward;


--
-- Name: times; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE times FROM PUBLIC;
REVOKE ALL ON TABLE times FROM pcic_meta;
GRANT ALL ON TABLE times TO pcic_meta;
GRANT SELECT ON TABLE times TO steward;


--
-- Name: variable_aliases; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE variable_aliases FROM PUBLIC;
REVOKE ALL ON TABLE variable_aliases FROM pcic_meta;
GRANT ALL ON TABLE variable_aliases TO pcic_meta;
GRANT SELECT,UPDATE ON TABLE variable_aliases TO steward;
GRANT SELECT ON TABLE variable_aliases TO httpd_meta;


--
-- Name: variable_aliases_variable_alias_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE variable_aliases_variable_alias_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE variable_aliases_variable_alias_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE variable_aliases_variable_alias_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE variable_aliases_variable_alias_id_seq TO steward;


--
-- Name: variables; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE variables FROM PUBLIC;
REVOKE ALL ON TABLE variables FROM pcic_meta;
GRANT ALL ON TABLE variables TO pcic_meta;
GRANT SELECT,UPDATE ON TABLE variables TO steward;
GRANT SELECT ON TABLE variables TO httpd_meta;


--
-- Name: variables_variable_id_seq; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON SEQUENCE variables_variable_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE variables_variable_id_seq FROM pcic_meta;
GRANT ALL ON SEQUENCE variables_variable_id_seq TO pcic_meta;
GRANT USAGE ON SEQUENCE variables_variable_id_seq TO steward;


--
-- Name: y_cell_bounds; Type: ACL; Schema: pcic_meta; Owner: pcic_meta
--

REVOKE ALL ON TABLE y_cell_bounds FROM PUBLIC;
REVOKE ALL ON TABLE y_cell_bounds FROM pcic_meta;
GRANT ALL ON TABLE y_cell_bounds TO pcic_meta;
GRANT SELECT ON TABLE y_cell_bounds TO steward;


-- View of current ens version.
CREATE VIEW ensembles_cur_version AS select e1.* from ensembles e1 left outer join ensembles e2 on (e1.ensemble_id = e2.ensemble_id and e1.version < e2.version) where e2.ensemble_id is null;

--
-- PostgreSQL database dump complete
--

