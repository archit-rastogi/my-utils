CREATE TABLE IF NOT EXISTS base_task (
     uuid    VARCHAR(32)   PRIMARY KEY,
     name    VARCHAR(200)   not null,
     create_time DATETIME not null,
     start_time DATETIME,
     end_time DATETIME,
     status TINYINT not null,
     component_type TINYINT not null,
     task_type TINYINT not null,
     error_msg TEXT,
     meta_data TEXT
);


CREATE TABLE IF NOT EXISTS session (
     uuid    VARCHAR(32)   PRIMARY KEY,
     name    VARCHAR(200)   not null,
     create_time DATETIME not null,
     start_time DATETIME,
     end_time DATETIME,
     status TINYINT not null,
     component_type TINYINT not null,
     error_msg TEXT,
     meta_data TEXT
);

CREATE TABLE IF NOT EXISTS session_tasks (
     session_uuid VARCHAR(32),
     task_uuid VARCHAR(32),
     meta_data TEXT,
     PRIMARY KEY (session_uuid, task_uuid),
     FOREIGN KEY (session_uuid) REFERENCES session(uuid),
     FOREIGN KEY (task_uuid) REFERENCES base_task(uuid)
);

CREATE TABLE IF NOT EXISTS phase (
     uuid VARCHAR(32) PRIMARY KEY,
     name    VARCHAR(200)   not null,
     create_time DATETIME not null,
     start_time DATETIME,
     end_time DATETIME,
     status TINYINT not null,
     component_type TINYINT not null,
     error_msg TEXT,
     meta_data TEXT
);

CREATE TABLE IF NOT EXISTS phase_sessions (
    phase_uuid VARCHAR(32),
    session_uuid VARCHAR(32),
    meta_data TEXT,
    PRIMARY KEY (phase_uuid, session_uuid),
    FOREIGN KEY (phase_uuid) REFERENCES phase(uuid),
    FOREIGN KEY (session_uuid) REFERENCES session(uuid)
);

CREATE TABLE IF NOT EXISTS workload (
    uuid VARCHAR(32) PRIMARY KEY,
    name    VARCHAR(200)   not null,
    create_time DATETIME not null,
    start_time DATETIME,
    end_time DATETIME,
    status TINYINT not null,
    component_type TINYINT not null,
    error_msg TEXT,
    meta_data TEXT
);


CREATE TABLE IF NOT EXISTS workload_phases (
    workload_uuid VARCHAR(32),
    phase_uuid VARCHAR(32),
    meta_data TEXT,
    PRIMARY KEY (workload_uuid, phase_uuid),
    FOREIGN KEY (phase_uuid) REFERENCES phase(uuid),
    FOREIGN KEY (workload_uuid) REFERENCES workload(uuid)
);
