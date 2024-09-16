CREATE TABLE logs_table (
	id int auto_increment primary key,
    script_name varchar(50),
    source_db varchar(50),
    destination_db varchar(50),
    name_table varchar(50),
    log_message TEXT,
    log_time timestamp default current_timestamp
);