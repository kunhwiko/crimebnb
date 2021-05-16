use crimebnb;

CREATE TABLE borough (
    name varchar(14),
    primary key (name)
);

CREATE TABLE coordinate (
    latitude decimal(10, 8),
    longitude decimal(10, 8),
    primary key (latitude, longitude)
);

CREATE TABLE crime (
    code smallint unsigned,
    description varchar(255),
    primary key (code)
);

CREATE TABLE offense (
    code smallint unsigned,
    description varchar(255),
    primary key (code)
);


CREATE TABLE park (
    name varchar(255),
    primary key (name)
);

CREATE TABLE development (
    code int unsigned,
    name varchar(255),
    primary key (code, name)
);

CREATE TABLE agency (
    name varchar(255),
    code smallint unsigned,
    primary key (name)
);

CREATE TABLE incident (
    number int unsigned,
    report_date date,
    status char(9),
    classification varchar(12),
    start_date date,
    start_time time,
    end_date date,
    end_time time,
    premises_type varchar(255),
    precinct tinyint unsigned,
    patrol_borough varchar(255),
    station_name varchar(255),
    transit_district tinyint unsigned,
    crime smallint unsigned null,
    park varchar(255) null,
    development_code int unsigned null,
    development_name varchar(255) null,
    agency varchar(255) not null,
    latitude decimal(10, 8) null,
    longitude decimal(10, 8) null,
    primary key (number, report_date),
    foreign key (crime) references crime(code),
    foreign key (park) references park(name),
    foreign key (agency) references agency(name),
    foreign key (development_code, development_name) references development(code, name),
    foreign key (latitude, longitude) references coordinate(latitude, longitude)
);


CREATE TABLE victim (
    incident_number int unsigned not null,
    sex char(1),
    age varchar(24),
    race varchar(255),
    foreign key (incident_number) references incident(number)
);

CREATE TABLE suspect (
    incident_number int unsigned not null,
    sex char(1),
    age varchar(24),
    race varchar(255),
    foreign key (incident_number) references incident(number)
);

