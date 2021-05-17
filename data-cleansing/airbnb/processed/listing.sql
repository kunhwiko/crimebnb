use crimebnb;

CREATE TABLE host (
    id int,
    hostname varchar(255),
    host_total_listing int,
    primary key (id)
);

CREATE TABLE listing (
    host_id int,
    listing_id int,
    listing_name varchar(255),
    host_is_superhost varchar(10),
    room_type varchar(25),
    accommodates int,
    price int,
    min_nights int,
    max_nights int,
    latitude decimal(10,8),
    longitude decimal(10,8),
    borough varchar(15),
    neighborhood varchar(35),
    primary key (host_id, listing_id),
    foreign key (borough) references borough(name),
    foreign key (host_id) references host(id)
);

CREATE TABLE neighborhood (
    name varchar(100),
    borough varchar(15),
    primary key (name),
    primary key (borough)
);

CREATE TABLE amenities (
    host_id int,
    listing_id int,
    bathrooms int,
    bedrooms int, 
    kitchen varchar(10),
    cable_tv varchar(10),
    internet varchar(10),
    wifi varchar(10),
    free_parking varchar(10),
    heating varchar(10),
    air_conditioning varchar(10),
    primary key (host_id, listing_id),
    foreign key (host_id) references listing(host_id),
    foreign key (listing_id) references listing(listing_id)    
);

CREATE TABLE ratings (
    host_id int,
    listing_id int,
    number_of_ratings int,
    rating_score int, 
    accuracy_score varchar(10),
    cleanliness_score varchar(10),
    checkin_score varchar(10),
    communication_score varchar(10),
    location_score varchar(10),
    value_score varchar(10),
    primary key (host_id, listing_id),
    foreign key (host_id) references listing(host_id),
    foreign key (listing_id) references listing(listing_id)    
);
