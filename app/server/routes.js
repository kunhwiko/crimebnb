const config = require('./db-config.js');
const mysql = require('mysql');

config.connectionLimit = 10;
const connection = mysql.createPool(config);

/* -------------------------------------------------- */
/* ------------------- Route Handlers --------------- */
/* -------------------------------------------------- */

const getListingLocation = (req, res) => {
  var listing_id = req.params.id;
  const query = `
    SELECT
      l.listing_id,
      listing_name,
      price,
      borough,
      neighborhood,
      room_type,
      accommodates,
      latitude,
      longitude,
      hostname,
      host_total_listing,
      host_is_superhost,
      number_of_ratings,
      rating_score,
      cleanliness_score,
      communication_score,
      location_score,
      value_score
    FROM crimebnb.listing l
    JOIN crimebnb.host h ON h.id = l.host_id
    JOIN crimebnb.ratings r on r.listing_id  = l.listing_id
    WHERE l.listing_id = '${listing_id}'
    LIMIT 1
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getSimilarListings = (req, res) => {
  // anything withing 10% of the current listing's price
  var price = parseFloat(req.query.price);

  var max_price = price + price*0.1
  var min_price = price - price*0.1
  
  var lat = parseFloat(req.query.latitude);
  var lon = parseFloat(req.query.longitude);


  const query = `
    SELECT
      l.listing_id,
      listing_name,
      neighborhood,
      price,
      room_type,
      accommodates,
      latitude,
      longitude,
      rating_score,
      sqrt(power((latitude - ${lat}), 2) + power((longitude - ${lon}), 2)) as distance
    FROM crimebnb.listing l
    JOIN crimebnb.ratings r on r.listing_id  = l.listing_id
    WHERE (
      l.listing_id != '${req.query.listing_id}' 
      AND l.price BETWEEN ${min_price} AND ${max_price}

    )
    ORDER BY distance ASC
    LIMIT ${req.params.limit || 5}
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getListingCrimeSummary = (req, res) => {
  var listing_id = req.params.id;
  const query = `
  with
	listing_location as (
		select latitude as listing_lat, longitude as listing_lon, round(latitude, 2) as close_lat, round(longitude, 2) as close_lon
		from crimebnb.listing l 
		where listing_id = ${listing_id}
	),
	closest_precinct as (
		select precinct
		from crimebnb.incident i 
		join listing_location l on round(i.latitude, 2) = l.close_lat and round(i.longitude, 2) = l.close_lon
		order by sqrt(POWER((latitude - listing_lat), 2) + power((longitude - listing_lon), 2)) desc
		limit 1
	),
	precinct_crimes as (
		select i.precinct, description as crime_type, count(crime) as n_reported_incidents
		from crimebnb.incident i
		join crimebnb.crime c on c.code = i.crime
		join closest_precinct cp on cp.precinct = i.precinct 
		where year(report_date) > 2015
		group by description
		order by count(crime) desc
		limit 5
	)
  select * from precinct_crimes
  `;

  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });

}

const getMaxPrices = (req, res) => {
  const query = `
    SELECT DISTINCT truncate(price, -2) AS price
    FROM crimebnb.listing
    WHERE truncate(price, -2) >= 50
    ORDER BY truncate(price, -2) ASC
    LIMIT 50
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getNumAccommodates = (req, res) => {
  const query = `
    SELECT DISTINCT accommodates
    FROM crimebnb.listing
    WHERE accommodates IS NOT NULL
    ORDER BY accommodates ASC
    LIMIT 15
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getBoroughs = (req, res) => {
  const query = `
    SELECT DISTINCT borough
    FROM crimebnb.listing
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getBeds = (req, res) => {
  const query = `
    SELECT DISTINCT bedrooms
    FROM crimebnb.amenities
    WHERE bedrooms IS NOT NULL
    ORDER BY bedrooms ASC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getBaths = (req, res) => {
  const query = `
    SELECT DISTINCT bathrooms
    FROM crimebnb.amenities
    WHERE bathrooms IS NOT NULL
    ORDER BY bathrooms ASC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getLocScores = (req, res) => {
  const query = `
    SELECT 1 as location_score
    UNION ALL
    SELECT DISTINCT location_score
    FROM crimebnb.ratings
    ORDER BY location_score ASC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getCleanScores = (req, res) => {
  const query = `
    SELECT 1 as cleanliness_score
    UNION ALL
    SELECT DISTINCT cleanliness_score
    FROM crimebnb.ratings
    ORDER BY cleanliness_score ASC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getListings = (req, res) => {
  var inputNum = req.params.maxprice;
  var minAccommodates = req.params.accommodates;
  var borough = req.params.borough;
  var maxPrice = parseInt(inputNum);
  var beds = req.params.beds;
  var baths = req.params.baths;
  var locscore = req.params.locscore;
  var cleanscore = req.params.cleanscore;
  const query = `
  WITH listings AS (
    SELECT l.listing_id, l.listing_name, l.price, l.accommodates, l.borough, l.latitude, l.longitude, l.room_type
    FROM crimebnb.listing l
    WHERE l.price < '${maxPrice}' AND l.price > 0 AND l.accommodates >= '${minAccommodates}' AND l.borough = '${borough}'
    ), 
    amenities AS (
    SELECT a.listing_id, a.bedrooms, a.bathrooms
    FROM crimebnb.amenities a
    WHERE a.bedrooms >= '${beds}' AND a.bathrooms >= '${baths}'
    ),
    ratings AS (
    SELECT r.listing_id, r.location_score, r.cleanliness_score
    FROM crimebnb.ratings r
    WHERE r.location_score >= '${locscore}' AND r.cleanliness_score >= '${cleanscore}'
    ),
    incident AS (
    SELECT i.latitude, i.longitude, i.start_date
    FROM crimebnb.incident i
    ),
    tmp AS (
      SELECT DISTINCT l.listing_id, l.listing_name, l.price, l.accommodates, l.borough, l.latitude, l.longitude, l.room_type
      FROM listings l JOIN amenities a ON l.listing_id = a.listing_id JOIN ratings r ON l.listing_id = r.listing_id
      ORDER BY l.price ASC
    )
    
  SELECT DISTINCT listing_id, listing_name, price, accommodates, room_type, borough, tmp.latitude, tmp.longitude, COUNT(*) as num_crimes
  FROM tmp JOIN incident ON ROUND(tmp.latitude,3) = ROUND(incident.latitude, 3) AND ROUND(tmp.longitude,3) = ROUND(incident.longitude, 3)
  WHERE start_date > '2010-01-01'
  GROUP BY listing_name
  ORDER BY num_crimes ASC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getTopListings = (req, res) => {
  const query = `
  SELECT l.listing_id, latitude, longitude
  from crimebnb.listing l
  join crimebnb.ratings r on r.listing_id = l.listing_id 
  where (
    latitude is not null and
    longitude is not null and
    accommodates is not null and
    r.number_of_ratings > 25 and
    r.rating_score > 95
    )
    limit 100
  `
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}

const getTopCrimeAreas = (req, res) => {
  const query = `
    SELECT count(number) as n_crimes, latitude, longitude
	  FROM crimebnb.incident i
	  WHERE latitude is not null AND longitude is not null
	  GROUP BY latitude, longitude
	  ORDER BY count(number) desc
    LIMIT 100;
  `
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });

};

const getNearbyCrimes = (req, res) => {
  var lat = parseFloat(req.query.latitude);
  var lon = parseFloat(req.query.longitude);

  const buffer = 0.001;

  const max_lat = lat + buffer;
  const min_lat = lat - buffer;
  const max_lon = lon + buffer;
  const min_lon = lon - buffer;

  const query = `
    SELECT count(number) as n_crimes, latitude, longitude
    FROM crimebnb.incident
    WHERE (latitude between ${min_lat} AND ${max_lat}) AND (longitude BETWEEN ${min_lon} AND ${max_lon})
    GROUP BY latitude, longitude
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}



const getBoroughAllListingInfo = (req, res) => {
  var currBorough = req.params.borough; 
  if (currBorough === "staten-island") {
    currBorough = "Staten Island";
  } else {
    currBorough = currBorough.charAt(0).toUpperCase() + currBorough.slice(1);
  }

  /* 
   * find number of listings and mean/median price of listings in a borough  
   */
  const query = `
    WITH tmp AS (
      SELECT price 
      FROM crimebnb.listing
      WHERE borough = "${currBorough}"
      ORDER BY price 
    ),
    statistics AS (
      SELECT count(price) AS cnt, AVG(price) AS average 
      FROM tmp
    )
    SELECT statistics.cnt, statistics.average, AVG(price) AS median
    FROM (
      SELECT price, @rownum:=@rownum+1 AS "row_number", @total_rows:=@rownum
      FROM tmp, (SELECT @rownum:=0) r
    ) AS p, statistics
    WHERE p.row_number IN ( FLOOR((@total_rows+1)/2), FLOOR((@total_rows+2)/2) );
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getBoroughBestListingInfo = (req, res) => {
  var currBorough = req.params.borough; 
  if (currBorough === "staten-island") {
    currBorough = "Staten Island";
  } else {
    currBorough = currBorough.charAt(0).toUpperCase() + currBorough.slice(1);
  }

  /* 
   * find neighborhoods in a borough with the most listings & 
   * find neighborhoods in a borough with listings with the highest average ratings 
   */
  const query = `
    WITH listing_info AS (
      SELECT neighborhood, COUNT(neighborhood) AS cnt, AVG(rating_score) AS avg_ratings 
      FROM (
        SELECT listing_id, neighborhood
        FROM crimebnb.listing 
        WHERE borough = "${currBorough}"
      ) L 
      JOIN (
        SELECT listing_id, rating_score 
        FROM crimebnb.ratings 
        WHERE rating_score > 0 
      ) R ON L.listing_id = R.listing_id 
      GROUP BY neighborhood  
    ), listing_by_count AS (
      SELECT neighborhood, cnt, avg_ratings 
      FROM listing_info
      ORDER BY cnt DESC
      LIMIT 5
    ), listing_by_rating AS (
      SELECT neighborhood, cnt, avg_ratings  
      FROM listing_info
      WHERE cnt > 10 
      ORDER BY avg_ratings DESC
      LIMIT 5
    )
    SELECT neighborhood, cnt, avg_ratings
    FROM listing_by_count
    UNION ALL
    SELECT neighborhood, cnt, avg_ratings
    FROM listing_by_rating;
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getBoroughAllCrimeInfo = (req, res) => {
  var currBorough = req.params.borough; 

  if (currBorough === "manhattan") {
    currBorough = "PATROL BORO MAN";  
  } else if (currBorough === "brooklyn") {
    currBorough = "PATROL BORO BKLYN";      
  } else if (currBorough === "queens") {
    currBorough = "PATROL BORO QUEENS";      
  } else if (currBorough === "bronx") {
    currBorough = "PATROL BORO BRONX";      
  } else if (currBorough === "staten-island") {
    currBorough = "PATROL BORO STATEN";      
  }

  /* 
   * find total number of crimes for a borough post 2016 &  
   * find most common crimes in a borough post 2016 
   */
  const query = `
    WITH crime_date_from_2015 AS (
      SELECT code, description, crime_years
      FROM (
        SELECT code, description 
        FROM crimebnb.crime 	
      ) crime_code
      JOIN (
        SELECT crime, YEAR(report_date) AS crime_years
        FROM crimebnb.incident 
        WHERE patrol_borough LIKE "${currBorough}%" AND report_date >= DATE("2015-01-01") AND report_date < DATE("2019-01-01")
      ) crimes ON crime_code.code = crimes.crime 
    ), most_reported_crimes AS (
      SELECT description, COUNT(description) AS cnt 
      FROM crime_date_from_2015
      GROUP BY description
      ORDER BY COUNT(description) DESC
      LIMIT 5
    ), crimes_2015 AS (
      SELECT description, COUNT(crime_years) AS cnt_2015
      FROM crime_date_from_2015
      WHERE crime_years = 2015
      GROUP BY description
    ), crimes_2016 AS (
      SELECT description, COUNT(crime_years) AS cnt_2016
      FROM crime_date_from_2015
      WHERE crime_years = 2016
      GROUP BY description
    ), crimes_2017 AS (
      SELECT description, COUNT(crime_years) AS cnt_2017
      FROM crime_date_from_2015
      WHERE crime_years = 2017
      GROUP BY description
    ), crimes_2018 AS (
      SELECT description, COUNT(crime_years) AS cnt_2018
      FROM crime_date_from_2015
      WHERE crime_years = 2018
      GROUP BY description
    )
    SELECT most_reported_crimes.description, cnt, cnt_2015, cnt_2016, cnt_2017, cnt_2018
    FROM most_reported_crimes
    JOIN crimes_2015 ON most_reported_crimes.description = crimes_2015.description
    JOIN crimes_2016 ON most_reported_crimes.description = crimes_2016.description
    JOIN crimes_2017 ON most_reported_crimes.description = crimes_2017.description
    JOIN crimes_2018 ON most_reported_crimes.description = crimes_2018.description
    `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
};

const getCrimeList = (req, res) => {
  const query = `
    SELECT DISTINCT description
    FROM crimebnb.offense
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}

const getSexGroup = (req, res) => {
  const query = `
    SELECT DISTINCT sex
    FROM crimebnb.victim
    WHERE sex IS NOT NULL
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}

const getAgeGroup = (req, res) => {
  const query = `
    SELECT DISTINCT age
    FROM crimebnb.victim
    WHERE age IS NOT NULL
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}


const getCrimeRank = (req, res) => {

  var inputCrime = req.params.crime;
  var inputYear = parseInt(req.params.year);
  const query = `
  WITH relevantCrimes as (SELECT *
  FROM  crimebnb.incident JOIN crimebnb.offense ON incident.crime = offense.code
  WHERE offense.description LIKE  "${inputCrime}" AND CONVERT(SUBSTRING(incident.start_date,1, 4), SIGNED) > "${inputYear}"),

  distinctBorough as (
    select DISTINCT incident.patrol_borough
    FROM crimebnb.incident
    WHERE incident.patrol_borough IS NOT NULL
    ),
  
  counted as (SELECT relevantCrimes.patrol_borough AS name, count(*) AS stat
  FROM relevantCrimes
  WHERE relevantCrimes.patrol_borough IS NOT NULL
  GROUP BY relevantCrimes.patrol_borough)

  select RIGHT(distinctBorough.patrol_borough, LENGTH(distinctBorough.patrol_borough) - 12) AS name, COALESCE(stat, 0)  as stat
  FROM distinctBorough LEFT JOIN counted ON distinctBorough.patrol_borough = counted.name
  ORDER BY stat DESC

  
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}

const getVictimRank = (req, res) => {
  var inputSex = req.params.sex;
  var inputAge = req.params.age;
  const query = `
  With victimCatalogue as (
  select incident.patrol_borough
  FROM crimebnb.crime JOIN crimebnb.incident ON incident.crime = crime.code
                      JOIN crimebnb.victim ON victim.incident_number = incident.number
  WHERE victim.sex LIKE "${inputSex}" AND victim.age LIKE "${inputAge}"),

  distinctBorough as (
  select DISTINCT incident.patrol_borough
  FROM crimebnb.incident
  WHERE incident.patrol_borough IS NOT NULL
  ),

  crimePerBorough as (
  SELECT victimCatalogue.patrol_borough AS name, count(*) AS stat
  FROM victimCatalogue
  WHERE victimCatalogue.patrol_borough IS NOT NULL
  GROUP BY victimCatalogue.patrol_borough
  ORDER By stat DESC)

  select RIGHT(distinctBorough.patrol_borough, LENGTH(distinctBorough.patrol_borough) - 12) AS name, COALESCE(stat, 0)  as stat
  FROM distinctBorough LEFT JOIN crimePerBorough ON distinctBorough.patrol_borough = crimePerBorough.name
  ORDER BY stat DESC
  `;
  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}

const getRatingRank = (req, res) => {
  var inputMax = parseInt(req.params.maxNight);
  var inputMin = parseInt(req.params.minNight);
  var inputBath = parseInt(req.params.bathrooms);
  var inputBed = parseInt(req.params.bedrooms);
  var inputMaxPrice = parseInt(req.params.maxPrice);
  var inputMinPrice = parseInt(req.params.minPrice);

  var inputKitchen = req.params.kitchen;
  
  var inputCableTV = req.params.cableTV;
  var inputInternet = req.params.internet;
  var inputWIFI = req.params.wifi;
  var inputHeating = req.params.heating;
  var inputAC = req.params.airCondition;
  var inputParking = req.params.parking;

  const query = `
  With listingFiltered as (SELECT listing.listing_id, listing.borough
    FROM crimebnb.listing
    WHERE listing.min_nights >= "${inputMin}" AND listing.max_nights <= "${inputMax}" 
    AND listing.price <= "${inputMaxPrice}" AND listing.price >= "${inputMinPrice}"),

    distinctBorough as (
      select DISTINCT listing.borough
      FROM crimebnb.listing
      WHERE listing.borough IS NOT NULL
      ),
    
    amenitiesFiltered as (SELECT amenities.listing_id, listingFiltered.borough
    FROM crimebnb.amenities JOIN listingFiltered ON amenities.listing_id = listingFiltered.listing_id
    WHERE (amenities.kitchen LIKE "${inputKitchen}" OR "${inputKitchen}" LIKE "FLAG") 
    AND (amenities.cable_tv LIKE "${inputCableTV}" OR "${inputCableTV}" LIKE "FLAG") 

    AND (amenities.internet LIKE "${inputInternet}" OR "${inputInternet}" LIKE "FLAG") 
    AND (amenities.wifi LIKE "${inputWIFI}" OR "${inputWIFI}" LIKE "FLAG") 
    AND (amenities.heating LIKE "${inputHeating}" OR "${inputHeating}" LIKE "FLAG") 
    AND (amenities.air_conditioning LIKE "${inputAC}" OR "${inputAC}" LIKE "FLAG")
    AND (amenities.free_parking LIKE "${inputParking}" OR "${inputParking}" LIKE "FLAG")
    
    AND amenities.bathrooms >= "${inputBath}"
    AND amenities.bedrooms >= "${inputBed}"),
    
    accommodationFiltered as (SELECT amenitiesFiltered.borough as name, AVG(ratings.rating_score) as stat
    FROM amenitiesFiltered JOIN crimebnb.ratings ON amenitiesFiltered.listing_id = ratings.listing_id
    GROUP BY amenitiesFiltered.borough)

    select distinctBorough.borough AS name, COALESCE(accommodationFiltered.stat, 0) as stat
    FROM distinctBorough LEFT JOIN accommodationFiltered ON distinctBorough.borough = accommodationFiltered.name
    ORDER BY stat DESC;


  `;

  connection.query(query, (err, rows, fields) => {
    if (err) console.log(err);
    else res.json(rows);
  });
}


module.exports = {
  getListingLocation: getListingLocation,
  getListings: getListings,
  getMaxPrices: getMaxPrices,
  getBoroughs: getBoroughs,
  getBeds: getBeds,
  getBaths: getBaths,
  getLocScores: getLocScores,
  getCleanScores: getCleanScores,
  getNumAccommodates: getNumAccommodates,
  getTopListings: getTopListings,
  getTopCrimeAreas: getTopCrimeAreas,
  getNearbyCrimes: getNearbyCrimes,
  getBoroughAllListingInfo: getBoroughAllListingInfo,
  getBoroughBestListingInfo: getBoroughBestListingInfo,
  getBoroughAllCrimeInfo: getBoroughAllCrimeInfo,
  getSimilarListings: getSimilarListings,
  getListingCrimeSummary: getListingCrimeSummary,
  getCrimeList: getCrimeList,
  getSexGroup: getSexGroup,
  getAgeGroup: getAgeGroup,
  getCrimeRank: getCrimeRank,
  getVictimRank: getVictimRank,
  getRatingRank: getRatingRank
};
