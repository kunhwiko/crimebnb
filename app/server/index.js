const bodyParser = require('body-parser');
const express = require('express');
var routes = require("./routes.js");
const cors = require('cors');

const app = express();

app.use(cors({credentials: true, origin: 'http://localhost:3000'}));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

/* ---------------------------------------------------------------- */
/* ------------------- Route handler registration ----------------- */
/* ---------------------------------------------------------------- */
app.get('/listings/', routes.getTopListings);

app.get('/listings/:id', routes.getListingLocation);

app.get('/recommendations/', routes.getSimilarListings);

app.get('/maxprices', routes.getMaxPrices);

app.get('/minaccommodates', routes.getNumAccommodates);

app.get('/boroughs', routes.getBoroughs);

app.get('/beds', routes.getBeds);

app.get('/baths', routes.getBaths);

app.get('/location', routes.getLocScores);

app.get('/clean', routes.getCleanScores);

app.get('/filteredListings/:maxprice/:accommodates/:borough/:beds/:baths/:cleanscore/:locscore', routes.getListings);

app.get('/crimes/', routes.getTopCrimeAreas);

app.get('/crimes/nearby/', routes.getNearbyCrimes);

app.get('/listings/:id/crime-summary', routes.getListingCrimeSummary);

app.get('/neighborhoods/:borough/all-listing', routes.getBoroughAllListingInfo); 

app.get('/neighborhoods/:borough/best-listing', routes.getBoroughBestListingInfo); 

app.get('/neighborhoods/:borough/all-crime', routes.getBoroughAllCrimeInfo); 

app.get('/AllNeighborhoodCrimes', routes.getCrimeList);

app.get('/AllNeighborhoodSex', routes.getSexGroup);

app.get('/AllNeighborhoodAge', routes.getAgeGroup);

app.get('/crimeRank/:crime/:year', routes.getCrimeRank);

app.get('/victimRank/:sex/:age', routes.getVictimRank);

app.get('/ratingRank/:minNight/:maxNight/:bedrooms/:bathrooms/:kitchen/:cableTV/:internet/:wifi/:heating/:airCondition/:parking/:maxPrice/:minPrice', routes.getRatingRank);


app.listen(5000, () => {
	console.log(`Server listening on PORT 5000`);
});
