import React from 'react';
import PageNavbar from './PageNavbar';
import FilteredListingRow from './FilteredListingRow';
import '../style/filteredListings.css';
import mapboxgl from 'mapbox-gl/dist/mapbox-gl-csp';
import MapboxWorker from 'worker-loader!mapbox-gl/dist/mapbox-gl-csp-worker';

mapboxgl.workerClass = MapboxWorker;
mapboxgl.accessToken = 'pk.eyJ1IjoiemR1ZXkiLCJhIjoiY2ttNzNsbTRkMHVsZzJ1cHJ0M3l0dXd6bCJ9.XI4O4UFwSgw2fGy5Fp9sSA';

const locations = [
	{
	  'id': '1',
	  'title': 'Manhattan',
	  'description': 'The capital of culture, finance, entertainment, and fashion. Need we say more?',
	  'camera': {center: [-74.007, 40.7937], bearing: 25.3, zoom: 10.95, pitch: 50},
	},
	{
	  'id': '2',
	  'title': 'Bronx',
	  'description': "A fantastic zoo and botanical garden. Not to mention the birthplace of hip-hop!",
	  'camera': {center: [-73.8709, 40.8555], bearing: 0, zoom: 11.31, pitch: 50},
	},
	{
	  'id': '3',
	  'title': 'Brooklyn',
	  'description': "This borough is experiencing a renaissance and continues to bring new surprises.",
	  'camera': {center: [-73.9499, 40.681], bearing: -8.9, zoom: 10.98, pitch: 50},
	},
	{
	  'id': '4',
	  'title': 'Queens',
	  'description': "Experience one of the world's most diverse places!",
	  'camera': {center: [-73.8432, 40.7223], bearing: 36, zoom: 10.67, pitch: 50},
	},
	{
	  'id': '5',
	  'title': 'Staten Island',
	  'description': 'A great place for family and friends with a stunning view, tons of parks, and a free ferry ride!',
	  'camera': {center: [-74.1991, 40.5991], bearing: 28.4, zoom: 10.64, pitch: 50},
	},
	{
	  'title': 'Five Boroughs of New York',
	  'description': 'New York City is made up of five boroughs: Bronx, Brooklyn, Manhattan, Queens and Staten Island. Each one has its own attractions-not to mention data!',
	  'camera': {center: [-74.0315, 40.6989], zoom: 9.68, bearing: 0, pitch: 0},
	}
  ];

export default class FilteredListings extends React.Component {
	constructor(props) {
		super(props);

		this.state = {
			selectedMaxPrice: "100",
			selectedMinAccommodates: "1",
			selectedBorough: "BRONX",
			selectedBeds: "1",
			selectedBaths: "0",
			selectedLocScore: "0",
			selectedCleanScore: "0",
			maxPrices: [],
			minAccommodates: [],
			boroughs: [],
			beds: [],
			baths: [],
			locscore: [],
			cleanscore: [],
			listings: [],
			currMap: null,
			count: 1
		};

		this.mapContainer = React.createRef();
		this.submitFilteredListings = this.submitFilteredListings.bind(this);
		this.handleMaxPriceChange = this.handleMaxPriceChange.bind(this);
		this.handleBoroughChange = this.handleBoroughChange.bind(this);
		this.handleMinAccommodatesChange = this.handleMinAccommodatesChange.bind(this);
		this.handleBedsChange = this.handleBedsChange.bind(this);
		this.handleBathsChange = this.handleBathsChange.bind(this);
		this.handleCleanScoreChange = this.handleCleanScoreChange.bind(this);
		this.handleLocScoreChange = this.handleLocScoreChange.bind(this);
	};

	componentDidMount() {
		// Send an HTTP request to the server.
		fetch("http://localhost:5000/maxprices",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(priceList => {
		  if (!priceList) return;

		  const maxPriceOptions = priceList.map((price, i) =>
			<option className="priceOption" key={i} value={price.price}>${price.price}</option>
		  );
	
		  this.setState({
			maxPrices: maxPriceOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/minaccommodates",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(accommodatesList => {
		  if (!accommodatesList) return;

		  const minAccommodatesOptions = accommodatesList.map((num, i) =>
			<option className="accommodatesOption" key={i} value={num.accommodates}>{num.accommodates}</option>
		  );
	
		  this.setState({
			minAccommodates: minAccommodatesOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/boroughs",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(boroughsList => {
		  if (!boroughsList) return;

		  const boroughsOptions = boroughsList.map((borough, i) =>
			<option className="boroughsOption" key={i} value={borough.borough}>{borough.borough}</option>
		  );
	
		  this.setState({
			boroughs: boroughsOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/beds",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(bedsList => {
		  if (!bedsList) return;
		  const bedOptions = bedsList.map((beds, i) =>
			<option className="bedOption" key={i} value={beds.bedrooms}>{beds.bedrooms}</option>
		  );
	
		  this.setState({
			beds: bedOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/baths",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(bathsList => {
		  if (!bathsList) return;
		  const bathOptions = bathsList.map((baths, i) =>
			<option className="bathOption" key={i} value={baths.bathrooms}>{baths.bathrooms}</option>
		  );
	
		  this.setState({
			baths: bathOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/location",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(scoresList => {
		  if (!scoresList) return;
		  const locscoreOptions = scoresList.map((score, i) =>
			<option className="locscoreOption" key={i} value={score.location_score}>{score.location_score}</option>
		  );
	
		  this.setState({
			locscore: locscoreOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});

		fetch("http://localhost:5000/clean",
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(scoresList => {
		  if (!scoresList) return;
		  const cleanscoreOptions = scoresList.map((score, i) =>
			<option className="cleanscoreOption" key={i} value={score.cleanliness_score}>{score.cleanliness_score}</option>
		  );
	
		  this.setState({
			cleanscore: cleanscoreOptions
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});
	};

	handleMaxPriceChange(e) {
		this.setState({
			selectedMaxPrice: e.target.value
		});
	};

	handleMinAccommodatesChange(e) {
		this.setState({
			selectedMinAccommodates: e.target.value
		});
	};

	handleBoroughChange(e) {
		this.setState({
			selectedBorough: e.target.value,
		});
		if (e.target.value == 'MANHATTAN') { 
			this.setState({count: 0});
		} else if (e.target.value == 'STATEN ISLAND') { 
			this.setState({count: 4});
		} else if (e.target.value == 'BROOKLYN') { 
			this.setState({count: 2});
		} else if (e.target.value == 'QUEENS') { 
			this.setState({count: 3});
		} else { 
			this.setState({count: 1});
		}
	};

	handleBedsChange(e) {
		this.setState({
			selectedBeds: e.target.value
		});
	};

	handleBathsChange(e) {
		this.setState({
			selectedBaths: e.target.value
		});
	};

	handleLocScoreChange(e) {
		this.setState({
			selectedLocScore: e.target.value
		});
	};

	handleCleanScoreChange(e) {
		this.setState({
			selectedCleanScore: e.target.value
		});
	};

	submitFilteredListings() {
		// Send an HTTP request to the server.
		fetch("http://localhost:5000/filteredListings/" + this.state.selectedMaxPrice + "/" + this.state.selectedMinAccommodates + "/" + this.state.selectedBorough + "/" + this.state.selectedBeds + "/" + this.state.selectedBaths + "/" + this.state.selectedCleanScore + "/" + this.state.selectedLocScore,
		{
		  method: 'GET' // The type of HTTP request.
		}).then(res => {
		  // Convert the response data to a JSON.
		  return res.json();
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		}).then(listingsList => {
		  if (!listingsList) return;
          console.log(listingsList);
		  const filteredListings = listingsList.map((listing, i) =>
		  	<FilteredListingRow key={i} name={listing.listing_name} price={listing.price} accommodates={listing.accommodates} crimes={listing.num_crimes} listing_id={listing.listing_id} beds={listing.bedrooms} baths={listing.bathrooms} locscore={listing.location_score} cleanscore={listing.cleanliness_score} room_type={listing.room_type}/>
		  );



		  const map = new mapboxgl.Map({
			container: this.mapContainer.current,
			style: 'mapbox://styles/mapbox/streets-v11',
			center: [locations[this.state.count].camera.center[0], locations[this.state.count].camera.center[1]],
			maxZoom: 16,
			minZoom: 9,
			zoom: locations[this.state.count].camera.zoom,
			bearing: locations[this.state.count].camera.bearing,
			pitch: 0 
		  });
  
		  // add listing markers
		  var i = 0;
		  for (i = 0; i < listingsList.length; i++) {
			const listing = listingsList[i]
			var marker = new mapboxgl.Marker({color:"#000080"})
			  .setLngLat({lng: listing.longitude, lat: listing.latitude})
			  .setPopup(new mapboxgl.Popup().setHTML('<a href=/listings/' + listing.listing_id + '>View</a>'))
			  .addTo(map);
		  }

		  this.setState({currMap: map}); 
	
		  this.setState({
			listings: filteredListings
		  });
		}, err => {
		  // Print the error if there is one.
		  console.log(err);
		});
	};

	render() {
		return (
			<div className="FilteredListings">
				
				<PageNavbar active="listings" />

				<div className="container filteredlistings-container">
				<div className="h5">Filter Listings</div>
					<div className="jumbotron-top">
						<div className="boxLeft">
							<div className="dropdown-container">
								Max Price/Night &nbsp;
								<select value={this.state.selectedMaxPrice} onChange={this.handleMaxPriceChange} className="dropdown" id="pricesDropdown">
									{this.state.maxPrices}
								</select>
							</div>
							<div className="dropdown-container">
								Min Accommodations &nbsp;
								<select value={this.state.selectedMinAccommodates} onChange={this.handleMinAccommodatesChange} className="dropdown" id="accomodatesDropdown">
									{this.state.minAccommodates}
								</select>
							</div>
							<div className="dropdown-container">
								Borough &nbsp;
								<select value={this.state.selectedBorough} onChange={this.handleBoroughChange} className="dropdown" id="boroughDropdown">
									{this.state.boroughs}
								</select>
							</div>
						</div>
						<div className="boxCenter">
							<div className="dropdown-container">
								Min Bedrooms &nbsp;
								<select value={this.state.selectedBeds} onChange={this.handleBedsChange} className="dropdown" id="bedsDropdown">
									{this.state.beds}
								</select>
							</div>
							<div className="dropdown-container">
								Min Bathrooms &nbsp;
								<select value={this.state.selectedBaths} onChange={this.handleBathsChange} className="dropdown" id="bathsDropdown">
									{this.state.baths}
								</select>
							</div>
						</div>
						<div className="boxRight">
							<div className="dropdown-container">
								Min Cleanliness Score &nbsp;
								<select value={this.state.selectedCleanScore} onChange={this.handleCleanScoreChange} className="dropdown" id="cleanDropdown">
									{this.state.cleanscore}
								</select>
							</div>
							<div className="dropdown-container">
								Min Location Score &nbsp;
								<select value={this.state.selectedLocScore} onChange={this.handleLocScoreChange} className="dropdown" id="locDropdown">
									{this.state.locscore}
								</select>
							</div>
						</div> 
					</div>
					<div className="btn-container">
						<button className="btn btn-dark" id="submitBtn" onClick={this.submitFilteredListings}>Submit</button>
					</div>
					<div className="jumbotron-bottom">
						<div className="listings-container">
							<div className="listing">
			        </div>
					<div className="container-fluid">
					<div className="row d-flex flex-fill min-vh-100">
						<div ref={this.mapContainer} className="map flex-grow-1">
						</div> 
					</div>
					</div>
			        <div className="listings-container" id="results">
			          {this.state.listings}
			        </div>
			      </div>
			    </div>
			  </div>
			</div>
		);
	};
};