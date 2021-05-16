import React from 'react';
import '../style/singleListing.css';
import PageNavbar from './PageNavbar';
import Map from './Map';
import ListingPreview from './ListingPreview';
import SingleNeighborhoodListingGraph from './SingleNeighborhoodListingGraph';

export default class SingleListing extends React.Component {
  constructor(props) {
    super(props);

    this.state = {};
  }

  componentDidMount() {
    fetch(`http://localhost:5000/listings/${this.props.match.params.id}`,
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(listings => {
      if (!listings) return;
      const listing = listings[0];
      this.setState({
        name: listing.listing_name,
        price: listing.price,
        rating: listing.rating_score / 25.0,
        n_reviews: listing.number_of_ratings,
        cleanliness: listing.cleanliness_score / 2.0,
        communication: listing.communication_score / 2.0,
        location: listing.location_score / 2.0,
        value: listing.value_score / 2.0,
        guests: listing.accommodates,
        room_type: listing.room_type,
        borough: listing.borough,
        neighborhood: listing.neighborhood,
        host: listing.hostname,
        superhost: listing.host_is_superhost,
        n_listings: listing.host_total_listing
      });
      return listing 
    }, err => {
      console.log(err);
    }).then(listing => {
      return fetch(`http://localhost:5000/crimes/nearby/?latitude=${listing.latitude}&longitude=${listing.longitude}`,
      {
        method: 'GET'
      }).then(res => {
        return res.json();
      }, err => {
        console.log(err);
      }).then(crimes => {
        if (!crimes) return;
        const sl_map = <Map
          center={{lat:listing.latitude, lng:listing.longitude}}
          zoom={16}
          crimes={crimes}
        />;
        this.setState({
          map: sl_map,
        });
        return listing
      }, err => {
        console.log(err);
      })
    }).then(listing => {
      console.log(listing);
      return fetch(`http://localhost:5000/recommendations/?listing_id=${listing.listing_id}&latitude=${listing.latitude}&longitude=${listing.longitude}&accommodates=${listing.accommodates}&rating=${listing.rating}&price=${listing.price}`,
      {
        method: 'GET'
      }).then(res => {
        return res.json();
      }, err => {
        console.log(err);
      }).then(similar_listings => {
        if (!similar_listings) return;
        const similarListings = similar_listings.map(l => (
          <div className="col">
            <ListingPreview
              id={l.listing_id}
              name={l.listing_name}
              price={l.price}
              neighborhood={l.neighborhood}
              accommodates={l.accommodates}
              rating={l.rating_score}
            />
          </div>
        ))
        this.setState({similar_listings: similarListings})
      }, err => {
        console.log(err);
      })
    });
    fetch(`http://localhost:5000/listings/${this.props.match.params.id}/crime-summary`,
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(summary => {
      if (!summary) return;
      console.log(summary);
      const x_label = "Number of Crimes";
      const title = `Top 5 Crimes in Precinct ${summary[0].precinct}`;

      var crime_types = [];
      var crime_counts = [];
      for (var i = 0; i < summary.length; i++) {
        crime_types.push(summary[i].crime_type);
        crime_counts.push(summary[i].n_reported_incidents);
      }
      this.setState({
        precinct: summary[0].precinct,
        crime_summary: <SingleNeighborhoodListingGraph name={crime_types} score={crime_counts} text={title} tick={x_label}/>
      });

    }, err => {
      console.log(err);
    });
  };

  render() {
    return (
      <div className="single-listing">
        <PageNavbar active="Listing" />
        <header className="App-header">
        </header>
        <div className="container-fluid">
          <div className="row">
            <div className="col">
              <h2>{this.state.name}</h2>
            </div>
          </div>
          <div className="row">
            <div className="col-1">
              <p>{this.state.borough}</p>
            </div>
            <div className="col">
              <i className="rating bi-star-fill" width="32" height="32">{this.state.rating} ({this.state.n_reviews} reviews)</i>
            </div>
          </div>
          <div className="row">
            <div className="col">
              <div className="card">
                <div className="card-body">
                  <h5 className="card-title">Overview</h5>
                  <ul className="list-group">
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Price
                      <span className="badge badge-secondary badge-pill">{this.state.price}/night</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Room Type
                      <span className="badge badge-secondary badge-pill">{this.state.room_type}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Accommodates
                      <span className="badge badge-secondary badge-pill">{this.state.guests} guests</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Neighborhood
                      <span className="badge badge-secondary badge-pill">{this.state.neighborhood}</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>
            <div className="col">
              <div className="card">
                <div className="card-body">
                  <h5 className="card-title">Ratings</h5>
                  <ul className="list-group">
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Cleanliness
                      <span className="badge badge-secondary badge-pill">{this.state.cleanliness}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Communication
                      <span className="badge badge-secondary badge-pill">{this.state.communication}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Location
                      <span className="badge badge-secondary badge-pill">{this.state.location}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Value
                      <span className="badge badge-secondary badge-pill">{this.state.value}</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>
            <div className="col">
              <div className="card">
                <div className="card-body">
                  <h5 className="card-title">Host</h5>
                  <ul className="list-group">
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Name
                      <span className="badge badge-secondary badge-pill">{this.state.host}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Superhost
                      <span className="badge badge-secondary badge-pill">{this.state.superhost}</span>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      Number of Listings
                      <span className="badge badge-secondary badge-pill">{this.state.n_listings}</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>
            <div className="col">
              <div className="card">
                <div className="card-body">
                  {this.state.crime_summary}
                </div>
              </div>
            </div>
          </div>
          <div className="map row d-flex flex-fill">
            <div className="col d-flex flex-fill">
              {this.state.map}
            </div>
          </div>
          <div className="row d-flex flex-fill">
            <div className="col d-flex flex-fill">
              <h2>Recommended listings nearby</h2>
            </div>
          </div>
          <div className="row d-flex flex-fill">
              {this.state.similar_listings}
          </div>
        </div>
      </div>
    );
  }
}
