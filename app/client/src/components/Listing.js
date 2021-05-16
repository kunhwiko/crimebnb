import React from 'react';
import '../style/homepage.css';
import PageNavbar from './PageNavbar';
import Map from './Map';


export default class Listing extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className="all-listings">
        <PageNavbar active="Listing" />
        <header className="App-header">
        </header>
        <div className="container-fluid">
          <div className="row d-flex flex-fill min-vh-100">
              <Map center={{lat:40.8813, lng: -73.7465}} zoom={10}/>
          </div>
        </div>
      </div>
    );
  }
}
