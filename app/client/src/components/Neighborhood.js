import React from "react";
import {Link} from "react-router-dom";
import Button from '@material-ui/core/Button';
import "../style/neighborhood.css";
import "bootstrap/dist/css/bootstrap.min.css";
import mapboxgl from 'mapbox-gl/dist/mapbox-gl-csp';
import MapboxWorker from 'worker-loader!mapbox-gl/dist/mapbox-gl-csp-worker';
import PageNavbar from './PageNavbar';

mapboxgl.workerClass = MapboxWorker;
mapboxgl.accessToken = 'pk.eyJ1IjoiemR1ZXkiLCJhIjoiY2ttNzNsbTRkMHVsZzJ1cHJ0M3l0dXd6bCJ9.XI4O4UFwSgw2fGy5Fp9sSA';


const locations = [
  {
    'id': '1',
    'title': 'Manhattan',
    'description': 'The capital of culture, finance, entertainment, and fashion. Need we say more?',
    'camera': {center: [-74.007, 40.7437], bearing: 25.3, zoom: 11.5, pitch: 50},
    'link': <Link to= {"/neighborhoods/manhattan"}>More on Manhattan</Link>
  },
  {
    'id': '2',
    'title': 'Bronx',
    'description': "A fantastic zoo and botanical garden. Not to mention the birthplace of hip-hop!",
    'camera': {center: [-73.8709, 40.8255], bearing: 0, zoom: 12.21, pitch: 50},
    'link': <Link to= {"/neighborhoods/bronx"}>More on Bronx</Link>
  },
  {
    'id': '3',
    'title': 'Brooklyn',
    'description': "This borough is experiencing a renaissance and continues to bring new surprises.",
    'camera': {center: [-73.9499, 40.626], bearing: -8.9, zoom: 11.68, pitch: 50},
    'link': <Link to= {"/neighborhoods/brooklyn"}>More on Brooklyn</Link>
  },
  {
    'id': '4',
    'title': 'Queens',
    'description': "Experience one of the world's most diverse places!",
    'camera': {center: [-73.8432, 40.6923], bearing: 36, zoom: 11.37, pitch: 50},
    'link': <Link to= {"/neighborhoods/queens"}>More on Queens</Link>
  },
  {
    'id': '5',
    'title': 'Staten Island',
    'description': 'A great place for family and friends with a stunning view, tons of parks, and a free ferry ride!',
    'camera': {center: [-74.1991, 40.5441], bearing: 28.4, zoom: 11.64, pitch: 50},
    'link': <Link to= {"/neighborhoods/staten-island"}>More on Staten Island</Link>
  },
  {
    'title': 'Five Boroughs of New York',
    'description': 'New York City is made up of five boroughs: Bronx, Brooklyn, Manhattan, Queens and Staten Island. Each one has its own attractions-not to mention data!',
    'camera': {center: [-74.0315, 40.6989], zoom: 9.68, bearing: 0, pitch: 0},
    'link': <Link to= {"/neighborhood-comparison"}>Find the best borough for you</Link>
  }
];


export default class Neighborhood extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      count: 5,
      selected: false, 
      currMap: null, 
      link: null, 
    }
    this.home = this.home.bind(this);
    this.manhattan = this.manhattan.bind(this);
    this.bronx = this.bronx.bind(this);
    this.brooklyn = this.brooklyn.bind(this);
    this.queens = this.queens.bind(this);
    this.staten = this.staten.bind(this);
    this.mapContainer = React.createRef(); 
  }

  componentDidMount() {
    const { count } = this.state;

    fetch('http://localhost:5000/crimes/',
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(crimes => {
      if (!crimes) return
      return crimes
    }, err => {
      console.log(err);
    }).then(crimes => {
      return fetch('http://localhost:5000/listings/',
      {
        method: 'GET'
      }).then(res => {
        return res.json();
      }, err => {
        console.log(err);
      }).then(listings => {
        const map = new mapboxgl.Map({
          container: this.mapContainer.current,
          style: 'mapbox://styles/mapbox/streets-v11',
          center: [locations[count].camera.center[0], locations[count].camera.center[1]],
          maxZoom: 16,
          minZoom: 9,
          zoom: locations[count].camera.zoom,
          bearing: locations[count].camera.bearing,
          pitch: 0 
        });

        // add listing markers
        var i = 0;
        for (i = 0; i < listings.length; i++) {
          const listing = listings[i]
          var marker = new mapboxgl.Marker({color:"#000080"})
            .setLngLat({lng: listing.longitude, lat: listing.latitude})
            .setPopup(new mapboxgl.Popup().setHTML('<a href=/listings/' + listing.listing_id + '>View</a>'))
            .addTo(map);
        }

        // add crime markers
        var i = 0;
        for (i = 0; i < crimes.length; i++) {
          var marker = new mapboxgl.Marker({color:"#900C3F"})
            .setLngLat({lng: crimes[i].longitude, lat: crimes[i].latitude})
            .setPopup(new mapboxgl.Popup().setText(`${crimes[i].n_crimes} crimes reported`))
            .addTo(map);
        }
        this.setState({currMap: map}); 
      }, err => {
        console.log(err);
      })
    });
  }

  animate(currMap, selected, index) {
    if (selected == false) {
      currMap.addSource('boroughs', {
        'type': 'vector',
        'url': 'mapbox://mapbox.8ibmsn6u'
      });
      currMap.addLayer(
        {
        'id': 'highlight',
        'type': 'fill',
        'source': 'boroughs',
        'source-layer': 'original',
        'paint': {
        'fill-color': '#fd6b50',
        'fill-opacity': 0.25
        },
        'filter': ['==', 'borocode', '']
        },
        'settlement-subdivision-label'
      ); 
      this.setState({selected: true})
    }
    currMap.setFilter('highlight', ['==', 'borocode', locations[index].id ? locations[index].id : '']);
    currMap.flyTo(locations[index].camera);
  }

  manhattan() {
    const { currMap, selected } = this.state;
    this.setState({count: 0, link: locations[0].link});
    this.animate(currMap, selected, 0);
  }

  bronx() {
    const { currMap, selected } = this.state;
    this.setState({count: 1, link: locations[1].link});
    this.animate(currMap, selected, 1);
  }

  brooklyn() {
    const { currMap, selected } = this.state;
    this.setState({count: 2, link: locations[2].link});
    this.animate(currMap, selected, 2);
  }

  queens() {
    const { currMap, selected } = this.state;
    this.setState({count: 3, link: locations[3].link});
    this.animate(currMap, selected, 3);
  }

  staten() {
    const { currMap, selected } = this.state;
    this.setState({count: 4, link: locations[4].link});
    this.animate(currMap, selected, 4);
  }

  home() {
    const { currMap } = this.state;
    this.setState({count: 5, link: locations[5].link});
    currMap.flyTo(locations[5].camera);
  }

  render() {
    const { count, link } = this.state;
    return (
      <div className="all-neighborhoods">
        <PageNavbar active="Neighborhood" />
        <header className="App-header">
        </header>
        <div className="button-container">
          <Button onClick={this.home} variant="contained">Home</Button>
          <Button onClick={this.manhattan} variant="contained">Manhattan</Button>
          <Button onClick={this.bronx} variant="contained">Bronx</Button>
          <Button onClick={this.brooklyn} variant="contained">Brooklyn</Button>
          <Button onClick={this.queens} variant="contained">Queens</Button>
          <Button onClick={this.staten} variant="contained">Staten Island</Button>
        </div>
        <div className="container-fluid">
          <div className="row d-flex flex-fill min-vh-100">
            <div ref={this.mapContainer} className="map-container flex-grow-1">
              <div className="map-overlay-container">
                <div className="map-overlay">
                  <h2 id="location-title">{locations[count].title}</h2>
                  <p id="location-description">{locations[count].description}</p>
                  {link}
                </div>
              </div>
            </div> 
          </div>
        </div>
      </div>
    );
  }
}
