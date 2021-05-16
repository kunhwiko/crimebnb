import React from 'react';
import "../style/mapbox.css";
import "bootstrap/dist/css/bootstrap.min.css";
import mapboxgl from 'mapbox-gl/dist/mapbox-gl-csp';
import MapboxWorker from 'worker-loader!mapbox-gl/dist/mapbox-gl-csp-worker';
 
mapboxgl.workerClass = MapboxWorker;
mapboxgl.accessToken = 'pk.eyJ1IjoiemR1ZXkiLCJhIjoiY2ttNzNsbTRkMHVsZzJ1cHJ0M3l0dXd6bCJ9.XI4O4UFwSgw2fGy5Fp9sSA';
 
export default class Map extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      lng: this.props.center.lng,
      lat: this.props.center.lat,
      zoom: this.props.zoom,
      crimes: this.props.crimes || [],
      listings: this.props.listings || [],
      bearing: this.props.bearing || 0,
      pitch: this.props.picth || 0
    };
    this.mapContainer = React.createRef();
  }
  componentDidMount() {
    const { lng, lat, zoom } = this.state;
    const map = new mapboxgl.Map({
      container: this.mapContainer.current,
      style: 'mapbox://styles/mapbox/streets-v11',
      center: [lng, lat],
      zoom: zoom
    });
   // add marker for the middle of the map
    new mapboxgl.Marker().setLngLat({lng: this.state.lng, lat: this.state.lat}).addTo(map);

    // add a marker for each crime that was passed
    var i = 0;
    for (i = 0; i < this.state.crimes.length; i++) {
      var marker = new mapboxgl.Marker({color:"#900C3F"})
        .setLngLat({lng: this.state.crimes[i].longitude, lat: this.state.crimes[i].latitude})
        .setPopup(new mapboxgl.Popup().setText(`${this.state.crimes[i].n_crimes} crimes reported`))
        .addTo(map);
    }

    // add a marker for each listing (must have listing_id, latitude, and longitude) that was passed
    var i = 0;
    for (i = 0; i < this.state.listings.length; i++) {
      const listing = this.state.listings[i]
      var marker = new mapboxgl.Marker({color:"#000080"})
        .setLngLat({lng: this.state.listings[i].longitude, lat: listing.latitude})
        .setPopup(new mapboxgl.Popup().setHTML('<a href=/listings/' + listing.listing_id + '>View</a>'))
        .addTo(map);
    }

  }
  render() {
    const { lng, lat, zoom } = this.state;
    return (
      <div ref={this.mapContainer} className="map-container flex-grow-1" />
    );
  }
}
