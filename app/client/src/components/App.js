import React from 'react';
import {
  BrowserRouter as Router,
  Route,
  Switch
} from 'react-router-dom';
import Homepage from './Homepage';
import Neighborhood from './Neighborhood';
import SingleListing from './SingleListing';
import SingleNeighborhood from './SingleNeighborhood';
import FilteredListings from './FilteredListings';
import AllNeighborhood from './AllNeighborhood';

export default class App extends React.Component {

  render() {
    return (
      <div className="App">
        <Router>
          <Switch>
            <Route
              exact
              path="/"
              component={Homepage}
            />
            <Route
              exact
              path="/home"
              component={Homepage}
            />
            <Route
              path="/listings/:id"
              component={SingleListing}
            />
            <Route
              exact
              path="/filteredListings"
              component={FilteredListings}
            />
            <Route
              path="/neighborhoods/:borough"
              component={SingleNeighborhood}
            />
            <Route
              exact
              path="/neighborhoods"
              component={Neighborhood}
            />
            <Route
              exact
              path="/neighborhood-comparison"
              component={AllNeighborhood}
            />
          </Switch>
        </Router>
      </div>
    );
  }
}
