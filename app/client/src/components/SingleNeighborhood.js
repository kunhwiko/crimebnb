import React from 'react';
import '../style/singleNeighborhood.css';
import PageNavbar from './PageNavbar';
import SingleNeighborhoodListingStats from './SingleNeighborhoodListingStats';
import SingleNeighborhoodListingGraph from './SingleNeighborhoodListingGraph';
import SingleNeighborhoodCrimeStats from './SingleNeighborhoodCrimeStats';
import SingleNeighborhoodCrimeGraph from './SingleNeighborhoodCrimeGraph'; 

export default class SingleNeighborhood extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    fetch(`http://localhost:5000/neighborhoods/${this.props.match.params.borough}/all-listing` ,
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(boroughAllListingData => {
      if (!boroughAllListingData) return;
      var borough = this.props.match.params.borough.charAt(0).toUpperCase() + this.props.match.params.borough.slice(1);
      if (borough == "Staten-island") {
        borough = "Staten Island"
      }
      this.setState({
        stats: <SingleNeighborhoodListingStats count={boroughAllListingData[0].cnt} mean={boroughAllListingData[0].average} median={boroughAllListingData[0].median}/>, 
        borough: borough
      });
    }, err => {
      console.log(err);
    }); 

    fetch(`http://localhost:5000/neighborhoods/${this.props.match.params.borough}/best-listing` ,
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(boroughBestListingData => {
      if (!boroughBestListingData) return;
      var borough = this.props.match.params.borough.charAt(0).toUpperCase() + this.props.match.params.borough.slice(1);
      if (borough == "Staten-island") {
        borough = "Staten Island"
      }
      var neighborhoods1 = [], neighborhoods2 = [], counts = [], ratings = []; 
      var countText = "Neighborhoods with Most Listings", countTick = "# of Listings";
      var ratingText = "Neighborhoods with Highest Average Ratings", ratingTick = "Rating Scores";

      for (var i = 0; i < 10; i++) {
        if (i < 5) {
          neighborhoods1.push(boroughBestListingData[i]["neighborhood"]); 
          counts.push(boroughBestListingData[i]["cnt"]); 
        } else {
          neighborhoods2.push(boroughBestListingData[i]["neighborhood"]); 
          ratings.push(boroughBestListingData[i]["avg_ratings"]);           
        }
      }
      this.setState({
        countGraph: <SingleNeighborhoodListingGraph name={neighborhoods1} score={counts} text={countText} tick={countTick}/>,
        ratingGraph: <SingleNeighborhoodListingGraph name={neighborhoods2} score={ratings} text={ratingText} tick={ratingTick}/>,
      });
    }, err => {
      console.log(err);
    }); 

    fetch(`http://localhost:5000/neighborhoods/${this.props.match.params.borough}/all-crime` ,
    {
      method: 'GET'
    }).then(res => {
      return res.json();
    }, err => {
      console.log(err);
    }).then(boroughAllCrimeData => {
      if (!boroughAllCrimeData) return;
      var borough = this.props.match.params.borough.charAt(0).toUpperCase() + this.props.match.params.borough.slice(1);
      if (borough == "Staten-island") {
        borough = "Staten Island"
      }
      var crimes = [], countCrime = [], crime1 = [], crime2 = [], crime3 = [], crime4 = [], crime5 = []; 

      for (var i = 0; i < 5; i++) {
        crimes.push(boroughAllCrimeData[i]["description"]); 
        countCrime.push(boroughAllCrimeData[i]["cnt"]); 
        if (i == 0) {
          crime1.push(boroughAllCrimeData[i]["cnt_2015"], boroughAllCrimeData[i]["cnt_2016"],
          boroughAllCrimeData[i]["cnt_2017"], boroughAllCrimeData[i]["cnt_2018"]);
        } else if (i == 1) {
          crime2.push(boroughAllCrimeData[i]["cnt_2015"], boroughAllCrimeData[i]["cnt_2016"],
          boroughAllCrimeData[i]["cnt_2017"], boroughAllCrimeData[i]["cnt_2018"]);          
        } else if (i == 2) {
          crime3.push(boroughAllCrimeData[i]["cnt_2015"], boroughAllCrimeData[i]["cnt_2016"],
          boroughAllCrimeData[i]["cnt_2017"], boroughAllCrimeData[i]["cnt_2018"]);
        } else if (i == 3) {
          crime4.push(boroughAllCrimeData[i]["cnt_2015"], boroughAllCrimeData[i]["cnt_2016"],
          boroughAllCrimeData[i]["cnt_2017"], boroughAllCrimeData[i]["cnt_2018"]);
        } else if (i == 4) {
          crime5.push(boroughAllCrimeData[i]["cnt_2015"], boroughAllCrimeData[i]["cnt_2016"],
          boroughAllCrimeData[i]["cnt_2017"], boroughAllCrimeData[i]["cnt_2018"]);
        }   
      }
      this.setState({
        countCrimeGraph: <SingleNeighborhoodCrimeGraph name={crimes} score={countCrime}/>,
        crimesPerYear: <SingleNeighborhoodCrimeStats name={crimes} crime1={crime1} 
        crime2={crime2} crime3={crime3} crime4={crime4} crime5 ={crime5}/>
      });
    }, err => {
      console.log(err);
    }); 
  };


  render() {
    const { stats, borough, countGraph, ratingGraph, countCrimeGraph, crimesPerYear } = this.state;
    return (
      <div className="single-neighborhood">
        <PageNavbar active="Neighborhoods" />
        <header className="App-header">
        </header>
        <div className="stats">
          <h3 className="stats1-title">Statistics for Listings in {borough}</h3>
          {stats}
        </div>
        <div className="stats">
          <h3 className="stats1-title">Listings per Neighborhood in {borough}</h3>
          <div className="row">
            {
            <div className="col-xs-6 col-sm-6">
              {countGraph}
            </div>
            }
            {
            <div className="col-xs-6 col-sm-6">
              {ratingGraph}
            </div>
            }
          </div>
        </div>
        <div className="stats">
          <h3 className="stats1-title">Most Common Crimes in {borough} 2015-2018</h3>
          <div className="row">
            {
            <div className="col-xs-5 col-sm-5">
              {countCrimeGraph}
            </div>
            }
            {
            <div className="col-xs-6 col-sm-6">
              {crimesPerYear}
            </div>
            }
          </div>
        </div>
      </div>
    );
  }
}

