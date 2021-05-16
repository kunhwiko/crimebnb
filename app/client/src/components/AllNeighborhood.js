import React from 'react';
import '../style/allNeighborhood.css';
import PageNavbar from './PageNavbar';
import AllNeighborhoodRow from './AllNeighborhoodRow';


export default class AllNeighborhood extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
          selectedCrime: "",
          crimes: [],
          startingYear: "",
          neighborhoodRanked : [],
          selectedSexGroup : "",
          sexGroup : [],
          selectedAgeGroup : "",
          ageGroup : [],
          bedRoom : "1",
          bathRoom : "1",
          maxNight: "100",
          minNight: "0",
          maxPrice: "500",
          minPrice: "0",
          kitchen : "FLAG",
          cable_tv : "FLAG",
          internet : "FLAG",
          wifi : "FLAG",
          heating : "FLAG",
          air_conditioning : "FLAG",
          free_parking : "FLAG" 

        };

        this.handleBedRoomChange = this.handleBedRoomChange.bind(this);
        this.handleBathRoomChange = this.handleBathRoomChange.bind(this);
        this.handleKitchenChange = this.handleKitchenChange.bind(this);
        this.handleCableTVChange = this.handleCableTVChange.bind(this);
        this.handleInternetChange = this.handleInternetChange.bind(this);
        this.handleWIFIChange = this.handleWIFIChange.bind(this);
        this.handleHeatingChange = this.handleHeatingChange.bind(this);
        this.handleACChange = this.handleACChange.bind(this);
        this.handleFreeParkingChange = this.handleFreeParkingChange.bind(this);


        this.handleMaxPriceChange = this.handleMaxPriceChange.bind(this);
        this.handleMinPriceChange = this.handleMinPriceChange.bind(this);
        this.handleMaxNightChange = this.handleMaxNightChange.bind(this);
        this.handleMinNightChange = this.handleMinNightChange.bind(this);
        this.handleCrimeChange = this.handleCrimeChange.bind(this);
		    this.handleYearChange = this.handleYearChange.bind(this);
        this.handleSexChange = this.handleSexChange.bind(this);
        this.handleAgeChange = this.handleAgeChange.bind(this);

        this.submitSettings = this.submitSettings.bind(this);
        this.submitVictimSettings = this.submitVictimSettings.bind(this);
        this.submitAccomodationSetting = this.submitAccomodationSetting.bind(this);
		};
    
    componentDidMount() {
      fetch("http://localhost:5000/AllNeighborhoodCrimes",
        {
            method: 'GET' // The type of HTTP request.
        }).then(res => {
          return res.json();
        }, err => {
          console.log(err);
        }).then(componentList => {
        if (!componentList) return;
            const componentDivs = componentList.map((componentItem, i) =>
            <option classname = "crimeOption" key = {i} value = {componentItem.description} >
              {componentItem.description}
            </option>
          );

        // Set the state of the keywords list to the value returned by the HTTP response from the server.
        this.setState({
          crimes: componentDivs,
          selectedCrime : componentList[0]['description']
        });
      console.log(componentList[0]['description']);
      console.log(this.state.crimes);
      }, err => {
        // Print the error if there is one.
          console.log(err);
      });
      fetch("http://localhost:5000/AllNeighborhoodSex" ,
        {
            method: 'GET' // The type of HTTP request.
        }).then(res => {
          return res.json();
        }, err => {
          console.log(err);
        }).then(componentSexList => {
        if (!componentSexList) return;
            const componentSexDivs = componentSexList.map((componentSexItem, i) =>
            <option classname = "sexOption" key = {i} value = {componentSexItem.sex} >
              {componentSexItem.sex}
            </option>
            
          );
          

        // Set the state of the keywords list to the value returned by the HTTP response from the server.
        this.setState({
          sexGroup: componentSexDivs,
          selectedSexGroup : componentSexList[0]['sex']
        });
      }, err => {
        // Print the error if there is one.
          console.log(err);
      });
      fetch("http://localhost:5000/AllNeighborhoodAge" ,
        {
            method: 'GET' // The type of HTTP request.
        }).then(res => {
          return res.json();
        }, err => {
          console.log(err);
        }).then(componentAgeList => {
        if (!componentAgeList) return;
            const componentAgeDivs = componentAgeList.map((componentAgeItem, i) =>
            <option classname = "ageOption" key = {i} value = {componentAgeItem.age} >
              {componentAgeItem.age}
            </option>
          );

        // Set the state of the keywords list to the value returned by the HTTP response from the server.
        this.setState({
          ageGroup: componentAgeDivs,
          selectedAgeGroup : componentAgeList[0]['age']
        });
      console.log(componentAgeList[0]['age']);
      
      }, err => {
        // Print the error if there is one.
          console.log(err);
      });

    };


    handleCrimeChange(e) {
		this.setState({
			selectedCrime: e.target.value
		});
	};

    handleYearChange(e) {
		this.setState({
			startingYear: e.target.value
		});
	};
    handleSexChange(e) {
		this.setState({
			selectedSexGroup: e.target.value
		});
	};
    handleAgeChange(e) {
		this.setState({
			selectedAgeGroup: e.target.value
		});
	};

  handleMaxNightChange(e) {
		this.setState({
			maxNight: e.target.value
		});
	};

  handleMinNightChange(e) {
		this.setState({
			minNight: e.target.value
		});
	};

  handleMaxPriceChange(e) {
		this.setState({
			maxPrice: e.target.value
		});
	};
  handleMinPriceChange(e) {
		this.setState({
			minPrice: e.target.value
		});
	};

  handleKitchenChange(e) {
		this.setState({
			kitchen: e.target.value
		});
	};
  handleCableTVChange(e){
    this.setState({
			cable_tv: e.target.value
		});
  }
  handleInternetChange(e){
    this.setState({
			internet: e.target.value
		});

  }
  handleWIFIChange(e){
    this.setState({
			wifi: e.target.value
		});

  }
  handleACChange(e){
    this.setState({
			air_conditioning: e.target.value
		});

  }
  handleHeatingChange(e){
    this.setState({
			heating: e.target.value
		});

  }
  handleFreeParkingChange(e){
    this.setState({
			free_parking: e.target.value
		});
  }
  handleBathRoomChange(e){
    this.setState({
			bathRoom: e.target.value
		});
  }
  handleBedRoomChange(e){
    this.setState({
			bedRoom: e.target.value
		});
  }



  submitSettings() {
    
    fetch("http://localhost:5000/crimeRank/" + this.state.selectedCrime + '/' + this.state.startingYear,
    {
      method: "GET", 
    })
    .then(res => res.json()) 
    .then(neighborhoodList => {  

        const neighborhoodDiv = neighborhoodList.map((neighborhoodItem, i) =>
		  	<AllNeighborhoodRow key={i} name={neighborhoodItem.name} stat={neighborhoodItem.stat}/>);
      this.setState({
        neighborhoodRanked: neighborhoodDiv,
      });
    });
  };


  submitVictimSettings(){
    console.log("Hello im' victim setting");
    fetch("http://localhost:5000/victimRank/" + this.state.selectedSexGroup + '/' + this.state.selectedAgeGroup ,
    {
      method: "GET", 
    })
    .then(res => res.json()) 
    .then(neighborhoodList => {

      const neighborhoodDiv = neighborhoodList.map((neighborhoodItem, i) =>
        <AllNeighborhoodRow key={i} name={neighborhoodItem.name} stat={neighborhoodItem.stat}/>);

      console.log(neighborhoodDiv);
      this.setState({
        neighborhoodRanked: neighborhoodDiv,
      });
    });
  }

  submitAccomodationSetting(){
    console.log( "Hey man. I'm here!"
    );
    fetch("http://localhost:5000/ratingRank/" + this.state.minNight+ '/' + this.state.maxNight + '/' + this.state.bedRoom
    + '/' + this.state.bathRoom + '/' + this.state.kitchen + '/' + this.state.cable_tv + '/' + this.state.internet + '/' 
    + this.state.wifi + '/' + this.state.heating + '/' + this.state.air_conditioning + '/' + this.state.free_parking
    + '/' + this.state.maxPrice + '/' + this.state.minPrice, 
    {
      method: "GET", 
    })
    .then(res => res.json()) 
    .then(neighborhoodList => {

      const neighborhoodDiv = neighborhoodList.map((neighborhoodItem, i) =>
        <AllNeighborhoodRow key={i} name={neighborhoodItem.name} stat={neighborhoodItem.stat}/>);

      console.log(neighborhoodDiv);
      this.setState({
        neighborhoodRanked: neighborhoodDiv,
      });
    });
  }

    
    
  render() {
    return (
      <div className="All-neighborhood">
        <PageNavbar active="Neighborhoods" />
        <header className="App-header">
        </header>
        <div className="stats1">
          <h3 className="stats1-title">Comparing Five Neighborhood in New York</h3>
        </div>

        <div className="jumbotron">
            <div className="listing">
                <div className="h5"><strong>Crime Filter</strong></div>
                <div className="headerVictim"><strong>Victim Filter</strong></div>
            </div>  
            <div className="headerCrime"> <strong> Crime type </strong> </div>
            <div className="dropdown-container">
                <select value={this.state.selectedCrime} onChange={this.handleCrimeChange} className="dropdown" id="crimesDropdown">
                    {this.state.crimes}
                </select>
            </div>
            <div className="headerCrime">Set Starting Year.</div>
            <div className="input-container">
                <input type='text' placeholder="2010" value={this.state.startingYear} onChange={this.handleYearChange} id="yearSet" className="year-input"/>
            </div>
            <button className="submit-btn" id="submitBtn" onClick={this.submitSettings}>Submit Crime Setting</button>

            <div className="headerVictimTwo">Set Victim Age.</div>
            <div className="dropdown-container-vic">
                <select value={this.state.selectedAgeGroup} onChange={this.handleAgeChange} className="dropdown" id="ageDropdown">
                    {this.state.ageGroup}
                </select>
            </div>
            <div className="headerVictimTwo">Set Victim Sex.</div>
            <div className="dropdown-container-vic">
                <select value={this.state.selectedSexGroup} onChange={this.handleSexChange} className="dropdown" id="sexDropdown">
                    {this.state.sexGroup}
                </select>
            </div>
            <button className="victim-submission-btn" id="submitBtnTwo" onClick={this.submitVictimSettings}>Submit Victim Setting</button>
            
        </div>

        <div className="jumbotron">
          <div className = "h5" > Accomodation Filter </div>
          <div className="accomodationTextOne">Max Night</div>
            <div className="input-container">
                <input type='text' placeholder="100" value={this.state.maxNight} onChange={this.handleMaxNightChange} id ="MaxSet" 
                className="max-input"/>
            </div>

          <div className="h5">Min Night</div>
            <div className="input-container">
                <input type='text' placeholder="0" value={this.state.minNight} onChange={this.handleMinNightChange} id="MinSet" 
                className="min-input"/>
            </div>

            <div className="accomodationTextThree">Max Price</div>
            <div className="input-container-three">
                <input type='text' placeholder="100" value={this.state.maxPrice} onChange={this.handleMaxPriceChange} id="MaxSet" 
                className="max-input"/>
            </div>
          <div className="h5-4">Min Price</div>
            <div className="input-container-4">
                <input type='text' placeholder="0" value={this.state.minPrice} onChange={this.handleMinPriceChange} id="MinSet" 
                className="min-input"/>
            </div>

            <div className="h5-5">Min Bathroom Count</div>
            <div className="input-container-5">
                <input type='text' placeholder="1" value={this.state.bathRoom} onChange={this.handleBathRoomChange} id="MinSet" 
                className="min-input"/>
            </div>
            
            <div className="h5-5">Min Bedroom Count</div>
            <div className="input-container-5">
                <input type='text' placeholder="1" value={this.state.bedRoom} onChange={this.handleBedRoomChange} id="MinSet" 
                className="min-input"/>
            </div>



            <div className="h5-6">Kitchen Availability </div>
            <div className="dropdown-container-6">
                <select value={this.state.kitchen} onChange={this.handleKitchenChange} className="dropdown" id="kitchenDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                  
                </select>
            </div>

            <div className="h5-7">Cable TV Availability </div>
            <div className="dropdown-container-7">
                <select value={this.state.cable_tv} onChange={this.handleCableTVChange} className="dropdown" id="cableTVDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                  
                </select>
            </div>

            <div className="h5-8">Internet Availability </div>
            <div className="dropdown-container-8">
                <select value={this.state.internet} onChange={this.handleInternetChange} className="dropdown" id="internetDropdown">
                   <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                </select>
            </div>

            <div className="h5-9">WIFI Availability </div>
            <div className="dropdown-container-9">
                <select value={this.state.wifi} onChange={this.handleWIFIChange} className="dropdown" id="wifiDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                  
                </select>
            </div>

            <div className="h5-10">Heating Availability </div>
            <div className="dropdown-container-10">
                <select value={this.state.heating} onChange={this.handleHeatingChange} className="dropdown" id="heatingDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                </select>
            </div>

            <div className="h5-11">AC Availability </div>
            <div className="dropdown-container-11">
                <select value={this.state.air_conditioning} onChange={this.handleACChange} className="dropdown" id="acDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>

                </select>
            </div>

            <div className="h5-12">Free Parking Availability </div>
            <div className="dropdown-container-12">
                <select value={this.state.free_parking} onChange={this.handleFreeParkingChange} className="dropdown" id="freeParkingDropdown">
                  <option value="FLAG">Don't Care</option>
                  <option value="TRUE">TRUE</option>
                </select>
            </div>

            <button className="submit-btn-accommo" id="submitBtnThree" onClick={this.submitAccomodationSetting}>Submit Accomodation Setting</button>
        </div>

        <div className="jumbotron">
					<div className="listings-container">
						<div className="listing">
			        <div className="header"><strong>Borough</strong></div>
			        <div className="header"><strong>Stats</strong></div>
			      </div>
			      <div className="listings-container" id="results">
              {this.state.neighborhoodRanked}
			      </div>
          </div>
        </div>
      </div>
    );
  }
}
