import React from 'react';
import { Slide } from "react-slideshow-image";
import 'react-slideshow-image/dist/styles.css';
import '../style/homepage.css';

export default class Home extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div id="about" className="container">
        <div className="left-split-container">
          <h1>Crimebnb</h1>
          <nav>
            <ul>
              <li><a className="title-text" href="/home">HOME</a></li>
              <li><h2 className="title-text">/</h2></li>
              <li><a className="title-text" href="/filteredListings">LISTINGS</a></li>
              <li><h2 className="title-text">/</h2></li>
              <li><a className="title-text" href="/neighborhoods">NEIGHBORHOODS</a></li>
            </ul>
          </nav>
        </div>
        <div className="right-split-container">
          <div className="slide-container">
            <Slide>
              <div className="each-slide">
                <div style={{'backgroundImage': 'url(images/lease1.jpg)'}}>
                <h1 className="slide-text1">Travel Safe</h1>
                </div>
              </div>
              <div className="each-slide">
                <div style={{'backgroundImage': 'url(images/lease2.jpg)'}}>
                <h1 className="slide-text2">Travel More</h1>
                </div>
              </div>
              <div className="each-slide">
                <div style={{'backgroundImage': 'url(images/lease3.jpg)'}}>
                <h1 className="slide-text3">Travel Further</h1>
                </div>
              </div>
            </Slide>
          </div>
        </div>
      </div>
    );
  }
}