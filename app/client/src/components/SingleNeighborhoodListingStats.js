import React from 'react';
import '../style/singleNeighborhoodListingStats.css';

export default class SingleNeighborhoodListingStats extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className="row">
        {
        <div className="col-xs-6 col-sm-4">
          <div className="box">
            <div className="num">{this.props.count}</div>
            <div className="name">number of listings</div>
          </div>
        </div>
        }
        {
        <div className="col-xs-6 col-sm-4">
          <div className="box">
            <div className="num">${this.props.mean}</div>
            <div className="name">mean price</div>
          </div>
        </div>
        }
        {
        <div className="col-xs-6 col-sm-4">
          <div className="box">
            <div className="num">${this.props.median}</div>
            <div className="name">median price</div>
          </div>
        </div>
        }
      </div>
    );
  }
};

