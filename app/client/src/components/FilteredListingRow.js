import React from 'react';

export default class FilteredListingRow extends React.Component {
	render() {
		return (
			<div className="listingResults" onClick={event =>  window.location.href=`/listings/${this.props.listing_id}`}>
				<div className="stats-top">
				{this.props.room_type == 'ENTIRE HOME/APT' &&
				<span role="img" aria-label="home">🏠&nbsp;</span> 
				}
				{this.props.room_type == 'PRIVATE ROOM' &&
				<span role="img" aria-label="room">🛏️&nbsp;</span> 
				}
				{this.props.room_type == 'SHARED ROOM' &&
				<span role="img" aria-label="room">🛏️&nbsp;</span> 
				}
				{this.props.room_type == 'HOTEL ROOM' &&
				<span role="img" aria-label="hotel">🏨&nbsp;</span> 
				}

				<strong>${this.props.price}</strong> / night
				</div>
				<div className="name">
					{this.props.name}
				</div>
				<div className="stats-bottom">
					<div>{this.props.accommodates} guests | {this.props.beds} bed | {this.props.baths} bath</div>
					Cleanliness {this.props.cleanscore} <i className="rating bi-star-fill"></i> &nbsp;
					Location {this.props.locscore} <i className="rating bi-star-fill"></i>
					<div># Felonies Nearby Since 2010: {this.props.crimes}</div>
				</div>
			</div>
		);
	};
};