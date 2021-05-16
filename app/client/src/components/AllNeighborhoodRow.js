import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

export default class AllNeighborhoodRow extends React.Component {
	render() {
		return (
			<div className="listingResults">
				<div className="name">
					{this.props.name}
				</div>
				<div className="stat">
					{this.props.stat}
				</div>
			</div>
		);
	};
};