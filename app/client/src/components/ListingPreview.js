import React from 'react';

export default class ListingPreview extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      id: this.props.id,
      name: this.props.name,
      price: this.props.price,
      neighborhood: this.props.neighborhood,
      accommodates: this.props.accommodates,
      rating: this.props.rating,
      link: `/listings/${this.props.id}/`
    };
  }

  render() {
    const href_link = `/listings/{this.state.id}`;
    return (
      <div className="card">
        <div className="card-body">
          <h5 className="card-title">{this.state.name}</h5>
          <h6 className="card-subtitle mb-2 text-muted">{this.state.price}/night</h6>
          <a href={this.state.link} className="card-link">View</a>
        </div>
      </div>
    );
  }
};
