import React from 'react';
import { Line } from 'react-chartjs-2';

export default class SingleNeighborhoodCrimeGraph extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    const data = {
      labels: [2015, 2016, 2017, 2018],
      datasets: [
        {
          label: this.props.name[0],
          data: this.props.crime1,
          fill: false,
          backgroundColor: 'rgba(255, 99, 132, 0.2)',
          borderColor: 'rgba(255, 99, 132, 1)',
        },
        {
          label: this.props.name[1],
          data: this.props.crime2,
          fill: false,
          backgroundColor: 'rgba(54, 162, 235, 0.2)',
          borderColor: 'rgba(54, 162, 235, 1)',
        },
        {
          label: this.props.name[2],
          data: this.props.crime3,
          fill: false,
          backgroundColor: 'rgba(255, 159, 64, 0.2)',
          borderColor: 'rgba(255, 159, 64, 1)',
        },
        {
          label: this.props.name[3],
          data: this.props.crime4,
          fill: false,
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          borderColor: 'rgba(75, 192, 192, 1)',
        },
        {
          label: this.props.name[4],
          data: this.props.crime5,
          fill: false,
          backgroundColor: 'rgba(153, 102, 255, 0.2)',
          borderColor: 'rgba(153, 102, 255, 1)',
        },
      ],
    };

    return (
      <div className>
        <Line data={data}/>
      </div>
    );
  }
};
