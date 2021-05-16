import React from 'react';
import { Pie } from 'react-chartjs-2';

export default class SingleNeighborhoodCrimeGraph extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    const data = {
      labels: this.props.name,
      datasets: [
        {
          label: this.props.tick,
          data: this.props.score,
          backgroundColor: [
            'rgba(255, 99, 132, 0.2)',
            'rgba(54, 162, 235, 0.2)',
            'rgba(255, 159, 64, 0.2)',
            'rgba(75, 192, 192, 0.2)',
            'rgba(153, 102, 255, 0.2)',
          ],
          borderColor: [
            'rgba(255, 99, 132, 1)',
            'rgba(54, 162, 235, 1)',
            'rgba(255, 159, 64, 1)',
            'rgba(75, 192, 192, 1)',
            'rgba(153, 102, 255, 1)',
          ],
          borderWidth: 1,
        },
      ],
    };


    return (
      <div className>
        <Pie data={data}/>
      </div>
    );
  }
};
