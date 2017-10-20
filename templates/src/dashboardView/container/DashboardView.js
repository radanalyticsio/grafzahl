import React, { Component } from "react";
import  c3 from 'c3'

// Chart Component
let columns = [
    ['My Numbers', 30, 200, 100, 400, 150, 250],
    ['Your Numbers', 50, 20, 10, 40, 15, 25]
  ];
  
class DashboardView extends Component {
    constructor(props) {
        super(props);
        this._setBarChart = this._setBarChart.bind(this);
        this._setLineChart = this._setLineChart.bind(this);
        this.state = {
          chartType: 'line'
        };
      }
      _setBarChart() {
        this.setState({ chartType: 'bar' });
      }
      _setLineChart() {
        this.setState({ chartType: 'line' });
      }
        render() {
            return (
          <div className="app-wrap">
            <Chart 
              columns={columns}
              chartType={this.state.chartType} />
            <p>
              Chart Type
              <button onClick={this._setBarChart}>bar</button> 
              <button onClick={this._setLineChart}>Line</button>
            </p>
          </div>
        );
      }
    
}


class Chart extends Component {
    componentDidMount() {
      this._updateChart();
    }
    componentDidUpdate() {
      this._updateChart();
    }
    _updateChart() {

        const chart = c3.generate({
            bindto: '#chart',
            data: {
                columns: columns
            }
        });

    }
    render() {
      return <div id="chart">hi</div>;    
    }
  }


export default DashboardView;

 