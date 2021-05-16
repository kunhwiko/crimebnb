import React from 'react';
import '../style/pagenavbar.css';
import 'bootstrap/dist/css/bootstrap.min.css';

export default class PageNavbar extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
        navDivs: []
    }
  }

  componentDidMount() {
    const pageList = ['home', 'listings', 'neighborhoods'];

    let navbarDivs = pageList.map((page, i) => {
      if (this.props.active === page) {
        if (page != 'listings') {
          return <a className="nav-item nav-link active" key={i} href={"/" + page}>{page.charAt(0).toUpperCase() + page.substring(1, page.length)}</a>
        } else { 
          return <a className="nav-item nav-link active" key={i} href={"/filteredListings"}>{page.charAt(0).toUpperCase() + page.substring(1, page.length)}</a>
        }
      }
      else {
        if (page != 'listings') {
          return <a className="nav-item nav-link" key={i} href={"/" + page}>{page.charAt(0).toUpperCase() + page.substring(1, page.length)}</a>
        } else { 
          return <a className="nav-item nav-link" key={i} href={"/filteredListings"}>{page.charAt(0).toUpperCase() + page.substring(1, page.length)}</a>
        }
      }
    })

    this.setState({
      navDivs: navbarDivs
    });
  }

  render() {
    return (
      <div className="PageNavbar">
        <nav className="navbar navbar-expand-lg navbar-light bg-light">
          <span className="navbar-brand center"><a className="brand-text" href="/home">crimebnb</a></span>
          <div className="collapse navbar-collapse" id="navbarNavAltMarkup">
            <div className="navbar-nav">
                {this.state.navDivs}
            </div>
          </div>
        </nav>
      </div>
    );
  }
}
