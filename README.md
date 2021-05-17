# Crimebnb

### Authors
-----
- [Kun Hwi Ko](https://github.com/kunhwiko)
- [Tommy Drenis](https://github.com/tdrenis)
- [Zach Duey](https://github.com/zduey)
- [Sungbum Hong](https://github.com/peter0135)

### Description 
-----
Services like Airbnb are becoming increasingly popular when finding short-term rentals. Although Airbnb provides users with advanced search functionality for finding places to stay, it lacks the ability to filter results based on safety of the neighborhood where the rental is located. 

Our application aims to fill this gap by integrating crime data with that of Airbnb listings in New York City. This allows us to view crime trends near individual listings and the entire neighborhood in general. This will enable users of Airbnb to better understand the surrounding area in which a listing is located and the safety of this location for their visit. 

### Demo
-----
[Watch the demo here](https://github.com/kunhwiko/crimebnb/demo.mp4)

### Setup
-----
We are running our MySQL database on an AWS RDS instance. For security reasons, we help you to test the application here in a local environment.   

##### Data Parsing: Listings

Create a database named 'crimebnb'. We recommend using this name due to configuration reasons. Access the `data-cleansing/airbnb/processed` directory and look for `listing.sql`. Use this to create tables in your local MySQL database. 

Open `listing_parse.ipynb` on Jupyter Notebook or Google Colabs, you will need a Kaggle API token to execute the python code. Instructions for how to do this are provided in the comments. 

After executing the python code, the parsed csv files will be mounted to your google drive. Download the files to the `data-cleansing/airbnb/processed` directory. 

Execute `csv_to_sql.py` either on your terminal or through an IDE. This will generate txt files with INSERT INTO scripts. Execute the insert into statements in the following order: host --> listing --> amenities --> ratings --> neighborhoods. 

Check to see if your database is properly populated. 


##### Data Parsing: Crimes 

Now locate the `data-cleansing/complaints/processed` directory. Use `crime.sql` to populate the MySQL database. 

Due to an immensive amount of crimes data, CSV imports or INSERT INTO scripts are simply too slow to populate the table. We have therefore pipelined the entire process of data cleansing and inserting data into the database as seen in `run.py`. 

Notice that for this process, we require the user to have the crimes data locally. We have deleted the csv file due to the file size. Please download the file [here](https://www.kaggle.com/mrmorj/new-york-city-police-crime-data-historic/code) into `data-cleansing/compliants/raw` with the name as `NYPD_Compliant_Data_Historic.csv`.  

To properly execute run.py on the terminal, make sure you have the username, password, host, and database name information of your MySQL database. Here we use os.environ to get the information as environment variables. You could alternatively fill this in manually. 


##### Program Execution 

We have most of the useful commands in our Makefile. The first time you run the app, or after dependencies have changed, run: 

```bash
make build
```

Next, you will need to configure the necessary database credentials by filling out the required configuration in `app/server/db-config.js`.

While the typical `npm start` in `app/client` and `app/server` works, `make client` and `make server` accomplishes the same but with the use of docker containers to ensure reproducibility across environments.

```bash
make client
```

```bash
make server
```

Each command will spin up a separate docker container. The client is available on port `3000` and the server is available on `5000`.


