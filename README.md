# Group 41: CIS-550 Final Project

## Group Members

- [Tommy Drenis](https://github.com/tdrenis)
- [Zach Duey](https://github.com/zduey)
- [Sungbum Hong](https://github.com/peter0135)
- [Kun Hwi Ko](https://github.com/kunhwiko)


## Data Parsing: Listings

Before following this step, make sure to create tables following the schema in the final report in your MySQL database. In our case, we named the database 'crimebnb', and highly recommend users to do the same. 

This step is a guide to parse the Airbnb listing data. First, locate the `data-cleansing/airbnb/parse` directory.

Open 'listing_parse.ipynb' on Jupyter Notebook or Google Colabs, you will need a Kaggle API token to execute the python code. Instructions for how to do this are provided in the comments. 

After executing the python code, the parsed csv files will be mounted to your google drive. Download the files to the `data-cleansing/airbnb/parse` directory. 

Execute 'csv_to_sql.py' either on your terminal or through an IDE. This will generate txt files with INSERT INTO scripts. Execute the insert into statements in the following order: host --> listing --> amenities --> ratings --> neighborhoods. 

Your database should now be properly populated. 


## Data Parsing: Crimes 

Now locate the pipeline directory. Here we have create table statements in 'crime.sql' to generate tables in the crimebnb database. 

Due to an immensive amount of crimes data, CSV imports or INSERT INTO scripts are simply too slow to populate the table. We have therefore pipelined the entire process of data cleansing and inserting data into the database as seen in 'run.py'. 

Notice that for this process, we require the user to have the crimes data locally. We have deleted the csv file in this zip folder due to the file size. Please download the file [here](https://www.kaggle.com/mrmorj/new-york-city-police-crime-data-historic/code) into `data-cleansing/compliants/raw` with the name as `NYPD_Compliant_Data_Historic.csv`.  

To properly execute run.py on the terminal, make sure you have the username, password, host, and database name information of your MySQL database. Here we use os.environ to get the information as environment variables. You could alternatively fill this in manually. 


## Program Execution 

We have most of the useful commands in our Makefile. The first time you run the app, or after dependencies have changed, run: 

```bash
make build
```

Next, you will need to configure the necessary database credentials by filling out the required configuration in `app/server/db-config.js`. For the purpose of grading, we have provided the credentials that we have used. 

While the typical `npm start` in `app/client` and `app/server` as in homework 2 works, `make client` and `make server` accomplishes the same but with the use of docker containers to ensure reproducibility across environments.

```bash
make client
```

```bash
make server
```

Each command will spin up a separate docker container. The client is available
on port `3000` and the server is available on `5000`.


