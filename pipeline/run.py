import logging
import multiprocessing
from pathman import Path

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(
    raw_complaint_loc: Path,
    processed_complaint_loc: Path,
    engine: Engine,
    from_scratch: bool = False,
):
    logger.info("Starting NY complaint data processing...")
    if from_scratch:
        df = convert_to_parquet(raw_complaint_loc, processed_complaint_loc)
    else:
        df = pd.read_parquet(processed_complaint_loc)

    write_to_database(df, engine, n_records=500000)


def convert_to_parquet(source: Path, target: Path) -> pd.DataFrame:
    logger.info("Setting up dask client")
    n_workers = multiprocessing.cpu_count() - 1
    client = Client(n_workers=n_workers)

    custom_dtypes = {"HOUSING_PSA": "object"}
    date_vars = ["RPT_DT"]

    logger.info("Reading source CSV")
    raw_df = dd.read_csv(
        source,
        assume_missing=True,
        low_memory=False,
        dtype=custom_dtypes,
        parse_dates=date_vars,
    )

    temp = target.dirname() / ".tmp"
    if not temp.exists():
        temp.mkdir()
    logger.info(f"Writing 5 temporary partitions to {temp}")
    df = raw_df.repartition(npartitions=5)
    df.to_parquet(temp, write_index=False, overwrite=True, write_metadata_file=False)

    logger.info("Loading partitioned files back into memory as single dataframe")
    partitions = temp.glob("*.parquet")
    data = []
    for source in partitions:
        df = pd.read_parquet(source)
        data.append(df)

    temp.rmdir(recursive=True)

    logger.info(f"Writing combined data to {target}")
    combined_df = pd.concat(data)
    combined_df.to_parquet(target, index=False)

    return combined_df


def write_to_database(df: pd.DataFrame, engine: Engine, n_records=None):
    if n_records is None:
        n_records = len(df)

    # create a random sample of the data to work with
    logger.info("Sampling the data")
    df = df.sample(n=n_records, random_state=123)

    logger.info("Generating agency table")
    agency_df = pd.DataFrame()
    agency_df["name"] = df["JURIS_DESC"]
    agency_df["code"] = df["JURISDICTION_CODE"].astype("Int64")
    agency_df = agency_df.drop_duplicates(subset=["name"])

    logger.info("Writing agency records to database")
    agency_df.to_sql("agency", con=engine, if_exists="append", index=False)

    logger.info("Generating borough table")
    borough_df = pd.DataFrame()
    borough_df["name"] = df["BORO_NM"]
    borough_df = borough_df.drop_duplicates()
    borough_df = borough_df.dropna()

    logger.info("Writing borough records to database")
    borough_df.to_sql("borough", con=engine, if_exists="append", index=False)

    logger.info("Generating crime table")
    crime_df = pd.DataFrame()
    crime_df["code"] = df["PD_CD"].astype("Int64")
    crime_df["description"] = df["PD_DESC"]
    # there are two entries with code 234, but different descriptions:
    # BURGLARY,UNKNOWN TIME and BURGLARY, TRUCK UNKNOWN TIME. We keep the first
    # option, which is more general than the second
    crime_df = crime_df.drop_duplicates(subset=["code"])
    # about 5k records having missing codes and descriptions. there is never a
    # case where one is filled in, but the other is not, so we drop missing records
    crime_df = crime_df.dropna(subset=["code"])

    logger.info("Writing crime records to database")
    crime_df.to_sql("crime", con=engine, if_exists="append", index=False)

    # update the original PD_DESC records with these new mappings
    crime_code_to_desc_map = dict(zip(crime_df["code"], crime_df["description"]))
    df["PD_DESC"] = df["PD_DESC"].map(crime_code_to_desc_map)

    logger.info("Generating development table")
    development_df = pd.DataFrame()
    df["HOUSING_PSA"] = df["HOUSING_PSA"].apply(_str_to_int).astype("Int64")
    development_df["name"] = df["HADEVELOPT"]
    development_df["code"] = df["HOUSING_PSA"]
    # there is a m:m relationship between name and code and 279 records without
    # a code. we opt to drop these rather than filling based on other records and
    development_df = development_df.drop_duplicates()
    development_df = development_df.dropna(subset=["code", "name"])

    logger.info("Writing housing development records to database")
    development_df.to_sql("development", con=engine, if_exists="append", index=False)

    logger.info("Generating offense table")
    offense_df = pd.DataFrame()
    offense_df["code"] = df["KY_CD"].astype("Int64")
    offense_df["description"] = df["OFNS_DESC"]
    # there are some cases where a given code maps to different descriptions, but
    # in almost all cases these appear to be the same offense, so just keep one
    # description per code
    offense_df = offense_df.drop_duplicates(subset="code")

    logger.info("Writing offense records to database")
    offense_df.to_sql("offense", con=engine, if_exists="append", index=False)

    logger.info("Generating park table")
    park_df = pd.DataFrame()
    park_df["name"] = df["PARKS_NM"]
    park_df = park_df.drop_duplicates()
    park_df = park_df.dropna()

    logger.info("Writing park records to database")
    park_df.to_sql("park", con=engine, if_exists="append", index=False)

    logger.info("Generating premises table")
    premises_df = pd.DataFrame()
    premises_df["location"] = df["LOC_OF_OCCUR_DESC"]
    premises_df = premises_df.drop_duplicates()
    premises_df = premises_df.dropna()

    logger.info("Writing premises records to database")
    premises_df.to_sql("premises", con=engine, if_exists="append", index=False)

    logger.info("Generating coordinate table")
    coordinate_df = pd.DataFrame()
    coordinate_df["latitude"] = df["Latitude"]
    coordinate_df["longitude"] = df["Longitude"]
    coordinate_df = coordinate_df.dropna()
    # keep only 4 decimal places of precision
    coordinate_df["latitude"] = coordinate_df["latitude"].apply(lambda x: round(x, 8))
    coordinate_df["longitude"] = coordinate_df["longitude"].apply(lambda x: round(x, 8))
    coordinate_df = coordinate_df.drop_duplicates()

    logger.info("Writing coordinate table to database")
    coordinate_df.to_sql("coordinate", con=engine, if_exists="append", index=False)

    logger.info("Generating incident table")
    incident_df = _create_incident_table(df)
    logger.info("Writing incident records to database")
    incident_df.to_sql(
        "incident",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=100000,
        method="multi",
    )

    logger.info("Generating suspect table")
    age_groups = ["<18", "18-24", "25-44", "45-64", "65+", "UNKNOWN"]
    sex_categories = ["M", "F"]
    suspect_df = pd.DataFrame()
    suspect_df["incident_number"] = df["CMPLNT_NUM"].astype("Int64")
    suspect_df["age"] = df["SUSP_AGE_GROUP"]
    suspect_df["sex"] = df["SUSP_SEX"]
    suspect_df["race"] = df["SUSP_RACE"]
    # this table represents a weak entity set, but we don't want to be storing
    # tuples that have only null values
    suspect_df = suspect_df.dropna(how="all")
    # there are a bunch of erroneous values in this column. replace with missing
    # if they aren't a known age group
    suspect_df.loc[suspect_df["age"].isin(age_groups) == False, "age"] = None
    suspect_df.loc[suspect_df["sex"].isin(sex_categories) == False, "sex"] = None

    logger.info("Writing suspect records to database")
    suspect_df.to_sql(
        "suspect",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=100000,
        method="multi",
    )

    logger.info("Generating victim table")
    victim_df = pd.DataFrame()
    victim_df["incident_number"] = df["CMPLNT_NUM"].astype("Int64")
    victim_df["age"] = df["VIC_AGE_GROUP"]
    victim_df["sex"] = df["VIC_SEX"]
    victim_df["race"] = df["VIC_RACE"]
    # this table represents a weak entity set, but we don't want to be storing
    # tuples that have only null values
    victim_df = victim_df.dropna(how="all")
    victim_df.loc[victim_df["age"].isin(age_groups) == False, "age"] = None
    # there are more categories for sex of victim's than there are for suspects
    # these should be comparable. since we do not have information about what
    # sex=E and sex=D, and sex=U means, we replace them with missing
    victim_df.loc[victim_df["sex"].isin(sex_categories) == False, "sex"] = None
    logger.info("Writing victim records to database")
    victim_df.to_sql(
        "victim",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=100000,
        method="multi",
    )


def _create_incident_table(df: pd.DataFrame) -> pd.DataFrame:
    incident_df = pd.DataFrame()

    # set up primary key
    incident_df["number"] = df["CMPLNT_NUM"].astype("Int64")

    # ensure dates are correctly formatted. any invalid dates will be converted
    # to missing values
    incident_df["start_date"] = pd.to_datetime(
        df["CMPLNT_FR_DT"], format="%m/%d/%Y", errors="coerce"
    )
    incident_df["end_date"] = pd.to_datetime(
        df["CMPLNT_TO_DT"], format="%m/%d/%Y", errors="coerce"
    )
    incident_df["report_date"] = pd.to_datetime(df["RPT_DT"], format="%m/%d/%Y")

    # ensure times are correctly formatted. any invalid times will be converted
    # to missing values
    incident_df["start_time"] = pd.to_datetime(
        df["CMPLNT_FR_TM"], format="%H:%M:%S", errors="coerce"
    ).dt.time
    incident_df["end_time"] = pd.to_datetime(
        df["CMPLNT_TO_TM"], format="%H:%M:%S", errors="coerce"
    ).dt.time

    # explicitly convert integers to avoids pandas' representation of int fields
    # that have some missing values as flaots
    incident_df["precinct"] = df["ADDR_PCT_CD"].astype("Int64")
    incident_df["transit_district"] = df["TRANSIT_DISTRICT"].astype("Int64")
    incident_df["crime"] = df["PD_CD"].astype("Int64")

    # simple renamings
    incident_df["status"] = df["CRM_ATPT_CPTD_CD"]
    incident_df["classification"] = df["LAW_CAT_CD"]
    incident_df["premises_type"] = df["PREM_TYP_DESC"]
    incident_df["patrol_borough"] = df["PATROL_BORO"]
    incident_df["station_name"] = df["STATION_NAME"]
    incident_df["park"] = df["PARKS_NM"]
    incident_df["development_name"] = df["HADEVELOPT"]
    incident_df["development_code"] = df["HOUSING_PSA"]
    incident_df["agency"] = df["JURIS_DESC"]

    # duplicate lat/lon preprocessing
    incident_df["latitude"] = df["Latitude"]
    incident_df["latitude"] = incident_df["latitude"].apply(lambda x: round(x, 8))
    incident_df["longitude"] = df["Longitude"]
    incident_df["longitude"] = incident_df["longitude"].apply(lambda x: round(x, 8))

    # there are duplicate entries for a given (number, report_date). find these
    # duplicate entries and collapse them where possible
    logger.info("Resolving duplicate incidents")
    duped_reports = (
        incident_df.groupby(by=["number", "report_date"])
        .agg({"status": "count"})
        .reset_index()
        .rename(columns={"status": "n"})
    )
    incident_df = incident_df.merge(duped_reports, how="left")

    # separate unique incidents from duplicated ones. handle duped ones separately
    duped_incidents = incident_df.loc[incident_df["n"] > 1]
    del duped_incidents["n"]

    # short-circquit if there are no duplicates to deal with
    if len(duped_incidents) == 0:
        del incident_df["n"]
        return incident_df

    unique_incidents = incident_df.loc[incident_df["n"] < 2]
    del unique_incidents["n"]

    del incident_df["n"]

    # for duped records, keep the first non-null value. most of the time, there
    # is just one such value so we aren't combining information incorrectly
    agg_cols = list(set(incident_df.columns).difference(set(["number", "report_date"])))
    agg_funcs = {k: "unique" for k in agg_cols}
    agg_duped_incidents = (
        duped_incidents.groupby(by=["number", "report_date"])
        .agg(agg_funcs)
        .reset_index()
    )
    collapsed_incidents = agg_duped_incidents[["number", "report_date"]]
    for col in agg_cols:
        collapsed_incidents[col] = agg_duped_incidents[col].apply(collapse)

    # re-combine the unique incidents with the de-duplicated ones
    incident_df = pd.concat([unique_incidents, collapsed_incidents])

    return incident_df


def collapse(entry):
    if len(entry) == 1:
        return entry[0]
    non_null_vals = list(filter(pd.isnull, entry))
    if len(non_null_vals) == 0:
        return None
    return non_null_vals[0]


def _str_to_int(val):
    if val is None:
        return val
    return int(val.replace(",", ""))


if __name__ == "__main__":
    import os
    from sqlalchemy import create_engine

    raw_complaint_data = Path(
        "./data/complaints/raw/NYPD_Complaint_Data_Historic.csv"
    ).abspath()
    processed_complaint_data = Path(
        "./data/complaints/processed/complaints.parquet"
    ).abspath()

    username = os.environ.get("DB_USERNAME")
    password = os.environ.get("DB_PASSWORD")
    hostname = os.environ.get("DB_HOST")
    name = os.environ.get("DB_NAME")
    db_engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{hostname}/{name}?charset=utf8mb4"
    )
    run(raw_complaint_data, processed_complaint_data, db_engine, from_scratch=False)
