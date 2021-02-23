"""
data enrichment
    - use endLat and endLon to enrich data when doLocationId is null
    - use doLocationId to enrich data when endLat and endLon are null

basic app flow:
    download data from azure-opendatasets sdk and store in pandas dataframe 1 month at a time
    enrich data based on above in the dataframe by joining on doLocationId or spacial joining on created geometry column
    once enriched, store data in sqlite database
    once a full year is stored in the database, pull aggregated data (median, average) by year, and location and add to csv file
    truncate table and repeat for all years
"""

from datetime import datetime, timedelta
from dateutil import parser

import pandas
import geopandas as gp
from azureml.opendatasets import NycTlcYellow

from utils import startup_db, startup, append_to_csv
from config import table

startup()
db = startup_db()

# file for geo spatial data - url link expires, so data is saved to source
nyc_df = gp.read_file('nyc.geojson.json')
nyc_df = nyc_df[['neighborhood', 'borough', 'geometry']]

start = datetime.now()

boroughs = ['Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island']

# url for doLocationId
taxi_location_url = r"https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
loc_id_df = pandas.read_csv(taxi_location_url, index_col='LocationID')

date_ranges = [
    ('2019-01-01', '2019-01-31'),
    ('2019-02-01', '2019-02-28'),
    ('2019-03-01', '2019-03-31'),
    ('2019-04-01', '2019-04-30'),
    ('2019-05-01', '2019-05-31'),
    ('2019-06-01', '2019-06-30'),
    ('2019-07-01', '2019-07-30'),
    ('2019-08-01', '2019-08-31'),
    ('2019-09-01', '2019-09-30'),
    ('2019-10-01', '2019-10-31'),
    ('2019-11-01', '2019-11-30'),
    ('2019-12-01', '2019-12-31'),
    ('2018-01-01', '2018-01-31'),
    ('2018-02-01', '2018-02-28'),
    ('2018-03-01', '2018-03-31'),
    ('2018-04-01', '2018-04-30'),
    ('2018-05-01', '2018-05-31'),
    ('2018-06-01', '2018-06-30'),
    ('2018-07-01', '2018-07-30'),
    ('2018-08-01', '2018-08-31'),
    ('2018-09-01', '2018-09-30'),
    ('2018-10-01', '2018-10-31'),
    ('2018-11-01', '2018-11-30'),
    ('2018-12-01', '2018-12-31'),
    ('2017-01-01', '2017-01-31'),
    ('2017-02-01', '2017-02-28'),
    ('2017-03-01', '2017-03-31'),
    ('2017-04-01', '2017-04-30'),
    ('2017-05-01', '2017-05-31'),
    ('2017-06-01', '2017-06-30'),
    ('2017-07-01', '2017-07-30'),
    ('2017-08-01', '2017-08-31'),
    ('2017-09-01', '2017-09-30'),
    ('2017-10-01', '2017-10-31'),
    ('2017-11-01', '2017-11-30'),
    ('2017-12-01', '2017-12-31'),
    ('2016-01-01', '2016-01-31'),
    ('2016-02-01', '2016-02-29'),
    ('2016-03-01', '2016-03-31'),
    ('2016-04-01', '2016-04-30'),
    ('2016-05-01', '2016-05-31'),
    ('2016-06-01', '2016-06-30'),
    ('2016-07-01', '2016-07-30'),
    ('2016-08-01', '2016-08-31'),
    ('2016-09-01', '2016-09-30'),
    ('2016-10-01', '2016-10-31'),
    ('2016-11-01', '2016-11-30'),
    ('2016-12-01', '2016-12-31'),
    ('2015-01-01', '2015-01-31'),
    ('2015-02-01', '2015-02-28'),
    ('2015-03-01', '2015-03-31'),
    ('2015-04-01', '2015-04-30'),
    ('2015-05-01', '2015-05-31'),
    ('2015-06-01', '2015-06-30'),
    ('2015-07-01', '2015-07-30'),
    ('2015-08-01', '2015-08-31'),
    ('2015-09-01', '2015-09-30'),
    ('2015-10-01', '2015-10-31'),
    ('2015-11-01', '2015-11-30'),
    ('2015-12-01', '2015-12-31'),
]


def get_date_dict(_date_ranges):
    ret = {}
    for _start_date, _end_date in _date_ranges:
        _start_date = parser.parse(_start_date)
        _end_date = parser.parse(_end_date)
        key = _start_date.year
        if key not in ret.keys():
            ret[key] = []
        ret[key].append(set_dates(_start_date, _end_date), )

    return ret


def set_dates(_start_date, _end_date):
    return _start_date, _end_date + timedelta(hours=23) + timedelta(minutes=59) + timedelta(seconds=59)


def store_all_data(start_date, end_date):
    data_cols = ['doLocationId', 'tipAmount', 'tpepDropoffDateTime', 'endLat', 'endLon']
    data = NycTlcYellow(start_date=start_date, end_date=end_date, cols=data_cols, enable_telemetry=False)
    df = data.to_pandas_dataframe()

    df['Year'] = df['tpepDropoffDateTime'].dt.year

    filtered_geo_df = df[df['endLon'].notnull()]

    if len(filtered_geo_df):
        store_geo_df(filtered_geo_df)

    filtered_lid_df = df[df['doLocationId'].notnull()]

    if len(filtered_lid_df):
        store_lid_df(filtered_lid_df)


def store_geo_df(filtered_geo_df):
    """
    :param filtered_geo_df: dataframe filtered to valid lat/lon values
    :return:
    """
    # convert to geo dataframe
    gdf = gp.GeoDataFrame(filtered_geo_df, geometry=gp.points_from_xy(filtered_geo_df.endLon, filtered_geo_df.endLat),
                          crs='epsg:4326')

    # drop unnecessary columns
    gdf = gdf[['geometry', 'Year', 'tipAmount']]

    print('performing spatial join')
    # perform the data enrichment via spatial join
    full_df = gp.sjoin(gdf, nyc_df, op='within')

    # remove unnecessary columns
    full_df = full_df[['tipAmount', 'Year', 'neighborhood', 'borough']]
    full_df.reset_index(drop=True, inplace=True)
    full_df = full_df[full_df['borough'].isin(boroughs)]

    # store data in sqlite db
    full_df.to_sql(table, db, if_exists='append', index=False)

    return


def store_lid_df(filtered_lid_df):
    """
    :param filtered_lid_df: dataframe filtered to valid doLocationIds
    :return:
    """
    df = filtered_lid_df.astype({'doLocationId': 'int'}, copy=False)

    df = df.join(loc_id_df, on='doLocationId')

    df = df[['tipAmount', 'Year', 'Zone', 'Borough']]

    df.rename(columns={
        'Zone': 'neighborhood',
        'Borough': 'borough'
    }, inplace=True)

    df = df[df['borough'].isin(boroughs)]

    df.to_sql(table, db, if_exists='append', index=False)


def main():
    date_dict = get_date_dict(date_ranges)
    for year, _date_ranges in date_dict.items():
        for _start_date, _end_date in _date_ranges:
            store_all_data(start_date=_start_date, end_date=_end_date)

        print(f'appending to csv for {year}')
        append_to_csv(db)

    print(f'total time taken {datetime.now() - start}')


if __name__ == '__main__':
    main()
