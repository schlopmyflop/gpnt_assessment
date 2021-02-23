this app was written for linux-based OS specifically Ubuntu 20.04. below are instructions that should allow the app to run without issue. 

// install dotnet runtime

sudo apt-get update; \
  sudo apt-get install -y apt-transport-https && \
  sudo apt-get update && \
  sudo apt-get install -y dotnet-runtime-2.1

// get virtual environment package

sudo apt-get install python3-venv

// pull the repo

git clone https://github.com/schlopmyflop/gpnt_assessment.git [[directory name]]

// cd into the new directory

cd [[directory name]

// create the virtual environment

python3 -m venv venv

// activate the virtual environment

source venv/bin/activate

// install requirements

pip install -r requirements.txt

// run the app

python main.py

// output file will be in the [[directory name]] directory as "output.csv"

// app overview

data enrichment
    - use endLat and endLon to enrich data when doLocationId is null
    - use doLocationId to enrich data when endLat and endLon are null
basic app flow:
    download data from azure-opendatasets sdk and store in pandas dataframe 1 month at a time
    enrich data based on above in the dataframe by joining on doLocationId or spatial joining on created geometry column via geopandas
    once enriched, store data in sqlite database to aleviate memory usage tied with dataframes
    once a full year is stored in the database, pull aggregated data (median, average) by year, and location and add to csv file
    truncate table and repeat for all years
misc:
    the app takes a while to run (probably 3 hours depending on a variety of factors). this is mostly due to the spatial joining. there's a sqlite spatial addon that looks like it may be able to speed up the process, but i decided against using this
    you can comment out date ranges to limit the data that's being pulled just to speed up the app.
