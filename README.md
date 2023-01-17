# Formula-1-Data-Analysis

## Description
Formula 1 data analysis is performed on bunch of data generated after the race completion.
It generates circuit related data, race qulifier data, driver data, pit stops data etc.
I tried to achieve:
1. The Dominating player of all time
2. The Dominating Team of all time



## Data sets
1. cricuits.csv
2. races.csv
3. laptime.csv
4. qualifying.json
5. constructors.json
6. drivers.json
7. pit_stops.json
8. results.json


## Data Pipeline

Data Ingestion(Azure Storage)--> Transformation(Databricks)--> Loading to Presentation Layer(Azure Storage)---> Analysis(Power BI) 
