# Real-time visualization

Start up all the services:

```
docker compose up -d
```

## Postgres
Using a sql IDE (e.g. DBeaver, pgAdmin), login to Postgres and create the sink table in database "visual".  
Host:port = localhost:5432
The Postgres user / pw is in the Docker environment settings. 

```
CREATE TABLE stream_loc
(name VARCHAR(100),
latitude FLOAT,
longitude FLOAT)
```

## Grafana
Login to Grafana by going to localhost:3000  
User/pw = admin/admin. You can skip creating new password.  
Navigate to Data Sources (e.g. from left side menu: Configuration --> Data sources, OR Add your first data source on welcome screen)  
Find postgreSQL. The user/pw is in the Docker environment settings, host:port = postgres:5432.
Disable TLS/SSL Mode. Save & Test.
Create a new dashboard (from left side menu: Dashboards --> New dashboard).  
Add a new panel.  
In the query tab (bottom left), choose Format as Table and click on Edit SQL.

```
SELECT name
, latitude
, longitude 
FROM stream_loc
```

On the top right, instead of Time series, choose Geomap. Click Apply.
On the dashboard screen, next to refresh button there is a dropdown menu for refresh frequency. Choose every 5 seconds (5s).

## Pyspark
Navigate to localhost:8888 on your browser to access Jupyter notebooks.
There should be two notebooks. 
First, open 00_Consumer and run all cells. It may take up to 1-2 minutes to get everything started.
The last cell will keep running as it is pulling data from Kafka. 

## Local
In the mean-time, open the kafka_generator notebook from your local machine (e.g. VS Code) and execute it.
It will start fetching 2 new users every 4 seconds.

## Pyspark
Navigate back to 00_Consumer notebook in Jupyter. If the last cell has started executing and some folders have appeared (checkpoints, output), then navigate to 04_Postgres. Execute all cells.

## Grafana
Now, go back to Grafana (localhost:3000) and view the dashboard updating every 5 seconds.

Done.

