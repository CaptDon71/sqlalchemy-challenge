# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table

# Assign the Measurement class to a variable called `Measurement`
Measurement = Base.classes.measurement
# Assign the Station class to a variable called `Station`
Station = Base.classes.station


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
# Define what to do when a user hits the precipitation route.
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Find the most recent date in the data set.
    most_recent_date = session.query(func.max(Measurement.date)).first()

    # Close the session
    session.close()
    # Starting from the most recent data point in the database. 
    formatted = dt.datetime.strptime(most_recent_date[0], '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    one_year_ago = formatted - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    precip_data = session.query(Measurement.date, Measurement.prcp).filter(
        Measurement.date >= one_year_ago
        ).all()
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    df = pd.DataFrame(precip_data, columns=['Date', 'Precipitation'])
    # Sort the dataframe by date
    df = df.sort_values(by='Date')
    list_creation_df = list(np.ravel(df))

    return jsonify(list_creation_df)

# Define what to do when a user hits the /normal route
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Design a query to calculate the total number of stations in the dataset
    most_active_stations = (
    session.query(Station.station, Station.name)
    .group_by(Station.station)
    .order_by(func.count(Station.station).desc())
    .all()
)
    station_creation_df = list(np.ravel(most_active_stations))
    # Close the session
    session.close()
    return jsonify(station_creation_df)

# Define what to do when a user hits the /jsonified route
@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    most_active_station_result = (
        session.query(Measurement.station)
        .group_by(Measurement.station)
        .order_by(func.count(Measurement.station).desc())
        .first()  
    )
    # Check if the most active station was found
    if most_active_station_result:
        most_active_station_id = most_active_station_result[0]
        # Query to get temperature statistics for the most active station
        temperature_stats = (
            session.query(
                func.min(Measurement.tobs).label('min_temp'),
                func.max(Measurement.tobs).label('max_temp'),
                func.avg(Measurement.tobs).label('avg_temp')
            )
            .filter(Measurement.station == most_active_station_id)
            .one()
        )
        # Convert the result into a dictionary
        temperature_stats_dict = {
            'min_temp': temperature_stats.min_temp,
            'max_temp': temperature_stats.max_temp,
            'avg_temp': temperature_stats.avg_temp
        }

        # Close the session
        session.close()
    return jsonify(temperature_stats_dict)

# Define what to do when a user hits the /normal route
@app.route('/api/v1.0/<start>', methods=['GET'])
@app.route('/api/v1.0/<start>/<end>', methods=['GET'])
def temperature_stats(start, end=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Convert start and end to datetime objects
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        if end:
            end_date = datetime.strptime(end, '%Y-%m-%d')
        else:
            end_date = None
    except ValueError:
        return jsonify({"error": "Date format should be YYYY-MM-DD"}), 400

    # Query to calculate TMIN, TAVG, TMAX
    if end_date:
        # Case with both start and end date
        temperature_stats = (
            session.query(
                func.min(Measurement.tobs).label('TMIN'),
                func.avg(Measurement.tobs).label('TAVG'),
                func.max(Measurement.tobs).label('TMAX')
            )
            .filter(Measurement.date >= start_date)
            .filter(Measurement.date <= end_date)
            .one()
        )
    else:
        # Case with only start date
        temperature_stats = (
            session.query(
                func.min(Measurement.tobs).label('TMIN'),
                func.avg(Measurement.tobs).label('TAVG'),
                func.max(Measurement.tobs).label('TMAX')
            )
            .filter(Measurement.date >= start_date)
            .one()
        )

    # Convert the result into a dictionary
    temperature_stats_dict = {
        'TMIN': temperature_stats.TMIN,
        'TAVG': temperature_stats.TAVG,
        'TMAX': temperature_stats.TMAX
    }

    # Return the JSON response
    return jsonify(temperature_stats_dict)

if __name__ == "__main__":
    app.run(debug=True)
