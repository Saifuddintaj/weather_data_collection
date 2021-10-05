from numpy.lib.function_base import append
from credentials import API_KEY
import requests,json
import pandas as pd
import time
import sqlalchemy
import psycopg2 as pg

engine = sqlalchemy.create_engine('postgresql+psycopg2://temperature_user:temperature_password@localhost/temperature_db')

BASE_URL="https://api.openweathermap.org/data/2.5/weather?"

CITIES=["Khammam","Hyderabad","Delhi","Kolkata","Chennai","Bangalore","Mumbai","Visakhapatnam","London"]

def run_weather_collection():
    '''
    This function runs the ETL part of the app. It extracts data from API, Transforms it to Pandas Data Frame and Loads it to a Postgres Database
    '''

    for i in range(len(CITIES)):

        URL = BASE_URL+"q="+CITIES[i]+"&appid="+API_KEY+"&units=metric"
        response=requests.get(URL)

        #Checking if the API is giving the response

        if response.status_code == 200:

            #Extraction Part of the Function
            data=response.json()
            
            # Transformation Part of the Function
            epoch_time = data['dt']
            human_time = time.strftime("%d-%b-%Y %H:%M:%S", time.localtime(epoch_time))
            time_of_collection=human_time.split(' ')[1]
            date_of_collection=human_time.split(' ')[0]
            weather_description=data['weather'][0]['description']
            temperature=data['main']['temp']
            city_id=data['id']
            city_name=CITIES[i]
            weather_dict={
            "city_id":[city_id],
            "temperature":[temperature],
            "description": [weather_description],
            "date_of_collection":[date_of_collection],
            "time_of_collection":[time_of_collection]}
            weather_df=pd.DataFrame(weather_dict,columns=["city_id","temperature","description","date_of_collection","time_of_collection"])
            
            # Loading to the Postgres Database
            weather_df.to_sql("weather_details",engine,index=False,if_exists="append")
        
#End of the script


