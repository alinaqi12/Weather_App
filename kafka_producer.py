import time
import json
from kafka import KafkaProducer
import requests


kafka_bootstrap_server=''
kafka_topic_name='sampletopic'

producer=KafkaProducer(bootstrap_servers=kafka_bootstrap_server,
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))

json_message=None
city_name=None
temperature=None
humidity=None
openweathermap_api_endpoint=None
appid=None

def get_weather(openweather_api_endpoint):
    api_response=requests.get(openweathermap_api_endpoint)
    json_data=api_response.json()
    city_name=json_data["name"]
    humidity=json_data["main"]['humidity']
    temperature=json_data["main"]['temp']
    json_message={"CityName":city_name,"Temperature":temperature,"Humidity":humidity,
                  "CreationTime":time.strftime("%Y-%m-%d %H:%M:%S")}
    
    return json_message

while True:
    city_name="Chennai"
    appid=""
    openweathermap_api_endpoint="api.openweathermap.org/data/2.5/weather?q="+city_name+"&APPID=84b7ae7ec520109fd1f66dedcf29e572"
    json_message=get_weather(openweathermap_api_endpoint)
    producer.send(kafka_topic_name,json_message)
    time.sleep(2)

    