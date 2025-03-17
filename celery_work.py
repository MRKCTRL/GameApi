from celery import Celery
from app import fetch_data_from_rapidapi, clean_and_transform_data, save_to_sql
from dotenv import load_dotenv 
import os

app=Celery('tasks', broker=os.getenv(''))


app.conf.update(
    result_backend=os.getenv('CELERY_BROKER_URL'),
    tasl_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True
)


@app.task
def process_data_async():
    data = fetch_data_from_rapidapi()
    if not data:
        return {"error": "Failed to fetch data from RapidAPI"}
    
    df = clean_and_transform_data(data)
    if df is None:
        return {"error": "Failed to clean and transform data"}
    
    if not save_to_sql(df):
        return {"error": "failed to save data SQL"}
    
    return {"message" : "data processed and saved to SQL successfully"}