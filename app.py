from flask import Flask, request, jsonify 
import requests 
import pandas as pd 
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os

from celery import AsyncResult




app=Flask(__name__)
load_dotenv()

RAPIDAPI_KEY = os.getenv('')
RAPIDAPI_HOST = os.getenv('')
RAPIDAPI_URl = os.getenv('')
DATABASE_URI = 'mysql+pymysql://username:password@localhost/dbname'

if not all([RAPIDAPI_KEY, RAPIDAPI_HOST, DATABASE_URI]):
    raise EnvironmentError("Missing required environment variables.")





def fetch_data_from_rapidapi():
    headers = {
        'x-rapidapi-key':RAPIDAPI_KEY,
        'x-rapidapi-key': RAPIDAPI_HOST
    }
    
    try:
        response = requests.get(RAPIDAPI_URl, headers=headers)
        response.raise_for_status()
        return response.json()
    
    except: requests.exceptions.RequestException as e:
        app.logger.error(f"Failed to fetch data from RapidAPI: {e}")
        return None

    
    

def clean_and_transform_data(data):
    try:
        df= pd.DataFrame(data)
        if df.empty:
            raise ValueError("No data found in the Api response.")
        
    df.dropna(inplace=True)
    df['new_column'] = df['existing_column'] * 2
    
    return df 

    except Exception as e:
        app.logger.error(f"Error during data cleaning and transformation: {e}")
        return None
    
    
def save_to_sql(df):
    try:
        engine=create_engine(DATABASE_URI)
        df.to_sql('', con=engine, if_exists='replace', index=False)
        return True
    except exc.SQLAlchemyError as e:
        app.longer.error(f"Failed to save data to SQL: {e}")
        return False


@app.route('/process-data',methods=['GET'])
def process_data():
    task = process_data.delay()
    return jsonify({"task_id": task.id, "status": "Task started"}), 202
    
    data=fetch_data_from_rapidapi()
    if not data:
        return jsonify({"error": "failed to fetch data from RapidApi"}), 500
     
    df = clean_and_transform_data(data)
    
    if df is None:
        return jsonify({"error": "Failed to clean and transform data"}), 500
    
    
    if not save_to_sql(df):
        return jsonify({"error": "Failed to save data to SQL"}), 500
    
    return jsonify({"message": "data processed and saved to SQL successfullu"}), 200

   
@app.route('/task-status/<task-id>', methods=['GET'])
def task_status(task_id):
    task_result=AsyncResult(task_id)
    return jsonify({
        "task_id":task_id,
        "status": task_result.status,
        "result":task_result.result,
    })    
    
if __name__=="__name__":
    app.run(debug=True)
    