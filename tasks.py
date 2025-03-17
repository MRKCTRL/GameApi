from celery.result import AsyncResult
from flask import jsonify
from celery_work import app
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
import dotenv import load_dotenv 
import os 

load_dotenv()

RAPIDAPI_KEY = os.getenv('')
RAPIDAPI_HOST = os.getenv('')
DATABASE_URI = os.getenv('')
RAPIDAPI_URL = ''

@app.task
def process_data_async():
    try:
        headers = {
            'x-rapidapi-key':RAPIDAPI_KEY,
            'x-rapidapi-host':RAPIDAPI_HOST,
        }
        response=requests.get(RAPIDAPI_URL, headers=headers)
        response.raise_for_status()
        data=response.json()
        
        
        df=pd.DataFrame(data)
        if df.empty:
            raise ValueError("No data found in the API response.")
        df.dropna(inplace=True)
        df['new_column'] = df['existing_column'] * 2
        
        engine = create_engine(DATABASE_URI)
        df.to_sql('your_table_name', con=engine, if_exists='replace', index=False)
        
        return {"message": "Data processed and saved to SQL successfully!"}
    except requests.exceptions.RequestException as e:
        return {"error": f"failed to fetch data from RapidAPI: {e}"}
    except ValueError as e:
        return {"error": f"Data validation error: {e}"}
    except exc.SQLAlchemyError as e:
        return {"error": f"failed to save data to SQL: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}




@app.route('/task-status/<task_id>', methods=['GET'])
def task_status(task_id):
    task_result = AsyncResult(task_id)
    return jsonify({
        "tassk_id": task_id,
        "status": task_result.staus,
        "result": task_result.result,
    })