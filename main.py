import logging
from threading import Thread
from fastapi import FastAPI, Depends, HTTPException,Request
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import get_db
from crud import create_record, update_record
from producer import produce_data
from logging_anurag import log_fail
import xmltodict
import json
import uvicorn


app = FastAPI()

# Utility function to convert XML to JSON
def convert_xml_to_json(xml_data: str) -> dict:
   try:
      json_data = xmltodict.parse(xml_data)
      return json.loads(json.dumps(json_data))
   except Exception as e:
      raise HTTPException(status_code=400, detail=f"Error converting XML to JSON: {str(e)}")
     
# Pydantic models for API requests
class RecordRequest(BaseModel):
    tablename: str
    record: dict

# API for creating records
@app.post("/create_record/")
async def create_record_endpoint(request: Request, db: Session=Depends(get_db)):
   try:
      print("ENter try")
      request_data = await request.body()
      print(f"Received request data: {request_data}")
      print("dckwufhwa",request.headers.get('Content-Type'))
      if request.headers.get('Content-Type') == 'application/xml':
         request_data = convert_xml_to_json(request_data.decode('utf-8'))
         request_data=request_data.get("RecordRequest")
         print(f"Converted XML to JSON: {json.dumps(request_data)}")
      else:
         request_data = json.loads(request_data)
         print(f"Parsed JSON request data: {request_data}")
      record_request = RecordRequest(**request_data)
      return create_record(record_request.tablename, record_request.record, db)
   except json.JSONDecodeError as e:
      log_fail("record could not be added", "JSON decode error", "unknown")
      logging.error(f"JSON decode error: {str(e)}")
      raise HTTPException(status_code=400, detail=f"JSON decode error: {str(e)}")
   except Exception as e:
      log_fail("record could not be added", "error in request", "unknown")
      logging.error(f"Error processing request: {str(e)}")
      raise HTTPException(status_code=500, detail=str(e))


# API for updating records
@app.post("/update_record/")
def update_record_endpoint(request: RecordRequest, db: Session=Depends(get_db)):
   try:
      return update_record(request.tablename, request.record, db)
   except Exception as e:
      log_fail("record could not be added","error in request",request.tablename)
      raise HTTPException(status_code=500, detail=str(e))

def run_server():
   uvicorn.run(app=app)
   
if __name__=="__main__":
   t1=Thread(target=run_server)   
   t2=Thread(target=produce_data)
   t1.start()
   t2.start()
   t1.join()
   t2.join()