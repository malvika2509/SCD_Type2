import logging
import threading
import asyncio
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from .database import get_db
from .crud import create_record, update_record
from .producer import produce_data
from .logger import logger

##################################################
def log_fail(whatfailed, reason,tablename):
    logger.info('', extra={
        'username': 'malvika',
        'log_id': 1,
        'values': {
            'whatfailed': whatfailed,
            'reason':reason,
            'iserrorlog': 1,
            'table':tablename
        }
    })

def log_incrementalloading(tablename,count):
    logger.info('', extra={
        'username': 'malvika',
        'log_id': 1,
        'values': {
            'iserrorlog': 0,
            'table':tablename,
            'count': count
    }
})   
    
def log_update(tablename,scd_type):
    logger.info('', extra={
        'username': 'malvika',
        'log_id': 1,
        'values': {
            'iserrorlog': 0,
            'table':tablename,
            'scd_type': scd_type
    }
}) 
    
def log_overwrite():
    logger.info('', extra={
        'username': 'malvika',
        'log_id': 1,
        'values': {
            'iserrorlog': 0,
            'table':'DimRegion'
    }
})


##################################################

app = FastAPI()

# Start data generation on startup
@app.on_event("startup")
async def startup_event():
    logging.info("Starting up...")
    # Start the data generation in a separate thread
    loop = asyncio.get_event_loop()
    loop.create_task(produce_data())
    logging.info("Data generation started.")
    logging.info("Startup complete.")

# Pydantic models for API requests
class RecordRequest(BaseModel):
    tablename: str
    record: dict

# API for creating records
@app.post("/create_record/")
def create_record_endpoint(request: RecordRequest, db: Session = Depends(get_db)):
    try:
        return create_record(request.tablename, request.record, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API for updating records
@app.post("/update_record/")
def update_record_endpoint(request: RecordRequest, db: Session = Depends(get_db)):
    try:
        return update_record(request.tablename, request.record, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
