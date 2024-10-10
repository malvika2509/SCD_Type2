from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
# import sys
from .database import get_db
from .crud import create_record, update_record

app = FastAPI()

@app.post("/create_record/")
def create_record_endpoint(record: dict, db: Session = Depends(get_db)):
    try:
        return create_record(record, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update_record/")
def update_record_endpoint(new_record: dict, db: Session = Depends(get_db)):
    try:
        return update_record(new_record, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
