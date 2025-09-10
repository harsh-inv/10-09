from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List
import os
import uuid
import tempfile
import sqlite3
import csv
from datetime import datetime
import logging
from pathlib import Path
import shutil

# Import your existing classes (remove pandas import from original file)
import sys
import re

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Quality Checker API",
    description="Upload configuration files, connect to database, and run quality checks",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class SessionResponse(BaseModel):
    session_id: str
    message: str
    status: str

class FileUploadResponse(BaseModel):
    session_id: str
    filename: str
    file_type: str
    message: str
    status: str

class SessionStatus(BaseModel):
    session_id: str
    data_quality_config_uploaded: bool
    system_codes_config_uploaded: bool
    both_files_uploaded: bool
    database_connected: bool
    ready_to_run_checks: bool
    has_results: bool
    created_at: str
    db_path: Optional[str] = None

# Session Manager
class SessionManager:
    def __init__(self):
        self.sessions = {}
    
    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            'created_at': datetime.now(),
            'data_quality_config_path': None,
            'system_codes_config_path': None,
            'db_connection': None,
            'checker': None,
            'results': None,
            'temp_dir': tempfile.mkdtemp(),
            'db_path': None
        }
        logger.info(f"Created session: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Dict:
        if session_id not in self.sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        return self.sessions[session_id]

session_manager = SessionManager()

# ==================== GET ENDPOINTS ====================

@app.get("/")
async def root():
    """GET: API information and workflow"""
    return {
        "message": "Data Quality Checker API - Live on Render",
        "version": "1.0.0",
        "status": "running",
        "workflow": [
            "1. POST /session/create - Create new session",
            "2. POST /files/upload/data-quality-config/{session_id} - Upload config file",
            "3. GET /session/status/{session_id} - Check session status",
            "4. POST /checks/run - Run quality checks",
            "5. GET /checks/results/{session_id} - Get results",
            "6. GET /files/download/results-csv/{session_id} - Download CSV"
        ],
        "active_sessions": len(session_manager.sessions)
    }

@app.get("/health")
async def health_check():
    """GET: Health check for Render monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(session_manager.sessions)
    }

@app.get("/session/status/{session_id}", response_model=SessionStatus)
async def get_session_status(session_id: str):
    """GET: Get current session status and progress"""
    try:
        session = session_manager.get_session(session_id)
        
        data_quality_uploaded = session['data_quality_config_path'] is not None
        system_codes_uploaded = session['system_codes_config_path'] is not None
        both_files_uploaded = data_quality_uploaded and system_codes_uploaded
        database_connected = session['db_connection'] is not None
        ready_to_run = both_files_uploaded and database_connected
        has_results = session['results'] is not None
        
        return SessionStatus(
            session_id=session_id,
            data_quality_config_uploaded=data_quality_uploaded,
            system_codes_config_uploaded=system_codes_uploaded,
            both_files_uploaded=both_files_uploaded,
            database_connected=database_connected,
            ready_to_run_checks=ready_to_run,
            has_results=has_results,
            created_at=session['created_at'].isoformat(),
            db_path=session.get('db_path')
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting session status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting session status: {str(e)}")

# ==================== POST ENDPOINTS ====================

@app.post("/session/create", response_model=SessionResponse)
async def create_session():
    """POST: Create a new session for data quality checking"""
    try:
        session_id = session_manager.create_session()
        return SessionResponse(
            session_id=session_id,
            message="Session created successfully. Upload configuration files next.",
            status="created"
        )
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating session: {str(e)}")

@app.post("/files/upload/data-quality-config/{session_id}", response_model=FileUploadResponse)
async def upload_data_quality_config(
    session_id: str,
    file: UploadFile = File(...)
):
    """POST: Upload data quality configuration CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        # Validate file
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV file")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], f"data_quality_config_{session_id}.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        session['data_quality_config_path'] = file_path
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="data_quality_config",
            message="Data quality configuration file uploaded successfully",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading data quality config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api_server:app", host="0.0.0.0", port=port, reload=False)
