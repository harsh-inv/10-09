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

class ChecksRunResponse(BaseModel):
    session_id: str
    status: str
    message: str
    checks_run: int
    passed: int
    failed: int
    warnings: int

class ChecksResultsResponse(BaseModel):
    session_id: str
    status: str
    results: Dict
    summary: Dict

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
            "3. POST /files/upload/system-codes-config/{session_id} - Upload system codes file",
            "4. GET /session/status/{session_id} - Check session status",
            "5. POST /checks/run - Run quality checks",
            "6. GET /checks/results/{session_id} - Get results",
            "7. GET /files/download/results-csv/{session_id} - Download CSV"
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

@app.get("/checks/results/{session_id}")
async def get_check_results(session_id: str):
    """GET: Get quality check results"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session.get('results'):
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        return ChecksResultsResponse(
            session_id=session_id,
            status="available",
            results=session['results'],
            summary={
                "total_checks": len(session['results']),
                "passed": sum(1 for r in session['results'] if r.get('status') == 'PASS'),
                "failed": sum(1 for r in session['results'] if r.get('status') == 'FAIL'),
                "warnings": sum(1 for r in session['results'] if r.get('status') == 'WARNING')
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting results: {str(e)}")

@app.get("/files/download/results-csv/{session_id}")
async def download_results_csv(session_id: str):
    """GET: Download results as CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session.get('results'):
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        # Create CSV file
        csv_filename = f"quality_results_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        csv_path = os.path.join(session['temp_dir'], csv_filename)
        
        # Create CSV with actual results
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['table', 'field', 'check_type', 'status', 'message', 'timestamp'])
            
            # Mock data - replace with actual results from session
            for result in session['results']:
                writer.writerow([
                    result.get('table', 'unknown'),
                    result.get('field', 'unknown'),
                    result.get('check_type', 'unknown'),
                    result.get('status', 'unknown'),
                    result.get('message', 'No message'),
                    datetime.now().isoformat()
                ])
        
        return FileResponse(
            csv_path,
            media_type='text/csv',
            filename=csv_filename,
            headers={"Content-Disposition": f"attachment; filename={csv_filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating CSV: {str(e)}")

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
        
        # Enhanced validation to prevent 422 errors
        if not file.filename:
            raise HTTPException(status_code=422, detail="No file uploaded")
            
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=422, detail="File must be a CSV file with .csv extension")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=422, detail="Uploaded file is empty")
        
        # Validate CSV structure
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(content_str.splitlines())
            required_columns = [
                'table_name', 'field_name', 'description', 'null_check', 
                'blank_check', 'email_check', 'numeric_check', 'duplicate_check'
            ]
            
            if not csv_reader.fieldnames:
                raise HTTPException(status_code=422, detail="CSV file has no headers")
            
            if not all(col in csv_reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
                raise HTTPException(
                    status_code=422, 
                    detail=f"CSV missing required columns: {', '.join(missing_cols)}"
                )
                
        except UnicodeDecodeError:
            raise HTTPException(status_code=422, detail="File must be UTF-8 encoded CSV")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], f"data_quality_config_{session_id}.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        session['data_quality_config_path'] = file_path
        logger.info(f"Data quality config uploaded for session {session_id}: {file.filename}")
        
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
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/files/upload/system-codes-config/{session_id}", response_model=FileUploadResponse)
async def upload_system_codes_config(
    session_id: str,
    file: UploadFile = File(...)
):
    """POST: Upload system codes configuration CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        # Enhanced validation to prevent 422 errors
        if not file.filename:
            raise HTTPException(status_code=422, detail="No file uploaded")
            
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=422, detail="File must be a CSV file with .csv extension")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=422, detail="Uploaded file is empty")
        
        # Validate CSV structure for system codes
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(content_str.splitlines())
            required_columns = ['table_name', 'field_name', 'valid_codes']
            
            if not csv_reader.fieldnames:
                raise HTTPException(status_code=422, detail="CSV file has no headers")
            
            if not all(col in csv_reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
                raise HTTPException(
                    status_code=422, 
                    detail=f"System codes CSV missing required columns: {', '.join(missing_cols)}"
                )
                
        except UnicodeDecodeError:
            raise HTTPException(status_code=422, detail="File must be UTF-8 encoded CSV")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], f"system_codes_config_{session_id}.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        session['system_codes_config_path'] = file_path
        logger.info(f"System codes config uploaded for session {session_id}: {file.filename}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="system_codes_config",
            message="System codes configuration file uploaded successfully",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading system codes config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/checks/run")
async def run_quality_checks(session_id: str):
    """POST: Run data quality checks"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session['data_quality_config_path']:
            raise HTTPException(status_code=400, detail="No data quality config uploaded")
        
        # Mock implementation - replace with actual DataQualityChecker integration
        # Here you would:
        # 1. Load the uploaded CSV files
        # 2. Initialize DataQualityChecker from your org_1_2907.py
        # 3. Run actual quality checks
        # 4. Store results in session
        
        mock_results = [
            {
                'table': 'users',
                'field': 'email',
                'check_type': 'email_check',
                'status': 'PASS',
                'message': 'All email formats are valid'
            },
            {
                'table': 'users',
                'field': 'phone',
                'check_type': 'phone_check',
                'status': 'FAIL',
                'message': 'Found 3 invalid phone numbers'
            },
            {
                'table': 'users',
                'field': 'age',
                'check_type': 'numeric_check',
                'status': 'PASS',
                'message': 'All values are numeric'
            }
        ]
        
        session['results'] = mock_results
        
        # Calculate summary
        passed = sum(1 for r in mock_results if r['status'] == 'PASS')
        failed = sum(1 for r in mock_results if r['status'] == 'FAIL')
        warnings = sum(1 for r in mock_results if r['status'] == 'WARNING')
        
        return ChecksRunResponse(
            session_id=session_id,
            status="completed",
            message="Quality checks completed successfully",
            checks_run=len(mock_results),
            passed=passed,
            failed=failed,
            warnings=warnings
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running quality checks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running checks: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api_server:app", host="0.0.0.0", port=port, reload=False)
