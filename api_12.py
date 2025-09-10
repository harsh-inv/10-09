from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import os
import uuid
import tempfile
import sqlite3
import json
from datetime import datetime
import pandas as pd
import io
import asyncio
import logging
from pathlib import Path
import shutil

# Import your existing classes from the original file
from org_1_2907 import DataQualityChecker, DataMaskingManager, ResultsManager

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

# CORS middleware for UI5 and web integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response validation
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

class CheckResults(BaseModel):
    session_id: str
    status: str
    message: str
    results: Optional[Dict[str, List[Dict]]] = None
    summary: Optional[Dict[str, int]] = None
    timestamp: str

class DatabaseConnectRequest(BaseModel):
    db_path: str

class RunChecksRequest(BaseModel):
    session_id: str

# Session Manager for handling user sessions
class SessionManager:
    def __init__(self):
        self.sessions = {}
    
    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            'created_at': datetime.now(),
            'data_quality_config_path': None,
            'system_codes_config_path': None,
            'data_quality_config_filename': None,
            'system_codes_config_filename': None,
            'db_connection': None,
            'checker': None,
            'results': None,
            'results_manager': None,
            'temp_dir': tempfile.mkdtemp(),
            'db_path': None
        }
        logger.info(f"Created session: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Dict:
        if session_id not in self.sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        return self.sessions[session_id]
    
    def cleanup_session(self, session_id: str):
        if session_id in self.sessions:
            session = self.sessions[session_id]
            try:
                if session['db_connection']:
                    session['db_connection'].close()
                if session['results_manager']:
                    session['results_manager'].close()
                temp_dir = Path(session['temp_dir'])
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                del self.sessions[session_id]
                logger.info(f"Session {session_id} cleaned up")
            except Exception as e:
                logger.error(f"Error cleaning up session {session_id}: {str(e)}")

session_manager = SessionManager()

# App lifecycle events
@app.on_event("startup")
async def startup_event():
    logger.info("Data Quality Checker API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down - cleaning up sessions...")
    for session_id in list(session_manager.sessions.keys()):
        session_manager.cleanup_session(session_id)

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
            "3. POST /files/upload/system-codes-config/{session_id} - Upload codes file", 
            "4. GET /session/status/{session_id} - Check session status",
            "5. POST /database/connect - Connect to database",
            "6. POST /checks/run - Run quality checks",
            "7. GET /checks/results/{session_id} - Get results",
            "8. GET /files/download/results-csv/{session_id} - Download CSV"
        ],
        "active_sessions": len(session_manager.sessions)
    }

@app.get("/health")
async def health_check():
    """GET: Health check for Render monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(session_manager.sessions),
        "memory_usage": "OK"
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
    """GET: Retrieve the latest check results"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session['results']:
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        return {
            "session_id": session_id,
            "status": "success",
            "results": session['results'],
            "summary": session.get('summary', {}),
            "timestamp": datetime.now().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting results: {str(e)}")

@app.get("/files/download/results-csv/{session_id}")
async def download_results_csv(session_id: str):
    """GET: Download check results as CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session['results']:
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        # Create CSV content
        csv_rows = []
        csv_rows.append("table,field,check_type,status,message,timestamp")
        
        timestamp = datetime.now().isoformat()
        for table_name, table_results in session['results'].items():
            for result in table_results:
                message = result['message'].replace('"', '""')
                row = f'"{result["table"]}","{result["field"]}","{result["check_type"]}","{result["status"]}","{message}","{timestamp}"'
                csv_rows.append(row)
        
        # Save to temporary file
        csv_filename = f"data_quality_results_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        csv_path = os.path.join(session['temp_dir'], csv_filename)
        
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(csv_rows))
        
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

@app.get("/files/download/failed-values-csv/{session_id}")
async def download_failed_values_csv(session_id: str):
    """GET: Download detailed failing values as CSV"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session['results'] or not session['checker']:
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        # Create detailed failing values data
        failing_records = []
        for table_name, table_results in session['results'].items():
            for result in table_results:
                if result['status'] in ['FAIL', 'ERROR']:
                    failing_values = session['checker']._get_failing_values_from_db(
                        table_name, result['field'], result['check_type']
                    )
                    
                    for failing_value in failing_values:
                        failing_records.append({
                            'table': table_name,
                            'field_name': result['field'],
                            'check_type': result['check_type'],
                            'failing_value': str(failing_value),
                            'status': result['status'],
                            'message': result['message'],
                            'timestamp': datetime.now().isoformat()
                        })
        
        if not failing_records:
            raise HTTPException(status_code=404, detail="No failing values found.")
        
        # Convert to CSV
        df = pd.DataFrame(failing_records)
        csv_filename = f"failing_values_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        csv_path = os.path.join(session['temp_dir'], csv_filename)
        df.to_csv(csv_path, index=False)
        
        return FileResponse(
            csv_path,
            media_type='text/csv',
            filename=csv_filename,
            headers={"Content-Disposition": f"attachment; filename={csv_filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating failing values CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating failing values CSV: {str(e)}")

# ==================== POST ENDPOINTS ====================

@app.post("/session/create", response_model=SessionResponse)
async def create_session():
    """POST: Create a new session for data quality checking"""
    try:
        session_id = session_manager.create_session()
        return SessionResponse(
            session_id=session_id,
            message="Session created successfully. Upload both configuration files next.",
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
        
        # Validate CSV structure
        try:
            import csv
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                required_columns = ['table_name', 'field_name', 'description']
                if not all(col in reader.fieldnames for col in required_columns):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"CSV must contain columns: {required_columns}"
                    )
        except Exception as e:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
        
        session['data_quality_config_path'] = file_path
        session['data_quality_config_filename'] = file.filename
        
        logger.info(f"Data quality config uploaded for session {session_id}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="data_quality_config",
            message="Data quality configuration uploaded successfully.",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading data quality config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")

@app.post("/files/upload/system-codes-config/{session_id}", response_model=FileUploadResponse)
async def upload_system_codes_config(
    session_id: str,
    file: UploadFile = File(...)
):
    """POST: Upload system codes configuration CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        # Validate file
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV file")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], f"system_codes_config_{session_id}.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Validate CSV structure
        try:
            import csv
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                required_columns = ['table_name', 'field_name', 'valid_codes']
                if not all(col in reader.fieldnames for col in required_columns):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"CSV must contain columns: {required_columns}"
                    )
        except Exception as e:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
        
        session['system_codes_config_path'] = file_path
        session['system_codes_config_filename'] = file.filename
        
        logger.info(f"System codes config uploaded for session {session_id}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="system_codes_config",
            message="System codes configuration uploaded successfully.",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading system codes config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")

@app.post("/database/connect")
async def connect_database(request: Dict[str, str]):
    """POST: Connect to user's database"""
    try:
        session_id = request.get('session_id')
        db_path = request.get('db_path')
        
        if not session_id or not db_path:
            raise HTTPException(status_code=400, detail="session_id and db_path are required")
        
        session = session_manager.get_session(session_id)
        
        # Check if both files are uploaded
        if not session['data_quality_config_path'] or not session['system_codes_config_path']:
            raise HTTPException(
                status_code=400, 
                detail="Both configuration files must be uploaded before connecting to database"
            )
        
        # Validate database file exists
        if not os.path.exists(db_path):
            raise HTTPException(status_code=400, detail=f"Database file not found: {db_path}")
        
        # Connect to database
        try:
            db_connection = sqlite3.connect(db_path, check_same_thread=False)
            db_connection.row_factory = sqlite3.Row
            
            # Test connection
            cursor = db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if not tables:
                db_connection.close()
                raise HTTPException(status_code=400, detail="Database contains no tables")
            
        except sqlite3.Error as e:
            raise HTTPException(status_code=400, detail=f"Cannot connect to database: {str(e)}")
        
        # Create checker and load configs
        checker = DataQualityChecker(db_connection)
        results_manager = ResultsManager()
        
        # Load configurations
        if not checker.load_checks_config(session['data_quality_config_path']):
            db_connection.close()
            raise HTTPException(status_code=400, detail="Failed to load data quality configuration")
        
        if not checker.load_system_codes_config(session['system_codes_config_path']):
            db_connection.close()
            raise HTTPException(status_code=400, detail="Failed to load system codes configuration")
        
        # Store in session
        session['db_connection'] = db_connection
        session['checker'] = checker
        session['results_manager'] = results_manager
        session['db_path'] = db_path
        
        logger.info(f"Database connected for session {session_id}: {db_path}")
        
        return {
            "session_id": session_id,
            "message": f"Successfully connected to database: {db_path}",
            "status": "connected",
            "tables_found": len(tables),
            "table_names": [table[0] for table in tables],
            "data_quality_config_loaded": True,
            "system_codes_config_loaded": True,
            "ready_to_run_checks": True
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")

@app.post("/checks/run", response_model=CheckResults)
async def run_quality_checks(request: RunChecksRequest):
    """POST: Run data quality checks on the connected database"""
    try:
        session_id = request.session_id
        session = session_manager.get_session(session_id)
        
        # Validate prerequisites
        if not session['data_quality_config_path']:
            raise HTTPException(status_code=400, detail="Data quality configuration not uploaded")
        
        if not session['system_codes_config_path']:
            raise HTTPException(status_code=400, detail="System codes configuration not uploaded")
        
        if not session['checker']:
            raise HTTPException(status_code=400, detail="Database not connected")
        
        if not session['checker'].checks_config:
            raise HTTPException(status_code=400, detail="Data quality configuration not loaded")
        
        logger.info(f"Starting quality checks for session {session_id}")
        
        # Run checks
        results = session['checker'].run_all_checks()
        
        if not results:
            return CheckResults(
                session_id=session_id,
                status="no_issues",
                message="No data quality issues found or no data to check",
                results={},
                summary={'total_checks': 0, 'passed_checks': 0, 'failed_checks': 0, 'warnings': 0},
                timestamp=datetime.now().isoformat()
            )
        
        # Calculate summary
        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        warnings = 0
        
        for table_results in results.values():
            for result in table_results:
                total_checks += 1
                if result['status'] == 'PASS':
                    passed_checks += 1
                elif result['status'] == 'FAIL':
                    failed_checks += 1
                elif result['status'] == 'WARNING':
                    warnings += 1
                elif result['status'] == 'ERROR':
                    failed_checks += 1
        
        summary = {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'warnings': warnings
        }
        
        # Store results in session
        session['results'] = results
        session['summary'] = summary
        
        logger.info(f"Quality checks completed for session {session_id}: {summary}")
        
        return CheckResults(
            session_id=session_id,
            status="completed",
            message=f"Quality checks completed. Found {failed_checks} failures and {warnings} warnings.",
            results=results,
            summary=summary,
            timestamp=datetime.now().isoformat()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running checks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running checks: {str(e)}")

@app.delete("/session/{session_id}")
async def delete_session(session_id: str):
    """DELETE: Clean up session resources"""
    try:
        session_manager.cleanup_session(session_id)
        return {"message": f"Session {session_id} deleted successfully", "status": "deleted"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting session: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting session: {str(e)}")

# Main entry point for Render
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
