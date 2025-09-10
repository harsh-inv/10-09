# main.py - Complete FastAPI Web API for Data Quality Checker
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import os
import uuid
import tempfile
import sqlite3
import csv
import json
from datetime import datetime
import logging
import sys
import shutil
import io

# Import your existing classes from org_1_2907.py
try:
    from org_1_2907 import DataQualityChecker, ResultsManager, DataMaskingManager
except ImportError:
    print("Warning: Could not import from org_1_2907.py - using fallback classes")
    # Fallback classes would go here if needed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="Northwind Data Quality Checker API",
    description="Upload Northwind database and configuration files to run comprehensive data quality checks",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class SessionResponse(BaseModel):
    session_id: str
    message: str
    status: str

class CheckResults(BaseModel):
    session_id: str
    results: Dict[str, Any]
    summary: Dict[str, int]
    timestamp: str

class FileInfo(BaseModel):
    filename: str
    size: int
    uploaded_at: str
    file_type: str

class SessionStatus(BaseModel):
    session_id: str
    files_uploaded: Dict[str, FileInfo]
    database_connected: bool
    config_loaded: bool
    system_codes_loaded: bool
    last_check_results: Optional[Dict[str, Any]] = None

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
            'northwind_db_path': None,
            'db_connection': None,
            'checker': None,
            'results_manager': None,
            'results': None,
            'temp_dir': tempfile.mkdtemp(),
            'files_info': {}
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
            # Close database connections
            if session.get('db_connection'):
                session['db_connection'].close()
            if session.get('results_manager'):
                session['results_manager'].close()
            # Clean up temporary files
            if session.get('temp_dir') and os.path.exists(session['temp_dir']):
                shutil.rmtree(session['temp_dir'], ignore_errors=True)
            del self.sessions[session_id]
            logger.info(f"Cleaned up session: {session_id}")

session_manager = SessionManager()

# Helper Functions
def save_uploaded_file(upload_file: UploadFile, session_dir: str, file_type: str) -> str:
    """Save uploaded file to session directory"""
    file_path = os.path.join(session_dir, f"{file_type}_{upload_file.filename}")
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    return file_path

def validate_csv_structure(file_path: str, expected_columns: List[str]) -> bool:
    """Validate CSV file has expected columns"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            file_columns = set(reader.fieldnames or [])
            expected_columns_set = set(expected_columns)
            return expected_columns_set.issubset(file_columns)
    except Exception as e:
        logger.error(f"Error validating CSV structure: {str(e)}")
        return False

def get_database_tables(db_path: str) -> List[str]:
    """Get list of tables in SQLite database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        logger.error(f"Error getting database tables: {str(e)}")
        return []

# API Endpoints

@app.get("/")
async def root():
    return {
        "message": "Northwind Data Quality Checker API - Live on Render",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "create_session": "/sessions/create",
            "upload_database": "/sessions/{session_id}/upload/database",
            "upload_config": "/sessions/{session_id}/upload/config",
            "run_checks": "/sessions/{session_id}/run-checks",
            "get_results": "/sessions/{session_id}/results",
            "export_results": "/sessions/{session_id}/export/{format}",
            "docs": "/docs"
        },
        "active_sessions": len(session_manager.sessions)
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/sessions/create", response_model=SessionResponse)
async def create_session():
    """Create a new session for data quality checking"""
    session_id = session_manager.create_session()
    return SessionResponse(
        session_id=session_id,
        message="Session created successfully",
        status="created"
    )

@app.get("/sessions/{session_id}/status", response_model=SessionStatus)
async def get_session_status(session_id: str):
    """Get current status of a session"""
    session = session_manager.get_session(session_id)
    
    return SessionStatus(
        session_id=session_id,
        files_uploaded=session.get('files_info', {}),
        database_connected=session.get('db_connection') is not None,
        config_loaded=session.get('data_quality_config_path') is not None,
        system_codes_loaded=session.get('system_codes_config_path') is not None,
        last_check_results=session.get('results')
    )

@app.post("/sessions/{session_id}/upload/database")
async def upload_database(session_id: str, database: UploadFile = File(...)):
    """Upload Northwind SQLite database file"""
    session = session_manager.get_session(session_id)
    
    # Validate file type
    if not database.filename.lower().endswith(('.db', '.sqlite', '.sqlite3')):
        raise HTTPException(status_code=400, detail="Database file must be .db, .sqlite, or .sqlite3")
    
    try:
        # Save uploaded file
        db_path = save_uploaded_file(database, session['temp_dir'], "database")
        
        # Test database connection and get table info
        tables = get_database_tables(db_path)
        if not tables:
            raise HTTPException(status_code=400, detail="Invalid database file or no tables found")
        
        # Connect to database
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        
        # Update session
        session['northwind_db_path'] = db_path
        session['db_connection'] = connection
        session['checker'] = DataQualityChecker(connection)
        session['results_manager'] = ResultsManager()
        session['files_info']['database'] = FileInfo(
            filename=database.filename,
            size=os.path.getsize(db_path),
            uploaded_at=datetime.now().isoformat(),
            file_type="database"
        )
        
        logger.info(f"Database uploaded for session {session_id}: {database.filename}")
        
        return {
            "message": "Database uploaded and connected successfully",
            "filename": database.filename,
            "tables_found": len(tables),
            "table_names": tables[:10],  # Show first 10 tables
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error uploading database for session {session_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing database: {str(e)}")

@app.post("/sessions/{session_id}/upload/config")
async def upload_config(
    session_id: str, 
    data_quality_config: UploadFile = File(...),
    system_codes_config: Optional[UploadFile] = File(None)
):
    """Upload data quality configuration files"""
    session = session_manager.get_session(session_id)
    
    if not session.get('checker'):
        raise HTTPException(status_code=400, detail="Database must be uploaded first")
    
    try:
        # Process data quality config
        if not data_quality_config.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Data quality config must be a CSV file")
        
        config_path = save_uploaded_file(data_quality_config, session['temp_dir'], "data_quality_config")
        
        # Validate CSV structure
        expected_columns = ['table_name', 'field_name', 'description', 'null_check', 'blank_check']
        if not validate_csv_structure(config_path, expected_columns):
            raise HTTPException(status_code=400, detail="Invalid data quality config CSV structure")
        
        # Load configuration
        success = session['checker'].load_checks_config(config_path)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to load data quality configuration")
        
        session['data_quality_config_path'] = config_path
        session['files_info']['data_quality_config'] = FileInfo(
            filename=data_quality_config.filename,
            size=os.path.getsize(config_path),
            uploaded_at=datetime.now().isoformat(),
            file_type="data_quality_config"
        )
        
        response_data = {
            "message": "Data quality config loaded successfully",
            "data_quality_config": data_quality_config.filename,
            "tables_configured": len(session['checker'].checks_config),
            "status": "success"
        }
        
        # Process system codes config if provided
        if system_codes_config:
            if not system_codes_config.filename.lower().endswith('.csv'):
                raise HTTPException(status_code=400, detail="System codes config must be a CSV file")
            
            system_codes_path = save_uploaded_file(system_codes_config, session['temp_dir'], "system_codes_config")
            
            # Validate CSV structure
            system_codes_columns = ['table_name', 'field_name', 'valid_codes']
            if not validate_csv_structure(system_codes_path, system_codes_columns):
                raise HTTPException(status_code=400, detail="Invalid system codes config CSV structure")
            
            # Load system codes configuration
            success = session['checker'].load_system_codes_config(system_codes_path)
            if success:
                session['system_codes_config_path'] = system_codes_path
                session['files_info']['system_codes_config'] = FileInfo(
                    filename=system_codes_config.filename,
                    size=os.path.getsize(system_codes_path),
                    uploaded_at=datetime.now().isoformat(),
                    file_type="system_codes_config"
                )
                response_data.update({
                    "system_codes_config": system_codes_config.filename,
                    "system_codes_loaded": True
                })
        
        logger.info(f"Configurations uploaded for session {session_id}")
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading config for session {session_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing configuration: {str(e)}")

@app.post("/sessions/{session_id}/run-checks", response_model=CheckResults)
async def run_data_quality_checks(session_id: str, specific_table: Optional[str] = None):
    """Run data quality checks on uploaded database"""
    session = session_manager.get_session(session_id)
    
    if not session.get('checker'):
        raise HTTPException(status_code=400, detail="Database and configuration must be uploaded first")
    
    if not session['checker'].checks_config:
        raise HTTPException(status_code=400, detail="Data quality configuration not loaded")
    
    try:
        # Run checks
        if specific_table:
            results = session['checker'].run_checks_for_specific_table(specific_table)
        else:
            results = session['checker'].run_all_checks()
        
        # Calculate summary statistics
        summary = {"total": 0, "passed": 0, "failed": 0, "warnings": 0}
        
        for table_results in results.values():
            for result in table_results:
                summary["total"] += 1
                status = result["status"]
                if status == "PASS":
                    summary["passed"] += 1
                elif status == "FAIL" or status == "ERROR":
                    summary["failed"] += 1
                elif status == "WARNING":
                    summary["warnings"] += 1
        
        # Store results in session
        session['results'] = results
        
        logger.info(f"Data quality checks completed for session {session_id}")
        
        return CheckResults(
            session_id=session_id,
            results=results,
            summary=summary,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error running checks for session {session_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running data quality checks: {str(e)}")

@app.get("/sessions/{session_id}/results")
async def get_results(session_id: str):
    """Get the last run results for a session"""
    session = session_manager.get_session(session_id)
    
    if not session.get('results'):
        raise HTTPException(status_code=404, detail="No results found. Run checks first.")
    
    return {
        "session_id": session_id,
        "results": session['results'],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/sessions/{session_id}/export/{format}")
async def export_results(session_id: str, format: str, check_type: str = "all"):
    """Export results in different formats"""
    session = session_manager.get_session(session_id)
    
    if not session.get('results'):
        raise HTTPException(status_code=404, detail="No results found. Run checks first.")
    
    if format.lower() not in ['csv', 'json']:
        raise HTTPException(status_code=400, detail="Format must be 'csv' or 'json'")
    
    if check_type.lower() not in ['all', 'failed', 'passed']:
        raise HTTPException(status_code=400, detail="Check type must be 'all', 'failed', or 'passed'")
    
    try:
        results = session['results']
        
        # Filter results based on check_type
        filtered_results = {}
        for table_name, table_results in results.items():
            if check_type.lower() == 'failed':
                filtered_table_results = [r for r in table_results if r['status'] in ['FAIL', 'ERROR']]
            elif check_type.lower() == 'passed':
                filtered_table_results = [r for r in table_results if r['status'] in ['PASS', 'INFO']]
            else:
                filtered_table_results = table_results
            
            if filtered_table_results:
                filtered_results[table_name] = filtered_table_results
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format.lower() == 'csv':
            # Create CSV export
            filename = f"data_quality_results_{check_type}_{timestamp}.csv"
            file_path = os.path.join(session['temp_dir'], filename)
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['table', 'field', 'check_type', 'status', 'message', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for table_name, table_results in filtered_results.items():
                    for result in table_results:
                        writer.writerow({
                            'table': result['table'],
                            'field': result['field'],
                            'check_type': result['check_type'],
                            'status': result['status'],
                            'message': result['message'],
                            'timestamp': datetime.now().isoformat()
                        })
            
            return FileResponse(
                path=file_path,
                filename=filename,
                media_type='text/csv'
            )
        
        else:  # JSON format
            filename = f"data_quality_results_{check_type}_{timestamp}.json"
            export_data = {
                "session_id": session_id,
                "export_type": check_type,
                "timestamp": datetime.now().isoformat(),
                "results": filtered_results
            }
            
            return JSONResponse(
                content=export_data,
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
            
    except Exception as e:
        logger.error(f"Error exporting results for session {session_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error exporting results: {str(e)}")

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session and clean up resources"""
    try:
        session_manager.cleanup_session(session_id)
        return {"message": f"Session {session_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting session {session_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting session: {str(e)}")

@app.get("/sessions")
async def list_sessions():
    """List all active sessions"""
    sessions_info = []
    for session_id, session_data in session_manager.sessions.items():
        sessions_info.append({
            "session_id": session_id,
            "created_at": session_data['created_at'].isoformat(),
            "files_uploaded": len(session_data.get('files_info', {})),
            "database_connected": session_data.get('db_connection') is not None,
            "has_results": session_data.get('results') is not None
        })
    
    return {
        "total_sessions": len(sessions_info),
        "sessions": sessions_info
    }

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"message": "Resource not found", "status": "error"}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "status": "error"}
    )

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    logger.info("Data Quality Checker API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Data Quality Checker API shutting down...")
    # Clean up all sessions
    for session_id in list(session_manager.sessions.keys()):
        session_manager.cleanup_session(session_id)

# Run the app
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
