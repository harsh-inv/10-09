from fastapi import FastAPI, File, UploadFile, HTTPException, Form
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
import sys
import shutil

# Import your DataQualityChecker from org_1_2907.py
try:
    from org_1_2907 import DataQualityChecker, ResultsManager
except ImportError:
    # Fallback if import fails
    DataQualityChecker = None
    ResultsManager = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Northwind Data Quality Checker API",
    description="Upload Northwind database and configuration files to run quality checks",
    version="2.0.0",
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
    northwind_db_uploaded: bool
    all_files_uploaded: bool
    ready_to_run_checks: bool
    has_results: bool
    created_at: str
    files_info: Dict

class ChecksRunResponse(BaseModel):
    session_id: str
    status: str
    message: str
    checks_run: int
    passed: int
    failed: int
    warnings: int

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

session_manager = SessionManager()

# ==================== ENDPOINTS ====================

@app.get("/")
async def root():
    """GET: API information and workflow for Northwind database"""
    return {
        "message": "Northwind Data Quality Checker API - Live on Render",
        "version": "2.0.0",
        "status": "running",
        "workflow": [
            "1. POST /session/create - Create new session",
            "2. POST /files/upload/northwind-db/{session_id} - Upload Northwind.db file",
            "3. POST /files/upload/data-quality-config/{session_id} - Upload data_quality_config.csv",
            "4. POST /files/upload/system-codes-config/{session_id} - Upload Sys_codes.csv",
            "5. GET /session/status/{session_id} - Check session status",
            "6. POST /checks/run - Run quality checks on Northwind database",
            "7. GET /checks/results/{session_id} - Get detailed results",
            "8. GET /files/download/results-csv/{session_id} - Download CSV report"
        ],
        "expected_files": [
            "Northwind.db (SQLite database)",
            "data_quality_config.csv (Quality check configuration)",
            "Sys_codes.csv (System codes validation)"
        ],
        "active_sessions": len(session_manager.sessions)
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(session_manager.sessions)
    }

@app.post("/session/create", response_model=SessionResponse)
async def create_session():
    """POST: Create a new session for Northwind data quality checking"""
    try:
        session_id = session_manager.create_session()
        return SessionResponse(
            session_id=session_id,
            message="Session created successfully. Upload Northwind database and configuration files.",
            status="created"
        )
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating session: {str(e)}")

@app.post("/files/upload/northwind-db/{session_id}", response_model=FileUploadResponse)
async def upload_northwind_database(
    session_id: str,
    file: UploadFile = File(...)
):
    """POST: Upload Northwind SQLite database file"""
    try:
        session = session_manager.get_session(session_id)
        
        # Validation
        if not file or not file.filename:
            raise HTTPException(status_code=422, detail="No database file provided")
            
        if not file.filename.lower().endswith('.db'):
            raise HTTPException(status_code=422, detail="File must be a SQLite database with .db extension")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=422, detail="Uploaded database file is empty")
        
        # Save database file
        db_path = os.path.join(session['temp_dir'], "northwind.db")
        with open(db_path, 'wb') as f:
            f.write(content)
        
        # Verify it's a valid SQLite database
        try:
            test_conn = sqlite3.connect(db_path)
            cursor = test_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            test_conn.close()
            
            if not tables:
                raise HTTPException(status_code=422, detail="Database file contains no tables")
                
        except sqlite3.Error as e:
            raise HTTPException(status_code=422, detail=f"Invalid SQLite database: {str(e)}")
        
        session['northwind_db_path'] = db_path
        session['files_info']['northwind_db'] = {
            'filename': file.filename,
            'size': len(content),
            'tables_count': len(tables)
        }
        
        logger.info(f"Northwind database uploaded for session {session_id}: {file.filename}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="northwind_database",
            message=f"Northwind database uploaded successfully. Found {len(tables)} tables.",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading Northwind database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/files/upload/data-quality-config/{session_id}", response_model=FileUploadResponse)
async def upload_data_quality_config(
    session_id: str,
    file: UploadFile = File(...)
):
    """POST: Upload data quality configuration CSV file for Northwind"""
    try:
        session = session_manager.get_session(session_id)
        
        # Validation
        if not file or not file.filename:
            raise HTTPException(status_code=422, detail="No configuration file provided")
            
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=422, detail="File must be a CSV file with .csv extension")
        
        content = await file.read()
        if not content:
            raise HTTPException(status_code=422, detail="Uploaded file is empty")
        
        # Validate CSV structure for Northwind
        try:
            content_str = content.decode('utf-8')
            csv_reader = csv.DictReader(content_str.splitlines())
            required_columns = [
                'table_name', 'field_name', 'description', 'null_check', 
                'blank_check', 'email_check', 'numeric_check', 'duplicate_check',
                'special_characters_check', 'system_codes_check', 'language_check',
                'phone_number_check', 'date_check', 'max_value_check', 'min_value_check',
                'max_count_check'
            ]
            
            if not csv_reader.fieldnames:
                raise HTTPException(status_code=422, detail="CSV file has no headers")
            
            missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
            if missing_cols:
                raise HTTPException(
                    status_code=422, 
                    detail=f"CSV missing required columns: {', '.join(missing_cols)}"
                )
                
            # Count configured tables for Northwind
            tables_configured = set()
            row_count = 0
            for row in csv_reader:
                tables_configured.add(row['table_name'])
                row_count += 1
                
        except UnicodeDecodeError:
            raise HTTPException(status_code=422, detail="File must be UTF-8 encoded CSV")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], "data_quality_config.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        session['data_quality_config_path'] = file_path
        session['files_info']['data_quality_config'] = {
            'filename': file.filename,
            'size': len(content),
            'tables_configured': len(tables_configured),
            'checks_count': row_count
        }
        
        logger.info(f"Data quality config uploaded for session {session_id}: {file.filename}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="data_quality_config",
            message=f"Data quality configuration uploaded successfully. Configured {len(tables_configured)} tables with {row_count} field checks.",
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
    """POST: Upload system codes configuration CSV file (Sys_codes.csv)"""
    try:
        session = session_manager.get_session(session_id)
        
        # Validation
        if not file or not file.filename:
            raise HTTPException(status_code=422, detail="No system codes file provided")
            
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
            
            missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
            if missing_cols:
                raise HTTPException(
                    status_code=422, 
                    detail=f"System codes CSV missing required columns: {', '.join(missing_cols)}"
                )
            
            # Count system codes configurations
            codes_count = 0
            for row in csv_reader:
                if row['valid_codes']:
                    codes_count += len(row['valid_codes'].split(','))
                    
        except UnicodeDecodeError:
            raise HTTPException(status_code=422, detail="File must be UTF-8 encoded CSV")
        
        # Save file
        file_path = os.path.join(session['temp_dir'], "system_codes_config.csv")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        session['system_codes_config_path'] = file_path
        session['files_info']['system_codes_config'] = {
            'filename': file.filename,
            'size': len(content),
            'total_codes': codes_count
        }
        
        logger.info(f"System codes config uploaded for session {session_id}: {file.filename}")
        
        return FileUploadResponse(
            session_id=session_id,
            filename=file.filename,
            file_type="system_codes_config",
            message=f"System codes configuration uploaded successfully. Loaded {codes_count} validation codes.",
            status="uploaded"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading system codes config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/session/status/{session_id}", response_model=SessionStatus)
async def get_session_status(session_id: str):
    """GET: Get current session status for Northwind processing"""
    try:
        session = session_manager.get_session(session_id)
        
        data_quality_uploaded = session['data_quality_config_path'] is not None
        system_codes_uploaded = session['system_codes_config_path'] is not None
        northwind_db_uploaded = session['northwind_db_path'] is not None
        all_files_uploaded = data_quality_uploaded and system_codes_uploaded and northwind_db_uploaded
        ready_to_run = all_files_uploaded
        has_results = session['results'] is not None
        
        return SessionStatus(
            session_id=session_id,
            data_quality_config_uploaded=data_quality_uploaded,
            system_codes_config_uploaded=system_codes_uploaded,
            northwind_db_uploaded=northwind_db_uploaded,
            all_files_uploaded=all_files_uploaded,
            ready_to_run_checks=ready_to_run,
            has_results=has_results,
            created_at=session['created_at'].isoformat(),
            files_info=session['files_info']
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting session status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting session status: {str(e)}")

@app.post("/checks/run", response_model=ChecksRunResponse)
async def run_quality_checks(session_id: str = Form(...)):
    """POST: Run data quality checks on Northwind database"""
    try:
        session = session_manager.get_session(session_id)
        
        # Verify all files are uploaded
        if not session['northwind_db_path']:
            raise HTTPException(status_code=400, detail="Northwind database not uploaded")
        if not session['data_quality_config_path']:
            raise HTTPException(status_code=400, detail="Data quality configuration not uploaded")
        if not session['system_codes_config_path']:
            raise HTTPException(status_code=400, detail="System codes configuration not uploaded")
        
        # Initialize database connection and quality checker
        if DataQualityChecker is None:
            raise HTTPException(status_code=500, detail="DataQualityChecker not available")
        
        try:
            # Connect to uploaded Northwind database
            db_connection = sqlite3.connect(session['northwind_db_path'])
            db_connection.row_factory = sqlite3.Row
            
            # Initialize quality checker and results manager
            quality_checker = DataQualityChecker(db_connection)
            results_manager = ResultsManager() if ResultsManager else None
            
            # Load configurations
            config_loaded = quality_checker.load_checks_config(session['data_quality_config_path'])
            if not config_loaded:
                raise HTTPException(status_code=500, detail="Failed to load data quality configuration")
            
            codes_loaded = quality_checker.load_system_codes_config(session['system_codes_config_path'])
            if not codes_loaded:
                raise HTTPException(status_code=500, detail="Failed to load system codes configuration")
            
            # Run quality checks
            results = quality_checker.run_all_checks()
            
            if not results:
                return ChecksRunResponse(
                    session_id=session_id,
                    status="completed",
                    message="No quality checks were executed (no matching tables/fields found)",
                    checks_run=0,
                    passed=0,
                    failed=0,
                    warnings=0
                )
            
            # Calculate statistics
            total_checks = 0
            passed = 0
            failed = 0
            warnings = 0
            
            for table_name, table_results in results.items():
                for result in table_results:
                    total_checks += 1
                    if result['status'] == 'PASS':
                        passed += 1
                    elif result['status'] == 'FAIL':
                        failed += 1
                    elif result['status'] == 'WARNING':
                        warnings += 1
            
            # Store results in session
            session['results'] = results
            session['db_connection'] = db_connection
            session['checker'] = quality_checker
            session['results_manager'] = results_manager
            
            return ChecksRunResponse(
                session_id=session_id,
                status="completed",
                message=f"Quality checks completed successfully on Northwind database",
                checks_run=total_checks,
                passed=passed,
                failed=failed,
                warnings=warnings
            )
            
        except Exception as db_error:
            raise HTTPException(status_code=500, detail=f"Database processing error: {str(db_error)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running quality checks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running checks: {str(e)}")

@app.get("/checks/results/{session_id}")
async def get_check_results(session_id: str):
    """GET: Get detailed quality check results"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session.get('results'):
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        results = session['results']
        
        # Calculate detailed statistics
        total_checks = 0
        passed = 0
        failed = 0
        warnings = 0
        table_summary = {}
        
        for table_name, table_results in results.items():
            table_stats = {'total': 0, 'passed': 0, 'failed': 0, 'warnings': 0}
            
            for result in table_results:
                total_checks += 1
                table_stats['total'] += 1
                
                if result['status'] == 'PASS':
                    passed += 1
                    table_stats['passed'] += 1
                elif result['status'] == 'FAIL':
                    failed += 1
                    table_stats['failed'] += 1
                elif result['status'] == 'WARNING':
                    warnings += 1
                    table_stats['warnings'] += 1
            
            table_summary[table_name] = table_stats
        
        return {
            "session_id": session_id,
            "status": "available",
            "results": results,
            "summary": {
                "total_checks": total_checks,
                "passed": passed,
                "failed": failed,
                "warnings": warnings,
                "tables_processed": len(results),
                "table_summary": table_summary
            },
            "northwind_info": session['files_info']
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting results: {str(e)}")

@app.get("/files/download/results-csv/{session_id}")
async def download_results_csv(session_id: str):
    """GET: Download comprehensive results as CSV file"""
    try:
        session = session_manager.get_session(session_id)
        
        if not session.get('results'):
            raise HTTPException(status_code=404, detail="No results available. Run checks first.")
        
        # Create comprehensive CSV file
        csv_filename = f"northwind_quality_results_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        csv_path = os.path.join(session['temp_dir'], csv_filename)
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'table_name', 'field_name', 'check_type', 'status', 
                'message', 'timestamp', 'session_id'
            ])
            
            for table_name, table_results in session['results'].items():
                for result in table_results:
                    writer.writerow([
                        result.get('table', table_name),
                        result.get('field', 'unknown'),
                        result.get('check_type', 'unknown'),
                        result.get('status', 'unknown'),
                        result.get('message', 'No message'),
                        datetime.now().isoformat(),
                        session_id
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
