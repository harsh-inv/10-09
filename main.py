# main.py - Complete FastAPI Web API for Data Quality Checker
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import os
import uuid
import tempfile
import sqlite3
import csv
import json
import re
from datetime import datetime
import logging
import sys
import shutil
import io

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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# CLASSES FROM org_1_2907.py - INTEGRATED DIRECTLY
# ============================================================================

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class DataMaskingManager:
    def __init__(self):
        self.table_mapping = {}
        self.column_mapping = {}
        self.reverse_table_mapping = {}
        self.reverse_column_mapping = {}

    def mask_table_name(self, original_name: str) -> str:
        if original_name not in self.table_mapping:
            masked_name = f"table_{len(self.table_mapping) + 1}"
            self.table_mapping[original_name] = masked_name
            self.reverse_table_mapping[masked_name] = original_name
        return self.table_mapping[original_name]

class DataQualityChecker:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.checks_config = {}
        self.system_codes_config = {}

    def load_checks_config(self, csv_file_path: str) -> bool:
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table_name = row['table_name'].strip()
                    field_name = row['field_name'].strip()
                    
                    if table_name not in self.checks_config:
                        self.checks_config[table_name] = {}
                    
                    self.checks_config[table_name][field_name] = {
                        'description': row['description'],
                        'special_characters_check': row['special_characters_check'] == '1',
                        'null_check': row['null_check'] == '1',
                        'blank_check': row['blank_check'] == '1',
                        'max_value_check': row['max_value_check'] == '1',
                        'min_value_check': row['min_value_check'] == '1',
                        'max_count_check': row['max_count_check'] == '1',
                        'email_check': row['email_check'] == '1',
                        'numeric_check': row['numeric_check'] == '1',
                        'system_codes_check': row['system_codes_check'] == '1',
                        'language_check': row['language_check'] == '1',
                        'phone_number_check': row['phone_number_check'] == '1',
                        'duplicate_check': row['duplicate_check'] == '1',
                        'date_check': row['date_check'] == '1'
                    }
            return True
        except Exception as e:
            logger.error(f"Error loading checks configuration: {str(e)}")
            return False

    def load_system_codes_config(self, csv_file_path: str) -> bool:
        try:
            self.system_codes_config = {}
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table_name = row['table_name'].strip()
                    field_name = row['field_name'].strip()
                    valid_codes_str = row['valid_codes']
                    
                    valid_codes = [code.strip() for code in valid_codes_str.split(',') if code.strip()]
                    
                    if table_name not in self.system_codes_config:
                        self.system_codes_config[table_name] = {}
                    
                    self.system_codes_config[table_name][field_name] = valid_codes
            return True
        except Exception as e:
            logger.error(f"Error loading system codes configuration: {str(e)}")
            return False

    def _table_exists(self, table_name: str) -> bool:
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            return cursor.fetchone() is not None
        except sqlite3.Error:
            return False

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            return column_name in columns
        except sqlite3.Error:
            return False

    def _is_numeric(self, value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def _is_valid_email(self, email: str) -> bool:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_pattern, email) is not None

    def _is_valid_phone(self, phone: str) -> bool:
        cleaned_phone = re.sub(r'[^\d+]', '', phone)
        if len(cleaned_phone) < 10 or len(cleaned_phone) > 15:
            return False
        phone_pattern = r'^\+?[1-9]\d{9,14}$'
        return re.match(phone_pattern, cleaned_phone) is not None

    def _is_valid_date(self, date_str: str) -> bool:
        date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S',
            '%m-%d-%Y', '%d-%m-%Y', '%Y/%m/%d', '%d.%m.%Y',
            '%Y', '%m/%Y', '%Y-%m'
        ]
        for fmt in date_formats:
            try:
                datetime.strptime(str(date_str), fmt)
                return True
            except ValueError:
                continue
        return False

    def _has_special_characters(self, text: str) -> bool:
        allowed_pattern = r'^[a-zA-Z0-9\s.,@_-]+$'
        return not re.match(allowed_pattern, text)

    def _has_non_ascii_characters(self, text: str) -> bool:
        try:
            text.encode('ascii')
            return False
        except UnicodeEncodeError:
            return True

    def _get_valid_system_codes(self, table_name: str, field_name: str) -> List[str]:
        return self.system_codes_config.get(table_name, {}).get(field_name, [])

    def _run_field_checks(self, table_name: str, field_name: str, checks: Dict) -> List[Dict]:
        results = []
        
        if not self._column_exists(table_name, field_name):
            results.append({
                'table': table_name,
                'field': field_name,
                'check_type': 'column_existence',
                'status': 'FAIL',
                'message': f"Column '{field_name}' does not exist in table '{table_name}'"
            })
            return results

        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            total_rows = cursor.fetchone()[0]

            if total_rows == 0:
                results.append({
                    'table': table_name,
                    'field': field_name,
                    'check_type': 'data_existence',
                    'status': 'WARNING',
                    'message': f"Table '{table_name}' has no data"
                })
                return results

            # Null check
            if checks.get('null_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{field_name}] IS NULL")
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'null_check',
                        'status': 'FAIL',
                        'message': f"Found {null_count} NULL values out of {total_rows} total rows"
                    })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'null_check',
                        'status': 'PASS',
                        'message': f"No NULL values found"
                    })

            # Blank check
            if checks.get('blank_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{field_name}] = ''")
                blank_count = cursor.fetchone()[0]
                
                if blank_count > 0:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'blank_check',
                        'status': 'FAIL',
                        'message': f"Found {blank_count} blank values out of {total_rows} total rows"
                    })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'blank_check',
                        'status': 'PASS',
                        'message': f"No blank values found"
                    })

            # Email check
            if checks.get('email_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{field_name}] IS NOT NULL AND [{field_name}] != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT [{field_name}] FROM [{table_name}] WHERE [{field_name}] IS NOT NULL AND [{field_name}] != '' LIMIT 100")
                    values = cursor.fetchall()
                    invalid_emails = []
                    
                    for value in values:
                        email = str(value[0]).strip()
                        if not self._is_valid_email(email):
                            invalid_emails.append(email)
                    
                    if invalid_emails:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'email_check',
                            'status': 'FAIL',
                            'message': f"Found {len(invalid_emails)} invalid email formats out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'email_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} email formats appear valid"
                        })

            # System codes check
            if checks.get('system_codes_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{field_name}] IS NOT NULL AND [{field_name}] != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT DISTINCT [{field_name}] FROM [{table_name}] WHERE [{field_name}] IS NOT NULL AND [{field_name}] != '' LIMIT 100")
                    values = cursor.fetchall()
                    
                    valid_codes_list = self._get_valid_system_codes(table_name, field_name)
                    invalid_system_codes = []
                    
                    for value in values:
                        code = str(value[0]).strip().upper()
                        valid_codes_upper = [vc.upper() for vc in valid_codes_list] if valid_codes_list else []
                        
                        if valid_codes_list and code not in valid_codes_upper:
                            invalid_system_codes.append(str(value[0]).strip())
                    
                    if invalid_system_codes:
                        message = f"Found {len(invalid_system_codes)} invalid system codes out of {non_null_count} values"
                        if valid_codes_list:
                            message += f" (Valid codes: {len(valid_codes_list)} defined)"
                        
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'system_codes_check',
                            'status': 'FAIL',
                            'message': message
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'system_codes_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} values are valid system codes"
                        })

            # Duplicate check
            if checks.get('duplicate_check', False):
                cursor.execute(f"""
                    SELECT [{field_name}], COUNT(*) as count
                    FROM [{table_name}]
                    WHERE [{field_name}] IS NOT NULL
                    GROUP BY [{field_name}]
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                """)
                duplicates = cursor.fetchall()
                
                if duplicates:
                    total_duplicate_count = sum(count - 1 for _, count in duplicates)
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'duplicate_check',
                        'status': 'FAIL',
                        'message': f"Found {total_duplicate_count} duplicate values across {len(duplicates)} distinct values"
                    })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'duplicate_check',
                        'status': 'PASS',
                        'message': f"No duplicate values found"
                    })

        except sqlite3.Error as e:
            results.append({
                'table': table_name,
                'field': field_name,
                'check_type': 'database_error',
                'status': 'ERROR',
                'message': f"Database error: {str(e)}"
            })

        return results

    def run_all_checks(self) -> Dict[str, List[Dict]]:
        if not self.checks_config:
            return {}

        results = {}
        for table_name, fields in self.checks_config.items():
            if not self._table_exists(table_name):
                continue
                
            table_results = []
            for field_name, checks in fields.items():
                field_results = self._run_field_checks(table_name, field_name, checks)
                if field_results:
                    table_results.extend(field_results)
            
            if table_results:
                results[table_name] = table_results

        return results

    def run_checks_for_specific_table(self, table_name: str) -> Dict[str, List[Dict]]:
        if table_name not in self.checks_config:
            return {}
        
        if not self._table_exists(table_name):
            return {}

        table_results = []
        fields = self.checks_config[table_name]
        
        for field_name, checks in fields.items():
            field_results = self._run_field_checks(table_name, field_name, checks)
            if field_results:
                table_results.extend(field_results)

        if table_results:
            return {table_name: table_results}
        else:
            return {}

class ResultsManager:
    def __init__(self):
        self.results_db_path = "Results.db"
        self.results_connection = None
        self._initialize_results_db()

    def _initialize_results_db(self):
        try:
            self.results_connection = sqlite3.connect(self.results_db_path)
            cursor = self.results_connection.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    execution_date TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    original_query TEXT NOT NULL,
                    row_count INTEGER,
                    column_count INTEGER,
                    description TEXT,
                    created_timestamp TEXT NOT NULL,
                    UNIQUE(table_name, version)
                )
            ''')
            self.results_connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing Results database: {str(e)}")

    def close(self):
        if self.results_connection:
            self.results_connection.close()

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

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

# ============================================================================
# SESSION MANAGER
# ============================================================================

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
            if session.get('db_connection'):
                session['db_connection'].close()
            if session.get('results_manager'):
                session['results_manager'].close()
            if session.get('temp_dir') and os.path.exists(session['temp_dir']):
                shutil.rmtree(session['temp_dir'], ignore_errors=True)
            del self.sessions[session_id]
            logger.info(f"Cleaned up session: {session_id}")

session_manager = SessionManager()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def save_uploaded_file(upload_file: UploadFile, session_dir: str, file_type: str) -> str:
    file_path = os.path.join(session_dir, f"{file_type}_{upload_file.filename}")
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    return file_path

def validate_csv_structure(file_path: str, expected_columns: List[str]) -> bool:
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

# ============================================================================
# API ENDPOINTS
# ============================================================================

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
    session_id = session_manager.create_session()
    return SessionResponse(
        session_id=session_id,
        message="Session created successfully",
        status="created"
    )

@app.get("/sessions/{session_id}/status", response_model=SessionStatus)
async def get_session_status(session_id: str):
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
    session = session_manager.get_session(session_id)
    
    if not database.filename.lower().endswith(('.db', '.sqlite', '.sqlite3')):
        raise HTTPException(status_code=400, detail="Database file must be .db, .sqlite, or .sqlite3")
    
    try:
        db_path = save_uploaded_file(database, session['temp_dir'], "database")
        tables = get_database_tables(db_path)
        
        if not tables:
            raise HTTPException(status_code=400, detail="Invalid database file or no tables found")
        
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        
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
            "table_names": tables[:10],
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
    session = session_manager.get_session(session_id)
    
    if not session.get('checker'):
        raise HTTPException(status_code=400, detail="Database must be uploaded first")
    
    try:
        if not data_quality_config.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Data quality config must be a CSV file")
        
        config_path = save_uploaded_file(data_quality_config, session['temp_dir'], "data_quality_config")
        
        expected_columns = ['table_name', 'field_name', 'description', 'null_check', 'blank_check']
        if not validate_csv_structure(config_path, expected_columns):
            raise HTTPException(status_code=400, detail="Invalid data quality config CSV structure")
        
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
        
        if system_codes_config:
            if not system_codes_config.filename.lower().endswith('.csv'):
                raise HTTPException(status_code=400, detail="System codes config must be a CSV file")
            
            system_codes_path = save_uploaded_file(system_codes_config, session['temp_dir'], "system_codes_config")
            
            system_codes_columns = ['table_name', 'field_name', 'valid_codes']
            if not validate_csv_structure(system_codes_path, system_codes_columns):
                raise HTTPException(status_code=400, detail="Invalid system codes config CSV structure")
            
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
    session = session_manager.get_session(session_id)
    
    if not session.get('checker'):
        raise HTTPException(status_code=400, detail="Database and configuration must be uploaded first")
    
    if not session['checker'].checks_config:
        raise HTTPException(status_code=400, detail="Data quality configuration not loaded")
    
    try:
        if specific_table:
            results = session['checker'].run_checks_for_specific_table(specific_table)
        else:
            results = session['checker'].run_all_checks()
        
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
    session = session_manager.get_session(session_id)
    
    if not session.get('results'):
        raise HTTPException(status_code=404, detail="No results found. Run checks first.")
    
    if format.lower() not in ['csv', 'json']:
        raise HTTPException(status_code=400, detail="Format must be 'csv' or 'json'")
    
    if check_type.lower() not in ['all', 'failed', 'passed']:
        raise HTTPException(status_code=400, detail="Check type must be 'all', 'failed', or 'passed'")
    
    try:
        results = session['results']
        
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
        
        else:
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

# ============================================================================
# ERROR HANDLERS & STARTUP/SHUTDOWN
# ============================================================================

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

@app.on_event("startup")
async def startup_event():
    logger.info("Data Quality Checker API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Data Quality Checker API shutting down...")
    for session_id in list(session_manager.sessions.keys()):
        session_manager.cleanup_session(session_id)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
