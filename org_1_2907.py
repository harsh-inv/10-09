import sqlite3
import csv
import re
from typing import Dict, List, Optional
from datetime import datetime
import statistics
import os
import pandas as pd
import requests
import sys
import json

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
        self.table_mapping = {}  # original_name -> masked_name
        self.column_mapping = {}  # table_name -> {original_col -> masked_col}
        self.reverse_table_mapping = {}  # masked_name -> original_name
        self.reverse_column_mapping = {}  # table_name -> {masked_col -> original_col}
        
    def mask_table_name(self, original_name: str) -> str:
        """Convert original table name to masked version"""
        if original_name not in self.table_mapping:
            masked_name = f"table_{len(self.table_mapping) + 1}"
            self.table_mapping[original_name] = masked_name
            self.reverse_table_mapping[masked_name] = original_name
        return self.table_mapping[original_name]
    
    def mask_column_name(self, table_name: str, original_col: str) -> str:
        """Convert original column name to masked version"""
        if table_name not in self.column_mapping:
            self.column_mapping[table_name] = {}
            self.reverse_column_mapping[table_name] = {}
        
        if original_col not in self.column_mapping[table_name]:
            masked_col = f"col_{len(self.column_mapping[table_name]) + 1}"
            self.column_mapping[table_name][original_col] = masked_col
            self.reverse_column_mapping[table_name][masked_col] = original_col
        
        return self.column_mapping[table_name][original_col]
    
    def unmask_table_name(self, masked_name: str) -> str:
        """Convert masked table name back to original"""
        return self.reverse_table_mapping.get(masked_name, masked_name)
    
    def unmask_column_name(self, table_name: str, masked_col: str) -> str:
        """Convert masked column name back to original"""
        original_table = self.unmask_table_name(table_name)
        if original_table in self.reverse_column_mapping:
            return self.reverse_column_mapping[original_table].get(masked_col, masked_col)
        return masked_col
    def unmask_sql_query(self, masked_query: str) -> str:
        """Convert masked SQL query back to original names with better error handling"""
        unmasked_query = masked_query
        
        try:
            # Unmask table names first
            for masked_table, original_table in self.reverse_table_mapping.items():
                pattern = r'\b' + re.escape(masked_table) + r'\b'
                unmasked_query = re.sub(pattern, original_table, unmasked_query, flags=re.IGNORECASE)
            
            # Unmask column names - FIXED: iterate through all tables
            for original_table, col_mapping in self.column_mapping.items():
                for original_col, masked_col in col_mapping.items():
                    # Use word boundary to avoid partial matches
                    col_pattern = r'\b' + re.escape(masked_col) + r'\b'
                    unmasked_query = re.sub(col_pattern, original_col, unmasked_query, flags=re.IGNORECASE)
            
            return unmasked_query
            
        except Exception as e:
            print(f"{Colors.WARNING}Warning: Error during unmasking: {str(e)}{Colors.ENDC}")
            print(f"{Colors.WARNING}Returning original masked query{Colors.ENDC}")
            return masked_query
    def mask_user_query(self, user_query: str, schema_info: str) -> str:
        """Mask table and column names in user query"""
        masked_query = user_query
        
        # Extract table names from schema and mask them in query
        for original_table in self.table_mapping.keys():
            masked_table = self.table_mapping[original_table]
            # Replace table names (case insensitive)
            import re
            pattern = r'\b' + re.escape(original_table) + r'\b'
            masked_query = re.sub(pattern, masked_table, masked_query, flags=re.IGNORECASE)
            
            # Mask column names for this table
            if original_table in self.column_mapping:
                for original_col, masked_col in self.column_mapping[original_table].items():
                    col_pattern = r'\b' + re.escape(original_col) + r'\b'
                    masked_query = re.sub(col_pattern, masked_col, masked_query, flags=re.IGNORECASE)
        
        return masked_query
    
    def mask_schema_info(self, schema_info: str) -> str:
        """Mask table and column names in schema information"""
        masked_schema = schema_info
        
        for original_table in self.table_mapping.keys():
            masked_table = self.table_mapping[original_table]
            masked_schema = masked_schema.replace(f"Table: {original_table}", f"Table: {masked_table}")
            
            if original_table in self.column_mapping:
                for original_col, masked_col in self.column_mapping[original_table].items():
                    masked_schema = masked_schema.replace(original_col, masked_col)
        
        return masked_schema
    
    def unmask_sql_query(self, masked_query: str) -> str:
        """Convert masked SQL query back to original names"""
        unmasked_query = masked_query
        
        # Unmask table names
        for masked_table, original_table in self.reverse_table_mapping.items():
            import re
            pattern = r'\b' + re.escape(masked_table) + r'\b'
            unmasked_query = re.sub(pattern, original_table, unmasked_query, flags=re.IGNORECASE)
        
        # Unmask column names
        for table_name, col_mapping in self.reverse_column_mapping.items():
            for masked_col, original_col in col_mapping.items():
                col_pattern = r'\b' + re.escape(masked_col) + r'\b'
                unmasked_query = re.sub(col_pattern, original_col, unmasked_query, flags=re.IGNORECASE)
        
        return unmasked_query

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
                    table_name = row['table_name']
                    field_name = row['field_name']
                    
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
            
            print(f"✓ Data quality checks configuration loaded successfully")
            print(f"Tables configured: {list(self.checks_config.keys())}")
            return True
        except Exception as e:
            print(f"Error loading checks configuration: {str(e)}")
            return False

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
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
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
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NULL")
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
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} = ''")
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
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
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

            # Phone number check
            if checks.get('phone_number_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    invalid_phones = []
                    for value in values:
                        phone = str(value[0]).strip()
                        if not self._is_valid_phone(phone):
                            invalid_phones.append(phone)
                    
                    if invalid_phones:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'phone_number_check',
                            'status': 'FAIL',
                            'message': f"Found {len(invalid_phones)} invalid phone numbers out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'phone_number_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} phone numbers appear valid"
                        })

            # Date check
            if checks.get('date_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    invalid_dates = []
                    for value in values:
                        date_str = str(value[0]).strip()
                        if not self._is_valid_date(date_str):
                            invalid_dates.append(date_str)
                    
                    if invalid_dates:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'date_check',
                            'status': 'FAIL',
                            'message': f"Found {len(invalid_dates)} invalid date formats out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'date_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} date formats appear valid"
                        })

            # Numeric check
            if checks.get('numeric_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    non_numeric_values = []
                    for value in values:
                        val_str = str(value[0]).strip()
                        if not self._is_numeric(val_str):
                            non_numeric_values.append(val_str)
                    
                    if non_numeric_values:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'numeric_check',
                            'status': 'FAIL',
                            'message': f"Found {len(non_numeric_values)} non-numeric values out of {non_null_count} non-null values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'numeric_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} values are numeric"
                        })

            # Duplicate check
            if checks.get('duplicate_check', False):
                cursor.execute(f"""
                    SELECT {field_name}, COUNT(*) as count
                    FROM {table_name}
                    WHERE {field_name} IS NOT NULL
                    GROUP BY {field_name}
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

            # Special characters check
            if checks.get('special_characters_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    special_char_values = []
                    for value in values:
                        text = str(value[0]).strip()
                        if self._has_special_characters(text):
                            special_char_values.append(text)
                    
                    if special_char_values:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'special_characters_check',
                            'status': 'FAIL',
                            'message': f"Found {len(special_char_values)} values with special characters out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'special_characters_check',
                            'status': 'PASS',
                            'message': f"No special characters found"
                        })

            if checks.get('system_codes_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    # Get predefined valid codes for this table/field
                    valid_codes_list = self._get_valid_system_codes(table_name, field_name)
                    invalid_system_codes = []
                    
                    for value in values:
                        code = str(value[0]).strip().upper()
                        # Convert valid codes to uppercase for comparison
                        valid_codes_upper = [vc.upper() for vc in valid_codes_list] if valid_codes_list else []
                        
                        if valid_codes_list and code not in valid_codes_upper:
                            invalid_system_codes.append(str(value[0]).strip())  # Keep original case for display
                        elif not valid_codes_list and not self._looks_like_system_code(code):
                            invalid_system_codes.append(str(value[0]).strip())
                    
                    if invalid_system_codes:
                        if valid_codes_list:
                            message = f"Found {len(invalid_system_codes)} invalid system codes out of {non_null_count} values"
                            message += f" (Valid codes: {len(valid_codes_list)} defined)"
                        else:
                            message = f"Found {len(invalid_system_codes)} values that don't match system code patterns out of {non_null_count} values"
                        
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'system_codes_check',
                            'status': 'FAIL',
                            'message': message
                        })
                    else:
                        if valid_codes_list:
                            results.append({
                                'table': table_name,
                                'field': field_name,
                                'check_type': 'system_codes_check',
                                'status': 'PASS',
                                'message': f"All {non_null_count} values are valid system codes from external config ({len(valid_codes_list)} codes)"
                            })
                        else:
                            results.append({
                                'table': table_name,
                                'field': field_name,
                                'check_type': 'system_codes_check',
                                'status': 'PASS',
                                'message': f"All {non_null_count} values match system code patterns"
                            })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'system_codes_check',
                        'status': 'WARNING',
                        'message': f"No data found to check system codes"
                    })


            # Language check (non-ASCII characters)
            if checks.get('language_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    
                    non_ascii_values = []
                    for value in values:
                        text = str(value[0]).strip()
                        if self._has_non_ascii_characters(text):
                            non_ascii_values.append(text)
                    
                    if non_ascii_values:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'language_check',
                            'status': 'FAIL',
                            'message': f"Found {len(non_ascii_values)} values with non-ASCII characters out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'language_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} values contain only ASCII characters"
                        })

            # Max count check
            if checks.get('max_count_check', False):
                cursor.execute(f"""
                    SELECT {field_name}, COUNT(*) as count
                    FROM {table_name}
                    WHERE {field_name} IS NOT NULL AND {field_name} != ''
                    GROUP BY {field_name}
                    ORDER BY count DESC
                    LIMIT 1
                """)
                max_count_result = cursor.fetchone()
                
                if max_count_result:
                    max_value, max_count = max_count_result
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'max_count_check',
                        'status': 'INFO',
                        'message': f"Most frequent value: '{max_value}' appears {max_count} times"
                    })

                if checks['max_value_check']:
                                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                                non_null_count = cursor.fetchone()[0]
                                
                                if non_null_count > 0:
                                    cursor.execute(f"""
                                        SELECT {field_name} FROM {table_name} 
                                        WHERE {field_name} IS NOT NULL AND {field_name} != ''
                                    """)
                                    values = cursor.fetchall()
                                    
                                    numeric_values = []
                                    text_values = []
                                    
                                    # Separate numeric and text values
                                    for value in values:
                                        val_str = str(value[0]).strip()
                                        if self._is_numeric(val_str):
                                            numeric_values.append(float(val_str))
                                        else:
                                            text_values.append(val_str)
                                    
                                    # Handle numeric values
                                    if numeric_values:
                                        max_numeric = max(numeric_values)
                                        avg_numeric = sum(numeric_values) / len(numeric_values)
                                        
                                        if max_numeric > avg_numeric * 10:
                                            results.append({
                                                'table': table_name,
                                                'field': field_name,
                                                'check_type': 'max_value_check',
                                                'status': 'WARNING',
                                                'message': f"Max numeric value {max_numeric} is significantly higher than average {avg_numeric:.2f} (potential outlier)"
                                            })
                                        else:
                                            results.append({
                                                'table': table_name,
                                                'field': field_name,
                                                'check_type': 'max_value_check',
                                                'status': 'PASS',
                                                'message': f"Max numeric value {max_numeric} appears reasonable (avg: {avg_numeric:.2f})"
                                            })
                                    
                                    # Handle text values - find alphabetically last value
                                    if text_values:
                                        max_text = max(text_values, key=str.lower)  # Case-insensitive sorting
                                        unique_text_count = len(set(text_values))
                                        
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'max_value_check',
                                            'status': 'INFO',
                                            'message': f"Alphabetically last text value: '{max_text}' (found {len(text_values)} text values, {unique_text_count} unique)"
                                        })
                                    
                                    # Summary message
                                    if numeric_values and text_values:
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'max_value_check',
                                            'status': 'INFO',
                                            'message': f"Field contains mixed data types: {len(numeric_values)} numeric, {len(text_values)} text values"
                                        })
                                    elif not numeric_values and not text_values:
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'max_value_check',
                                            'status': 'WARNING',
                                            'message': f"No valid values found for max value analysis"
                                        })
                            
                if checks['min_value_check']:
                                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                                non_null_count = cursor.fetchone()[0]
                                
                                if non_null_count > 0:
                                    cursor.execute(f"""
                                        SELECT {field_name} FROM {table_name} 
                                        WHERE {field_name} IS NOT NULL AND {field_name} != ''
                                    """)
                                    values = cursor.fetchall()
                                    
                                    numeric_values = []
                                    text_values = []
                                    
                                    # Separate numeric and text values
                                    for value in values:
                                        val_str = str(value[0]).strip()
                                        if self._is_numeric(val_str):
                                            numeric_values.append(float(val_str))
                                        else:
                                            text_values.append(val_str)
                                    
                                    # Handle numeric values
                                    if numeric_values:
                                        min_numeric = min(numeric_values)
                                        avg_numeric = sum(numeric_values) / len(numeric_values)
                                        
                                        if min_numeric < 0:
                                            results.append({
                                                'table': table_name,
                                                'field': field_name,
                                                'check_type': 'min_value_check',
                                                'status': 'WARNING',
                                                'message': f"Found negative minimum value: {min_numeric}"
                                            })
                                        elif min_numeric < avg_numeric * 0.1 and avg_numeric > 0:
                                            results.append({
                                                'table': table_name,
                                                'field': field_name,
                                                'check_type': 'min_value_check',
                                                'status': 'WARNING',
                                                'message': f"Min numeric value {min_numeric} is significantly lower than average {avg_numeric:.2f} (potential outlier)"
                                            })
                                        else:
                                            results.append({
                                                'table': table_name,
                                                'field': field_name,
                                                'check_type': 'min_value_check',
                                                'status': 'PASS',
                                                'message': f"Min numeric value {min_numeric} appears reasonable (avg: {avg_numeric:.2f})"
                                            })
                                    
                                    # Handle text values - find alphabetically first value
                                    if text_values:
                                        min_text = min(text_values, key=str.lower)  # Case-insensitive sorting
                                        unique_text_count = len(set(text_values))
                                        
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'min_value_check',
                                            'status': 'INFO',
                                            'message': f"Alphabetically first text value: '{min_text}' (found {len(text_values)} text values, {unique_text_count} unique)"
                                        })
                                    
                                    # Summary message
                                    if numeric_values and text_values:
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'min_value_check',
                                            'status': 'INFO',
                                            'message': f"Field contains mixed data types: {len(numeric_values)} numeric, {len(text_values)} text values"
                                        })
                                    elif not numeric_values and not text_values:
                                        results.append({
                                            'table': table_name,
                                            'field': field_name,
                                            'check_type': 'min_value_check',
                                            'status': 'WARNING',
                                            'message': f"No valid values found for min value analysis"
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

    def load_system_codes_config(self, csv_file_path: str) -> bool:
        """Load system codes configuration from CSV file"""
        try:
            self.system_codes_config = {}
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table_name = row['table_name']
                    field_name = row['field_name']
                    valid_codes_str = row['valid_codes']
                    
                    # Parse comma-separated codes
                    valid_codes = [code.strip() for code in valid_codes_str.split(',') if code.strip()]
                    
                    if table_name not in self.system_codes_config:
                        self.system_codes_config[table_name] = {}
                    
                    self.system_codes_config[table_name][field_name] = valid_codes
            
            print(f"✓ System codes configuration loaded successfully")
            print(f"Tables with system codes: {list(self.system_codes_config.keys())}")
            
            # Show loaded codes summary
            for table_name, fields in self.system_codes_config.items():
                print(f"  {table_name}: {list(fields.keys())}")
            
            return True
            
        except Exception as e:
            print(f"Error loading system codes configuration: {str(e)}")
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

    def _looks_like_system_code(self, code: str) -> bool:
        patterns = [
            r'^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$',
            r'^[A-Z]{2,3}\d{3,}$',
            r'^\d{6,}$',
            r'^[A-Z0-9]{8,}$',
        ]
        
        for pattern in patterns:
            if re.match(pattern, code.upper()):
                return True
        return False

    def _has_non_ascii_characters(self, text: str) -> bool:
        try:
            text.encode('ascii')
            return False
        except UnicodeEncodeError:
            return True
    def _get_valid_system_codes(self, table_name: str, field_name: str) -> List[str]:
        """Get predefined valid system codes for specific table and field from external config"""
        return self.system_codes_config.get(table_name, {}).get(field_name, [])

    def export_passed_checks_to_results_db(self, results: Dict[str, List[Dict]], results_manager) -> bool:
        """Export passed data quality checks to Results database"""
        if not results:
            print(f"{Colors.WARNING}No results to export{Colors.ENDC}")
            return False
        
        # Filter only passed results
        passed_records = []
        
        for table_name, table_results in results.items():
            for result in table_results:
                if result['status'] in ['PASS', 'INFO']:
                    passed_records.append([
                        table_name,
                        result['field'],
                        result['check_type'],
                        result['status'],
                        result['message'],
                        "N/A",  # No specific failing values for passed checks
                        datetime.now().strftime("%Y-%m-%d"),  # Date column
                        datetime.now().isoformat()  # Timestamp column
                    ])
        
        if not passed_records:
            print(f"{Colors.OKBLUE}ℹ No passed checks to export{Colors.ENDC}")
            return False
        
        # Define column names
        column_names = [
            'table_name', 
            'field_name', 
            'check_type', 
            'status', 
            'message', 
            'passing_info',
            'date',
            'timestamp'
        ]
        
        # Create description
        description = f"Passed data quality checks export - {len(passed_records)} passed records"
        
        # Store in Results database with custom table name
        stored_table = results_manager.store_passed_checks_results(
            passed_records, column_names, description
        )
        
        if stored_table:
            print(f"{Colors.OKGREEN}✓ Passed checks exported to Results database: {stored_table}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Passed records stored: {len(passed_records)}{Colors.ENDC}")
            return True
        else:
            print(f"{Colors.FAIL}Error exporting passed checks to Results database{Colors.ENDC}")
            return False

    def _get_failing_values_from_db(self, table_name: str, field_name: str, check_type: str) -> List[str]:
        """Get actual failing values from database based on check type"""
        failing_values = []
        
        try:
            cursor = self.db_connection.cursor()
            
            if check_type == 'null_check':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NULL")
                count = cursor.fetchone()[0]
                failing_values = [f"NULL (found {count} occurrences)"]

            elif check_type == 'system_codes_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                
                # Get predefined valid codes for this table/field
                valid_codes_list = self._get_valid_system_codes(table_name, field_name)
                
                for row in results:
                    code = str(row[0]).strip().upper()
                    original_code = str(row[0]).strip()
                    
                    if valid_codes_list:
                        # Convert valid codes to uppercase for comparison
                        valid_codes_upper = [vc.upper() for vc in valid_codes_list]
                        if code not in valid_codes_upper:
                            failing_values.append(f"{original_code} (not in external config)")
                    else:
                        # Fallback to pattern matching if no external config
                        if not self._looks_like_system_code(code):
                            failing_values.append(f"{original_code} (pattern mismatch)")


            elif check_type == 'blank_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} = '' OR {field_name} IS NULL LIMIT 50")
                results = cursor.fetchall()
                failing_values = [str(row[0]) if row[0] is not None else "NULL" for row in results]
                
            elif check_type == 'email_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                for row in results:
                    email = str(row[0]).strip()
                    if not self._is_valid_email(email):
                        failing_values.append(email)
                        
            elif check_type == 'phone_number_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                for row in results:
                    phone = str(row[0]).strip()
                    if not self._is_valid_phone(phone):
                        failing_values.append(phone)
                        
            elif check_type == 'date_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                for row in results:
                    date_str = str(row[0]).strip()
                    if not self._is_valid_date(date_str):
                        failing_values.append(date_str)
                        
            elif check_type == 'numeric_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                for row in results:
                    val_str = str(row[0]).strip()
                    if not self._is_numeric(val_str):
                        failing_values.append(val_str)
                        
            elif check_type == 'duplicate_check':
                cursor.execute(f"""
                    SELECT {field_name}, COUNT(*) as count
                    FROM {table_name}
                    WHERE {field_name} IS NOT NULL
                    GROUP BY {field_name}
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                    LIMIT 50
                """)
                results = cursor.fetchall()
                failing_values = [f"{row[0]} (appears {row[1]} times)" for row in results]
                
            elif check_type == 'special_characters_check':
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != '' LIMIT 100")
                results = cursor.fetchall()
                for row in results:
                    text = str(row[0]).strip()
                    if self._has_special_characters(text):
                        failing_values.append(text)
                
            # Limit the number of failing values to prevent huge files
            if len(failing_values) > 100:
                failing_values = failing_values[:100]
                failing_values.append(f"... and more (truncated to 100 values)")
                
        except sqlite3.Error as e:
            failing_values = [f"Error retrieving values: {str(e)}"]
        
        return failing_values

    def run_all_checks(self) -> Dict[str, List[Dict]]:
        if not self.checks_config:
            print("No checks configuration loaded")
            return {}

        results = {}
        for table_name, fields in self.checks_config.items():
            print(f"\nRunning checks for table: {table_name}")
            
            if not self._table_exists(table_name):
                print(f"Table '{table_name}' does not exist in database")
                continue

            table_results = []
            for field_name, checks in fields.items():
                print(f"  Checking field: {field_name}")
                field_results = self._run_field_checks(table_name, field_name, checks)
                if field_results:
                    table_results.extend(field_results)

            if table_results:
                results[table_name] = table_results

        return results

    def print_results(self, results: Dict[str, List[Dict]]):
        if not results:
            print(f"{Colors.WARNING}No data quality issues found or no checks were run{Colors.ENDC}")
            return

        print(f"\n{Colors.BOLD}{'='*80}{Colors.ENDC}")
        print(f"{Colors.BOLD}DATA QUALITY CHECK RESULTS{Colors.ENDC}")
        print(f"{Colors.BOLD}{'='*80}{Colors.ENDC}")

        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        warnings = 0

        for table_name, table_results in results.items():
            print(f"\n{Colors.BOLD}{Colors.UNDERLINE}Table: {table_name}{Colors.ENDC}")
            print("-" * 60)

            for result in table_results:
                total_checks += 1
                status = result['status']
                
                if status == 'PASS':
                    color = Colors.OKGREEN
                    passed_checks += 1
                elif status == 'FAIL':
                    color = Colors.FAIL
                    failed_checks += 1
                elif status == 'WARNING':
                    color = Colors.WARNING
                    warnings += 1
                else:
                    color = Colors.FAIL
                    failed_checks += 1

                print(f"{color}[{status}]{Colors.ENDC} {result['field']} - {result['check_type']}")
                print(f"  {result['message']}")
                print()

        print(f"\n{Colors.BOLD}{'='*80}{Colors.ENDC}")
        print(f"{Colors.BOLD}SUMMARY: Total: {total_checks}, Passed: {passed_checks}, Failed: {failed_checks}, Warnings: {warnings}{Colors.ENDC}")
        print(f"{Colors.BOLD}{'='*80}{Colors.ENDC}")

    def print_fields_status_summary(self, results: Dict[str, List[Dict]]):
        if not results:
            return

        print(f"\n{Colors.BOLD}{'='*60}{Colors.ENDC}")
        print(f"{Colors.BOLD}FIELD STATUS SUMMARY{Colors.ENDC}")
        print(f"{Colors.BOLD}{'='*60}{Colors.ENDC}")

        for table_name, table_results in results.items():
            print(f"\n{Colors.BOLD}Table: {table_name}{Colors.ENDC}")
            print("-" * 40)

            field_status = {}
            for result in table_results:
                field_name = result['field']
                if field_name not in field_status:
                    field_status[field_name] = {'pass': 0, 'fail': 0, 'warning': 0}
                
                if result['status'] == 'PASS':
                    field_status[field_name]['pass'] += 1
                elif result['status'] == 'FAIL':
                    field_status[field_name]['fail'] += 1
                elif result['status'] == 'WARNING':
                    field_status[field_name]['warning'] += 1

            for field_name, status in field_status.items():
                if status['fail'] > 0:
                    status_color = Colors.FAIL
                    status_text = "FAIL"
                elif status['warning'] > 0:
                    status_color = Colors.WARNING
                    status_text = "WARNING"
                else:
                    status_color = Colors.OKGREEN
                    status_text = "PASS"

                print(f"  {status_color}[{status_text}]{Colors.ENDC} {field_name} "
                      f"(P:{status['pass']}, F:{status['fail']}, W:{status['warning']})")


    def export_failed_checks_to_results_db(self, results: Dict[str, List[Dict]], results_manager) -> bool:
        """Export failed data quality checks to Results database"""
        if not results:
            print(f"{Colors.WARNING}No results to export{Colors.ENDC}")
            return False
        
        # Filter only failed and error results
        failed_records = []
        
        for table_name, table_results in results.items():
            for result in table_results:
                if result['status'] in ['FAIL', 'ERROR']:
                    # Get actual failing values from database
                    failing_values = self._get_failing_values_from_db(
                        table_name, result['field'], result['check_type']
                    )
                    
                    # Create a record for each failing value or one record if no specific values
                    if failing_values:
                        for failing_value in failing_values:
                            failed_records.append([
                                table_name,
                                result['field'],
                                result['check_type'],
                                result['status'],
                                result['message'],
                                failing_value,
                                datetime.now().strftime("%Y-%m-%d"),  # Date column
                                datetime.now().isoformat()  # Timestamp column
                            ])
                    else:
                        failed_records.append([
                            table_name,
                            result['field'],
                            result['check_type'],
                            result['status'],
                            result['message'],
                            "No specific values",
                            datetime.now().strftime("%Y-%m-%d"),  # Date column
                            datetime.now().isoformat()  # Timestamp column
                        ])
        
        if not failed_records:
            print(f"{Colors.OKBLUE}ℹ No failed checks to export{Colors.ENDC}")
            return False
        
        # Define column names
        column_names = [
            'table_name', 
            'field_name', 
            'check_type', 
            'status', 
            'message', 
            'failing_value',
            'date',
            'timestamp'
        ]
        
        # Create description
        description = f"Failed data quality checks export - {len(failed_records)} failed records"
        
        # Store in Results database with custom table name
        stored_table = results_manager.store_failed_checks_results(
            failed_records, column_names, description
        )
        
        if stored_table:
            print(f"{Colors.OKGREEN}✓ Failed checks exported to Results database: {stored_table}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Failed records stored: {len(failed_records)}{Colors.ENDC}")
            return True
        else:
            print(f"{Colors.FAIL}Error exporting failed checks to Results database{Colors.ENDC}")
            return False

    def export_results_to_csv(self, results: Dict[str, List[Dict]]):
        """Export ALL results to CSV (both passed and failed)"""
        if not results:
            print(f"{Colors.WARNING}No results to export{Colors.ENDC}")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_quality_report_{timestamp}.csv"

        try:
            current_dir = os.getcwd()
            full_filepath = os.path.join(current_dir, filename)
            
            with open(full_filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['table', 'field', 'check_type', 'status', 'message', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for table_name, table_results in results.items():
                    for result in table_results:
                        writer.writerow({
                            'table': result['table'],
                            'field': result['field'],
                            'check_type': result['check_type'],
                            'status': result['status'],
                            'message': result['message'],
                            'timestamp': datetime.now().isoformat()
                        })

            if os.path.exists(full_filepath):
                file_size = os.path.getsize(full_filepath)
                print(f"{Colors.OKGREEN}✓ All results exported to: {full_filepath}{Colors.ENDC}")
                print(f"{Colors.OKCYAN}  File size: {file_size} bytes{Colors.ENDC}")
                
                # Check if there are any failures to export detailed values
                has_failures = any(
                    result['status'] in ['FAIL', 'ERROR'] 
                    for table_results in results.values() 
                    for result in table_results
                )
                
                if has_failures:
                    failing_values_filename = f"failing_values_report_{timestamp}.csv"
                    self.export_failing_values_to_csv(results, failing_values_filename)
            else:
                print(f"{Colors.FAIL}Error: File was not created{Colors.ENDC}")

        except Exception as e:
            print(f"{Colors.FAIL}Error exporting results: {str(e)}{Colors.ENDC}")
            print(f"{Colors.WARNING}Current directory: {os.getcwd()}{Colors.ENDC}")

    def export_failing_values_to_csv(self, results: Dict[str, List[Dict]], filename: str = None):
        """Export detailed failing values to a separate CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"failing_values_detailed_{timestamp}.csv"

        try:
            current_dir = os.getcwd()
            full_filepath = os.path.join(current_dir, filename)
            
            failing_records = []
            
            # Collect all failing values from the database
            for table_name, table_results in results.items():
                for result in table_results:
                    if result['status'] in ['FAIL', 'ERROR']:
                        field_name = result['field']
                        check_type = result['check_type']
                        
                        # Get actual failing values from database
                        failing_values = self._get_failing_values_from_db(
                            table_name, field_name, check_type
                        )
                        
                        for failing_value in failing_values:
                            failing_records.append({
                                'table': table_name,
                                'field_name': field_name,
                                'check_type': check_type,
                                'failing_value': failing_value,
                                'status': result['status'],
                                'message': result['message'],
                                'timestamp': datetime.now().isoformat()
                            })

            if failing_records:
                with open(full_filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['table', 'field_name', 'check_type', 'failing_value', 'status', 'message', 'timestamp']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for record in failing_records:
                        writer.writerow(record)

                if os.path.exists(full_filepath):
                    file_size = os.path.getsize(full_filepath)
                    print(f"{Colors.OKGREEN}✓ Failing values exported to: {full_filepath}{Colors.ENDC}")
                    print(f"{Colors.OKCYAN}  Total failing records: {len(failing_records)}{Colors.ENDC}")
                    print(f"{Colors.OKCYAN}  File size: {file_size} bytes{Colors.ENDC}")
                else:
                    print(f"{Colors.FAIL}Error: Failing values file was not created{Colors.ENDC}")
            else:
                print(f"{Colors.OKBLUE}ℹ No failing values to export{Colors.ENDC}")

        except Exception as e:
            print(f"{Colors.FAIL}Error exporting failing values: {str(e)}{Colors.ENDC}")
            print(f"{Colors.WARNING}Current directory: {os.getcwd()}{Colors.ENDC}")

    def run_checks_for_specific_table(self, table_name: str) -> Dict[str, List[Dict]]:
        """Run data quality checks for a specific table"""
        if table_name not in self.checks_config:
            print(f"{Colors.FAIL}No configuration found for table: {table_name}{Colors.ENDC}")
            return {}

        if not self._table_exists(table_name):
            print(f"{Colors.WARNING}Table '{table_name}' does not exist in database{Colors.ENDC}")
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

    def get_failed_fields_summary(self, results: Dict[str, List[Dict]]) -> Dict[str, Dict[str, List[str]]]:
        """Get a summary of fields that have failed checks"""
        failed_fields = {}
        
        for table_name, table_results in results.items():
            table_failed_fields = {}
            
            for result in table_results:
                if result['status'] in ['FAIL', 'ERROR']:
                    field_name = result['field']
                    if field_name not in table_failed_fields:
                        table_failed_fields[field_name] = []
                    table_failed_fields[field_name].append(result['check_type'])
            
            if table_failed_fields:
                failed_fields[table_name] = table_failed_fields
        
        return failed_fields


class ResultsManager:
    """Manages storing query results in a separate Results database"""
    
    def __init__(self):
        self.results_db_path = "Results.db"
        self.results_connection = None
        self._initialize_results_db()
    
    def _initialize_results_db(self):
        """Initialize the Results database and create metadata table"""
        try:
            self.results_connection = sqlite3.connect(self.results_db_path)
            cursor = self.results_connection.cursor()
            
            # Create metadata table to track query executions
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
            print(f"{Colors.OKGREEN}✓ Results database initialized: {self.results_db_path}{Colors.ENDC}")
            
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error initializing Results database: {str(e)}{Colors.ENDC}")
    
    def _get_next_version(self, base_table_name: str) -> int:
        """Get the next version number for a table"""
        cursor = self.results_connection.cursor()
        cursor.execute('''
            SELECT MAX(version) FROM query_metadata 
            WHERE table_name LIKE ? || '%'
        ''', (base_table_name,))
        
        result = cursor.fetchone()
        max_version = result[0] if result[0] is not None else 0
        return max_version + 1
    
    def _generate_table_name(self, base_name: str = "query_result") -> tuple:
        """Generate a unique table name with date and version"""
        current_date = datetime.now().strftime("%Y%m%d")
        base_table_name = f"{base_name}_{current_date}"
        
        version = self._get_next_version(base_table_name)
        table_name = f"{base_table_name}_v{version}"
        
        return table_name, version
    def _generate_failed_checks_table_name(self) -> tuple:
        """Generate a unique table name for failed checks with date and version"""
        current_date = datetime.now().strftime("%Y%m%d")
        base_table_name = f"failedchecks_{current_date}"
        version = self._get_next_version(base_table_name)
        table_name = f"{base_table_name}_v{version}"
        return table_name, version
    def _generate_passed_checks_table_name(self) -> tuple:
        """Generate a unique table name for passed checks with date and version"""
        current_date = datetime.now().strftime("%Y%m%d")
        base_table_name = f"passedchecks_{current_date}"
        version = self._get_next_version(base_table_name)
        table_name = f"{base_table_name}_v{version}"
        return table_name, version

    def store_passed_checks_results(self, passed_records: List[List], column_names: List[str], description: str = "") -> Optional[str]:
        """Store passed data quality check results in the Results database"""
        if not passed_records or not column_names:
            print(f"{Colors.WARNING}No passed check results to store{Colors.ENDC}")
            return None

        try:
            table_name, version = self._generate_passed_checks_table_name()
            cursor = self.results_connection.cursor()

            # Create table structure for passed checks
            columns_def = [
                "result_id INTEGER PRIMARY KEY AUTOINCREMENT"
            ]
            
            # Add all columns
            for col_name in column_names:
                columns_def.append(f"'{col_name}' TEXT")

            create_table_sql = f'''
            CREATE TABLE {table_name} (
                {', '.join(columns_def)}
            )
            '''

            cursor.execute(create_table_sql)

            # Insert data
            placeholders = ', '.join(['?' for _ in column_names])
            column_names_quoted = [f"'{col}'" for col in column_names]
            insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names_quoted)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_sql, passed_records)

            # Store metadata with a special query indicator for passed checks
            metadata_sql = '''
            INSERT INTO query_metadata
            (table_name, execution_date, version, original_query, row_count, column_count, description, created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            current_timestamp = datetime.now().isoformat()
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            cursor.execute(metadata_sql, (
                table_name,
                current_date,
                version,
                "DATA_QUALITY_PASSED_CHECKS_EXPORT",  # Special identifier
                len(passed_records),
                len(column_names),
                description,
                current_timestamp
            ))

            self.results_connection.commit()

            print(f"{Colors.OKGREEN}✓ Passed checks stored in table: {table_name}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Passed records stored: {len(passed_records)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Columns: {len(column_names)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Version: {version}{Colors.ENDC}")

            return table_name

        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error storing passed checks: {str(e)}{Colors.ENDC}")
            return None


    def store_failed_checks_results(self, failed_records: List[List], column_names: List[str], description: str = "") -> Optional[str]:
        """Store failed data quality check results in the Results database"""
        if not failed_records or not column_names:
            print(f"{Colors.WARNING}No failed check results to store{Colors.ENDC}")
            return None

        try:
            table_name, version = self._generate_failed_checks_table_name()
            cursor = self.results_connection.cursor()

            # Create table structure for failed checks
            columns_def = [
                "result_id INTEGER PRIMARY KEY AUTOINCREMENT"
            ]
            
            # Add all columns
            for col_name in column_names:
                columns_def.append(f"'{col_name}' TEXT")

            create_table_sql = f'''
            CREATE TABLE {table_name} (
                {', '.join(columns_def)}
            )
            '''

            cursor.execute(create_table_sql)

            # Insert data
            placeholders = ', '.join(['?' for _ in column_names])
            column_names_quoted = [f"'{col}'" for col in column_names]
            insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names_quoted)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_sql, failed_records)

            # Store metadata with a special query indicator for failed checks
            metadata_sql = '''
            INSERT INTO query_metadata
            (table_name, execution_date, version, original_query, row_count, column_count, description, created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            current_timestamp = datetime.now().isoformat()
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            cursor.execute(metadata_sql, (
                table_name,
                current_date,
                version,
                "DATA_QUALITY_FAILED_CHECKS_EXPORT",  # Special identifier
                len(failed_records),
                len(column_names),
                description,
                current_timestamp
            ))

            self.results_connection.commit()

            print(f"{Colors.OKGREEN}✓ Failed checks stored in table: {table_name}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Failed records stored: {len(failed_records)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Columns: {len(column_names)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN} - Version: {version}{Colors.ENDC}")

            return table_name

        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error storing failed checks: {str(e)}{Colors.ENDC}")
            return None

    def store_query_results(self, query: str, results: List[tuple], column_names: List[str],description: str = "") -> Optional[str]:
        """Store query results in the Results database"""
        if not results or not column_names:
            print(f"{Colors.WARNING}No results to store{Colors.ENDC}")
            return None
        
        try:
            table_name, version = self._generate_table_name()
            cursor = self.results_connection.cursor()
            
            # Check if 'id' column already exists in the original results
            has_id_column = 'id' in [col.lower() for col in column_names]
            
            # Create table structure based on results
            columns_def = []
            
            # Only add auto-increment ID if the original results don't have an 'id' column
            if not has_id_column:
                columns_def.append("result_id INTEGER PRIMARY KEY AUTOINCREMENT")
            
            # Add all original columns
            for col_name in column_names:
                columns_def.append(f"'{col_name}' TEXT")
            
            create_table_sql = f'''
                CREATE TABLE {table_name} (
                    {', '.join(columns_def)}
                )
            '''
            
            cursor.execute(create_table_sql)
            
            # Insert data
            placeholders = ', '.join(['?' for _ in column_names])
            column_names_quoted = [f"'{col}'" for col in column_names]
            insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names_quoted)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_sql, results)
            
            # Store metadata
            metadata_sql = '''
                INSERT INTO query_metadata 
                (table_name, execution_date, version, original_query, row_count, column_count, description, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            current_timestamp = datetime.now().isoformat()
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            cursor.execute(metadata_sql, (
                table_name,
                current_date,
                version,
                query,
                len(results),
                len(column_names),
                description,
                current_timestamp
            ))
            
            self.results_connection.commit()
            
            print(f"{Colors.OKGREEN}✓ Results stored in table: {table_name}{Colors.ENDC}")
            print(f"{Colors.OKCYAN}  - Rows stored: {len(results)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN}  - Columns: {len(column_names)}{Colors.ENDC}")
            print(f"{Colors.OKCYAN}  - Version: {version}{Colors.ENDC}")
            if has_id_column:
                print(f"{Colors.OKCYAN}  - Note: Used original 'id' column (no auto-increment added){Colors.ENDC}")
            
            return table_name
            
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error storing results: {str(e)}{Colors.ENDC}")
            return None

    
    def list_stored_results(self):
        """List all stored query results"""
        try:
            cursor = self.results_connection.cursor()
            cursor.execute('''
                SELECT table_name, execution_date, version, row_count, column_count, 
                       description, created_timestamp, original_query
                FROM query_metadata 
                ORDER BY created_timestamp DESC
            ''')
            
            results = cursor.fetchall()
            
            if not results:
                print(f"{Colors.WARNING}No stored results found{Colors.ENDC}")
                return
            
            print(f"\n{Colors.BOLD}{'='*100}{Colors.ENDC}")
            print(f"{Colors.BOLD}STORED QUERY RESULTS{Colors.ENDC}")
            print(f"{Colors.BOLD}{'='*100}{Colors.ENDC}")
            
            for result in results:
                table_name, exec_date, version, row_count, col_count, desc, timestamp, query = result
                
                print(f"\n{Colors.BOLD}Table: {table_name}{Colors.ENDC}")
                print(f"Date: {exec_date} | Version: {version} | Rows: {row_count} | Columns: {col_count}")
                print(f"Created: {timestamp}")
                if desc:
                    print(f"Description: {desc}")
                print(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")
                print("-" * 80)
                
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error listing results: {str(e)}{Colors.ENDC}")
    
    def view_stored_result(self, table_name: str):
        """View data from a stored result table"""
        try:
            cursor = self.results_connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
            results = cursor.fetchall()
            
            if not results:
                print(f"{Colors.WARNING}No data found in table {table_name}{Colors.ENDC}")
                return
            
            column_names = [description[0] for description in cursor.description]
            
            print(f"\n{Colors.OKGREEN}Data from table: {table_name}{Colors.ENDC}")
            print("-" * 100)
            
            # Print header
            header = " | ".join(f"{col:15}" for col in column_names)
            print(f"{Colors.BOLD}{header}{Colors.ENDC}")
            print("-" * 100)
            
            # Print data
            for row in results:
                row_str = " | ".join(f"{str(val):15}" for val in row)
                print(row_str)
            
            print("-" * 100)
            print(f"{Colors.OKCYAN}Showing {len(results)} rows (limited to 100){Colors.ENDC}")
            
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error viewing stored result: {str(e)}{Colors.ENDC}")
    
    def delete_stored_result(self, table_name: str):
        """Delete a stored result table"""
        try:
            cursor = self.results_connection.cursor()
            
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                print(f"{Colors.FAIL}Table {table_name} not found{Colors.ENDC}")
                return
            
            # Delete the table
            cursor.execute(f"DROP TABLE {table_name}")
            
            # Delete metadata
            cursor.execute("DELETE FROM query_metadata WHERE table_name = ?", (table_name,))
            
            self.results_connection.commit()
            print(f"{Colors.OKGREEN}✓ Deleted stored result: {table_name}{Colors.ENDC}")
            
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Error deleting stored result: {str(e)}{Colors.ENDC}")
    
    def close(self):
        """Close the Results database connection"""
        if self.results_connection:
            self.results_connection.close()

# Keep your existing DataQualityChecker class exactly as it is
# [Your existing DataQualityChecker class code here - no changes needed]

class SQLGenerator:
    def __init__(self, groq_api_key: str = None):
        """Initialize SQL Generator with Groq API key and Results manager"""
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.groq_base_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "meta-llama/llama-4-maverick-17b-128e-instruct"
        self.db_connection = None
        self.db_path = None
        self.data_quality_checker = None
        self.results_manager = ResultsManager()
        self.masking_manager = DataMaskingManager()  # Add this line
        
        if not self.groq_api_key:
            print(f"{Colors.WARNING}Warning: No Groq API key found. Set GROQ_API_KEY environment variable or provide it when running.{Colors.ENDC}")

    def print_banner(self):
        """Print application banner"""
        banner = f"""
{Colors.HEADER}{Colors.BOLD}
╔══════════════════════════════════════════════════════════════╗
║                    SQL CODE GENERATOR                        ║
║                   Powered by Groq API                        ║
║                                                              ║
║   Generate SQL queries using AI and execute on SQLite DB     ║
║              With Automated Data Quality Checks              ║
╚══════════════════════════════════════════════════════════════╝
{Colors.ENDC}
"""
        print(banner)
    def load_system_codes_config(self):
        """Load system codes configuration from CSV file"""
        if not self.data_quality_checker:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return

        csv_path = input(f"{Colors.OKCYAN}Enter path to system codes CSV file: {Colors.ENDC}").strip()
        if not csv_path:
            print(f"{Colors.WARNING}No file path provided{Colors.ENDC}")
            return

        if not os.path.exists(csv_path):
            print(f"{Colors.FAIL}Error: File '{csv_path}' not found{Colors.ENDC}")
            return

        success = self.data_quality_checker.load_system_codes_config(csv_path)
        if success:
            print(f"{Colors.OKGREEN}✓ System codes configuration loaded successfully{Colors.ENDC}")
    def generate_sql_query(self, user_request: str, original_schema_info: str = "", masked_schema_info: str = "") -> Optional[str]:
        """Generate SQL query using Groq API with masked data"""
        if not self.groq_api_key:
            print(f"{Colors.FAIL}Error: Groq API key not configured{Colors.ENDC}")
            return None

        # Mask the user request
        masked_user_request = self.masking_manager.mask_user_query(user_request, original_schema_info)
        
        system_prompt = f"""You are an expert SQL developer. Generate only valid SQL queries based on user requests.

    Rules:
    1. Return ONLY the SQL query, no explanations or markdown
    2. Use proper SQL syntax for SQLite
    3. If schema information is provided, use it to create accurate queries
    4. For data manipulation queries, be careful with syntax
    5. Always use semicolon at the end

    {f"Database Schema: {masked_schema_info}" if masked_schema_info else ""}

    Generate SQL query for the following request:"""

        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": masked_user_request}
            ],
            "max_tokens": 1000,
            "temperature": 0.1
        }

        try:
            print(f"{Colors.OKCYAN}Generating SQL query...{Colors.ENDC}")
            response = requests.post(self.groq_base_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                masked_sql_query = result['choices'][0]['message']['content'].strip()
                masked_sql_query = re.sub(r'```sql\n?', '', masked_sql_query)
                masked_sql_query = re.sub(r'```\n?', '', masked_sql_query)
                masked_sql_query = masked_sql_query.strip()
                
                # Unmask the generated query before returning
                original_sql_query = self.masking_manager.unmask_sql_query(masked_sql_query)
                return original_sql_query
            else:
                print(f"{Colors.FAIL}Error: {response.status_code} - {response.text}{Colors.ENDC}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"{Colors.FAIL}Network error: {str(e)}{Colors.ENDC}")
            return None
        except Exception as e:
            print(f"{Colors.FAIL}Error generating query: {str(e)}{Colors.ENDC}")
            return None


    def connect_database(self, db_path: str = None) -> bool:
        """Connect to SQLite database"""
        if not db_path:
            db_path = input(f"{Colors.OKCYAN}Enter database path (or press Enter for 'test.db'): {Colors.ENDC}").strip()
            if not db_path:
                db_path = "test.db"

        try:
            self.db_connection = sqlite3.connect(db_path)
            self.db_connection.row_factory = sqlite3.Row
            self.db_path = db_path
            self.data_quality_checker = DataQualityChecker(self.db_connection)
            print(f"{Colors.OKGREEN}✓ Connected to database: {db_path}{Colors.ENDC}")
            return True
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}Database connection error: {str(e)}{Colors.ENDC}")
            return False

    def get_database_schema(self) -> tuple:
        """Get database schema information and build masking mappings"""
        if not self.db_connection:
            return "", ""

        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            original_schema_info = []
            masked_schema_info = []

            for table in tables:
                original_table_name = table[0]
                masked_table_name = self.masking_manager.mask_table_name(original_table_name)
                
                cursor.execute(f"PRAGMA table_info({original_table_name});")
                columns = cursor.fetchall()
                
                original_column_info = []
                masked_column_info = []
                
                for col in columns:
                    original_col_name = col[1]
                    col_type = col[2]
                    masked_col_name = self.masking_manager.mask_column_name(original_table_name, original_col_name)
                    
                    original_column_info.append(f"{original_col_name} {col_type}")
                    masked_column_info.append(f"{masked_col_name} {col_type}")
                
                original_schema_info.append(f"Table: {original_table_name} ({', '.join(original_column_info)})")
                masked_schema_info.append(f"Table: {masked_table_name} ({', '.join(masked_column_info)})")

            return "\n".join(original_schema_info), "\n".join(masked_schema_info)

        except sqlite3.Error as e:
            print(f"{Colors.WARNING}Warning: Could not retrieve schema: {str(e)}{Colors.ENDC}")
            return "", ""


    def execute_query(self, query: str) -> bool:
        """Execute SQL query on connected database with option to store results"""
        if not self.db_connection:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return False

        try:
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                if results:
                    column_names = [description[0] for description in cursor.description]
                    print(f"\n{Colors.OKGREEN}Query Results:{Colors.ENDC}")
                    print("-" * 80)
                    header = " | ".join(f"{col:15}" for col in column_names)
                    print(f"{Colors.BOLD}{header}{Colors.ENDC}")
                    print("-" * 80)
                    
                    for row in results:
                        row_str = " | ".join(f"{str(val):15}" for val in row)
                        print(row_str)
                    
                    print("-" * 80)
                    print(f"{Colors.OKCYAN}Total rows: {len(results)}{Colors.ENDC}")
                    
                    # Ask if user wants to store results
                    store_choice = input(f"\n{Colors.OKCYAN}Store these results in Results database? (y/n): {Colors.ENDC}").strip().lower()
                    if store_choice == 'y':
                        description = input(f"{Colors.OKCYAN}Enter description for this result set (optional): {Colors.ENDC}").strip()
                        stored_table = self.results_manager.store_query_results(
                            query, results, column_names, description
                        )
                        if stored_table:
                            print(f"{Colors.OKGREEN}✓ Results successfully stored in Results.db{Colors.ENDC}")
                else:
                    print(f"{Colors.WARNING}No results found{Colors.ENDC}")
            else:
                self.db_connection.commit()
                print(f"{Colors.OKGREEN}✓ Query executed successfully. Rows affected: {cursor.rowcount}{Colors.ENDC}")
            
            return True
            
        except sqlite3.Error as e:
            print(f"{Colors.FAIL}SQL Error: {str(e)}{Colors.ENDC}")
            return False
        
    def load_data_quality_config(self):
        """Load data quality checks configuration from CSV file"""
        if not self.data_quality_checker:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return

        csv_path = input(f"{Colors.OKCYAN}Enter path to data quality checks CSV file: {Colors.ENDC}").strip()
        if not csv_path:
            print(f"{Colors.WARNING}No file path provided{Colors.ENDC}")
            return

        if not os.path.exists(csv_path):
            print(f"{Colors.FAIL}Error: File '{csv_path}' not found{Colors.ENDC}")
            return

        success = self.data_quality_checker.load_checks_config(csv_path)
        if success:
            print(f"{Colors.OKGREEN}✓ Data quality configuration loaded successfully{Colors.ENDC}")
            run_checks = input(f"{Colors.OKCYAN}Run data quality checks now? (y/n): {Colors.ENDC}").strip().lower()
            if run_checks == 'y':
                self.run_data_quality_checks()

    def run_data_quality_checks(self):
        """Run all configured data quality checks"""
        if not self.data_quality_checker:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return

        if not self.data_quality_checker.checks_config:
            print(f"{Colors.WARNING}No data quality checks configured. Please load configuration first.{Colors.ENDC}")
            return

        print(f"{Colors.OKCYAN}Running data quality checks...{Colors.ENDC}")
        results = self.data_quality_checker.run_all_checks()

        self.data_quality_checker.print_results(results)
        self.data_quality_checker.print_fields_status_summary(results)

        if results:
            print(f"\n{Colors.BOLD}Export Options:{Colors.ENDC}")
            export_choice = input(f"{Colors.OKCYAN}Choose export option:\n1. Export to CSV\n2. Export to Results database\n3. Both\n4. Skip\nEnter choice (1-4): {Colors.ENDC}").strip()
            
            if export_choice in ['1', '3']:
                self.data_quality_checker.export_results_to_csv(results)
            
            if export_choice in ['2', '3']:
                # Ask what to export to database
                db_export_choice = input(f"{Colors.OKCYAN}Export to database:\n1. Failed checks only\n2. Passed checks only\n3. Both\nEnter choice (1-3): {Colors.ENDC}").strip()
                
                if db_export_choice in ['1', '3']:
                    success = self.data_quality_checker.export_failed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Failed checks exported to Results database{Colors.ENDC}")
                
                if db_export_choice in ['2', '3']:
                    success = self.data_quality_checker.export_passed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Passed checks exported to Results database{Colors.ENDC}")


    def run_table_specific_checks(self):
        """Run data quality checks for a specific table"""
        if not self.data_quality_checker:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return

        if not self.data_quality_checker.checks_config:
            print(f"{Colors.WARNING}No data quality checks configured. Please load configuration first.{Colors.ENDC}")
            return

        available_tables = list(self.data_quality_checker.checks_config.keys())
        print(f"\n{Colors.OKCYAN}Available tables with configured checks:{Colors.ENDC}")
        for i, table in enumerate(available_tables, 1):
            print(f"{i}. {table}")

        table_choice = input(f"\n{Colors.OKCYAN}Enter table name or number: {Colors.ENDC}").strip()

        if table_choice.isdigit():
            table_index = int(table_choice) - 1
            if 0 <= table_index < len(available_tables):
                table_name = available_tables[table_index]
            else:
                print(f"{Colors.FAIL}Invalid table number{Colors.ENDC}")
                return
        else:
            table_name = table_choice

        if table_name not in available_tables:
            print(f"{Colors.FAIL}Table '{table_name}' not found in configuration{Colors.ENDC}")
            return

        print(f"{Colors.OKCYAN}Running checks for table: {table_name}...{Colors.ENDC}")
        results = self.data_quality_checker.run_checks_for_specific_table(table_name)

        if results:
            self.data_quality_checker.print_results(results)
            self.data_quality_checker.print_fields_status_summary(results)
            
            # Add export options here too
            print(f"\n{Colors.BOLD}Export Options:{Colors.ENDC}")
            export_choice = input(f"{Colors.OKCYAN}Choose export option:\n1. Export to CSV\n2. Export to Results database\n3. Both\n4. Skip\nEnter choice (1-4): {Colors.ENDC}").strip()
            
            if export_choice in ['1', '3']:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data_quality_report_{table_name}_{timestamp}.csv"
                self.data_quality_checker.export_results_to_csv(results)
            
            if export_choice in ['2', '3']:
                # Ask what to export to database
                db_export_choice = input(f"{Colors.OKCYAN}Export to database:\n1. Failed checks only\n2. Passed checks only\n3. Both\nEnter choice (1-3): {Colors.ENDC}").strip()
                
                if db_export_choice in ['1', '3']:
                    success = self.data_quality_checker.export_failed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Failed checks exported to Results database{Colors.ENDC}")
                
                if db_export_choice in ['2', '3']:
                    success = self.data_quality_checker.export_passed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Passed checks exported to Results database{Colors.ENDC}")
        else:
            print(f"{Colors.WARNING}No results found for table '{table_name}'{Colors.ENDC}")

    def show_failed_fields_only(self):
        """Show only the fields that have failed data quality checks"""
        if not self.data_quality_checker:
            print(f"{Colors.FAIL}Error: No database connection{Colors.ENDC}")
            return

        if not self.data_quality_checker.checks_config:
            print(f"{Colors.WARNING}No data quality checks configured. Please load configuration first.{Colors.ENDC}")
            return

        print(f"{Colors.OKCYAN}Running data quality checks to identify failed fields...{Colors.ENDC}")
        results = self.data_quality_checker.run_all_checks()

        if not results:
            print(f"{Colors.WARNING}No data quality issues found{Colors.ENDC}")
            return

        failed_fields = self.data_quality_checker.get_failed_fields_summary(results)

        if not failed_fields:
            print(f"{Colors.OKGREEN}✓ No fields have failed data quality checks!{Colors.ENDC}")
            return

        print(f"\n{Colors.BOLD}{Colors.FAIL}{'='*60}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.FAIL}FAILED FIELDS REPORT{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.FAIL}{'='*60}{Colors.ENDC}")

        total_failed_fields = 0
        for table_name, fields in failed_fields.items():
            total_failed_fields += len(fields)
            print(f"\n{Colors.BOLD}{Colors.FAIL}Table: {table_name}{Colors.ENDC}")
            print(f"{Colors.FAIL}Failed fields: {len(fields)}{Colors.ENDC}")
            print("-" * 40)

            for field_name, failed_checks_list in fields.items():
                failed_checks_str = ", ".join(failed_checks_list)
                print(f"  {Colors.FAIL}• {field_name}{Colors.ENDC}")
                print(f"    Failed checks: {failed_checks_str}")

        print(f"\n{Colors.BOLD}{Colors.FAIL}{'='*60}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.FAIL}TOTAL FAILED FIELDS: {total_failed_fields}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.FAIL}{'='*60}{Colors.ENDC}")

        detailed_choice = input(f"\n{Colors.OKCYAN}Show detailed results for failed fields? (y/n): {Colors.ENDC}").strip().lower()
        if detailed_choice == 'y':
            failed_results = {}
            for table_name, table_results in results.items():
                failed_table_results = [r for r in table_results if r['status'] in ['FAIL', 'ERROR']]
                if failed_table_results:
                    failed_results[table_name] = failed_table_results

            if failed_results:
                print(f"\n{Colors.BOLD}DETAILED FAILED CHECKS:{Colors.ENDC}")
                self.data_quality_checker.print_results(failed_results)

    def show_menu(self):
        """Display main menu - UPDATED TO SHOW OPTIONS 1-19"""
        menu = f"""
    {Colors.BOLD}Main Menu:{Colors.ENDC}
    {Colors.OKCYAN}1.{Colors.ENDC} Generate SQL query using AI
    {Colors.OKCYAN}2.{Colors.ENDC} Execute manual SQL query
    {Colors.OKCYAN}3.{Colors.ENDC} Connect to database
    {Colors.OKCYAN}4.{Colors.ENDC} Show database schema
    {Colors.OKCYAN}5.{Colors.ENDC} Create sample database
    {Colors.OKCYAN}6.{Colors.ENDC} Load data quality checks configuration
    {Colors.OKCYAN}7.{Colors.ENDC} Load system codes configuration
    {Colors.OKCYAN}8.{Colors.ENDC} Run all data quality checks
    {Colors.OKCYAN}9.{Colors.ENDC} Run data quality checks for specific table
    {Colors.OKCYAN}10.{Colors.ENDC} Show failed fields only
    {Colors.OKCYAN}11.{Colors.ENDC} Export failing values to separate CSV
    {Colors.OKCYAN}12.{Colors.ENDC} Set Groq API key
    {Colors.OKCYAN}13.{Colors.ENDC} View stored query results
    {Colors.OKCYAN}14.{Colors.ENDC} List all stored results
    {Colors.OKCYAN}15.{Colors.ENDC} Delete stored result
    {Colors.OKCYAN}16.{Colors.ENDC} Export failed checks to Results database
    {Colors.OKCYAN}17.{Colors.ENDC} Export passed checks to Results database
    {Colors.OKCYAN}18.{Colors.ENDC} Show data masking mappings
    {Colors.OKCYAN}19.{Colors.ENDC} Exit
    """
        print(menu)

    def view_stored_results_menu(self):
        """Menu for viewing stored results"""
        self.results_manager.list_stored_results()
        
        table_name = input(f"\n{Colors.OKCYAN}Enter table name to view (or press Enter to return): {Colors.ENDC}").strip()
        if table_name:
            self.results_manager.view_stored_result(table_name)

    def delete_stored_results_menu(self):
        """Menu for deleting stored results"""
        self.results_manager.list_stored_results()
        
        table_name = input(f"\n{Colors.OKCYAN}Enter table name to delete (or press Enter to cancel): {Colors.ENDC}").strip()
        if table_name:
            confirm = input(f"{Colors.WARNING}Are you sure you want to delete '{table_name}'? (y/n): {Colors.ENDC}").strip().lower()
            if confirm == 'y':
                self.results_manager.delete_stored_result(table_name)

    def show_masking_mappings(self):
        """Display current masking mappings"""
        print(f"\n{Colors.BOLD}DATA MASKING MAPPINGS{Colors.ENDC}")
        print("=" * 50)
        
        print(f"\n{Colors.BOLD}Table Mappings:{Colors.ENDC}")
        for original, masked in self.masking_manager.table_mapping.items():
            print(f"  {original} → {masked}")
        
        print(f"\n{Colors.BOLD}Column Mappings:{Colors.ENDC}")
        for table, columns in self.masking_manager.column_mapping.items():
            print(f"  Table: {table}")
            for original_col, masked_col in columns.items():
                print(f"    {original_col} → {masked_col}")

    def run(self):
        """Main application loop - UPDATED TO HANDLE OPTIONS 1-19"""
        self.print_banner()
        
        while True:
            self.show_menu()
            choice = input(f"{Colors.BOLD}Enter your choice (1-19): {Colors.ENDC}").strip()

            if choice == '1':
                if not self.groq_api_key:
                    print(f"{Colors.FAIL}Error: Groq API key not configured. Please set it first (option 12){Colors.ENDC}")
                    continue

                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue

                user_request = input(f"{Colors.OKCYAN}Enter your query request: {Colors.ENDC}")
                if not user_request:
                    continue

                original_schema_info, masked_schema_info = self.get_database_schema()
                sql_query = self.generate_sql_query(user_request, original_schema_info, masked_schema_info)

                if sql_query:
                    print(f"\n{Colors.OKGREEN}Generated SQL Query:{Colors.ENDC}")
                    print(f"{Colors.BOLD}{sql_query}{Colors.ENDC}")
                    
                    execute_choice = input(f"{Colors.OKCYAN}Execute this query? (y/n): {Colors.ENDC}").strip().lower()
                    if execute_choice == 'y':
                        self.execute_query(sql_query)


            elif choice == '2':
                # [Keep your existing code for option 2]
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue

                print(f"{Colors.OKCYAN}Enter SQL query (press Enter twice to execute):{Colors.ENDC}")
                lines = []
                while True:
                    line = input()
                    if line == "" and lines:
                        break
                    lines.append(line)

                query = "\n".join(lines).strip()
                if query:
                    self.execute_query(query)

            # [Keep all your existing elif cases for options 3-12 exactly as they are]
            elif choice == '3':
                self.connect_database()
            elif choice == '4':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                schema = self.get_database_schema()
                if schema:
                    print(f"\n{Colors.OKGREEN}Database Schema:{Colors.ENDC}")
                    print(f"{Colors.BOLD}{schema}{Colors.ENDC}")
                else:
                    print(f"{Colors.WARNING}No tables found in database{Colors.ENDC}")
            elif choice == '5':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                confirm = input(f"{Colors.WARNING}This will create/overwrite sample tables. Continue? (y/n): {Colors.ENDC}").strip().lower()
                if confirm == 'y':
                    self.create_sample_database()
            elif choice == '6':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                self.load_data_quality_config()
            elif choice == '7':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                self.load_system_codes_config()
            elif choice == '8':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                self.run_data_quality_checks()
            elif choice == '9':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                self.run_table_specific_checks()
            elif choice == '10':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                self.show_failed_fields_only()
            elif choice == '11':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                print(f"{Colors.OKCYAN}Running data quality checks to export failing values...{Colors.ENDC}")
                results = self.data_quality_checker.run_all_checks()
                if results:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"failing_values_only_{timestamp}.csv"
                    self.data_quality_checker.export_failing_values_to_csv(results, filename)
                else:
                    print(f"{Colors.OKBLUE}No data quality issues found{Colors.ENDC}")
            elif choice == '12':
                new_api_key = input(f"{Colors.OKCYAN}Enter Groq API key: {Colors.ENDC}").strip()
                if new_api_key:
                    self.groq_api_key = new_api_key
                    print(f"{Colors.OKGREEN}✓ Groq API key updated{Colors.ENDC}")
                else:
                    print(f"{Colors.WARNING}No API key provided{Colors.ENDC}")
            
            # NEW OPTIONS 13-16
            elif choice == '13':
                self.view_stored_results_menu()

            elif choice == '14':
                self.results_manager.list_stored_results()

            elif choice == '15':
                self.delete_stored_results_menu()

            elif choice == '16':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                if not self.data_quality_checker.checks_config:
                    print(f"{Colors.WARNING}No data quality checks configured. Please load configuration first.{Colors.ENDC}")
                    continue
                
                print(f"{Colors.OKCYAN}Running data quality checks to export failed checks to Results database...{Colors.ENDC}")
                results = self.data_quality_checker.run_all_checks()
                
                if results:
                    success = self.data_quality_checker.export_failed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Failed checks successfully exported to Results database{Colors.ENDC}")
                else:
                    print(f"{Colors.OKBLUE}No data quality issues found to export{Colors.ENDC}")

            elif choice == '17':
                if not self.db_connection:
                    print(f"{Colors.FAIL}Error: No database connection. Please connect first (option 3){Colors.ENDC}")
                    continue
                if not self.data_quality_checker.checks_config:
                    print(f"{Colors.WARNING}No data quality checks configured. Please load configuration first.{Colors.ENDC}")
                    continue
                
                print(f"{Colors.OKCYAN}Running data quality checks to export passed checks to Results database...{Colors.ENDC}")
                results = self.data_quality_checker.run_all_checks()
                
                if results:
                    success = self.data_quality_checker.export_passed_checks_to_results_db(results, self.results_manager)
                    if success:
                        print(f"{Colors.OKGREEN}✓ Passed checks successfully exported to Results database{Colors.ENDC}")
                else:
                    print(f"{Colors.OKBLUE}No data quality check results found to export{Colors.ENDC}")

            elif choice == '18':
                self.show_masking_mappings()

            elif choice == '19':  # Updated exit option;;
                if self.db_connection:
                    self.db_connection.close()
                self.results_manager.close()
                print(f"{Colors.OKGREEN}✓ Database connections closed{Colors.ENDC}")
                print(f"{Colors.OKBLUE}Thank you for using SQL Code Generator!{Colors.ENDC}")
                break 
            else:
                print(f"{Colors.FAIL}Invalid choice. Please enter a number between 1 and 19.{Colors.ENDC}")
# [Keep your existing main function exactly as it is]

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='SQL Code Generator CLI with Groq API Integration')
    parser.add_argument('--api-key', help='Groq API key')
    parser.add_argument('--db-path', help='Path to SQLite database file')
    parser.add_argument('--create-sample', action='store_true', help='Create sample database on startup')
    parser.add_argument('--quality-config', help='Path to data quality checks CSV configuration file')
    
    args = parser.parse_args()
    
    generator = SQLGenerator(groq_api_key=args.api_key)
    
    if args.db_path:
        generator.connect_database(args.db_path)
    
    if args.create_sample and generator.db_connection:
        generator.create_sample_database()
    
    if args.quality_config and generator.db_connection:
        if os.path.exists(args.quality_config):
            generator.data_quality_checker.load_checks_config(args.quality_config)
        else:
            print(f"{Colors.FAIL}Error: Quality config file '{args.quality_config}' not found{Colors.ENDC}")
    
    try:
        generator.run()
    except KeyboardInterrupt:
        print(f"\n{Colors.OKBLUE}Goodbye!{Colors.ENDC}")
        if generator.db_connection:
            generator.db_connection.close()
        sys.exit(0)
    except Exception as e:
        print(f"{Colors.FAIL}Unexpected error: {str(e)}{Colors.ENDC}")
        if generator.db_connection:
            generator.db_connection.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
