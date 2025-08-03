import base64
import os
import logging
import json
import shutil
from typing import Dict, List, Optional, Tuple
import uuid
from sqlalchemy import create_engine, text
import pandas as pd
import re
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import chromadb
from openai import OpenAI
import hashlib
import pypdfium2 as pdfium
from io import BytesIO
import time
import markdown2
import queue
from threading import Thread
import threading
import html
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError
import glob
from typing import List, Dict, Tuple, Optional

# Dictionary to store progress for each upload session
progress_queues = {}
last_progress_states = {}  # Store last known progress state per session

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('openai').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def enable_debug_mode():
    logging.getLogger().setLevel(logging.DEBUG)

if os.getenv("DEBUG") == "1":
    enable_debug_mode()

# Database setup (only financial_sql database)
DB_PASSWORD = "mysecretpassword"
financial_db_uri = f"postgresql+psycopg2://postgres:{DB_PASSWORD}@13.212.76.228:3000/financial_sql"
financial_engine = create_engine(financial_db_uri)

try:
    with financial_engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
except Exception as e:
    logger.error("Failed to connect to database: %s", str(e))
    exit(1)

# Email configuration
SENDER_EMAIL = "tengis0810@gmail.com"
SENDER_PASSWORD = "oufa cpre ypzo dpbi"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# API Keys
OPENAI_API_KEY = "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"
GOOGLE_API_KEY = "AIzaSyBPwhPFnUJ1YzshqGRL4Lc_REuMdh-8XzM"
genai.configure(api_key=GOOGLE_API_KEY)
GEMINI_MODEL = genai.GenerativeModel('gemini-2.5-flash')

# Directory paths
input_dir = "data/pdfs"  # Updated to process all PDFs from pdfs folder
image_base_dir = "data/images"
output_file = os.path.join("data/pdfs", "output.txt")
excel_folder_path = "data/excels"
table_metadata_file = os.path.join(excel_folder_path, "frc_explained.xlsx")

# Ensure directories exist
os.makedirs(input_dir, exist_ok=True)
os.makedirs(image_base_dir, exist_ok=True)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
os.makedirs(excel_folder_path, exist_ok=True)

# Global caches for Excel
TABLE_METADATA_CACHE = None
COLUMN_METADATA_CACHE = None
DB_TABLE_NAMES_CACHE = None

# Declare agents globally
excel_agent = None
pdf_agent = None
agent_lock = threading.Lock()


def parse_pdf_names(query: str, available_pdfs: List[str]) -> List[str]:
    """Extract PDF names mentioned in the query, matching against available PDFs."""
    query_lower = query.lower()
    mentioned_pdfs = []
    for pdf in available_pdfs:
        pdf_name = os.path.splitext(pdf)[0].lower()
        if pdf_name in query_lower or pdf.lower() in query_lower:
            mentioned_pdfs.append(pdf_name)
    # If no specific PDFs mentioned, assume all available PDFs
    if not mentioned_pdfs:
        mentioned_pdfs = [os.path.splitext(pdf)[0].lower() for pdf in available_pdfs]
    return mentioned_pdfs

def detect_comparison_request(query: str) -> bool:
    """Detect if the query requests a comparison using GPT-4o analysis."""
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", "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"))
        prompt = f"""
        You are an expert in natural language understanding. Analyze the following query to determine if it requests a comparison between multiple entities (e.g., documents, data points, or values). A comparison request involves contrasting or evaluating differences, similarities, or relationships (e.g., 'compare', 'vs', 'differ', 'higher than', or implicit comparisons like 'how do X and Y relate to Z?'). 

        Query: "{query}"

        Instructions:
        - Return a JSON object with a single key 'is_comparison' set to true if the query requests a comparison, false otherwise.
        - Do not provide any explanation or additional text, only the JSON object.
        """
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a language analysis assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            temperature=0.1
        )
        result = response.choices[0].message.content.strip()
        try:
            parsed_result = json.loads(result)
            is_comparison = parsed_result.get('is_comparison', False)
            logger.info(f"GPT-4o detected comparison request for query '{query}': {is_comparison}")
            return is_comparison
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GPT-4o response: {e}, response: {result}")
            return False
    except Exception as e:
        logger.error(f"GPT-4o analysis failed for query '{query}': {str(e)}")
        return False

# Excel Helper Functions
def is_year_column(column_name):
    return isinstance(column_name, str) and any(term in column_name.lower() for term in ['он', 'жил'])

def is_month_column(column_name):
    return isinstance(column_name, str) and 'сар' in column_name.lower()

def fill_year_column(column):
    result = column.astype(str).replace('nan', pd.NA)
    return result.ffill()

def clean_year_month_value(value):
    if pd.isna(value):
        return value
    value_str = str(value).strip()
    cleaned = re.sub(r'(?i)\s*(он|жил|сар)\s*', '', value_str)
    match = re.match(r'^\d+$', cleaned)
    return cleaned if match else value

def clean_numeric_value(value):
    if pd.isna(value):
        return pd.NA
    value_str = str(value).strip()
    if value_str.lower() == 'нийт' or value_str == ' -   ' or value_str == 'УУЛ УУРХАЙН БҮТЭЭГДЭХҮҮНИЙ БИРЖ':
        return pd.NA
    cleaned = re.sub(r',(?=\d{3})', '', value_str)
    cleaned = cleaned.replace(',', '.')
    try:
        float(cleaned)
        return cleaned
    except (ValueError, TypeError):
        return pd.NA

def sanitize_table_name(name):
    return re.sub(r'\s+', '_', name.strip())

def shorten_column_names(column_names, original_row, nearby_headers):
    prompt = (
        f"Given the following list of column names from a financial and mining dataset in Mongolian: {json.dumps(column_names, ensure_ascii=False)}, "
        f"and nearby headers for context: {nearby_headers}, "
        f"provide a JSON array of shortened column names. Each shortened name must: "
        f"- Be in Mongolian, preserving the key meaning of the original name. "
        f"- Be concise, under 50 characters to allow for potential duplicate suffixes. "
        f"- Align with the dataset's context (financial metrics like trading volumes, market capitalization, or mining products like coal, iron, cashmere). "
        f"- Avoid special characters like '/', '(', ')', and multiple spaces. "
        f"Return only the JSON array of shortened names, with no additional text."
    )
    try:
        response = GEMINI_MODEL.generate_content(prompt)
        raw_response = response.text
        cleaned_response = raw_response.strip()
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:-3].strip()
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response[3:-3].strip()
        if not cleaned_response:
            raise ValueError("Cleaned API response is empty")
        shortened_names = json.loads(cleaned_response)
        if len(shortened_names) != len(column_names):
            raise ValueError(f"Number of shortened names ({len(shortened_names)}) does not match input ({len(column_names)})")
        return [re.sub(r'[\s/()]+', '_', name.strip())[:50] for name in shortened_names]
    except GoogleAPIError as e:
        logger.error(f"Gemini API error: {e}")
        return [re.sub(r'[\s/()]+', '_', name.strip())[:50] for name in column_names]
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}, Cleaned response: {cleaned_response}")
        return [re.sub(r'[\s/()]+', '_', name.strip())[:50] for name in column_names]
    except Exception as e:
        logger.error(f"Unexpected error in shorten_column_names: {e}")
        return [re.sub(r'[\s/()]+', '_', name.strip())[:50] for name in column_names]

def sanitize_column_names(column_names, original_row, nearby_headers):
    shortened_names = shorten_column_names(column_names, original_row, nearby_headers)
    col_name_counts = {}
    unique_names = []
    for idx, name in enumerate(shortened_names):
        if pd.isna(name) or not name:
            safe_value = f"column_{idx}"
        else:
            safe_value = name
            if safe_value in col_name_counts:
                col_name_counts[safe_value] += 1
                safe_value = f"{safe_value}_{col_name_counts[safe_value]}"[:63]
            else:
                col_name_counts[safe_value] = 0
            safe_value = safe_value[:63]
        unique_names.append(safe_value)
    return unique_names

def table_exists(engine, table_name):
    query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)")
    with engine.connect() as connection:
        result = connection.execute(query, {"table_name": table_name}).scalar()
    return result

def load_table_and_column_metadata(excel_file):
    try:
        df = pd.read_excel(excel_file, sheet_name='тайлбар', header=None)
        xls = pd.ExcelFile(excel_file)
        sheet_names = [sanitize_table_name(name) for name in xls.sheet_names]

        table_metadata = {}
        column_metadata_dict = {}
        current_table = None

        for index, row in df.iterrows():
            if len(row) < 2:
                logger.debug(f"Skipping row {index}: Insufficient columns ({len(row)})")
                continue

            row_0 = str(row[0]).strip() if not pd.isna(row[0]) else ''
            row_1 = str(row[1]).strip() if not pd.isna(row[1]) else ''

            if not row_0:
                logger.debug(f"Skipping row {index}: Empty row_0")
                continue

            sanitized_row_0 = sanitize_table_name(row_0)
            logger.debug(f"Processing row {index}: row_0={row_0}, sanitized_row_0={sanitized_row_0}, row_1={row_1}")

            if sanitized_row_0 in sheet_names or row_0.lower().startswith(('стат', 'вххүү', 'үхэхз', 'үм')):
                current_table = sanitized_row_0
                table_metadata[current_table] = row_1
                column_metadata_dict[current_table] = {}
                logger.debug(f"Identified table: {current_table}, Description: {row_1}")
            elif current_table:
                column_metadata_dict[current_table][row_0] = row_1
                logger.debug(f"Added column to {current_table}: {row_0} -> {row_1}")

        logger.debug(f"Table metadata: {json.dumps(table_metadata, ensure_ascii=False, indent=2)}")
        logger.debug(f"Column metadata: {json.dumps(column_metadata_dict, ensure_ascii=False, indent=2)}")

        return table_metadata, column_metadata_dict
    except Exception as e:
        logger.error(f"Error loading metadata from 'тайлбар' sheet in {excel_file}: {e}")
        return {}, {}

def map_sheet_to_table_name(sheet_name, table_metadata):
    for table_name in table_metadata.keys():
        unsanitized = table_name.replace('_', ' ')
        if unsanitized == sheet_name or table_name == sanitize_table_name(sheet_name):
            return table_name
    return sanitize_table_name(sheet_name)

def get_db_table_names(engine):
    global DB_TABLE_NAMES_CACHE
    if DB_TABLE_NAMES_CACHE is not None:
        return DB_TABLE_NAMES_CACHE
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    try:
        with engine.connect() as connection:
            result = connection.execute(query).fetchall()
            DB_TABLE_NAMES_CACHE = [row[0] for row in result]
            return DB_TABLE_NAMES_CACHE
    except Exception as e:
        logger.error(f"Error retrieving table names: {e}")
        return []

def get_postgresql_columns(engine, table_name):
    query = text("SELECT column_name FROM information_schema.columns WHERE table_name = :table_name")
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"table_name": table_name}).fetchall()
            columns = [row[0] for row in result]
            logger.debug(f"PostgreSQL columns for {table_name}: {columns}")
            return columns
    except Exception as e:
        logger.error(f"Error retrieving columns for table {table_name}: {e}")
        return []

def analyze_question_for_table(question, table_metadata, engine):
    table_info = [
        f"Table: {table_name}, Description: {table_desc}"
        for table_name, table_desc in table_metadata.items()
    ]
    prompt = (
        f"Given the following question in Mongolian: '{question}', and the following table metadata from a financial and mining dataset:\n"
        f"{'\n'.join(table_info)}\n"
        f"Select all relevant tables that best match the question based on their descriptions. "
        f"Return a JSON array of table names (e.g., ['стат_ББСБ_2024_', 'стат_ББСБ_2006_2023']). If no relevant tables are found, return an empty array []."
    )
    try:
        response = GEMINI_MODEL.generate_content(prompt)
        raw_response = response.text.strip()
        if raw_response.startswith('```json'):
            raw_response = raw_response[7:-3].strip()
        elif raw_response.startswith('```'):
            raw_response = raw_response[3:-3].strip()

        try:
            selected_tables = json.loads(raw_response)
            if not isinstance(selected_tables, list):
                logger.debug(f"Invalid response format, expected list, got: {raw_response}")
                selected_tables = []
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}, Cleaned response: {raw_response}")
            table_matches = re.findall(r'[\w_]+', raw_response)
            selected_tables = [t for t in table_matches if t in table_metadata or sanitize_table_name(t) in table_metadata]
            if not selected_tables and 'ББСБ' in question:
                selected_tables = [t for t in table_metadata if 'ББСБ' in t]
                if 'одоо' in question.lower():
                    selected_tables = [t for t in selected_tables if '2024_' in t] or selected_tables

        db_tables = get_db_table_names(engine)
        corrected_tables = []
        for table in selected_tables:
            corrected_table = table.replace('c', 'с')
            if corrected_table in db_tables:
                corrected_tables.append(corrected_table)
            elif table in db_tables:
                corrected_tables.append(table)
            else:
                sanitized_table = sanitize_table_name(table)
                if sanitized_table in db_tables:
                    corrected_tables.append(sanitized_table)
                else:
                    for db_table in db_tables:
                        if db_table.lower() == corrected_table.lower() or db_table.lower() == sanitized_table.lower():
                            corrected_tables.append(db_table)
                            break

        selected_tables = list(set(corrected_tables))
        logger.debug(f"Selected tables after correction: {selected_tables}")
        return selected_tables
    except Exception as e:
        logger.error(f"Unexpected error analyzing question: {e}")
        return []

def generate_sql_query(question, table_metadata, column_metadata_dict, engine):
    selected_tables = analyze_question_for_table(question, table_metadata, engine)
    if not selected_tables:
        logger.info("Failed to select any relevant tables.")
        return None, "No relevant tables found for the question."

    db_table_names = []
    db_tables = get_db_table_names(engine)

    for selected_table in selected_tables:
        if selected_table in db_tables:
            db_table_names.append(selected_table)
        else:
            for orig_table_name in table_metadata:
                if sanitize_table_name(orig_table_name) == selected_table:
                    prompt = (
                        f"Given the table name '{orig_table_name}' from a financial and mining dataset in Mongolian, "
                        f"and the actual table names in the PostgreSQL database: {json.dumps(db_tables, ensure_ascii=False)}, "
                        f"select the most appropriate table name from the database that matches '{orig_table_name}'. "
                        f"Return only the matching database table name as a string."
                    )
                    try:
                        response = GEMINI_MODEL.generate_content(prompt)
                        matched_table = response.text.strip()
                        if matched_table in db_tables:
                            db_table_names.append(matched_table)
                            logger.debug(f"Mapped table {selected_table} to database table {matched_table}")
                    except GoogleAPIError as e:
                        logger.error(f"Gemini API error mapping table {selected_table}: {e}")

    if not db_table_names:
        logger.info("No matching database tables found.")
        return None, "No matching database tables found."

    table_columns = {}
    for table_name in db_table_names:
        table_columns[table_name] = get_postgresql_columns(engine, table_name)

    all_column_info = []
    for table_name in selected_tables:
        columns = column_metadata_dict.get(table_name, {})
        column_info = [f"{table_name}.{col_name}: {col_desc}" for col_name, col_desc in columns.items()]
        all_column_info.extend(column_info)
        table_desc = table_metadata.get(table_name, '')
        all_column_info.append(f"Table {table_name} Description: {table_desc}")

    prompt = (
        f"Given the following question in Mongolian: '{question}', and the following metadata for tables {selected_tables}:\n"
        f"{' '.join(all_column_info)}\n"
        f"Generate a PostgreSQL query to answer the question and provide a detailed explanation. The response must be in JSON format with two keys:\n"
        f"- QUERY: The PostgreSQL query as a string, using table names {db_table_names} and ONLY column names from PostgreSQL columns {table_columns}.\n"
        f"- DETAILED_EXPLANATION: A 60-word explanation covering tables, columns, joins, and calculations used in the query.\n"
        f"Additional requirements:\n"
        f"- Do not use UNION calls.\n"
        f"- Use INNER JOIN or CROSS JOIN on 'Жил' and 'Сар' columns when combining multiple tables.\n"
        f"- Reference Excel column names and descriptions to select relevant columns (e.g., 'Ямааны_Ноолуур' for cashmere export volume).\n"
        f"- For virtual asset transaction volume, use SUM('Бүртгэлтэй_ВХҮҮ_дотоод_ВХ_гүйлгээ_') + SUM('Бүртгэлтэй_ВХҮҮ_гадаад_ВХ_гүйлгээ_').\n"
        f"- For export volumes (e.g., 'Ямааны ноолуурын экспортын хэмжээ'), use SUM('Ямааны_Ноолуур').\n"
        f"- Use PostgreSQL syntax with NUMERIC columns.\n"
        f"- Include conditions for years or quarters using 'Жил' or 'Улирал' if mentioned.\n"
        f"- Use double quotes around column and table names with special characters.\n"
        f"Return only the JSON object with QUERY and DETAILED_EXPLANATION."
    )
    try:
        response = GEMINI_MODEL.generate_content(prompt)
        raw_response = response.text.strip()
        if raw_response.startswith('```json'):
            raw_response = raw_response[7:-3].strip()
        elif raw_response.startswith('```'):
            raw_response = raw_response[3:-3].strip()
        result = json.loads(raw_response)
        query = result.get('QUERY', '').strip()
        explanation = result.get('DETAILED_EXPLANATION', '').strip()
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Explanation: {explanation}")
        return query, explanation
    except GoogleAPIError as e:
        logger.error(f"Gemini API error generating SQL query: {e}")
        return None, f"Failed to generate query due to API error: {e}"
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}, Cleaned response: {raw_response}")
        return None, f"Failed to parse API response: {e}"
    except Exception as e:
        logger.error(f"Unexpected error generating SQL query: {e}")
        return None, f"Unexpected error: {e}"

def excel_answer_question(question, table_metadata_file, engine):
    global TABLE_METADATA_CACHE, COLUMN_METADATA_CACHE
    if TABLE_METADATA_CACHE is None or COLUMN_METADATA_CACHE is None:
        TABLE_METADATA_CACHE, COLUMN_METADATA_CACHE = load_table_and_column_metadata(table_metadata_file)

    query, explanation = generate_sql_query(question, TABLE_METADATA_CACHE, COLUMN_METADATA_CACHE, engine)
    if not query:
        return f"Sorry, I couldn't generate a query to answer your question. {explanation}"

    logger.debug(f"Generated SQL Query: {query}")

    try:
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection)
            response = (
                f"To answer your question, I queried the database and got the following results:\n\n"
                f"{result.to_string(index=False)}\n\n"
                f"Here's how the query works: {explanation}"
            )
            return response
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        return f"Sorry, there was an error running the query: {e}. The query was designed to: {explanation}"

def process_excel_sheets(excel_file, table_metadata_file, output_file, header_row_index=None, use_csv=False, session_id=None, engine=financial_engine):
    table_metadata, _ = load_table_and_column_metadata(table_metadata_file)
    total_sheets = len(pd.ExcelFile(excel_file).sheet_names)
    processed_sheets = 0

    if session_id in progress_queues:
        progress_queues[session_id].put({
            "progress": 0,
            "page": 0,
            "total_pages": total_sheets,
            "status": f"Starting processing of {os.path.basename(excel_file)}",
            "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Starting Excel processing"
        })

    xls = pd.ExcelFile(excel_file)
    for sheet_name in xls.sheet_names:
        sanitized_table_name = map_sheet_to_table_name(sheet_name, table_metadata)
        if table_exists(engine, sanitized_table_name):
            logger.info(f"Table {sanitized_table_name} already exists in the database. Skipping sheet {sheet_name}.")
            processed_sheets += 1
            if session_id in progress_queues:
                progress = (processed_sheets / total_sheets) * 100
                progress_queues[session_id].put({
                    "progress": progress,
                    "page": processed_sheets,
                    "total_pages": total_sheets,
                    "status": f"Table {sanitized_table_name} already exists. Skipping sheet {sheet_name}.",
                    "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Skipped existing table"
                })
            continue

        logger.info(f"\nProcessing Sheet: {sheet_name} (Table: {sanitized_table_name})")
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        if df.empty or 0 not in df.columns:
            logger.warning(f"Sheet {sheet_name} is empty or has no first column. Skipping.")
            processed_sheets += 1
            if session_id in progress_queues:
                progress = (processed_sheets / total_sheets) * 100
                progress_queues[session_id].put({
                    "progress": progress,
                    "page": processed_sheets,
                    "total_pages": total_sheets,
                    "status": f"Sheet {sheet_name} is empty or has no first column. Skipping.",
                    "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Skipped empty sheet"
                })
            continue

        df[0] = df[0].astype(object)
        label_column = df[0].copy()
        current_main = None
        current_fill = None
        for i in range(min(5, len(df))):
            if pd.isna(label_column[i]):
                if current_fill is not None:
                    df.at[i, 0] = current_fill
            else:
                if isinstance(label_column[i], str):
                    combined = label_column[i]
                    if i > 0 and pd.isna(label_column[i - 1]):
                        if current_main is not None:
                            combined = current_main + " " + combined
                    else:
                        current_main = combined
                    df.at[i, 0] = combined
                    current_fill = combined

        found_row = False
        selected_header_index = header_row_index if header_row_index is not None else None
        modified_row = None
        if header_row_index is None:
            for i in range(min(5, len(df))):
                nan_count = sum(1 for value in df.iloc[i] if pd.isna(value))
                consecutive_nans = 0
                max_consecutive_nans = 0
                found_string_after_nan = False
                found_nan = False
                for value in df.iloc[i]:
                    if pd.isna(value):
                        consecutive_nans += 1
                        found_nan = True
                    else:
                        if isinstance(value, str):
                            if found_nan:
                                found_string_after_nan = True
                            max_consecutive_nans = max(max_consecutive_nans, consecutive_nans)
                            consecutive_nans = 0
                max_consecutive_nans = max(max_consecutive_nans, consecutive_nans)
                if found_string_after_nan and max_consecutive_nans <= 8:
                    nan_count = 0
                if nan_count < 4 and not found_row:
                    modified_row = df.iloc[i].copy()
                    original_row = df.iloc[i]
                    if i + 1 < len(df):
                        next_row = df.iloc[i + 1]
                        preceding_value = None
                        for col in range(len(modified_row)):
                            if not pd.isna(original_row[col]):
                                preceding_value = str(original_row[col])
                                if isinstance(next_row[col], str):
                                    replacement = preceding_value + " " + next_row[col]
                                    replacement = re.sub(r'\s+', ' ', replacement)
                                    modified_row[col] = replacement
                                elif isinstance(next_row[col], (int, float)) and 1900 <= next_row[col] <= 2100:
                                    if not is_year_column(preceding_value):
                                        replacement = f"{preceding_value} Жил"
                                    else:
                                        replacement = preceding_value
                                    replacement = re.sub(r'\s+', ' ', replacement)
                                    modified_row[col] = replacement
                            if pd.isna(modified_row[col]):
                                if isinstance(next_row[col], str):
                                    replacement = next_row[col]
                                    if preceding_value is not None:
                                        replacement = preceding_value + " " + replacement
                                    replacement = re.sub(r'\s+', ' ', replacement)
                                    modified_row[col] = replacement
                                elif isinstance(next_row[col], (int, float)) and 1900 <= next_row[col] <= 2100:
                                    if preceding_value is not None and not is_year_column(preceding_value):
                                        replacement = f"{preceding_value} Жил"
                                    else:
                                        replacement = "Жил"
                                    replacement = re.sub(r'\s+', ' ', replacement)
                                    modified_row[col] = replacement
                                else:
                                    try:
                                        start_idx = max(0, col - 5)
                                        end_idx = min(len(original_row), col + 6)
                                        nearby_headers = original_row[start_idx:end_idx].to_list()
                                        prompt = (
                                            f"Given the following row of column headers from a financial and mining dataset in Mongolian: {original_row.to_list()}, "
                                            f"and the next row: {next_row.to_list()}, suggest a single, concise column name in Mongolian for the column at index {col}. "
                                            f"The name must align with the dataset's context (financial metrics like trading volumes, market capitalization, or mining products like coal, iron, cashmere) "
                                            f"and match the style of nearby headers (columns {start_idx} to {end_idx-1}: {nearby_headers}). "
                                            f"Return only the column name, with no additional text or explanation."
                                        )
                                        response = GEMINI_MODEL.generate_content(prompt)
                                        logger.debug(f"Single column name response for col {col}: {response.text}")
                                        generated_name = response.text.strip()
                                        replacement = generated_name
                                        if preceding_value is not None:
                                            replacement = preceding_value + " " + generated_name
                                        replacement = re.sub(r'\s+', ' ', replacement)
                                        modified_row[col] = replacement
                                    except Exception as e:
                                        logger.warning(f"Failed to generate column name for column {col} with Gemini API: {e}")
                    column_names = []
                    for col_idx, value in enumerate(modified_row):
                        safe_value = str(value).replace('"', '').replace(' ', '_').replace('__', '') if not pd.isna(value) else f"column_{col_idx}"
                        column_names.append(safe_value)
                    start_idx = max(0, len(modified_row) - 5)
                    end_idx = len(modified_row)
                    nearby_headers = modified_row[start_idx:end_idx].to_list()
                    unique_column_names = sanitize_column_names(column_names, modified_row, nearby_headers)
                    sanitized_table_name = map_sheet_to_table_name(sheet_name, table_metadata)
                    if table_exists(engine, sanitized_table_name):
                        logger.info(f"Table {sanitized_table_name} already exists. Skipping creation.")
                    else:
                        QUERY = f'CREATE TABLE "{sanitized_table_name}" ('
                        for col_name in unique_column_names:
                            QUERY += f'"{col_name}" NUMERIC(25,5),'
                        QUERY = QUERY[:-1] + ");"
                        logger.debug(QUERY)
                        try:
                            with engine.connect() as connection:
                                connection.execute(text(QUERY))
                                connection.commit()
                            logger.info(f"Table {sanitized_table_name} created in database.")
                        except Exception as e:
                            logger.error(f"Error creating table {sanitized_table_name}: {e}")
                    selected_header_index = i
                    found_row = True

        if found_row and selected_header_index is not None:
            data_start_index = selected_header_index + 2 if selected_header_index + 1 < len(df) and any(isinstance(x, str) for x in df.iloc[selected_header_index + 1]) else selected_header_index + 1
            if data_start_index < len(df):
                data_df = df.iloc[data_start_index:].reset_index(drop=True)
                year_month_columns = [
                    col_idx for col_idx, col_name in enumerate(modified_row)
                    if is_year_column(str(col_name)) or is_month_column(str(col_name))
                ]
                for col_idx in year_month_columns:
                    data_df[col_idx] = data_df[col_idx].apply(clean_year_month_value)
                    data_df[col_idx] = fill_year_column(data_df[col_idx])
                for col_idx in range(len(data_df.columns)):
                    data_df[col_idx] = data_df[col_idx].apply(clean_numeric_value)
                data_df.columns = unique_column_names
                try:
                    with engine.connect() as connection:
                        data_df.to_sql(sanitized_table_name, connection, if_exists='append', index=False)
                        connection.commit()
                        logger.info(f"Data for sheet {sheet_name} inserted into table {sanitized_table_name} with cleaned year/month columns and numeric values.")
                except Exception as e:
                    logger.error(f"Error inserting data into table {sanitized_table_name}: {e}")
                if use_csv:
                    output_path = f"{sheet_name}.csv"
                    data_df.to_csv(output_path, index=False, header=False)
                    logger.info(f"Data for sheet {sheet_name} saved to {output_path} without headers, with year/month columns cleaned and filled")
            else:
                logger.warning(f"No data rows found after headers in sheet {sheet_name}. Skipping.")
        else:
            logger.warning(f"No valid header row found in the first 5 rows of sheet {sheet_name}. Skipping.")

        processed_sheets += 1
        if session_id in progress_queues:
            progress = (processed_sheets / total_sheets) * 100
            progress_queues[session_id].put({
                "progress": progress,
                "page": processed_sheets,
                "total_pages": total_sheets,
                "status": f"Processed sheet {sheet_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Processed sheet {sheet_name}"
            })

    if session_id in progress_queues:
        progress_queues[session_id].put({
            "progress": 100,
            "page": total_sheets,
            "total_pages": total_sheets,
            "status": f"Completed processing {os.path.basename(excel_file)}",
            "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Completed Excel processing"
        })

def process_excel_folder(folder_path, table_metadata_file, engine=financial_engine):
    excel_files = [f for f in glob.glob(os.path.join(folder_path, "*.xlsx")) if os.path.basename(f) != "frc_explained.xlsx"]
    if not excel_files:
        logger.warning(f"No Excel files found in folder: {folder_path}")
        return
    for excel_file in excel_files:
        logger.info(f"\nProcessing Excel file: {excel_file}")
        session_id = str(uuid.uuid4())
        progress_queues[session_id] = queue.Queue()
        process_excel_sheets(excel_file, table_metadata_file, output_file=None, use_csv=False, session_id=session_id, engine=engine)

# PDF Helper Functions
def pdf_to_images(pdf_path, output_base_dir, session_id):
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    logger.info(f"Extracted PDF base name: {pdf_name}")
    output_dir = os.path.join(output_base_dir, pdf_name)
    if os.path.exists(output_dir):
        logger.info(f"Image folder for {pdf_name} already exists, skipping conversion.")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 20,
                "page": 0,
                "total_pages": 0,
                "status": f"Image folder for {pdf_name} already exists, skipping conversion",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Image folder exists."
            })
        return
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = pdfium.PdfDocument(pdf_path)
        total_pages = len(pdf)
        logger.info(f"Total pages in PDF {pdf_name}: {total_pages}")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 0,
                "page": 0,
                "total_pages": total_pages,
                "status": f"Starting conversion of {pdf_name} to images",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Starting conversion"
            })
        for page_num in range(total_pages):
            page = pdf[page_num]
            bitmap = page.render(scale=600/72)  # High resolution
            pil_image = bitmap.to_pil()
            output_path = os.path.join(output_dir, f"{pdf_name}_page_{page_num+1}.png")
            pil_image.save(output_path, format="PNG")
            logger.info(f"Saved page {page_num+1} of {pdf_name} as {output_path}")
            if session_id in progress_queues:
                page_progress = ((page_num + 1) / total_pages) * 20
                progress_queues[session_id].put({
                    "progress": page_progress,
                    "page": page_num + 1,
                    "total_pages": total_pages,
                    "status": f"Converting page {page_num + 1} of {total_pages} for {pdf_name}",
                    "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Saved page {page_num+1}"
                })
        pdf.close()
    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 0,
                "page": 0,
                "total_pages": 0,
                "status": f"ERROR: Failed to convert PDF {pdf_name}: {str(e)}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - ERROR - Failed to convert"
            })
        raise

def ensure_pdf_images():
    if not os.path.exists(input_dir):
        logger.warning(f"Input directory {input_dir} does not exist. Creating it.")
        os.makedirs(input_dir, exist_ok=True)
    if not os.path.exists(image_base_dir):
        os.makedirs(image_base_dir, exist_ok=True)
    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    if not pdf_files:
        logger.warning("No PDF files found in data/pdfs directory.")
        return
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_dir, pdf_file)
        pdf_name = os.path.splitext(pdf_file)[0]
        image_dir = os.path.join(image_base_dir, pdf_name)
        if not os.path.exists(image_dir):
            logger.info(f"Image folder for {pdf_name} does not exist. Converting PDF to images...")
            try:
                pdf_to_images(pdf_path, image_base_dir, session_id=str(uuid.uuid4()))
            except Exception as e:
                logger.error(f"Failed to convert PDF {pdf_name} to images: {str(e)}")
                continue
        else:
            logger.info(f"Image folder for {pdf_name} already exists.")

def sanitize_collection_name(name):
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    sanitized = sanitized.strip('_')
    if not sanitized:
        sanitized = 'default'
    if len(sanitized) < 3:
        sanitized = sanitized + '0' * (3 - len(sanitized))
    if len(sanitized) > 50:
        sanitized = sanitized[:50]
    hash_suffix = hashlib.md5(name.encode('utf-8')).hexdigest()[:4]
    return f"{sanitized}_{hash_suffix}"

def initialize_chroma_client(chroma_path="tmp/chromadb"):
    try:
        if os.path.exists(chroma_path):
            logging.info(f"Found existing ChromaDB at {chroma_path}. Attempting to load...")
            try:
                client = chromadb.PersistentClient(path=chroma_path)
                client.list_collections()
                logging.info("Successfully loaded existing ChromaDB.")
                return client
            except Exception as e:
                logging.warning(f"Failed to load existing ChromaDB: {str(e)}. Resetting database...")
                shutil.rmtree(chroma_path)
                logging.info(f"Removed existing ChromaDB directory: {chroma_path}")
        os.makedirs(chroma_path, exist_ok=True)
        client = chromadb.PersistentClient(path=chroma_path)
        logging.info(f"Initialized new ChromaDB client at {chroma_path}.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize ChromaDB client: {str(e)}")
        raise SystemExit("Cannot proceed without a valid ChromaDB client.")

chroma_client = initialize_chroma_client()

def initialize_chroma_collection(file_name):
    collection_name = f"facts_{sanitize_collection_name(file_name)}"
    logger.info(f"Attempting to initialize collection '{collection_name}' for file '{file_name}'")
    with threading.Lock():
        try:
            try:
                collection = chroma_client.get_collection(name=collection_name)
                logger.info(f"Collection '{collection_name}' already exists.")
            except Exception:
                collection = chroma_client.create_collection(
                    name=collection_name,
                    metadata={"hnsw:space": "cosine"}
                )
                logger.info(f"Created new collection '{collection_name}'.")
            existing = collection.get(ids=[f"separator_{file_name}"])
            if not existing['ids']:
                collection.add(
                    documents=[f"Start of data for file: {file_name}"],
                    metadatas=[{
                        "type": "separator",
                        "source_name": file_name,
                        "source": "separator"
                    }],
                    ids=[f"separator_{file_name}"]
                )
                logger.info(f"Added separator for file '{file_name}' in ChromaDB.")
            return collection
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB collection '{collection_name}': {str(e)}")
            raise RuntimeError(f"Cannot proceed without a valid ChromaDB collection.") from e

def encode_image(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Failed to encode image: {str(e)}")
        return None

def analyze_with_openai(image_path, output_language="English"):
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        base64_image = encode_image(image_path)
        if not base64_image:
            logger.error(f"Failed to encode image {image_path}")
            return "Failed to encode image"
        prompt = f"""
You are an expert in analyzing documents using visual content, focusing on business-related PDFs that may contain Mongolian Cyrillic, English, or both. The attached image is a page from a PDF. Your task is to:

1. Extract all text visible in the image, preserving the exact wording and structure (e.g., headings, lists, tables).
2. Identify the primary language (Mongolian or English).
3. Provide a clear explanation of the page's purpose, key elements, structure, and overall message.
4. Extract ALL numerical data exactly as it appears (e.g., dates, quantities, clauses).
5. Output in {output_language}, with Mongolian Cyrillic for 'mon' or English for 'eng'.

Output format:
- Detected Language: [mon or eng]
- Extracted Text: [full text from image]
- Explanation: [detailed narrative]
- Numerical Data: [list of numbers]

Ensure accuracy, especially for numerical data and proper nouns (e.g., dates).
"""
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a document analysis assistant."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
                    ]
                }
            ],
            max_tokens=1500,
            temperature=0.1
        )
        explanation = response.choices[0].message.content.strip()
        logger.debug(f"Document analysis for {image_path}: {explanation[:100]}...")
        return explanation
    except Exception as e:
        logger.error(f"OpenAI analysis failed for {image_path}: {str(e)}")
        return f"OpenAI error: {str(e)}"

def clean_text(text):
    return ' '.join(text.split()).strip()

def extract_pdf_metadata(pdf_path):
    try:
        pdf = pdfium.PdfDocument(pdf_path)
        total_pages = len(pdf)
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        pdf.close()
        return {
            "pdf_name": pdf_name,
            "total_pages": total_pages,
            "file_path": pdf_path
        }
    except Exception as e:
        logger.error(f"Failed to extract metadata from PDF {pdf_path}: {str(e)}")
        return {}

def process_pdf(pdf_name: str, output_file: str, output_language: str = "English", session_id: str = None) -> None:
    try:
        logger.debug(f"Starting processing for {pdf_name}, session_id: {session_id}")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 20,
                "page": 0,
                "total_pages": 0,
                "status": f"Initializing ChromaDB collection for {pdf_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Initializing collection"
            })
        collection = initialize_chroma_collection(pdf_name)
        pdf_path = os.path.join(input_dir, f"{pdf_name}.pdf")
        metadata = extract_pdf_metadata(pdf_path)
        total_pages = metadata.get("total_pages", 0)
        if metadata:
            collection.add(
                documents=[f"Metadata for {pdf_name}: {json.dumps(metadata)}"],
                metadatas=[{
                    "source_type": "pdf",
                    "source_name": pdf_name,
                    "type": "metadata"
                }],
                ids=[f"{pdf_name}_metadata_{uuid.uuid4().hex[:8]}"]
            )
            logger.info(f"Added metadata for {pdf_name} to ChromaDB.")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 30,
                "page": 0,
                "total_pages": total_pages,
                "status": f"Processing images for {pdf_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Processing images"
            })
        process_images(image_base_dir, pdf_name, output_file=output_file, output_language=output_language, session_id=session_id)
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 90,
                "page": 0,
                "total_pages": total_pages,
                "status": f"Loading unified knowledge base for {pdf_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Loading knowledge base"
            })
        unified_collection = load_unified_knowledge_base()
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 100,
                "page": 0,
                "total_pages": total_pages,
                "status": f"Processing completed for {pdf_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Processing completed"
            })
        logger.info(f"PDF {pdf_name} processed successfully.")
    except Exception as e:
        logger.error(f"Failed to process PDF {pdf_name}: {str(e)}")
        if session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 0,
                "page": 0,
                "total_pages": 0,
                "status": f"ERROR: Failed to process {pdf_name}: {str(e)}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - ERROR - Failed to process"
            })
        raise

def detect_language(text):
    return 'mon' if any(c in text for c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя') else 'eng'

def process_images(image_dir, pdf_name, output_file=None, output_language="English", session_id=None):
    collection_name = f"facts_{sanitize_collection_name(pdf_name)}"
    try:
        collection = initialize_chroma_collection(pdf_name)
    except Exception as e:
        logger.error(f"Failed to initialize collection for PDF '{pdf_name}': {str(e)}")
        if session_id and session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 0,
                "page": 0,
                "total_pages": 0,
                "status": f"ERROR: Failed to initialize collection for {pdf_name}: {str(e)}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - ERROR - Failed to initialize"
            })
        return
    try:
        pdf_image_dir = os.path.join(image_dir, pdf_name)
        if not os.path.exists(pdf_image_dir):
            logger.warning(f"No image directory found for PDF: {pdf_name}")
            if session_id and session_id in progress_queues:
                progress_queues[session_id].put({
                    "progress": 0,
                    "page": 0,
                    "total_pages": 0,
                    "status": f"ERROR: No image directory found for {pdf_name}",
                    "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - WARNING - No image directory"
                })
            return
        image_files = [f for f in os.listdir(pdf_image_dir) if f.lower().endswith('.png')]
        image_files.sort(key=lambda x: int(x.split('_page_')[-1].split('.')[0]))
        total_images = len(image_files)
        logger.info(f"Total images to process for {pdf_name}: {total_images}")
        for idx, image_file in enumerate(image_files):
            try:
                image_path = os.path.join(pdf_image_dir, image_file)
                filename = os.path.splitext(image_file)[0]
                page_number = filename.split('_page_')[-1] if '_page_' in filename else "unknown"
                document_id = f"{pdf_name}_{filename}"
                existing_data = collection.get(ids=[document_id], include=["metadatas"])
                if existing_data['ids']:
                    logger.info(f"Skipping image {image_file} from PDF {pdf_name}: Already processed.")
                    continue
                logger.info(f"Processing image: {image_file} from PDF: {pdf_name}")
                analysis_result = analyze_with_openai(image_path, output_language=output_language)
                detected_lang = 'mon' if 'Detected Language: mon' in analysis_result else 'eng'
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"\n{'=' * 40}\n")
                    f.write(f"Processing Image: {image_file} (PDF: {pdf_name})\n")
                    f.write(f"{'=' * 40}\n\n")
                    f.write("--- Visual Analysis ---\n")
                    f.write(f"\tLanguage Detected: {detected_lang}\n")
                    f.write(f"\tAnalysis Result:\n")
                    for line in analysis_result.split('\n'):
                        f.write(f"\t\t- {line}\n")
                    f.write(f"{'-' * 40}\n")
                if analysis_result and not analysis_result.startswith("OpenAI error"):
                    collection.add(
                        documents=[analysis_result],
                        metadatas=[{
                            "source_type": "pdf",
                            "source_name": pdf_name,
                            "image_path": image_path,
                            "type": "visual_analysis",
                            "source": filename,
                            "language": detected_lang,
                            "page_number": page_number
                        }],
                        ids=[document_id]
                    )
                    logger.info(f"Added analysis {document_id} to ChromaDB.")
                if session_id and session_id in progress_queues:
                    image_progress = 30 + ((idx + 1) / total_images) * 60
                    logger.info(f"Progress update for {image_file}: {image_progress}%")
                    progress_queues[session_id].put({
                        "progress": image_progress,
                        "page": idx + 1,
                        "total_pages": total_images,
                        "status": f"Processed page {idx + 1} of {total_images} for {pdf_name}",
                        "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Processed image"
                    })
            except Exception as e:
                logger.error(f"Error processing image {image_file} for PDF '{pdf_name}': {str(e)}")
                if session_id and session_id in progress_queues:
                    progress_queues[session_id].put({
                        "progress": 30 + (idx / total_images) * 60,
                        "page": idx + 1,
                        "total_pages": total_images,
                        "status": f"ERROR: Failed to process image {image_file}: {str(e)}",
                        "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - ERROR - Failed to process"
                    })
                continue
        if session_id and session_id in progress_queues:
            logger.info(f"Completed processing all images for {pdf_name}")
            progress_queues[session_id].put({
                "progress": 90,
                "page": total_images,
                "total_pages": total_images,
                "status": f"Completed processing all images for {pdf_name}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - INFO - Completed processing"
            })
    except Exception as e:
        logger.error(f"Error processing images for PDF '{pdf_name}': {str(e)}")
        if session_id and session_id in progress_queues:
            progress_queues[session_id].put({
                "progress": 0,
                "page": 0,
                "total_pages": 0,
                "status": f"ERROR: {str(e)}",
                "log": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - ERROR - Error processing"
            })

def save_vector_data(output_file, collections):
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write("\n=== Visual Analysis Data ===\n")
            for file_name, collection in collections.items():
                results = collection.get(include=["documents", "metadatas"])
                if not results['documents']:
                    f.write(f"No data found in ChromaDB for file: {file_name}\n")
                    continue
                analyses = []
                for doc, meta, doc_id in zip(results['documents'], results['metadatas'], results['ids']):
                    if meta.get('type') == 'separator':
                        continue
                    analyses.append((doc_id, doc, meta))
                f.write(f"\n=== Data for File: {file_name} ===\n")
                f.write("\nVisual Analyses:\n")
                if not analyses:
                    f.write("No analysis data found.\n")
                for doc_id, doc, meta in analyses:
                    f.write(f"ID: {doc_id}\n")
                    f.write(f"Source: {meta.get('source', 'Unknown')}\n")
                    f.write(f"Language: {meta.get('language', 'Unknown')}\n")
                    f.write(f"Content:\n{doc}\n")
                    f.write(f"Metadata: {meta}\n")
                    f.write("-" * 50 + "\n")
        logger.info(f"Visual analysis data saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save visual data: {str(e)}")

def load_unified_knowledge_base(chroma_path="tmp/chromadb"):
    try:
        chroma_client = initialize_chroma_client(chroma_path)
        unified_collection_name = "facts_combined"
        unified_collection = chroma_client.get_or_create_collection(
            name=unified_collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        logger.info(f"Unified collection '{unified_collection_name}' loaded or created.")
        processed_collections = set()
        all_collections = chroma_client.list_collections()
        facts_collections = [coll for coll in all_collections if coll.name.startswith("facts_") and coll.name != unified_collection_name]
        for collection in facts_collections:
            if collection.name in processed_collections:
                continue
            try:
                results = collection.get(include=["documents", "metadatas"])
                documents = []
                metadatas = []
                ids = []
                for doc, meta, doc_id in zip(results['documents'], results['metadatas'], results['ids']):
                    if meta.get('type') != 'separator':
                        unique_id = f"{collection.name}_{doc_id}"
                        documents.append(doc)
                        metadatas.append(meta)
                        ids.append(unique_id)
                if documents:
                    existing_ids = unified_collection.get(ids=ids, include=[])
                    new_ids = [id_ for id_ in ids if id_ not in existing_ids['ids']]
                    if new_ids:
                        new_documents = [doc for doc, id_ in zip(documents, ids) if id_ in new_ids]
                        new_metadatas = [meta for meta, id_ in zip(metadatas, ids) if id_ in new_ids]
                        unified_collection.add(
                            documents=new_documents,
                            metadatas=new_metadatas,
                            ids=new_ids
                        )
                        logger.info(f"Added {len(new_documents)} new documents from {collection.name} to unified collection.")
                processed_collections.add(collection.name)
            except Exception as e:
                logger.error(f"Failed to process collection {collection.name}: {str(e)}")
                continue
        return unified_collection
    except Exception as e:
        logger.error(f"Failed to load unified knowledge base: {str(e)}")
        raise SystemExit("Cannot proceed without a valid unified ChromaDB collection.")

class ExcelAgent:
    def __init__(self, engine, table_metadata_file):
        self.engine = engine
        self.table_metadata_file = table_metadata_file
        logger.info("ExcelAgent initialized successfully.")

    def run(self, question):
        return excel_answer_question(question, self.table_metadata_file, self.engine)
    
class PDFAgent:
    def __init__(self, collection, api_key, model="gpt-4o"):
        self.collection = collection
        self.model = model
        self.client = OpenAI(api_key=api_key)
        logger.info("PDF RAG Agent initialized.")

    def search_knowledge(self, query: str, pdf_name_filter: Optional[List[str]] = None, 
                       page_number_filter: Optional[str] = None, limit: int = 40) -> Tuple[List[Dict], bool]:
        """Search ChromaDB for relevant documents, filtering by PDF names and page numbers."""
        try:
            # Step 1: Filter out separator documents
            where_filter = {"type": {"$ne": "separator"}}
            results = self.collection.get(
                where=where_filter,
                include=["documents", "metadatas"]
            )

            # Step 2: Filter by PDF names and page number in Python
            documents = [
                {"content": doc, "metadata": meta, "id": doc_id}
                for doc, meta, doc_id in zip(results['documents'], results['metadatas'], results['ids'])
                if meta.get('type') != 'separator'
            ]

            if pdf_name_filter:
                documents = [
                    doc for doc in documents 
                    if doc['metadata'].get('source_name', '').lower() in [name.lower() for name in pdf_name_filter]
                ]

            if page_number_filter:
                documents = [
                    doc for doc in documents 
                    if doc['metadata'].get('page_number', '') == str(page_number_filter)
                ]

            # Step 3: Enhanced keyword matching
            keywords = [word.lower() for word in query.split() if len(word) > 2]
            filtered_documents = []
            for doc in documents:
                doc_text = doc['content'].lower()
                relevance_score = sum(1 for keyword in keywords if keyword in doc_text)
                if relevance_score > 0 or not keywords:
                    doc['relevance_score'] = relevance_score
                    filtered_documents.append(doc)

            # Step 4: Sort by relevance and limit results
            filtered_documents = sorted(
                filtered_documents,
                key=lambda x: x.get('relevance_score', 0),
                reverse=True
            )[:limit]

            keywords_found = bool(filtered_documents)
            logger.info(f"Retrieved {len(filtered_documents)} documents for query: {query}")
            return filtered_documents, keywords_found
        except Exception as e:
            logger.error(f"Knowledge search failed: {str(e)}")
            return [], False

    def generate_answer(self, query: str, documents: List[Dict], keywords_found: bool, 
                      query_language: str, max_tokens: int = 2000, temperature: float = 0.1) -> str:
        """Generate an answer based on retrieved documents, handling single-PDF or multi-PDF comparisons."""
        try:
            if not documents:
                return "Ямар ч холбогдох баримт бичиг олдсонгүй. Илүү тодорхой асуулт оруулна уу эсвэл PDF өгөгдөл байгаа эсэхийг шалгана уу."

            # Group documents by source PDF
            pdf_groups = {}
            for doc in documents:
                pdf_name = doc['metadata'].get('source_name', 'Unknown')
                if pdf_name not in pdf_groups:
                    pdf_groups[pdf_name] = []
                pdf_groups[pdf_name].append(doc)

            # Detect if comparison is requested
            is_comparison = detect_comparison_request(query)

            # Prepare context for each PDF
            context_parts = []
            for pdf_name, docs in pdf_groups.items():
                pdf_context = "\n\n".join([
                    f"{doc['metadata'].get('source_name', 'Тодорхойгүй')} (Хуудас {doc['metadata'].get('page_number', 'unknown')}): {doc['content']}"
                    for doc in docs
                ])
                context_parts.append(f"PDF {pdf_name}:\n{pdf_context}")

            context = "\n\n".join(context_parts)

            # Adjust prompt for comparison or single-PDF query
            if is_comparison:
                prompt = f"""
                Та баримт бичгүүдийн контекст дээр үндэслэн асуултанд хариулах мэргэжлийн туслах юм. Таны даалгавар бол Монгол хэл дээр авсаархан хариулт өгчих байх, дараах зааврыг баримтална уу:
                - never give me a table output only give me the following answers
                - **Ерөнхий дүгнэлт**: Харьцуулалт шаардлагатай бол '{', '.join(pdf_groups.keys())}' PDF-үүдийн тоон утгуудыг нэгтгэж, харьцуул (жишээ нь, 6,768.00 кВт.ц vs 274,720.00 кВт.ц). Ялгааг хувь эсвэл харьцаагаар тооцоол (жишээ нь, '40 дахин их'). Товч, тоо баримтад тулгуурласан дүгнэлт хий.
                - Тайлбарыг Markdown болон HTML форматтайгаар (жишээ нь, **Тайлбар**) бич.
                - never do \n between the Texts
                - **Яг Утгыг Хадгалах**: Баримт бичгийн контекстээс яг ижил үг хэллэг, огноо, тоон мэдээлэл (жишээ нь, '6,768.00 кВт.ц'), нэгжийг өөрчлөлтгүй ашигла.
                - **Түлхүүр Үгсийг Тодруулах**: Гол нэр томъёог (жишээ нь, 'ЦРП-5 ЗМЗ яч18', 'Хэрэглээ') Markdown синтаксаар тодруул (**Хэрэглээ**).
                - **Эх сурвалж**: Always include the source of the information in the format **Эх сурвалж**: [PDF names, page numbers].
                - **Хангалтгүй Контекст**: Хэрвээ контекст хангалтгүй бол тийм гэж хэлж, тайлбарла.
                - never use \n when getting newline that is it
                Баримт Бичгийн Контекст:
                {context}
                Асуулт: {query}
                Хариултыг зөв Markdown болон HTML форматтайгаар. дараа нь ерөнхий дүгнэлтийг оруул. **Эх сурвалж** оруулахыг сана.
                """
            else:
                prompt = f"""
                Та баримт бичгүүдийн контекст дээр үндэслэн асуултанд хариулах мэргэжлийн туслах юм. Таны даалгавар бол Монгол хэл дээр авсаархан хариулт өгчих байх, дараах зааврыг баримтална уу:
                - never give me a table output only give me the following answers
                - **Ерөнхий дүгнэлт**: Контекстээс гол мэдээллийг нэгтгэн, товч дүгнэлт хий (жишээ нь, 'Хэрэглээ: 6,768.00 кВт.ц'). Тоон мэдээлэлд тулгуурла.
                - Тайлбарыг Markdown болон HTML форматтайгаар (жишээ нь, **Тайлбар**) бич.
                - never do \n between the Texts
                - **Яг Утгыг Хадгалах**: Баримт бичгийн контекстээс яг ижил үг хэллэг, огноо, тоон мэдээлэл (жишээ нь, '6,768.00 кВт.ц'), нэгжийг өөрчлөлтгүй ашигла.
                - **Түлхүүр Үгсийг Тодруулах**: Гол нэр томъёог (жишээ нь, 'ЦРП-5 ЗМЗ яч18', 'Хэрэглээ') Markdown синтаксаар тодруул (**Хэрэглээ**).
                - **Эх сурвалж**: Always include the source of the information in the format **Эх сурвалж**: [PDF names, page numbers].
                - **Хангалтгүй Контекст**: Хэрвээ контекст хангалтгүй бол тийм гэж хэлж, тайлбарла.
                - never use \n when getting newline that is it
                Баримт Бичгийн Контекст:
                {context}
                Асуулт: {query}
                Хариултыг зөв Markdown болон HTML форматтайгаар. дараа нь ерөнхий дүгнэлтийг оруул. **Эх сурвалж** оруулахыг сана.
                """

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Та туслагч туслах юм."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=temperature
            )
            answer = response.choices[0].message.content.strip()
            if not keywords_found:
                answer += " Хариулт нь боломжтой мэдээлэлд үндэслэсэн боловч яг таг тохирох мэдээлэл олдсонгүй. Илүү тодорхой асуулт оруулна уу."
            return answer
        except Exception as e:
            logger.error(f"Хариулт үүсгэхэд алдаа гарлаа: {str(e)}")
            return f"Хариулт үүсгэхэд алдаа гарлаа: {str(e)}."

    def validate_answer(self, query: str, answer: str, documents: List[Dict]) -> str:
        """Validate the generated answer against the query and documents."""
        if not documents:
            return "Асуултанд ямар ч холбогдох баримт бичиг олдсонгүй. Илүү тодорхой асуулт оруулна уу эсвэл PDF өгөгдөл байгаа эсэхийг шалгана уу."
        validation_prompt = f"""
        Асуултанд хариулт зохистой, холбогдох эсэхийг контекстоос шалга.
        Асуулт: {query}
        Хариулт: {answer}
        Контекст: {" ".join([doc['content'][:200] for doc in documents])}
        Хэрвээ зохистой, холбогдох бол 'Valid' гэж буцаа, эсвэл товч тайлбар өг.
        """
        try:
            validation_response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Та баталгаажуулагч туслах юм."},
                    {"role": "user", "content": validation_prompt}
                ],
                max_tokens=100,
                temperature=0.1
            )
            validation_result = validation_response.choices[0].message.content.strip()
            if validation_result == "Valid":
                return answer
            else:
                return f"{answer}"
        except Exception as e:
            logger.error(f"Баталгаажуулалт амжилтгүй боллоо: {str(e)}")
            return f"{answer}"

    def run(self, query: str, max_tokens: int = 2000, temperature: float = 0.1) -> str:
        """Run the PDFAgent to process a query, handling single or multi-PDF requests."""
        query_language = detect_language(query)
        logger.info(f"Detected query language: {'Mongolian' if query_language == 'mon' else 'English'}")
        
        # Get available PDFs
        input_dir = "data/pdfs"
        available_pdfs = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
        
        # Parse mentioned PDFs
        pdf_name_filter = parse_pdf_names(query, available_pdfs)
        logger.info(f"PDFs to query: {pdf_name_filter if pdf_name_filter else 'All available PDFs'}")
        
        # Search knowledge base
        documents, keywords_found = self.search_knowledge(query, pdf_name_filter=pdf_name_filter)
        
        # Generate and validate answer
        answer = self.generate_answer(query, documents, keywords_found, query_language, max_tokens, temperature)
        validated_answer = self.validate_answer(query, answer, documents)
        return validated_answer

def extract_emails(sentence):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, sentence)
    logger.info(f"Extracted emails from '{sentence}': {emails}")
    return emails

def send_email(recipient, subject, body, query, answer, query_language):
    recipient_name = recipient.split('@')[0].capitalize()
    if not subject:
        subject = "Таны Асуултын Хариу"
        if "invoice" in query.lower():
            subject = "Microsoft-ийн Нэхэмжлэлийн Дэлгэрэнгүй"
        elif "хандах эрх" in query.lower():
            subject = "Хандах Эрхийн Журмын Дэлгэрэнгүй"
    try:
        clean_answer = html.unescape(re.sub(r'<[^>]+>', '', answer))
        clean_answer = re.sub(r'###\s*[^#]+', '', clean_answer)
        clean_answer = clean_answer.strip()
    except Exception as e:
        logger.warning(f"Failed to clean answer: {str(e)}")
        clean_answer = answer
    details_text = clean_answer
    formatted_body = f"""
Эрхэм {recipient_name},

Таны асуултын хариуг доор харуулав:

{details_text}

Хүндэтгэсэн,
Coretech
"""
    msg = MIMEText(formatted_body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient
    logger.info(f"Attempting to send email to {recipient} with subject '{subject}'")
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        logger.info(f"Email sent successfully to {recipient}")
    except Exception as e:
        logger.error(f"Failed to send email to {recipient}: {str(e)}")
        raise Exception(f"Failed to send email: {str(e)}")

def initialize_pdf_processing():
    global pdf_agent
    try:
        logger.info("Starting initialization of PDF processing...")
        ensure_pdf_images()
        pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
        logger.info(f"Found {len(pdf_files)} PDFs to process: {pdf_files}")
        if not pdf_files:
            logger.warning("No PDFs found. Creating empty collection.")
            chroma_client = initialize_chroma_client()
            unified_collection = chroma_client.get_or_create_collection(
                name="facts_combined",
                metadata={"hnsw:space": "cosine"}
            )
            with agent_lock:
                pdf_agent = PDFAgent(collection=unified_collection, api_key=OPENAI_API_KEY)
                logger.info("Fallback PDFAgent initialized with empty collection.")
            return
        if os.path.exists(output_file):
            os.remove(output_file)
        collections = {}
        for pdf_file in pdf_files:
            pdf_name = os.path.splitext(pdf_file)[0]
            logger.info(f"Processing PDF: {pdf_name}")
            session_id = str(uuid.uuid4())
            progress_queues[session_id] = queue.Queue()
            try:
                collection = initialize_chroma_collection(pdf_name)
                collections[pdf_name] = collection
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"\n=== Processing PDF: {pdf_name} ===\n")
                process_pdf(pdf_name, output_file, output_language="English", session_id=session_id)
            except Exception as e:
                logger.error(f"Failed to process PDF {pdf_name}: {str(e)}")
                continue
        if not collections:
            logger.warning("No PDFs processed successfully. Creating empty collection.")
            chroma_client = initialize_chroma_client()
            unified_collection = chroma_client.get_or_create_collection(
                name="facts_combined",
                metadata={"hnsw:space": "cosine"}
            )
            with agent_lock:
                pdf_agent = PDFAgent(collection=unified_collection, api_key=OPENAI_API_KEY)
                logger.info("Fallback PDFAgent initialized with empty collection.")
            return
        save_vector_data(output_file, collections)
        logger.info("Loading```python unified knowledge base for RAG...")
        unified_collection = load_unified_knowledge_base()
        logger.info("Unified knowledge base loaded successfully.")
        with agent_lock:
            pdf_agent = PDFAgent(collection=unified_collection, api_key=OPENAI_API_KEY)
            logger.info("PDFAgent initialized successfully.")
    except Exception as e:
        logger.error(f"Initialization failed: {str(e)}")
        raise

def initialize_agents():
    global excel_agent, pdf_agent
    # Process Excels at startup
    process_excel_folder(excel_folder_path, table_metadata_file, financial_engine)
    # Excel Agent
    with agent_lock:
        excel_agent = ExcelAgent(financial_engine, table_metadata_file)
        logger.info("ExcelAgent initialized successfully.")
    # PDF Agent
    initialize_pdf_processing()

def hybrid_answer_question(question):
    global TABLE_METADATA_CACHE, COLUMN_METADATA_CACHE
    if TABLE_METADATA_CACHE is None or COLUMN_METADATA_CACHE is None:
        TABLE_METADATA_CACHE, COLUMN_METADATA_CACHE = load_table_and_column_metadata(table_metadata_file)

    # Clean the question (though no EXCEL/PDF suffixes are expected, kept for robustness)
    clean_question = question.strip()
    logger.info(f"Processing question: '{clean_question}', Running both ExcelAgent and PDFAgent")

    # Run ExcelAgent
    excel_answer = None
    excel_error = None
    try:
        excel_answer = excel_agent.run(clean_question)
        if not excel_answer or "Sorry" in excel_answer or "error" in excel_answer.lower():
            excel_error = "ExcelAgent failed to provide a valid answer or encountered an error."
            logger.warning(excel_error)
        else:
            logger.info("ExcelAgent successfully provided an answer")
    except Exception as e:
        excel_error = f"ExcelAgent error: {str(e)}"
        logger.error(excel_error)
        excel_answer = f"Алдаа: Excel өгөгдлөөр хариулах боломжгүй: {str(e)}"

    # Run PDFAgent
    pdf_answer = None
    pdf_error = None
    accessible_pdfs = [os.path.splitext(f)[0] for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    if not accessible_pdfs:
        logger.warning("No PDFs available to query")
        pdf_answer = "Асуух PDF байхгүй байна. PDF файлууд 'data/pdfs' хавтсанд байгаа эсэхийг шалгана уу."
    else:
        retries = 3
        for attempt in range(retries):
            try:
                pdf_answer = pdf_agent.run(
                    query=clean_question,
                    max_tokens=2000,
                    temperature=0.1
                )
                logger.info(f"PDF answer generated: {pdf_answer[:100]}...")
                break
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{retries} failed: {str(e)}. Retrying...")
                    time.sleep(5)
                else:
                    logger.error(f"Failed after {retries} attempts: {str(e)}")
                    pdf_answer = f"Алдаа: Асуултыг боловсруулахад алдаа гарлаа: {str(e)}. PDF өгөгдөл эсвэл ChromaDB тохиргоог шалгана уу."
                    pdf_error = f"PDFAgent error: {str(e)}"

    # Use Gemini 1.5 Flash to select the best answer
    prompt = f"""
    You are an expert assistant tasked with evaluating two responses to a user query and selecting the most suitable one based on relevance, accuracy, and completeness. The query and responses are provided below. Follow these instructions:

    - Query: "{clean_question}"
    - Excel Response: "{excel_answer}"
    - PDF Response: "{pdf_answer}"

    Instructions:
    1. Evaluate each response for relevance to the query, ensuring it addresses the specific question asked.
    2. Check for accuracy and completeness, prioritizing responses that provide concrete information (e.g., specific data, facts, or details) over generic or error messages.
    3. If a response contains error messages (e.g., "Sorry", "error", or "Алдаа"), consider it less suitable unless it provides partial relevant information.
    4. If both responses are errors or lack relevant information, select the one with a clearer explanation or suggest a default response.
    5. Return a JSON object with:
       - selected_source: Either "Excel" or "PDF" indicating the chosen response's source.
       - selected_answer: The full text of the chosen response.
       - explanation: A brief (up to 50 words) explanation of why this response was chosen.

    Output only the JSON object, with no additional formatting.
    """
    try:
        response = GEMINI_MODEL.generate_content(prompt)
        raw_response = response.text.strip()
        if raw_response.startswith('```json'):
            raw_response = raw_response[7:-3].strip()
        elif raw_response.startswith('```'):
            raw_response = raw_response[3:-3].strip()
        result = json.loads(raw_response)
        selected_source = result.get('selected_source', 'Excel')  # Default to Excel if selection fails
        selected_answer = result.get('selected_answer', excel_answer if selected_source == 'Excel' else pdf_answer)
        selection_explanation = result.get('explanation', 'No explanation provided.')
        logger.info(f"Gemini selected {selected_source} response: {selection_explanation}")
    except (GoogleAPIError, json.JSONDecodeError, Exception) as e:
        logger.error(f"Gemini evaluation failed: {str(e)}. Defaulting to Excel response.")
        selected_source = "Excel"
        selected_answer = excel_answer
        selection_explanation = f"Gemini evaluation failed: {str(e)}. Defaulted to Excel response."

    # Clean and format the selected answer
    selected_answer = re.sub(r'\n\s+', '\n\n', selected_answer.strip())
    selected_answer = re.sub(r'\t+', ' ', selected_answer)
    selected_answer = '\n'.join(line.strip() for line in selected_answer.split('\n') if line.strip())
    html_answer = markdown2.markdown(selected_answer, extras=["fenced-code-blocks", "cuddled-lists"])

    # Handle email sending for the selected answer
    emails = extract_emails(clean_question)
    if emails:
        logger.info(f"Emails found: {emails}")
        for email in emails:
            try:
                send_email(
                    recipient=email,
                    subject="",
                    body=selected_answer,
                    query=clean_question,
                    answer=selected_answer,
                    query_language='mon'
                )
            except Exception as e:
                logger.error(f"Failed to send email to {email}: {str(e)}")

    return {
        "response": html_answer,
        "detailed": html_answer,
        "source": selected_source
    }

if __name__ == "__main__":
    initialize_agents()
    while True:
        question = input("Questions shall be answered my good sir: ")
        result = hybrid_answer_question(question)
        print(f"Source: {result['source']}")
        print(f"Response:\n{result['response']}")