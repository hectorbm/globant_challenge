from pandas_schema import Schema, Column
from fastapi import HTTPException
import pandas as pd


def is_allowed_file(file_type) -> bool:
    valid_files = [
        'jobs', 
        'departments', 
        'employees'
    ]

    return file_type in valid_files

def get_employees_schema() -> Schema:
    return Schema([
        Column('id'),
        Column('name'),
        Column('datetime'),
        Column('department_id'),
        Column('job_id')
    ])


def get_departments_schema() -> Schema:
    return Schema([
        Column('id'),
        Column('department')
    ])

def get_jobs_schema() -> Schema:
    return Schema([
        Column('id'),
        Column('job')
    ])

def get_file_schema(file_type) -> Schema:
    valid_file_schemas = {
        'jobs': get_jobs_schema(),
        'departments': get_departments_schema(),
        'employees': get_employees_schema()
    }

    try:
        schema = valid_file_schemas[file_type]

    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="File type: '{}' doesn't contain schema information".format(file_type)
        )

    return schema

def validate_is_csv_file(file) -> None:
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=400,
            detail="The file format is not allowed, please use CSV files!"
        )

def validate_is_valid_file_type(file_type):
    if not is_allowed_file(file_type):
        raise HTTPException(
            status_code=400,
            detail="File type: '{}' is not valid file".format(file_type)
        )

def validate_file_content(schema, df):
    errors = schema.validate(df)
    if len(errors) > 0:
        errors = [error.message for error in errors]
        
        raise HTTPException(
            status_code=400,
            detail={
                "status": "Upload Failure",
                "content_validation_error": errors
            }
        )

def read_comma_separated_no_header(file):
    try:
        df = pd.read_csv(file.file, sep=',', header=None)
    except pd.errors.ParserError as e:
        raise HTTPException(
            status_code=400,
            detail="Parsing error reading the CSV file, please verify the file content!"
        )
        
    return df

def assign_columns_no_header_file(df, schema):
    file_cols = [col.name for col in schema.columns]

    try:
        df.columns = file_cols
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "status": "Upload Failure",
                "error_message": "The file provided doesn't match the expected schema!",
                "suggestion": "Please provide file with the following schema: {}".format(str(file_cols))
            }
        )