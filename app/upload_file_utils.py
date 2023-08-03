from pandas_schema import Schema, Column
from fastapi import HTTPException, UploadFile
import pandas as pd
from pandas_schema.validation import CustomSeriesValidation
import re


def is_integer_or_float_with_decimal_zero_or_null(series):
    try:
        # Check if each value is an integer, a float (ending with '.0'), or null (NaN)
        is_int_or_float_with_decimal_zero_or_null = series.apply(
            lambda x: isinstance(x, (int, float)) and (x.is_integer() if isinstance(x, float) else True) or pd.isna(x)
        )
    except AttributeError:
        # If any value cannot be checked, return False
        return pd.Series(False, index=series.index)
    return is_int_or_float_with_decimal_zero_or_null

def is_integer_or_float_with_decimal_zero(series):
    try:
        # Check if each value is an integer or a float (ending with '.0')
        is_int_or_float_with_decimal_zero = series.apply(
            lambda x: isinstance(x, (int, float)) and (x.is_integer() if isinstance(x, float) else True)
        )
    except AttributeError:
        # If any value cannot be checked, return False
        return pd.Series(False, index=series.index)
    return is_int_or_float_with_decimal_zero

def is_iso_timestamp(series):
    try:
        # Regular expression pattern for ISO format timestamp
        iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'

        # Check if each value matches the ISO format pattern, or if it's a non-string value (including None and NaN)
        is_iso_timestamp = series.apply(lambda x: pd.isna(x) or (isinstance(x, str) and bool(re.match(iso_pattern, x))))
    except AttributeError:
        # If any value cannot be checked, return False
        return pd.Series(False, index=series.index)
    return is_iso_timestamp

def is_string(series):
    try:
        # Check if each value matches the ISO format pattern, or if it's a non-string value (including None and NaN)
        is_string = series.apply(lambda x: pd.isna(x) or isinstance(x, str))
    except AttributeError:
        # If any value cannot be checked, return False
        return pd.Series(False, index=series.index)
    return is_string


def is_allowed_file(file_type) -> bool:
    """
    Return if the file is allowed to start the upload/validation process.
    Parameters:
    file_type: str with the file_type provided as upload param
    """
    valid_files = [
        'jobs', 
        'departments', 
        'employees'
    ]

    return file_type in valid_files

def get_employees_schema() -> Schema:
    """
    Returns: 
    Schema with the expected columns for the file employees
    """
    return Schema([
        Column('id', [CustomSeriesValidation(is_integer_or_float_with_decimal_zero, 'Column should contain only integers.')]),
        Column('name', [CustomSeriesValidation(is_string, 'Column should contain only strings.')]),
        Column('datetime', [CustomSeriesValidation(is_iso_timestamp, 'Column should contain ISO Timestamps.')]),
        Column('department_id', [CustomSeriesValidation(is_integer_or_float_with_decimal_zero_or_null, 'Column should contain only integers.')]),
        Column('job_id', [CustomSeriesValidation(is_integer_or_float_with_decimal_zero_or_null, 'Column should contain only integers.')])
    ])

def get_departments_schema() -> Schema:
    """
    Returns: 
    Schema with the expected columns for the file departments
    """
    return Schema([
        Column('id', [CustomSeriesValidation(is_integer_or_float_with_decimal_zero, 'Column should contain only integers.')]),
        Column('department', [CustomSeriesValidation(is_string, 'Column should contain only strings.')])
    ])

def get_jobs_schema() -> Schema:
    """
    Returns: 
    Schema with the expected columns for the file jobs
    """
    return Schema([
        Column('id', [CustomSeriesValidation(is_integer_or_float_with_decimal_zero, 'Column should contain only integers.')]),
        Column('job', [CustomSeriesValidation(is_string, 'Column should contain only strings.')])
    ])

def get_file_schema(file_type) -> Schema:
    """
    Get the correct schema depending on the file_type to validate.
    Params:
    file_type: str with the param of the file updated.

    Returns: 
    Schema with the expected columns for the file employees.
    """
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

def validate_is_csv_file(file: UploadFile) -> None:
    """
    Validate a file is .csv
    Params:
    file (Upload File): The file uploaded by the user.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=400,
            detail="The file format is not allowed, please use CSV files!"
        )

def validate_is_valid_file_type(file_type: str):
    """
    Validate the file_type provided is in the allowed files provided the logic of is_allowed_file function 
    """
    if not is_allowed_file(file_type):
        raise HTTPException(
            status_code=400,
            detail="File type: '{}' is not valid file".format(file_type)
        )

def validate_file_content(schema: Schema, df: pd.DataFrame):
    """
    Validate the file against its defined Schema and potentially defined constraints, 
    use the pandas-schema library to perform the validation
    Params:
    schema (Schema): The Schema defined for the file.
    df (pd.DataFrame):  The pandas DataFrame containing the uploaded file information. 
    """
    errors = schema.validate(df)
    if len(errors) > 0:
        errors = [{
            "row": error.row, 
            "column": error.column, 
            "message": error.message
        } for error in errors]
        
        raise HTTPException(
            status_code=400,
            detail={
                "status": "Upload Failure",
                "content_validation_error": errors
            }
        )

def read_comma_separated_no_header(file: UploadFile):
    """
    Read a comma separated file uploaded by the user.
    Params:
    file (Upload File): The file uploaded by the user.
    """
    try:
        df = pd.read_csv(file.file, sep=',', header=None)
    except pd.errors.ParserError as e:
        raise HTTPException(
            status_code=400,
            detail="Parsing error reading the CSV file, please verify the file content!"
        )
    except pd.errors.EmptyDataError as e: 
        raise HTTPException(
            status_code=400,
            detail="The file provided was empty, please verify the uploaded file!"
        )
        
    return df

def assign_columns_no_header_file(df: pd.DataFrame, schema: Schema):
    """
    The files uploaded don't contain a header, therefore we recover the original name of each column based on the schema, 
    if the file column count doesn't match the expectation, an error will be raised.
    Params:
    schema (Schema): The Schema defined for the file.
    df (pd.DataFrame):  The pandas DataFrame containing the uploaded file information.
    """
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