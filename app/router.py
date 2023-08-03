# api/router.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from .upload_file_utils import *
from .database import get_db
from .queries import *
from sqlalchemy.orm import Session
from .utils import *
import pandas as pd
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/", response_model=str)
def read_root():
    """Get the root endpoint of the application.

    Returns:
        str: A dictionary containing a welcome message.
    """

    return "Simple application for a Challenge!"

@router.post("/uploadDocument/{file_type}")
def upload_document(file_type: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Uploads a .csv document of the specified file_type to the database.

    Parameters:
        file_type (str): Type of the file being uploaded.
        file (UploadFile): The .csv file to be uploaded (form-data, file param).
        db (Session): The database session.

    Returns:
        dict: A dictionary with a validation status or success message.
    """
    # Validate the file provided is a .csv file
    validate_is_csv_file(file)

    # Validate against allowed files list
    validate_is_valid_file_type(file_type)

    # Get the schema with the restrictions for the file
    schema = get_file_schema(file_type)
    
    # Read a comma separated file without headers
    df = read_comma_separated_no_header(file)

    # Add column names for a file without headers
    assign_columns_no_header_file(df, schema)

    # Validate file content based on the restrictions defined in the schema configuration
    validate_file_content(schema, df)

    df = df.where(pd.notna(df), None)

    # Load the file content into SQL
    bulk_upsert_data_to_db(file_type, db, df)
    
    return {"message": "File uploaded correctly"}

@router.get("/employeePerQuarters", response_class=HTMLResponse)
def get_employee_quarters(db: Session = Depends(get_db)):
    """
    Retrieves the hired employees in 2021 splitted into quarters and presents it as an HTML table.

    Parameters:
        db (Session): The database session.
    """
    try:
        
        results = get_results_sql_employees_per_quarter(db)

        table_html = generate_table_html_employees_per_quarter(results)

        return table_html

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/departmentsHiringMoreThanAvg", response_class=HTMLResponse)
def get_hiring_more_than_avg( db: Session = Depends(get_db)):
    """
    Retrieves departments with hiring counts higher than the average the departments hired in 2021 and presents the results as an HTML table.

    Parameters:
        db (Session): The database session.
    """
    subquery = get_subquery_avg_hiring_per_department_only2021(db)

    # Get the average of all departments 2021
    avg_value = get_avg_all_departments_hiring_subquery(db, subquery)

    # Check if the average exist
    if avg_value is None:
        raise HTTPException(
            400,
            detail="Data does not contain the values to compute the average!"
        )

    results = get_sql_results_hiring_more_than_avg(db, avg_value)

    table_html = generate_table_html_hiring_more_than_avg(results)

    return table_html
