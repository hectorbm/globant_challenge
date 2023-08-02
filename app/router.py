# api/router.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from .upload_file_utils import *
from .database import get_db
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, cast, TIMESTAMP, case
from .db_models import Departments, Jobs, Employees
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

    if file_type == 'employees':
        df['job_id'] = df['job_id'].fillna(0).astype(int)
        df['department_id'] = df['department_id'].fillna(0).astype(int)

    # Load the file content into SQL
    bulk_upsert_data_to_db(file_type, db, df)
    
    return {"message": "File uploaded correctly"}

def bulk_upsert_data_to_db(file_type, db, df):
    jobs = []
    employees = []
    departments = []

    for idx, row in df.iterrows():
        if file_type == 'jobs':
            jobs.append({'id': row['id'], 'job': row['job']})
            
        elif file_type == 'employees':
            employees.append(
                {
                "id": row['id'], 
                "name": row['name'], 
                "datetime": row['datetime'],
                "department_id": row['department_id'] if row['department_id'] > 0 else None, 
                "job_id": row['job_id'] if row['job_id'] > 0 else None
                }
            )

        else:
            departments.append(
                {'id': row['id'], 'department': row['department']}
            )


    if file_type == 'jobs':    
        Jobs.create_jobs(Jobs, jobs, db)
    elif file_type == 'employees':    
        Employees.create_employees(Employees, employees=employees, db=db)
    else:
        Departments.create_departments(Departments, departments, db)

@router.get("/employeePerQuarters", response_class=HTMLResponse)
def get_employee_quarters(db: Session = Depends(get_db)):
    try:
        
        results = get_results_sql_employees_per_quarter(db)
        
        db.close()

        table_html = generate_table_html_employees_per_quarter(results)

        return table_html

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_results_sql_employees_per_quarter(db):
    return db.query(
            Departments.department,
            Jobs.job,
            func.sum(
                case(
                    (extract('quarter', cast(Employees.datetime, TIMESTAMP)) == '1', 1), 
                    else_= 0
                )
            ).label("Q1"),
            func.sum(
                case(
                    (extract('quarter', cast(Employees.datetime, TIMESTAMP)) == '2', 1), 
                    else_= 0
                )
            ).label("Q2"),
            func.sum(
                case(
                    (extract('quarter', cast(Employees.datetime, TIMESTAMP)) == '3', 1), 
                    else_= 0
                )
            ).label("Q3"),
            func.sum(
                case(
                    (extract('quarter', cast(Employees.datetime, TIMESTAMP)) == '4', 1), 
                    else_= 0
                )
            ).label("Q4")
            ).select_from(Employees).join(
                Jobs, Jobs.id == Employees.job_id
            ).join(
                Departments, Departments.id == Employees.department_id
            ).filter(
                extract("year", cast(Employees.datetime, TIMESTAMP)) == 2021
            ).group_by(
                Departments.department, Jobs.job
            ).order_by(
                Departments.department, Jobs.job
            ).all()

def generate_table_html_employees_per_quarter(results):
    result_list = [
        {
                'job': job_title,
                'department': department,
                'Q1': q1_sum,
                'Q2': q2_sum,
                'Q3': q3_sum,
                'Q4': q4_sum
            }
            for job_title, department, q1_sum, q2_sum, q3_sum, q4_sum in results
        ]

        # Generate the HTML table
    table_html = "<table border='1'>\n<tr><th>Department</th><th>Job</th><th>Q1</th><th>Q2</th><th>Q3</th><th>Q4</th></tr>\n"

    for row in result_list:
        table_html += f"<tr><td>{row['job']}</td><td>{row['department']}</td><td>{row['Q1']}</td><td>{row['Q2']}</td><td>{row['Q3']}</td><td>{row['Q4']}</td></tr>\n"

    table_html += "</table>"
    return table_html
   
@router.get("/departmentsHiringMoreThanAvg", response_class=HTMLResponse)
def get_hiring_more_than_avg( db: Session = Depends(get_db)):
    subquery = get_subquery_avg_hiring_per_department_only2021(db)

    # Get the average of all departments 2021
    avg_value = db.query(
        func.avg(subquery.c.hiring_count)
    ).all()[0][0]

    # Check if the average exist
    if avg_value is None:
        raise HTTPException(
            400,
            detail="Data does not contain the values to compute the average!"
        )

    result2 = get_sql_results_hiring_more_than_avg(db, avg_value)

    db.close()

    table_html = generate_table_html_hiring_more_than_avg(result2)

    return table_html

def get_sql_results_hiring_more_than_avg(db, avg_value):
    result2 = db.query(
        Departments.id,
        Departments.department,
        func.count().label("hired")
    ).select_from(Employees).join(
        Departments, Departments.id == Employees.department_id
    ).group_by(
        Departments.id, Departments.department
    ).having(
        func.count() > avg_value
    ).order_by(func.count().desc()).all()
    
    return result2

def generate_table_html_hiring_more_than_avg(result2):
    result_list = [
        {
                'id': department_id,
                'department': department,
                'hired': hired
            }
            for department_id, department, hired in result2
        ]

    # Generate the HTML table
    table_html = "<table border='1'>\n<tr><th>id</th><th>department</th><th>hired</th></tr>\n"

    for row in result_list:
        table_html += f"<tr><td>{row['id']}</td><td>{row['department']}</td><td>{row['hired']}</td></tr>\n"

    table_html += "</table>"
    return table_html

def get_subquery_avg_hiring_per_department_only2021(db):
    subquery = db.query(
        Employees.department_id,
        func.count().label("hiring_count")
    ).select_from(Employees).join(
        Departments, Departments.id == Employees.department_id
    ).filter(
        extract("year", cast(Employees.datetime, TIMESTAMP)) == 2021
    ).group_by(
        Employees.department_id
    ).subquery()
    
    return subquery