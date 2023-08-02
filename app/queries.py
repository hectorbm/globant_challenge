from sqlalchemy import func, extract, cast, TIMESTAMP, case
from .db_models import Departments, Jobs, Employees


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

def get_avg_all_departments_hiring_subquery(db, subquery):
    avg_value = db.query(
        func.avg(subquery.c.hiring_count)
    ).all()[0][0]
    
    return avg_value
