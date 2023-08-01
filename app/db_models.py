from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import Session
from .database import Base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert


class Employees(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    datetime = Column(String)
    department_id = Column(Integer, ForeignKey("departments.id"), nullable=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=True)

    def create_employees(cls, employees, db: Session):
        try:
            stmt = insert(Employees).values(employees)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint="employees_pkey",
                set_={
                    "name": stmt.excluded.name,
                    "datetime": stmt.excluded.datetime,
                    "department_id": stmt.excluded.department_id,
                    "job_id": stmt.excluded.job_id
                }
            )
            db.execute(on_conflict_stmt)
            db.commit()

        except SQLAlchemyError as e:
            # Log the error or handle it appropriately
            db.rollback()

class Jobs(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    job = Column(String)

    def create_jobs(cls, jobs, db: Session):
        try:
            stmt = insert(Jobs).values(jobs)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint="jobs_pkey",
                set_={
                    "job": stmt.excluded.job,
                }
            )
            db.execute(on_conflict_stmt)
            db.commit()

        except SQLAlchemyError as e:
            # Log the error or handle it appropriately
            db.rollback()

class Departments(Base):
    __tablename__ = "departments"

    id = Column(Integer, primary_key=True)
    department = Column(String)

    def create_departments(cls, departments, db: Session):
        print(departments)
        try:
            stmt = insert(Departments).values(departments)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint="departments_pkey",
                set_={
                    "department": stmt.excluded.department,
                }
            )
            db.execute(on_conflict_stmt)
            db.commit()

        except SQLAlchemyError as e:
            # Log the error or handle it appropriately
            db.rollback()
