import io
from app.upload_file_utils import *
from fastapi import HTTPException
import pytest


def test_is_allowed_file():
    assert is_allowed_file('jobs')
    assert is_allowed_file('departments')
    assert is_allowed_file('employees')
    assert is_allowed_file('X') == False

def test_get_file_schema():
    assert_all_columns_match(get_file_schema('jobs').columns, get_jobs_schema().columns)
    assert_all_columns_match(get_file_schema('departments').columns, get_departments_schema().columns)
    assert_all_columns_match(get_file_schema('employees').columns, get_employees_schema().columns)
    with pytest.raises(HTTPException) as e:
        get_file_schema('X')
        assert e == "File type: 'X' doesn't contain schema information"

def assert_all_columns_match(cols_schema1, cols_schema2):
    for col1, col2 in zip(cols_schema1, cols_schema2):
        assert col1.name == col2.name

def test_validate_is_valid_file_type():
    assert validate_is_valid_file_type("jobs") is None
    assert validate_is_valid_file_type("departments") is None
    assert validate_is_valid_file_type("employees") is None
    with pytest.raises(HTTPException) as e:
        validate_is_valid_file_type("X")
        assert e == "File type: 'X' is not valid file"
    with pytest.raises(HTTPException) as e:
        validate_is_valid_file_type("EMPLOYEES")
        assert e == "File type: 'EMPLOYEES' is not valid file"

def test_validate_file_content():
    # Dummy data
    data = [
        {'id': 1, 'job': 'engineer'},
        {'id': 2, 'job': 'manager'},
        {'id': 3, 'job': 'technician'}
    ]

    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    assert validate_file_content(
        get_file_schema('jobs'),
        df
    ) is None

    # Wrong Dummy data
    data = [
        {'id': 1, 'job': 'engineer', 'col': 1},
        {'id': 2, 'job': 'manager', 'col': 1},
        {'id': 3, 'job': 'technician', 'col': 1}
    ]

    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    with pytest.raises(HTTPException) as e:
        validate_file_content(
            get_file_schema('jobs'),
            df
        )

def test_assign_columns_no_header_file():
    data = [
        {'id': 1, 'job': 'engineer', 'col': 1},
        {'id': 2, 'job': 'manager', 'col': 1},
        {'id': 3, 'job': 'technician', 'col': 1}
    ]

    # Create the pandas DataFrame
    df = pd.DataFrame(data)

    with pytest.raises(HTTPException) as e:
        assign_columns_no_header_file(df, get_jobs_schema())

def test_read_comma_separated_no_header():
    binary_data = b"1,Sales"
    binary_io = io.BytesIO(binary_data)
    df = read_comma_separated_no_header(UploadFile(binary_io))
    assert (type(df) == pd.DataFrame)

    binary_data = b""
    binary_io = io.BytesIO(binary_data)
    with pytest.raises(HTTPException) as e:
        read_comma_separated_no_header(UploadFile(binary_io))
    

    binary_data = b",,,"
    binary_io = io.BytesIO(binary_data)
    assert (type(df) == pd.DataFrame)

    binary_data = b";;;;"
    binary_io = io.BytesIO(binary_data)
    assert (type(df) == pd.DataFrame)

    binary_data = b";;;;"
    binary_io = io.BytesIO(binary_data)
    assert (type(df) == pd.DataFrame)

def test_validate_is_csv_file():
    binary_data = b"1,Sales"
    binary_io = io.BytesIO(binary_data)
    df = read_comma_separated_no_header(UploadFile(binary_io, filename="test.csv"))
    assert (type(df) == pd.DataFrame)

    with pytest.raises(HTTPException) as e:
        read_comma_separated_no_header(UploadFile(binary_io, filename="test.txt"))
    
    with pytest.raises(HTTPException) as e:
        read_comma_separated_no_header(UploadFile(binary_io, filename="test"))

    with pytest.raises(HTTPException) as e:
        read_comma_separated_no_header(UploadFile(binary_io, filename=""))

