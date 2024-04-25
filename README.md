# API Documentation

This API provides endpoints for uploading documents, retrieving employee hiring data, and fetching department hiring statistics.

## Endpoints

### 1. Root Endpoint

- **URL:** `/`
- **Method:** GET
- **Description:** Get the root endpoint of the application.
- **Response Model:** String
- **Example:**
    ```http
    GET /
    ```
    ```json
    "Simple application for a Challenge!"
    ```

### 2. Upload Document

- **URL:** `/uploadDocument/{file_type}`
- **Method:** POST
- **Description:** Uploads a .csv document of the specified file type to the database.
- **Parameters:**
    - `file_type` (String): Type of the file being uploaded.
    - `file` (File): The .csv file to be uploaded (form-data, file param).
- **Response Model:** Dictionary
- **Example:**
    ```http
    POST /uploadDocument/employee_data
    Content-Type: multipart/form-data
    
    [CSV File Content]
    ```
    ```json
    {
        "message": "File uploaded correctly"
    }
    ```

### 3. Employee Per Quarters

- **URL:** `/employeePerQuarters`
- **Method:** GET
- **Description:** Retrieves the hired employees in 2021 split into quarters and presents it as an HTML table.
- **Response:** HTML
- **Example:**
    ```http
    GET /employeePerQuarters
    ```
    ```html
    [HTML Table]
    ```

### 4. Departments Hiring More Than Average

- **URL:** `/departmentsHiringMoreThanAvg`
- **Method:** GET
- **Description:** Retrieves departments with hiring counts higher than the average the departments hired in 2021 and presents the results as an HTML table.
- **Response:** HTML
- **Example:**
    ```http
    GET /departmentsHiringMoreThanAvg
    ```
    ```html
    [HTML Table]
    ```

## Note
- All endpoints require a valid database connection.
- Detailed parameter descriptions and request/response examples are provided in the endpoint descriptions.
