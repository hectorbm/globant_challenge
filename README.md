## gChallenge Repository
Small repo for a coding challenge

Database:
For development porpuoses, i use a Postgres SQL database. The database is running in a docker container.

Steps to get the Postgres image and run the database in local environment:
1) docker pull postgres
2) Define a USER and PASSWORD, and path to keep the data of the database and use them in the next step
3) docker run --name postgresdb -e POSTGRES_USER=USER -e POSTGRES_PASSWORD=PASSWORD -p 5432:5432 -v /some/path/to/attach -d postgres 


API:
For simplicity i use FastAPI as i usually work with Python as main development language. 

Once the repository is cloned into the local machine, i suggest to create a Virtual Environment. 
After that, please use the requirements.txt file to install all the neccesary packages.
Please define the database connection params as Environment Variables, check the "database.py" file to get the required name for each parameter.

To run the application succesfully:
1) Run the database: docker run postgres
2) Run the API: uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload


Dockerfile:
Additionally, if the user wants to create a docker image and use containers. A dockerfile is provided.

### API Description

The API consist of 4 endpoints:

GET: "/" -> Returns a welcome message to the application. 

GET: "/employeePerQuarters" -> Returns the number of employees hired for each job and department in 2021 divided by quarter, ordered alphabetically by department and job.

GET: "/departmentsHiringMoreThanAvg" -> Returns the list of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

POST: "/uploadDocument/{file_type}" -> Allows you to upload a file of type: departments, jobs, employees. It will check for .csv types, schema check in columns and validate the content matches the defined schema restrictions, this could be enriched with more restrictions for better control of the allowed data. The API will insert the data using an UPSERT based on the id provided in the file for each table, in order to avoid duplicating the indexes, but accepting that some records could be re-defined in each upload, taking the latest one as the correct one.

