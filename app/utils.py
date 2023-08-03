def generate_table_html_hiring_more_than_avg(results: dict):
    """
    Create the response table for the departments hiring more than the 2021 average.
    Params:
    results (dict): Dictionary containing the rows content of the response table
    """
    result_list = [
        {
                'id': department_id,
                'department': department,
                'hired': hired
            }
            for department_id, department, hired in results
        ]

    # Generate the HTML table
    table_html = "<table border='1'>\n<tr><th>id</th><th>department</th><th>hired</th></tr>\n"

    for row in result_list:
        table_html += f"<tr><td>{row['id']}</td><td>{row['department']}</td><td>{row['hired']}</td></tr>\n"

    table_html += "</table>"
    return table_html

def generate_table_html_employees_per_quarter(results):
    """
    Create the response table for the employees hired per quarter in 2021
    Params:
    results (dict): Dictionary containing the rows content of the response table
    """
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
