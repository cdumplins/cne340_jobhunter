import mysql.connector
import time
import json
import requests
from datetime import date
import html2text
# Connect to database
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='password',
                                   host='127.0.0.1', database='cne340')
    return conn
# Create the table structure
def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
                        id INT PRIMARY KEY auto_increment,
                        Job_id VARCHAR(50),
                        company VARCHAR(300),
                        Candidate_required_location VARCHAR(300),
                        Created_at DATE,
                        url VARCHAR(30000),
                        Title LONGBLOB,
                        Description LONGBLOB
                    )''')
# Query the database.
def query_sql(cursor, query, params=None):
    cursor.execute(query, params)
    return cursor
# Add a new job
def add_new_job(cursor, jobdetails):
    url = jobdetails["url"]
    id = jobdetails["id"]
    title = html2text.html2text(jobdetails['title'])
    description = html2text.html2text(jobdetails['description'])
    company = jobdetails['company_name']
    location = jobdetails['candidate_required_location']
    date = jobdetails['publication_date'][0:10]
    query = ("INSERT INTO jobs(Job_id, company, Candidate_required_location, Created_at, url, Title, Description) "
             "VALUES(%s,%s,%s,%s,%s,%s,%s)")
    params = (id, company, location, date, url, title, description)
    query_sql(cursor, query, params)
# Check if job already exists
def check_if_job_exists(cursor, jobdetails):
    query = ("SELECT * FROM jobs WHERE Job_id=%s")
    params = (jobdetails['id'],)
    query_sql(cursor, query, params)
    return len(cursor.fetchall()) > 0
# Deletes job
def delete_job(cursor, jobdetails):
    query = "DELETE FROM jobs WHERE Job_id=%s"
    params = (jobdetails['id'],)
    query_sql(cursor, query, params)
# Grab new jobs from API and insert into database
def fetch_new_jobs():
    query = requests.get("https://remotive.io/api/remote-jobs")
    datas = json.loads(query.text)
    return datas
# Main function
def jobhunt(cursor):
    # Fetch new jobs from API
    jobpage = fetch_new_jobs()
    # Process each job in the API response
    for jobdetails in jobpage['jobs']:
        # Check if job already exists in database
        if check_if_job_exists(cursor, jobdetails):
            continue
        # Add new job to database
        add_new_job(cursor, jobdetails)
        # Notify user of new job posting
        print(f"New job added: {jobdetails['title']} ({jobdetails['company_name']})")
    # Sleep for 4 hours
    time.sleep(4 * 60 * 60)
# Setup portion of the program
def main():
    # Connect to MySQL database
    conn = connect_to_sql()
    cursor = conn.cursor()
    # Create table if it doesn't already exist
    create_tables(cursor)
    # Continuously fetch new jobs and insert into database
    while True:
        jobhunt(cursor)
    # Close MySQL connection
    conn.close()


if __name__ == '__main__':
    main()

