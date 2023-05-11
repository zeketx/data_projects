import datetime
import time
import pandas
import requests
import json
import logging
import pytz
import pyarrow
from google.oauth2 import service_account
from google.cloud import bigquery

# key credentials and access point
# Documentation link 👇🏼 used as a guide
# https://github.com/googleapis/python-bigquery/blob/35627d145a41d57768f19d4392ef235928e00f72/samples/load_table_dataframe.py
key_path= "/user/.../Documents/code/data/projectExample/subdir/credentials.json"
project_id="metro-rail-etl-v1"
dataset_id = "metro_data"
table="metro_data_set"
table_id="{}.{}.{}".format(project_id, dataset_id, table)
print("********* NAME OF TABLE IS", table_id)

def load_table_dataframe(key_path,project_id,table_id):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=project_id)
    # retrieve data from API call
    url = 'https://data.texas.gov/resource/u57e-u5kq.json?year_num=2022'
    response = requests.get(url)
    statusResponse = requests.request("GET", url)
    print(statusResponse)
    res_data = statusResponse.json()
    time.sleep(2) # give data time to load
    # Convert data to proper data types
    for row in res_data:
        row["route_sort_order"] = int(row["route_sort_order"])
        row["ridership_average"] = float(row["ridership_average"])
        row["ytd_total_ridership_count"] = float(row["ytd_total_ridership_count"])
        row["qtd_total_ridership_count"] = float(row["qtd_total_ridership_count"])
        row["py_qtd_total_ridership_count"] = float(row["py_qtd_total_ridership_count"])
        row["py_ytd_total_ridership_count"] = float(row["py_ytd_total_ridership_count"])
        row["py_ridership_average"] = float(row["py_ridership_average"])
        row["year_num"] = int(row["year_num"])
        row["month_end_date"] = pandas.to_datetime(row["month_end_date"], format="%Y%m%d").date()
        row["fiscal_year_number"] = int(row["fiscal_year_number"])
        row["fiscal_month_number"] = int(row["fiscal_month_number"])
        row["fiscal_quarter_number"] = int(row["fiscal_quarter_number"])
        row["show_on_dashboard"] = int(row["show_on_dashboard"])

    # create a new dataframe
    dataframe = pandas.DataFrame(
    res_data,
    # In the loaded table, the column order reflects the order of the
    # columns in the DataFrame.
        columns=[
            "route_sort_order",
            "ridership_average",
            "ytd_total_ridership_count",
            "qtd_total_ridership_count",
            "py_qtd_total_ridership_count",
            "py_ytd_total_ridership_count",
            "py_ridership_average",
            "year_num",
            "month_name",
            "month_short_name",
            "month_end_date",
            "month_year",
            "day_type",
            "mode_name",
            "sub_mode",
            "route_name",
            "data_as_of",
            "show_on_dashboard",
            "fiscal_year_number",
            "fiscal_month_number",
            "fiscal_quarter_number"
        ]
    )
    print(dataframe)
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("route_sort_order", bigquery.enums.SqlTypeNames.INTEGER),    
            bigquery.SchemaField("ridership_average", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("ytd_total_ridership_count", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("qtd_total_ridership_count", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("py_qtd_total_ridership_count", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("py_ytd_total_ridership_count", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("py_ridership_average", bigquery.enums.SqlTypeNames.FLOAT),    
            bigquery.SchemaField("year_num", bigquery.enums.SqlTypeNames.INTEGER),    
            bigquery.SchemaField("month_name", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("month_short_name", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("month_end_date", bigquery.enums.SqlTypeNames.DATE),    
            bigquery.SchemaField("month_year", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("day_type", bigquery.enums.SqlTypeNames.STRING),   
            bigquery.SchemaField("mode_name", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("sub_mode", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("route_name", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("data_as_of", bigquery.enums.SqlTypeNames.STRING),    
            bigquery.SchemaField("show_on_dashboard", bigquery.enums.SqlTypeNames.INTEGER),    
            bigquery.SchemaField("fiscal_year_number", bigquery.enums.SqlTypeNames.INTEGER),    
            bigquery.SchemaField("fiscal_month_number", bigquery.enums.SqlTypeNames.INTEGER),    
            bigquery.SchemaField("fiscal_quarter_number", bigquery.enums.SqlTypeNames.INTEGER)
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    data = client.get_table(table_id)  
    return data

data = load_table_dataframe(key_path,project_id, table_id)