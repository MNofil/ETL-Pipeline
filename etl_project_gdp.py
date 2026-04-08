# importing libraries
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3
import re

# Code for ETL operations on Country-GDP data

def main():
    csv_file = "./Countries_by_GDP.csv"
    table_name = "Countries_by_GDP"
    attribute_list = ["Country", "GDP_USD_billion"]
    db_file = "World_Economies.db"

    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    
    # Log the initialization of the ETL process 
    log_progress("ETL Job Started") 

    # Log the beginning of the Extraction process 
    log_progress("Extract phase Started")
    extracted_data = extract(url, attribute_list)

    # Log the completion of the Extraction process
    log_progress("Extract phase Completed")
    
    # Log the beginning of the Transformation process 
    log_progress("Transform phase Started") 
    transformed_data = transform(extracted_data)
    
    # Log the completion of the Transformation process
    log_progress("Transform phase Completed")
    
    # Log the beginning of the Load process 
    log_progress("Load phase Started")
    load_to_csv(transformed_data, csv_file)
    
    # Log the completion of the first Load phase
    log_progress("Load phase Completed (CSV)")
    
    # Load to database
    sql_connection = sqlite3.connect(db_file)
    load_to_db(transformed_data, sql_connection, table_name)
    
    # Log the completion of the second Load phase
    log_progress("Load phase Completed (Database)")
    
    log_progress('Running the query')
    query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billion >= 100"
    run_query(query_statement, sql_connection)

    # Log the completion of the ETL process
    log_progress("ETL Job Completed")
    
    sql_connection.close()


def extract(url, table_attribs):
	'''
	This function extracts the required
	information from the website and saves it to a dataframe. The
	function returns the dataframe for further processing.
	'''
	data_list = []
	df = pd.DataFrame(columns=table_attribs)
	html_page = requests.get(url).text
	data = BeautifulSoup(html_page, 'html.parser')
	tables = data.find_all("table")
	rows = tables[2].find_all("tr")
	for row in rows:
		col = row.find_all("td")
		if len(col) >= 3:
			country = col[0].text.strip()
			gdp = col[2].text.strip()

			# Cleaning reference like [1], [2], etc. from country names
			country = re.sub(r"\[.*?\]", "", country)
			gdp = re.sub(r"\[.*?\]", "", gdp)

			# appending the cleaned data to the list
			data_list.append({
				"Country": country,
				"GDP_USD_billion": gdp
			})
	df1 = pd.DataFrame(data_list)
	df = pd.concat([df, df1], ignore_index=True)
	return df


def transform(df):
	''' 
	This function converts the GDP information from Currency
	format to float value, transforms the information of GDP from
	USD (Millions) to USD (Billions) rounding to 2 decimal places.
	The function returns the transformed dataframe.
	'''
	# Removing reference like [1], [2], etc. from GDP values and converting to float
	df["GDP_USD_billion"] = df["GDP_USD_billion"].apply(lambda x: re.sub(r"\[.*?\]", "", x))
	# Removing commas
	df["GDP_USD_billion"] = df["GDP_USD_billion"].str.replace(",", "", regex=False)
	# Removing any non-numeric characters
	df["GDP_USD_billion"] = df["GDP_USD_billion"].apply(lambda x: re.sub(r"[^\d.]", "", x))
	# Convert to numeric
	df["GDP_USD_billion"] = pd.to_numeric(df["GDP_USD_billion"], errors="coerce")
	# Rounding to 2 decimal places and converting from Millions to Billions
	df["GDP_USD_billion"] = round(df["GDP_USD_billion"] / 1000, 2)
	return df


def load_to_csv(df, csv_path):
	''' 
	This function saves the final dataframe as a `CSV` file 
	in the provided path. Function returns nothing.
	'''
	df.to_csv(csv_path, index=False)
	

def load_to_db(df, sql_connection, table_name):
	'''
	This function saves the final dataframe as a database table
	with the provided name. Function returns nothing.
	'''
	df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
	'''
	This function runs the stated query on the database table and
	prints the output on the terminal. Function returns nothing.
	'''
	return pd.read_sql_query(query_statement, sql_connection)


def log_progress(message):
	''' 
	This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing
	'''
	log_file = "etl_log.txt"
	# Year-Monthname-Day-Hour-Minute-Second 
	timestamp_format = '%Y-%m-%d-%H:%M:%S'
	# get current timestamp 
	now = datetime.now()  
	timestamp = now.strftime(timestamp_format) 
	with open(log_file,"a") as f: 
		f.write(timestamp + ',' + message + '\n')


if __name__ == "__main__":
	main() 	
