import json
import make_company_list
from make_company_list import msci_list, eurostoxx_list
from crawling import scrape_google_and_order
import requests
from bs4 import BeautifulSoup
from config import headers
import pandas as pd
import os

def file_exists(filepath):
    return os.path.exists(filepath)

years_to_search = ["2017", "2018", "2019", "2020", "2021", "2022"]
companies = make_company_list.make_clean_list(msci_list, eurostoxx_list)
print(len(companies))

columns = ['sustainability reports found', 'annual reports found', 'sustainability reports doubt', 'annual reports doubt', 'no sustainability report', 'no annual report']

# Create an empty DataFrame with the specified columns
results_df = pd.DataFrame(index = years_to_search,columns=columns)
results_df = results_df.fillna(0)


# Get statistics
path = f"C:/SFDH/Downloaded Reports/"
for company in companies:
    for year in years_to_search:
        filepath_doubt_annual = path + "doubtPDFs/" + company + "/" +  year +"_annual_report.pdf"
        if file_exists(filepath_doubt_annual):
            results_df.loc[year, 'annual reports doubt'] += 1
        filepath_found_annual = path + "foundPDFs/" + company + "/" +  year +"_annual_report.pdf"
        if file_exists(filepath_found_annual):
            results_df.loc[year, 'annual reports found'] += 1
        filepath_doubt_sustainability = path + "doubtPDFs/" + company + "/" +  year +"_sustainability_report.pdf"
        if file_exists(filepath_doubt_sustainability):
            results_df.loc[year, 'sustainability reports doubt'] += 1
        filepath_found_sustainability = path + "foundPDFs/" + company + "/" +  year +"_sustainability_report.pdf"
        if file_exists(filepath_found_sustainability):
            results_df.loc[year, 'sustainability reports found'] += 1

# fill in missing reports
for year in years_to_search:
    results_df.loc[year, 'no annual report'] = len(companies) - results_df.loc[year, 'annual reports doubt'] - results_df.loc[year, 'annual reports found']
    results_df.loc[year, 'no sustainability report'] = len(companies) - results_df.loc[year, 'sustainability reports doubt'] - results_df.loc[year, 'sustainability reports found']


# Print the DataFrame
print(results_df)

percentage_df = (results_df / len(companies)) 
print(percentage_df)

# Save as excel
excel_file_path = 'result_statistics.xlsx'
results_df.to_excel(excel_file_path, index = True)
percentage_df.to_excel('percentage_' + excel_file_path, index = True)