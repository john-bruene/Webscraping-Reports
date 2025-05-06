import json
from crawling import scrape_google_and_order
from download import download_pdf
from text_reading import read_and_reorder_pdf
from write_results import write_stats
import os, time
import shutil
import make_company_list
from make_company_list import msci_list, eurostoxx_list
from tqdm import tqdm
import pandas as pd
# from config import dbx

def make_query(company, year):
    # Hauptname extrahieren und in Anführungszeichen setzen
    company_main = company.strip().split()[0]
    quoted_company = f'"{company_main}"'

    # Smarter Query mit alternativen Begriffen, aber nur 1x API-Aufruf
    return f'{quoted_company} (sustainability OR ESG OR responsibility OR CSR) report {year} filetype:pdf'



def file_exists(filepath):
    return os.path.exists(filepath)

def find_where_to_start(file):
    """reads where the code last stopped in txt files (at which company and which year)

    Args:
        file (string): filepath of the txt to read

    Returns:
        int, int : indexes of year and company where it stopped
        bool : if the search or download has started
    """

    last_comp_file = open(file,'r')
    last_comp_line = last_comp_file.read()
    last_comp_file.close()

    if not last_comp_line == "" and not last_comp_line == "\n":
        last_year = last_comp_line.split('--')[0]
        last_comp = last_comp_line.split('--')[1]
        last_comp_index = list(companies).index(last_comp)
        last_year_index = years_to_search.index(last_year)
        started = True
    else:
        started = False
        last_year_index = 0
        last_comp_index = -1
    
    return last_year_index, last_comp_index, started


def find_links(last_comp_index, last_year_index):
    """loops through years and companies starting at specific index and scrapes google results for given query - then writes where it stopped

    Args:
        last_comp_index (int): index of company where it stopped
        last_year_index (int): index of year where it stopped
    """
    year_changed = False
    for year in tqdm(years_to_search[last_year_index:], desc="Finding Links", unit="year"): 
        # when we change year, we want to start at the first company (-1 because we want to start 0 and we do +1)
        if year_changed:
            last_comp_index = -1
        if last_comp_index+1 == len(companies):
            break
        for company in tqdm(companies[last_comp_index+1:], desc="Finding Links", unit=f"company for year {year}"):
            #print(company)
            #scrape_google_and_order(company + " annual report " + year + " filetype:pdf", year, company)
            query = make_query(company, year)
            scrape_google_and_order(query, year, company)
            f = open('stopped_search_at.txt','w')
            f.truncate()
            f.write(year + "--" + company)
            time.sleep(0.8)
        
        year_changed = True
        write_stats(year, "0")


def copy_found_file():
    """copies the list of found companies before download to new file 
    """
    original_found = 'found_results_0.json'
    new_found = 'found_results_1.json'
    shutil.copyfile(original_found, new_found)

def init_result_lists():
    """reads list result files and convert it in json tables

    Returns:
        array of dicts : lists of links
    """
    with open('doubt_results_0.json','r') as file:
        try:
            doubt_list = json.load(file)
        except:
            doubt_list = {}
    with open('found_results_0.json','r') as file:
        try:
            found_list = json.load(file)
        except:
            found_list = {}
    
    return doubt_list, found_list


def download_from_doubt_links(year, result, is_doubt):
    """downloading pdf from given doubtful link and reading it

    Args:
        year (srting)
        result (dict): element of doubt_list
        is_doubt (bool): if the link searched for is in the doubt_list or not

    Returns:
        bool: if the link searched for is in the doubt_list or not
    """
    
    #print(result)
    filepath = download_pdf(result["link"], year, result["company"], result["query"], "doubt")
    read_and_reorder_pdf(filepath, year, result["company"], result["query"], result["link"])
    is_doubt = True
    # if filepath != None:
    #     os.remove(filepath)
    
    return is_doubt

def download_from_found_links(year, result):
    """downloading pdf from given found link

    Args:
        year (string): _description_
        result (dict): element of found_list
    """
    #print(result)
    filepath = download_pdf(result["link"], year, result["company"], result["query"], "found")
    # if filepath != None:
    #     os.remove(filepath)
                

def download_read_pdfs(last_comp_index, last_year_index, doubt_list, found_list):
    """loops through years and companies starting at specific index and downloads report for given link
     Different behaviour if link searched for is in doubt_list or found_list - then writes where it stopped

    Args:
        last_comp_index (int): index of company where it stopped
        last_year_index (int): index of year where it stopped
        doubt_list (array of dicts): doubtful list of links
        found_list (array of dicts): right links
    """
    doubt_list_orig=doubt_list
    found_list_orig = found_list
    path = f"C:/SFDH/Downloaded Reports/"
    # Get two json lists of doubt reports separated by annual or sustainability
    doubt_annual_reports = {}
    doubt_sustainability_reports = {}

    for year, items in doubt_list.items():
        doubt_annual_reports[year] = []
        doubt_sustainability_reports[year] = []
        for item in items:
            if "annual report" in item["query"].lower():
                doubt_annual_reports[year].append(item)
            elif "sustainability report" in item["query"].lower():
                doubt_sustainability_reports[year].append(item)

    # Get two json lists of found reports separated by annual or sustainability
    found_annual_reports = {}
    found_sustainability_reports = {}

    for year, items in found_list.items():
        found_annual_reports[year] = []
        found_sustainability_reports[year] = []
        for item in items:
            if "annual report" in item["query"].lower():
                found_annual_reports[year].append(item)
            elif "sustainability report" in item["query"].lower():
                found_sustainability_reports[year].append(item)

    # download annual reports
    """doubt_list = doubt_annual_reports
    found_list = found_annual_reports
    year_changed = False
    for year in tqdm(years_to_search[last_year_index:], desc="Downloading Annual Reports", unit="year"):
        if year_changed:
            last_comp_index = -1
        if last_comp_index+1 == len(companies):
            break

        for company in tqdm(companies[last_comp_index+1:], desc="Downloading", unit=f"Annual Report for year {year}"):
            is_doubt = False
            if year in doubt_list.keys():
                for result in doubt_list[year]:
                    if company in result.values():
                        filepath_to_check = path + "doubtPDFs/" + company + "/" +  year +"_annual_report.pdf"
                        if file_exists(filepath_to_check):
                            break
                        else:    
                            is_doubt = download_from_doubt_links(year, result, is_doubt)
                            break

            if year in found_list.keys() and not is_doubt:
                for result in found_list[year]:
                    if company in result.values():
                        filepath_to_check = path + "foundPDFs/" + company + "/" +  year +"_annual_report.pdf"
                        if file_exists(filepath_to_check):
                            break
                        else:
                            download_from_found_links(year, result)
                            break

            f = open('stopped_download_at.txt','w')
            f.truncate()
            f.write(year + "--" + company)

        year_changed = True
        write_stats(year, "1")"""
    #aktuell auskommentiert, weil wir nur sustainability reports wollen

    # download sustainability reports
    doubt_list = doubt_sustainability_reports
    found_list = found_sustainability_reports
    year_changed = False
    for year in tqdm(years_to_search[last_year_index:], desc="Downloading Sustainability Reports", unit="year"):
        if year_changed:
            last_comp_index = -1
        if last_comp_index+1 == len(companies):
            break

        for company in tqdm(companies[last_comp_index+1:], desc="Downloading", unit=f"Sustainability Report for year {year}"):
            is_doubt = False
            if year in doubt_list.keys():
                for result in doubt_list[year]:
                    if company in result.values():
                        filepath_to_check = path + "doubtPDFs/" + company + "/" +  year +"_sustainability_report.pdf"
                        if file_exists(filepath_to_check):
                            break
                        else:
                            is_doubt = download_from_doubt_links(year, result, is_doubt)
                            break

            if year in found_list.keys() and not is_doubt:
                for result in found_list[year]:
                    if company in result.values():
                        filepath_to_check = path + "foundPDFs/" + company + "/" +  year +"_sustainability_report.pdf"
                        if file_exists(filepath_to_check):
                            break
                        else:
                            download_from_found_links(year, result)
                            break

            f = open('stopped_download_at.txt','w')
            f.truncate()
            f.write(year + "--" + company)

        year_changed = True
        write_stats(year, "1")

def result_statistic():
    columns = [
        'sustainability reports found',
        'sustainability reports doubt',
        'no sustainability report'
    ]

    # Leeres DataFrame vorbereiten
    results_df = pd.DataFrame(index=years_to_search, columns=columns)
    results_df = results_df.fillna(0)

    path = "C:/SFDH/Downloaded Reports/"

    # Durch alle Firmen und Jahre iterieren
    for company in companies:
        for year in years_to_search:
            filepath_doubt = os.path.join(path, "doubtPDFs", company, f"{year}_sustainability_report.pdf")
            filepath_found = os.path.join(path, "foundPDFs", company, f"{year}_sustainability_report.pdf")

            if file_exists(filepath_doubt):
                results_df.loc[year, 'sustainability reports doubt'] += 1
            if file_exists(filepath_found):
                results_df.loc[year, 'sustainability reports found'] += 1

    # Fehlende Berichte berechnen
    for year in years_to_search:
        results_df.loc[year, 'no sustainability report'] = (
            len(companies)
            - results_df.loc[year, 'sustainability reports doubt']
            - results_df.loc[year, 'sustainability reports found']
        )

    # Ergebnisse anzeigen
    print(results_df)

    # Prozentuale Darstellung
    percentage_df = results_df / len(companies)
    print(percentage_df)

    # Speichern als Excel
    results_df.to_excel('result_statistics.xlsx', index=True)
    percentage_df.to_excel('percentage_result_statistics.xlsx', index=True)



def empty_temporary_files():
    # empty txt files before next run
    f = open('stopped_search_at.txt','w')
    f.truncate()
    f = open('stopped_download_at.txt','w')
    f.truncate()
    f = open('stats0.txt','w')
    f.truncate()
    f = open('stats1.txt','w')
    f.truncate()
    # empty json files before next run
    with open('doubt_results_0.json', 'w') as file:
        pass
    with open('doubt_results_1.json', 'w') as file:
        pass
    with open('exception_at_download.json', 'w') as file:
        pass
    with open('found_results_0.json', 'w') as file:
        pass
    with open('found_results_1.json', 'w') as file:
        pass


years_to_search = ["2023"]
companies = make_company_list.make_clean_list(pd.Series(dtype=str), eurostoxx_list)
companies = companies[:5]  # optional: nur 5 Unternehmen für Testlauf

#print(companies)
#print(companies.shape)

empty_temporary_files() # uncomment if start from the beginning

last_year_index_search, last_comp_index_search, seach_started = find_where_to_start('stopped_search_at.txt')
last_year_index_dl, last_comp_index_dl, download_started = find_where_to_start('stopped_download_at.txt')
if not download_started:
    find_links(last_comp_index_search, last_year_index_search)
    copy_found_file()

# comment if do not want to start download from the beginning
# download_started = False
#last_year_index_dl = 0
#last_comp_index_dl = -1

doubt_list, found_list = init_result_lists()
download_read_pdfs(last_comp_index_dl, last_year_index_dl, doubt_list, found_list)

result_statistic()



