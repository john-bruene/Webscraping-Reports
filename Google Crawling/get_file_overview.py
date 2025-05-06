from os import listdir
from os.path import isfile, join
import pandas
import os
path_found = "foundPDFs"
path_doubt = "doubtPDFs"

companys_found = [f for f in listdir(path_found)]
companys_doubt = [f for f in listdir(path_doubt)]
companys = companys_found + companys_doubt
result = []
for company in companys:
    row_entry = {"name": company}
    if os.path.exists(f"{path_found}/{company}"):

        reports = [f for f in listdir(f"{path_found}/{company}")]
        if "2017_report.pdf" in reports:
            row_entry["2017"] = "d"
        if "2018_report.pdf" in reports:
            row_entry["2018"] = "d"
        if "2019_report.pdf" in reports:
            row_entry["2019"] = "d"
        if "2020_report.pdf" in reports:
            row_entry["2020"] = "d"
        if "2021_report.pdf" in reports:
            row_entry["2021"] = "d"
        if "2022_report.pdf" in reports:
            row_entry["2022"] = "d"
    
    if os.path.exists(f"{path_doubt}/{company}"):
        reports = [f for f in listdir(f"{path_doubt}/{company}")]
        if "2017_report.pdf" in reports:
            row_entry["2017"] = "f"
        if "2018_report.pdf" in reports:
            row_entry["2018"] = "f"
        if "2019_report.pdf" in reports:
            row_entry["2019"] = "f"
        if "2020_report.pdf" in reports:
            row_entry["2020"] = "f"
        if "2021_report.pdf" in reports:
            row_entry["2021"] = "f"
        if "2022_report.pdf" in reports:
            row_entry["2022"] = "f" 
    result.append(row_entry)
df = pandas.DataFrame(data=result, columns=["name", "2017", "2018", "2019", "2020", "2021", "2022"] )
df.to_excel("overview.xlsx")