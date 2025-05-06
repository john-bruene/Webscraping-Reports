import pandas as pd
import re

msci_list = pd.read_csv("Google Crawling/msci.csv")["Name"]
dax_list = pd.read_csv("Google Crawling/dax.csv")["COMPANIES"]
eurostoxx_list = pd.read_csv("Google Crawling/eurostoxx600.csv", sep=';')["firm"]

not_allowed_terms_old = [
    ' inc',
    ' group',
    " class",
    " ltd",
    " plc",
    " ag",
    " sa",
    " corp",
    " entertainment",
    " holding",
    " holdings",
    " company",
    " reit",
    " pharmaceuticals",
    " pharmaceutical",
    " nv", 
    " industries",
    " communications",
    " corporation",
    " resources",
    " vacations",
    " solutions",
    " a",
    " stapled units",
    " n",
    " units",
    " unit",
    " investment",
    " b",
    " energia",
    " energias"
]

not_allowed_terms = [
    " class",
    " nv", 
    " a",
    " stapled units",
    " n",
    " units",
    " unit",
    " b",
    " i"
]

tolerated_terms = [
    ' inc',
    ' group',
    " ltd",
    " plc",
    " ag",
    " sa",
    " corp",
    " entertainment",
    " holding",
    " holdings",
    " company",
    " reit",
    " pharmaceuticals",
    " pharmaceutical",
    " industries",
    " communications",
    " corporation",
    " resources",
    " vacations",
    " solutions",
    " investment",
    " energia",
    " energias"
]

def make_clean_list(msci_list, eurostoxx_list):
    """Cleans the MSCI companies list and joins all the dataframes together in one list

    Args:
        msci_list (Dataframe)

    Returns:
        Dataframe: merged Dataframe of all companies
    """
    msci_list = msci_list.apply(str.lower)
    eurostoxx_list = eurostoxx_list.apply(str.lower)

    # clean msci list
    for key, company in msci_list.items():
        for word in not_allowed_terms:
            found = re.search(word + "(\s|$)", company)
            if found:
                company = company.split(word, 1)[0]
                msci_list.iloc[key] = company
        
        for word in tolerated_terms:
            found = re.search(word + "(\s|$)", company)
            if found:
                company = company.split(word, 1)[0] + word
                msci_list.iloc[key] = company
    # Clean eurosstoxx list
    for key, company in eurostoxx_list.items():
        for word in not_allowed_terms:
            found = re.search(word + "(\s|$)", company)
            if found:
                company = company.split(word, 1)[0]
                eurostoxx_list.iloc[key] = company
        
        for word in tolerated_terms:
            found = re.search(word + "(\s|$)", company)
            if found:
                company = company.split(word, 1)[0] + word
                eurostoxx_list.iloc[key] = company


    companies = pd.concat([dax_list, msci_list, eurostoxx_list])
    companies = companies.drop_duplicates()
    companies = companies.apply(lambda x: x.replace('&', 'and') if isinstance(x, str) else x)
    companies = companies.apply(lambda x: x.replace('_', ' ') if isinstance(x, str) else x)

    return companies