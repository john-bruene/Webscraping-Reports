import requests
from bs4 import BeautifulSoup
from config import headers
import time
import os

import json
from write_results import write_json

SERPAPI_KEY = os.getenv("SERPAPI_KEY") or "7562a48eb2bacf64636122a3c97f8c45c3aa056b4d28c45176fc84c36487527c"


def scrape_google_and_order(query, year, company):
    """
    Verarbeitet SerpAPI-Ergebnisse, wählt den besten Link aus den ersten Treffern
    und speichert ihn als 'found' oder 'doubt' basierend auf Heuristiken.
    """

    # Erstelle Varianten des Firmennamens zum Abgleich in Links
    companySmall = company.lower()
    companySmallNoSpace = companySmall.replace(" ", "")
    companySmallUnderscore = companySmall.replace(" ", "_")
    companySmallDash = companySmall.replace(" ", "-")
    companySmallNoDot = companySmall.replace(".", "")
    companySlice0 = companySmall.split(" ")[0]

    allowed_names_in_link = [
        companySmall,
        companySmallNoSpace,
        companySmallUnderscore,
        companySmallDash,
        companySmallNoDot,
        companySlice0
    ]

    if len(company.split(" ")) > 1:
        companySlice1 = companySmall.split(" ")[1]
        allowed_names_in_link.append(companySlice1)

    most_relevant_link, allData = scrape_google(query)

    if not allData:
        print(f"[❌] Keine Ergebnisse für: {query}")
        return

    found = False

    for i, result in enumerate(allData[:2]):
        link = result.get("link")
        if not link or not link.endswith(".pdf"):
            continue

        filename = link.split("/")[-1].lower()

        if (
            any(name in link.lower() for name in allowed_names_in_link)
            and year in filename
            and any(word in link.lower() for word in ["report", "bericht"])
        ):
            most_relevant_link = link
            write_json({
                "company": company,
                "query": query,
                "link": most_relevant_link
            }, 'found_results_0.json', year)
            found = True
            break

    if not found:
        # Wenn keiner der ersten zwei Links alle Kriterien erfüllt
        write_json({
            "company": company,
            "query": query,
            "link": most_relevant_link
        }, 'doubt_results_0.json', year)




def scrape_google(query):
    print(f"[🔍] SerpAPI Query: {query}")
    
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": 10  # max. 10 Ergebnisse
    }

    try:
        response = requests.get("https://serpapi.com/search", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[⚠️] SerpAPI request failed: {e}")
        return None, []

    # Filtere nur echte Links aus den organischen Ergebnissen
    organic_results = data.get("organic_results", [])
    if not organic_results:
        print("[❌] No results found.")
        return None, []

    first_link = organic_results[0].get("link")
    print(f"[✅] Found: {first_link}")
    time.sleep(2)  # kleine Pause für SerpAPI-Etikette

    return first_link, organic_results