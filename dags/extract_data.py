from datetime import timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def population_data():
    url = 'https://api.worldbank.org/v2/countries/all/indicators/SP.POP.TOTL/?format=json&date=2018:2022&per_page=15312'

    response = requests.get(url)
    data = response.json()[1]

    data_list = []

    for item in data:
        country_code = item["countryiso3code"]
        country = item["country"]["value"]
        indicator = item["indicator"]["id"]
        indicator_name = item["indicator"]["value"]
        value = item["value"]
        date = item["date"]

        data_list.append({
            "Country Code": country_code,
            "Country Name": country,
            "Indicator Code": indicator,
            "Indicator Name": indicator_name,
            "Value": value,
            "Year": date,
        })

    df10 = pd.DataFrame(data_list)
    df10.to_csv("extracted_data/population.csv", index=False)

def gdp_data():
    url = 'https://api.worldbank.org/v2/countries/all/indicators/NY.GDP.MKTP.CD/?format=json&date=2018:2022&per_page=15312'

    response = requests.get(url)
    data = response.json()[1]

    data_list_gdp = []

    for item in data:
        country_code = item["countryiso3code"]
        country = item["country"]["value"]
        indicator = item["indicator"]["id"]
        indicator_name = item["indicator"]["value"]
        value = item["value"]
        date = item["date"]

        data_list_gdp.append({
            "Country Name": country,
            "Country Code": country_code,
            "Indicator Name": indicator_name,
            "Indicator Code": indicator,
            "Year": date,
            "GDP": value,
        })

    gdp = pd.DataFrame(data_list_gdp)
    gdp.to_csv("extracted_data/gdp.csv", index=False)

def electricity_data():
    url = 'https://api.worldbank.org/v2/countries/all/indicators/EG.ELC.ACCS.ZS/?format=json&date=2018:2023&per_page=15312'

    response = requests.get(url)
    data = response.json()[1]

    data_list_electricity = []

    for item in data:
        country_code = item["countryiso3code"]
        country = item["country"]["value"]
        indicator = item["indicator"]["id"]
        indicator_name = item["indicator"]["value"]
        value = item["value"]
        date = item["date"]

        data_list_electricity.append({
            "Country Name": country,
            "Country Code": country_code,
            "Indicator Name": indicator_name,
            "Indicator Code": indicator,
            "Year": date,
            "electricityaccesspercent": value,
        })

    electricity = pd.DataFrame(data_list_electricity)
    electricity.to_csv("extracted_data/electricity.csv", index=False)

def rural_data():
    url = 'https://api.worldbank.org/v2/countries/all/indicators/SP.RUR.TOTL.ZS/?format=json&date=2018:2022&per_page=15312'

    response = requests.get(url)
    data = response.json()[1]

    data_list_rural = []

    for item in data:
        country_code = item["countryiso3code"]
        country = item["country"]["value"]
        indicator = item["indicator"]["id"]
        indicator_name = item["indicator"]["value"]
        value = item["value"]
        date = item["date"]

        data_list_rural.append({
            "Country Name": country,
            "Country Code": country_code,
            "Indicator Name": indicator_name,
            "Indicator Code": indicator,
            "Year": date,
            "ruralpopulationpercent": value,
        })

    rural = pd.DataFrame(data_list_rural)
    rural.to_csv("extracted_data/rural.csv", index=False)


default_args = {
    'owner': 'Raihan',
    'start_date': days_ago(2)
}

with DAG(
    dag_id='Extract_API_Data',
    default_args=default_args,
    schedule_interval='@weekly',
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    extract_pop = PythonOperator(
        task_id='population_data',
        python_callable=population_data,
        dag=dag,
    )
    extract_gdp = PythonOperator(
        task_id='gdp_data',
        python_callable=gdp_data,
        dag=dag,
    )
    extract_elec = PythonOperator(
        task_id='electricity_data',
        python_callable=electricity_data,
        dag=dag,
    )
    extract_rural = PythonOperator(
    task_id='rural_data',
    python_callable=rural_data,
    dag=dag,
    )


extract_pop >> extract_gdp >> extract_elec >> extract_rural