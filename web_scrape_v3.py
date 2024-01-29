from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from io import StringIO
import psycopg2, yaml
import time

# Instantiate connection with MailServer & Postgres
path = '/home/ely/airflow/pyscript/secret.yml'
with open (path, 'r') as f:
    data = yaml.full_load(f)

param_dic = {
    "host"      : data.get('host'),
    "database"  : data.get('database'),
    "user"      : data.get('user'),
    "password"  : data.get('password')
}
options = Options()
options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--headless")
options.add_argument('disable-infobars')
driver = webdriver.Chrome(options = options)
#driver.maximize_window()
driver.get("https://www.weatherapi.com/weather/")
search_box = driver.find_element(By.XPATH,
                                 '//*[@id="ctl00_Search1_txtSearch"]').send_keys("Davao, Davao City")#,Keys.ENTER
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="ctl00_Search1_PanelSearch"]/span/div/div/div'))).click()

time.sleep(3)
weather_temp = driver.find_element(By.XPATH,'//*[@id="weatherapi-weather-london-city-of-london-greater-london-united-kingdom"]/div/div[3]').text
weather_cond = driver.find_element(By.XPATH,'//*[@id="weatherapi-weather-london-city-of-london-greater-london-united-kingdom"]/div/div[1]/span').text
weather_precip = driver.find_element(By.XPATH,'//*[@id="weatherapi-weather-london-city-of-london-greater-london-united-kingdom"]/div/div[2]').text
# print(weather_temp+' '+weather_cond)
#print(weather_precip)


table = driver.find_element(By.XPATH,'//*[@id="aspnetForm"]/div[4]/section/div[2]/section/div[4]/div[2]/div/div/table').get_attribute("outerHTML")
data = pd.read_html(table)
data = data[0]
data = data.transpose()
data.columns = data.iloc[0]
data = data[1:]
data['temperature'] = weather_temp
data['condition'] = weather_cond
# strip() and split() precip_weather data
result = dict((a.strip(), (b.strip()))
              for a, b in (element.split(':')
                           for element in weather_precip.split('\n')))
precip_df = pd.DataFrame([result])
# Reset the index to zero (default)
precip_df.reset_index(drop=True, inplace=True)
data.reset_index(drop=True, inplace=True)
# merge weather_data and precip_data Dataframes
df = pd.concat([data, precip_df], axis=1)
df['Sunrise:'] = pd.to_datetime(df['Sunrise:'])
df['Sunset:'] = pd.to_datetime(df['Sunset:'])
df['Current time:'] = pd.to_datetime(df['Current time:'])
df = df.rename(columns={
    "Country:": "country",
    "Region:": "region",
    "Lat/Lon:": "latitude_longitude",
    "Current time:": "fetched_date",
    "Time Zone ID:": "time_zone_id",
    "Time Zone:": "time_zone",
    "Sunrise:": "sunrise",
    "Sunset:": "sunset",
    "temperature": "temperature",
    "condition": "weather_condition",
    "Wind": "wind",
    "Precip": "precipitation",
    "Pressure": "pressure"
})
driver.close()
# Connect and Load the DATA in PostgreSQL
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
    
conn = connect(param_dic)
# Function to execute any query in the database
def execute_query(conn, query):
    """ Execute a single query """
    
    ret = 0 # Return value
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    # If this was a select query, return the result
    if 'select' in query.lower():
        ret = cursor.fetchall()
    cursor.close()
    return ret
# Here we use the execute_many operation
def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()

# Run the execute_many strategy
execute_many(conn, df, 'weather_dim')
conn.close()