# Importing required modules
import requests  # for sending HTTP requests
from bs4 import BeautifulSoup  # for parsing HTML and XML documents
import pandas as pd  # for creating and manipulating dataframes
from time import sleep  # for pausing the program for a certain amount of time
from selenium import webdriver  # for automating web browser interactions
from webdriver_manager.chrome import ChromeDriverManager  # for downloading and installing the ChromeDriver executable
from selenium.webdriver import ActionChains  # for automating user actions like clicks and keypresses
from selenium.webdriver.common.action_chains import ActionChains  # for creating a chain of user actions

# Setting up the ChromeDriver executable using webdriver_manager
chrome_service = webdriver.chrome.service.Service(ChromeDriverManager().install())

# Launching the ChromeDriver using selenium.webdriver
driver = webdriver.Chrome(service=chrome_service)
# specify the path to your webdriver executable
driver = webdriver.Chrome('/path/to/chromedriver')

# load the website and wait for it to fully render
driver.get('https://www.skyscanner.com/transport/flights/mem/oax/230609/230612/?adultsv2=2&cabinclass=economy&childrenv2=&inboundaltsenabled=false&is_banana_refferal=true&outboundaltsenabled=false&preferdirects=false&ref=home&rtn=1')
print(driver.current_url)
url = driver.current_url
if "captcha" in url:
    print("URL contains captcha!")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    print(soup)
    captcha = driver.execute_async_script('')
else:
    print("URL does not contain captcha.")
driver.implicitly_wait(10)

# Filter out all text with provided class values
class_values = ['LegInfo_routePartialTimee__OTFkN', 
                'LegInfo_stopsContainer__NWIyN', 
                'LegInfo_routePartialCityTooltip__NTE4Z', 
                'Duration_duration__NmUyM', 
                'LegInfo_stopStation__M2E5N', 
                'LegInfo_routePartialTime__OTFkN', 
                'LegInfo_stopsLabelContainer__MmM0Z', 
                'LegInfo_routePartialArrive__Y2U1N', 
                'LegInfo_stopsLabelContainer__MmM0Z', 
                'LegInfo_routePartialDepart__NzEwY', 
                'LegInfo_stopsContainer__NWIyN', 
                'Price_mainPriceContainer__MDM3O']
# extract the div elements with class name "FlightsResults_dayViewItems__ZDFlO"
# Get all text data from <div> with class="FlightsResults_dayViewItems__ZDFlO"
soup = BeautifulSoup(driver.page_source, 'html.parser')
divs = soup.find_all('div', class_='FlightsResults_dayViewItems__ZDFlO')
for div in divs:
    for cls in class_values:
        for elem in div.find_all(class_=cls):
            print(elem.text)




# close the browser when you're done
driver.quit()