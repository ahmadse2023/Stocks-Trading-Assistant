from selenium import webdriver
from joblib import Parallel, delayed
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import gspread
from gspread.models import Cell
from oauth2client.service_account import ServiceAccountCredentials

options = webdriver.ChromeOptions()
# options.add_argument("headless")
driver = webdriver.Chrome(executable_path=r"/usr/local/bin/chromedriver", options=options)

def get_names_from_page(all_names, page):
    state = True
    driver.get("https://www.barchart.com/stocks/highs-lows/highs?timeFrame=1y&orderBy=symbol&orderDir=asc&page=" + str(page))
    time.sleep(4)
    cur_url = driver.current_url
    if cur_url[-1] != str(page):
        state = False
        return state

    wait = WebDriverWait(driver, 10)
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "symbol")))
    names = driver.find_elements_by_class_name('symbol')
    for name in names:
        sym = name.text
        if sym == 'Symbol':
            continue
        all_names.append(name.text)

    return state

def get_names():
    names = []
    page = 1
    while True:
        state = get_names_from_page(names, page)
        if not state:
            break
        page+=1
    return names

def split_into_groups(urls):
    par_size = int(len(urls) / 4)
    groups = []
    for i in range(0, 3):
        groups.append(urls[i * par_size:(i + 1) * par_size])
    if len(urls) > (3*par_size):
        groups.append(urls[3 * par_size:])

    return groups

def get_sof(group):
    dict = {}
    driver = webdriver.Chrome(executable_path=r"/usr/local/bin/chromedriver", options=options)
    for url in group:
        name = url.split('=')[1]
        driver.get(url)
        cells = driver.find_elements_by_tag_name('td')

        for i in range(len(cells)):
            if "Short % of Float" in cells[i].text:
                break
        if i + 1 < len(cells) - 1:
            dict[name] = cells[i + 1].text
    return dict

all_names = get_names()
urls = ["https://finance.yahoo.com/quote/" + name + "/key-statistics?p=" + name for name in all_names]
urls_groups = split_into_groups(urls)
driver.close()

values = Parallel(n_jobs=-1)(delayed(get_sof)(group) for group in urls_groups)

final_dict = {}
for dict in values:
    final_dict.update(dict)

symbols = list(final_dict.keys())
sof = list(final_dict.values())


scope = ['https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(credentials)
sheet = client.open('Stocks').sheet1

cells = []
sheet.resize(rows=1)
sheet.resize(rows=len(final_dict)+1)
for i in range(2,len(final_dict)+2):
    cells.append(Cell(row=i, col=1, value=symbols[i-2]))
    cells.append(Cell(row=i, col=2, value=sof[i-2]))
sheet.update_cells(cells)

