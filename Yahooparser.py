import re
import time
from datetime import datetime


import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials


def gspread_file(credential_file, sheet_name):
    # Authenticate with Google and open sheets
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, 
                                                                   ['https://www.googleapis.com/auth/spreadsheets',
                                                                    'https://www.googleapis.com/auth/drive'])
    file = gspread.authorize(credentials)
    sheet = file.open(sheet_name).worksheet('Stocks')
    return sheet


def get_stock_list(sheet):
    # Get stock list from gspread
    result = {}
    for name in ['Symbol', 'Rating', 'Target Price', 'Number of Analysts']:
        result[name] = sheet.col_values(sheet.find(name).col)[1:]
    return result


def get_stock_page(stock_symbol):
    # Get yahoo stock page
    tries = 3
    yahoo_url = 'https://finance.yahoo.com/quote/' + stock_symbol
    while tries > 0:
        stock_page = requests.get(yahoo_url)
        tries -= 1
        if stock_page.status_code == 200:
            return stock_page.text


def page_parse(parse_list):
    # Find data on yahoo and create data lists
    yahoo_rating_list = []
    target_price_list = []
    number_analysts_list = []
    time_list = []
    count = 0

    for stock in parse_list:
        time_start = time.time()
        stock_page = get_stock_page(stock)
        yahoo_rating = re.findall(r"\"recommendationMean\":{\"raw\":([\d.]+)", stock_page)
        target_price = re.findall(r"\"targetMeanPrice\":{\"raw\":([\d.]+)", stock_page)
        number_analysts = re.findall(r"\"numberOfAnalystOpinions\":{\"raw\":([\d.]+)", stock_page)
        yahoo_rating_list.append([float(yahoo_rating[0])])
        target_price_list.append([float(target_price[0])])
        number_analysts_list.append([int(number_analysts[0])])
        time_end = time.time()

        # Print Status #
        count += 1
        if len(time_list) > 5:
            time_list.pop(0)
        time_list.append(time_end - time_start)
        h, m, s = convertmillis(sum(time_list) / len(time_list) * (len(stock_list['Symbol']) - count))
        print(f'Completed: {count} of {len(parse_list)} - {stock} ({round(count / len(parse_list) * 100, 2)}%)')
        print('Time left:', end='')
        if h > 0:
            print(f' {h} hours,', end='')
        if m > 0:
            print(f' {m} minutes,', end='')
        print(f' {s} seconds \n')

    return yahoo_rating_list, target_price_list, number_analysts_list


def status(sheet, col_title, completed):
    now = datetime.now()
    col_address = int(sheet.find(col_title).col)
    row_address = int(sheet.find(col_title).address[1])
    if completed:
        sheet.update_cell(row_address, col_address+1, str(now.strftime("%d-%m-%Y %H:%M")))
    else:
        sheet.update_cell(row_address, col_address+1, 'Error!')


def put_data_gsheet(parse_list, col_name, sheet):
    col_address = sheet.find(col_name).address
    sheet.update(f'{col_address[0]}{int(col_address[1]) + 1}:{col_address[0]}9999', parse_list)


def convertmillis(millis):
    seconds = int(millis % 60)
    minutes = int((millis / 60) % 60)
    hours = int((millis / (60 * 60)) % 24)
    return hours, minutes, seconds


gsheet = gspread_file('creds.json', 'PythonTest')
stock_list = get_stock_list(gsheet)
stock_list['New Ratings'], stock_list['New Targets'], stock_list['New Analysts'] = page_parse(stock_list['Symbol'])

put_data_gsheet(stock_list['New Ratings'], 'Rating', gsheet)
put_data_gsheet(stock_list['New Targets'], 'Target Price', gsheet)
put_data_gsheet(stock_list['New Analysts'], 'Number of Analysts', gsheet)

status(gsheet, "Updated:", True)

"""
    status(gsheet, "Updated:", True)
except:
    status(gsheet, "Updated:", False)
"""