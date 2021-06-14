import re
import time
from datetime import datetime
import async_timeout
import asyncio
import Settings

import aiohttp
import gspread
from oauth2client.service_account import ServiceAccountCredentials


start = time.time()


def gspread_file(credential_file, sheet_name):
    # Authenticate with Google and open sheets
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, 
                                                                   ['https://www.googleapis.com/auth/spreadsheets',
                                                                    'https://www.googleapis.com/auth/drive'])
    file = gspread.authorize(credentials)
    stock = file.open(sheet_name).worksheet('Stocks')
    log = file.open(sheet_name).worksheet('Log')
    return stock, log


def get_stock_list(sheet):
    # Get data list from gspread
    result = {}

    for name in ['Symbol', 'Rating', 'Target Price', 'Number of Analysts']:
        result[name] = sheet.col_values(sheet.find(name).col, 'UNFORMATTED_VALUE')[1:]

    return result


async def fetch(session, url):
    with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()


async def main(loop, urls):
    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results


def page_parse():
    # Find data on yahoo and create data lists
    yahoo_rating_list = []
    target_price_list = []
    number_analysts_list = []

    for body in stocks['Body']:
        # time_start = time.time()
        yahoo_rating = re.findall(r"\"recommendationMean\":{\"raw\":([\d.]+)", body)
        target_price = re.findall(r"\"targetMeanPrice\":{\"raw\":([\d.]+)", body)
        number_analysts = re.findall(r"\"numberOfAnalystOpinions\":{\"raw\":([\d.]+)", body)
        yahoo_rating_list.append([float(yahoo_rating[0])])
        target_price_list.append([float(target_price[0])])
        number_analysts_list.append([int(number_analysts[0])])

        # Print Status #
        '''
        count += 1
        if len(time_list) > 10:
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
        '''

    return yahoo_rating_list, target_price_list, number_analysts_list


def status(sheet, col_title, completed):
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


def log_changes(data1, data2, label):
    result = []
    for i in range(len(data1)):
        if not data1[i]:
            result.append([str(now.strftime('%d-%m-%Y')), stocks['Symbol'][i], label, 'First', data2[i][0]])
        elif data1[i] != data2[i][0]:
            result.append([str(now.strftime('%d-%m-%Y')), stocks['Symbol'][i], label,
                           data1[i], data2[i][0]])
    return result


# Read data from gsheet
print('Reading Google sheet file...')
stock_sheet, log_sheet = gspread_file('creds.json', Settings.gsheet_name)
stocks = get_stock_list(stock_sheet)
stocks['URL'] = ['https://finance.yahoo.com/quote/' + stock for stock in stocks['Symbol']]

# Fetch data from Yahoo site
print('Fetching data from Yahoo...')
loop = asyncio.get_event_loop()
stocks['Body'] = loop.run_until_complete(main(loop, stocks['URL']))

stocks['New Ratings'], stocks['New Targets'], stocks['New Analysts'] = page_parse()

# Update data
print('Updating google sheet...')
put_data_gsheet(stocks['New Analysts'], 'Number of Analysts', stock_sheet)
put_data_gsheet(stocks['New Targets'], 'Target Price', stock_sheet)
put_data_gsheet(stocks['New Ratings'], 'Rating', stock_sheet)

# Log updates
now = datetime.now()
status(stock_sheet, "Updated:", True)

changes = (log_changes(stocks['Rating'], stocks['New Ratings'], 'Rating') +
           log_changes(stocks['Target Price'], stocks['New Targets'], 'Target Price') +
           log_changes(stocks['Number of Analysts'], stocks['New Analysts'], 'Number of Analysts'))
log_sheet.insert_rows(sorted(changes, key=lambda x: x[1]), 2)

print('Completed!')
print("----%.2f----" % (time.time()-start))
time.sleep(3)
