import pandas as pd
import numpy as np
import requests


xlsx_file_name = "data.xlsx"
xlsx_file = pd.ExcelFile(xlsx_file_name)
sheets = xlsx_file.book.worksheets

visible_sheets = []
for sheet in sheets:
    if sheet.sheet_state == 'visible':
        visible_sheets.append(sheet.title)

stores_sheet_names = visible_sheets[0]
sales_sheets_names = visible_sheets[1:]

ERROR_ROW = 149
VALIDATE_COORD_FLAG = False

stores_sheet_1 = pd.read_excel('data.xlsx', sheet_name=stores_sheet_names, nrows=ERROR_ROW)
stores_header = list(stores_sheet_1.columns[:2]) + list(stores_sheet_1.values[0][3:])
stores_sheet_1=stores_sheet_1.drop(stores_sheet_1.columns[2], axis=1)[1:]
stores_sheet_1.columns = stores_header

stores_sheet_2 = pd.read_excel('data.xlsx', sheet_name=stores_sheet_names, skiprows=ERROR_ROW)
stores_sheet_2=stores_sheet_2.drop(stores_sheet_2.columns[-1], axis=1)
stores_sheet_2.columns = stores_header

stores_sheet_2['№ ТТ'] = list(map(lambda s: int(s.replace('N', '')), stores_sheet_2['№ ТТ']))

stores_sheet = pd.concat([stores_sheet_1, stores_sheet_2], ignore_index=True)

stores_sheet = stores_sheet.rename(columns={'ДОЛ.':'ШИР.', 'ШИР.': 'ДОЛ.'}, errors="raise")

stores_sheet['ШИР.'] = list(map(lambda s: str(s).replace(',', '.'), stores_sheet['ШИР.']))
stores_sheet['ДОЛ.'] = list(map(lambda s: str(s).replace(',', '.'), stores_sheet['ДОЛ.']))
# print(stores_sheet.values)

def validate_stores_sheet(stores_sheet:pd.DataFrame):
    error_log = set()
    for store, opening_date, closing_date in zip(stores_sheet['№ ТТ'], stores_sheet['ДАТА ОТКР.'], stores_sheet['ДАТА ЗАКР.']):
        if not pd.notna(opening_date):
            error_log.add(f"Дата открытия ТТ {store} не найдена")
            continue
        if (not pd.notna(closing_date)) or (closing_date is np.nan):
            continue
        elif closing_date < opening_date:
            error_log.add(f"Некорректная дата открытия/закрытия ТТ {store}")

    if len(error_log) > 0:
        print(error_log)

    def coordinates_validate(lat, lon):
        token = "989267f0a4f25c2b2adc08b67b6976bc21d69e77"
        url = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/geolocate/address'

        payload = {'token': token, 'lat': lat, 'lon': lon, 'count': 1}
        response = requests.get(f"{url}", params=payload)
        response_data = response.json().get('suggestions')
        if response_data == [] or response_data == None:
            print(lat, lon)
            return False
        country = response_data[0]['data']['country']
        return country == 'Россия'

    
    if VALIDATE_COORD_FLAG:
        for store, lat, lon in zip(stores_sheet['№ ТТ'],stores_sheet['ШИР.'], stores_sheet['ДОЛ.']):
            if not coordinates_validate(lat, lon):
                error_log.add(f"Неверные координаты ТТ {store}")
            print(store)
        print(error_log)

stores_sheet.to_csv('stores.csv', index=False)
validate_stores_sheet(stores_sheet)


def delete_empty_row(df:pd.DataFrame) -> pd.DataFrame:
    headers = df.columns
    df.dropna(inplace=True)
    if all(map(lambda x: "Unnamed" in x, headers)) > 0:
        df.columns = df.iloc[0]
        df = df[1:]
    return df


def validate_sales_sheet(sales_sheet: pd.DataFrame, sales_sheet_name):
    headers = sales_sheet.columns

    error_log = set()
    for date in sales_sheet['НЕДЕЛЯ']:
        if date.weekday() != 0:
            error_log.add(f"Лист {sales_sheet_name} имеет дату, отличную от понедельника")
            break
    
    for number_of_sales in sales_sheet['КОЛ-ВО']:
        if number_of_sales < 500: # Нужно какое то адекватное условие
            error_log.add(f"Лист {sales_sheet_name} имеет некорректное кол-во")
            break

    for store_point in sales_sheet['№ TT'].values:
        if int(store_point) not in stores_sheet['№ ТТ'].values:
            error_log.add(f"Лист {sales_sheet_name}: № ТТ {store_point} не существует в stores")

    if len(error_log) > 0:
        print(error_log)


for sales_sheet_name in sales_sheets_names:
    sales_sheet = pd.read_excel(xlsx_file_name, sales_sheet_name)
    sales_sheet = delete_empty_row(sales_sheet)
    validate_sales_sheet(sales_sheet, sales_sheet_name)
    # print(sales_sheet)