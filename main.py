import pandas as pd
import numpy as np
import requests
import settings


def prepare_stores_sheet(stores_sheet_names: list) -> pd.DataFrame:
    stores_sheet_1: pd.DataFrame = pd.read_excel(settings.XLSX_FILE_NAME, sheet_name=stores_sheet_names, nrows=settings.ERROR_ROW)
    stores_header = list(stores_sheet_1.columns[:2]) + list(stores_sheet_1.values[0][3:])
    stores_sheet_1=stores_sheet_1.drop(stores_sheet_1.columns[2], axis=1)[1:]
    stores_sheet_1.columns = stores_header

    stores_sheet_2: pd.DataFrame = pd.read_excel(settings.XLSX_FILE_NAME, sheet_name=stores_sheet_names, skiprows=settings.ERROR_ROW)
    stores_sheet_2=stores_sheet_2.drop(stores_sheet_2.columns[-1], axis=1)
    stores_sheet_2.columns = stores_header

    stores_sheet_2['№ ТТ'] = list(map(lambda s: int(s.replace('N', '')), stores_sheet_2['№ ТТ']))

    stores_sheet = pd.concat([stores_sheet_1, stores_sheet_2], ignore_index=True)

    stores_sheet = stores_sheet.rename(columns={'ДОЛ.':'ШИР.', 'ШИР.': 'ДОЛ.'}, errors="raise")

    stores_sheet['ШИР.'] = list(map(lambda s: str(s).replace(',', '.'), stores_sheet['ШИР.']))
    stores_sheet['ДОЛ.'] = list(map(lambda s: str(s).replace(',', '.'), stores_sheet['ДОЛ.']))

    return stores_sheet


def validate_stores_sheet(stores_sheet:pd.DataFrame) -> (pd.DataFrame, set()):
    error_log = set()
    for row_index, (store, opening_date, closing_date) in enumerate(
        zip(stores_sheet['№ ТТ'], stores_sheet['ДАТА ОТКР.'], stores_sheet['ДАТА ЗАКР.'])
        ):
        drop_flag = False
        if not pd.notna(opening_date):
            error_log.add(f"Дата открытия ТТ {store} не найдена")
            stores_sheet.drop(row_index, axis=0, inplace=True)
            continue
        if (not pd.notna(closing_date)) or (closing_date is np.nan):
            continue
        elif closing_date < opening_date:
            error_log.add(f"Некорректная дата открытия/закрытия ТТ {store}")
            drop_flag = True
        if drop_flag:
            stores_sheet.drop(row_index, axis=0, inplace=True)

    def coordinates_validate(lat: float | str, lon: float | str) -> bool:
        token = "989267f0a4f25c2b2adc08b67b6976bc21d69e77"
        url = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/geolocate/address'

        payload = {'token': token, 'lat': lat, 'lon': lon, 'count': 1}
        response = requests.get(f"{url}", params=payload)
        response_data = response.json().get('suggestions')
        if response_data == [] or response_data == None:
            return False
        country = response_data[0]['data']['country']
        return country == 'Россия'

    
    if settings.VALIDATE_COORD_FLAG:
        for store, lat, lon in zip(stores_sheet['№ ТТ'],stores_sheet['ШИР.'], stores_sheet['ДОЛ.']):
            if not coordinates_validate(lat, lon):
                error_log.add(f"Неверные координаты ТТ {store}")

    return stores_sheet, error_log


def delete_empty_row(df:pd.DataFrame) -> pd.DataFrame:
    headers = df.columns
    df.dropna(inplace=True)
    if all(map(lambda x: "Unnamed" in x, headers)) > 0:
        df.columns = df.iloc[0]
        df = df[1:]
        df.reset_index(drop=True, inplace=True)
    return df


def validate_sales_sheet(sales_sheet: pd.DataFrame, sales_sheet_name: str, stores_sheet: pd.DataFrame):
    error_log = set()
    for row_index, (date, number_of_sales, store_point) in enumerate(zip(
        sales_sheet['НЕДЕЛЯ'], sales_sheet['КОЛ-ВО'], sales_sheet['№ TT'].values)
        ):
        drop_flag = False

        if date.weekday() != 0:
            error_log.add(f"Лист {sales_sheet_name} имеет дату, отличную от понедельника")
            drop_flag = True
    
        if number_of_sales < 500: # Нужно какое то адекватное условие
            error_log.add(f"Лист {sales_sheet_name} имеет некорректное кол-во")
            drop_flag = True

        if int(store_point) not in stores_sheet['№ ТТ'].values:
            error_log.add(f"Лист {sales_sheet_name}: № ТТ {store_point} не существует в stores")
            drop_flag = True

        if drop_flag:
            sales_sheet.drop(row_index, axis=0, inplace=True)

    return sales_sheet, error_log


def get_visible_sheets(sheets: list) -> list:
    visible_sheets = []
    for sheet in sheets:
        if sheet.sheet_state == 'visible':
            visible_sheets.append(sheet.title)
    return visible_sheets


def validate_sales_sheets(sales_sheets_names: list, stores_sheet: pd.DataFrame) -> (pd.DataFrame, set()):
    errors_log = set()
    sales_df = []
    for sales_sheet_name in sales_sheets_names:
        sales_sheet = pd.read_excel(settings.XLSX_FILE_NAME, sales_sheet_name)
        sales_sheet = delete_empty_row(sales_sheet)
        sales_sheet, sales_errors = validate_sales_sheet(sales_sheet, sales_sheet_name, stores_sheet)
        errors_log.update(list(sales_errors))
        sales_df.append(sales_sheet)

    return pd.concat(sales_df), errors_log


def write_errors_to_file(errors: list, filename: str) -> None:
    with open(filename, "w", encoding='utf-8') as file:
        for error in errors:
            file.write(error + '\n')


def main() -> None:
    xlsx_file = pd.ExcelFile(settings.XLSX_FILE_NAME)
    sheets = xlsx_file.book.worksheets

    visible_sheets = get_visible_sheets(sheets)
    stores_sheet_names = visible_sheets[0]
    sales_sheets_names = visible_sheets[1:]

    stores_sheet:pd.DataFrame = prepare_stores_sheet(stores_sheet_names)
    stores, stores_errors = validate_stores_sheet(stores_sheet)
    stores.to_csv(f'{settings.OUTPUT_DATA_DIR}/stores.csv', index=False)

    sales, sales_errors = validate_sales_sheets(sales_sheets_names, stores_sheet)
    sales.to_csv(f'{settings.OUTPUT_DATA_DIR}/sales.csv', index=False)

    errors = list(stores_errors) + list(sales_errors)
    write_errors_to_file(errors, f'{settings.OUTPUT_DATA_DIR}/errors.txt')
    
if __name__ == "__main__":
    main()