import random
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import xlsxwriter
import time
import shutil

# Настройка логирования
log_file = Path(__file__).resolve().parent / 'log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.info('Начало работы приложения')
logging.info('Блок скачивания файлов')

# Функция для безопасного имени файла
def safe_filename(filename):
    return filename.replace('/', '_').replace('\\', '_')

# Создание папки для загрузок с очисткой
def ensure_output_folder(folder_name):
    folder_path = Path(__file__).resolve().parent / folder_name

    # Проверяем, существует ли папка
    if folder_path.exists():
        # Очищаем папку (удаляем все файлы и подпапки)
        logging.info(f'Очищаем папку для сохранения файлов: {folder_path}')
        shutil.rmtree(folder_path)
        folder_path.mkdir(parents=True)  # Пересоздаем папку
        logging.info(f'Папка очищена и пересоздана: {folder_path}')
    else:
        # Если папка не существует, создаем ее
        folder_path.mkdir(parents=True)
        logging.info(f'Создана папка для сохранения файлов: {folder_path}')

    return folder_path

output_folder = ensure_output_folder('downloads')

# Параметры
format = "%d.%m.%Y"
end_date_user = datetime.strptime("31.10.2024", "%d.%m.%Y").date()

base_url = 'https://spimex.com'  # Базовый URL для формирования полного пути
expected_mime_types = ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']

# Функция для скачивания файла
def download_file(url, output_path):
    try:
        logging.info(f'Скачивание файла: {url}')
        with requests.Session() as session:
            response = session.get(url, stream=True, timeout=60)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '')
            if content_type not in expected_mime_types:
                logging.warning(f'Пропущено: {url} (неподдерживаемый тип содержимого: {content_type})')
                return False

            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            logging.info(f'Файл сохранен: {output_path}')
            return True
    except requests.exceptions.RequestException as e:
        logging.error(f'Ошибка при скачивании {url}: {e}', exc_info=True)
    except FileNotFoundError as fnf_error:
        logging.error(f"Ошибка при сохранении файла {url}: {fnf_error}", exc_info=True)
    return False

# Основная логика
page = 1
# Примерная дата, с которой начнем цикл
current_end_date = datetime.strptime("31.12.2099", "%d.%m.%Y").date()

while current_end_date >= end_date_user:
    try:
        url = f"https://spimex.com//markets/derivatives/trades/results/?page=page-{page}"
        with requests.Session() as session:
            req = session.get(url, timeout=60)
            logging.info(req)
            req.raise_for_status()
            soup = BeautifulSoup(req.text, 'html.parser')

        for tag in soup.find_all('div', class_='accordeon-inner__item'):
            if 'files' not in str(tag):
                # Получаем ссылку href
                link_tag = tag.find('a', class_='accordeon-inner__item-title link xls')
                href = link_tag.get('href') if link_tag else None

                # Получаем дату из span
                date_span = tag.find('span')
                trade_date = date_span.text.strip() if date_span else None

                if not trade_date or not href:
                    continue

                file_url = base_url + href
                file_name = href.split('/')[-1].split('?')[0]
                file_name = safe_filename(file_name)
                file_path = output_folder / file_name

                # Преобразуем строку в дату
                current_end_date = datetime.strptime(trade_date, "%d.%m.%Y").date()

                # Прекращаем цикл, если current_end_date <= end_date_user
                if current_end_date < end_date_user:
                    logging.info(f"Достигнута конечная дата {end_date_user}. Завершаем загрузку.")
                    break

                download_file(file_url, file_path)

                # Рандомная пауза между загрузкой 
                pause_duration = random.uniform(0.1, 1)  # Случайная пауза от 0.5 до 2 секунд
                logging.info(f"Пауза между загрузкой: {pause_duration:.2f} секунд")
                time.sleep(pause_duration)  # Пауза в секундах

        page += 1
        logging.info('Переход на страницу'+' '+str(page))
    except requests.exceptions.RequestException as e:
        logging.error(f'Ошибка при обработке страницы {url}: {e}', exc_info=True)
        break
    except Exception as e:
        logging.error(f'Неожиданная ошибка: {e}', exc_info=True)
        break

logging.info('Блок обработки файлов')
# Обработка Файлов
def search(target_value, df):
    """
    Функция для поиска всех ячеек DataFrame, содержащих target_value.

    Parameters:
    target_value (str): Строка для поиска.
    df (pd.DataFrame): DataFrame, в котором выполняется поиск.

    Returns:
    list: Список кортежей с координатами всех совпадений (строка, столбец).
    """
    try:
        # Преобразуем все значения в DataFrame в строки, чтобы избежать ошибок при применении .str.contains()
        df_str = df.astype(str)
        
        # Применяем str.contains() ко всем ячейкам DataFrame
        matches = df_str.apply(lambda x: x.str.contains(target_value, na=False))
        
        # Получаем координаты всех совпадений (строки, столбцы)
        indices = list(zip(*matches.values.nonzero()))
        return indices
    except Exception as e:
        logging.error(f"Ошибка при поиске значения '{target_value}' в DataFrame: {e}", exc_info=True)
        return []

# Создаем новый Excel-файл
try:
    workbook = xlsxwriter.Workbook(f"Итоги торгов в секции срочного рынка {end_date_user}-{current_end_date}.xlsx")
except Exception as e:
    logging.error(f"Ошибка при создании Excel-файла: {e}", exc_info=True)
    raise

worksheet = workbook.add_worksheet("Основной режим торгов")

# Определяем форматирование для заголовков
header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
# Формат для текста с переносом
text_wrap_format = workbook.add_format({'border': 1})

# Объединение ячеек для заголовков
try:
    worksheet.merge_range('A1:A2', 'Наименование типа срочных контрактов', header_format)
    worksheet.merge_range('B1:B2', 'Код типа срочных контрактов', header_format)
    worksheet.merge_range('C1:C2', 'Наименование серии срочных контрактов', header_format)
    worksheet.merge_range('D1:D2', 'Код серии срочных контрактов', header_format)
    worksheet.merge_range('E1:E2', 'Наименование валюты контракта', header_format)
    worksheet.merge_range('F1:F2', 'Код валюты контракта', header_format)
    worksheet.merge_range('G1:G2', 'Курс валюты в рублях', header_format)

    worksheet.merge_range('H1:I1', 'Объём договоров, контр.', header_format)
    worksheet.merge_range('J1:K1', 'Объём договоров, валюта контракта', header_format)
    worksheet.merge_range('L1:M1', 'Объём договоров, руб.', header_format)

    worksheet.merge_range('N1:N2', 'Расчетная цена, валюта контракта', header_format)
    worksheet.merge_range('O1:O2', 'Средневзвешенная цена, валюта контракта', header_format)
    worksheet.merge_range('P1:P2', 'Расчетная цена пред. торгового дня, валюта контракта', header_format)

    worksheet.merge_range('Q1:R1', 'Цена безадр. договора, валюта контракта', header_format)
    worksheet.merge_range('S1:T1', 'Цена адр. договора, валюта контракта', header_format)
    worksheet.merge_range('U1:V1', 'Цена заявки, валюта контракта', header_format)
    worksheet.merge_range('Y1:Z1', 'Количество договоров, шт.	', header_format)

    worksheet.merge_range('W1:W2', 'Открытые позиции', header_format)
    worksheet.merge_range('X1:X2', 'Открытые позиции пред. торгового дня', header_format)
    worksheet.merge_range('AA1:AA2', 'Дата торгов', header_format)

    list1 = ['H','I','J','K','L','M','Q','R','S','T','U','V','Y','Z']
    list2 = ['Безадресные','Адресные','Безадресные','Адресные','Безадресные','Адресные','Мин.','Макс.','Мин.','Макс.','Лучшее предложение','Лучший спрос','Безадресные','Адресные']

    for i, j in zip(list1, list2):
        worksheet.write(f"{i}2", j, header_format)
except Exception as e:
    logging.error(f"Ошибка при создании заголовков или объединении ячеек в Excel: {e}", exc_info=True)
    workbook.close()  # Закрываем файл в случае ошибки
    raise

# Укажите путь к папке
folder_path = Path(f"downloads")

# Получаем список всех файлов в папке
try:
    files = [f.name for f in folder_path.iterdir() if f.is_file()]
except Exception as e:
    logging.error(f"Ошибка при получении списка файлов из папки {folder_path}: {e}", exc_info=True)
    files = []

result = []

# Обрабатываем каждый файл в папке
for file in files:
    try:
        df = pd.read_excel(f"{folder_path}/{file}")
    except Exception as e:
        logging.error(f"Ошибка при чтении файла {file}: {e}", exc_info=True)
        continue

    try:
        date_indices = list(search('Дата торгов:', df))  # Координаты всех совпадений (строка, столбец)
        if date_indices:
            date = df.iloc[date_indices[0][0], date_indices[0][1]].split(' ')[2]
            df = df[df['Unnamed: 0'].str.contains('фьюч.контракт', na=False)].reset_index(drop=True).dropna(axis=1)
            df['Дата торгов'] = date
            result.append(df)
        else:
            logging.warning(f"Дата торгов не найдена в файле {file}")
    except Exception as e:
        logging.error(f"Ошибка при обработке данных из файла {file}: {e}", exc_info=True)

# Объединяем результаты
if result:
    try:
        res = pd.concat(result)
    except Exception as e:
        logging.error(f"Ошибка при объединении DataFrame: {e}", exc_info=True)
        res = pd.DataFrame()  # Если ошибка, создаем пустой DataFrame

    # Записываем данные в Excel
    try:
        row = 1  # Начинаем с третьей строки (индекс 2)
        for index, data_row in res.iterrows():
            row += 1
            for col, value in enumerate(data_row):
                worksheet.write(row, col, value, text_wrap_format)

        # Сохраняем файл
        workbook.close()
    except Exception as e:
        logging.error(f"Ошибка при записи в Excel: {e}", exc_info=True)
        workbook.close()  # Закрываем файл в случае ошибки
else:
    logging.warning("Нет данных для записи в Excel.")

logging.info(f"Было обработано {len(files)} файлов")
logging.info('Работа приложения завершена')

