import os
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

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

# Функция для безопасного имени файла
def safe_filename(filename):
    return filename.replace('/', '_').replace('\\', '_')

# Создание папки для загрузок
def ensure_output_folder(folder_name):
    folder_path = Path(__file__).resolve().parent / folder_name
    if not folder_path.exists():
        folder_path.mkdir(parents=True)
        logging.info(f'Создана папка для сохранения файлов: {folder_path}')
    else:
        logging.info(f'Папка для сохранения файлов уже существует: {folder_path}')
    return folder_path

output_folder = ensure_output_folder('downloads')

# Параметры
format = "%d.%m.%Y"
end_date_user = datetime(2024, 10, 1).date().strftime(format)

base_url = 'https://spimex.com'  # Базовый URL для формирования полного пути
expected_mime_types = ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']

# Функция для скачивания файла
def download_file(url, output_path):
    try:
        logging.info(f'Скачивание файла: {url}')
        with requests.Session() as session:
            response = session.get(url, stream=True, timeout=10)
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
current_end_date = None

while current_end_date != end_date_user:
    try:
        url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}"
        with requests.Session() as session:
            req = session.get(url, timeout=10)
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

                current_end_date = datetime.strptime(trade_date, "%d.%m.%Y").date().strftime(format)
                if current_end_date == end_date_user:
                    break

                file_url = base_url + href
                file_name = href.split('/')[-1].split('?')[0]
                file_name = safe_filename(file_name)
                file_path = output_folder / file_name

                download_file(file_url, file_path)

        page += 1
    except requests.exceptions.RequestException as e:
        logging.error(f'Ошибка при обработке страницы {url}: {e}', exc_info=True)
        break
    except Exception as e:
        logging.error(f'Неожиданная ошибка: {e}', exc_info=True)
        break

# Укажите путь к папке
folder_path = Path('downloads')

# Получаем список всех файлов в папке
files = [f.name for f in folder_path.iterdir() if f.is_file()]

result = []
for file in files:

    df = pd.read_excel("downloads/" + file)
    print(file)
    def search(target_value, df):
        # Преобразуем все значения в DataFrame в строки, чтобы избежать ошибок при применении .str.contains()
        df_str = df.astype(str)
        
        # Применяем str.contains() ко всем ячейкам DataFrame
        matches = df_str.apply(lambda x: x.str.contains(target_value, na=False))
        
        # Получаем координаты всех совпадений (строки, столбцы)
        indices = list(zip(*matches.values.nonzero()))
        
        return indices
    
    
    # Заданное значение
    target_value_start = 'Код\nИнструмента'
    target_value_end = 'Итого:'
    
    trimmed_dfs = []

    start_indices = search('Код\nИнструмента',df)  # Координаты всех совпадений (строка, столбец)
    
    end_indices = search('Итого:',df)  # Координаты всех совпадений (строка, столбец)
    date_indices = list(search('Дата торгов:',df))  # Координаты всех совпадений (строка, столбец)
    if len(start_indices) == 2:
        date = df.iloc[date_indices[0][0],date_indices[0][1]].split(' ')[2]
        df1 = df.iloc[start_indices[0][0]:end_indices[0][0]].reset_index(drop=True).iloc[2:]
        df2 =df.iloc[start_indices[1][0]+2:end_indices[1][0]].reset_index(drop=True)
        df = pd.concat([df1,df2]).iloc[:,1:]
    else:
        date = df.iloc[date_indices[0][0],date_indices[0][1]].split(' ')[2]
        df = df.iloc[start_indices[0][0]:end_indices[0][0],1:].reset_index(drop=True).iloc[2:]
    df.columns = [
            'Код Инструмента',
            'Наименование Инструмента',
            'Базис поставки',
            'Объем Договоров в единицах измерения',
            'Обьем Договоров, руб.',
            'Изменение рыночной цены к цене предыдуего дняРуб.',
            'Изменение рыночной цены к цене предыдуего дня%',
            'Цена (за единицу измерения), руб.Минимальная',
            'Цена (за единицу измерения), руб.Средневзвешенная',
            'Цена (за единицу измерения), руб.Максимальная',
            'Цена (за единицу измерения), руб.Рыночная',
            'Цена в Заявках (за единицу измерения)Лучшее предложение',
            'Цена в Заявках (за единицу измерения)Лучший спрос',
            'Количество Договоров, шт.Количество Договоров, шт.'
        ]
    df['Дата торгов'] = date
    result.append(df)
res = pd.concat(result)
res.to_excel('Итог.xlsx',index=False)

logging.info('Работа приложения завершена')