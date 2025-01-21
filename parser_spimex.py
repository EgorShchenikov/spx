import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import logging
import random
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import xlsxwriter
import time
import shutil

from pathlib import Path
import sys

# Определяем базовую директорию, где находится exe или скрипт
if getattr(sys, 'frozen', False):
    # Программа запущена из exe
    BASE_DIR = Path(sys.executable).parent
else:
    # Программа запущена как скрипт
    BASE_DIR = Path(__file__).resolve().parent

# Создаем основной интерфейс
def start_parsing():
    try:
        # Получаем дату из поля ввода
        end_date_input = end_date_entry.get()
        end_date_user = datetime.strptime(end_date_input, "%d.%m.%Y").date()

        # Время начала парсинга
        start_time = time.time()

        # Запускаем парсинг с полученной датой
        run_parser(end_date_user)

        # Время окончания парсинга
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Обновляем метку с информацией о времени работы
        elapsed_time_text = f"Парсинг завершен! Время работы: {elapsed_time:.2f} секунд"
        result_label.config(text=elapsed_time_text)
        
    except ValueError:
        messagebox.showerror("Ошибка", "Неверный формат даты. Пожалуйста, используйте формат: дд.мм.гггг.")

# Основная логика парсинга
def run_parser(end_date_user):
    # Настройка логирования
    log_file = BASE_DIR / 'log.txt'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info('Начало работы приложения')
    logging.info(f"Начинаем парсинг до даты {end_date_user}")

    # Функция для безопасного имени файла
    def safe_filename(filename):
        return filename.replace('/', '_').replace('\\', '_')

    # Создание папки для загрузок с очисткой
    def ensure_output_folder(folder_name):
        folder_path = BASE_DIR / folder_name
        if folder_path.exists():
            logging.info(f'Очищаем папку для сохранения файлов: {folder_path}')
            shutil.rmtree(folder_path)  # Очищаем папку
        folder_path.mkdir(parents=True)  # Пересоздаем папку
        logging.info(f'Папка для сохранения файлов создана или очищена: {folder_path}')
        return folder_path

    output_folder = ensure_output_folder('downloads')

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
    current_end_date = datetime.strptime("31.12.2099", "%d.%m.%Y").date()

    while current_end_date >= end_date_user:
        try:
            url = f"https://spimex.com/markets/derivatives/trades/results/?page=page-{page}"
            with requests.Session() as session:
                req = session.get(url, timeout=60)
                req.raise_for_status()
                soup = BeautifulSoup(req.text, 'html.parser')

            for tag in soup.find_all('div', class_='accordeon-inner__item'):
                if 'files' not in str(tag):
                    link_tag = tag.find('a', class_='accordeon-inner__item-title link xls')
                    href = link_tag.get('href') if link_tag else None
                    date_span = tag.find('span')
                    trade_date = date_span.text.strip() if date_span else None

                    if not trade_date or not href:
                        continue

                    file_url = base_url + href
                    file_name = safe_filename(href.split('/')[-1].split('?')[0])
                    file_path = output_folder / file_name

                    current_end_date = datetime.strptime(trade_date, "%d.%m.%Y").date()

                    if current_end_date < end_date_user:
                        logging.info(f"Достигнута конечная дата {end_date_user}. Завершаем загрузку.")
                        break

                    download_file(file_url, file_path)

                    # Рандомная пауза между загрузками
                    pause_duration = random.uniform(0.1, 1)
                    logging.info(f"Пауза между загрузкой: {pause_duration:.2f} секунд")
                    time.sleep(pause_duration)

            page += 1
            logging.info(f'Переход на страницу {page}')
        except requests.exceptions.RequestException as e:
            logging.error(f'Ошибка при обработке страницы {url}: {e}', exc_info=True)
            break
        except Exception as e:
            logging.error(f'Неожиданная ошибка: {e}', exc_info=True)
            break

    logging.info('Блок обработки файлов')

    # Функция для поиска значений в DataFrame
    def search(target_value, df):
        try:
            df_str = df.astype(str)
            matches = df_str.apply(lambda x: x.str.contains(target_value, na=False))
            indices = list(zip(*matches.values.nonzero()))
            return indices
        except Exception as e:
            logging.error(f"Ошибка при поиске значения '{target_value}' в DataFrame: {e}", exc_info=True)
            return []

    try:
        # Формируем путь к выходному Excel-файлу в текущей директории
        output_excel_path = BASE_DIR / f"Итоги торгов {end_date_user}-{current_end_date}.xlsx"
        workbook = xlsxwriter.Workbook(output_excel_path)
    except Exception as e:
        logging.error(f"Ошибка при создании Excel-файла: {e}", exc_info=True)
        raise

    worksheet = workbook.add_worksheet("Основной режим торгов")
    header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    text_wrap_format = workbook.add_format({'border': 1})

    # Заголовки
    try:
        header_cells = [
            ('A1:A2', 'Наименование типа срочных контрактов'),
            ('B1:B2', 'Код типа срочных контрактов'),
            ('C1:C2', 'Наименование серии срочных контрактов'),
            ('D1:D2', 'Код серии срочных контрактов'),
            ('E1:E2', 'Наименование валюты контракта'),
            ('F1:F2', 'Код валюты контракта'),
            ('G1:G2', 'Курс валюты в рублях'),
            ('H1:I1', 'Объём договоров, контр.'),
            ('J1:K1', 'Объём договоров, валюта контракта'),
            ('L1:M1', 'Объём договоров, руб.'),
            ('N1:N2', 'Расчетная цена, валюта контракта'),
            ('O1:O2', 'Средневзвешенная цена, валюта контракта'),
            ('P1:P2', 'Расчетная цена пред. торгового дня, валюта контракта'),
            ('Q1:R1', 'Цена безадр. договора, валюта контракта'),
            ('S1:T1', 'Цена адр. договора, валюта контракта'),
            ('U1:V1', 'Цена заявки, валюта контракта'),
            ('Y1:Z1', 'Количество договоров, шт.'),
            ('W1:W2', 'Открытые позиции'),
            ('X1:X2', 'Открытые позиции пред. торгового дня'),
            ('AA1:AA2', 'Дата торгов')
        ]

        for cell, text in header_cells:
            worksheet.merge_range(cell, text, header_format)

    except Exception as e:
        logging.error(f"Ошибка при создании заголовков или объединении ячеек в Excel: {e}", exc_info=True)
        workbook.close()
        raise

    folder_path = BASE_DIR / 'downloads'

    try:
        files = [f.name for f in folder_path.iterdir() if f.is_file()]
    except Exception as e:
        logging.error(f"Ошибка при получении списка файлов: {e}", exc_info=True)
        files = []

    result = []

    # Обрабатываем каждый файл
    for file in files:
        try:
            df = pd.read_excel(f"{folder_path}/{file}")
        except Exception as e:
            logging.error(f"Ошибка при чтении файла {file}: {e}", exc_info=True)
            continue

        try:
            date_indices = list(search('Дата торгов:', df))
            if date_indices:
                date = df.iloc[date_indices[0][0], date_indices[0][1]].split(' ')[2]
                df = df[df['Unnamed: 0'].str.contains('фьюч.контракт', na=False)].reset_index(drop=True).dropna(axis=1)
                df['Дата торгов'] = date
                result.append(df)
            else:
                logging.warning(f"Дата торгов не найдена в файле {file}")
        except Exception as e:
            logging.error(f"Ошибка при обработке данных из файла {file}: {e}", exc_info=True)

    if result:
        try:
            res = pd.concat(result)
        except Exception as e:
            logging.error(f"Ошибка при объединении DataFrame: {e}", exc_info=True)
            res = pd.DataFrame()

        # Запись в Excel
        try:
            row = 1
            for _, data_row in res.iterrows():
                row += 1
                for col, value in enumerate(data_row):
                    worksheet.write(row, col, value, text_wrap_format)

            workbook.close()
        except Exception as e:
            logging.error(f"Ошибка при записи в Excel: {e}", exc_info=True)
            workbook.close()

    else:
        logging.warning("Нет данных для записи в Excel.")

    logging.info(f"Было обработано {len(files)} файлов")
    logging.info('Работа приложения завершена')

# Создаем окно
root = tk.Tk()
root.title("Парсер торгов")
root.config(bg="white")
root.geometry("300x200")
root.resizable(False, False)

# Добавляем поле для ввода даты
label = tk.Label(root, text="Введите конечную дату (дд.мм.гггг):", bg="white", font=("Arial", 12), fg="black")
label.pack(padx=10, pady=5)

end_date_entry = tk.Entry(root, bg="white", font=("Arial", 12), fg="black")
end_date_entry.pack(padx=10, pady=5)

# Кнопка для запуска парсинга
start_button = tk.Button(root, text="Начать парсинг", command=start_parsing, bg="white", font=("Arial", 12), fg="black")
start_button.pack(pady=20)

# Метка для вывода информации о времени работы
result_label = tk.Label(root, text="", bg="white", font=("Arial", 9), fg="black")
result_label.pack(pady=10)

# Запуск интерфейса
root.mainloop()
