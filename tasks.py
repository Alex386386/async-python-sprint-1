import concurrent
import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict

from external.analyzer import load_data, analyze_json, dump_data
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES_NAMES, BASE_DIR


class DataFetchingTask:
    def __init__(self):
        self.logger = logging.getLogger('mylog')

    def download_data(self, data: Dict, city: str):
        filename = f'{city}.json'
        downloads_dir = BASE_DIR / 'output_data'
        downloads_dir.mkdir(exist_ok=True)
        archive_path = downloads_dir / filename
        if data:
            with open(archive_path, 'w') as file:
                formatted_data = json.dumps(data, indent=2)
                file.write(formatted_data)
            self.logger.info(
                f'Информация о городе {CITIES_NAMES[city]} загружена.')

    def get_data(self, city_name: str):
        try:
            url_with_data = get_url_by_city_name(city_name)
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            if resp:
                self.download_data(resp, city_name)
        except Exception as ex:
            self.logger.error(f'Возникли проблемы с получением данных: {ex}.')


class DataCalculationTask:
    def __init__(self):
        self.logger = logging.getLogger('mylog')

    def process_file(self, file_path: Path):
        file_path = str(file_path).split('/')[-1]
        data_dir = BASE_DIR / 'output_data'
        write_dir = BASE_DIR / 'analyzed_data'
        write_dir.mkdir(exist_ok=True)
        output_path = str(write_dir / file_path)
        data = load_data(f'{data_dir / file_path}')
        data = analyze_json(data)
        dump_data(data, output_path)
        self.logger.info(
            f'Информация из файла {file_path} обработана успешно.')

    def data_processing(self):
        data_dir = BASE_DIR / 'output_data'
        directory = Path(data_dir)

        with ThreadPoolExecutor() as executor:
            executor.map(self.process_file, directory.glob('*.json'))
        self.logger.info('Все файлы прошли анализ.')


class DataAggregationTask:
    def __init__(self):
        self.logger = logging.getLogger('mylog')

    def process_file(self, file_path: Path):
        with open(file_path, 'r') as file:
            data = json.load(file)
            filter_days = [x for x in data['days'] if
                           x['hours_start'] == 9 and x['hours_end'] == 19]
            city_name = file_path.stem
            filtered_data = {CITIES_NAMES[city_name]: filter_days}
            city_temp_data = [CITIES_NAMES[city_name], 'Температура, среднее']
            city_hours_data = ['', 'Без осадков, часов']

            for day in filtered_data[CITIES_NAMES[city_name]]:
                city_temp_data.append(day['temp_avg'])
                city_hours_data.append(day['relevant_cond_hours'])

            average_temp = sum(city_temp_data[-3:]) / 3
            average_hours = sum(city_hours_data[-3:]) / 3
            city_temp_data.append(average_temp)
            city_hours_data.append(average_hours)
            city_hours_data.append('')
            subresult = {CITIES_NAMES[city_name]: {'temp': average_temp,
                                                   'hours': average_hours}}
            result = [city_temp_data, city_hours_data]
            self.logger.info(
                f'По файлу {file_path} произведён подсчёт значений.')
            return subresult, result

    def collect_and_overwrite(self):
        directory = BASE_DIR / 'analyzed_data'
        result = [('Город', 'Ед.изм', '2022-05-26', '2022-05-27',
                   '2022-05-28', 'Среднее', 'Рейтинг')]
        subresult = {}

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.process_file, file_path)
                       for file_path in directory.glob('*.json')]
            for future in concurrent.futures.as_completed(futures):
                sr, res = future.result()
                subresult.update(sr)
                result.extend(res)
        self.logger.info('Подсчёт средней температуры произведён.')
        return subresult, result

    def filter_of_data(self, subresult: Dict[str, Dict[str, float]],
                       result: List[List[str]]):
        sorted_data = sorted(subresult.items(),
                             key=lambda x: (x[1]['temp'], x[1]['hours']),
                             reverse=True)
        counter: int = 1
        for city in sorted_data:
            for data in result:
                if city[0] in data:
                    data.append(str(counter))
                    counter += 1
        self.logger.info('Добавлен рейтинг для городов.')
        return result

    def write_filtered_data(self, result: List[List[str]]):
        results_dir = BASE_DIR / 'results'
        results_dir.mkdir(exist_ok=True)
        file_name = 'result.csv'
        file_path = results_dir / file_name
        with open(file_path, 'w', encoding='utf-8') as f:
            writer = csv.writer(f, dialect='unix')
            writer.writerows(result)
        self.logger.info(f'Данные записаны по адресу {file_path}.')


class DataAnalyzingTask:
    def __init__(self):
        self.logger = logging.getLogger('mylog')

    def get_the_answer(self):
        directory = BASE_DIR / 'results'
        file_path = directory / 'result.csv'
        result = {}
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            reader = list(reader)
            for idx, row in enumerate(reader):
                if row[-1] != '' and row[-1] != 'Рейтинг':
                    result[row[0]] = (row[-2], reader[idx + 1][-2], row[-1])
            result = sorted(result.items(), key=lambda item: int(item[1][-1]))
            self.logger.info('Вывод результатов анализа.')
            for idx, city in enumerate(result):
                if idx == 0:
                    print(city[0])
                elif idx > 0 and city[1][0] == result[idx - 1][1][0]:
                    print(city[0])
                elif idx > 0 and city[1][0] != result[idx - 1][1][0]:
                    break
