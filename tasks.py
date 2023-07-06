import concurrent
import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
from pathlib import Path

from external.analyzer import load_data, analyze_json, dump_data
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES_NAMES, BASE_DIR

logger = logging.getLogger('mylog')
logging.basicConfig(level=logging.INFO)


class DataFetchingTask:
    def __init__(self, downloads_dir: Path, queue: Queue):
        self.downloads_dir = downloads_dir
        self.queue = queue

    def download_data(self, data: dict, city: str) -> None:
        if data:
            filename = f'{city}.json'
            self.downloads_dir.mkdir(exist_ok=True)
            archive_path = self.downloads_dir / filename
            with open(archive_path, 'w') as file:
                formatted_data = json.dumps(data, indent=2)
                file.write(formatted_data)
            logger.info(
                f'Информация о городе {CITIES_NAMES[city]} загружена.')

    def get_data(self, city_name: str) -> None:
        try:
            url_with_data = get_url_by_city_name(city_name)
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            if resp:
                self.download_data(resp, city_name)
                self.queue.put(city_name)
        except Exception as ex:
            logger.error(f'Возникли проблемы с получением данных: {ex}.')


class DataCalculationTask:
    def __init__(self, data_dir: Path, write_dir: Path, queue: Queue):
        self.data_dir = data_dir
        self.write_dir = write_dir
        self.queue = queue

    def process_file(self, file_path: Path) -> None:
        file_path = str(file_path).split('/')[-1]
        self.write_dir.mkdir(exist_ok=True)
        output_path = str(self.write_dir / file_path)
        data = load_data(f'{self.data_dir / file_path}')
        data = analyze_json(data)
        dump_data(data, output_path)
        logger.info(f'Информация из файла {file_path} обработана успешно.')

    def process_queue(self) -> None:
        while True:
            file_name = self.queue.get()
            if file_name is None:
                break
            file_path = Path(f'{file_name}.json')
            self.process_file(file_path)


class DataAggregationTask:
    def __init__(self, results_dir: Path):
        self.results_dir = results_dir

    @staticmethod
    def process_file(file_path: Path) -> tuple[dict[str, dict[str, float]],
                                               list[list[str]]]:
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

        # Подсчёт среднего значения температуры для города.
        average_temp = sum(city_temp_data[2:]) / len(city_temp_data[2:])
        # Подсчёт среднего значения количества часов без осадков.
        average_hours = sum(city_hours_data[2:]) / len(city_hours_data[2:])
        city_temp_data.append(average_temp)
        city_hours_data.append(average_hours)
        city_hours_data.append('')
        sub_result = {CITIES_NAMES[city_name]: {'temp': average_temp,
                                                'hours': average_hours}}
        result = [city_temp_data, city_hours_data]
        logger.info(f'По файлу {file_path} произведён подсчёт значений.')
        return sub_result, result

    def collect_and_overwrite(self) -> tuple[dict[str, dict[str, float]],
                                             list[list[str]]]:
        directory = BASE_DIR / 'analyzed_data'
        result = [('Город', 'Ед.изм', '2022-05-26', '2022-05-27',
                   '2022-05-28', 'Среднее', 'Рейтинг')]
        sub_result = {}

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.process_file, file_path)
                       for file_path in directory.glob('*.json')]
            for future in concurrent.futures.as_completed(futures):
                sr, res = future.result()
                sub_result.update(sr)
                result.extend(res)
        logger.info('Подсчёт средней температуры произведён.')
        return sub_result, result

    @staticmethod
    def filtering_of_data(sub_result: dict[str, dict[str, float]],
                          result: list[list[str]]) -> list[list[str]]:
        sorted_data = sorted(sub_result.items(),
                             key=lambda x: (x[1]['temp'], x[1]['hours']),
                             reverse=True)
        counter: int = 1
        for city in sorted_data:
            for data in result:
                if city[0] in data:
                    data.append(str(counter))
                    counter += 1
        logger.info('Добавлен рейтинг для городов.')
        return result

    def write_filtered_data(self, result: list[list[str]]) -> None:
        self.results_dir.mkdir(exist_ok=True)
        file_name = 'result.csv'
        file_path = self.results_dir / file_name
        with open(file_path, 'w', encoding='utf-8') as f:
            writer = csv.writer(f, dialect='unix')
            writer.writerows(result)
        logger.info(f'Данные записаны по адресу {file_path}.')


class DataAnalyzingTask:
    def __init__(self, directory: Path):
        self.directory = directory

    def get_the_answer(self) -> None:
        file_path = self.directory / 'result.csv'
        result = {}
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            reader = list(reader)
        for idx, row in enumerate(reader):
            if row[-1] != '' and row[-1] != 'Рейтинг':
                result[row[0]] = (row[-2], reader[idx + 1][-2], row[-1])
        result = sorted(result.items(), key=lambda item: int(item[1][-1]))
        logger.info('Вывод результатов анализа.')
        for idx, city in enumerate(result):
            if idx == 0:
                print(city[0])
            elif idx > 0 and city[1][0] == result[idx - 1][1][0]:
                print(city[0])
            elif idx > 0 and city[1][0] != result[idx - 1][1][0]:
                break
