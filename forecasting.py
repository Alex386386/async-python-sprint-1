import shutil
from multiprocessing import Manager
from multiprocessing.pool import Pool
from threading import Thread

from tasks import (DataFetchingTask, DataCalculationTask, DataAggregationTask,
                   DataAnalyzingTask, )
from utils import BASE_DIR, CITIES


def start_aggregation():
    data_aggregation_task = DataAggregationTask(BASE_DIR / 'results')

    sub_result, result = data_aggregation_task.collect_and_overwrite()
    new_result = data_aggregation_task.filtering_of_data(sub_result, result)
    data_aggregation_task.write_filtered_data(new_result)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    manager = Manager()
    data_queue = manager.Queue()
    data_fetching_task = DataFetchingTask(BASE_DIR / 'output_data', data_queue)
    data_calculation_task = DataCalculationTask(BASE_DIR / 'output_data',
                                                BASE_DIR / 'analyzed_data',
                                                data_queue)
    data_analyzing_task = DataAnalyzingTask(BASE_DIR / 'results')

    processing_thread = Thread(target=data_calculation_task.process_queue)
    processing_thread.start()

    with Pool() as fetch_pool:
        fetch_pool.map(data_fetching_task.get_data, CITIES.keys())

    data_queue.put(None)

    processing_thread.join()

    start_aggregation()

    data_analyzing_task.get_the_answer()

    shutil.rmtree(BASE_DIR / 'output_data')
    shutil.rmtree(BASE_DIR / 'analyzed_data')


if __name__ == "__main__":
    forecast_weather()
