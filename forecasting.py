import logging
import shutil
from multiprocessing.pool import Pool

from tasks import (DataFetchingTask, DataCalculationTask, DataAggregationTask,
                   DataAnalyzingTask, )
from utils import BASE_DIR
from utils import CITIES

logger = logging.getLogger('mylog')
logging.basicConfig(level=logging.INFO)


def start_aggregation():
    dataaggregationtask = DataAggregationTask()

    subresult, result = dataaggregationtask.collect_and_overwrite()
    new_result = dataaggregationtask.filter_of_data(subresult, result)
    dataaggregationtask.write_filtered_data(new_result)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    datafetchingtask = DataFetchingTask()
    datacalculationtask = DataCalculationTask()
    dataanalyzingtask = DataAnalyzingTask()

    with Pool() as pool:
        pool.map(datafetchingtask.get_data, CITIES.keys())

    datacalculationtask.data_processing()

    start_aggregation()

    dataanalyzingtask.get_the_answer()

    shutil.rmtree(BASE_DIR / 'output_data')
    shutil.rmtree(BASE_DIR / 'analyzed_data')


if __name__ == "__main__":
    forecast_weather()
