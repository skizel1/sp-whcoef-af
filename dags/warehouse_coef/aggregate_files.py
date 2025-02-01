import os
from datetime import datetime, timedelta

# S3 options
PROJECT_NAME = 'warehouse_coef'
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
ENDPOINT = "https://storage.yandexcloud.net"
REGION_NAME = 'ru-central1'
STORAGE_OPTIONS = {
    'endpoint_url': ENDPOINT,
    'key': AWS_ACCESS_KEY_ID,
    'secret': AWS_SECRET_ACCESS_KEY,
    'client_kwargs': {
        'region_name': REGION_NAME,
    }
}
S3_PREFIX = f's3://{S3_BUCKET_NAME}'

COEFF_SETTINGS = {
    "Бесплатная приемка": 0,
    "х1 и меньше": 1,
    "х2 и меньше": 2,
    "х3 и меньше": 3,
    "х4 и меньше": 4,
    "х5 и меньше": 5,
    "х10 и меньше": 10,
    "х20 и меньше": 20,
}

TARGET_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def truncate_datetime(granularity, current_datetime):
    current_datetime = datetime.strptime(current_datetime, TARGET_DATETIME_FORMAT)
    if granularity == 'by_hour':
        current_datetime = current_datetime.replace(hour = 0, minute = 0, second = 0)
    elif granularity == 'by_day':
        current_datetime = current_datetime.replace(day = 1, hour = 0, minute = 0, second = 0)
    current_datetime = datetime.strftime(current_datetime, TARGET_DATETIME_FORMAT)
    return current_datetime

def s3_save_parquet(df, path_to_save):
    #period_start_datetime = truncate_datetime(granularity, current_datetime)
    file_path_name = f's3://{S3_BUCKET_NAME}/{path_to_save}'
    df.to_parquet(file_path_name, engine = 'fastparquet', storage_options = STORAGE_OPTIONS)
    print(f'Saved file with S3 path: {file_path_name}')

def s3_delete_parquet(s3_client, s3_all_objects):
    if s3_all_objects == []:
        print('There is no files to delete')
    else:
        objects_to_delete = [{'Key': s3_object} for s3_object in s3_all_objects]
        response = s3_client.delete_objects(
            Bucket = S3_BUCKET_NAME,
            Delete = {'Objects': objects_to_delete}
        )
        deleted_objects = [deleted_key['Key'] for deleted_key in response['Deleted']]
        print(f'Files has been deleted. The list of the deleted files: {deleted_objects}')

def s3_create_boto_client(aws_access_key_id, aws_secret_access_key, region_name, endpoint_url):
    import boto3
    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region_name,
        endpoint_url = endpoint_url
    )
    return s3_client

def s3_get_objects_list(s3_client, path_to_read):

    print(f'Getting S3 object names from path: {path_to_read}...')
    s3_all_objects = s3_client.list_objects_v2(#only up to 1000 files
        Bucket = S3_BUCKET_NAME,
        Prefix = path_to_read
    )
    if s3_all_objects['KeyCount'] == 0:
        print('There is no files.')
        return list()
    s3_all_objects_list = [s3_object["Key"] for s3_object in s3_all_objects['Contents']]
    print(f'Got {len(s3_all_objects_list)} file(-s).')
    return s3_all_objects_list

def s3_read_parquet(s3_all_objects):
    import pandas as pd
    s3_keys_list = [f'{S3_PREFIX}/{s3_object}' for s3_object in s3_all_objects]
    print('Reading files from S3...')
    df = pd.DataFrame()
    for s3_key in s3_keys_list:
        print(f'Reading S3 file: {s3_key}...')
        raw_data = pd.read_parquet(s3_key, engine = 'fastparquet', storage_options = STORAGE_OPTIONS)
        df = pd.concat([df, raw_data])
    df = df.reset_index(drop = True)
    df_rows_count = df.shape[0]
    print(f'Got {df_rows_count} rows from S3.')

    return df

def aggregate_files(granularity_from, granularity_to, current_datetime):
    s3_client = s3_create_boto_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION_NAME, ENDPOINT)
    granularity_from_datetime = truncate_datetime(granularity_from, current_datetime)
    granularity_to_datetime = truncate_datetime(granularity_to, current_datetime)
    #period_start_datetime = truncate_datetime(granularity_to, current_datetime)
    source_files_path = f'project={PROJECT_NAME}/{granularity_from}/{granularity_from_datetime}'
    merged_file_path = f'project={PROJECT_NAME}/{granularity_to}/{granularity_to_datetime}/{granularity_from_datetime}.parquet'
    s3_source_objects = s3_get_objects_list(s3_client, source_files_path)
    s3_merged_object = s3_get_objects_list(s3_client, merged_file_path)
    s3_all_objects = s3_source_objects + s3_merged_object
    df = s3_read_parquet(s3_all_objects)
    s3_save_parquet(df, merged_file_path)
    s3_delete_parquet(s3_client, s3_source_objects)



### Calculate the limits
def get_aggregate_data(coeffs_data, coeff_borders):
    import pandas as pd
    """
    Функция для обработки DataFrame с лимитами.
    
    Args:
        coeffs_data (pd.DataFrame): Сырые данные.
        coeff_borders (dict): Границы коэффициентов.
    Returns:
        DataFrame - агрегированные данные.
    """
    # Фильтруем по датам

    # Добавляем дополнительные столбцы
    coeffs_data['date'] = coeffs_data['created_hour'].dt.date
    coeffs_data['hour'] = coeffs_data['created_hour'].dt.hour

    # Список столбцов для низких коэффициентов
    border_columns = []
    for border in coeff_borders.values():
        column_name = f'is_not_more_{border}'
        border_columns.append(column_name)
        coeffs_data[column_name] = (
            coeffs_data['coefficient'] <= border
            ).astype(int)

    # Настраиваем агрегацию
    agg_dict = {col: 'sum' for col in border_columns}
    agg_dict.update({'coefficient': 'count'})

    # Агрегация по датам отгрузки
    limits_by_date = (
        coeffs_data
        .groupby(
            ['warehouseName', 'boxTypeName', 'created_hour', 
             'hour', 'date', 'days_before_shipment']
        )
        .agg(agg_dict)
        .reset_index()
    )

    # Добавляем доли времени с подходящими коэффициентами
    for column in border_columns:
        border = column.split('_')[-1]
        limits_by_date[f'time_share_{border}'] = (
            limits_by_date[column] / limits_by_date['coefficient']
        )
        
        # Удаляем временные столбцы
        del limits_by_date[column]
    limits_by_date['date'] = pd.to_datetime(limits_by_date['date']) ## без конвертации не сохраняет
    return limits_by_date


def prepare_mart(current_datetime):
    month_date = truncate_datetime('by_day', current_datetime)
    day_date = truncate_datetime('by_hour', current_datetime)
    s3_objects_to_read = [f'project={PROJECT_NAME}/by_day/{month_date}/{day_date}.parquet']
    df = s3_read_parquet(s3_objects_to_read)
    limits_by_date = get_aggregate_data(df, COEFF_SETTINGS)
    s3_save_parquet(limits_by_date, f'project={PROJECT_NAME}/data_mart=limits_by_date/{day_date}.parquet')

def merge_marts(current_datetime, calc_depth_days):
    '''
        target_dates_list - надо сравнить? или получить все и потом отфилтровать по дням?
    '''
    import pandas as pd
    s3_client = s3_create_boto_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION_NAME, ENDPOINT)
    

    calc_depth_days = calc_depth_days - 1
    period_end_string = truncate_datetime('by_hour', current_datetime)
    period_end_date = datetime.strptime(period_end_string, TARGET_DATETIME_FORMAT)
    period_start_date = period_end_date - timedelta(days = calc_depth_days)
    period_start_string = datetime.strftime(period_start_date, TARGET_DATETIME_FORMAT)
    period_month_start_date = truncate_datetime('by_day', period_start_string)
    

    datetimes_list = pd.date_range(period_start_date, period_end_date, freq = 'd').to_list()
    month_start_dates_list = pd.date_range(period_month_start_date, period_end_date, freq = 'MS').to_list()
    s3_objects = []
    for month_start_date in month_start_dates_list:
        month_prefix = str(month_start_date)[0:7]
        path_to_read = f'project={PROJECT_NAME}/data_mart=limits_by_date/{month_prefix}'
        s3_objects += s3_get_objects_list(s3_client, path_to_read)

    target_dates_list = [date.strftime(TARGET_DATETIME_FORMAT) for date in datetimes_list]
    fact_dates_list = [s3_object.split('/')[-1].split('.')[0] for s3_object in s3_objects]

    dates_list_to_read = []
    for date in target_dates_list:
        if date in fact_dates_list:
            dates_list_to_read.append(date)

    files_prefix = f'project={PROJECT_NAME}/data_mart=limits_by_date'
    s3_object_files_path = [f'{files_prefix}/{file_date}.parquet' for file_date in dates_list_to_read]
    print(s3_object_files_path)

    df = s3_read_parquet(s3_object_files_path)
    s3_save_parquet(df, f'project={PROJECT_NAME}/for_app/limits_by_date.parquet')


'''
    aggregate_files - мержим сырье по часу/дню
    prepare_mart - делаем агрегацию из сырья
    merge_marts - мержим агрегацию по дням
'''

#aggregate_files('by_second', 'by_hour', '2025-02-01T13:00:00')


#aggregate_files('by_hour', 'by_day', '2025-02-01T00:00:00')
#aggregate_files('by_hour', 'by_day', '2025-01-31T00:00:00')
#aggregate_files('by_hour', 'by_day', '2025-01-27T00:00:00')
#aggregate_files('by_hour', 'by_day', '2025-01-28T00:00:00')

#prepare_mart('2025-02-T00:00:00')
#prepare_mart('2025-01-26T00:00:00')
#prepare_mart('2025-01-30T00:00:00')
#prepare_mart('2025-01-31T00:00:00')

#merge_marts('2025-02-01T00:00:00', calc_depth_days = 30)