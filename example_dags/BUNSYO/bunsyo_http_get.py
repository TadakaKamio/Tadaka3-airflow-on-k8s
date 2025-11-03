from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
import pyarrow.fs as fs
import os
import requests
from datetime import datetime
import time
import csv
from requests.auth import HTTPBasicAuth
from urllib.parse import unquote
from BUNSYO.logger import logger
from BUNSYO.bunsyo_http_get_preprocess import *
from BUNSYO.bunsyo_http_get_postprocess import *
#import json


variables = Variable.get('BUNSYO_HTTP_GET', deserialize_json=True)
#print(variables["cpu_limit"])
#print(dir(variables))

#cpu_limit_value = variables["cpu_limit"]
#memory_limit_value = variables["memory_limit"]

#print(f"cpu_limit: {cpu_limit}, memory_limit: {memory_limit}")

# DAGのスペック設定
cpu_req = cpu_lim = variables.get("cpu_limit") # 480m
ram_req = ram_lim = variables.get('memory_limit') # 3G

pod_config = {
    'pod_override':
    k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
        k8s.V1Container(name='I-am-rich',
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                'cpu': cpu_lim,
                                'memory': ram_lim
                            },
                            requests={
                                'cpu': cpu_req,
                                'memory': ram_req
                            }))
    ]))
}

# ファイルダウンロード(HTTP_リクエスト)
def get_file(dtapfs, dest_path, file, auth, decode_filename, max_retries, wait_seconds):
    retry_times = max_retries
    while retry_times > 0:
        try:
            # ファイルをダウンロード            
            response = requests.get(file, auth=auth, verify=False, stream=True)
            response.raise_for_status()

            dst_path = os.path.join(dest_path, decode_filename)

            # ファイルの書き込み
            with dtapfs.open_output_stream(dst_path, compression=None) as dst_file:
                for chunk in response.iter_content(chunk_size=1024**2):
                    dst_file.write(chunk)

            response.close()

            return decode_filename
            
        except Exception as e:
            retry_times -= 1
            msg = f'Failed to download or save file from {file}. Error: {e}'
        if retry_times > 0:
            time.sleep(wait_seconds)
    raise Exception(msg)

# CSVファイルの作成
def create_csv(dtapfs, dest_path, uploaded_file_list, file_list_csv, exist_files):
    import shutil

    os.environ['TZ'] = 'Asia/Tokyo'
    time.tzset()
    process_date = datetime.now().strftime('%Y%m%d')
    csv_day = f'{file_list_csv}_{process_date}.csv'

    csv_hdfs_path = os.path.join(dest_path, csv_day)

    def download_existing_csv(hdfs_path, local_file):
        try:
            with dtapfs.open_input_file(hdfs_path) as remote_file, open(local_file, 'wb') as local_out_file:
                local_out_file.write(remote_file.read())
        except Exception as e:
            error_message = f'Error: Failed to fetch existing CSV file from {hdfs_path}. Error: {str(e)}'
            logger.error(error_message)
            raise Exception(error_message)
        
    def write_and_upload_csv(local_file, headers, rows, hdfs_target_path, append):
        mode = 'a' if append else 'w'
        with open(local_file, mode, newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            if not append:
                writer.writerow(headers)
            writer.writerows(rows)
        
        try:
            with open(local_file, 'rb') as local_file_in:
                with dtapfs.open_output_stream(hdfs_target_path) as remote_out_file:
                    shutil.copyfileobj(local_file_in, remote_out_file)
            logger.info(f'Success: Successfully uploaded {local_file} to {hdfs_target_path}.')
        except Exception as e:
            error_message = f'Error: Failed to upload {local_file} to {hdfs_target_path}. Error: {str(e)}'
            logger.error(error_message)
            raise Exception(error_message)

    csv_row = [(filename, putpath) for filename, _, putpath in uploaded_file_list]

    append = csv_day in exist_files
    headers = ['FileName', 'PutPath']
    if append:
        download_existing_csv(csv_hdfs_path, csv_day)       
    write_and_upload_csv(csv_day, headers, csv_row, csv_hdfs_path, append)

def main(**kwargs):
    method = variables.get('method')
    base_url = variables.get('base_url')
    src_path = variables.get('src_path')
    user_auth = variables.get('user_auth')
    dtap_path = variables.get('dtap_path')
    dest_path = variables.get('dest_path')
    file_list_csv = variables.get('file_list_csv')
    max_retries = int(variables.get('max_retries'))
    wait_seconds = int(variables.get('wait_seconds'))
    csv_flag = (variables.get('csv_flg')) == "True"
    uuid_flg = (variables.get('uuid_flg')) == "True"
    filter_target = variables.get('filter_target')

    auth = HTTPBasicAuth(user_auth.get('user'),user_auth.get('password'))
    upload_file_list = []
    error_messages = []

    # HDFSへの接続
    try:
        dtapfs = fs.HadoopFileSystem.from_uri(dtap_path)
        logger.info('Success: success to connect hdfs')
    except Exception as e:
        logger.error(f'Error: falied to connect hdfs. Reason: {e}')
        raise e
    
    # ディレクトリ内の全てのファイルを削除
    logger.info('Start cleaning up HDFS directory except backup directories.')

    try:
        file_info = dtapfs.get_file_info(fs.FileSelector(dest_path, recursive=False))
    except Exception as e:
        msg = f'Failed to list files in path {dest_path}. Error: {str(e)}'
        logger.error(msg)
        raise Exception(msg)

    # "BK_YYYYmmddd"ディレクトリ以外のファイルを削除
    for info in file_info:
        if info.type == fs.FileType.File:
            try:
                dtapfs.delete_file(info.path)
                logger.info(f"Deleted file: {info.path}")
            except Exception as e:
                logger.error(f"Could not delete file: {info.path}. Error: {e}")

    logger.info('Cleanup finished.')
    
    logger.info('Start preprocess')
    # 前処理
    download_list, exist_files = preprocess(dtapfs, dest_path, method, base_url, src_path, 
                                            auth, file_list_csv, filter_target)
    
    # ダウンロード対象ファイルがない場合、日別のCSVを作成し終了
    if not download_list:
        logger.info('No files to download. Finish processing.')
        create_csv(dtapfs, dest_path, upload_file_list, file_list_csv, exist_files)
        return

    logger.info('Success preprocess')

    logger.info('Start file download')
    logger.info('download file. file is ' + str(download_list))
     # ダウンロード対象ファイルを1ファイルずつ処理
    for file, URI in download_list:
        try:
            # ファイルをダウンロード
            decode_filename = unquote(os.path.basename(file))
            
            filename = get_file(dtapfs, dest_path, file, auth, 
                                     decode_filename, max_retries, wait_seconds)

            logger.info(f'HDFS uploaded files: {decode_filename}')
            upload_file_list.append((filename, URI, URI))

        except Exception as e:
            err_file_msg = f'error download files : {file}.'
            logger.error(err_file_msg)
            err_msg = f'error message : {e}.'
            logger.error(err_msg)
            error_messages.append({
                'error_file': err_file_msg,
                'error_message': err_msg
            })
                         
    if error_messages:
        raise Exception(str(error_messages))
    
    logger.info('Success file download')
                
    logger.info('Start postprocess')

    new_upload_file_list = postprocess(dtapfs, dest_path, upload_file_list, 
                                       csv_flag, uuid_flg, exist_files)
    
    logger.info('Success postprocess')

    create_csv(dtapfs, dest_path, new_upload_file_list, file_list_csv, exist_files)


# DAG ################################
default_args = {
    'owner': '6S',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 18),
}

schedule = variables.get('schedule_interval')
dag = DAG(
    dag_id='BUNSYO_HTTP_GET',
    default_args=default_args,
    description='BUNSYO_HTTP_GET',
    schedule_interval=schedule if bool(schedule) else None,
    catchup=False,
)

# task #############################
download_task = PythonOperator(
    task_id='bunsyo_http_get',
    python_callable=main,
    dag=dag,
    executor_config=pod_config,
    provide_context=True,
)
####################################

download_task
