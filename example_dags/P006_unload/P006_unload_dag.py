from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from airflow.models import Variable
from airflow.utils.dates import days_ago
from common.log import MyAppLog
logger = MyAppLog()

variables = Variable.get('HTTP_GET', deserialize_json=True)

# DAGのスペック設定
cpu_req = cpu_lim = Variable.get("P006_cpu_limit") # 500m
ram_req = ram_lim = Variable.get("P006_memory_limit") # 3G

pod_config = {
    "pod_override":
    k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
        k8s.V1Container(name="I-am-rich",
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                "cpu": cpu_lim,
                                "memory": ram_lim
                            },
                            requests={
                                "cpu": cpu_req,
                                "memory": ram_req
                            }))
    ]))
}

# タスクから呼び出すmain関数
def P006_unload(**kwargs):
    import paramiko
    from datetime import datetime

    # 親DAGから以下の値を取得する
    try:
        targetymd = kwargs['dag_run'].conf.get('targetymd')
        logger.info(f"Success: success to get variables {targetymd} from parent DAG.")
    except Exception as e:
        logger.error(f"Error: failed to get variables from Parent DAG. Reason: {e}")
        raise e
    
    # 変数
    db_name = Variable.get("P006_unload_db_name") # pgss1
    table_name = Variable.get("P006_unload_table_name") # dwh_d30_lo_sm_corr
    current_date = datetime.now().strftime('%Y%m%d') # 20240403
    df_mount_point = Variable.get("P006_df_mount_point") # /tdh
    dtap_work_dir = Variable.get("P006_dtap_work_dir") # /volume7/unload/dwh_d30_lo_sm_corr
    shell_script_path = Variable.get("P006_script_path")  # /home/mapr/script/airflow/P006/P006_unload.sh
    python_script_path = Variable.get("P006_python_path") # /home/mapr/script/airflow/P006/P006_unload_spark.py
    csv_prefix = Variable.get("P006_csv_prefix") # SM_D30_LO_HC
    df_work_dir = f'{df_mount_point}{dtap_work_dir}' # /tdh/volume7/unload/dwh_d30_lo_sm_corr
    
    # DFのクレデンシャル
    df_auth = Variable.get("P006_df_auth", deserialize_json=True)
    
    # bashスクリプトを実行するコマンドを作成する
    command = f'bash {shell_script_path} {python_script_path} {df_work_dir} {db_name} {table_name} {csv_prefix} {current_date} {targetymd} '
    
    # HiveテーブルのデータをDF上にcsvファイルとして出力する
    try:
        # SSHクライアントのインスタンスを作成
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # DF01から順番にSSHサーバーへの接続を試みる
        connection_success = False
        for hostname in df_auth["hostname"]:
            try:
                client.connect(
                    hostname,
                    int(df_auth["port"]),
                    df_auth["username"],
                    df_auth["password"],
                )
                logger.info(f"Successfully connected to {hostname}.")
                connection_success = True
                break  # 接続成功時はループを抜ける
            except Exception as e:
                logger.warn(f"Failed to connect to {hostname}, error: {e}")
        
        # 全てのDFに接続できなかった場合はエラーを発生させる
        if not connection_success:
            logger.error("Failed to connect to all hosts.")
            raise

        # コマンドの実行
        logger.info(f"Start executing Shell Script: {command}")
        stdin, stdout, stderr = client.exec_command(command)
        
        # 実行結果を取得(バイナリから文字列に変換する)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        
        # コマンドがエラー終了した場合
        if stderr.channel.recv_exit_status() != 0:
            logger.error(f"Command execution failed: {error}")
            raise
        else:
            logger.info(f"Command execution succeeded: {output}")

    except Exception as e:
        # エラーがあれば表示
        logger.error(f"Failed to exec Shell Script : {e}")
        raise
    finally:
        # SSH接続を閉じる
        client.close()

    # 処理完了のログ
    logger.info("All processes have been completed successfully.")
    
# DAG ################################
default_args = {
    'owner': 'U2',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='P006_unload',
    default_args=default_args,
    description='P006_unload',
    schedule_interval=None,
    catchup=False,
)

# task #############################
unload_dwh_d30_lo_sm_corr = PythonOperator(
    task_id='P006_unload',
    python_callable=P006_unload,
    dag=dag,
    executor_config=pod_config,
    provide_context=True,
)
####################################

unload_dwh_d30_lo_sm_corr
