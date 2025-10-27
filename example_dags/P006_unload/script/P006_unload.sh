#!/bin/bash

# スクリプトの実行例
# bash P006_unload.sh <PythonスクリプトPath> <作業ディレクトリPath> <DB名> <出力ファイルのPrefix> <処理日> <算出対象年月日>
# bash P006_unload.sh /home/mapr/script/airflow/P006/P006_unload_spark.py /tdh/volume7/unload/dwh_d30_lo_sm_corr pgss1 SM_D30_LO_HC 20240403 20230502

# 引数の数をチェックする
if [ $# -ne 7 ]; then
    echo "Error: 5 arguments required"
    echo "Usage: $0 <PY_SCRIPT_PATH> <DF_WORK_DIR> <DB_NAME> <TABLE_NAME> <CSV_PREFIX> <CURRENT_DATE> <TARGETYMD>"
    exit 1
fi

# 引数のいずれかが空かどうかをチェックする
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ] || [ -z "$6" ] || [ -z "$7" ]; then
    echo "Error: empty argument"
    echo "Usage: $0 <PY_SCRIPT_PATH> <DF_WORK_DIR> <DB_NAME> <TABLE_NAME> <CSV_PREFIX> <CURRENT_DATE> <TARGETYMD>"
    exit 1
fi

# 引数
PY_SPARK_PATH=$1 # /home/mapr/script/airflow/P006/P006_unload_spark.py
DF_WORK_DIR=$2 # /tdh/volume7/unload/dwh_d30_lo_sm_corr
DB_NAME=$3 # pgss1
TABLE_NAME=$4 # DWH_D30_LO_SM_CORR
CSV_PREFIX=$5 # SM_D30_LO_HC
CURRENT_DATE=$6 # 20240403
TARGETYMD=$7 # 20230502

# 変数
RANDOM_STR=$(uuidgen) # ランダムな文字列を生成してユニークな作業ディレクトリを作成
DF_CSV_PATH="${DF_WORK_DIR}/${CSV_PREFIX}_${CURRENT_DATE}_${TARGETYMD}.csv" # /tdh/volume7/unload/dwh_d30_lo_sm_corr/SM_D30_LO_HC_20240403_20230502.csv
DF_QUERY_OUTPUT_DIR="${DF_WORK_DIR}/work_${RANDOM_STR}" # /tdh/volume7/unload/dwh_d30_lo_sm_corr/work_<ランダム文字列>

# spark-submitコマンドの実行
/opt/mapr/spark/spark-3.3.1/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --executor-cores=2 \
    --executor-memory=1G \
    --driver-memory=3G \
    --conf "spark.driver.memoryOverhead=1G" \
    --conf "spark.executor.memoryOverhead=1G" \
    --conf "spark.executor.instances=30" \
    ${PY_SPARK_PATH} ${DF_WORK_DIR} ${DB_NAME} ${TABLE_NAME} ${CSV_PREFIX} ${CURRENT_DATE} ${TARGETYMD} || { echo "spark-submit command failed"; exit 1; }

# spark-submitコマンドの実行結果をチェックする
if [ $? -ne 0 ]; then
    echo "Error: spark-submit command failed"
    exit 1
fi

echo "Script on DF completed successfully"
exit 0
