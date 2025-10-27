"""
このスクリプトは、Apache Sparkを使用してHiveテーブルのデータをcsvファイルに出力するものです。

使用方法:
 /opt/mapr/spark/spark-3.3.1/bin/spark-submit\
    --master yarn \
    --deploy-mode client \
    --executor-cores=3 \
    --executor-memory=1G \
    --driver-memory 3G \
    --conf "spark.driver.memoryOverhead=1G" \
    --conf "spark.executor.memoryOverhead=1G" \
    --conf "spark.executor.instances=30" \
    --name "P006_unload" \
    <PythonスクリプトPath> "作業ディレクトリPath" "出力ファイルのPrefix" "DB名" "テーブル名" "スクリプト実行年月日" "算出対象年月日"

使用例:
 /opt/mapr/spark/spark-3.3.1/bin/spark-submit\
    --master yarn \
    --deploy-mode client \
    --executor-cores=3 \
    --executor-memory=1G \
    --driver-memory 3G \
    --conf "spark.driver.memoryOverhead=1G" \
    --conf "spark.executor.memoryOverhead=1G" \
    --conf "spark.executor.instances=30" \
    --name "P006_unload" \
    "/home/mapr/script/airflow/P006/P006_unload_spark_gz.py" "/tdh/volume7/unload/dwh_d30_lo_sm_corr" "pgss1" "dwh_d30_lo_sm_corr"  "SM_D30_LO_HC" "20240501" "20240301"
"""

import sys, logging
import uuid
import subprocess
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType, DecimalType, StructField, StructType

# ログレベルの設定
fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
logging.basicConfig(level=logging.INFO, format=fmt)
# ロガーの取得
logger = logging.getLogger(__name__)

logger.info("=== Processing started ===")

# 引数の数を確認
if len(sys.argv) != 7:
    logger.error("Error: The number of arguments is incorrect. Please specify the work directory path, DB name, output file prefix, execution date, and target date.")
    sys.exit(1)

try:
    # SparkSessionを初期化
    logger.info("=== Initialize SparkSession with Hive support ===")
    spark = SparkSession.builder \
        .appName("unload") \
        .enableHiveSupport() \
        .config("spark.sql.parquet.writeLegacyFormat", "true")\
        .config("spark.sql.parquet.compression.codec", "gzip") \
        .getOrCreate()

    # 変数をセット
    logger.info("=== Setting variables ===")
    DF_WORK_DIR=sys.argv[1] # /tdh/volume7/unload/dwh_d30_lo_sm_corr
    DB_NAME=sys.argv[2] # pgss1
    TABLE_NAME=sys.argv[3] # dwh_d30_lo_sm_corr
    CSV_PREFIX=sys.argv[4] # SM_D30_LO_HC
    CURRENT_DATE=sys.argv[5] # 20240501
    TARGETYMD=sys.argv[6] # 20240301

    logger.info("=== Setting paths ===")
    # Pathの設定
    TMP_CSV_PATH = f"/tmp/{CSV_PREFIX}_{CURRENT_DATE}_{TARGETYMD}.csv" # /tmp/SM_D30_LO_HC_20240403_20230502.csv
    DF_ZIP_PATH = f"{DF_WORK_DIR}/{CSV_PREFIX}_{CURRENT_DATE}_{TARGETYMD}.csv.gz" # /tdh/volume7/unload/dwh_d30_lo_sm_corr/SM_D30_LO_HC_20240403_20230502.csv.gz
    RANDOM_STR=uuid.uuid1() # ランダムな文字列を生成してユニークな作業ディレクトリを作成
    DF_QUERY_OUTPUT_DIR = f"{DF_WORK_DIR}/unload_{RANDOM_STR}" # /tdh/volume7/unload/dwh_d30_lo_sm_corr/unload_6fa459ea-ee8a-11d9-9c12-0011098af96b
    TMP_ZIP_PATH = f"/tmp/{CSV_PREFIX}_{CURRENT_DATE}_{TARGETYMD}_{RANDOM_STR}.csv.gz" # /tmp/SM_D30_LO_HC_20240403_20230502_6fa459ea-ee8a-11d9-9c12-0011098af96b.csv.gz

    # 出力するカラムの指定
    logger.info("=== Defining columns ===")
    cols = [
        "KEIKI_ID",
        "CHOURYUU_KUBUN",
        "SANSHUTSU_TAISH_YMD",
        "HIS_NUMBER",
        "KKT_CD",
        "DENRYRY01_JOTAI_CD",
        "TRKMT01_CD",
        "DENRYRY01_HS_KWH",
        "DENRYRY01_HS_CD",
        "DENRYRY02_JOTAI_CD",
        "TRKMT02_CD",
        "DENRYRY02_HS_KWH",
        "DENRYRY02_HS_CD",
        "DENRYRY03_JOTAI_CD",
        "TRKMT03_CD",
        "DENRYRY03_HS_KWH",
        "DENRYRY03_HS_CD",
        "DENRYRY04_JOTAI_CD",
        "TRKMT04_CD",
        "DENRYRY04_HS_KWH",
        "DENRYRY04_HS_CD",
        "DENRYRY05_JOTAI_CD",
        "TRKMT05_CD",
        "DENRYRY05_HS_KWH",
        "DENRYRY05_HS_CD",
        "DENRYRY06_JOTAI_CD",
        "TRKMT06_CD",
        "DENRYRY06_HS_KWH",
        "DENRYRY06_HS_CD",
        "DENRYRY07_JOTAI_CD",
        "TRKMT07_CD",
        "DENRYRY07_HS_KWH",
        "DENRYRY07_HS_CD",
        "DENRYRY08_JOTAI_CD",
        "TRKMT08_CD",
        "DENRYRY08_HS_KWH",
        "DENRYRY08_HS_CD",
        "DENRYRY09_JOTAI_CD",
        "TRKMT09_CD",
        "DENRYRY09_HS_KWH",
        "DENRYRY09_HS_CD",
        "DENRYRY10_JOTAI_CD",
        "TRKMT10_CD",
        "DENRYRY10_HS_KWH",
        "DENRYRY10_HS_CD",
        "DENRYRY11_JOTAI_CD",
        "TRKMT11_CD",
        "DENRYRY11_HS_KWH",
        "DENRYRY11_HS_CD",
        "DENRYRY12_JOTAI_CD",
        "TRKMT12_CD",
        "DENRYRY12_HS_KWH",
        "DENRYRY12_HS_CD",
        "DENRYRY13_JOTAI_CD",
        "TRKMT13_CD",
        "DENRYRY13_HS_KWH",
        "DENRYRY13_HS_CD",
        "DENRYRY14_JOTAI_CD",
        "TRKMT14_CD",
        "DENRYRY14_HS_KWH",
        "DENRYRY14_HS_CD",
        "DENRYRY15_JOTAI_CD",
        "TRKMT15_CD",
        "DENRYRY15_HS_KWH",
        "DENRYRY15_HS_CD",
        "DENRYRY16_JOTAI_CD",
        "TRKMT16_CD",
        "DENRYRY16_HS_KWH",
        "DENRYRY16_HS_CD",
        "DENRYRY17_JOTAI_CD",
        "TRKMT17_CD",
        "DENRYRY17_HS_KWH",
        "DENRYRY17_HS_CD",
        "DENRYRY18_JOTAI_CD",
        "TRKMT18_CD",
        "DENRYRY18_HS_KWH",
        "DENRYRY18_HS_CD",
        "DENRYRY19_JOTAI_CD",
        "TRKMT19_CD",
        "DENRYRY19_HS_KWH",
        "DENRYRY19_HS_CD",
        "DENRYRY20_JOTAI_CD",
        "TRKMT20_CD",
        "DENRYRY20_HS_KWH",
        "DENRYRY20_HS_CD",
        "DENRYRY21_JOTAI_CD",
        "TRKMT21_CD",
        "DENRYRY21_HS_KWH",
        "DENRYRY21_HS_CD",
        "DENRYRY22_JOTAI_CD",
        "TRKMT22_CD",
        "DENRYRY22_HS_KWH",
        "DENRYRY22_HS_CD",
        "DENRYRY23_JOTAI_CD",
        "TRKMT23_CD",
        "DENRYRY23_HS_KWH",
        "DENRYRY23_HS_CD",
        "DENRYRY24_JOTAI_CD",
        "TRKMT24_CD",
        "DENRYRY24_HS_KWH",
        "DENRYRY24_HS_CD",
        "DENRYRY25_JOTAI_CD",
        "TRKMT25_CD",
        "DENRYRY25_HS_KWH",
        "DENRYRY25_HS_CD",
        "DENRYRY26_JOTAI_CD",
        "TRKMT26_CD",
        "DENRYRY26_HS_KWH",
        "DENRYRY26_HS_CD",
        "DENRYRY27_JOTAI_CD",
        "TRKMT27_CD",
        "DENRYRY27_HS_KWH",
        "DENRYRY27_HS_CD",
        "DENRYRY28_JOTAI_CD",
        "TRKMT28_CD",
        "DENRYRY28_HS_KWH",
        "DENRYRY28_HS_CD",
        "DENRYRY29_JOTAI_CD",
        "TRKMT29_CD",
        "DENRYRY29_HS_KWH",
        "DENRYRY29_HS_CD",
        "DENRYRY30_JOTAI_CD",
        "TRKMT30_CD",
        "DENRYRY30_HS_KWH",
        "DENRYRY30_HS_CD",
        "DENRYRY31_JOTAI_CD",
        "TRKMT31_CD",
        "DENRYRY31_HS_KWH",
        "DENRYRY31_HS_CD",
        "DENRYRY32_JOTAI_CD",
        "TRKMT32_CD",
        "DENRYRY32_HS_KWH",
        "DENRYRY32_HS_CD",
        "DENRYRY33_JOTAI_CD",
        "TRKMT33_CD",
        "DENRYRY33_HS_KWH",
        "DENRYRY33_HS_CD",
        "DENRYRY34_JOTAI_CD",
        "TRKMT34_CD",
        "DENRYRY34_HS_KWH",
        "DENRYRY34_HS_CD",
        "DENRYRY35_JOTAI_CD",
        "TRKMT35_CD",
        "DENRYRY35_HS_KWH",
        "DENRYRY35_HS_CD",
        "DENRYRY36_JOTAI_CD",
        "TRKMT36_CD",
        "DENRYRY36_HS_KWH",
        "DENRYRY36_HS_CD",
        "DENRYRY37_JOTAI_CD",
        "TRKMT37_CD",
        "DENRYRY37_HS_KWH",
        "DENRYRY37_HS_CD",
        "DENRYRY38_JOTAI_CD",
        "TRKMT38_CD",
        "DENRYRY38_HS_KWH",
        "DENRYRY38_HS_CD",
        "DENRYRY39_JOTAI_CD",
        "TRKMT39_CD",
        "DENRYRY39_HS_KWH",
        "DENRYRY39_HS_CD",
        "DENRYRY40_JOTAI_CD",
        "TRKMT40_CD",
        "DENRYRY40_HS_KWH",
        "DENRYRY40_HS_CD",
        "DENRYRY41_JOTAI_CD",
        "TRKMT41_CD",
        "DENRYRY41_HS_KWH",
        "DENRYRY41_HS_CD",
        "DENRYRY42_JOTAI_CD",
        "TRKMT42_CD",
        "DENRYRY42_HS_KWH",
        "DENRYRY42_HS_CD",
        "DENRYRY43_JOTAI_CD",
        "TRKMT43_CD",
        "DENRYRY43_HS_KWH",
        "DENRYRY43_HS_CD",
        "DENRYRY44_JOTAI_CD",
        "TRKMT44_CD",
        "DENRYRY44_HS_KWH",
        "DENRYRY44_HS_CD",
        "DENRYRY45_JOTAI_CD",
        "TRKMT45_CD",
        "DENRYRY45_HS_KWH",
        "DENRYRY45_HS_CD",
        "DENRYRY46_JOTAI_CD",
        "TRKMT46_CD",
        "DENRYRY46_HS_KWH",
        "DENRYRY46_HS_CD",
        "DENRYRY47_JOTAI_CD",
        "TRKMT47_CD",
        "DENRYRY47_HS_KWH",
        "DENRYRY47_HS_CD",
        "DENRYRY48_JOTAI_CD",
        "TRKMT48_CD",
        "DENRYRY48_HS_KWH",
        "DENRYRY48_HS_CD",
        "JIGSH_CD",
        "CTN_ID",
        "KEIKI_SIKBT_NUMBER",
        "KEIKI_JORIT_SU",
        "KEIKI_KYTDR_SONST_01_PS",
        "KEIKI_KYOUTEIDENRY_SONST_01_PS",
        "KEIYK_JOTAI_CD",
        "KANRN_USE_SHUBT_CD",
        "KEIKI_TORIT_MOKUTEKI_CD",
        "KEIKI_NUMBER",
        "SABNKEIRY_KUBUN_CD",
        "TKKYK_CD",
        "IKOUYOU_MSTR_CD",
        "FUKUKYK_KUBUN_CD",
        "KEIKI_SENSK_CD"
    ]

    # データを読み込み、指定した日付のデータを抽出
    logger.info("=== START: Reading data from Hive table ===")
    df = spark.table(f"{DB_NAME}.{TABLE_NAME}")
    df = df.select(cols).filter(df["SANSHUTSU_TAISH_YMD"] == TARGETYMD)
    logger.info("=== END: Data read complete ===")

    # データフレームの行数をカウント
    logger.info("=== START: Counting DataFrame rows ===")
    df_count = df.count()
    logger.info("=== END: Row count complete ===")

    # データフレームをCSVファイルに書き込む
    logger.info("=== START: Writing DataFrame to CSV ===")
    logger.info(f"CSV Path: {DF_QUERY_OUTPUT_DIR}")
    df.write.mode("overwrite").option("header", "False").option("quoteAll", "True").csv(DF_QUERY_OUTPUT_DIR)
    logger.info("=== END: CSV write complete ===")

    # csvファイルのリストを取得
    hdfs_files = subprocess.check_output(["hadoop", "fs", "-ls", f"{DF_QUERY_OUTPUT_DIR}/part-*.csv"]).decode("utf-8").strip().split("\n")
    csv_file_paths = [line.split()[-1] for line in hdfs_files if "part-" in line]

    # TMP_CSV_PATHにファイルが存在するか確認し、存在する場合は削除する
    if os.path.isfile(TMP_CSV_PATH):
        os.remove(TMP_CSV_PATH)

    # 空の TMP_CSV_PATH を作成する
    with open(TMP_CSV_PATH, 'w') as f:
        pass

    # DF_ZIP_PATHにファイルが存在するか確認し、存在する場合は削除する
    if subprocess.run(["hadoop", "fs", "-test", "-e", DF_ZIP_PATH], check=False).returncode == 0:
        subprocess.run(["hadoop", "fs", "-rm", DF_ZIP_PATH], check=True)
        logger.info(f"Success to remove the existing file {DF_ZIP_PATH}.")

    # 一時アウトプットをcsvファイルに追記する
    logger.info("=== START: Merging CSV files ===")
    for file in csv_file_paths:
        if file:  # ファイルパスが空でないことを確認
            command = f"hadoop fs -cat {file} >> {TMP_CSV_PATH}"
            subprocess.run(command, shell=True, check=True)

    logger.info("=== END: The temporary output has been appended to the CSV file. ===")

    # CSVファイルの行数をカウント
    logger.info("=== START: Counting rows in the CSV file ===")
    try:
        result = subprocess.run(["wc", "-l", TMP_CSV_PATH], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
        csv_count = int(result.stdout.split()[0])
        logger.info(f"=== END: Row count of CSV file {TMP_CSV_PATH}: {csv_count} ===")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while counting rows in CSV file {TMP_CSV_PATH}: {e}")
        sys.exit(1)

    # データフレーム数とCSVファイルの行数を比較して数をログに出力する。一致しない場合はエラーを出力する
    logger.info(f"DataFrame row count: {df_count}")
    logger.info(f"CSV file row count: {csv_count}")
    if df_count != csv_count:
        logger.error(f"The number of rows in the DataFrame ({df_count}) and the CSV file ({csv_count}) do not match.")
        sys.exit(1)

    # TMP_CSV_PATHをgzipに圧縮してTMP_ZIP_PATHに保存
    logger.info("=== START: Creating a gzip file ===")
    try:
        with open(TMP_ZIP_PATH, 'wb') as f_out:
            subprocess.run(["pigz", "-c", TMP_CSV_PATH], stdout=f_out, check=True)
            logger.info(f"=== END: Success to create the gzip file {TMP_ZIP_PATH}. ===")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while compressing the file: {e}")

    # TMP_ZIP_PATHをDF_ZIP_PATHに移動
    subprocess.run(["hadoop", "fs", "-moveFromLocal", TMP_ZIP_PATH, DF_ZIP_PATH], check=True)
    logger.info(f"Success to move the zip file {TMP_ZIP_PATH} to {DF_ZIP_PATH}.")

    logger.info(f"Success to create the zip file {DF_ZIP_PATH}.")
    logger.info("=== Processing completed ===")

except Exception as e:
    logger.error(f"An error occurred: {e}")
    sys.exit(1)

finally:
    # 作成したファイルとディレクトリが在ったら削除する
    if os.path.exists(TMP_CSV_PATH):
        os.remove(TMP_CSV_PATH)
        logger.info(f"Success to remove the file {TMP_CSV_PATH}.")
    if os.path.exists(TMP_ZIP_PATH):
        os.remove(TMP_ZIP_PATH)
        logger.info(f"Success to remove the file {TMP_ZIP_PATH}.")
    if subprocess.run(["hadoop", "fs", "-test", "-e", DF_QUERY_OUTPUT_DIR], check=False).returncode == 0:
        subprocess.run(["hadoop", "fs", "-rm", "-r", DF_QUERY_OUTPUT_DIR], check=True)
        logger.info(f"Success to remove the directory {DF_QUERY_OUTPUT_DIR}.")

    spark.stop()
    sys.exit(0)