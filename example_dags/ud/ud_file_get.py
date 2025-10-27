import sys
sys.path.append("/usr/local/airflow/dags/gitdags/dags")

from common.log import MyAppLog

def main():
    # Call the imported function
    MyAppLog()
    print("Main program execution completed")

if __name__ == "__main__":
    main()
