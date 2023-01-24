import argparse

import glob
import os

from propdesk_azure_services.azure_databricks import upload_file

def main(python_script, all_files):

    if not all_files and not python_script:
        print('no script specified to deploy. use -all to deploy all spark_*.py files')
        return


    spark_files = glob.glob("./spark_jobs/spark_*.py")
    if python_script:
        spark_files = [sf for sf in spark_files if python_script in sf]
    
    for spark_file in spark_files:
        uploaded_datapath = upload_file(spark_file)
        print(f"Uploaded {spark_file} to {uploaded_datapath}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-py", "--python_script", type=str, help='python script to upload')
    parser.add_argument("-all", "--all", action='store_true', help='upload all files')
    
    args = parser.parse_args()
    python_script = args.python_script
    all_files = args.all

    main(python_script, all_files)
