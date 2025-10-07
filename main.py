import psycopg2
import hashlib
from psycopg2.extras import RealDictCursor
import dotenv
import time
import csv
import subprocess
import datetime
import zipfile
import os
from os import listdir
from os.path import isfile, join, dirname
import zipfile

dotenv.load_dotenv()

search = [
            ("xxx",
             f"""
                SELECT * 
             """)
            ]


def run_query(search, foler_name):
    for row in search:
        sql_name = row[0].__str__()
        sql = row[1].__str__()

        dir_path = os.path.dirname(os.path.realpath(__file__))

        if os.path.isfile(dir_path):
            os.remove(f"{sql_name}.csv")
            print(f"{sql_name}.csv deleted.")
        else:
            print(f"{sql_name}.csv does not exist yet.")

        csv_file = os.path.join(dir_path, foler_name, f"{sql_name}.csv")

        print(f"--- Running query: {sql_name}.sql ---")
        start_time = time.time()
        output_query = (f"""--pyReport
                        COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ';', FORCE_QUOTE *)""")

        start_time_step = time.perf_counter()

        start_timestamp = datetime.datetime.now()

        with psycopg2.connect(
                host=os.environ.get('DB_HOST'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                dbname=os.environ.get('DB_NAME'),
                port=os.environ.get('DB_PORT'),
        ) as conn:
            with conn.cursor() as cur:
                with open(csv_file, "w", encoding="utf-8-sig") as f:
                    cur.copy_expert(output_query, f)

        print(f"--- {(time.time() - start_time)} seconds to run {sql_name}.sql ---\n")
        start_time = time.time()

        with open(csv_file, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)


        # optional: generate md5 hash file and zip file
        # print(f"--- {csv_file} MD5: {file_hash.hexdigest()} ---")
        # open(f"{csv_file}.md5", "w").write(f"{file_hash.hexdigest()} at: {start_timestamp}")
        # print(f"--- {(time.time() - start_time)} seconds to generate MD5 hash for file {csv_file} ---\n")
        #
        # ##compacting csv file
        # zip = zipfile.ZipFile(f"{csv_file}-python.zip", "w", zipfile.ZIP_DEFLATED)
        # zip.write(f"{csv_file}")
        # zip.close()
        #
        # print(f"--- {csv_file}-python.zip MD5: {file_hash.hexdigest()} ---")
        # open(f"{csv_file}-python.zip.md5", "w").write(f"{file_hash.hexdigest()} at: {start_timestamp}")
        # print(f"--- {(time.time() - start_time)} seconds to generate MD5 hash for file {csv_file} ---\n")


def main():
    run_query(search, "output")

if __name__ == "__main__":
    main()