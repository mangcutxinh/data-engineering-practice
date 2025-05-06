# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import zipfile
import os
import shutil


def giai_nen_zip(zip_path, extract_path):
    """Giải nén tệp ZIP"""
    try:
        os.makedirs(extract_path, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        for file in os.listdir(extract_path):
            if file.endswith('.csv'):
                return os.path.join(extract_path, file)
        raise ValueError("Không tìm thấy tệp CSV trong ZIP")
    except Exception as e:
        print(f"Lỗi khi giải nén ZIP: {str(e)}")
        raise


def xu_ly_du_lieu_o_cung(spark, csv_path):
    """Xử lý tệp CSV"""
    try:
        # Đọc tệp CSV
        df = spark.read.option("header", "true") \
                       .option("encoding", "UTF-8") \
                       .option("inferSchema", "true") \
                       .csv(csv_path)
        
        # Kiểm tra cột cần thiết
        required_columns = ["date", "serial_number", "model", "capacity_bytes"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Thiếu các cột: {missing_columns}")

        # In schema để kiểm tra
        print("Schema của DataFrame:")
        df.printSchema()

        # Ví dụ xử lý dữ liệu
        df = df.withColumn("brand", F.split(F.col("model"), " ")[0])  # Trích xuất thương hiệu
        return df
    except Exception as e:
        print(f"Lỗi khi xử lý dữ liệu: {str(e)}")
        raise


def main():
    spark = SparkSession.builder.appName("Bài tập 7").enableHiveSupport().getOrCreate()

    zip_path = "/app/data/hard-drive-2022-01-01-failures.csv.zip"
    extract_path = "/app/data/extracted"

    # Xóa thư mục giải nén cũ nếu tồn tại
    if os.path.exists(extract_path):
        shutil.rmtree(extract_path)

    # Giải nén ZIP
    csv_path = giai_nen_zip(zip_path, extract_path)

    # Xử lý dữ liệu
    ket_qua_df = xu_ly_du_lieu_o_cung(spark, csv_path)

    # Hiển thị kết quả
    ket_qua_df.show(5, truncate=False)


if __name__ == "__main__":
    main()