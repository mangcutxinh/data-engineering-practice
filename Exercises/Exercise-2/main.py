import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL của trang web chứa danh sách tệp
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
LAST_MODIFIED = "2024-01-19 10:27"
DOWNLOAD_DIR = "downloads"

# Tạo thư mục lưu tệp nếu chưa tồn tại
def create_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Web scrape để tìm tệp dựa trên thời gian "Last Modified"
def find_file():
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Tìm tất cả các hàng trong bảng (nếu có)
    rows = soup.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            modified_time = cols[1].text.strip()
            if modified_time == LAST_MODIFIED:
                return cols[0].find("a")["href"]  # Lấy tên tệp từ cột đầu tiên
    return None

# Tải tệp từ URL
def download_file(filename):
    file_url = f"{BASE_URL}{filename}"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    response = requests.get(file_url, stream=True)
    response.raise_for_status()
    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Đã tải tệp: {filepath}")
    return filepath

# Phân tích dữ liệu với Pandas
def analyze_file(filepath):
    try:
        # Đọc tệp CSV bằng Pandas
        df = pd.read_csv(filepath)

        # Tìm giá trị cao nhất của HourlyDryBulbTemperature
        max_temp = df["HourlyDryBulbTemperature"].max()
        highest_records = df[df["HourlyDryBulbTemperature"] == max_temp]

        print("Các bản ghi có HourlyDryBulbTemperature cao nhất:")
        print(highest_records)
    except Exception as e:
        print(f"Lỗi khi phân tích tệp: {e}")

def main():
    create_download_dir()
    
    print("Đang tìm tệp...")
    filename = find_file()
    if not filename:
        print("Không tìm thấy tệp phù hợp.")
        return

    print(f"Đã tìm thấy tệp: {filename}")
    filepath = download_file(filename)

    print("Đang phân tích tệp...")
    analyze_file(filepath)

if __name__ == "__main__":
    main()