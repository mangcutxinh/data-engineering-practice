import gzip
import io
import requests

def extract_gz_file(gz_content):
    """
    Giải nén file gzip và xử lý mã hóa
    
    Args:
        gz_content (bytes): Nội dung file gzip
        
    Returns:
        str: Nội dung đã giải nén
    """
    print("Extracting gzip file")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(gz_content), mode='rb') as f:
            # Đọc nội dung dưới dạng bytes
            raw_content = f.read()

            # Thử nhiều encoding khác nhau
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    decoded_content = raw_content.decode(encoding)
                    print(f"Successfully decoded with {encoding} encoding")
                    return decoded_content
                except UnicodeDecodeError:
                    continue
            
            # Nếu không encoding nào hoạt động, sử dụng replace mode với utf-8
            print("Could not decode with common encodings, using utf-8 with replace mode")
            return raw_content.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"Error extracting file: {e}")
        raise

def download_file(url):
    """
    Tải file từ URL
    
    Args:
        url (str): Đường dẫn URL
        
    Returns:
        bytes: Nội dung file
    """
    print(f"Downloading file from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

def main():
    """
    Main function để chạy ứng dụng
    """
    url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    try:
        # Tải file .gz
        gz_content = download_file(url)
        
        # Giải nén file .gz
        content = extract_gz_file(gz_content)
        
        # In nội dung
        print("Extracted Content (First 500 characters):")
        print(content[:500])  # Hiển thị 500 ký tự đầu tiên
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()