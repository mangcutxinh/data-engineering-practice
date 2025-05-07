Nguyễn Thị Quỳnh Trang - 23676071
Ngô Phước Thiên - 23670311
Phạm Ngọc Toàn - 23672111
**Báo cáo Bài tập 1: Tải và giải nén tệp với Python**
_Mục tiêu_
Thực hành kỹ năng xử lý file trong Python, bao gồm:

Tải dữ liệu từ các đường dẫn HTTP

Tự động tạo thư mục lưu trữ

Giải nén file .zip và trích xuất file .csv

Môi trường thực hiện

Sử dụng Docker để tạo môi trường làm việc nhất quán:

docker build --tag=exercise-1 .

docker-compose up run

_Các bước thực hiện_

Tạo thư mục downloads (nếu chưa tồn tại) bằng Python

Tải lần lượt 10 file .zip từ các URL (được cung cấp trong main.py) bằng thư viện requests

Tách tên file từ URL để lưu đúng tên

Giải nén file .zip và trích xuất .csv

Xóa file .zip sau khi giải nén

_Kết quả_

Tạo được thư mục downloads chứa các file .csv được giải nén đúng định dạng

**Báo cáo Bài tập 2: Web Scraping và Tải File Tự Động**
_Mục tiêu_
Mở rộng kỹ năng Python bằng cách kết hợp web scraping, tải file và phân tích dữ liệu với Pandas.

Tự động tìm kiếm tệp khí hậu tương ứng với thời điểm xác định trên một trang HTML

Tải file đó về

Phân tích dữ liệu khí hậu để tìm giá trị nhiệt độ cao nhất trong ngày

Môi trường thực hiện

Làm việc bên trong thư mục Exercise-2

Dùng Docker để build và chạy:

docker build --tag=exercise-2 .

docker-compose up run

_Các bước thực hiện_

Web Scraping trang:
https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/
→ Tìm file có thời điểm Last Modified = 2024-01-19 10:27

Xác định tên file tương ứng với thời điểm đó

Tạo URL hoàn chỉnh, tải file về và lưu cục bộ bằng Python

Dùng Pandas mở file CSV đã tải, tìm bản ghi có HourlyDryBulbTemperature cao nhất

In kết quả ra màn hình

_Kết quả kỳ vọng_

Tự động tải đúng file khí hậu dựa vào thời gian

Trích xuất và hiển thị đúng bản ghi nhiệt độ cao nhất trong file

**Báo cáo Bài tập 3 (Cập nhật): Tải và xử lý dữ liệu Common Crawl bằng Requests + Python**

Thay vì truy xuất dữ liệu từ AWS S3 bằng boto3, ta sẽ dùng requests để tải file từ các đường dẫn HTTP công khai của Common Crawl. Quy trình vẫn bao gồm:

Tải file .gz

Đọc nội dung dòng đầu tiên để lấy URL của file .wet

Tải và in nội dung của file .wet

Các bước thực hiện (dùng requests)

Tải file .gz từ:
https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz
→ dùng requests.get(..., stream=True) để tránh load toàn bộ vào bộ nhớ

Giải nén nội dung .gz trực tiếp trong bộ nhớ bằng gzip + io.BytesIO

Lấy dòng đầu tiên, ví dụ: crawl-data/CC-MAIN-2022-05/segments/.../wet/...
→ Ghép thành URL đầy đủ:
https://data.commoncrawl.org/{dòng đầu tiên}

Tải tiếp file .wet từ URL mới và stream từng dòng ra stdout

Ưu điểm của cách dùng requests

Đơn giản, không cần cấu hình AWS hoặc quyền truy cập

Phù hợp với các bài tập học thuật hoặc hệ thống không có boto3

_Kết quả kỳ vọng_

In nội dung của file .wet (dạng văn bản thuần của trang web)

Hoàn thành toàn bộ thao tác mà không cần tải file về đĩa

**Báo cáo Bài tập 4: Duyệt thư mục và chuyển đổi dữ liệu JSON sang CSV**
Mục tiêu
Thực hành thao tác với hệ thống file và xử lý dữ liệu dạng JSON, cụ thể:

Dò tìm tất cả các tệp .json trong một cây thư mục không đồng đều

Đọc và phân tích nội dung các file JSON

Làm phẳng cấu trúc dữ liệu lồng nhau

Xuất dữ liệu sang file CSV tương ứng

Môi trường thực hiện

Làm việc trong thư mục Exercise-4

Dùng Docker để thiết lập môi trường:

docker build --tag=exercise-4 .

docker-compose up run

_Các bước thực hiện_

Dò toàn bộ thư mục data/ bằng thư viện glob để tìm file .json (bao gồm cả trong thư mục con)

Mở từng file JSON bằng thư viện json, đọc dữ liệu

Làm phẳng cấu trúc dữ liệu nếu có dạng lồng nhau (ví dụ: trường "coordinates": [-99.9, 16.88333] → tách thành 2 cột)

Ghi ra file .csv tương ứng với mỗi file JSON đã đọc, bao gồm tiêu đề cột

_Kết quả _

Mỗi file JSON được chuyển thành một file CSV trong cùng thư mục (hoặc thư mục đích mới)

Cấu trúc CSV đầy đủ, đúng thứ tự và tiêu đề rõ ràng

Script hoạt động tốt với số lượng file lớn và cây thư mục sâu

**Báo cáo Bài tập 5: Thiết kế bảng và nạp dữ liệu vào PostgreSQL bằng Python**
Mục tiêu
Thực hành 3 kỹ năng quan trọng trong Data Engineering:

Thiết kế mô hình dữ liệu (data modeling)

Kết nối và thao tác với PostgreSQL

Nạp dữ liệu từ CSV vào cơ sở dữ liệu

Môi trường thực hiện

Làm việc trong thư mục Exercise-5

Dùng Docker để chạy môi trường PostgreSQL:

docker build --tag=exercise-5 .

docker-compose up run

Các bước thực hiện

Phân tích nội dung 3 file CSV trong thư mục data/ để xác định các trường, kiểu dữ liệu, và mối quan hệ

Viết câu lệnh CREATE TABLE cho mỗi bảng, bao gồm:

Kiểu dữ liệu phù hợp (INTEGER, VARCHAR, TIMESTAMP, v.v.)

Khóa chính (Primary Key) cho mỗi bảng

Khóa ngoại (Foreign Key) nếu có mối liên hệ giữa các bảng

Chỉ mục (INDEX) để tối ưu truy vấn

Dùng thư viện psycopg2 trong Python để:

Kết nối đến PostgreSQL trên localhost

Chạy script CREATE TABLE để tạo bảng

Đọc dữ liệu từ file CSV và nạp vào bảng bằng psycopg2

Có thể dùng COPY hoặc executemany() tùy khối lượng dữ liệu

_Kết quả_

3 bảng được tạo trong PostgreSQL, cấu trúc chuẩn hóa, có khóa và chỉ mục

Dữ liệu từ CSV được nạp đầy đủ vào bảng

Có thể kiểm tra bằng truy vấn SQL đơn giản trong PostgreSQL

****Báo cáo Bài tập 6: Ingestion và Aggregation với PySpark****
Mục tiêu
Làm quen với xử lý dữ liệu lớn bằng Apache Spark thông qua thư viện PySpark của Python.
Nhiệm vụ gồm:

Đọc dữ liệu từ các file .zip chứa .csv

Thực hiện các phép tổng hợp dữ liệu

Ghi kết quả ra các báo cáo CSV

Môi trường thực hiện

Làm việc trong thư mục Exercise-6

Dùng Docker để chạy Spark:

docker build --tag=exercise-6 .

docker-compose up run

Dữ liệu

2 file .zip chứa dữ liệu chuyến đi bằng xe đạp

Dữ liệu có các cột như: trip_id, start_time, end_time, tripduration, from_station_name, gender, birthyear,...

Yêu cầu chính

Tính trung bình thời lượng chuyến đi theo ngày

Đếm số chuyến đi mỗi ngày

Tìm trạm xuất phát phổ biến nhất mỗi tháng

Xác định top 3 trạm mỗi ngày trong 2 tuần cuối cùng

So sánh trung bình thời lượng chuyến đi giữa nam và nữ

Tìm top 10 độ tuổi có chuyến đi dài nhất và ngắn nhất

Kết quả mỗi câu cần được ghi ra file .csv trong thư mục reports/

**Bài tập 7: Các bước kiểm tra mã PySpark trong GitHub**
Kiểm tra tệp main.py:

Đảm bảo rằng tệp này chứa mã PySpark thực hiện các yêu cầu sau:

Đọc dữ liệu từ tệp .csv.zip.

Thêm cột source_file vào DataFrame.

Trích xuất ngày từ tên tệp và lưu vào cột file_date.

Thêm cột brand dựa trên giá trị của cột model.

Thêm cột storage_ranking dựa trên dung lượng capacity_bytes.

Tạo một cột primary_key bằng cách sử dụng hàm hash từ các cột đặc trưng của bản ghi.

Kiểm tra việc sử dụng các hàm PySpark:

Đảm bảo rằng bạn chỉ sử dụng các hàm có sẵn trong pyspark.sql.functions, không sử dụng UDFs hoặc các phương thức Python thuần túy.

Một số hàm hữu ích có thể bao gồm:

input_file_name(): Lấy tên tệp hiện tại trong quá trình xử lý.

to_date() hoặc to_timestamp(): Chuyển đổi chuỗi thành kiểu dữ liệu ngày hoặc thời gian.

split(): Tách chuỗi thành mảng dựa trên dấu phân cách.

when(), otherwise(): Điều kiện hóa trong biểu thức.

rank(), dense_rank(): Xếp hạng các giá trị trong cột.

hash(): Tạo giá trị băm từ các cột.

Kiểm tra việc xử lý dữ liệu:

Đảm bảo rằng mã của bạn thực hiện đúng các bước xử lý dữ liệu như:

Đọc dữ liệu từ tệp nén.

Thêm và tính toán các cột mới.

Xử lý các giá trị thiếu hoặc không hợp lệ.

Lưu kết quả vào tệp đầu ra hoặc cơ sở dữ liệu.

Kiểm tra các tệp liên quan:

Đảm bảo rằng tất cả các tệp cần thiết, bao gồm main.py, tệp dữ liệu .csv.zip, và các tệp cấu hình Docker (nếu có), đã được đẩy lên GitHub.

Kiểm tra lại repo trên GitHub để đảm bảo các tệp này có sẵn và được cập nhật đúng cách.

Kiểm tra việc chạy mã:

Nếu bạn đã thiết lập Docker, hãy chạy mã của bạn trong môi trường Docker để kiểm tra tính đúng đắn.

Sử dụng các lệnh như docker build và docker-compose up để xây dựng và chạy môi trường.

Kiểm tra kết quả đầu ra:

Sau khi chạy mã, kiểm tra kết quả đầu ra để đảm bảo rằng các cột như source_file, file_date, brand, storage_ranking, và primary_key đã được thêm vào chính xác.

Sử dụng các phương thức như df.show() hoặc df.printSchema() để hiển thị kết quả.

