# SHOPEE CRAWLER

## Cài đặt:
- PYTHON 3.8 
```commandline
pip install -r requirements.txt
```


## Chạy chương trình
- Test 1: Crawl tất cả các sản phẩm thuộc các nhóm hàng lớn liệt kê ở trang chủ shopee:
```commandline
python3 test1.py
```
- Test 2: Transform dữ liệu đã lấy được ra định dạng csv
```commandline
python3 test2.py
```


## Kết quả:
- Test 1:
    + Số lượng sản phẩm lấy được: 145939, cho tổng số 27 loại mặt hàng lớn
    + Số lượng sản phẩm lấy được trong một phút xấp xỉ: 71152 sản phẩm
- Test 2:
    + Số lượng sản phẩm transform trong một phút xấp xỉ: 1010660 sản phẩm
- Kết quả chạy được lưu cụ thể ở trong folder result
- Về phần dữ liệu, do lượng dữ liệu lớn nên các file csv, xlsx được lưu ở [link này](https://drive.google.com/drive/folders/1vSlhMG31RkfUZj3uwThuXbT8bE6Xq8Gr?usp=sharing)
