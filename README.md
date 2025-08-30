# XÂY DỰNG CHATBOT TRUY VẤN DỮ LIỆU BÁN HÀNG (SQL)
Chatbot đơn giản được xây dựng để truy vấn những câu hỏi liên quan đến dữ liệu bán hàng. Ở đây, sử dụng langchain giúp tự động hóa việc truy vấn SQL và tìm kiếm thông tin trên Internet.

## 1. Cấu hình config/config.yaml
Điền thông tin về API và Hệ quản trị CSDL/Server của bạn.

## 2. Cài đặt thư viện
```
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

## 3. Xây dựng cơ sở dữ liệu SQL từ file excel
```
python scripts/build_sql.py
```

## 4. Chạy mô hình
```
python scripts/rag_gradio.py
```

## 5. Tham khảo mô hình thử nghiệm tôi đã deploy trên Huggingface
[PhucNT2511/SQL_RAG](https://huggingface.co/spaces/PhucNT2511/SQL_RAG)



