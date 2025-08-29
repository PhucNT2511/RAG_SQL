import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# ================== LOAD CONFIG ==================
with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)

conn_str = cfg["mysql_db"]["url"]
engine = create_engine(
    conn_str,
    connect_args={
        "ssl": {"ssl-mode": "REQUIRED"}
    }
)

# ================== LOAD EXCEL ==================
excel_file = "data/dữ liệu bán hàng.xlsx"
sheet_names = ["Nhân viên", "Sản phẩm", "Khách hàng", "Chi nhánh", "KPI", "Dữ liệu bán hàng"]
dfs = {sheet: pd.read_excel(excel_file, sheet_name=sheet) for sheet in sheet_names}

# ================== CHUẨN HÓA DỮ LIỆU ==================
def clean_df_for_sql(df, table_name, engine, primary_keys=None):
    """
    Loại bỏ các dòng gây lỗi trước khi chèn:
    - Xóa các giá trị trùng khóa chính
    - Xóa các dòng null ở khóa chính
    """
    if primary_keys is None:
        return df  # không biết khóa chính thì ko làm gì

    # 1. Xóa dòng null ở khóa chính
    df = df.dropna(subset=primary_keys)

    # 2. Loại bỏ trùng trong df
    df = df.drop_duplicates(subset=primary_keys)

    # 3. Loại bỏ các dòng đã tồn tại trong SQL
    with engine.begin() as conn:
        existing_keys = pd.read_sql(
            f"SELECT {', '.join([f'`{col}`' for col in primary_keys])} FROM `{table_name}`",
            conn
        )

    if not existing_keys.empty:
        for key in primary_keys:
            df = df[~df[key].isin(existing_keys[key])]

    return df

# ================== CHÈN DỮ LIỆU ==================
primary_keys_map = {
    "Nhân viên": ["Mã NV"],
    "Sản phẩm": ["Mã SP"],
    "Khách hàng": ["Mã KH"],
    "Chi nhánh": ["Mã CN"],
    "KPI": ["YearMonth", "Mã CN"],
    "Dữ liệu bán hàng": ["Mã ĐH"]
}

for table, df in dfs.items():
    # Chỉ lấy các cột đã định nghĩa trong SQL
    if table == "Nhân viên":
        cols = ["Mã NV", "Nhân viên bán"]
    elif table == "Sản phẩm":
        cols = ["Mã SP", "Sản phẩm", "Nhóm sản phẩm", "Giá vốn"]
    elif table == "Khách hàng":
        cols = ["Mã KH", "Khách hàng"]
    elif table == "Chi nhánh":
        cols = ["Mã CN", "Tên chi nhánh", "Tỉnh thành phố"]
    elif table == "KPI":
        cols = ["YearMonth", "Mã CN", "KPI"]
    elif table == "Dữ liệu bán hàng":
        cols = ["Ngày hạch toán", "Mã ĐH", "Mã KH", "Mã SP", "Số lượng bán",
                "Đơn giá", "Doanh thu", "Giá vốn hàng hóa", "Mã NV", "Mã CN"]
    df = df[cols]

    # Loại bỏ dòng gây lỗi
    df = clean_df_for_sql(df, table, engine, primary_keys_map.get(table))

    # Chèn vào SQL
    if not df.empty:
        df.to_sql(table, engine, if_exists="append", index=False, method='multi')
    dfs[table] = df  # cập nhật lại df đã làm sạch

# ================== CHUẨN HÓA CỘT ĐẶC BIỆT ==================
# Ngày tháng
dfs["Dữ liệu bán hàng"]["Ngày hạch toán"] = pd.to_datetime(
    dfs["Dữ liệu bán hàng"]["Ngày hạch toán"], errors="coerce"
)

# Chuyển số
dfs["Sản phẩm"]["Giá vốn"] = pd.to_numeric(dfs["Sản phẩm"]["Giá vốn"], errors="coerce")
dfs["Dữ liệu bán hàng"]["Số lượng bán"] = pd.to_numeric(
    dfs["Dữ liệu bán hàng"]["Số lượng bán"], errors="coerce", downcast="integer"
)
dfs["Dữ liệu bán hàng"]["Đơn giá"] = pd.to_numeric(dfs["Dữ liệu bán hàng"]["Đơn giá"], errors="coerce")
dfs["Dữ liệu bán hàng"]["Doanh thu"] = pd.to_numeric(dfs["Dữ liệu bán hàng"]["Doanh thu"], errors="coerce")
dfs["Dữ liệu bán hàng"]["Giá vốn hàng hóa"] = pd.to_numeric(dfs["Dữ liệu bán hàng"]["Giá vốn hàng hóa"], errors="coerce")
dfs["KPI"]["KPI"] = pd.to_numeric(dfs["KPI"]["KPI"], errors="coerce")

# YearMonth -> chuẩn hóa 'YYYY-MM'
df_kpi = dfs["KPI"]
df_kpi["YearMonth"] = df_kpi["YearMonth"].astype(str).str.strip()
df_kpi["YearMonth"] = df_kpi["YearMonth"].apply(
    lambda x: f"{str(x)[:4]}-{str(x)[-2:]}" if len(str(x)) == 6 else str(x)
)
dfs["KPI"] = df_kpi

# ================== XÓA CÁC BẢNG CŨ ==================
with engine.begin() as conn:  # begin() auto commit
    for table in ["Dữ liệu bán hàng", "KPI", "Chi nhánh", "Khách hàng", "Sản phẩm", "Nhân viên"]:
        conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))

# ================== TẠO BẢNG MỚI ==================
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE `Nhân viên` (
        `Mã NV` VARCHAR(50) PRIMARY KEY,
        `Nhân viên bán` VARCHAR(255)
    ) CHARSET=utf8mb4
    """))
    
    conn.execute(text("""
    CREATE TABLE `Sản phẩm` (
        `Mã SP` VARCHAR(50) PRIMARY KEY,
        `Sản phẩm` VARCHAR(255),
        `Nhóm sản phẩm` VARCHAR(255),
        `Giá vốn` DECIMAL(20,2)
    ) CHARSET=utf8mb4
    """))
    
    conn.execute(text("""
    CREATE TABLE `Khách hàng` (
        `Mã KH` VARCHAR(50) PRIMARY KEY,
        `Khách hàng` VARCHAR(255)
    ) CHARSET=utf8mb4
    """))
    
    conn.execute(text("""
    CREATE TABLE `Chi nhánh` (
        `Mã CN` VARCHAR(50) PRIMARY KEY,
        `Tên chi nhánh` VARCHAR(255),
        `Tỉnh thành phố` VARCHAR(255)
    ) CHARSET=utf8mb4
    """))
    
    conn.execute(text("""
    CREATE TABLE `KPI` (
        `YearMonth` VARCHAR(10),
        `Mã CN` VARCHAR(50),
        `KPI` DECIMAL(20,2),
        PRIMARY KEY (`YearMonth`, `Mã CN`),
        FOREIGN KEY (`Mã CN`) REFERENCES `Chi nhánh`(`Mã CN`)
    ) CHARSET=utf8mb4;
    """))

    conn.execute(text("""
    CREATE TABLE `Dữ liệu bán hàng` (
        `Ngày hạch toán` DATE,
        `Mã ĐH` VARCHAR(50) PRIMARY KEY,
        `Mã KH` VARCHAR(50),
        `Mã SP` VARCHAR(50),
        `Số lượng bán` INT,
        `Đơn giá` DECIMAL(20,2),
        `Doanh thu` DECIMAL(20,2),
        `Giá vốn hàng hóa` DECIMAL(20,2),
        `Mã NV` VARCHAR(50),
        `Mã CN` VARCHAR(50),
        FOREIGN KEY (`Mã KH`) REFERENCES `Khách hàng`(`Mã KH`),
        FOREIGN KEY (`Mã SP`) REFERENCES `Sản phẩm`(`Mã SP`),
        FOREIGN KEY (`Mã NV`) REFERENCES `Nhân viên`(`Mã NV`),
        FOREIGN KEY (`Mã CN`) REFERENCES `Chi nhánh`(`Mã CN`)
    ) CHARSET=utf8mb4
    """))

# ================== CHÈN DỮ LIỆU ==================
for table, df in dfs.items():
    # Chỉ lấy các cột đã định nghĩa trong SQL
    if table == "Nhân viên":
        cols = ["Mã NV", "Nhân viên bán"]
    elif table == "Sản phẩm":
        cols = ["Mã SP", "Sản phẩm", "Nhóm sản phẩm", "Giá vốn"]
    elif table == "Khách hàng":
        cols = ["Mã KH", "Khách hàng"]
    elif table == "Chi nhánh":
        cols = ["Mã CN", "Tên chi nhánh", "Tỉnh thành phố"]
    elif table == "KPI":
        cols = ["YearMonth", "Mã CN", "KPI"]
    elif table == "Dữ liệu bán hàng":
        cols = ["Ngày hạch toán", "Mã ĐH", "Mã KH", "Mã SP", "Số lượng bán", "Đơn giá", "Doanh thu", "Giá vốn hàng hóa", "Mã NV", "Mã CN"]
    df = df[cols]
    
    df.to_sql(
        table,
        engine,
        if_exists="append",
        index=False,
        method='multi',
        chunksize=1000,
    )

print("✅ Đã xóa bảng cũ, tạo mới và chèn dữ liệu tiếng Việt thành công.")
