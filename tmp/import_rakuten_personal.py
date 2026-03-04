"""楽天カード個人PDFから明細を抽出してNocoDBにインポート"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
import os
import re

# PDFからテキスト抽出用
try:
    import pdfplumber
except ImportError:
    print("pdfplumberが必要です。--with pdfplumber で実行してください")
    sys.exit(1)

PDF_DIR = "C:/Users/ninni/projects/rawdata/楽天カード個人"
DB_PATH = "C:/Users/ninni/nocodb/noco.db"

def parse_rakuten_pdf(pdf_path):
    """楽天カードPDFから利用明細を抽出"""
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            for line in text.split('\n'):
                # パターン: 2025/MM/DD 店名 本人* 1回払い 金額 ...
                m = re.match(r'^(\d{4}/\d{2}/\d{2})\s+(.+?)\s+本人\*?\s+1回払い\s+([\d,]+)\s+', line)
                if m:
                    date_str = m.group(1).replace('/', '-')
                    shop = m.group(2).strip()
                    amount = int(m.group(3).replace(',', ''))
                    records.append((date_str, shop, amount))
    return records

def get_statement_period(filename):
    """ファイル名からYYYYMM部分を取得"""
    m = re.search(r'statement_(\d{6})\.pdf', filename)
    return m.group(1) if m else None

# PDFファイル一覧（2025年1月請求=202501以降）
pdf_files = sorted([
    f for f in os.listdir(PDF_DIR)
    if f.startswith('statement_') and f.endswith('.pdf')
    and get_statement_period(f) and get_statement_period(f) >= '202501'
])

print(f"対象PDF: {len(pdf_files)}ファイル")

all_records = []
for pdf_file in pdf_files:
    period = get_statement_period(pdf_file)
    pdf_path = os.path.join(PDF_DIR, pdf_file)
    records = parse_rakuten_pdf(pdf_path)
    print(f"  {pdf_file}: {len(records)}件")
    for r in records:
        all_records.append((*r, f"PDF_{period}"))

# 2025年以降のみフィルタ（2024年末のデータがPDFに含まれる場合を除外）
records_2025 = [r for r in all_records if r[0] >= '2025-01-01']
print(f"\n2025年以降: {len(records_2025)}件")

# 重複チェック（同じ日付・店名・金額の組み合わせ）
# 既存データとの重複は利用日が2024-12-29以前なので問題ないが念のため
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 既にインポート済みの2025年データがあるか確認
cur.execute("SELECT COUNT(*) FROM nc_mtf3___楽天カード個人明細 WHERE 利用日 >= '2025-01-01'")
existing_2025 = cur.fetchone()[0]
if existing_2025 > 0:
    print(f"\n警告: 既に2025年データが{existing_2025}件存在します。スキップします。")
    conn.close()
    sys.exit(0)

# IDとnc_orderの最大値取得
cur.execute("SELECT MAX(id), MAX(nc_order) FROM nc_mtf3___楽天カード個人明細")
max_id, max_order = cur.fetchone()
max_id = max_id or 0
max_order = max_order or 0

print(f"現在のmax_id={max_id}, max_order={max_order}")
print(f"挿入件数: {len(records_2025)}")

# INSERT
for i, (date, shop, amount, source) in enumerate(records_2025):
    new_id = max_id + 1 + i
    new_order = max_order + 1 + i
    cur.execute("""
        INSERT INTO nc_mtf3___楽天カード個人明細
        (id, created_at, updated_at, nc_order, 利用日, 利用店名, 利用金額, ソース)
        VALUES (?, datetime('now'), datetime('now'), ?, ?, ?, ?, ?)
    """, (new_id, new_order, date, shop, amount, source))

conn.commit()

# 検証
cur.execute("SELECT COUNT(*) FROM nc_mtf3___楽天カード個人明細 WHERE 利用日 >= '2025-01-01'")
inserted = cur.fetchone()[0]
cur.execute("SELECT MIN(利用日), MAX(利用日) FROM nc_mtf3___楽天カード個人明細 WHERE 利用日 >= '2025-01-01'")
min_d, max_d = cur.fetchone()
cur.execute("SELECT COUNT(*) FROM nc_mtf3___楽天カード個人明細")
total = cur.fetchone()[0]

print(f"\n=== 結果 ===")
print(f"挿入: {inserted}件 ({min_d} ~ {max_d})")
print(f"テーブル合計: {total}件")

conn.close()
