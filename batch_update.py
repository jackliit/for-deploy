import os
import io
import pandas as pd
import requests
import urllib3
from supabase import create_client, Client

# 忽略不安全的 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Supabase 設定
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("請確認環境變數 SUPABASE_URL 與 SUPABASE_KEY 是否已設定")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
# 資料表名稱
TABLE_NAME = "unified_numbers"

def fetch_and_extract(url, source_name):
    print(f"正在處理: {source_name}")
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        
        try:
            content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            content = response.content.decode('cp950')
            
        df = pd.read_csv(io.StringIO(content), dtype=str)
        df.columns = [c.strip() for c in df.columns]
        
        name_col = None
        possible_names = ['單位名稱', '機關單位名稱', '機關名稱']
        for col in df.columns:
            if col in possible_names:
                name_col = col
                break
        
        if '統一編號' in df.columns and name_col:
            # 選取需要的欄位
            sub_df = df[['統一編號', name_col]].copy()
            # 重新命名為資料庫欄位: tax_id (統一編號), name (單位名稱)
            sub_df.columns = ['tax_id', 'name'] 
            sub_df['tax_id'] = sub_df['tax_id'].str.strip()
            sub_df['name'] = sub_df['name'].str.strip()
            # 加入 source (資料來源)
            sub_df['source'] = source_name 
            return sub_df
        else:
            return None
    except Exception as e:
        print(f"  -> 發生錯誤: {e}")
        return None

def main():
    sources = [
        ("https://eip.fia.gov.tw/data/BGMOPEN99X.csv", "全國各級學校"),
        ("https://www.fia.gov.tw/download/9bc4de1485014443b518beb37d8f35fe", "行政院所屬機關"),
        ("https://www.fia.gov.tw/download/2d35e0525c484964a84798baf39c72d2", "地方政府機關"),
        ("https://eip.fia.gov.tw/data/BGMOPEN99.csv", "非營利事業")
    ]
    
    all_dfs = []
    for url, name in sources:
        df = fetch_and_extract(url, name)
        if df is not None:
            all_dfs.append(df)
            
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # 以 'tax_id' 去重
        final_df.drop_duplicates(subset=['tax_id'], keep='first', inplace=True)
        
        print(f"準備上傳 {len(final_df)} 筆資料到 Supabase (Table: {TABLE_NAME})...")
        
        records = final_df.to_dict(orient='records')
        
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                # Upsert on tax_id
                supabase.table(TABLE_NAME).upsert(batch, on_conflict='tax_id').execute()
                print(f"  已處理批次 {i} - {i + len(batch)}")
            except Exception as e:
                print(f"  批次 {i} 上傳失敗: {e}")
                
        print("所有資料已更新完成。")
    else:
        print("未獲取到任何資料。")

if __name__ == "__main__":
    main()
