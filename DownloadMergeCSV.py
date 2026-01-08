import pandas as pd
import requests
import io
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_and_extract(url, source_name):
    print(f"正在處理: {source_name}")
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        
        try:
            content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            content = response.content.decode('cp950')
            
        # 使用 dtype=str 強制保留 0
        df = pd.read_csv(io.StringIO(content), dtype=str)
        df.columns = [c.strip() for c in df.columns]
        
        name_col = None
        possible_names = ['單位名稱', '機關單位名稱', '機關名稱']
        for col in df.columns:
            if col in possible_names:
                name_col = col
                break
        
        if '統一編號' in df.columns and name_col:
            sub_df = df[['統一編號', name_col]].copy()
            sub_df.columns = ['統一編號', '單位名稱']
            sub_df['統一編號'] = sub_df['統一編號'].str.strip()
            sub_df['單位名稱'] = sub_df['單位名稱'].str.strip()
            # 加入來源標記，方便辨識重複是從哪裡來的
            sub_df['來源'] = source_name 
            return sub_df
        else:
            return None
    except Exception as e:
        print(f"  -> 發生錯誤: {e}")
        return None

# 定義網址與名稱
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
    
    # --- 步驟 1: 找出並印出重複 ---
    # keep=False 表示標記所有重複項
    duplicates = final_df[final_df.duplicated(subset=['統一編號'], keep=False)]
    
    if not duplicates.empty:
        print(f"\n發現 {len(duplicates)} 筆重複資料 (相同統一編號)：")
        # 排序以便查看
        duplicates_sorted = duplicates.sort_values(by='統一編號')
        
        # 印出前 20 筆範例
        print(duplicates_sorted[['統一編號', '單位名稱', '來源']].head(20).to_string(index=False))
        
        # 也可以選擇將重複清單存檔
        duplicates_sorted.to_csv("duplicate_report.csv", index=False, encoding='utf-8-sig')
        print("... (完整重複清單已儲存為 duplicate_report.csv)")
    else:
        print("\n太棒了，沒有發現重複資料。")

    # --- 步驟 2: 去除重複並存檔 ---
    # keep='first' 表示保留第一筆，刪除後面的
    final_df_unique = final_df.drop_duplicates(subset=['統一編號'], keep='first')
    
    # 移除暫用的'來源'欄位，只留客戶要的
    output_df = final_df_unique[['統一編號', '單位名稱']]
    
    output_df.to_excel("final_unified_ids_unique.xlsx", index=False)
    output_df.to_csv("final_unified_ids_unique.csv", index=False, encoding='utf-8-sig')
    
    print(f"\n最終檔案處理完成！共 {len(output_df)} 筆唯一資料。")
    print("已儲存為 final_unified_ids_unique.xlsx (建議使用) 及 .csv")
