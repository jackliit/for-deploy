from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import os
import json
import requests
from supabase import create_client, Client

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # 初始化 Supabase
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        # 解析 URL 與參數
        parsed_path = urlparse(self.path)
        query_components = parse_qs(parsed_path.query)
        
        # 支援參數: id / 統一編號, name / 單位名稱
        id_param = query_components.get('id', [None])[0] or query_components.get('統一編號', [None])[0]
        name_param = query_components.get('name', [None])[0] or query_components.get('單位名稱', [None])[0]

        # 如果沒有提供參數，回傳 HTML 說明頁面
        if not id_param and not name_param:
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            
            html_content = """
            <!DOCTYPE html>
            <html lang="zh-TW">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>統一編號查詢服務</title>
                <style>
                    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; line-height: 1.6; color: #333; }
                    h1 { border-bottom: 2px solid #eaeaea; padding-bottom: 10px; }
                    code { background: #f4f4f4; padding: 2px 5px; border-radius: 3px; font-family: monospace; }
                    .endpoint { background: #f0f7ff; padding: 15px; border-radius: 8px; border-left: 5px solid #0070f3; margin: 20px 0; }
                    .example { background: #fafafa; padding: 15px; border-radius: 8px; border: 1px solid #eaeaea; }
                    a { color: #0070f3; text-decoration: none; }
                    a:hover { text-decoration: underline; }
                </style>
            </head>
            <body>
                <h1>統一編號查詢 API</h1>
                <p>這是一個公開的統一編號查詢服務。您可以使用統一編號或單位名稱進行查詢。</p>
                <p><strong>查詢順序：</strong> 優先查詢經濟部商業司資料，若無則查詢自建資料庫 (學校、機關等)。</p>
                
                <div class="endpoint">
                    <h3>使用方式</h3>
                    <p>GET <code>/?統一編號={8碼統編}</code></p>
                    <p>GET <code>/?單位名稱={關鍵字}</code></p>
                </div>

                <h3>範例</h3>
                <div class="example">
                    <p><strong>查詢統編：</strong> <a href="/?統一編號=03730043">/?統一編號=03730043</a></p>
                    <p><strong>查詢名稱：</strong> <a href="/?單位名稱=台灣大學">/?單位名稱=台灣大學</a></p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
            return

        # --- 優先查詢政府開放資料 (僅限統編查詢) ---
        found_in_govt = False
        govt_data = []
        
        if id_param:
            try:
                # 經濟部商業司 API
                govt_url = 'https://data.gcis.nat.gov.tw/od/data/api/9D17AE0D-09B5-4732-A8F4-81ADED04B679'
                params = {
                    '$format': 'json',
                    '$filter': f'Business_Accounting_NO eq {id_param}',
                    '$skip': 0,
                    '$top': 1
                }
                # 增加 timeout 避免卡住
                gov_response = requests.get(govt_url, params=params, timeout=5)
                
                if gov_response.status_code == 200:
                    json_data = gov_response.json()
                    if isinstance(json_data, list) and len(json_data) > 0:
                        item = json_data[0]
                        comp_name = item.get('Company_Name') or item.get('Business_Name')
                        if comp_name:
                            govt_data.append({
                                "統一編號": id_param,
                                "單位名稱": comp_name,
                                "資料來源": "經濟部商業司"
                            })
                            found_in_govt = True
            except Exception as e:
                # 若外部 API 失敗，則忽略，繼續查本地 DB
                print(f"Govt API Error: {e}")
                pass

        if found_in_govt:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            result = { "data": govt_data, "error": None }
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())
            return


        # --- 若政府資料查不到，查詢 Supabase ---
        if not url or not key:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Server Configuration Error"}).encode())
            return

        supabase: Client = create_client(url, key)
        
        data = []
        error = None
        
        try:
            query = supabase.table("unified_numbers").select("*")
            
            if id_param:
                query = query.eq("tax_id", id_param)
            elif name_param:
                query = query.ilike("name", f"%{name_param}%")

            response = query.limit(50).execute()
            raw_data = response.data
            
            # 格式轉換
            for item in raw_data:
                data.append({
                    "統一編號": item.get("tax_id"),
                    "單位名稱": item.get("name"),
                    "資料來源": item.get("source")
                })
            
        except Exception as e:
            error = str(e)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        result = {
            "data": data,
            "error": error
        }
        self.wfile.write(json.dumps(result, ensure_ascii=False).encode())
