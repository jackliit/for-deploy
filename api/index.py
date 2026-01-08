from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import os
import json
from supabase import create_client, Client

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # 初始化 Supabase
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Configuration error"}).encode())
            return

        supabase: Client = create_client(url, key)
        
        # 解析參數
        query_components = parse_qs(urlparse(self.path).query)
        # 支援用 中文 或 英文 參數查詢
        id_param = query_components.get('id', [None])[0] or query_components.get('統一編號', [None])[0]
        name_param = query_components.get('name', [None])[0] or query_components.get('單位名稱', [None])[0]
        
        data = []
        error = None
        
        try:
            # 使用新的欄位名稱 tax_id, name, source
            query = supabase.table("unified_numbers").select("*")
            
            if id_param:
                query = query.eq("tax_id", id_param)
            elif name_param:
                # 使用 ilike 進行模糊搜尋
                query = query.ilike("name", f"%{name_param}%")
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Please provide query parameters"}).encode())
                return

            response = query.limit(50).execute()
            raw_data = response.data
            
            # --- 格式轉換: 將資料庫欄位轉換為中文 Key 回傳 ---
            data = []
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
