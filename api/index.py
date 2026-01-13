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
        skip_govt_param = query_components.get('skip_govt', ['false'])[0].lower() == 'true'

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
                    <h3>單筆查詢</h3>
                    <p>GET <code>/?統一編號={8碼統編}</code></p>
                    <p>GET <code>/?單位名稱={關鍵字}</code></p>
                </div>

                <div class="endpoint">
                    <h3>多筆查詢 (GUI)</h3>
                    <p>輸入多個統一編號 (每行一個)，一次查詢：</p>
                    <textarea id="bulkInput" rows="10" style="width: 100%; padding: 10px; margin-bottom: 10px;" placeholder="03730043&#10;04199019"></textarea>
                    <button onclick="doBulkQuery(false)" style="background: #0070f3; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; margin-right: 10px;">查詢全部</button>
                    <button onclick="doBulkQuery(true)" style="background: #333; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer;">僅查詢資料庫</button>
                    <div id="loading" style="display:none; margin-top: 10px; color: #666;">查詢中...</div>
                    <textarea id="resultArea" rows="10" style="width: 100%; padding: 10px; margin-top: 10px; border: 1px solid #eaeaea; display:none;" readonly></textarea>
                </div>

                <h3>範例連結</h3>
                <div class="example">
                    <p><strong>查詢統編：</strong> <a href="/?統一編號=03730043">/?統一編號=03730043</a></p>
                    <p><strong>查詢名稱：</strong> <a href="/?單位名稱=台灣大學">/?單位名稱=台灣大學</a></p>
                </div>

                <script>
                    async function doBulkQuery(skipGovt) {
                        const input = document.getElementById('bulkInput').value;
                        const ids = input.replace(/[\\n\\r]+/g, ",").split(",").map(x => x.trim()).filter(x => x);
                        
                        if (ids.length === 0) {
                            alert("請輸入至少一個統一編號");
                            return;
                        }

                        document.getElementById('loading').style.display = 'block';
                        document.getElementById('resultArea').style.display = 'none';
                        document.getElementById('resultArea').value = '';

                        try {
                            const res = await fetch('/api', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ ids: ids, skip_govt: skipGovt })
                            });
                            const result = await res.json();
                            const list = result.data || [];
                            // Format: 統一編號 [TAB] 單位名稱 [TAB] 資料來源
                            const text = list.map(item => 
                                (item["統一編號"]||"") + "\\t" + (item["單位名稱"]||"") + "\\t" + (item["資料來源"]||"")
                            ).join("\\n");
                            
                            document.getElementById('resultArea').style.display = 'block';
                            document.getElementById('resultArea').value = text;
                        } catch (e) {
                            alert("查詢發生錯誤: " + e);
                        } finally {
                            document.getElementById('loading').style.display = 'none';
                        }
                    }
                </script>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
            return

        # --- 優先查詢政府開放資料 (僅限統編查詢) ---
        found_in_govt = False
        govt_data = []
        
        if id_param and not skip_govt_param:
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

    def do_POST(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")

        if not url or not key:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Configuration error"}).encode())
            return
            
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        
        try:
            body = json.loads(post_data.decode('utf-8'))
            ids = body.get('ids', [])
            skip_govt = body.get('skip_govt', False)
            if not isinstance(ids, list):
                raise ValueError("Format error: 'ids' must be a list")
            
            ids = [str(x).strip() for x in ids if str(x).strip()]
            
            if not ids:
                 self.send_response(400)
                 self.end_headers()
                 return
            
            final_results = {}
            
            # --- 步驟 1: 查詢政府 API (逐筆查詢) ---
            # 由於此 API 不支援 Business_Accounting_NO 的 OR 查詢，必須逐筆請求
            if not skip_govt:
                for tax_id in ids:
                    try:
                        govt_url = 'https://data.gcis.nat.gov.tw/od/data/api/9D17AE0D-09B5-4732-A8F4-81ADED04B679'
                        params = {
                            '$format': 'json',
                            '$filter': f"Business_Accounting_NO eq {tax_id}",
                            '$skip': 0,
                            '$top': 1
                        }
                        # 逐筆查詢，timeout 不宜過長
                        resp = requests.get(govt_url, params=params, timeout=4)
                        
                        if resp.status_code == 200:
                            j_data = resp.json()
                            if isinstance(j_data, list) and len(j_data) > 0:
                                item = j_data[0]
                                comp_name = item.get('Company_Name') or item.get('Business_Name')
                                
                                # 確保有拿到名稱
                                if comp_name:
                                    final_results[tax_id] = {
                                        "統一編號": tax_id,
                                        "單位名稱": comp_name,
                                        "資料來源": "經濟部商業司"
                                    }
                    except Exception as e:
                        print(f"Govt API Error ({tax_id}): {e}")
                        pass
            
            # --- 步驟 2: 查詢 Supabase (一次性優化) ---
            missing_ids = [x for x in ids if x not in final_results]
            
            if missing_ids:
                supabase_client: Client = create_client(url, key)
                response = supabase_client.table("unified_numbers").select("*").in_("tax_id", missing_ids).execute()
                
                for item in response.data:
                    t_id = item.get("tax_id")
                    final_results[t_id] = {
                        "統一編號": t_id,
                        "單位名稱": item.get("name"),
                        "資料來源": item.get("source")
                    }
            
            output_list = []
            for q_id in ids:
                if q_id in final_results:
                    output_list.append(final_results[q_id])
                else:
                    output_list.append({
                        "統一編號": q_id,
                        "單位名稱": None,
                        "資料來源": "查無資料"
                    })
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            result = {
                "data": output_list,
                "count": len(output_list),
                "error": None
            }
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
