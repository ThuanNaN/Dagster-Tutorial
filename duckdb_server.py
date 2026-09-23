import duckdb
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

conn = duckdb.connect("/app/data/mydb.duckdb")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            tables = conn.execute("SHOW TABLES").fetchall()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"tables": [t[0] for t in tables]}).encode())
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, format, *args): pass

print("DuckDB HTTP server on 8080")
HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
