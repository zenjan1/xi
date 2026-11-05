#!/usr/bin/env python3
"""xi 模型池代理 — 多模型容错切换(密钥外置, 源码不含任何 key)
xi 连本地 :8901/chat/completions, 代理按优先级轮询模型池, 遇 429/失败自动切下一个。
模型池配置(含密钥)从 .xi/pool.local.json 读取(该文件被 gitignore, 不进仓库)。
"""
import json, os, time, urllib.request, urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POOL_FILE = os.path.join(REPO, ".xi", "pool.local.json")
LOG = os.path.join(REPO, ".xi", "model-proxy.log")
PORT = 8901

# 从本地配置加载模型池(含密钥, gitignored)。格式:
# {"pool": [{"name":..., "base":..., "model":..., "key":...}, ...]}
def load_pool():
    if not os.path.exists(POOL_FILE):
        raise SystemExit(f"缺少模型池配置: {POOL_FILE}")
    with open(POOL_FILE) as f:
        cfg = json.load(f)
    pool = cfg.get("pool", [])
    if not pool:
        raise SystemExit("pool.local.json 中 pool 为空")
    return pool

POOL = load_pool()
cooldown_until = {}

def log(msg):
    try:
        with open(LOG, "a") as f:
            f.write(f"[{time.strftime('%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass

def try_provider(p, payload):
    now = time.time()
    if cooldown_until.get(p["name"], 0) > now:
        return None
    body = dict(payload)
    body["model"] = p["model"]
    req = urllib.request.Request(
        f"{p['base']}/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {p['key']}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read())
        if "choices" in data:
            return data, p["name"]
        err = data.get("error", {}).get("message", "")[:80] if isinstance(data.get("error"), dict) else str(data.get("error", ""))[:80]
        cooldown_until[p["name"]] = now + 60
        log(f"  {p['name']}: 无 choices: {err}")
        return None
    except urllib.error.HTTPError as e:
        cooldown_until[p["name"]] = now + (300 if e.code == 429 else 300)
        log(f"  {p['name']}: HTTP {e.code}")
        return None
    except Exception as e:
        cooldown_until[p["name"]] = now + 120
        log(f"  {p['name']}: {str(e)[:60]}")
        return None

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok", "pool": [p["name"] for p in POOL]})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/chat/completions":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except Exception:
            self._send(400, {"error": "bad json"})
            return
        log(f"请求: max_tokens={payload.get('max_tokens')}")
        for p in POOL:
            result = try_provider(p, payload)
            if result:
                data, name = result
                log(f"  命中: {name}")
                self._send(200, data)
                return
        self._send(429, {"error": {"message": "all providers unavailable"}, "choices": []})

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

if __name__ == "__main__":
    log(f"模型池代理启动 :{PORT}, 池={len(POOL)} 个 {[p['name'] for p in POOL]}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
