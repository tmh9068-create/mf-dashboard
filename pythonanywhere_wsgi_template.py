# ============================================================
# PythonAnywhere WSGI 設定ファイル
# このファイルの内容を PythonAnywhere の WSGI ファイルに貼り付ける
# パス例: /var/www/ユーザー名_pythonanywhere_com_wsgi.py
# ============================================================
import sys
import os

# ── アプリのパスを通す ────────────────────────────
# 「ユーザー名」を実際の PythonAnywhere ユーザー名に変更してください
USERNAME = 'ユーザー名'

APP_PATH  = f'/home/{USERNAME}/mf-dashboard'
DATA_PATH = f'/home/{USERNAME}/mf-data'

if APP_PATH not in sys.path:
    sys.path.insert(0, APP_PATH)

# ── データ保存先（SQLite など） ───────────────────
os.makedirs(DATA_PATH, exist_ok=True)
os.environ['DATA_DIR'] = DATA_PATH

# ── 環境変数（.env の代わり） ─────────────────────
os.environ['MF_EMAIL']    = 'あなたのMFメールアドレス'
os.environ['MF_PASSWORD'] = 'あなたのMFパスワード'

# LINE を使う場合は以下も設定
# os.environ['LINE_CHANNEL_ACCESS_TOKEN'] = '...'
# os.environ['LINE_CHANNEL_SECRET']       = '...'
# os.environ['LINE_USER_ID']              = '...'

# ── Flask アプリを読み込む ────────────────────────
from app import app as application  # noqa: E402
