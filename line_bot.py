"""
LINE Messaging API 連携モジュール
"""
import os, requests
from dotenv import load_dotenv

load_dotenv()

CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN', '')
LINE_USER_ID         = os.getenv('LINE_USER_ID', '')
CHANNEL_SECRET       = os.getenv('LINE_CHANNEL_SECRET', '')

PUSH_URL  = 'https://api.line.me/v2/bot/message/push'
REPLY_URL = 'https://api.line.me/v2/bot/message/reply'

def _headers():
    return {'Content-Type': 'application/json', 'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}

def is_configured() -> bool:
    return bool(CHANNEL_ACCESS_TOKEN and LINE_USER_ID)

def push_text(text: str) -> bool:
    if not is_configured():
        print('[LINE] 未設定のためスキップ')
        return False
    res = requests.post(PUSH_URL, headers=_headers(),
                        json={'to': LINE_USER_ID, 'messages': [{'type': 'text', 'text': text}]}, timeout=10)
    if res.status_code != 200:
        print(f'[LINE] push失敗: {res.status_code} {res.text}')
        return False
    return True

def reply_text(reply_token: str, text: str) -> bool:
    if not CHANNEL_ACCESS_TOKEN: return False
    res = requests.post(REPLY_URL, headers=_headers(),
                        json={'replyToken': reply_token, 'messages': [{'type': 'text', 'text': text}]}, timeout=10)
    return res.status_code == 200

HELP_TEXT = (
    '💬 使えるコマンド\n'
    '━━━━━━━━━━━━━━\n'
    '今日   → 今日の支出明細\n'
    '状況   → 今月のサマリー\n'
    '予算   → 予算残高カテゴリ別\n'
    '先月   → 先月のサマリー\n'
    '同期   → MoneyForward自動取得\n'
    'URL    → ダッシュボードURL\n'
    'ヘルプ → このリスト'
)

def push_server_started(url: str):
    msg = (
        f'🚀 MFダッシュボード 起動\n'
        f'━━━━━━━━━━━━━━\n'
        f'📱 URL: {url}\n'
        f'━━━━━━━━━━━━━━\n'
        f'{HELP_TEXT}'
    )
    push_text(msg)

SYNC_KEYWORDS      = {'同期', 'sync', 'mf', 'moneyforward', 'マネーフォワード', '更新', 'データ取得', '取得'}
STATUS_KEYWORDS    = {'状況', 'status', 'サマリー', '今月'}
URL_KEYWORDS       = {'url', 'URL', 'アドレス', 'リンク'}
TODAY_KEYWORDS     = {'今日', 'きょう', 'today', '今日の支出'}
BUDGET_KEYWORDS    = {'予算', '残り', '残高', '残', 'budget'}
LASTMONTH_KEYWORDS = {'先月', 'lastmonth', '先月分'}
HELP_KEYWORDS      = {'ヘルプ', 'help', 'コマンド', 'メニュー', '使い方'}

def parse_command(text: str) -> str | None:
    t  = text.strip()
    tl = t.lower()
    if any(k in t  for k in SYNC_KEYWORDS):      return 'sync'
    if any(k in t  for k in TODAY_KEYWORDS):      return 'today'
    if any(k in t  for k in STATUS_KEYWORDS):     return 'status'
    if any(k in t  for k in BUDGET_KEYWORDS):     return 'budget'
    if any(k in t  for k in LASTMONTH_KEYWORDS):  return 'lastmonth'
    if any(k in tl for k in URL_KEYWORDS):        return 'url'
    if any(k in t  for k in HELP_KEYWORDS):       return 'help'
    return None

def verify_signature(body: bytes, signature: str) -> bool:
    import hmac, hashlib, base64
    if not CHANNEL_SECRET: return True
    digest   = hmac.new(CHANNEL_SECRET.encode('utf-8'), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(expected, signature)
