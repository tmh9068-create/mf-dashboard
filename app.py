from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
import sqlite3, os, calendar
from datetime import datetime, date
import pandas as pd

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mf-dashboard-2026'
socketio = SocketIO(app, cors_allowed_origins='*')

_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR  = os.getenv('DATA_DIR', '')
if _DATA_DIR:
    os.makedirs(_DATA_DIR, exist_ok=True)
    _persistent_db = os.path.join(_DATA_DIR, 'mf.db')
    _bundle_db     = os.path.join(_BASE_DIR, 'mf.db')
    if not os.path.exists(_persistent_db) and os.path.exists(_bundle_db):
        import shutil as _shutil
        _shutil.copy2(_bundle_db, _persistent_db)
    DB_PATH = _persistent_db
else:
    DB_PATH = os.path.join(_BASE_DIR, 'mf.db')

UPLOAD_DIR = os.path.join(_BASE_DIR, 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

# MoneyForward CSV 列マッピング
MF_COL_MAP = {
    '日付':       'date',
    '内容':       'memo',
    '金額（円）': 'amount',
    '大項目':     'category',
    '中項目':     'subcategory',
    'メモ':       'note',
    '振替':       'is_transfer',
    '計算対象':   'is_counted',
}

# データ取得の最古月
EARLIEST_YEAR  = 2023
EARLIEST_MONTH = 9

# 予算管理対象外カテゴリ（集計は行うが予算との比較はしない）
NO_BUDGET_CATS = {'未分類', '現金・カード', 'その他', '自動車', '特別な支出', '保険', '税・社会保障'}


# ─────────────────────────────────────────
# DB
# ─────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def init_db():
    conn = get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK(type IN ('income','expense')),
            category TEXT NOT NULL,
            amount INTEGER NOT NULL,
            memo TEXT DEFAULT '',
            date TEXT NOT NULL,
            source TEXT DEFAULT 'csv',
            created_at TEXT NOT NULL
        )''')
    existing_cols = [row[1] for row in conn.execute('PRAGMA table_info(transactions)').fetchall()]
    if 'source' not in existing_cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN source TEXT DEFAULT 'csv'")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            category TEXT NOT NULL,
            amount INTEGER NOT NULL,
            UNIQUE(year, month, category)
        )''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            name TEXT PRIMARY KEY,
            type TEXT NOT NULL CHECK(type IN ('income','expense')),
            color TEXT DEFAULT '#6c8bff',
            sort_order INTEGER DEFAULT 0
        )''')
    conn.commit()
    conn.close()

def seed_default_categories():
    """MoneyForward 標準カテゴリで初期設定（DBが空の場合のみ）"""
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM categories').fetchone()[0]
    if count == 0:
        defaults = [
            # 支出・予算管理カテゴリ（実データに基づく順序）
            ('食費',         'expense', '#ff5e7d',  1),
            ('日用品',       'expense', '#4ade80',  2),
            ('教養・教育',   'expense', '#818cf8',  3),
            ('衣服・美容',   'expense', '#f472b6',  4),
            ('交通費',       'expense', '#38bdf8',  5),
            ('趣味・娯楽',   'expense', '#fb923c',  6),
            ('水道・光熱費', 'expense', '#ffd166',  7),
            ('通信費',       'expense', '#60a5fa',  8),
            ('健康・医療',   'expense', '#34d399',  9),
            ('交際費',       'expense', '#c084fc', 10),
            # 予算外カテゴリ（集計のみ）
            ('未分類',       'expense', '#64748b', 90),
            ('現金・カード', 'expense', '#475569', 91),
            ('その他',       'expense', '#6b7280', 92),
            ('自動車',       'expense', '#94a3b8', 93),
            ('特別な支出',   'expense', '#dc2626', 94),
            ('保険',         'expense', '#94a3b8', 95),
            ('税・社会保障', 'expense', '#64748b', 96),
            # 収入
            ('収入',         'income',  '#22d87b',  1),
            ('一時所得',     'income',  '#6c8bff',  2),
            ('その他収入',   'income',  '#a3e635',  3),
        ]
        conn.executemany(
            'INSERT OR IGNORE INTO categories (name,type,color,sort_order) VALUES (?,?,?,?)',
            defaults
        )
        conn.commit()
    conn.close()


# ─────────────────────────────────────────
# CSV インポート（MoneyForward 形式）
# ─────────────────────────────────────────
def parse_mf_csv(filepath):
    """MoneyForward ME CSV をパースしてトランザクション行リストを返す"""
    df = None
    for enc in ['utf-8-sig', 'utf-8', 'cp932']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            break
        except Exception:
            continue
    if df is None:
        raise ValueError('MF CSV 読み込み失敗')

    df = df.rename(columns={c: MF_COL_MAP[c] for c in df.columns if c in MF_COL_MAP})

    # 計算対象 == 1 のみ（文字列 '1' または数値 1）
    if 'is_counted' in df.columns:
        df = df[df['is_counted'].astype(str).str.strip() == '1']

    # 振替を除外（is_transfer == 1）
    if 'is_transfer' in df.columns:
        df = df[df['is_transfer'].astype(str).str.strip() != '1']

    # 日付パース（YYYY/MM/DD → date）
    df['date'] = pd.to_datetime(df.get('date', pd.Series(dtype=str)), errors='coerce', format='%Y/%m/%d')
    df = df.dropna(subset=['date'])

    # 2023年9月以降のみ
    cutoff = pd.Timestamp(EARLIEST_YEAR, EARLIEST_MONTH, 1)
    df = df[df['date'] >= cutoff]

    rows = []
    for _, row in df.iterrows():
        raw_amount = pd.to_numeric(row.get('amount', 0), errors='coerce') or 0
        cat  = str(row.get('category', '')).strip()
        # メモは「内容」と「メモ」を結合
        memo_parts = [str(row.get('memo', '')).strip(), str(row.get('note', '')).strip()]
        memo = ' '.join(p for p in memo_parts if p and p != 'nan')
        dt   = row['date'].strftime('%Y-%m-%d')

        if not cat or cat in ('nan', ''):
            continue

        if raw_amount < 0:
            # 負の金額 → 支出
            rows.append(('expense', cat, int(abs(raw_amount)), memo, dt))
        elif raw_amount > 0:
            # 正の金額 → 収入
            rows.append(('income', cat, int(raw_amount), memo, dt))

    return rows


def import_csv_to_db(filepath):
    rows = parse_mf_csv(filepath)
    conn = get_db()
    now = datetime.now().isoformat()

    existing_cats = {r['name'] for r in conn.execute('SELECT name FROM categories').fetchall()}
    new_cats = []
    for t_type, cat, *_ in rows:
        if cat and cat not in existing_cats:
            new_cats.append((cat, t_type, '#94a3b8', 99))
            existing_cats.add(cat)
    if new_cats:
        conn.executemany(
            'INSERT OR IGNORE INTO categories (name,type,color,sort_order) VALUES (?,?,?,?)',
            new_cats
        )

    prev_count = conn.execute("SELECT COUNT(*) FROM transactions WHERE source='csv'").fetchone()[0]
    conn.execute("DELETE FROM transactions WHERE source='csv'")
    conn.executemany(
        'INSERT INTO transactions (type,category,amount,memo,date,source,created_at) VALUES (?,?,?,?,?,?,?)',
        [(t, c, a, m, d, 'csv', now) for t, c, a, m, d in rows]
    )
    inserted = len(rows)
    skipped  = prev_count

    conn.commit()
    conn.close()
    return inserted, skipped


# ─────────────────────────────────────────
# 集計ロジック（kakeibo-dashboard と共通）
# ─────────────────────────────────────────
def get_expense_categories():
    conn = get_db()
    cats = [r['name'] for r in conn.execute(
        "SELECT name FROM categories WHERE type='expense' ORDER BY sort_order,name"
    ).fetchall()]
    conn.close()
    return cats

def get_yearly_matrix():
    today = date.today()
    conn  = get_db()

    first_row = conn.execute(
        "SELECT MIN(date) as first_date FROM transactions WHERE type='expense'"
    ).fetchone()
    if first_row and first_row['first_date']:
        fd = first_row['first_date'][:7]
        start_y, start_m = int(fd[:4]), int(fd[5:7])
    else:
        start_y, start_m = today.year, today.month
    if (start_y, start_m) < (EARLIEST_YEAR, EARLIEST_MONTH):
        start_y, start_m = EARLIEST_YEAR, EARLIEST_MONTH

    month_list = []
    y, m = start_y, start_m
    while (y * 12 + m - 1) <= (today.year * 12 + today.month - 1):
        month_list.append((y, m))
        m += 1
        if m > 12: m = 1; y += 1

    n_months = len(month_list)
    start_ym = f'{month_list[0][0]}-{month_list[0][1]:02d}-01'
    end_ym   = f'{today.year}-{today.month:02d}-31'
    rows = conn.execute(
        """SELECT strftime('%Y', date) as yr, strftime('%m', date) as mo,
                  category, SUM(amount) as total
           FROM transactions
           WHERE type='expense' AND date >= ? AND date <= ?
           GROUP BY yr, mo, category""",
        (start_ym, end_ym)
    ).fetchall()

    budget_rows = conn.execute(
        'SELECT category, amount FROM budgets WHERE year=? AND month=?',
        (today.year, today.month)
    ).fetchall()
    budget_map = {br['category']: br['amount'] for br in budget_rows}

    cat_rows = conn.execute(
        "SELECT name,color,sort_order FROM categories WHERE type='expense' ORDER BY sort_order,name"
    ).fetchall()
    conn.close()

    actuals = {}
    for row in rows:
        key = (int(row['yr']), int(row['mo']))
        actuals.setdefault(row['category'], {})[key] = int(row['total'])

    all_cats  = [r['name'] for r in cat_rows]
    extra_cats = [c for c in actuals if c not in all_cats]
    all_cats  = all_cats + extra_cats
    color_map = {r['name']: r['color'] for r in cat_rows}

    result = []
    monthly_totals = {i: 0 for i in range(n_months)}

    for cat in all_cats:
        cat_actuals    = actuals.get(cat, {})
        monthly_budget = budget_map.get(cat, 0)
        month_data = {}
        annual_total = 0
        has_any = False
        for idx, (y, m) in enumerate(month_list):
            actual = cat_actuals.get((y, m), 0)
            bgt    = monthly_budget
            pct    = round(actual / bgt * 100, 1) if bgt > 0 else None
            is_future = (y > today.year) or (y == today.year and m > today.month)
            month_data[idx] = {
                'actual': actual, 'budget': bgt, 'pct': pct,
                'is_future': is_future,
                'is_current': (y == today.year and m == today.month),
                'year': y, 'month': m,
            }
            annual_total += actual
            monthly_totals[idx] += actual
            if actual > 0:
                has_any = True
        if not has_any and monthly_budget == 0:
            continue

        months_with_data = [i for i in range(n_months) if month_data[i]['actual'] > 0]
        avg = annual_total // len(months_with_data) if months_with_data else 0
        result.append({
            'category': cat,
            'color': color_map.get(cat, '#94a3b8'),
            'monthly_budget': monthly_budget,
            'annual_total': annual_total,
            'monthly_avg': avg,
            'months': month_data,
            'is_no_budget': cat in NO_BUDGET_CATS,
        })

    budget_cats    = sorted([r for r in result if not r['is_no_budget']], key=lambda x: (-x['monthly_budget'], -x['annual_total']))
    no_budget_cats = sorted([r for r in result if r['is_no_budget']],     key=lambda x: -x['annual_total'])
    result = budget_cats + no_budget_cats

    monthly_totals_budget    = {i: 0 for i in range(n_months)}
    monthly_totals_no_budget = {i: 0 for i in range(n_months)}
    for r in budget_cats:
        for idx in range(n_months):
            monthly_totals_budget[idx] += r['months'][idx]['actual']
    for r in no_budget_cats:
        for idx in range(n_months):
            monthly_totals_no_budget[idx] += r['months'][idx]['actual']

    col_labels = []
    for i, (y, m) in enumerate(month_list):
        if m == 1 or i == 0:
            col_labels.append(f"'{str(y)[2:]}/{m}月")
        else:
            col_labels.append(f'{m}月')

    return {
        'col_labels': col_labels,
        'month_list': [[y, m] for y, m in month_list],
        'categories': result,
        'monthly_totals': monthly_totals,
        'monthly_totals_budget': monthly_totals_budget,
        'monthly_totals_no_budget': monthly_totals_no_budget,
        'no_budget_start_idx': len(budget_cats),
    }


def get_monthly_summary(year, month):
    conn = get_db()
    prefix = f'{year}-{month:02d}'
    rows = conn.execute(
        'SELECT type, SUM(amount) as total FROM transactions WHERE date LIKE ? GROUP BY type',
        (f'{prefix}%',)
    ).fetchall()
    income  = next((r['total'] for r in rows if r['type'] == 'income'),  0) or 0
    expense = next((r['total'] for r in rows if r['type'] == 'expense'), 0) or 0
    bgt_rows = conn.execute(
        'SELECT category, amount FROM budgets WHERE year=? AND month=?', (year, month)
    ).fetchall()
    bgt = sum(r['amount'] for r in bgt_rows if r['category'] not in NO_BUDGET_CATS)
    conn.close()
    return {'income': income, 'expense': expense, 'balance': income - expense, 'budget': bgt}


def get_budget_progress(year, month):
    conn = get_db()
    prefix = f'{year}-{month:02d}'
    budgets = {r['category']: r['amount'] for r in conn.execute(
        'SELECT category,amount FROM budgets WHERE year=? AND month=?', (year, month)
    ).fetchall()}
    actuals = {r['category']: int(r['total']) for r in conn.execute(
        "SELECT category,SUM(amount) as total FROM transactions WHERE type='expense' AND date LIKE ? GROUP BY category",
        (f'{prefix}%',)
    ).fetchall()}
    cats = conn.execute(
        "SELECT name,color FROM categories WHERE type='expense' ORDER BY sort_order,name"
    ).fetchall()
    conn.close()

    today = date.today()
    is_current = (year == today.year and month == today.month)
    _, last_day = calendar.monthrange(year, month)
    elapsed = min(today.day, last_day) if is_current else last_day
    month_progress = elapsed / last_day

    result = []
    for cr in cats:
        cat = cr['name']
        if cat in NO_BUDGET_CATS:
            continue
        budget = budgets.get(cat, 0)
        actual = actuals.get(cat, 0)
        if budget == 0 and actual == 0:
            continue
        pct      = round(actual / budget * 100, 1) if budget > 0 else None
        expected = int(budget * month_progress) if budget > 0 else None
        result.append({
            'category': cat, 'color': cr['color'],
            'budget': budget, 'actual': actual, 'pct': pct,
            'expected': expected,
            'over': (actual > budget) if budget > 0 else False,
        })
    result.sort(key=lambda x: -x['actual'])
    budget_cats = {k: v for k, v in budgets.items() if k not in NO_BUDGET_CATS}
    actual_cats = {k: v for k, v in actuals.items() if k not in NO_BUDGET_CATS}
    return {
        'categories': result,
        'total_budget': sum(budget_cats.values()),
        'total_actual': sum(actual_cats.values()),
        'month_progress': round(month_progress * 100, 1),
        'elapsed_days': elapsed,
        'last_day': last_day,
    }


def get_daily_data(year, month):
    conn = get_db()
    prefix = f'{year}-{month:02d}'
    _, last_day = calendar.monthrange(year, month)
    rows = conn.execute(
        """SELECT date,category,SUM(amount) as total
           FROM transactions WHERE type='expense' AND date LIKE ?
           GROUP BY date,category ORDER BY date""",
        (f'{prefix}%',)
    ).fetchall()
    cats   = [r['name'] for r in conn.execute(
        "SELECT name FROM categories WHERE type='expense' ORDER BY sort_order,name"
    ).fetchall()]
    colors = {r['name']: r['color'] for r in conn.execute('SELECT name,color FROM categories').fetchall()}
    days   = list(range(1, last_day + 1))
    day_cat = {d: {c: 0 for c in cats} for d in days}
    for row in rows:
        d = int(row['date'].split('-')[2])
        cat = row['category']
        if cat not in day_cat[d]: day_cat[d][cat] = 0
        day_cat[d][cat] = int(row['total'])
    conn.close()
    active_cats = [c for c in cats if any(day_cat[d].get(c, 0) > 0 for d in days)]
    datasets = [{
        'label': cat,
        'data': [day_cat[d].get(cat, 0) for d in days],
        'backgroundColor': colors.get(cat, '#94a3b8'),
    } for cat in active_cats]
    return {'labels': [f'{d}日' for d in days], 'datasets': datasets}


def get_cumulative_data(year, month, category=None):
    conn = get_db()
    prefix  = f'{year}-{month:02d}'
    _, last_day = calendar.monthrange(year, month)
    if category:
        rows = conn.execute(
            """SELECT date,SUM(amount) as daily_total
               FROM transactions WHERE type='expense' AND date LIKE ? AND category=?
               GROUP BY date ORDER BY date""",
            (f'{prefix}%', category)
        ).fetchall()
        bgt_row = conn.execute(
            'SELECT COALESCE(amount,0) as t FROM budgets WHERE year=? AND month=? AND category=?',
            (year, month, category)
        ).fetchone()
        bgt = bgt_row['t'] if bgt_row else 0
    else:
        rows = conn.execute(
            """SELECT date,SUM(amount) as daily_total
               FROM transactions WHERE type='expense' AND date LIKE ?
               GROUP BY date ORDER BY date""",
            (f'{prefix}%',)
        ).fetchall()
        bgt = conn.execute(
            'SELECT COALESCE(SUM(amount),0) as t FROM budgets WHERE year=? AND month=?',
            (year, month)
        ).fetchone()['t']
    conn.close()

    daily = {int(r['date'].split('-')[2]): int(r['daily_total']) for r in rows}
    cumulative = []
    running = 0
    for d in range(1, last_day + 1):
        running += daily.get(d, 0)
        cumulative.append(running)
    budget_line = [round(bgt * d / last_day) for d in range(1, last_day + 1)] if bgt > 0 else []

    today_dt = date.today()
    is_current = (year == today_dt.year and month == today_dt.month)
    today_idx  = (today_dt.day - 1) if is_current else (last_day - 1)

    forecast_low  = [None] * last_day
    forecast_high = [None] * last_day
    forecast_total_low = forecast_total_high = None

    if is_current and today_dt.day > 0:
        elapsed = today_dt.day
        current_total = cumulative[elapsed - 1] if elapsed <= last_day else cumulative[-1]
        if elapsed > 0 and current_total > 0:
            daily_rate = current_total / elapsed
            forecast_total_high = round(daily_rate * last_day)
            forecast_total_low  = max(bgt, current_total) if bgt > 0 else forecast_total_high
            remaining = last_day - (elapsed - 1)
            for d in range(elapsed - 1, last_day):
                ratio = (d - (elapsed - 2)) / remaining if remaining > 0 else 1.0
                forecast_high[d] = round(current_total + (forecast_total_high - current_total) * ratio)
                forecast_low[d]  = round(current_total + (forecast_total_low  - current_total) * ratio)

    return {
        'labels': [f'{d}日' for d in range(1, last_day + 1)],
        'actual': cumulative, 'budget_line': budget_line,
        'forecast_low': forecast_low, 'forecast_high': forecast_high,
        'budget_total': bgt, 'today_idx': today_idx,
        'forecast_total_low': forecast_total_low, 'forecast_total_high': forecast_total_high,
    }


def save_budgets_from_mf(budgets_dict: dict, year: int, month: int):
    """MFからスクレイプした予算をDBに保存（全月に適用）"""
    conn = get_db()
    for cat, amt in budgets_dict.items():
        for m in range(1, 13):
            conn.execute(
                'INSERT OR REPLACE INTO budgets (year,month,category,amount) VALUES (?,?,?,?)',
                (year, m, cat, int(amt))
            )
        conn.execute(
            'INSERT OR IGNORE INTO categories (name,type,color,sort_order) VALUES (?,?,?,?)',
            (cat, 'expense', '#94a3b8', 99)
        )
    conn.commit()
    conn.close()


def get_monthly_trend(category: str = None):
    today_dt = date.today()
    conn = get_db()

    first_row = conn.execute(
        "SELECT MIN(date) as first_date FROM transactions WHERE type='expense'"
    ).fetchone()
    if first_row and first_row['first_date']:
        fd = first_row['first_date'][:7]
        start_y, start_m = int(fd[:4]), int(fd[5:7])
    else:
        start_y, start_m = today_dt.year, today_dt.month
    if (start_y, start_m) < (EARLIEST_YEAR, EARLIEST_MONTH):
        start_y, start_m = EARLIEST_YEAR, EARLIEST_MONTH

    month_list = []
    y, m = start_y, start_m
    while (y * 12 + m - 1) <= (today_dt.year * 12 + today_dt.month - 1):
        month_list.append((y, m))
        m += 1
        if m > 12: m = 1; y += 1

    if category:
        cat_budget_row = conn.execute(
            'SELECT amount FROM budgets WHERE year=? AND month=? AND category=?',
            (today_dt.year, today_dt.month, category)
        ).fetchone()
        budget_monthly = int(cat_budget_row['amount']) if cat_budget_row else 0
    else:
        excl_placeholders = ','.join('?' * len(NO_BUDGET_CATS))
        budget_row = conn.execute(
            f'SELECT SUM(amount) as total FROM budgets WHERE year=? AND month=? AND category NOT IN ({excl_placeholders})',
            (today_dt.year, today_dt.month, *NO_BUDGET_CATS)
        ).fetchone()
        budget_monthly = int(budget_row['total'] or 0) if budget_row else 0

    labels, actuals, today_idx = [], [], -1
    month_cat_maps = []

    for i, (y, m) in enumerate(month_list):
        prefix = f'{y}-{m:02d}'
        rows = conn.execute(
            """SELECT category, SUM(amount) as total FROM transactions
               WHERE type='expense' AND date LIKE ? GROUP BY category""",
            (f'{prefix}%',)
        ).fetchall()
        cat_map = {r['category']: r['total'] for r in rows}
        actual  = int(cat_map.get(category, 0)) if category else sum(cat_map.values())
        if y == today_dt.year and m == today_dt.month:
            today_idx = i
        if m == 1 or (y == start_y and m == start_m):
            labels.append(f"'{str(y)[2:]}/{m}月")
        else:
            labels.append(f'{m}月')
        actuals.append(actual)
        month_cat_maps.append(cat_map)

    color_rows = conn.execute('SELECT name, color FROM categories WHERE type=?', ('expense',)).fetchall()
    cat_colors = {r['name']: r['color'] for r in color_rows}

    cat_totals = {}
    for d in month_cat_maps:
        for c, v in d.items():
            cat_totals[c] = cat_totals.get(c, 0) + v
    sorted_cats = sorted(cat_totals.keys(), key=lambda c: -cat_totals[c])

    stack_series = []
    for c in sorted_cats:
        series = []
        for i, d in enumerate(month_cat_maps):
            if today_idx >= 0 and i > today_idx:
                series.append(None)
            else:
                v = d.get(c, 0)
                series.append(int(v) if v else None)
        stack_series.append({'category': c, 'color': cat_colors.get(c, '#64748b'), 'data': series})

    past_actuals = [actuals[i] for i in range(len(actuals))
                    if actuals[i] > 0 and (today_idx < 0 or i < today_idx)]
    avg_actual = round(sum(past_actuals) / len(past_actuals)) if past_actuals else 0
    conn.close()

    return {
        'labels': labels,
        'month_list': [[y, m] for y, m in month_list],
        'actuals': actuals,
        'budgets': [budget_monthly] * len(month_list),
        'budget_monthly': budget_monthly,
        'stack_series': stack_series,
        'avg_actual': avg_actual,
        'today_idx': today_idx,
    }


def get_daily_matrix(year: int, month: int):
    today     = date.today()
    last_day  = calendar.monthrange(year, month)[1]
    is_current = (today.year == year and today.month == month)
    today_day  = today.day if is_current else None
    conn = get_db()

    rows = conn.execute(
        """SELECT CAST(strftime('%d', date) AS INTEGER) as d,
                  category, SUM(amount) as total
           FROM transactions
           WHERE type='expense' AND strftime('%Y-%m', date)=?
           GROUP BY d, category""",
        (f'{year}-{month:02d}',)
    ).fetchall()
    budget_rows = conn.execute(
        'SELECT category, amount FROM budgets WHERE year=? AND month=?', (year, month)
    ).fetchall()
    budget_map = {br['category']: br['amount'] for br in budget_rows}
    cat_rows = conn.execute(
        "SELECT name, color, sort_order FROM categories WHERE type='expense' ORDER BY sort_order, name"
    ).fetchall()
    conn.close()

    actuals = {}
    for row in rows:
        actuals.setdefault(row['category'], {})[int(row['d'])] = int(row['total'])

    all_cats  = [r['name'] for r in cat_rows]
    extra     = [c for c in actuals if c not in all_cats]
    all_cats  = all_cats + extra
    color_map = {r['name']: r['color'] for r in cat_rows}

    result = []
    daily_totals = {d: 0 for d in range(1, last_day + 1)}

    for cat in all_cats:
        cat_actuals    = actuals.get(cat, {})
        monthly_budget = budget_map.get(cat, 0)
        is_no_budget   = cat in NO_BUDGET_CATS

        if not cat_actuals and monthly_budget == 0 and not is_no_budget:
            continue

        daily_budget_f = monthly_budget / last_day if monthly_budget > 0 else 0
        day_data = {}
        cat_total = 0
        cumulative_actual = 0

        for d in range(1, last_day + 1):
            amt       = cat_actuals.get(d, 0)
            is_future = today_day is not None and d > today_day
            cumulative_actual += amt
            cumulative_budget_d = daily_budget_f * d
            if is_future:
                pct = None
            elif cumulative_budget_d > 0:
                pct = round(cumulative_actual / cumulative_budget_d * 100, 1)
            elif cumulative_actual > 0:
                pct = 999
            else:
                pct = 0
            day_data[d] = {'actual': amt, 'pct': pct, 'is_future': is_future, 'is_today': (d == today_day)}
            cat_total += amt
            if not is_future:
                daily_totals[d] += amt

        result.append({
            'category': cat, 'color': color_map.get(cat, '#94a3b8'),
            'monthly_budget': monthly_budget, 'total': cat_total,
            'is_no_budget': is_no_budget, 'days': day_data,
        })

    result.sort(key=lambda x: (-x['monthly_budget'], -x['total']))
    return {
        'year': year, 'month': month, 'last_day': last_day, 'today_day': today_day,
        'categories': result, 'daily_totals': daily_totals,
    }


# ─────────────────────────────────────────
# Routes
# ─────────────────────────────────────────
import time as _time
_STATIC_VER = str(int(_time.time()))

@app.route('/')
def index():
    today = date.today()
    return render_template('index.html', year=today.year, month=today.month, ver=_STATIC_VER)

@app.route('/api/daily-matrix')
def api_daily_matrix():
    today = date.today()
    return jsonify(get_daily_matrix(int(request.args.get('year', today.year)),
                                    int(request.args.get('month', today.month))))

@app.route('/api/monthly-trend')
def api_monthly_trend():
    return jsonify(get_monthly_trend(request.args.get('category') or None))

@app.route('/api/yearly-matrix')
def api_yearly_matrix():
    return jsonify(get_yearly_matrix())

@app.route('/api/summary')
def api_summary():
    return jsonify(get_monthly_summary(int(request.args.get('year', date.today().year)),
                                       int(request.args.get('month', date.today().month))))

@app.route('/api/budget-progress')
def api_budget_progress():
    return jsonify(get_budget_progress(int(request.args.get('year', date.today().year)),
                                       int(request.args.get('month', date.today().month))))

@app.route('/api/daily')
def api_daily():
    return jsonify(get_daily_data(int(request.args.get('year', date.today().year)),
                                  int(request.args.get('month', date.today().month))))

@app.route('/api/cumulative')
def api_cumulative():
    year     = int(request.args.get('year',  date.today().year))
    month    = int(request.args.get('month', date.today().month))
    category = request.args.get('category', '')
    return jsonify(get_cumulative_data(year, month, category if category else None))

@app.route('/api/categories')
def api_categories():
    conn = get_db()
    rows = conn.execute('''
        SELECT c.name, c.type, c.color, c.sort_order
        FROM categories c
        WHERE c.name IN (SELECT DISTINCT category FROM transactions)
          AND c.sort_order = (
              SELECT MAX(c2.sort_order) FROM categories c2
              WHERE c2.name = c.name AND c2.type = c.type
          )
        ORDER BY c.type, c.sort_order, c.name
    ''').fetchall()
    result = [dict(r) for r in rows]
    known_names = {r['name'] for r in result}
    extra = conn.execute(
        "SELECT DISTINCT category, type FROM transactions "
        "WHERE category NOT IN (SELECT name FROM categories)"
    ).fetchall()
    for r in extra:
        result.append({'name': r['category'], 'type': r['type'], 'color': '#7a9bc4', 'sort_order': 99})
    conn.close()
    result.sort(key=lambda x: (x['type'], x['sort_order'], x['name']))
    return jsonify(result)

@app.route('/api/transactions')
def api_transactions():
    year     = int(request.args.get('year',  date.today().year))
    month    = int(request.args.get('month', date.today().month))
    category = request.args.get('category', '')
    prefix   = f'{year}-{month:02d}'
    conn = get_db()
    if category:
        rows = conn.execute(
            "SELECT id,type,category,amount,memo,date FROM transactions "
            "WHERE date LIKE ? AND category=? ORDER BY date DESC,id DESC LIMIT 200",
            (f'{prefix}%', category)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT id,type,category,amount,memo,date FROM transactions WHERE date LIKE ? ORDER BY date DESC,id DESC LIMIT 200',
            (f'{prefix}%',)
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/mf-auto-download', methods=['POST'])
def api_mf_auto_download():
    """MoneyForward同期をバックグラウンドで実行"""
    try:
        from mf_downloader import run_download as _
    except ImportError as e:
        return jsonify({'error': f'mf_downloader が見つかりません: {e}'}), 500

    if not os.getenv('MF_EMAIL') or not os.getenv('MF_PASSWORD'):
        return jsonify({'error': '.env に MF_EMAIL / MF_PASSWORD が未設定です'}), 400

    import threading
    threading.Thread(target=_run_mf_sync_bg, daemon=True).start()
    return jsonify({'status': 'started'}), 202


@app.route('/webhook/line', methods=['POST'])
def line_webhook():
    from line_bot import verify_signature, parse_command, reply_text, push_text
    import json as _json

    body      = request.get_data()
    signature = request.headers.get('X-Line-Signature', '')
    if not verify_signature(body, signature):
        return 'Invalid signature', 400

    try:
        events = _json.loads(body).get('events', [])
    except Exception:
        return 'Bad request', 400

    for event in events:
        if event.get('type') != 'message': continue
        msg_obj = event.get('message', {})
        if msg_obj.get('type') != 'text': continue
        text        = msg_obj.get('text', '')
        reply_token = event.get('replyToken', '')
        command     = parse_command(text)

        if command == 'sync':
            reply_text(reply_token, '⟳ MoneyForward同期を開始します...')
            import threading
            threading.Thread(target=_run_mf_sync_bg, daemon=True).start()
        elif command == 'today':    _send_today_line(reply_token)
        elif command == 'status':   _send_status_line(reply_token)
        elif command == 'budget':   _send_budget_remaining_line(reply_token)
        elif command == 'lastmonth':_send_last_month_line(reply_token)
        elif command == 'url':
            public = os.getenv('RAILWAY_PUBLIC_DOMAIN') or os.getenv('NGROK_PUBLIC_URL') or 'URL取得できませんでした'
            if public and not public.startswith('http'): public = f'https://{public}'
            reply_text(reply_token, f'📱 ダッシュボードURL:\n{public}')
        else:
            from line_bot import HELP_TEXT
            reply_text(reply_token, HELP_TEXT)

    return 'OK', 200


def _run_mf_sync_bg():
    from line_bot import push_text
    try:
        from mf_downloader import run_download as mf_run
        result   = mf_run(download_dir=UPLOAD_DIR, db_path=DB_PATH)
        csv_path = result.get('csv_path')
        inserted = skipped = 0
        if csv_path and os.path.exists(csv_path):
            inserted, skipped = import_csv_to_db(csv_path)
        budgets = result.get('budgets', {})
        if budgets:
            today = date.today()
            save_budgets_from_mf(budgets, today.year, today.month)
        socketio.emit('mf_synced', {
            'inserted': inserted, 'skipped': skipped,
            'budgets_imported': len(budgets),
        })
        push_text(
            f'✅ MoneyForward同期完了！\n'
            f'━━━━━━━━━━━━━━\n'
            f'📥 新規: {inserted}件\n'
            f'💰 予算: {budgets}カテゴリ更新'
        )
    except Exception as e:
        push_text(f'❌ MoneyForward同期エラー:\n{e}')


def _send_status_line(reply_token):
    from line_bot import reply_text
    today = date.today()
    bp = get_budget_progress(today.year, today.month)
    expense   = bp['total_actual']
    budget    = bp['total_budget']
    remaining = budget - expense
    pct = round(expense / budget * 100) if budget > 0 else 0
    msg = (
        f'📊 {today.year}年{today.month}月 状況\n'
        f'━━━━━━━━━━━━━━\n'
        f'💸 支出:  ¥{expense:,}\n'
        f'🎯 予算:  ¥{budget:,}\n'
        f'💰 残高:  ¥{remaining:,}\n'
        f'📈 消化率: {pct}%\n'
        f'━━━━━━━━━━━━━━\n'
    )
    for cat in bp['categories'][:3]:
        bar = '🔴' if cat['over'] else ('🟡' if (cat['pct'] or 0) > 80 else '🟢')
        msg += f'{bar} {cat["category"]}: ¥{cat["actual"]:,}'
        if cat['budget']: msg += f' / ¥{cat["budget"]:,}'
        msg += '\n'
    reply_text(reply_token, msg.strip())


def _send_today_line(reply_token):
    from line_bot import reply_text
    today = date.today()
    conn = get_db()
    rows = conn.execute(
        "SELECT category, SUM(amount) as total FROM transactions "
        "WHERE date=? AND type='expense' GROUP BY category ORDER BY total DESC",
        (today.isoformat(),)
    ).fetchall()
    conn.close()
    total = sum(r['total'] for r in rows)
    if not rows:
        msg = f'📅 {today.month}月{today.day}日\n支出はありません'
    else:
        msg = f'📅 {today.month}月{today.day}日の支出\n━━━━━━━━━━━━━━\n'
        for r in rows: msg += f'  {r["category"]}: ¥{r["total"]:,}\n'
        msg += f'━━━━━━━━━━━━━━\n合計: ¥{total:,}'
    reply_text(reply_token, msg)


def _send_budget_remaining_line(reply_token):
    from line_bot import reply_text
    today = date.today()
    bp = get_budget_progress(today.year, today.month)
    remaining_total = bp['total_budget'] - bp['total_actual']
    msg = f'💰 {today.month}月 予算残高\n━━━━━━━━━━━━━━\n'
    for cat in bp['categories']:
        if not cat.get('budget'): continue
        rem  = cat['budget'] - cat['actual']
        icon = '🔴' if cat['over'] else ('🟡' if (cat['pct'] or 0) > 80 else '🟢')
        msg += f'{icon} {cat["category"]}: ¥{rem:,}\n'
    msg += f'━━━━━━━━━━━━━━\n残高合計: ¥{remaining_total:,}'
    reply_text(reply_token, msg)


def _send_last_month_line(reply_token):
    from line_bot import reply_text
    today = date.today()
    y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    bp = get_budget_progress(y, m)
    expense = bp['total_actual']
    budget  = bp['total_budget']
    pct = round(expense / budget * 100) if budget > 0 else 0
    msg = (f'📊 {y}年{m}月（先月）\n━━━━━━━━━━━━━━\n'
           f'💸 支出: ¥{expense:,}\n🎯 予算: ¥{budget:,}\n📈 消化率: {pct}%\n━━━━━━━━━━━━━━\n')
    for cat in bp['categories'][:5]:
        icon = '🔴' if cat['over'] else ('🟡' if (cat['pct'] or 0) > 80 else '🟢')
        msg += f'{icon} {cat["category"]}: ¥{cat["actual"]:,}'
        if cat['budget']: msg += f' / ¥{cat["budget"]:,}'
        msg += '\n'
    reply_text(reply_token, msg.strip())


@socketio.on('connect')
def on_connect():
    today = date.today()
    emit('init', {'year': today.year, 'month': today.month})


def start_ngrok(port: int = 5000) -> str | None:
    ngrok_token = os.getenv('NGROK_TOKEN', '')
    if not ngrok_token: return None
    try:
        import subprocess, time
        from pyngrok import ngrok, conf
        subprocess.run(['taskkill', '/f', '/im', 'ngrok.exe'], capture_output=True, check=False)
        time.sleep(1)
        conf.get_default().auth_token = ngrok_token
        tunnel = ngrok.connect(port, 'http')
        public_url = tunnel.public_url
        os.environ['NGROK_PUBLIC_URL'] = public_url
        print(f'[ngrok] 公開URL: {public_url}')
        return public_url
    except Exception as e:
        print(f'[ngrok] 起動失敗: {e}')
        return None


def _get_public_url(port: int) -> str:
    railway_domain = os.getenv('RAILWAY_PUBLIC_DOMAIN') or os.getenv('RAILWAY_STATIC_URL', '')
    if railway_domain:
        return f'https://{railway_domain}' if not railway_domain.startswith('http') else railway_domain
    render_url = os.getenv('RENDER_EXTERNAL_URL', '')
    if render_url: return render_url
    if os.getenv('RENDER') or os.getenv('FLY_APP_NAME'): return ''
    return start_ngrok(port) or ''


def _setup_line_on_startup(public_url: str):
    if not public_url: return
    webhook_url = f'{public_url}/webhook/line'
    try:
        from line_bot import push_server_started, is_configured, CHANNEL_ACCESS_TOKEN
        import requests as _req
        if CHANNEL_ACCESS_TOKEN:
            _req.post(
                'https://api.line.me/v2/bot/channel/webhook/endpoint',
                headers={'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}', 'Content-Type': 'application/json'},
                json={'webhook_endpoint': webhook_url}, timeout=8,
            )
        if is_configured():
            push_server_started(public_url)
    except Exception as e:
        print(f'[LINE] 起動通知スキップ: {e}')


init_db()
seed_default_categories()

_is_gunicorn = 'gunicorn' in os.getenv('SERVER_SOFTWARE', '') or not __name__ == '__main__'
if _is_gunicorn and os.getenv('RAILWAY_ENVIRONMENT'):
    _pub = _get_public_url(int(os.getenv('PORT', 5002)))
    print(f'MFダッシュボード起動中 (gunicorn/Railway)...')
    _setup_line_on_startup(_pub)

if __name__ == '__main__':
    _port = int(os.getenv('PORT', 5002))
    _pub  = _get_public_url(_port)
    _setup_line_on_startup(_pub)
    print(f'MFダッシュボード起動: http://localhost:{_port}')
    if _pub: print(f'  インターネット: {_pub}')
    socketio.run(app, host='0.0.0.0', port=_port, debug=False, allow_unsafe_werkzeug=True)
