"""
MoneyForward data analysis script
"""
import os, sys, time, glob
# Windows cp932 encoding fix
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

MF_EMAIL    = 'tmh9068@gmail.com'
MF_PASSWORD = 'Ht@14580006'
LOGIN_URL   = 'https://moneyforward.com/users/sign_in'
CSV_URL     = 'https://moneyforward.com/cf/csv?from_date=2023-09-01&to_date=2026-03-31'
BUDGET_URL  = 'https://moneyforward.com/spending_summaries'

DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def build_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    # ヘッドレスを無効: moneyforward.com がヘッドレスChromeをブロックするため
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1400,900')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--lang=ja')
    opts.add_experimental_option('excludeSwitches', ['enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_experimental_option('prefs', {
        'download.default_directory': DOWNLOAD_DIR,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
    })
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    return driver


def ss(driver, name):
    path = os.path.join(DOWNLOAD_DIR, f'{name}.png')
    driver.save_screenshot(path)
    print(f'  [SS] {path}')


def login(driver):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    print(f'[1] LOGIN: {LOGIN_URL}')
    driver.get(LOGIN_URL)
    time.sleep(3)
    ss(driver, '01_login_page')
    print(f'  title: {driver.title}')
    print(f'  url:   {driver.current_url}')

    # Find any text/email input
    inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="email"], input[type="text"], input[name*="email"]')
    print(f'  email inputs found: {len(inputs)}')
    for el in inputs:
        print(f'    id={el.get_attribute("id")} name={el.get_attribute("name")} type={el.get_attribute("type")}')

    # Try to fill email
    filled = False
    for sel in ['input[type="email"]', 'input[id*="email"]', 'input[name*="email"]',
                'input[type="text"]', '#mf-user-email']:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            el.clear()
            el.send_keys(MF_EMAIL)
            print(f'  email filled via: {sel}')
            filled = True
            break
        except:
            pass

    if not filled:
        print('  ERROR: could not find email input')
        body = driver.find_element(By.TAG_NAME, 'body').text[:500]
        print(f'  page text: {body}')
        return False

    # Click next/submit button
    submit_clicked = False
    for sel in ['input[type="submit"]', 'button[type="submit"]', 'button.sign-in-btn', '.submit-btn']:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            print(f'  clicked submit: {sel}')
            submit_clicked = True
            break
        except:
            pass
    if not submit_clicked:
        # Try pressing Enter in email field
        filled_el = driver.find_element(By.CSS_SELECTOR, 'input[type="email"]')
        filled_el.send_keys(Keys.RETURN)
        print('  pressed Enter in email field')

    # Wait for password field to appear (SPA: same URL, password input appears)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]'))
        )
        print('  password field appeared')
    except:
        print('  password field did not appear within 15s, checking page...')

    ss(driver, '02_after_email')
    print(f'  url after email: {driver.current_url}')

    # Log all inputs visible
    all_inputs = driver.find_elements(By.CSS_SELECTOR, 'input')
    print(f'  all inputs on page: {len(all_inputs)}')
    for el in all_inputs:
        print(f'    id={el.get_attribute("id")} name={el.get_attribute("name")} type={el.get_attribute("type")} visible={el.is_displayed()}')

    # Find password input
    pw_inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]')
    print(f'  password inputs: {len(pw_inputs)}')

    pw_filled = False
    for sel in ['input[type="password"]', 'input[id*="password"]', 'input[name*="password"]']:
        try:
            el = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            el.clear()
            el.send_keys(MF_PASSWORD)
            print(f'  password filled via: {sel}')
            pw_filled = True
            break
        except:
            pass

    if not pw_filled:
        print('  ERROR: could not find password input')
        body = driver.find_element(By.TAG_NAME, 'body').text[:600]
        print(f'  page text:\n{body}')
        return False

    # Submit login
    login_submitted = False
    for sel in ['input[type="submit"]', 'button[type="submit"]', 'button.sign-in-btn']:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            print(f'  clicked login: {sel}')
            login_submitted = True
            break
        except:
            pass
    if not login_submitted:
        pw_inputs[0].send_keys(Keys.RETURN)
        print('  pressed Enter in password field')

    # Wait for redirect
    try:
        WebDriverWait(driver, 25).until(
            lambda d: 'sign_in/password' not in d.current_url and 'sign_in' not in d.current_url
        )
        print(f'  after password: {driver.current_url}')
    except:
        ss(driver, '03_login_failed')
        print(f'  LOGIN FAILED: {driver.current_url}')
        return False

    ss(driver, '03_after_login')

    # Handle OTP if required
    if 'otp' in driver.current_url or 'two_factor' in driver.current_url or 'email_otp' in driver.current_url:
        print(f'  OTP required: {driver.current_url}')
        otp_inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="tel"], input[type="number"], input[type="text"][maxlength]')
        print(f'  OTP input fields: {len(otp_inputs)}')
        for el in otp_inputs:
            print(f'    id={el.get_attribute("id")} name={el.get_attribute("name")} maxlength={el.get_attribute("maxlength")}')

        # OTPファイルを事前にクリア
        otp_file = os.path.join(DOWNLOAD_DIR, 'otp.txt')
        if os.path.exists(otp_file):
            os.remove(otp_file)

        # 環境変数から先に確認
        otp_code = os.getenv('MF_OTP', '').strip()
        if otp_code:
            print(f'  OTP from env: {otp_code}')
        else:
            # OTPページに到達したことを通知し、ファイル経由で待機
            print('=' * 60)
            print('  >>> OTPページに到達しました <<<')
            print(f'  MoneyForwardからメールでOTPコードが届きます。')
            print(f'  届いたコードを以下のファイルに書き込んでください：')
            print(f'  {otp_file}')
            print(f'  （例: echo 123456 > "{otp_file}"）')
            print('  最大5分間待機します...')
            print('=' * 60)
            # 5分間、5秒ごとにファイルをチェック
            for _ in range(60):
                if os.path.exists(otp_file):
                    with open(otp_file, encoding='utf-8') as f:
                        otp_code = f.read().strip()
                    if otp_code:
                        print(f'  OTP from file: {otp_code}')
                        break
                time.sleep(5)
            if not otp_code:
                print('  OTP待機タイムアウト（5分）')
                return False

        # OTPページの全inputを列挙（デバッグ）
        ss(driver, '04_otp_page')
        all_inputs = driver.find_elements(By.CSS_SELECTOR, 'input')
        print(f'  OTP page all inputs: {len(all_inputs)}')
        for inp in all_inputs:
            print(f'    id={inp.get_attribute("id")} name={inp.get_attribute("name")} '
                  f'type={inp.get_attribute("type")} maxlen={inp.get_attribute("maxlength")} '
                  f'visible={inp.is_displayed()}')

        # Enter OTP
        otp_filled = False
        from selenium.webdriver.common.keys import Keys as K

        # Try digit-by-digit inputs first (1桁×6フィールド形式)
        digit_inputs = driver.find_elements(By.CSS_SELECTOR,
            'input[maxlength="1"]')
        visible_digits = [el for el in digit_inputs if el.is_displayed()]
        if len(visible_digits) >= len(otp_code):
            for i, digit in enumerate(otp_code):
                visible_digits[i].click()
                visible_digits[i].clear()
                visible_digits[i].send_keys(digit)
                time.sleep(0.1)
            print(f'  OTP entered digit-by-digit ({len(visible_digits)} fields): {otp_code}')
            otp_filled = True
        else:
            # Try known MoneyForward OTP input patterns
            for sel in [
                'input[name="mfid_user[email_otp]"]',
                'input[name*="email_otp"]',
                'input[name*="otp"]',
                'input[name*="code"]',
                'input[type="tel"]',
                'input[type="number"]',
                f'input[maxlength="{len(otp_code)}"]',
                'input[type="text"]:not([name*="email"]):not([name*="password"])',
            ]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    if el.is_displayed():
                        el.click()
                        el.clear()
                        el.send_keys(otp_code)
                        print(f'  OTP entered via: {sel}')
                        otp_filled = True
                        break
                except:
                    pass

        if not otp_filled:
            print('  Could not find OTP input field')
            ss(driver, '04_otp_no_field')
            return False

        time.sleep(0.5)
        ss(driver, '04_otp_entered')

        # Submit OTP
        submitted = False
        for sel in ['button[type="submit"]', 'input[type="submit"]', 'button.button-primary', 'button']:
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, sel)
                visible_btns = [b for b in btns if b.is_displayed()]
                if visible_btns:
                    visible_btns[0].click()
                    print(f'  OTP submitted via: {sel}')
                    submitted = True
                    break
            except:
                pass

        if not submitted:
            # Fallback: send RETURN on the OTP input
            try:
                otp_el = driver.find_element(By.CSS_SELECTOR,
                    'input[name*="otp"], input[name*="code"], input[maxlength="6"]')
                otp_el.send_keys(K.RETURN)
                print('  OTP submitted via RETURN key')
            except Exception as e:
                print(f'  Submit fallback failed: {e}')

        try:
            WebDriverWait(driver, 20).until(
                lambda d: 'otp' not in d.current_url and 'two_factor' not in d.current_url
            )
            print(f'  OTP success: {driver.current_url}')
        except:
            ss(driver, '04_otp_failed')
            print(f'  OTP failed: {driver.current_url}')
            return False

    # Final check: id.moneyforward.com/me ではなく moneyforward.com のサービス本体であることを確認
    # id.moneyforward.com のままの場合は moneyforward.com に移動してセッションを確立
    if driver.current_url.startswith('https://id.moneyforward.com'):
        print(f'  Still on ID service, navigating to main site...')
        driver.get('https://moneyforward.com')
        try:
            WebDriverWait(driver, 20).until(
                lambda d: not d.current_url.startswith('https://id.moneyforward.com')
            )
        except:
            pass
        print(f'  After main site nav: {driver.current_url}')

    if 'id.moneyforward.com/sign_in' in driver.current_url:
        print(f'  LOGIN FAILED (redirected back to login): {driver.current_url}')
        return False

    print(f'  LOGIN OK: {driver.current_url}')
    return True


def download_csv_all(driver):
    """2023/08〜現在まで全月CSVをダウンロードして結合する。
    URL形式: /cf/csv?from=YYYY%2FMM%2F25&month=M&year=YYYY
    （集計期間が25日始まり前提）
    """
    import pandas as pd
    from datetime import date

    print(f'\n[2] CSV全月ダウンロード (2023/08〜現在)')
    today = date.today()
    # 現在の会計月を特定（25日始まりなので、25日以降なら当月、それ以前なら前月）
    if today.day >= 25:
        cur_year, cur_month = today.year, today.month
    else:
        cur_year, cur_month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)

    all_dfs = []
    y, m = 2023, 8  # 2023/08から（8月25日〜9月24日の期間にも9月データが含まれる）
    while (y, m) <= (cur_year, cur_month):
        from_date = f'{y:04d}%2F{m:02d}%2F25'
        url = f'https://moneyforward.com/cf/csv?from={from_date}&month={m}&year={y}'
        before = set(glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv')))
        driver.get(url)
        # ダウンロード待機（最大15秒）
        csv_path = None
        for _ in range(15):
            after = set(glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv')))
            new = [f for f in after - before if not f.endswith('.crdownload')]
            if new:
                csv_path = max(new, key=os.path.getmtime)
                break
            time.sleep(1)
        if csv_path:
            print(f'  {y:04d}/{m:02d}: {os.path.basename(csv_path)}')
            for enc in ['cp932', 'utf-8-sig', 'utf-8']:
                try:
                    df = pd.read_csv(csv_path, encoding=enc)
                    all_dfs.append(df)
                    break
                except Exception:
                    pass
        else:
            print(f'  {y:04d}/{m:02d}: download failed (url={driver.current_url})')
        m += 1
        if m > 12:
            m = 1
            y += 1

    if not all_dfs:
        print('  全月ダウンロード失敗')
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    # 重複ID除去
    if 'ID' in combined.columns:
        before_len = len(combined)
        combined = combined.drop_duplicates(subset='ID')
        print(f'  結合: {len(all_dfs)}月分 → {before_len}行 → 重複除去後 {len(combined)}行')
    else:
        print(f'  結合: {len(all_dfs)}月分 → {len(combined)}行')

    out_path = os.path.join(DOWNLOAD_DIR, 'mf_all.csv')
    combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'  結合CSV保存: {out_path}')
    return out_path


def analyze_csv(csv_path):
    import pandas as pd

    print(f'\n[3] ANALYZE CSV: {csv_path}')
    df = None
    enc_used = None
    for enc in ['utf-8-sig', 'utf-8', 'cp932']:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            enc_used = enc
            break
        except:
            continue
    if df is None:
        print('  ERROR: failed to read CSV')
        return

    print(f'  encoding: {enc_used}')
    print(f'  shape: {df.shape}')
    print(f'  columns: {list(df.columns)}')
    print(f'\n  --- HEAD(3) ---')
    print(df.head(3).to_string())

    # Detect amount column
    amt_col = next((c for c in ['金額（円）', '金額', 'amount'] if c in df.columns), None)
    cat_col = next((c for c in ['大項目', 'カテゴリ', 'category'] if c in df.columns), None)
    sub_col = next((c for c in ['中項目', 'サブカテゴリ'] if c in df.columns), None)
    date_col = next((c for c in ['日付', 'date'] if c in df.columns), None)

    print(f'\n  amt_col={amt_col}  cat_col={cat_col}  sub_col={sub_col}  date_col={date_col}')

    # Filter
    df_work = df.copy()
    if '計算対象' in df.columns:
        cnt_before = len(df_work)
        df_work = df_work[df_work['計算対象'].astype(str).str.strip() == '1']
        print(f'  計算対象=1 filter: {cnt_before} -> {len(df_work)}')

    if '振替' in df.columns:
        cnt_before = len(df_work)
        df_work = df_work[df_work['振替'].astype(str).str.strip() != '1']
        print(f'  振替除外: {cnt_before} -> {len(df_work)}')

    if amt_col:
        df_work[amt_col] = pd.to_numeric(df_work[amt_col], errors='coerce').fillna(0)
        df_exp = df_work[df_work[amt_col] < 0].copy()
        df_inc = df_work[df_work[amt_col] > 0].copy()
        print(f'  expense rows: {len(df_exp)}  income rows: {len(df_inc)}')

    # Date range
    if date_col:
        df_work[date_col] = pd.to_datetime(df_work[date_col], errors='coerce')
        if date_col in df_exp.columns:
            df_exp[date_col] = pd.to_datetime(df_exp[date_col], errors='coerce')
        valid_dates = df_work[date_col].dropna()
        if len(valid_dates):
            print(f'  date range: {valid_dates.min().strftime("%Y-%m")} ~ {valid_dates.max().strftime("%Y-%m")}')
            months = df_exp[date_col].dt.to_period('M').nunique() if amt_col and len(df_exp) else 0

    # Category analysis
    if cat_col and amt_col and len(df_exp) > 0:
        print(f'\n  === EXPENSE CATEGORIES (大項目 x 中項目) ===')
        if sub_col:
            grp = df_exp.groupby([cat_col, sub_col])[amt_col].agg(['count','sum']).reset_index()
            grp['abs_sum'] = grp['sum'].abs()
            grp = grp.sort_values([cat_col, 'abs_sum'], ascending=[True, False])
            for _, r in grp.iterrows():
                print(f'    [{r[cat_col]}] {r[sub_col]}  {int(r["count"])}件  ¥{int(r["abs_sum"]):,}')
        else:
            grp = df_exp.groupby(cat_col)[amt_col].agg(['count','sum']).reset_index()
            grp['abs_sum'] = grp['sum'].abs()
            grp = grp.sort_values('abs_sum', ascending=False)
            for _, r in grp.iterrows():
                print(f'    [{r[cat_col]}]  {int(r["count"])}件  ¥{int(r["abs_sum"]):,}')

        print(f'\n  === MONTHLY AVERAGE PER CATEGORY ===')
        cat_sum = df_exp.groupby(cat_col)[amt_col].sum().abs().sort_values(ascending=False)
        for cat, total in cat_sum.items():
            avg = int(total / months) if months > 0 else 0
            print(f'    {cat}: total=¥{int(total):,}  monthly_avg=¥{avg:,}')

    # Income categories
    if cat_col and amt_col and len(df_inc) > 0:
        print(f'\n  === INCOME CATEGORIES ===')
        if sub_col:
            grp_i = df_inc.groupby([cat_col, sub_col])[amt_col].agg(['count','sum']).reset_index()
            grp_i = grp_i.sort_values([cat_col, 'sum'], ascending=[True, False])
            for _, r in grp_i.iterrows():
                print(f'    [{r[cat_col]}] {r[sub_col]}  {int(r["count"])}件  ¥{int(r["sum"]):,}')
        else:
            grp_i = df_inc.groupby(cat_col)[amt_col].sum().sort_values(ascending=False)
            for cat, total in grp_i.items():
                print(f'    [{cat}]  ¥{int(total):,}')

    # Excluded (計算対象=0)
    if '計算対象' in df.columns and cat_col:
        excl = df[df['計算対象'].astype(str).str.strip() != '1']
        if len(excl) > 0:
            print(f'\n  === EXCLUDED (計算対象!=1): {len(excl)}行 ===')
            excl_cats = excl[cat_col].value_counts()
            for cat, cnt in excl_cats.items():
                print(f'    {cat}: {cnt}件')

    # Transfers (振替=1)
    if '振替' in df.columns and cat_col:
        trans = df[df['振替'].astype(str).str.strip() == '1']
        if len(trans) > 0:
            print(f'\n  === TRANSFERS (振替=1): {len(trans)}行 ===')
            trans_cats = trans[cat_col].value_counts()
            for cat, cnt in trans_cats.items():
                print(f'    {cat}: {cnt}件')


def scrape_budgets(driver):
    from selenium.webdriver.common.by import By

    print(f'\n[4] 予算ページ探索')
    # まず /cf ページから予算リンクを探す
    driver.get('https://moneyforward.com/cf')
    time.sleep(3)
    links = driver.find_elements(By.TAG_NAME, 'a')
    budget_links = [(l.text.strip(), l.get_attribute('href')) for l in links
                    if l.get_attribute('href') and ('budget' in (l.get_attribute('href') or '').lower() or '予算' in l.text)]
    print(f'  予算リンク: {budget_links}')

    # ナビゲーションの「予算」リンクを探してクリック
    nav_budget = None
    for txt, href in budget_links:
        if href and ('budget' in href.lower() or '予算' in txt):
            nav_budget = href
            break

    # 試すURL一覧
    budget_urls_to_try = []
    if nav_budget:
        budget_urls_to_try.append(nav_budget)
    budget_urls_to_try += [
        'https://moneyforward.com/budgets',
        'https://moneyforward.com/cf/budgets',
        'https://moneyforward.com/budget',
        'https://moneyforward.com/cf',  # メインページの予算タブ
    ]

    for url in budget_urls_to_try:
        print(f'  Trying: {url}')
        driver.get(url)
        time.sleep(3)
        ss(driver, f'05_budget_{url.split("/")[-1]}')
        body = driver.find_element(By.TAG_NAME, 'body').text
        lines = [l.strip() for l in body.split('\n') if l.strip()]
        print(f'  → url={driver.current_url} lines={len(lines)}')
        if len(lines) > 8 and '見つかりません' not in body and 'Forbidden' not in body:
            print(f'  FOUND: {driver.current_url}')
            for line in lines[:100]:
                print(f'    {line}')
            break


if __name__ == '__main__':
    driver = build_driver()
    try:
        ok = login(driver)
        if not ok:
            print('Login failed.')
            sys.exit(1)

        csv_path = download_csv_all(driver)
        if csv_path:
            analyze_csv(csv_path)
        else:
            print('CSV download failed.')

        scrape_budgets(driver)

    finally:
        driver.quit()
        print('\nDone.')
