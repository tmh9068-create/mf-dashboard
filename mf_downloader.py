"""
MoneyForward ME 自動連携モジュール
- OAuth経由でログイン（moneyforward.com/users/sign_in → id.moneyforward.com → callback）
- 月別CSVを2023/08から現在までダウンロードして結合
- spending_summaries ページから予算データを取得
"""
import os, time, glob, shutil, re
from datetime import date
from dotenv import load_dotenv

load_dotenv()

MF_EMAIL    = os.getenv('MF_EMAIL', '')
MF_PASSWORD = os.getenv('MF_PASSWORD', '')

# OAuthフロー経由で moneyforward.com セッションを確立するため
# id.moneyforward.com に直接ではなく moneyforward.com から開始する
LOGIN_URL   = 'https://moneyforward.com/users/sign_in'
BUDGET_URL  = 'https://moneyforward.com/spending_summaries'

# 集計期間開始（この月の25日から取得開始）
FROM_YEAR  = 2023
FROM_MONTH = 8   # 8月25日〜9月24日の期間に9月初旬データが含まれる


def _build_driver(download_dir: str):
    """Selenium Chrome ドライバーを生成"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1280,900')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--lang=ja')
    opts.add_experimental_option('excludeSwitches', ['enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_experimental_option('prefs', {
        'download.default_directory': download_dir,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True,
    })

    # Railway 環境では headless が必要（DISPLAY が無い）
    if os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('HEADLESS', '').lower() == 'true':
        opts.add_argument('--headless=new')

    chrome_bin = os.getenv('CHROME_BIN', '')
    chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '')

    if chrome_bin:
        opts.binary_location = chrome_bin

    if chromedriver_path and os.path.exists(chromedriver_path):
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=opts)
    else:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
        except Exception:
            driver = webdriver.Chrome(options=opts)

    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    return driver


def _login(driver, download_dir: str = '') -> bool:
    """MoneyForward にログイン（OTP対応）"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    print('[MF] ログイン開始...')
    driver.get(LOGIN_URL)
    time.sleep(3)

    # moneyforward.com → id.moneyforward.com へのリダイレクトを待つ
    try:
        WebDriverWait(driver, 15).until(
            lambda d: 'id.moneyforward.com' in d.current_url or 'sign_in' in d.current_url
        )
    except Exception:
        pass

    # メールアドレス入力
    email_filled = False
    for sel in ['input[type="email"]', 'input[name*="email"]', '#mf-user-email']:
        try:
            el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            el.clear()
            el.send_keys(MF_EMAIL)
            el.send_keys(Keys.RETURN)
            email_filled = True
            break
        except Exception:
            pass
    if not email_filled:
        print('[MF] メール入力フィールドが見つかりません')
        return False

    # パスワードフィールドが現れるまで待機
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]')))
    except Exception:
        pass

    # パスワード入力
    pw_filled = False
    for sel in ['input[type="password"]', 'input[name*="password"]']:
        try:
            el = WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            el.clear()
            el.send_keys(MF_PASSWORD)
            el.send_keys(Keys.RETURN)
            pw_filled = True
            break
        except Exception:
            pass
    if not pw_filled:
        print('[MF] パスワード入力フィールドが見つかりません')
        return False

    # OTP or ログイン完了 を待つ
    try:
        WebDriverWait(driver, 20).until(
            lambda d: 'email_otp' in d.current_url or 'moneyforward.com/' == d.current_url
                      or (d.current_url.startswith('https://moneyforward.com') and 'sign_in' not in d.current_url)
        )
    except Exception:
        pass

    # OTP 処理
    if 'email_otp' in driver.current_url or 'otp' in driver.current_url:
        print('[MF] OTP 認証が必要です')
        otp_code = _get_otp(download_dir)
        if not otp_code:
            print('[MF] OTP が提供されませんでした')
            return False

        # OTP 入力
        otp_filled = False
        for sel in ['input[name="email_otp"]', 'input[name*="email_otp"]', 'input[name*="otp"]',
                    'input[type="text"]']:
            try:
                el = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                if el.is_displayed():
                    el.click()
                    el.clear()
                    el.send_keys(otp_code)
                    otp_filled = True
                    break
            except Exception:
                pass

        if not otp_filled:
            print('[MF] OTP 入力フィールドが見つかりません')
            return False

        time.sleep(0.5)

        # OTP 送信
        submitted = False
        for sel in ['button[type="submit"]', 'input[type="submit"]', 'button']:
            try:
                btns = [b for b in driver.find_elements(By.CSS_SELECTOR, sel) if b.is_displayed()]
                if btns:
                    btns[0].click()
                    submitted = True
                    break
            except Exception:
                pass
        if not submitted:
            try:
                driver.find_element(By.CSS_SELECTOR, 'input[name*="otp"]').send_keys(Keys.RETURN)
            except Exception:
                pass

        # OTP 後のリダイレクト待機
        try:
            WebDriverWait(driver, 25).until(
                lambda d: 'otp' not in d.current_url and 'two_factor' not in d.current_url
            )
        except Exception:
            print(f'[MF] OTP 後のリダイレクト待機タイムアウト: {driver.current_url}')
            return False

    if 'id.moneyforward.com/sign_in' in driver.current_url:
        print('[MF] ログインページに戻されました（認証失敗）')
        return False

    print(f'[MF] ログイン成功: {driver.current_url}')
    return True


def _get_otp(download_dir: str = '') -> str:
    """OTPコードを env var またはファイルから取得（最大5分待機）"""
    # 環境変数から先に確認
    code = os.getenv('MF_OTP', '').strip()
    if code:
        return code

    # ファイル経由
    otp_file = os.path.join(download_dir or '.', 'otp.txt')
    # 古いファイルを削除
    if os.path.exists(otp_file):
        os.remove(otp_file)

    print(f'[MF] OTPをファイルに書き込んでください: {otp_file}')
    for _ in range(60):  # 5分待機
        if os.path.exists(otp_file):
            try:
                with open(otp_file, encoding='utf-8') as f:
                    code = f.read().strip()
                if code:
                    return code
            except Exception:
                pass
        time.sleep(5)
    return ''


def _download_csv_all(driver, download_dir: str) -> str | None:
    """2023/08〜現在まで全月CSVをダウンロードして結合ファイルを返す"""
    import pandas as pd

    today = date.today()
    # 現在の会計月（集計期間が25日始まりのため）
    if today.day >= 25:
        cur_year, cur_month = today.year, today.month
    else:
        cur_year, cur_month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)

    all_dfs = []
    y, m = FROM_YEAR, FROM_MONTH
    while (y, m) <= (cur_year, cur_month):
        from_date = f'{y:04d}%2F{m:02d}%2F25'
        url = f'https://moneyforward.com/cf/csv?from={from_date}&month={m}&year={y}'
        before = set(glob.glob(os.path.join(download_dir, '*.csv')))
        driver.get(url)
        csv_path = None
        for _ in range(15):
            after = set(glob.glob(os.path.join(download_dir, '*.csv')))
            new = [f for f in after - before if not f.endswith('.crdownload')]
            if new:
                csv_path = max(new, key=os.path.getmtime)
                break
            time.sleep(1)
        if csv_path:
            for enc in ['cp932', 'utf-8-sig', 'utf-8']:
                try:
                    df = pd.read_csv(csv_path, encoding=enc)
                    all_dfs.append(df)
                    break
                except Exception:
                    pass
            print(f'[MF] {y:04d}/{m:02d} ダウンロード完了')
        else:
            print(f'[MF] {y:04d}/{m:02d} ダウンロード失敗')
        m += 1
        if m > 12:
            m = 1
            y += 1

    if not all_dfs:
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    if 'ID' in combined.columns:
        combined = combined.drop_duplicates(subset='ID')

    out_path = os.path.join(download_dir, 'mf_all.csv')
    combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'[MF] 全月結合CSV保存: {out_path} ({len(combined)}行)')
    return out_path


def _scrape_budgets(driver) -> dict:
    """spending_summaries ページから大項目ごとの月次予算を取得"""
    from selenium.webdriver.common.by import By

    print('[MF] 予算ページをスクレイプ...')
    driver.get(BUDGET_URL)
    time.sleep(3)

    page_text = driver.find_element(By.TAG_NAME, 'body').text
    budgets = _parse_spending_summaries(page_text)
    print(f'[MF] 予算取得: {len(budgets)}カテゴリ  {budgets}')
    return budgets


def _parse_spending_summaries(page_text: str) -> dict:
    """
    spending_summaries のページテキストから予算を抽出。
    フォーマット（1行ずつ）:
      カテゴリ名
      実績額円
      残額（正）または超過額（負）円
    budget = 実績 + 残額
    """
    BUDGET_CATS = {
        '食費', '日用品', '衣服・美容', '教養・教育',
        '交通費', '通信費', '水道・光熱費', '健康・医療',
        '趣味・娯楽', '交際費', '毎月決まった支出',
    }

    lines = [l.strip() for l in page_text.split('\n') if l.strip()]
    budgets = {}

    def parse_yen(s: str):
        cleaned = re.sub(r'[¥,\s円▲▼△▽\+\-]', '', s)
        try:
            return int(float(cleaned))
        except Exception:
            return None

    def parse_signed(s: str):
        """正負を考慮した金額パース（負数は超過を意味する）"""
        sign = -1 if s.startswith('-') or '▲' in s else 1
        cleaned = re.sub(r'[¥,\s円▲▼△▽\+\-]', '', s)
        try:
            return sign * int(float(cleaned))
        except Exception:
            return None

    i = 0
    while i < len(lines):
        line = lines[i]
        if line in BUDGET_CATS:
            if i + 2 < len(lines):
                actual = parse_yen(lines[i + 1])
                remaining = parse_signed(lines[i + 2])
                if actual is not None and remaining is not None:
                    budget = actual + remaining
                    if budget > 0:
                        budgets[line] = budget
            i += 3
        else:
            i += 1

    return budgets


def _parse_amount(text: str) -> int | None:
    """¥15,000 や 15000 などの文字列を int に変換"""
    if not text:
        return None
    cleaned = re.sub(r'[¥,\s円]', '', str(text))
    try:
        return int(float(cleaned))
    except Exception:
        return None


def run_download(download_dir: str, db_path: str = '') -> dict:
    """
    MoneyForward からデータを取得するメイン関数。
    Returns: {'csv_path': str|None, 'budgets': dict}
    """
    if not MF_EMAIL or not MF_PASSWORD:
        raise ValueError('MF_EMAIL / MF_PASSWORD が .env に未設定です')

    os.makedirs(download_dir, exist_ok=True)
    driver = _build_driver(download_dir)
    result = {'csv_path': None, 'budgets': {}}

    try:
        if not _login(driver, download_dir):
            raise RuntimeError('MoneyForward ログイン失敗')

        # 全月CSV ダウンロード
        csv_path = _download_csv_all(driver, download_dir)
        result['csv_path'] = csv_path

        # 予算スクレイプ
        try:
            result['budgets'] = _scrape_budgets(driver)
        except Exception as e:
            print(f'[MF] 予算スクレイプ失敗: {e}')

    finally:
        driver.quit()

    return result
