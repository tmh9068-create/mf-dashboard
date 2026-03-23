"""
MoneyForward ME 自動連携モジュール
- Selenium でログインし CSV をダウンロード
- 予算ページをスクレイプして予算データを取得
"""
import os, time, glob, shutil
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

MF_EMAIL    = os.getenv('MF_EMAIL', '')
MF_PASSWORD = os.getenv('MF_PASSWORD', '')

LOGIN_URL   = 'https://id.moneyforward.com/sign_in'
BUDGET_URL  = 'https://moneyforward.com/cf/budgets'
CSV_URL     = 'https://moneyforward.com/cf/csv'

# ── CSV ダウンロード対象期間
FROM_DATE = '2023-09-01'


def _build_driver(download_dir: str):
    """Selenium Chrome ドライバーを生成"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1280,900')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_experimental_option('excludeSwitches', ['enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_experimental_option('prefs', {
        'download.default_directory': download_dir,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True,
    })

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


def _login(driver) -> bool:
    """MoneyForward にログイン。成功したら True を返す"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    print('[MF] ログインページを開く...')
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)

    # メールアドレス入力
    try:
        email_input = wait.until(EC.presence_of_element_located((By.ID, 'mf-user-email')))
    except Exception:
        # フォールバック: name属性で探す
        email_input = wait.until(EC.presence_of_element_located((By.NAME, 'mf_user[email]')))
    email_input.clear()
    email_input.send_keys(MF_EMAIL)

    # 「ログインする」または「次へ」ボタン
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
        next_btn.click()
        time.sleep(1.5)
    except Exception:
        pass

    # パスワード入力
    try:
        pw_input = wait.until(EC.presence_of_element_located((By.ID, 'mf-user-password')))
    except Exception:
        pw_input = wait.until(EC.presence_of_element_located((By.NAME, 'mf_user[password]')))
    pw_input.clear()
    pw_input.send_keys(MF_PASSWORD)

    # ログインボタン
    try:
        login_btn = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
        login_btn.click()
    except Exception:
        from selenium.webdriver.common.keys import Keys
        pw_input.send_keys(Keys.RETURN)

    # ログイン完了待機（URLが変わるまで）
    try:
        WebDriverWait(driver, 20).until(
            lambda d: 'moneyforward.com' in d.current_url and 'sign_in' not in d.current_url
        )
        print(f'[MF] ログイン成功: {driver.current_url}')
        return True
    except Exception as e:
        print(f'[MF] ログイン失敗: {e}  現在URL={driver.current_url}')
        return False


def _download_csv(driver, download_dir: str) -> str | None:
    """CSV をダウンロードしてパスを返す"""
    today_str = date.today().strftime('%Y-%m-%d')
    url = f'{CSV_URL}?from={FROM_DATE}&to={today_str}'
    print(f'[MF] CSV ダウンロード: {url}')

    # ダウンロード前のファイル一覧を記録
    before = set(glob.glob(os.path.join(download_dir, '*.csv')))

    driver.get(url)
    time.sleep(3)  # ダウンロード開始まで待機

    # ダウンロード完了を待機（最大30秒）
    for _ in range(30):
        after = set(glob.glob(os.path.join(download_dir, '*.csv')))
        new_files = after - before
        # .crdownload（ダウンロード中）でないファイルを探す
        completed = [f for f in new_files if not f.endswith('.crdownload')]
        if completed:
            csv_path = max(completed, key=os.path.getmtime)
            print(f'[MF] CSV 取得完了: {csv_path}')
            return csv_path
        # .crdownload が残っている場合は待機
        crdownloads = glob.glob(os.path.join(download_dir, '*.crdownload'))
        if crdownloads:
            time.sleep(1)
            continue
        time.sleep(1)

    print('[MF] CSV ダウンロードタイムアウト')
    return None


def _scrape_budgets(driver) -> dict:
    """予算ページから大項目ごとの月次予算を取得"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    print('[MF] 予算ページをスクレイプ...')
    driver.get(BUDGET_URL)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table, .budget, [class*="budget"]'))
        )
    except Exception:
        time.sleep(3)

    budgets = {}

    # 戦略1: テーブル行から取得
    rows = driver.find_elements(By.CSS_SELECTOR, 'table tr')
    for row in rows:
        cells = row.find_elements(By.CSS_SELECTOR, 'td, th')
        if len(cells) >= 2:
            cat_text = cells[0].text.strip()
            amt_text = cells[-1].text.strip()
            amt = _parse_amount(amt_text)
            if cat_text and amt and amt > 0:
                budgets[cat_text] = amt

    # 戦略2: .budget-category や data-属性を持つ要素
    if not budgets:
        items = driver.find_elements(By.CSS_SELECTOR,
            '[class*="category"], [class*="budget-item"], [data-category]')
        for item in items:
            cat = item.get_attribute('data-category') or ''
            amt_el = None
            try:
                amt_el = item.find_element(By.CSS_SELECTOR, '[class*="amount"], input[type="number"], input[type="text"]')
            except Exception:
                pass
            if not cat:
                try:
                    cat = item.find_element(By.CSS_SELECTOR, '[class*="name"], [class*="label"]').text.strip()
                except Exception:
                    pass
            if amt_el:
                val = amt_el.get_attribute('value') or amt_el.text
                amt = _parse_amount(val)
                if cat and amt and amt > 0:
                    budgets[cat] = amt

    # 戦略3: ページ全体のテキストからパターンマッチ
    if not budgets:
        import re
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        pattern = re.compile(r'([^\d\n¥,]+?)\s*[：:]\s*¥?([\d,]+)')
        for m in pattern.finditer(page_text):
            cat = m.group(1).strip()
            amt = _parse_amount(m.group(2))
            if cat and amt and 1000 <= amt <= 10_000_000:
                budgets[cat] = amt

    print(f'[MF] 予算取得: {len(budgets)}カテゴリ  {budgets}')
    return budgets


def _parse_amount(text: str) -> int | None:
    """¥15,000 や 15000 などの文字列を int に変換"""
    import re
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
        if not _login(driver):
            raise RuntimeError('MoneyForward ログイン失敗')

        # CSV ダウンロード
        csv_path = _download_csv(driver, download_dir)
        result['csv_path'] = csv_path

        # 予算スクレイプ
        try:
            result['budgets'] = _scrape_budgets(driver)
        except Exception as e:
            print(f'[MF] 予算スクレイプ失敗: {e}')

    finally:
        driver.quit()

    return result
