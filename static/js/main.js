/* ──────────────────────────────────────
   状態
────────────────────────────────────── */
const TODAY = new Date();
let currentYear   = TODAY.getFullYear();
let currentMonth  = TODAY.getMonth() + 1;
let selectedYear  = currentYear;
let selectedMonth = currentMonth;

let allCategories   = [];
let matrixData      = null;
let stackedChart    = null;
let cumulChart      = null;
let detailCatChart  = null;
let mainCumulChart  = null;
let monthlyBarChart = null;
let socket          = null;
let selectedCategory  = null;

// 日次チャートの年月（デフォルト = 今月）
let selectedDailyYear  = currentYear;
let selectedDailyMonth = currentMonth;
// 日次チャートのカテゴリ（null = 合計）
let selectedDailyCat   = null;
// 月間トレンドの期間（ヶ月）
let trendMonths = 12;
// カテゴリスライダー用リスト（"合計" + 支出カテゴリ）
let dailyCatList = ['合計'];
// 月間トレンドのカテゴリフィルター
let selectedTrendCat = null;

const NO_BUDGET_CATS = new Set(['交通','高額支出(10万以上)','交際費','未分類','レジャー']);

// 前回値（変化アニメーション用）
let prevSummaryVals = { expense: 0, budget: 0, remaining: 0, daily: 0 };

const MONTH_NAMES = ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月'];

const CAT_ICONS = {
  // ── 支出・予算管理カテゴリ（kakeibo 準拠）
  'スーパー':           '🛒',
  '外食':               '🍽️',
  '学費':               '🎓',
  '固定費':             '🏠',
  '小遣い':             '🎮',
  '美容':               '💄',
  '日用品':             '🧴',
  'パパ昼食':           '🍱',
  '水道':               '🚿',
  'レオ':               '🐶',
  '衣服':               '👗',
  '電気':               '⚡',
  'ガス':               '🔥',
  // ── 予算外カテゴリ
  '交通':               '🚃',
  'レジャー':           '✈️',
  '交際費':             '🥂',
  '高額支出(10万以上)': '💸',
  '未分類':             '❓',
  // ── 収入
  '給与所得':           '💴',
  'その他収入':         '📥',
};

/* ──────────────────────────────────────
   初期化
────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', async () => {
  startClock();
  setupSocket();
  setupEvents();
  await loadCategories();
  initDailyCatSlider();
  initDailyMonthSelect();
  initTrendCatSelect();
  setupDailySwipe();
  setupMonthSwipe();
  updateDailyMonthLabel();
  // ポップアップ閉じるイベント
  document.getElementById('cell-popup-close')?.addEventListener('click', closeCellPopup);
  document.addEventListener('click', e => {
    const popup = document.getElementById('cell-detail-popup');
    if (popup && !popup.classList.contains('hidden') && !popup.contains(e.target)) {
      closeCellPopup();
    }
  });
  await refreshAll();
});


function startClock() {
  const el = document.getElementById('clock');
  const tick = () => {
    const n = new Date();
    el.textContent = n.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  };
  tick();
  setInterval(tick, 1000);
}


async function loadCategories() {
  try {
    const r = await fetch('/api/categories');
    allCategories = await r.json();
  } catch (e) { console.error('カテゴリ取得失敗', e); }
}

/* ──────────────────────────────────────
   WebSocket
────────────────────────────────────── */
function setupSocket() {
  socket = io({ transports: ['polling'] });
  socket.on('connect',      () => setLive(true));
  socket.on('disconnect',   () => setLive(false));
  socket.on('data_updated', () => refreshAll());
  socket.on('mf_synced',  (d) => {
    const src = d.source === 'zaim' ? 'Zaim' : 'MF';
    showToast(`✓ ${src}データ更新完了`, 'success');
    loadCategories().then(() => { initDailyCatSlider(); refreshAll(); });
  });
}

function setLive(on) {
  const b = document.getElementById('live-badge');
  b.textContent = on ? '● LIVE' : '● オフライン';
  b.className   = on ? 'live-badge' : 'live-badge offline';
}

/* ──────────────────────────────────────
   全体リフレッシュ
────────────────────────────────────── */
async function refreshAll() {
  showSkeleton(true);
  const catParam = selectedDailyCat ? `&category=${encodeURIComponent(selectedDailyCat)}` : '';
  const [mData, summaryData, bpData, cumulData, trendData, dmData] = await Promise.all([
    fetch('/api/yearly-matrix').then(r => r.json()),
    fetch(`/api/summary?year=${currentYear}&month=${currentMonth}`).then(r => r.json()),
    fetch(`/api/budget-progress?year=${currentYear}&month=${currentMonth}`).then(r => r.json()),
    fetch(`/api/cumulative?year=${selectedDailyYear}&month=${selectedDailyMonth}${catParam}`).then(r => r.json()),
    fetch(`/api/monthly-trend${selectedTrendCat ? `?category=${encodeURIComponent(selectedTrendCat)}` : ''}`).then(r => r.json()),
    fetch(`/api/daily-matrix?year=${selectedDailyYear}&month=${selectedDailyMonth}`).then(r => r.json()),
  ]);
  matrixData = mData;
  renderSummary(summaryData, bpData);
  renderMatrix(mData);
  renderMonthlyProgress(bpData, trendData);
  renderMainProgressChart(cumulData);
  renderDailyMatrix(dmData);
  showSkeleton(false);
}

function showSkeleton(show) {
  document.getElementById('matrix-skeleton').style.display = show ? 'block' : 'none';
  document.getElementById('matrix-table').style.display    = show ? 'none'  : '';
}

async function refreshDetail(year, month, category) {
  if (category) {
    // カテゴリセルクリック: そのカテゴリの累計グラフ＋取引一覧
    const catUrl    = encodeURIComponent(category);
    const [cumul, trans] = await Promise.all([
      fetch(`/api/cumulative?year=${year}&month=${month}&category=${catUrl}`).then(r => r.json()),
      fetch(`/api/transactions?year=${year}&month=${month}&category=${catUrl}`).then(r => r.json()),
    ]);
    document.getElementById('detail-cat-list').style.display       = 'none';
    document.getElementById('detail-cat-chart-wrap').style.display = '';
    renderDetailCatChart(cumul, category);
    renderTransactions(trans);
  } else {
    // 月ヘッダークリック: カテゴリ一覧＋全取引
    const [bp, trans] = await Promise.all([
      fetch(`/api/budget-progress?year=${year}&month=${month}`).then(r => r.json()),
      fetch(`/api/transactions?year=${year}&month=${month}`).then(r => r.json()),
    ]);
    document.getElementById('detail-cat-list').style.display       = '';
    document.getElementById('detail-cat-chart-wrap').style.display = 'none';
    if (detailCatChart) { detailCatChart.destroy(); detailCatChart = null; }
    renderDetailBadge(bp);
    renderDetailCatList(bp);
    renderTransactions(trans);
  }
}


/* ──────────────────────────────────────
   サマリーカード（統合カード）
────────────────────────────────────── */
function renderSummary(summary, bp) {
  const expense   = summary.expense  || 0;   // 予算外も含む全体支出
  const budget    = bp.total_budget  || 0;
  const budgetExp = bp.total_actual  || 0;   // 予算内カテゴリのみ（進捗バー用）
  const remaining = budget - budgetExp;
  const pct       = budget > 0 ? Math.round(budgetExp / budget * 100) : 0;
  const elapsed   = bp.elapsed_days || 0;
  const lastDay   = bp.last_day || 30;
  const daysLeft  = lastDay - elapsed;
  const overCats  = (bp.categories || []).filter(c => c.over);

  // カウントアップ
  animateValue('cur-expense', prevSummaryVals.expense, expense, 1200);
  animateValue('cur-budget',  prevSummaryVals.budget,  budget,  1000);
  prevSummaryVals = { expense, budget, remaining, daily: 0 };

  // 残り日数サブテキスト
  const vsEl = document.getElementById('vs-last-month');
  if (vsEl) vsEl.textContent = daysLeft > 0 ? `残り ${daysLeft}日` : '今月終了';

  // 消化バー
  const barFill  = document.getElementById('sc-bar-fill');
  const barLabel = document.getElementById('sc-bar-label');
  if (barFill) {
    const barPct = Math.min(pct, 100);
    const barCol = pct > 100 ? 'var(--expense)' : pct > 80 ? 'var(--warn)' : 'var(--income)';
    barFill.style.width      = `${barPct}%`;
    barFill.style.background = barCol;
    if (barLabel) barLabel.textContent = `${pct}%`;
  }

  // 超過アラート（統合カード内）
  const overAlert = document.getElementById('sc-over-alert');
  if (overAlert) {
    if (overCats.length > 0) {
      const names = overCats.slice(0, 4).map(c =>
        `${CAT_ICONS[c.category] || ''}${c.category} ${c.pct?.toFixed(0)}%`).join('　');
      overAlert.style.display = 'flex';
      overAlert.textContent   = `⚠️ 超過: ${names}${overCats.length > 4 ? ` 他${overCats.length - 4}件` : ''}`;
    } else {
      overAlert.style.display = 'none';
    }
  }
}

/* ──────────────────────────────────────
   カウントアップアニメーション
────────────────────────────────────── */
function animateValue(id, from, to, duration = 1000, prefix = '') {
  const el = document.getElementById(id);
  if (!el) return;
  const startTime = performance.now();
  const diff = to - from;

  el.classList.add('flash');
  setTimeout(() => el.classList.remove('flash'), 400);

  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(elapsed / duration, 1);
    // イーズアウト
    const ease = 1 - Math.pow(1 - progress, 3);
    const value = Math.round(from + diff * ease);
    el.textContent = prefix + fmtYen(value);
    if (progress < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

/* ──────────────────────────────────────
   棒グラフ数値ラベル（月間トレンド用カスタムプラグイン）
────────────────────────────────────── */
const _barValuePlugin = {
  id: '_barValuePlugin',
  afterDatasetsDraw(chart) {
    const ctx = chart.ctx;
    chart.data.datasets.forEach((dataset, i) => {
      if (dataset.type !== 'bar') return;
      const meta = chart.getDatasetMeta(i);
      meta.data.forEach((bar, j) => {
        const val = dataset.data[j];
        if (val == null || val === 0) return;
        const label = Math.round(val / 10000) + '万';
        ctx.save();
        ctx.fillStyle = 'rgba(228,236,248,.8)';
        ctx.font = 'bold 8px sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'bottom';
        ctx.fillText(label, bar.x, bar.y - 2);
        ctx.restore();
      });
    });
  }
};
Chart.register(_barValuePlugin);

/* ──────────────────────────────────────
   ヒートマップ：セルカラー計算
────────────────────────────────────── */
function heatColor(pct, actual, budget, isFuture) {
  if (isFuture)     return 'rgba(255,255,255,.03)';
  if (actual === 0) return 'rgba(255,255,255,.04)';
  if (budget === 0 || pct == null) {
    // 予算外カテゴリ：青系グラデーション
    const intensity = Math.min(actual / 80000, 1);
    return `rgba(80,160,210,${(0.22 + intensity * 0.55).toFixed(2)})`;
  }
  if (pct > 100) {
    return 'rgba(255,77,109,0.75)';
  }
  if (pct > 80) {
    return 'rgba(255,140,0,0.70)';
  }
  // 予算内（80%以下）：緑（使用率に応じて濃さが変化）
  const t = pct / 100;
  return `rgba(29,233,139,${(0.20 + t * 0.42).toFixed(2)})`;
}

// セル値フォーマット（千円未満切り上げ表示）
function fmtCellCeil(v) {
  if (v <= 0) return '-';
  const k = Math.ceil(v / 1000);  // 千円単位で切り上げ
  if (k < 10) return k + '千';
  return (k / 10).toFixed(1).replace(/\.0$/, '') + '万';
}
function fmtSen(v) { return v <= 0 ? '' : fmtCellCeil(v); }

/* ──────────────────────────────────────
   マトリクス描画（過去12ヶ月ローリング）
────────────────────────────────────── */
function renderMatrix(data) {
  const thead = document.getElementById('matrix-thead');
  const tbody = document.getElementById('matrix-tbody');
  const tfoot = document.getElementById('matrix-tfoot');
  thead.innerHTML = tbody.innerHTML = tfoot.innerHTML = '';

  const cats              = data.categories           || [];
  const colLabels         = data.col_labels           || [];
  const monthList         = data.month_list           || [];
  const totBudget         = data.monthly_totals_budget    || {};
  const totNoBudget       = data.monthly_totals_no_budget || {};
  const noBudgetStartIdx  = data.no_budget_start_idx  ?? cats.findIndex(c => c.is_no_budget);
  const recentStart       = data.recent_start_idx     ?? Math.max(0, monthList.length - 12);

  const budgetCats   = cats.slice(0, noBudgetStartIdx < 0 ? cats.length : noBudgetStartIdx);
  const noBudgetCats = noBudgetStartIdx >= 0 ? cats.slice(noBudgetStartIdx) : [];

  // ── ヘッダー行
  const headTr = document.createElement('tr');
  headTr.innerHTML = `<th class="hm-cat-th">カテゴリ</th>`;
  colLabels.forEach((lbl, idx) => {
    const [y, m] = monthList[idx] || [];
    const isCur  = (y === currentYear && m === currentMonth);
    const cls    = ['month-col', isCur ? 'current-month' : ''].filter(Boolean).join(' ');
    const th     = document.createElement('th');
    th.className = cls;
    th.dataset.idx = idx; th.dataset.year = y; th.dataset.month = m;
    th.textContent = lbl;
    headTr.appendChild(th);
  });
  headTr.innerHTML += `<th class="summary-col avg-col-hdr">月均</th><th class="summary-col">合計</th>`;
  thead.appendChild(headTr);

  // ── カテゴリ行を描画するヘルパー
  function appendCatRow(cat, rowIdx, isNoBudget) {
    const tr = document.createElement('tr');
    tr.style.animationDelay = `${rowIdx * 25}ms`;
    if (isNoBudget) tr.classList.add('no-budget-row');

    const catTd = document.createElement('td');
    const icon  = CAT_ICONS[cat.category] || '📌';
    catTd.innerHTML = `
      <div class="cat-label mat-cat-click" title="クリックでこのカテゴリに絞り込み">
        <span class="cat-icon">${icon}</span>
        <span class="cat-name">${escHtml(cat.category)}</span>
      </div>`;
    catTd.querySelector('.mat-cat-click').addEventListener('click', () => selectMatrixCategory(cat.category));
    tr.appendChild(catTd);

    for (let idx = 0; idx < colLabels.length; idx++) {
      const md      = cat.months[idx] || {};
      const actual  = md.actual  || 0;
      const budget  = md.budget  || 0;
      const pct     = md.pct;
      const isFuture  = md.is_future;
      const isCurrent = md.is_current;
      const y = md.year, m = md.month;
      const prevMd = cat.months[idx > 0 ? idx - 1 : 0] || {};

      const td   = document.createElement('td');
      const cell = document.createElement('div');
      let cls = 'hm-cell';
      if (isCurrent)              cls += ' cell-current-col';
      if (isNoBudget)             cls += ' hm-cell-nobudget';
      if (pct > 100 && !isFuture) cls += ' hm-over';
      cell.className = cls;
      // 色の決定
      if (isFuture) {
        cell.style.background = 'rgba(255,255,255,.03)';
      } else if (isNoBudget && actual > 0) {
        cell.style.background = '#4a1020';
      } else if (isNoBudget && actual === 0) {
        cell.style.background = '#1c2d3e';
      } else if (actual === 0 && budget > 0) {
        // 予算内かつ支出なし → 薄緑（予算内）
        cell.style.background = 'rgba(29,233,139,.14)';
      } else {
        cell.style.background = heatColor(pct, actual, budget, isFuture);
      }

      cell.dataset.idx = idx; cell.dataset.year = y; cell.dataset.month = m;
      cell.dataset.cat = cat.category; cell.dataset.color = cat.color;
      cell.dataset.actual = actual; cell.dataset.budget = budget;
      cell.dataset.pct = pct ?? ''; cell.dataset.prevActual = prevMd.actual ?? 0;

      // セル内テキスト（千円未満切り上げ表示）
      if (!isFuture) {
        cell.innerHTML = actual > 0
          ? `<span class="hm-val">${fmtCellCeil(actual)}</span>`
          : `<span class="hm-val hm-val-dash">-</span>`;
      }
      if (!isFuture) cell.addEventListener('click', () => selectCell(y, m, cat.category));

      td.appendChild(cell);
      tr.appendChild(td);
    }

    // 直近12か月 合計
    const recentTot = cat.recent_total ?? cat.annual_total;
    const annualTd = document.createElement('td');
    annualTd.className = 'summary-data-cell annual';
    annualTd.textContent = recentTot >= 10000
      ? `${(recentTot / 10000).toFixed(0)}万` : fmtYen(recentTot);

    // 直近12か月 月均
    const avgTd = document.createElement('td');
    avgTd.className = 'summary-data-cell avg';
    const avg    = cat.recent_avg ?? cat.monthly_avg;
    const bgt    = cat.monthly_budget || 0;
    const delta  = bgt > 0 ? bgt - avg : null;
    const avgTxt = avg >= 10000 ? `${(avg / 10000).toFixed(1)}万` : (avg > 0 ? fmtYen(avg) : '―');
    let deltaHtml = '';
    if (delta !== null && avg > 0) {
      const cls = delta >= 0 ? 'avg-delta-ok' : 'avg-delta-over';
      const sym = delta >= 0 ? '▼' : '▲';
      const abs = Math.abs(delta);
      const dTxt = abs >= 10000 ? `${(abs/10000).toFixed(1)}万` : fmtYen(abs);
      deltaHtml = `<div class="avg-delta ${cls}">${sym}${dTxt}</div>`;
    }
    avgTd.innerHTML = `<div class="avg-main">${avgTxt}</div>${deltaHtml}`;

    // 列順：月均 → 合計
    tr.appendChild(avgTd);
    tr.appendChild(annualTd);

    tbody.appendChild(tr);
  }

  // ── フッター行ヘルパー（grand は直近12か月のみ集計）
  function makeFootRow(label, totals, catList, cls = '') {
    const tr = document.createElement('tr');
    if (cls) tr.className = cls;
    let grand = 0;
    tr.innerHTML = `<td>${label}</td>`;
    const nCols = Object.keys(totals).length;
    for (let idx = 0; idx < nCols; idx++) {
      const t = totals[idx] || 0;
      if (idx >= recentStart) grand += t;   // 直近12か月のみ合算
      const v = t >= 10000 ? `${(t / 10000).toFixed(0)}万` : (t > 0 ? fmtYen(t) : '―');
      tr.innerHTML += `<td>${v}</td>`;
    }
    const g = grand >= 10000 ? `${(grand / 10000).toFixed(0)}万` : fmtYen(grand);
    // 月均も直近12か月平均の合計
    const avgSum = catList.reduce((s, c) => s + (c.recent_avg ?? c.monthly_avg ?? 0), 0);
    const avgTxt = avgSum >= 10000 ? `${(avgSum/10000).toFixed(0)}万` : (avgSum > 0 ? fmtYen(avgSum) : '―');
    // 列順：月均 → 合計
    tr.innerHTML += `<td class="avg-foot">${avgTxt}</td><td>${g}</td>`;
    return tr;
  }

  // ── 予算内カテゴリ → 予算内合計（tbody内）
  budgetCats.forEach((cat, i) => appendCatRow(cat, i, false));
  tbody.appendChild(makeFootRow('予算内合計', totBudget, budgetCats, 'foot-budget'));

  // ── 仕切り行 + 予算外カテゴリ → 全体合計（tbody内）
  if (noBudgetCats.length > 0) {
    const divTr = document.createElement('tr');
    divTr.className = 'matrix-section-divider';
    divTr.innerHTML = `
      <td colspan="${colLabels.length + 3}">
        <span class="matrix-section-label">予算外（集計のみ）</span>
      </td>`;
    tbody.appendChild(divTr);
    noBudgetCats.forEach((cat, i) => appendCatRow(cat, budgetCats.length + i + 1, true));
  }
  const totAll = {};
  for (let idx = 0; idx < colLabels.length; idx++) {
    totAll[idx] = (totBudget[idx] || 0) + (totNoBudget[idx] || 0);
  }
  tbody.appendChild(makeFootRow('全体合計', totAll, [...budgetCats, ...noBudgetCats], 'foot-grand'));
  tfoot.innerHTML = '';

  // 月ヘッダークリック
  document.querySelectorAll('.month-col').forEach(th => {
    th.addEventListener('click', () => selectMonth(
      parseInt(th.dataset.year), parseInt(th.dataset.month)
    ));
  });
}

/* ──────────────────────────────────────
   スパークライン（SVG）
────────────────────────────────────── */
function makeSvgSparkline(values, color) {
  const W = 88, H = 26;
  const max = Math.max(...values, 1);
  const nonZero = values.filter(v => v > 0);
  const avg = nonZero.length > 0 ? nonZero.reduce((a,b) => a+b, 0) / nonZero.length : 0;
  const avgY = H - (avg / max) * (H - 4) - 2;

  const pts = values.map((v, i) => {
    const x = (i / 11) * W;
    const y = v > 0 ? H - (v / max) * (H - 4) - 2 : H;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');

  const lastNonZeroIdx = values.map((v,i) => v > 0 ? i : -1).filter(i => i >= 0).pop() ?? -1;
  const dotX = lastNonZeroIdx >= 0 ? (lastNonZeroIdx / 11) * W : -10;
  const dotY = lastNonZeroIdx >= 0 ? H - (values[lastNonZeroIdx] / max) * (H - 4) - 2 : -10;

  return `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="overflow:visible">
    <line x1="0" y1="${avgY.toFixed(1)}" x2="${W}" y2="${avgY.toFixed(1)}"
          stroke="rgba(255,255,255,.06)" stroke-width="1" stroke-dasharray="3,3"/>
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5"
              stroke-linejoin="round" stroke-linecap="round" opacity=".8"/>
    <circle cx="${dotX.toFixed(1)}" cy="${dotY.toFixed(1)}" r="2.5" fill="${color}"
            style="filter:drop-shadow(0 0 3px ${color})"/>
  </svg>`;
}

/* ──────────────────────────────────────
   ツールチップ
────────────────────────────────────── */
function showTooltip(e, cellEl, cat, month) {
  const tt = document.getElementById('matrix-tooltip');
  const actual = parseInt(cellEl.dataset.actual) || 0;
  const budget = parseInt(cellEl.dataset.budget) || 0;
  const pct    = cellEl.dataset.pct !== '' ? parseFloat(cellEl.dataset.pct) : null;
  const prev   = parseInt(cellEl.dataset.prevActual) || 0;
  const color  = cellEl.dataset.color;

  document.getElementById('tt-dot').style.background = color;
  document.getElementById('tt-dot').style.boxShadow  = `0 0 5px ${color}`;
  document.getElementById('tt-cat').textContent   = cat.category;
  document.getElementById('tt-month').textContent = MONTH_NAMES[month - 1];
  document.getElementById('tt-actual').textContent = fmtYen(actual);

  const budgetRow = document.getElementById('tt-budget-row');
  const diffRow   = document.getElementById('tt-diff-row');
  const pctRow    = document.getElementById('tt-pct-row');
  const prevRow   = document.getElementById('tt-prev-row');

  if (budget > 0) {
    budgetRow.style.display = '';
    diffRow.style.display   = '';
    pctRow.style.display    = '';
    document.getElementById('tt-budget').textContent = fmtYen(budget);
    const diff = budget - actual;
    const diffEl = document.getElementById('tt-diff');
    diffEl.textContent = (diff >= 0 ? '+' : '') + fmtYen(diff);
    diffEl.style.color = diff >= 0 ? 'var(--income)' : 'var(--expense)';
    const pctEl = document.getElementById('tt-pct');
    pctEl.textContent = pct !== null ? `${pct.toFixed(1)}%` : '0%';
    pctEl.style.color = pct > 100 ? 'var(--expense)' : pct > 80 ? 'var(--warn)' : 'var(--income)';
  } else {
    budgetRow.style.display = diffRow.style.display = pctRow.style.display = 'none';
  }

  if (prev > 0 && actual > 0 && month > 1) {
    prevRow.style.display = '';
    const chg = ((actual - prev) / prev * 100).toFixed(1);
    const prevEl = document.getElementById('tt-prev');
    prevEl.textContent = (chg > 0 ? '+' : '') + chg + '%';
    prevEl.style.color = chg > 0 ? 'var(--expense)' : 'var(--income)';
  } else {
    prevRow.style.display = 'none';
  }

  tt.style.display = 'block';
  moveTooltip(e);
}

function moveTooltip(e) {
  const tt = document.getElementById('matrix-tooltip');
  const margin = 14;
  let x = e.clientX + margin;
  let y = e.clientY + margin;
  // 画面端補正
  const rect = tt.getBoundingClientRect();
  if (x + rect.width + margin > window.innerWidth)  x = e.clientX - rect.width - margin;
  if (y + rect.height + margin > window.innerHeight) y = e.clientY - rect.height - margin;
  tt.style.left = x + 'px';
  tt.style.top  = y + 'px';
}

function hideTooltip() {
  document.getElementById('matrix-tooltip').style.display = 'none';
}

/* ──────────────────────────────────────
   月選択 → ドリルダウン
────────────────────────────────────── */
async function selectMonth(year, month) {
  selectedMonth    = month;
  selectedCategory = null;

  document.querySelectorAll('.month-col').forEach(th => {
    th.classList.toggle('selected-month', parseInt(th.dataset.month) === month);
  });
  document.querySelectorAll('.hm-cell').forEach(c => {
    c.classList.toggle('cell-selected-col', parseInt(c.dataset.month) === month);
    c.classList.remove('cell-selected');
  });

  const section = document.getElementById('detail-section');
  section.style.display = 'flex';
  document.getElementById('detail-month-title').textContent =
    `${year}年 ${MONTH_NAMES[month - 1]} の集計`;

  section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  await refreshDetail(year, month, null);
}

async function selectCell(year, month, category) {
  selectedMonth    = month;
  selectedCategory = category;

  document.querySelectorAll('.month-col').forEach(th => {
    th.classList.toggle('selected-month', parseInt(th.dataset.month) === month);
  });
  document.querySelectorAll('.hm-cell').forEach(c => {
    c.classList.toggle('cell-selected-col', parseInt(c.dataset.month) === month);
    c.classList.remove('cell-selected');
  });
  // 選択セルをハイライト
  document.querySelectorAll(`.hm-cell[data-month="${month}"][data-cat="${CSS.escape(category)}"]`).forEach(c => {
    c.classList.add('cell-selected');
  });

  const section = document.getElementById('detail-section');
  section.style.display = 'flex';
  const icon = CAT_ICONS[category] || '📂';
  document.getElementById('detail-month-title').textContent =
    `${icon} ${category} — ${year}年 ${MONTH_NAMES[month - 1]}`;

  section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  await refreshDetail(year, month, category);
}

/* ──────────────────────────────────────
   詳細バッジ
────────────────────────────────────── */
function renderDetailBadge(bp) {
  const badge = document.getElementById('detail-status-badge');
  const total = bp.total_actual || 0;
  const budget = bp.total_budget || 0;
  if (budget === 0) { badge.textContent = ''; badge.className = 'detail-badge'; return; }
  const pct = Math.round(total / budget * 100);
  if (pct > 100) {
    badge.textContent = `予算超過 ${pct}%`; badge.className = 'detail-badge over';
  } else if (pct > 80) {
    badge.textContent = `消化率 ${pct}%`;  badge.className = 'detail-badge warn';
  } else {
    badge.textContent = `消化率 ${pct}%`;  badge.className = 'detail-badge ok';
  }
}

/* ──────────────────────────────────────
   月クリック: カテゴリ一覧（シンプル）
────────────────────────────────────── */
function renderDetailCatList(bp) {
  const container = document.getElementById('detail-cat-list');
  container.innerHTML = '';
  const cats = (bp.categories || []).sort((a, b) => b.actual - a.actual);
  if (cats.length === 0) {
    container.innerHTML = '<div class="detail-empty">データなし</div>';
    return;
  }
  const total = cats.reduce((s, c) => s + (c.actual || 0), 0);
  cats.forEach(cat => {
    const pct     = cat.pct;
    const hasBgt  = cat.budget > 0;
    const barW    = hasBgt ? Math.min(pct || 0, 100) : 0;
    const barCol  = pct > 100 ? 'var(--expense)' : pct > 80 ? 'var(--warn)' : cat.color;
    const icon    = CAT_ICONS[cat.category] || '📂';
    const sharePct = total > 0 ? (cat.actual / total * 100).toFixed(0) : '0';
    const row = document.createElement('div');
    row.className = `dcl-row${pct > 100 ? ' dcl-over' : pct > 80 ? ' dcl-warn' : ''}`;
    row.innerHTML = `
      <div class="dcl-left">
        <span class="dcl-icon">${icon}</span>
        <span class="dcl-name">${escHtml(cat.category)}</span>
      </div>
      <div class="dcl-mid">
        <div class="dcl-bar-wrap">
          <div class="dcl-bar-fill" style="width:${barW}%;background:${barCol}"></div>
        </div>
        ${hasBgt ? `<span class="dcl-pct ${pct > 100 ? 'over' : pct > 80 ? 'warn' : 'ok'}">${pct?.toFixed(0) ?? 0}%</span>` : ''}
      </div>
      <div class="dcl-right">
        <span class="dcl-actual" ${cat.actual < 0 ? 'style="color:var(--income)"' : ''}>${cat.actual < 0 ? '+' : ''}${fmtYen(cat.actual)}</span>
        ${hasBgt ? `<span class="dcl-budget">/ ${fmtYen(cat.budget)}</span>` : `<span class="dcl-share">(${sharePct}%)</span>`}
      </div>`;
    container.appendChild(row);
  });
}

/* ──────────────────────────────────────
   カテゴリセルクリック: 累計折れ線グラフ
────────────────────────────────────── */
function renderDetailCatChart(data, category) {
  const title   = document.getElementById('detail-cat-chart-title');
  const statsEl = document.getElementById('detail-cat-stats');
  if (title) title.textContent = `${CAT_ICONS[category] || '📂'} ${category} — 日次累計`;

  const todayIdx = data.today_idx ?? (data.actual.length - 1);
  const trimmed  = (data.actual || []).map((v, i) => i <= todayIdx ? v : null);
  const lastActual = trimmed.filter(v => v !== null).pop() || 0;
  const bgt        = data.budget_total || 0;

  if (statsEl) {
    const diff = bgt - lastActual;
    statsEl.innerHTML = `
      <div class="cumul-stat">実績: <span style="color:#ff4d6d;font-weight:700">${fmtYen(lastActual)}</span></div>
      ${bgt > 0 ? `<div class="cumul-stat">予算: <span>${fmtYen(bgt)}</span></div>` : ''}
      ${bgt > 0 ? `<div class="cumul-stat" style="color:${diff >= 0 ? 'var(--income)' : 'var(--expense)'}">
        差額: <span>${diff >= 0 ? '▼' : '▲'}${fmtYen(Math.abs(diff))}</span></div>` : ''}`;
  }

  const datasets = [{
    label: '実績（累計）',
    data: trimmed,
    borderColor: '#ff4d6d',
    backgroundColor: ctx2 => {
      const g = ctx2.chart.ctx.createLinearGradient(0, 0, 0, 220);
      g.addColorStop(0, 'rgba(255,77,109,.22)');
      g.addColorStop(1, 'rgba(255,77,109,.01)');
      return g;
    },
    fill: true, tension: 0.35,
    pointRadius: 2, pointHoverRadius: 6, borderWidth: 2.5, spanGaps: false,
  }];
  if ((data.budget_line || []).length > 0) {
    datasets.push({
      label: '予算ライン',
      data: data.budget_line,
      borderColor: 'rgba(91,127,255,.7)',
      borderDash: [7, 4], tension: 0,
      pointRadius: 0, borderWidth: 2, fill: false,
    });
  }
  if ((data.forecast_line || []).some(v => v !== null)) {
    datasets.push({
      label: '着地見込み',
      data: data.forecast_line,
      borderColor: 'rgba(255,197,61,.85)',
      borderDash: [5, 4], tension: 0.2,
      pointRadius: 0, borderWidth: 2, fill: false, spanGaps: false,
    });
  }

  const canvas = document.getElementById('detail-cat-chart');
  if (detailCatChart) { detailCatChart.destroy(); detailCatChart = null; }
  detailCatChart = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels: data.labels || [], datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 700, easing: 'easeOutCubic' },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#7a9bc4', font: { size: 11 }, boxWidth: 14, padding: 14 } },
        tooltip: {
          backgroundColor: 'rgba(10,16,32,.96)',
          borderColor: '#2a3d5c', borderWidth: 1,
          titleColor: '#e4ecf8', bodyColor: '#9aafcc',
          callbacks: { label: c => c.raw != null ? ` ${c.dataset.label}: ${fmtYen(c.raw)}` : null }
        }
      },
      scales: {
        x: { ticks: { color: '#5b6e8e', font: { size: 9 } }, grid: { color: 'rgba(30,45,70,.5)' } },
        y: {
          ticks: { color: '#5b6e8e', callback: v => v >= 10000 ? `¥${(v/10000).toFixed(0)}万` : `¥${v.toLocaleString()}` },
          grid: { color: 'rgba(30,45,70,.5)' }
        }
      }
    }
  });
}

/* ──────────────────────────────────────
   月間トレンド：全期間データ保持 + スライドウィンドウ
────────────────────────────────────── */
let _trendAllData = null;

function renderMonthlyProgress(bp, trend) {
  if (!trend) return;
  _trendAllData = trend;
  renderAllTrend();
}

function renderAllTrend() {
  if (!_trendAllData) return;
  const { labels, actuals, budgets, avg_actual, today_idx, month_list } = _trendAllData;

  // 今月以降はnull
  const actualDisplay = actuals.map((v, i) =>
    (today_idx >= 0 && i > today_idx) ? null : (v || null)
  );

  // バー色: 緑(予算内) / オレンジ(平均内) / 赤(平均超)
  const budget0 = budgets[0] || 0;
  const barColors = actualDisplay.map(v => {
    if (v === null) return 'transparent';
    if (budget0 > 0 && v <= budget0)                     return 'rgba(29,233,139,.85)';
    if (avg_actual > 0 && v <= avg_actual)                return 'rgba(255,197,61,.85)';
    return 'rgba(255,77,109,.85)';
  });
  const borderColors = actualDisplay.map(v => {
    if (v === null) return 'transparent';
    if (budget0 > 0 && v <= budget0)                     return '#1de98b';
    if (avg_actual > 0 && v <= avg_actual)                return '#ffc53d';
    return '#ff4d6d';
  });

  const datasets = [
    {
      type: 'line', label: '予算', data: budgets,
      borderColor: 'rgba(91,127,255,.7)', borderDash: [6, 4],
      borderWidth: 2, pointRadius: 0, fill: false, order: 2, tension: 0,
    },
    {
      type: 'bar', label: '実績', data: actualDisplay,
      backgroundColor: barColors, borderColor: borderColors,
      borderWidth: 1, borderRadius: 3, order: 1,
    },
    {
      type: 'line', label: '平均',
      data: labels.map(() => avg_actual > 0 ? avg_actual : null),
      borderColor: 'rgba(255,197,61,.8)', borderDash: [6, 4],
      borderWidth: 2, pointRadius: 0, fill: false, order: 0,
    },
  ];

  // 期間ラベル
  const statsEl = document.getElementById('monthly-chart-stats');
  if (statsEl && month_list && month_list.length >= 2) {
    const sm = month_list[0], em = month_list[month_list.length - 1];
    statsEl.innerHTML = `<div class="cumul-stat" style="color:var(--muted);font-size:.7rem">${sm[0]}年${sm[1]}月〜${em[0]}年${em[1]}月</div>`;
  }

  // スクロールコンテナのサイズ計算
  const scrollWrap  = document.getElementById('trend-scroll-wrap');
  const scrollInner = document.getElementById('trend-scroll-inner');
  const outerW  = scrollWrap?.clientWidth  || 400;
  const perBarPx = Math.max(Math.floor(outerW / 12), 34);
  const totalW  = perBarPx * labels.length;
  const chartH  = 240;

  if (scrollInner) {
    scrollInner.style.width  = totalW + 'px';
    scrollInner.style.height = chartH + 'px';
  }

  const canvas = document.getElementById('monthly-bar-chart');
  if (!canvas) return;
  canvas.width  = totalW;
  canvas.height = chartH;
  canvas.style.width  = totalW + 'px';
  canvas.style.height = chartH + 'px';

  if (monthlyBarChart) {
    monthlyBarChart.data.labels   = labels;
    monthlyBarChart.data.datasets = datasets;
    monthlyBarChart.update('none');
    // 最新月（右端）にスクロール
    if (scrollWrap) setTimeout(() => { scrollWrap.scrollLeft = scrollWrap.scrollWidth; }, 50);
    return;
  }

  monthlyBarChart = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: false,
      maintainAspectRatio: false,
      animation: { duration: 300, easing: 'easeOutCubic' },
      interaction: { mode: 'index', intersect: false },
      layout: { padding: { top: 18, bottom: 4 } },
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false },
      },
      scales: {
        x: {
          ticks: { color: '#5b6e8e', font: { size: 9 }, maxRotation: 0, minRotation: 0, autoSkip: false },
          grid: { color: 'rgba(30,45,70,.3)' }
        },
        y: {
          ticks: {
            color: '#5b6e8e', font: { size: 10 },
            callback: v => v >= 10000 ? (v/10000).toFixed(0) + '万' : v.toLocaleString()
          },
          grid: { color: 'rgba(30,45,70,.4)' }
        }
      }
    }
  });
  // 最新月（右端）にスクロール
  if (scrollWrap) setTimeout(() => { scrollWrap.scrollLeft = scrollWrap.scrollWidth; }, 100);
}

/* ──────────────────────────────────────
   メインダッシュボード：今月の進捗ライングラフ
────────────────────────────────────── */
function renderMainProgressChart(data) {
  const canvas = document.getElementById('main-cumulative-chart');
  if (!canvas) return;

  // タイトル更新
  const titleEl = document.getElementById('progress-title');
  if (titleEl) {
    const catPart = selectedDailyCat ? ` — ${CAT_ICONS[selectedDailyCat] || ''}${selectedDailyCat}` : '';
    titleEl.textContent = `${selectedDailyYear}年${selectedDailyMonth}月の日次進捗${catPart}`;
  }

  const todayIdx = data.today_idx ?? (data.actual.length - 1);
  const trimmed  = (data.actual || []).map((v, i) => i <= todayIdx ? v : null);

  const datasets = [{
    label: '実績（累計）',
    data: trimmed,
    borderColor: '#ff4d6d',
    backgroundColor: (ctx2) => {
      const g = ctx2.chart.ctx.createLinearGradient(0, 0, 0, 240);
      g.addColorStop(0, 'rgba(255,77,109,.25)');
      g.addColorStop(1, 'rgba(255,77,109,.02)');
      return g;
    },
    fill: true, tension: 0.35,
    pointRadius: 2, pointHoverRadius: 6, borderWidth: 2.5, spanGaps: false,
  }];

  if ((data.budget_line || []).length > 0) {
    datasets.push({
      label: '予算ライン',
      data: data.budget_line,
      borderColor: 'rgba(91,127,255,.7)',
      borderDash: [8, 5], tension: 0,
      pointRadius: 0, borderWidth: 2, fill: false,
    });
  }

  // 着地見込みバンド（楽観〜継続の幅で表示）
  const hasForecast = (data.forecast_low || []).some(v => v !== null);
  if (hasForecast) {
    datasets.push({
      label: '着地（楽観）',
      data: data.forecast_low,
      borderColor: 'rgba(255,197,61,.55)',
      borderDash: [4, 3], tension: 0.2,
      pointRadius: 0, borderWidth: 1.5,
      fill: '+1',
      backgroundColor: 'rgba(255,197,61,.08)',
      spanGaps: false, order: 0,
    });
    datasets.push({
      label: '着地（継続）',
      data: data.forecast_high,
      borderColor: 'rgba(255,197,61,.85)',
      borderDash: [4, 3], tension: 0.2,
      pointRadius: 0, borderWidth: 1.5,
      fill: false,
      spanGaps: false, order: 0,
    });
  }

  const chartOpts = {
    type: 'line',
    data: { labels: data.labels || [], datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 800, easing: 'easeOutCubic' },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      },
      onClick: (_ev, elements) => {
        if (!elements || elements.length === 0) return;
        const idx = elements[0].index;
        const day = idx + 1;
        openCellPopup(selectedDailyYear, selectedDailyMonth, day, selectedDailyCat || null, _ev.native);
      },
      scales: {
        x: { ticks: { color: '#5b6e8e', font: { size: 10 }, maxTicksLimit: 10 }, grid: { color: 'rgba(30,45,70,.4)' } },
        y: {
          ticks: { color: '#5b6e8e', font: { size: 10 }, callback: v => v >= 10000 ? `${(v/10000).toFixed(0)}万` : v.toLocaleString() },
          grid: { color: 'rgba(30,45,70,.4)' }
        }
      }
    }
  };

  if (mainCumulChart) {
    mainCumulChart.data.labels   = data.labels;
    mainCumulChart.data.datasets = datasets;
    mainCumulChart.update('none');
    return;
  }
  mainCumulChart = new Chart(canvas.getContext('2d'), chartOpts);
  canvas.style.cursor = 'pointer';
}


/* ──────────────────────────────────────
   日次マトリクス描画
────────────────────────────────────── */
function renderDailyMatrix(data) {
  const skelEl = document.getElementById('daily-matrix-skeleton');
  const wrap   = document.getElementById('daily-matrix-table-wrap');
  if (!wrap || !data) return;

  const { categories, daily_totals, last_day, today_day } = data;
  if (!categories || categories.length === 0) {
    wrap.innerHTML = '<div style="padding:16px;color:var(--muted);text-align:center">データなし</div>';
    if (skelEl) skelEl.style.display = 'none';
    wrap.style.display = '';
    return;
  }

  // 経過日数（今月中 = today_day、過去月 = last_day）
  const elapsed = today_day || last_day;

  // 日均フォーマット（1日あたりの金額向け）
  function fmtDailyVal(v) {
    if (!v || v === 0) return '―';
    const a = Math.abs(Math.round(v));
    if (a >= 10000) return `${(a / 10000).toFixed(1)}万`;
    if (a >= 1000)  return `${(a / 1000).toFixed(1)}千`;
    return `¥${a.toLocaleString()}`;
  }

  // 日均セル HTML 生成（月額予算から日あたり予算を算出して差額表示）
  function makeDailyAvgCell(total, monthlyBudget) {
    if (!total) return `<td class="summary-data-cell avg"><div class="avg-main">―</div></td>`;
    const dailyAvg = elapsed > 0 ? total / elapsed : 0;
    const dailyBgt = monthlyBudget > 0 ? monthlyBudget / last_day : 0;
    const avgTxt   = fmtDailyVal(dailyAvg);
    let deltaHtml  = '';
    if (dailyBgt > 0 && total > 0) {
      const delta = dailyBgt - dailyAvg;
      const cls   = delta >= 0 ? 'avg-delta-ok' : 'avg-delta-over';
      const sym   = delta >= 0 ? '▼' : '▲';
      deltaHtml = `<div class="avg-delta ${cls}">${sym}${fmtDailyVal(Math.abs(delta))}</div>`;
    }
    return `<td class="summary-data-cell avg"><div class="avg-main" style="font-size:.72rem">${avgTxt}</div>${deltaHtml}</td>`;
  }

  // -- ヘッダー（日均列を合計の左に追加）
  let html = '<table class="matrix-table dm-table"><thead><tr>';
  html += '<th class="hm-cat-th">カテゴリ</th>';
  for (let d = 1; d <= last_day; d++) {
    const isToday = (d === today_day);
    html += `<th class="dm-day-th${isToday ? ' current-month' : ''}">${d}日</th>`;
  }
  html += '<th class="summary-col avg-col-hdr">日均</th><th class="summary-col">計</th></tr></thead><tbody>';

  const budgetCats   = categories.filter(c => !c.is_no_budget);
  const noBudgetCats = categories.filter(c =>  c.is_no_budget);

  // 日別予算内合計・全体合計を事前計算
  const dayBudgetTotals = {};
  const dayGrandTotals  = {};
  for (let d = 1; d <= last_day; d++) { dayBudgetTotals[d] = 0; dayGrandTotals[d] = 0; }
  budgetCats.forEach(cat => {
    for (let d = 1; d <= last_day; d++) dayBudgetTotals[d] += (cat.days[d]?.actual || 0);
  });
  noBudgetCats.forEach(cat => {
    for (let d = 1; d <= last_day; d++) dayGrandTotals[d] += (cat.days[d]?.actual || 0);
  });
  for (let d = 1; d <= last_day; d++) dayGrandTotals[d] += dayBudgetTotals[d];

  const renderCatRow = (cat, rowIdx) => {
    const icon = CAT_ICONS[cat.category] || '📌';
    let row = `<tr class="${cat.is_no_budget ? 'no-budget-row' : ''}" style="animation-delay:${rowIdx*20}ms">`;
    row += `<td><div class="cat-label mat-cat-click dm-cat-click" data-cat="${escHtml(cat.category)}" title="クリックでこのカテゴリに絞り込み">
      <span class="cat-icon">${icon}</span><span class="cat-name">${escHtml(cat.category)}</span>
    </div></td>`;
    for (let d = 1; d <= last_day; d++) {
      const md       = cat.days[d] || {};
      const actual   = md.actual || 0;
      const pct      = md.pct;
      const isFuture = md.is_future;
      const isToday  = md.is_today;
      let bg;
      if (isFuture) {
        bg = 'rgba(255,255,255,.03)';
      } else if (cat.is_no_budget) {
        bg = actual > 0 ? '#4a1020' : '#1c2d3e';
      } else {
        bg = heatColor(pct, actual, cat.monthly_budget || 1, isFuture);
      }
      const valHtml = actual > 0
        ? `<span class="dm-val">${fmtCellCeil(actual)}</span>`
        : `<span class="dm-val hm-val-dash">-</span>`;
      const extraCls  = actual > 0 ? ' dm-cell-clickable' : '';
      const extraData = actual > 0 ? ` data-day="${d}" data-cat="${escHtml(cat.category)}"` : '';
      row += `<td class="dm-td"><div class="hm-cell dm-cell${isToday ? ' cell-current-col' : ''}${extraCls}"${extraData} style="background:${bg}">${valHtml}</div></td>`;
    }
    // 日均列（月額予算から日あたり予算を算出）
    row += makeDailyAvgCell(cat.total, cat.is_no_budget ? 0 : (cat.monthly_budget || 0));
    row += `<td class="summary-data-cell annual">${fmtSen(cat.total)}</td></tr>`;
    return row;
  };

  // 予算内カテゴリ行
  budgetCats.forEach((cat, i) => { html += renderCatRow(cat, i); });

  // 予算内合計行
  let budgetTotal = 0;
  let budgetMonthlySum = budgetCats.reduce((s, c) => s + (c.monthly_budget || 0), 0);
  html += `<tr class="foot-budget"><td>予算内合計</td>`;
  for (let d = 1; d <= last_day; d++) {
    const t = dayBudgetTotals[d];
    budgetTotal += t;
    const isToday = (d === today_day);
    const v = t > 0 ? `<span class="dm-val">${fmtCellCeil(t)}</span>` : `<span class="dm-val hm-val-dash">-</span>`;
    html += `<td class="dm-td"><div class="hm-cell dm-cell${isToday ? ' cell-current-col' : ''}" style="background:#0d2b1a">${v}</div></td>`;
  }
  html += makeDailyAvgCell(budgetTotal, budgetMonthlySum);
  html += `<td class="summary-data-cell annual">${fmtSen(budgetTotal)}</td></tr>`;

  // 予算外カテゴリ行
  if (noBudgetCats.length > 0) {
    html += `<tr class="matrix-section-divider"><td colspan="${last_day + 3}">
      <span class="matrix-section-label">予算外（集計のみ）</span></td></tr>`;
    noBudgetCats.forEach((cat, i) => { html += renderCatRow(cat, budgetCats.length + i); });
  }

  // 全体合計行
  let grandTotal = 0;
  html += `<tr class="foot-grand"><td>全体合計</td>`;
  for (let d = 1; d <= last_day; d++) {
    const t = dayGrandTotals[d];
    grandTotal += t;
    const isToday = (d === today_day);
    const v = t > 0 ? `<span class="dm-val">${fmtCellCeil(t)}</span>` : `<span class="dm-val hm-val-dash">-</span>`;
    html += `<td class="dm-td"><div class="hm-cell dm-cell${isToday ? ' cell-current-col' : ''}" style="background:#0c1d3a">${v}</div></td>`;
  }
  html += makeDailyAvgCell(grandTotal, budgetMonthlySum);
  html += `<td class="summary-data-cell annual">${fmtSen(grandTotal)}</td></tr>`;
  html += `</tbody></table>`;

  wrap.innerHTML = html;
  // カテゴリラベルのクリックで両チャート連動
  wrap.querySelectorAll('.dm-cat-click').forEach(el => {
    el.style.cursor = 'pointer';
    el.addEventListener('click', () => selectMatrixCategory(el.dataset.cat));
  });
  // 金額セルのクリックで取引ポップアップ
  wrap.querySelectorAll('.dm-cell-clickable').forEach(el => {
    el.addEventListener('click', e => {
      e.stopPropagation();
      openCellPopup(selectedDailyYear, selectedDailyMonth, parseInt(el.dataset.day), el.dataset.cat, e);
    });
  });
  if (skelEl) skelEl.style.display = 'none';
  wrap.style.display = '';
}

/* ──────────────────────────────────────
   日次セル取引ポップアップ
────────────────────────────────────── */
async function openCellPopup(year, month, day, catName, event) {
  const popup = document.getElementById('cell-detail-popup');
  if (!popup) return;

  // タイトル
  document.getElementById('cell-popup-title').textContent =
    catName ? `${month}月${day}日 ${catName}` : `${month}月${day}日 全カテゴリ`;

  const body = document.getElementById('cell-popup-body');
  body.innerHTML = '<div class="popup-empty">読込中…</div>';
  positionPopup(popup, event);
  popup.classList.remove('hidden');

  // 取引取得（月＋カテゴリ）
  try {
    const catQ = catName ? `&category=${encodeURIComponent(catName)}` : '';
    const txns = await fetch(
      `/api/transactions?year=${year}&month=${month}${catQ}`
    ).then(r => r.json());

    const dayStr = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
    // income（割引・返金含む）も表示
    const dayTxns = txns.filter(t => t.date === dayStr);

    if (dayTxns.length === 0) {
      body.innerHTML = '<div class="popup-empty">取引なし</div>';
    } else {
      body.innerHTML = dayTxns.map(t => {
        const isRefund = t.type === 'expense' && t.amount < 0;
        const isPositive = t.type === 'income' || isRefund;
        const sign = isPositive ? '+' : '-';
        const style = isPositive ? 'style="color:var(--income)"' : '';
        return `
          <div class="popup-txn-row">
            <span class="popup-memo">${escHtml(t.memo || '—')}</span>
            <span class="popup-amt" ${style}>${sign}¥${Math.abs(Number(t.amount)).toLocaleString()}</span>
          </div>`;
      }).join('');
    }
  } catch {
    body.innerHTML = '<div class="popup-empty">取得エラー</div>';
  }
}

function positionPopup(popup, event) {
  popup.style.display = '';
  const pw = popup.offsetWidth  || 260;
  const ph = popup.offsetHeight || 180;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const cx = event.clientX;
  const cy = event.clientY;
  let left = cx + 8;
  let top  = cy + 8;
  if (left + pw > vw - 8) left = cx - pw - 8;
  if (top  + ph > vh - 8) top  = cy - ph - 8;
  popup.style.left = `${Math.max(8, left)}px`;
  popup.style.top  = `${Math.max(8, top)}px`;
}

function closeCellPopup() {
  const popup = document.getElementById('cell-detail-popup');
  if (popup) popup.classList.add('hidden');
}

/* ──────────────────────────────────────
   取引一覧（カテゴリ列付き）
────────────────────────────────────── */
function renderTransactions(transactions) {
  const tbody = document.getElementById('trans-tbody');
  const empty = document.getElementById('trans-empty');
  document.getElementById('trans-count').textContent = `${transactions.length}件`;
  tbody.innerHTML = '';

  const colorMap = Object.fromEntries(allCategories.map(c => [c.name, c.color]));

  if (transactions.length === 0) { empty.style.display = 'block'; return; }
  empty.style.display = 'none';

  transactions.forEach((t, i) => {
    const color = colorMap[t.category] || '#94a3b8';
    const isRefund = t.type === 'expense' && t.amount < 0;
    const amtSign  = (t.type === 'income' || isRefund) ? '+' : '-';
    const amtCls   = (t.type === 'income' || isRefund) ? 'income' : 'expense';
    const tr = document.createElement('tr');
    tr.style.animationDelay = `${i * 20}ms`;
    const srcLabel = t.source === 'zaim' ? '<span class="source-badge zaim">Zaim</span>'
                   : t.source === 'mf'   ? '<span class="source-badge mf">MF</span>'
                   : '';
    tr.innerHTML = `
      <td style="color:var(--muted)">${fmtDate(t.date)}</td>
      <td><span class="chip"><span class="chip-dot" style="background:${color}"></span>${escHtml(t.category)}</span></td>
      <td style="color:var(--muted);font-size:.75rem">${escHtml(t.memo || '—')}</td>
      <td>${srcLabel}</td>
      <td class="tr amt ${amtCls}">${amtSign}${fmtYen(t.amount)}</td>`;
    tbody.appendChild(tr);
  });
}


/* ──────────────────────────────────────
   月間トレンド：カテゴリ選択
────────────────────────────────────── */
function initTrendCatSelect() {
  const sel = document.getElementById('trend-cat-select');
  if (!sel) return;
  sel.innerHTML = '<option value="">合計（全カテゴリ）</option>';
  allCategories.filter(c => c.type === 'expense').forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.name;
    opt.textContent = (CAT_ICONS[c.name] ? CAT_ICONS[c.name] + ' ' : '') + c.name;
    sel.appendChild(opt);
  });
  sel.onchange = () => { selectedTrendCat = sel.value || null; updateTrendCatLabel(); refreshTrendChart(); };
}

function updateTrendCatLabel() {
  const el = document.getElementById('trend-cat-label');
  if (!el) return;
  el.textContent = selectedTrendCat
    ? (CAT_ICONS[selectedTrendCat] ? CAT_ICONS[selectedTrendCat] + ' ' : '') + selectedTrendCat
    : '合計（全カテゴリ）';
  const sel = document.getElementById('trend-cat-select');
  if (sel) sel.value = selectedTrendCat || '';
}

async function refreshTrendChart() {
  const catParam = selectedTrendCat ? `?category=${encodeURIComponent(selectedTrendCat)}` : '';
  const trendData = await fetch(`/api/monthly-trend${catParam}`).then(r => r.json());
  renderMonthlyProgress(null, trendData);
}

/* ──────────────────────────────────────
   マトリクス カテゴリ行クリック → 両チャート連動
────────────────────────────────────── */
async function selectMatrixCategory(catName) {
  // 日次カテゴリ更新
  selectedDailyCat = catName || null;
  updateDailyCatLabel();
  const dailySel = document.getElementById('daily-cat-select');
  if (dailySel) {
    const idx = dailyCatList.indexOf(catName);
    if (idx >= 0) dailySel.value = idx;
  }
  // 月間トレンドカテゴリ更新
  selectedTrendCat = catName || null;
  updateTrendCatLabel();
  // 両チャート再取得
  refreshDailyChart();
  refreshTrendChart();
}

function updateDailyMonthLabel() {
  const el = document.getElementById('daily-month-label');
  if (el) el.textContent = `${selectedDailyYear}年${selectedDailyMonth}月`;
  const title = document.getElementById('daily-matrix-title');
  if (title) title.textContent = `日次マトリクス — ${selectedDailyYear}年${selectedDailyMonth}月`;
  const sel = document.getElementById('daily-month-select');
  if (sel) sel.value = `${selectedDailyYear}-${selectedDailyMonth}`;
}

function initDailyMonthSelect() {
  const sel = document.getElementById('daily-month-select');
  if (!sel) return;
  sel.innerHTML = '';
  let y = currentYear, m = currentMonth;
  for (let i = 0; i < 36; i++) {
    const opt = document.createElement('option');
    opt.value = `${y}-${m}`;
    opt.textContent = `${y}年${m}月`;
    if (y === selectedDailyYear && m === selectedDailyMonth) opt.selected = true;
    sel.appendChild(opt);
    m--;
    if (m < 1) { m = 12; y--; }
    if (y < currentYear - 3) break;
  }
  sel.onchange = () => {
    const parts = sel.value.split('-').map(Number);
    selectedDailyYear  = parts[0];
    selectedDailyMonth = parts[1];
    updateDailyMonthLabel();
    if (mainCumulChart) { mainCumulChart.destroy(); mainCumulChart = null; }
    refreshDailyChart();
  };
}

function updateDailyCatLabel() {
  const el = document.getElementById('daily-cat-label');
  if (!el) return;
  if (!selectedDailyCat) {
    el.textContent = '合計（全カテゴリ）';
  } else {
    const icon = CAT_ICONS[selectedDailyCat] || '';
    el.textContent = `${icon} ${selectedDailyCat}`;
  }
}

/* ──────────────────────────────────────
   日次：年月スワイプセットアップ
────────────────────────────────────── */
function setupMonthSwipe() {
  const zone = document.getElementById('daily-month-zone');
  if (!zone || zone._swipeInit) return;
  zone._swipeInit = true;
  let startX = 0, startY = 0;
  zone.addEventListener('touchstart', e => {
    startX = e.touches[0].clientX; startY = e.touches[0].clientY;
  }, { passive: true });
  zone.addEventListener('touchend', e => {
    const dx = e.changedTouches[0].clientX - startX;
    const dy = e.changedTouches[0].clientY - startY;
    if (Math.abs(dx) < 40 || Math.abs(dx) < Math.abs(dy)) return;
    changeDailyMonth(dx < 0 ? 1 : -1);
  }, { passive: true });
}

/* ──────────────────────────────────────
   日次：カテゴリスワイプセットアップ（チャートエリア + カテゴリゾーン）
────────────────────────────────────── */
function setupDailySwipe() {
  function attachSwipe(el) {
    if (!el || el._swipeInit) return;
    el._swipeInit = true;
    let startX = 0, startY = 0;
    el.addEventListener('touchstart', e => {
      startX = e.touches[0].clientX; startY = e.touches[0].clientY;
    }, { passive: true });
    el.addEventListener('touchend', e => {
      const dx = e.changedTouches[0].clientX - startX;
      const dy = e.changedTouches[0].clientY - startY;
      if (Math.abs(dx) < 40 || Math.abs(dx) < Math.abs(dy)) return;
      changeDailyCategory(dx < 0 ? 1 : -1);
    }, { passive: true });
  }
  attachSwipe(document.querySelector('.progress-chart-card .chart-wrap'));
  attachSwipe(document.getElementById('daily-cat-zone'));
}

function changeDailyCategory(delta) {
  const currentIdx = selectedDailyCat ? dailyCatList.indexOf(selectedDailyCat) : 0;
  const nextIdx    = (currentIdx + delta + dailyCatList.length) % dailyCatList.length;
  selectedDailyCat = nextIdx === 0 ? null : dailyCatList[nextIdx];

  const sel = document.getElementById('daily-cat-select');
  if (sel) sel.value = nextIdx;
  updateDailyCatLabel();
  refreshDailyChart();
}

/* ──────────────────────────────────────
   日次：カテゴリプルダウン初期化・更新
────────────────────────────────────── */
function initDailyCatSlider() {
  // カテゴリリストを構築（合計 + 支出カテゴリ全て）
  dailyCatList = ['合計', ...allCategories
    .filter(c => c.type === 'expense')
    .map(c => c.name)];

  const sel = document.getElementById('daily-cat-select');
  if (!sel) return;

  // 選択肢を再構築
  sel.innerHTML = '';
  dailyCatList.forEach((name, i) => {
    const opt  = document.createElement('option');
    opt.value  = i;
    const icon = CAT_ICONS[name];
    opt.textContent = i === 0
      ? '合計（全カテゴリ）'
      : (icon ? icon + ' ' + name : name);
    if (i === 0 && !selectedDailyCat) opt.selected = true;
    if (name === selectedDailyCat)    opt.selected = true;
    sel.appendChild(opt);
  });

  updateDailyCatLabel();
  sel.onchange = null;
  sel.addEventListener('change', () => {
    const idx = parseInt(sel.value);
    selectedDailyCat = idx === 0 ? null : dailyCatList[idx];
    updateDailyCatLabel();
    refreshDailyChart();
  });
}

/* ──────────────────────────────────────
   日次：年月ナビゲーション
────────────────────────────────────── */
function changeDailyMonth(delta) {
  let y = selectedDailyYear, m = selectedDailyMonth + delta;
  if (m > 12) { m = 1;  y++; }
  if (m < 1)  { m = 12; y--; }
  if (y > currentYear || (y === currentYear && m > currentMonth)) return;
  selectedDailyYear  = y;
  selectedDailyMonth = m;
  updateDailyMonthLabel();
  if (mainCumulChart) { mainCumulChart.destroy(); mainCumulChart = null; }
  refreshDailyChart();
}

async function refreshDailyChart() {
  if (mainCumulChart) { mainCumulChart.destroy(); mainCumulChart = null; }
  const catParam = selectedDailyCat ? `&category=${encodeURIComponent(selectedDailyCat)}` : '';
  const [cumulData, dmData] = await Promise.all([
    fetch(`/api/cumulative?year=${selectedDailyYear}&month=${selectedDailyMonth}${catParam}`).then(r => r.json()),
    fetch(`/api/daily-matrix?year=${selectedDailyYear}&month=${selectedDailyMonth}`).then(r => r.json()),
  ]);
  renderMainProgressChart(cumulData);
  renderDailyMatrix(dmData);
}

/* ──────────────────────────────────────
   イベント
────────────────────────────────────── */
function setupEvents() {
  document.getElementById('close-detail').addEventListener('click', closeDetail);
  document.getElementById('daily-prev-month')?.addEventListener('click', () => changeDailyMonth(-1));
  document.getElementById('daily-next-month')?.addEventListener('click', () => changeDailyMonth(+1));
  document.getElementById('daily-cat-prev')?.addEventListener('click',  () => changeDailyCategory(-1));
  document.getElementById('daily-cat-next')?.addEventListener('click',  () => changeDailyCategory(+1));

  // MF CSV アップロード
  const mfCsvBtn   = document.getElementById('mf-csv-btn');
  const mfCsvInput = document.getElementById('mf-csv-input');
  if (mfCsvBtn && mfCsvInput) {
    mfCsvBtn.addEventListener('click', () => mfCsvInput.click());
    mfCsvInput.addEventListener('change', handleMfCsvUpload);
  }

  // Zaim CSV アップロード
  const zaimCsvBtn   = document.getElementById('zaim-csv-btn');
  const zaimCsvInput = document.getElementById('zaim-csv-input');
  if (zaimCsvBtn && zaimCsvInput) {
    zaimCsvBtn.addEventListener('click', () => zaimCsvInput.click());
    zaimCsvInput.addEventListener('change', handleZaimCsvUpload);
  }
}

function closeDetail() {
  document.getElementById('detail-section').style.display = 'none';
  if (detailCatChart) { detailCatChart.destroy(); detailCatChart = null; }
  selectedCategory = null;
  document.querySelectorAll('.month-col').forEach(th => th.classList.remove('selected-month'));
  document.querySelectorAll('.hm-cell').forEach(c  => {
    c.classList.remove('cell-selected-col');
    c.classList.remove('cell-selected');
  });
}

/* ──────────────────────────────────────
   MF CSV アップロード
────────────────────────────────────── */
async function handleMfCsvUpload(event) {
  const file  = event.target.files[0];
  if (!file) return;
  const btn   = document.getElementById('mf-csv-btn');
  const label = document.getElementById('mf-csv-btn-label');
  btn.disabled = true; btn.classList.add('running');
  label.textContent = '📥 アップロード中...';
  showToast(`MF CSV「${file.name}」を処理中...`, 'info');

  const formData = new FormData();
  formData.append('file', file);
  try {
    const res  = await fetch('/api/upload-csv', { method: 'POST', body: formData });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '失敗');
    const yearsMsg = data.years ? ` (${data.years.join('・')}年分)` : '';
    showToast(`MF更新完了: ${data.inserted}件インポート${yearsMsg}`, 'success');
    await refreshAll();
  } catch (err) {
    showToast(`MF更新エラー: ${err.message}`, 'error');
  } finally {
    btn.disabled = false; btn.classList.remove('running');
    label.textContent = '📥 MF更新';
    event.target.value = '';
  }
}

/* ──────────────────────────────────────
   Zaim CSV アップロード
────────────────────────────────────── */
async function handleZaimCsvUpload(event) {
  const file  = event.target.files[0];
  if (!file) return;
  const btn   = document.getElementById('zaim-csv-btn');
  const label = document.getElementById('zaim-csv-btn-label');
  btn.disabled = true; btn.classList.add('running');
  label.textContent = '📥 アップロード中...';
  showToast(`Zaim CSV「${file.name}」を処理中...`, 'info');

  const formData = new FormData();
  formData.append('file', file);
  try {
    const res  = await fetch('/api/upload-zaim-csv', { method: 'POST', body: formData });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '失敗');
    showToast(
      `Zaim更新完了: ${data.inserted}件追加（重複スキップ: ${data.skipped_duplicate}件）`,
      'success'
    );
    await refreshAll();
  } catch (err) {
    showToast(`Zaim更新エラー: ${err.message}`, 'error');
  } finally {
    btn.disabled = false; btn.classList.remove('running');
    label.textContent = '📥 Zaim更新';
    event.target.value = '';
  }
}

/* ──────────────────────────────────────
   ユーティリティ
────────────────────────────────────── */
function fmtYen(v) {
  if (!v && v !== 0) return '¥0';
  return '¥' + Math.abs(v).toLocaleString('ja-JP');
}
function fmtDate(s) {
  const d = new Date(s);
  return `${d.getMonth()+1}/${d.getDate()}`;
}
function escHtml(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

let _toastTimer = null;
function showToast(msg, type = '') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = `toast ${type}`;
  t.style.display = 'block';
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => { t.style.display = 'none'; }, 4500);
}
