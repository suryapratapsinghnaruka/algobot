"""
AlgoBot Live Web Dashboard v2
"""
import json, os, time, logging
from datetime import datetime
log = logging.getLogger("Dashboard")
_price_cache: dict = {}
_PRICE_TTL = 60

def _get_live_price(symbol, market):
    cached = _price_cache.get(symbol)
    if cached and (time.time() - cached["ts"]) < _PRICE_TTL:
        return cached["price"]
    try:
        from paper_trader import _get_session
        import yfinance as yf
        session = _get_session()
        yf_sym = f"{symbol}.NS" if market == "stocks" else symbol.replace("USDT","") + "-USD"
        ticker = yf.Ticker(yf_sym, session=session)
        ltp = ticker.fast_info.get("lastPrice")
        if not ltp:
            hist = ticker.history(period="1d", interval="5m")
            ltp = float(hist["Close"].iloc[-1]) if not hist.empty else None
        if ltp:
            _price_cache[symbol] = {"price": float(ltp), "ts": time.time()}
        return float(ltp) if ltp else None
    except Exception:
        return None

TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AlgoBot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#0b0b0f;--bg2:#13131a;--bg3:#1a1a24;--border:#25253a;--text:#e2e2f0;--muted:#6060a0;--green:#26d4a8;--red:#f05050;--blue:#4d9de0;--amber:#f5a623;--purple:#9b6dff;--teal:#20c4c4}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;overflow:hidden;height:100vh}
.topbar{height:52px;background:var(--bg2);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 18px;gap:16px;flex-shrink:0}
.logo{font-size:17px;font-weight:700;color:var(--blue);letter-spacing:-.5px}
.badge{padding:3px 9px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:.5px}
.badge-paper{background:#0d3324;color:var(--green);border:1px solid #1a5a3a}
.badge-live{background:#3a0d0d;color:var(--red);border:1px solid #5a1a1a}
.market-pills{display:flex;gap:8px;margin-left:8px}
.pill{display:flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;border:1px solid var(--border)}
.pill-dot{width:6px;height:6px;border-radius:50%}
.pill-open{border-color:#1a5a3a;color:var(--green)}
.pill-open .pill-dot{background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse 2s infinite}
.pill-closed{border-color:#3a3a1a;color:var(--amber)}
.pill-closed .pill-dot{background:var(--amber)}
.pill-crypto{border-color:#1a2a4a;color:var(--blue)}
.pill-crypto .pill-dot{background:var(--blue);box-shadow:0 0 6px var(--blue);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.topbar-right{margin-left:auto;display:flex;align-items:center;gap:20px}
.stat{text-align:right}
.stat-val{font-size:14px;font-weight:700}
.stat-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;margin-top:1px}
.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--muted)}
#clock{font-size:13px;font-weight:600;color:var(--text);background:var(--bg3);padding:5px 10px;border-radius:6px;border:1px solid var(--border);font-variant-numeric:tabular-nums}
.layout{display:flex;height:calc(100vh - 52px);overflow:hidden}
.main{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:12px;min-width:0}
.sidebar{width:420px;min-width:420px;background:var(--bg2);border-left:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.card-head{padding:10px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;background:var(--bg3)}
.card-head h2{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;color:var(--muted)}
.card-head .hint{font-size:10px;color:var(--muted)}
.chart-tabs{display:flex;gap:6px;flex-wrap:nowrap;overflow-x:auto;padding:8px 12px;border-bottom:1px solid var(--border);background:var(--bg3)}
.chart-tabs::-webkit-scrollbar{height:3px}.chart-tabs::-webkit-scrollbar-thumb{background:var(--border)}
.ctab{padding:4px 10px;border-radius:5px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;border:1px solid var(--border);background:transparent;color:var(--muted);transition:all .15s}
.ctab:hover{background:var(--bg3);color:var(--text)}
.ctab.active.stock{background:var(--blue);border-color:var(--blue);color:#fff}
.ctab.active.crypto{background:var(--teal);border-color:var(--teal);color:#fff}
.ctab.stock{border-color:#1a3a5a}.ctab.crypto{border-color:#1a3a2a}
#chart-wrap{padding:8px;display:flex;flex-direction:column;gap:4px}
#main-chart{height:260px;border-radius:6px;overflow:hidden}
#rsi-chart{height:80px;border-radius:6px;overflow:hidden}
#macd-chart{height:90px;border-radius:6px;overflow:hidden}
.chart-legend{display:flex;gap:10px;padding:4px 2px;flex-wrap:wrap}
.leg-item{display:flex;align-items:center;gap:4px;font-size:10px;color:var(--muted)}
.leg-dot{width:10px;height:2px;border-radius:1px}
.section-head{padding:10px 14px 8px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg2);z-index:10}
.section-head .count{background:var(--bg3);border:1px solid var(--border);padding:1px 7px;border-radius:10px;font-size:10px;color:var(--text)}
.market-tag{font-size:9px;padding:1px 5px;border-radius:3px;font-weight:700}
.mt-stock{background:#0d2a4a;color:var(--blue)}.mt-crypto{background:#0d3324;color:var(--teal)}
.pos-row{display:grid;grid-template-columns:1fr 44px 68px 68px 78px 90px;padding:8px 14px;border-bottom:1px solid var(--border);cursor:pointer;transition:background .1s;gap:4px;align-items:center}
.pos-row:hover{background:var(--bg3)}
.pos-row.winning{border-left:2px solid var(--green)}
.pos-row.losing{border-left:2px solid var(--red)}
.pos-row.neutral{border-left:2px solid var(--border)}
.sym-name{font-weight:600;font-size:12px}
.sym-strat{font-size:10px;color:var(--muted);margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.side-buy{color:var(--green);font-weight:700;font-size:11px}
.side-sell{color:var(--red);font-weight:700;font-size:11px}
.price-val{font-size:12px;font-variant-numeric:tabular-nums;text-align:right}
.pnl-val{font-size:12px;font-weight:700;font-variant-numeric:tabular-nums;text-align:right}
.pnl-pct{font-size:10px;color:var(--muted);text-align:right}
.status-badge{display:inline-flex;align-items:center;justify-content:center;padding:2px 7px;border-radius:10px;font-size:10px;font-weight:600;white-space:nowrap}
.sb-profit{background:#0d3324;color:var(--green);border:1px solid #1a5a3a}
.sb-loss{background:#3a0d0d;color:var(--red);border:1px solid #5a1a1a}
.sb-tp{background:#0d3324;color:var(--green);border:1px solid var(--green);animation:glow-g 1.2s infinite}
.sb-sl{background:#3a0d0d;color:var(--red);border:1px solid var(--red);animation:glow-r 1.2s infinite}
@keyframes glow-g{0%,100%{box-shadow:0 0 0 var(--green)}50%{box-shadow:0 0 8px var(--green)}}
@keyframes glow-r{0%,100%{box-shadow:0 0 0 var(--red)}50%{box-shadow:0 0 8px var(--red)}}
.empty-state{padding:24px 14px;text-align:center;color:var(--muted);font-size:12px}
.trade-row{display:grid;grid-template-columns:1fr 72px 80px 1fr;padding:7px 14px;border-bottom:1px solid var(--border);gap:4px;align-items:center;font-size:12px}
.reason-tag{padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600}
.rt-tp{background:#0d3324;color:var(--green)}.rt-sl{background:#3a0d0d;color:var(--red)}.rt-sq{background:#1a1a3a;color:var(--purple)}
table{width:100%;border-collapse:collapse}
th{text-align:left;padding:8px 12px;color:var(--muted);font-size:11px;font-weight:600;border-bottom:1px solid var(--border);text-transform:uppercase;letter-spacing:.5px}
td{padding:7px 12px;border-bottom:1px solid var(--border);font-size:12px}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--bg3)}
.win-bar-wrap{width:60px;height:4px;background:var(--bg3);border-radius:2px;display:inline-block;vertical-align:middle;margin-right:6px}
.win-bar{height:4px;border-radius:2px;background:var(--green)}
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>
<div class="topbar">
  <span class="logo">AlgoBot</span>
  <span class="badge {{ 'badge-paper' if paper else 'badge-live' }}">{{ 'PAPER' if paper else 'LIVE' }}</span>
  <div class="market-pills">
    <div class="pill" id="nse-pill"><span class="pill-dot"></span><span id="nse-label">NSE --</span></div>
    <div class="pill pill-crypto"><span class="pill-dot"></span>CRYPTO 24/7</div>
    <div class="pill" id="rotation-pill" style="display:none;border-color:#9b6dff;color:#9b6dff">
      <span style="width:6px;height:6px;border-radius:50%;background:#9b6dff;animation:pulse 1.5s infinite;display:inline-block"></span>
      <span id="rotation-label">Rotating...</span>
    </div>
  </div>
  <div style="font-size:11px;color:var(--muted);background:var(--bg3);padding:3px 10px;border-radius:4px;border:1px solid var(--border)" id="capital-display">
    ₹-- capital
  </div>
  <div class="topbar-right">
    <div class="stat"><div class="stat-val neu" id="stocks-pnl">+0.00</div><div class="stat-lbl">Stocks P&L</div></div>
    <div class="stat"><div class="stat-val neu" id="crypto-pnl">+0.00</div><div class="stat-lbl">Crypto P&L</div></div>
    <div class="stat"><div class="stat-val" id="open-count">0</div><div class="stat-lbl">Open</div></div>
    <div id="clock">--:--:-- IST</div>
  </div>
</div>
<div class="layout">
  <div class="main">
    <div class="card">
      <div class="card-head"><h2>Live Chart</h2><span class="hint" id="chart-lbl">Click a position to load chart</span></div>
      <div class="chart-tabs" id="chart-tabs"><span style="font-size:11px;color:var(--muted);padding:4px 0">No open positions</span></div>
      <div id="chart-wrap">
        <div id="main-chart"></div>
        <div class="chart-legend">
          <div class="leg-item"><div class="leg-dot" style="background:#4d9de0"></div>EMA20</div>
          <div class="leg-item"><div class="leg-dot" style="background:#9b6dff"></div>BB Bands</div>
          <div class="leg-item"><div class="leg-dot" style="background:#f5a623"></div>VWAP</div>
        </div>
        <div id="rsi-chart"></div>
        <div id="macd-chart"></div>
      </div>
    </div>
    <div class="card">
      <div class="card-head"><h2>Strategy Performance</h2><span class="hint" id="strat-count"></span></div>
      <table><thead><tr><th>Strategy</th><th>W</th><th>L</th><th>Win %</th><th style="text-align:right">P&L</th></tr></thead>
      <tbody id="strat-body"><tr><td colspan="5" class="empty-state">No closed trades yet</td></tr></tbody></table>
    </div>
  </div>
  <div class="sidebar">
    <div style="padding:12px 14px;border-bottom:1px solid var(--border);display:flex;gap:8px">
      <div style="flex:1;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px 12px">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase">Stocks P&L</div>
        <div style="font-size:18px;font-weight:700;margin-top:3px" id="stocks-pnl-big">+0.00</div>
      </div>
      <div style="flex:1;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px 12px">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase">Crypto P&L</div>
        <div style="font-size:18px;font-weight:700;margin-top:3px" id="crypto-pnl-big">+0.00</div>
      </div>
    </div>
    <div class="section-head"><span>NSE Stocks <span class="market-tag mt-stock">MIS</span></span><span class="count" id="stock-count">0</span></div>
    <div id="stock-positions"></div>
    <div class="section-head"><span>Crypto <span class="market-tag mt-crypto">24/7</span></span><span class="count" id="crypto-count">0</span></div>
    <div id="crypto-positions"></div>
    <div class="section-head" style="margin-top:4px"><span>Recent Trades</span><span class="hint" id="refresh-hint">--</span></div>
    <div id="recent-trades"></div>
  </div>
</div>
<script>
let mainChart,candleSeries,emaSeries,bbUpperS,bbLowerS,vwapSeries;
let rsiChart,rsiSeries,rsiOBS,rsiOSS;
let macdChart,macdS,msigS,mhistS;
let curSym=null,curMkt=null;
const BG='#13131a';
const baseOpts={layout:{background:{color:BG},textColor:'#6060a0'},grid:{vertLines:{color:'#1a1a24'},horzLines:{color:'#1a1a24'}},timeScale:{borderColor:'#25253a',timeVisible:true,secondsVisible:false},crosshair:{mode:1},rightPriceScale:{borderColor:'#25253a'},handleScroll:true,handleScale:true};
function initCharts(){
  mainChart=LightweightCharts.createChart(document.getElementById('main-chart'),{...baseOpts,height:260});
  candleSeries=mainChart.addCandlestickSeries({upColor:'#26d4a8',downColor:'#f05050',borderUpColor:'#26d4a8',borderDownColor:'#f05050',wickUpColor:'#26d4a8',wickDownColor:'#f05050'});
  emaSeries=mainChart.addLineSeries({color:'#4d9de0',lineWidth:1});
  bbUpperS=mainChart.addLineSeries({color:'#9b6dff',lineWidth:1,lineStyle:2});
  bbLowerS=mainChart.addLineSeries({color:'#9b6dff',lineWidth:1,lineStyle:2});
  vwapSeries=mainChart.addLineSeries({color:'#f5a623',lineWidth:1});
  rsiChart=LightweightCharts.createChart(document.getElementById('rsi-chart'),{...baseOpts,height:80});
  rsiSeries=rsiChart.addLineSeries({color:'#4d9de0',lineWidth:1});
  rsiOBS=rsiChart.addLineSeries({color:'#f05050',lineWidth:1,lineStyle:2});
  rsiOSS=rsiChart.addLineSeries({color:'#26d4a8',lineWidth:1,lineStyle:2});
  macdChart=LightweightCharts.createChart(document.getElementById('macd-chart'),{...baseOpts,height:90});
  macdS=macdChart.addLineSeries({color:'#26d4a8',lineWidth:1});
  msigS=macdChart.addLineSeries({color:'#f05050',lineWidth:1});
  mhistS=macdChart.addHistogramSeries({priceFormat:{type:'price',precision:5}});
  mainChart.timeScale().subscribeVisibleLogicalRangeChange(r=>{if(r){rsiChart.timeScale().setVisibleLogicalRange(r);macdChart.timeScale().setVisibleLogicalRange(r)}});
}
async function loadChart(sym,mkt){
  curSym=sym;curMkt=mkt;
  document.getElementById('chart-lbl').textContent=sym+' — '+(mkt==='stocks'?'NSE':'Crypto')+' · 5m';
  document.querySelectorAll('.ctab').forEach(b=>b.classList.toggle('active',b.dataset.symbol===sym));
  try{
    const res=await fetch('/api/candles?symbol='+sym+'&market='+mkt);
    const d=await res.json();
    if(!d.candles||!d.candles.length)return;
    candleSeries.setData(d.candles);emaSeries.setData(d.ema20);bbUpperS.setData(d.bb_upper);bbLowerS.setData(d.bb_lower);vwapSeries.setData(d.vwap);
    rsiSeries.setData(d.rsi);rsiOBS.setData(d.rsi.map(p=>({time:p.time,value:70})));rsiOSS.setData(d.rsi.map(p=>({time:p.time,value:30})));
    macdS.setData(d.macd);msigS.setData(d.macd_signal);
    mhistS.setData(d.macd_hist.map(p=>({...p,color:p.value>=0?'#26d4a850':'#f0505050'})));
    mainChart.timeScale().fitContent();
  }catch(e){console.warn('Chart error:',e)}
}
function updateMarketStatus(){
  const now=new Date();const utc=now.getTime()+now.getTimezoneOffset()*60000;
  const ist=new Date(utc+5.5*3600000);
  const day=ist.getDay(),h=ist.getHours(),m=ist.getMinutes(),mins=h*60+m;
  const isWD=day>=1&&day<=5,isOpen=isWD&&mins>=555&&mins<=930,isPre=isWD&&mins>=540&&mins<555;
  const pill=document.getElementById('nse-pill'),lbl=document.getElementById('nse-label');
  if(isOpen){pill.className='pill pill-open';lbl.textContent='NSE OPEN'}
  else if(isPre){pill.className='pill pill-closed';lbl.textContent='NSE PRE-OPEN'}
  else if(!isWD){pill.className='pill pill-closed';lbl.textContent='NSE WEEKEND'}
  else{pill.className='pill pill-closed';lbl.textContent='NSE CLOSED'}
}
function fmtPnl(v,pfx='₹'){const s=(v>=0?'+':'')+v.toFixed(2);return `<span class="${v>0?'pos':v<0?'neg':'neu'}">${pfx}${s}</span>`}
function renderPos(positions,cid){
  const el=document.getElementById(cid);
  if(!positions.length){el.innerHTML='<div class="empty-state">No open positions</div>';return}
  el.innerHTML=positions.map(p=>{
    const pnl=p.unrealised_pnl||0,pct=p.unrealised_pct||0;
    const rc=pnl>0?'winning':pnl<0?'losing':'neutral';
    const ltp=p.ltp?p.ltp.toFixed(2):'--';
    let sb;
    if(p.near_tp)sb='<span class="status-badge sb-tp">&#9650; Near TP</span>';
    else if(p.near_sl)sb='<span class="status-badge sb-sl">&#9660; Near SL</span>';
    else if(pnl>0)sb=`<span class="status-badge sb-profit">+${pct.toFixed(1)}%</span>`;
    else if(pnl<0)sb=`<span class="status-badge sb-loss">${pct.toFixed(1)}%</span>`;
    else sb='<span class="status-badge" style="color:var(--muted)">0.0%</span>';
    const st=(p.strategy||'').replace('custom_','').replace(/_/g,' ');
    return `<div class="pos-row ${rc}" onclick="loadChart('${p.symbol}','${p.market}')">
      <div><div class="sym-name">${p.symbol}</div><div class="sym-strat">${st}</div></div>
      <div class="side-${(p.action||'buy').toLowerCase()}">${p.action}</div>
      <div class="price-val">${p.entry_price?p.entry_price.toFixed(2):'--'}</div>
      <div class="price-val" style="color:var(--text)">${ltp}</div>
      <div><div class="pnl-val ${pnl>=0?'pos':'neg'}">${(pnl>=0?'+':'')+pnl.toFixed(2)}</div><div class="pnl-pct">${(pct>=0?'+':'')+pct.toFixed(1)}%</div></div>
      <div>${sb}</div></div>`;
  }).join('');
}
async function refreshAll(){
  try{
    const res=await fetch('/api/positions');const d=await res.json();
    const sp=d.daily_pnl?.stocks||0,cp=d.daily_pnl?.crypto||0;
    document.getElementById('stocks-pnl').innerHTML=fmtPnl(sp,'₹');
    document.getElementById('crypto-pnl').innerHTML=fmtPnl(cp,'₹');
    document.getElementById('stocks-pnl-big').innerHTML=fmtPnl(sp,'₹');
    document.getElementById('crypto-pnl-big').innerHTML=fmtPnl(cp,'₹');
    const all=d.positions||[];
    document.getElementById('open-count').textContent=all.length;
    document.getElementById('refresh-hint').textContent='Updated '+new Date().toLocaleTimeString('en-IN');
    const stocks=all.filter(p=>p.market==='stocks'),crypto=all.filter(p=>p.market==='crypto');
    document.getElementById('stock-count').textContent=stocks.length;
    document.getElementById('crypto-count').textContent=crypto.length;
    renderPos(stocks,'stock-positions');renderPos(crypto,'crypto-positions');
    const tabs=document.getElementById('chart-tabs');
    if(all.length){
      tabs.innerHTML=all.map(p=>{const m=p.market==='stocks'?'stock':'crypto';return `<button class="ctab ${m} ${curSym===p.symbol?'active':''}" data-symbol="${p.symbol}" onclick="loadChart('${p.symbol}','${p.market}')">${p.symbol}</button>`}).join('');
      if(!curSym&&all.length){loadChart(all[0].symbol,all[0].market)}
    }else{tabs.innerHTML='<span style="font-size:11px;color:var(--muted);padding:4px 0">No open positions</span>'}
  }catch(e){console.warn(e)}
}
async function refreshHistory(){
  try{
    const res=await fetch('/api/trade_history');const d=await res.json();
    const el=document.getElementById('recent-trades');
    const trades=(d.trades||[]).slice(-20).reverse();
    if(!trades.length){el.innerHTML='<div class="empty-state">No closed trades yet</div>';return}
    el.innerHTML=trades.map(t=>{
      const pnl=parseFloat(t.pnl||0),rea=t.reason||'--';
      const rc=rea==='take-profit'?'rt-tp':rea==='squareoff'?'rt-sq':'rt-sl';
      const rl=rea==='take-profit'?'TP hit':rea==='squareoff'?'Square-off':'SL hit';
      const st=(t.strategy||'').replace('custom_','').replace(/_/g,' ');
      return `<div class="trade-row">
        <div><div style="font-weight:600">${t.symbol}</div><div style="font-size:10px;color:var(--muted)">${t.action||''}</div></div>
        <div class="${pnl>=0?'pos':'neg'}" style="font-weight:700;font-variant-numeric:tabular-nums">${(pnl>=0?'+':'')+pnl.toFixed(2)}</div>
        <div><span class="reason-tag ${rc}">${rl}</span></div>
        <div style="color:var(--muted);font-size:10px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${st}</div></div>`;
    }).join('');
  }catch(e){}
}
async function refreshStrategy(){
  try{
    const res=await fetch('/api/strategy_stats');const d=await res.json();
    const stats=d.stats||[];const el=document.getElementById('strat-body');
    document.getElementById('strat-count').textContent=stats.length+' strategies';
    if(!stats.length){el.innerHTML='<tr><td colspan="5" class="empty-state">No closed trades yet</td></tr>';return}
    el.innerHTML=stats.map(s=>{
      const tot=s.wins+s.losses,wr=tot>0?(s.wins/tot*100):0;
      const st=s.strategy.replace('custom_','').replace(/_/g,' ');
      return `<tr><td>${st}</td><td style="color:var(--green)">${s.wins}</td><td style="color:var(--red)">${s.losses}</td>
        <td><div style="display:flex;align-items:center;gap:6px"><div class="win-bar-wrap"><div class="win-bar" style="width:${wr}%"></div></div>${wr.toFixed(0)}%</div></td>
        <td style="text-align:right" class="${s.total_pnl>=0?'pos':'neg'}">${(s.total_pnl>=0?'+':'')+s.total_pnl.toFixed(2)}</td></tr>`;
    }).join('');
  }catch(e){}
}
function updateClock(){
  const now=new Date(),utc=now.getTime()+now.getTimezoneOffset()*60000,ist=new Date(utc+5.5*3600000);
  document.getElementById('clock').textContent=ist.toLocaleTimeString('en-IN',{hour12:false})+' IST';
}
async function refreshRotation(){
  try{
    const res=await fetch('/api/rotation_status');const d=await res.json();
    const pill=document.getElementById('rotation-pill');
    const lbl=document.getElementById('rotation-label');
    const cap=document.getElementById('capital-display');
    if(d.rotation_enabled){
      pill.style.display='flex';
      lbl.textContent=d.label;
      cap.textContent='₹'+d.capital.toLocaleString('en-IN')+' capital';
      // Dim the inactive market pill
      const nsePill=document.getElementById('nse-pill');
      if(d.mode==='crypto'){nsePill.style.opacity='0.4'}
      else{nsePill.style.opacity='1'}
    }else{
      pill.style.display='none';
      cap.textContent='₹'+d.capital.toLocaleString('en-IN')+' capital';
    }
  }catch(e){}
}
initCharts();refreshAll();refreshHistory();refreshStrategy();updateClock();updateMarketStatus();refreshRotation();
setInterval(refreshAll,30000);setInterval(refreshHistory,30000);setInterval(refreshStrategy,60000);
setInterval(updateClock,1000);setInterval(updateMarketStatus,30000);setInterval(refreshRotation,30000);
setInterval(()=>{if(curSym)loadChart(curSym,curMkt)},60000);
</script>
</body>
</html>"""

def create_app(brokers, journal, daily_pnl, config):
    from flask import Flask, jsonify, render_template_string
    from flask import request as freq
    app = Flask(__name__)
    POSITIONS_FILE = "logs/open_positions.json"

    @app.route("/")
    def index():
        return render_template_string(TEMPLATE, paper=config.get("PAPER_TRADING", True))

    @app.route("/api/positions")
    def api_positions():
        out = []
        try:
            if os.path.exists(POSITIONS_FILE):
                with open(POSITIONS_FILE) as f:
                    data = json.load(f)
                for market, positions in data.items():
                    for p in positions:
                        ltp = _get_live_price(p["symbol"], market)
                        entry = p.get("entry_price", 0)
                        qty = p.get("qty", 0)
                        action = p.get("action", "BUY")
                        unr_pnl = unr_pct = 0.0
                        if ltp and entry > 0:
                            if action == "BUY":
                                unr_pnl = (ltp - entry) * qty
                                unr_pct = (ltp - entry) / entry * 100
                            else:
                                unr_pnl = (entry - ltp) * qty
                                unr_pct = (entry - ltp) / entry * 100
                        sl = p.get("stop_loss", 0)
                        tp = p.get("take_profit", 0)
                        near_sl = bool(ltp and entry > 0 and abs(ltp - sl) / entry < 0.005)
                        near_tp = bool(ltp and entry > 0 and abs(ltp - tp) / entry < 0.005)
                        out.append({**p, "market": market, "ltp": ltp,
                                    "unrealised_pnl": round(unr_pnl, 2),
                                    "unrealised_pct": round(unr_pct, 2),
                                    "near_sl": near_sl, "near_tp": near_tp})
        except Exception as e:
            log.warning(f"Positions API error: {e}")
        return jsonify({"positions": out, "daily_pnl": daily_pnl})

    @app.route("/api/candles")
    def api_candles():
        symbol = freq.args.get("symbol", "RELIANCE")
        market = freq.args.get("market", "stocks")
        try:
            from paper_trader import _get_session
            import yfinance as yf
            import pandas as pd
            session = _get_session()
            yf_sym = f"{symbol}.NS" if market == "stocks" else symbol.replace("USDT", "") + "-USD"
            ticker = yf.Ticker(yf_sym, session=session)
            df = ticker.history(period="1d" if market=="stocks" else "2d", interval="5m", timeout=20)
            if df is None or df.empty:
                return jsonify({"error": "No data"}), 404
            df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
            df = df.tail(200).reset_index()
            # Build timestamps — handle both tz-aware and naive DatetimeIndex
            def ts(i):
                try:
                    if not isinstance(df.index, pd.RangeIndex):
                        ts_val = pd.Timestamp(df.index[i])
                        if ts_val.tzinfo is not None:
                            ts_val = ts_val.tz_convert("UTC").tz_localize(None)
                        v = int(ts_val.timestamp())
                        if v > 0:
                            return v
                except Exception:
                    pass
                return int(time.time()) - (len(df) - i) * 300

            times = [ts(i) for i in range(len(df))]

            def safe_float(v):
                if v is None:
                    return None
                try:
                    f = float(v)
                    return f if (f == f and f > 0) else None   # NaN and zero check
                except Exception:
                    return None

            candles = []
            for i in range(len(df)):
                o,h,l,c = safe_float(df["open"].iloc[i]),safe_float(df["high"].iloc[i]),safe_float(df["low"].iloc[i]),safe_float(df["close"].iloc[i])
                if None not in (o,h,l,c):
                    candles.append({"time":times[i],"open":round(o,6),"high":round(h,6),"low":round(l,6),"close":round(c,6)})

            def series(vals):
                result = []
                for i in range(len(df)):
                    v = safe_float(vals.iloc[i])
                    if v is not None:
                        result.append({"time":times[i],"value":round(v,6)})
                return result
            ema20 = df["close"].ewm(span=20,adjust=False).mean()
            bm = df["close"].rolling(20).mean(); bs = df["close"].rolling(20).std()
            tp_col = (df["high"]+df["low"]+df["close"])/3
            vwap = (tp_col*df["volume"]).rolling(20).sum()/df["volume"].rolling(20).sum()
            d = df["close"].diff()
            gain = d.clip(lower=0).rolling(14).mean(); loss = (-d.clip(upper=0)).rolling(14).mean()
            rsi = 100-(100/(1+gain/loss.replace(0,float("nan"))))
            ef = df["close"].ewm(span=12,adjust=False).mean(); es = df["close"].ewm(span=26,adjust=False).mean()
            macd = ef-es; msig = macd.ewm(span=9,adjust=False).mean(); mhst = macd-msig
            return jsonify({"candles":candles,"ema20":series(ema20),"bb_upper":series(bm+2*bs),"bb_lower":series(bm-2*bs),"vwap":series(vwap),"rsi":series(rsi),"macd":series(macd),"macd_signal":series(msig),"macd_hist":series(mhst)})
        except Exception as e:
            log.warning(f"Candles API error for {symbol}: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/rotation_status")
    def api_rotation_status():
        from datetime import timezone, timedelta as td
        IST     = timezone(td(hours=5, minutes=30))
        now_ist = datetime.now(IST)
        mins    = now_ist.hour * 60 + now_ist.minute
        is_wd   = now_ist.weekday() < 5
        rotation_enabled = config.get("CAPITAL_ROTATION_ENABLED", False)
        capital = config.get("CAPITAL", 1500)

        if not rotation_enabled:
            mode = "both"
            label = "Both markets active"
        elif not is_wd:
            mode = "crypto"
            label = "Weekend — Crypto only"
        elif mins >= 9*60+15 and mins <= 15*60+30:
            mode = "stocks"
            mins_left = (15*60+30) - mins
            label = f"NSE open — {mins_left//60}h {mins_left%60}m left"
        elif mins > 15*60+30:
            mode = "crypto"
            mins_until = (9*60+15 + 24*60) - mins
            label = f"Crypto mode — NSE opens in {mins_until//60}h {mins_until%60}m"
        else:
            mode = "crypto"
            mins_until = (9*60+15) - mins
            label = f"Pre-market — NSE opens in {mins_until//60}h {mins_until%60}m"

        stock_positions  = len([p for p in (json.load(open(POSITIONS_FILE)) if os.path.exists(POSITIONS_FILE) else {}).get("stocks", [])])
        crypto_positions = len([p for p in (json.load(open(POSITIONS_FILE)) if os.path.exists(POSITIONS_FILE) else {}).get("crypto", [])])

        return jsonify({
            "mode":             mode,
            "label":            label,
            "capital":          capital,
            "rotation_enabled": rotation_enabled,
            "stock_positions":  stock_positions,
            "crypto_positions": crypto_positions,
            "ist_time":         now_ist.strftime("%H:%M IST"),
        })

    @app.route("/api/strategy_stats")
    def api_strategy_stats():
        stats = []
        try:
            for strat, s in sorted(journal.strategy_stats.items(), key=lambda x: x[1].get("total_pnl",0), reverse=True):
                stats.append({"strategy":strat,"wins":s.get("wins",0),"losses":s.get("losses",0),"total_pnl":round(s.get("total_pnl",0.0),2)})
        except Exception as e:
            log.warning(f"Stats API error: {e}")
        return jsonify({"stats": stats})

    @app.route("/api/trade_history")
    def api_trade_history():
        try:
            return jsonify({"trades": journal.get_closed_trades()[-50:]})
        except Exception as e:
            return jsonify({"trades": [], "error": str(e)})

    return app

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from config import CONFIG
    from trade_journal import TradeJournal
    class _Stub:
        def get_open_positions(self): return []
    journal = TradeJournal()
    daily_pnl = {"stocks": 0.0, "crypto": 0.0}
    app = create_app({"stocks": _Stub(), "crypto": _Stub()}, journal, daily_pnl, CONFIG)
    port = CONFIG.get("DASHBOARD_PORT", 5001)
    print(f"Dashboard at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)