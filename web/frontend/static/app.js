"use strict";
const $ = id => document.getElementById(id);
const demo = new URLSearchParams(location.search).get("demo") === "1";
let base = location.origin, token = "", socket, latest, kind = "orders", page = 0, total = 0, timer, active = false, loading = false;
const el = (tag, text, cls) => { const n = document.createElement(tag); if(text != null) n.textContent = text; if(cls) n.className = cls; return n; };
const number = (v, digits = 4) => v == null || v === "-" || !Number.isFinite(Number(v)) ? "—" : Number(v).toLocaleString("en-US", {minimumFractionDigits:digits, maximumFractionDigits:digits});
const clock = v => v ? new Date(v).toLocaleTimeString("zh-CN", {hour12:false}) : "—";
const legs = () => [0,1].map(i => ({market:$("market"+i).value,symbol:$("symbol"+i).value.trim().toUpperCase()}));
$("backend").value = localStorage.getItem("monitorBackend") || location.origin;
async function api(path, options = {}) {
  const response = await fetch(base + path, {...options, headers:{"Authorization":"Bearer "+token, ...(options.headers || {})}});
  if(!response.ok) throw new Error(response.status === 401 ? "登录已过期或凭证错误，请重新登录" : "请求失败 ("+response.status+")");
  return response;
}
function badge(text, live = false) { $("connection").textContent = text; $("connection").className = "badge"+(live?" live":""); }
function warning(text) { $("warning").textContent=text; $("warning").hidden=!text; }
$("loginForm").addEventListener("submit", async event => {
  event.preventDefault(); $("loginError").textContent="";
  try {
    const url = new URL($("backend").value.trim());
    if(!["http:","https:"].includes(url.protocol) || url.username || url.password) throw new Error("请输入 HTTP / HTTPS 后端地址");
    if(url.protocol !== "https:" && !["localhost","127.0.0.1","[::1]"].includes(url.hostname)) throw new Error("远程后端必须使用 HTTPS");
    base=url.origin;
    const data=await (await api("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:$("username").value,password:$("password").value})})).json();
    token=data.token; $("password").value=""; localStorage.setItem("monitorBackend",base);
    $("account").replaceChildren(...data.accounts.map(a => {const o=el("option",a);o.value=a;return o;}));
    active=true; $("login").hidden=true; $("dashboard").hidden=false; connect();
  } catch(error) { $("loginError").textContent=error.message; }
});
function connect() {
  clearTimeout(timer); if(socket) {socket.onclose=null;socket.close();}
  if(demo) {render(demoSnapshot());loadRows();return;}
  if(!active) return;
  badge("连接中");
  const ws=socket=new WebSocket(base.replace(/^http/,"ws")+"/api/live");
  ws.onopen=()=>ws.send(JSON.stringify({token,account:$("account").value,legs:legs()}));
  ws.onmessage=event=>{try {render(JSON.parse(event.data));} catch {warning("收到无法解析的数据");}};
  ws.onclose=()=>{badge("已断开");warning("连接已断开，当前数值为最后快照，不代表实时状态。正在重连；若持续失败请重新登录。");timer=setTimeout(connect,3000);};
  ws.onerror=()=>badge("连接失败");
}
function render(data) {
  latest=data; badge(demo?"离线演示 · 模拟数据":"实时连接",!demo); $("day").textContent="交易日 "+data.day;
  const issues=[];
  if(demo) issues.push("演示模式：全部为模拟数据，无真实账户连接。");
  if(data.collectorStale) issues.push("采集端状态已过期，权益和挂单仅供参考。");
  if(data.error) issues.push("实时计算失败："+data.error+"；保留上次结果。");
  for(const [market,error] of Object.entries(data.depthErrors||{})) issues.push(market+" 行情："+error);
  for(const [stream,status] of Object.entries(data.status.private_streams||{})) if(!["CONNECTED","DISABLED"].includes(status)) issues.push(stream+" 私有连接："+status);
  warning(issues.join(" "));
  const s=data.summary, a=data.status;
  const metrics=[["总权益 / U",data.collectorStale?null:a.total_equity],["实际损益 / U",data.collectorStale?null:a.actual_profit],["交易损益 · 暂算 / U",s.tradeProfit],["普通配对",s.pairCount,0],["未匹配条数",s.unmatchedCount,0],["MMR",a.unimmr,0]];
  $("metrics").replaceChildren(...metrics.map(([label,value,digits])=>{const n=el("div",null,"metric");n.append(el("label",label),el("strong",number(value,digits??4),label.includes("损益")?(value<0?"negative":"positive"):""));return n;}));
  data.books.forEach((book,i)=>renderBook(book,i));
  $("asof").textContent="暂算更新 "+clock(s.asOf);
  $("verification").textContent="JSONL 核验："+(s.checkStatus||"等待")+" · "+clock(s.checkedAt);
  if(kind==="orders") showRows(data.orders.slice(page*50,(page+1)*50),data.orders.length);
}
function renderBook(book,index) {
  const target=$("book"+index), nodes=[];
  const head=el("div",null,"depthhead"); ["价格","数量","累计","我的挂单"].forEach(t=>head.append(el("span",t)));nodes.push(head);
  const all=book.orders||[];
  function side(rows,cls) {
    let cumulative=0;
    const values=rows.map(([p,q])=>({p,q,sum:cumulative+=Number(q)}));
    const max=cumulative||1; if(cls==="ask") values.reverse();
    return values.map(({p,q,sum})=>{
      const own=all.filter(o=>Number(o.price)===Number(p)&&o.side===(cls==="ask"?"SELL":"BUY"));
      const n=el("div",null,"depthrow "+cls+(own.length?" mine":""));n.style.setProperty("--bar",Math.min(100,sum/max*100)+"%");
      [p,number(q,4),number(sum,4),own.length?number(own.reduce((s,o)=>s+Number(o.remaining),0),4):"—"].forEach(t=>n.append(el("span",t)));
      if(own.length) n.title=own.map(o=>o.scope+" / "+o.clientId).join("\n");return n;
    });
  }
  nodes.push(...side(book.asks,"ask"));
  const mid=el("div",null,"mid"); const bestBid=Number(book.bids[0]?.[0]),bestAsk=Number(book.asks[0]?.[0]);
  mid.append(el("span",number((bestBid+bestAsk)/2,5)),el("small",book.stale?"行情过期 / 等待快照":"买卖中价 · 15档",book.stale?"stale":""));nodes.push(mid);
  nodes.push(...side(book.bids,"bid"));
  nodes.push(el("div","接收 "+clock(book.receivedTimeUs/1000)+" · "+book.market.toUpperCase()+" / "+book.symbol+" · 挂单 "+all.length+" 笔（含档外）","bookfoot"));
  target.replaceChildren(...nodes);
}
const columns={
 orders:["来源","交易对","方向","挂单价格","剩余数量","订单ID"],
 recentTrades:["时间","来源","交易对","方向","本次成交量","本次成交价","手续费原值 / 币种"],
 matches:["时间","交易对组合","订单ID","对冲订单ID","配对数量","收益 / U"],
 unmatched:["时间","来源","交易对","订单ID","方向","数量","手续费 / U"],
 exposures:["基础币","匹配数量","买入金额","卖出金额","收益 / U"]
};
function cells(row) {
  if(kind==="orders") return [row.scope,row.symbol,row.side,row.price,number(row.remaining),row.clientId];
  if(kind==="recentTrades") {const e=row.data||row,o=typeof e.o==="object"?e.o:e; return [clock(o.T||e.T),row.accountScope||e.fs||"—",o.s,o.S,o.l,o.L,(o.n||"0")+" / "+(o.N||"—")];}
  if(kind==="matches") return [clock(row.event_time_ms),row.current_symbol+"_"+row.match_symbol,row.current_id,row.match_id,number(row.quantity),number(row.profit,8)];
  if(kind==="unmatched") return [clock(row.fill_time||row.time_ms),row.account_scope||row.market_type,row.symbol,row.id,row.side,number(row.quantity),number(row.fee,8)];
  return [row.base,number(row.quantity),number(row.buy_amount),number(row.sell_amount),number(row.profit_delta,8)];
}
function showRows(rows,count) {
  total=count; const head=el("tr");columns[kind].forEach(t=>head.append(el("th",t)));$("thead").replaceChildren(head);
  $("tbody").replaceChildren(...rows.map(row=>{const tr=el("tr");cells(row).forEach(v=>tr.append(el("td",v??"—")));return tr;}));
  if(!rows.length) {const tr=el("tr"),td=el("td","暂无记录","empty");td.colSpan=columns[kind].length;tr.append(td);$("tbody").append(tr);}
  $("page").textContent=(page+1)+" / "+Math.max(1,Math.ceil(total/50));$("previous").disabled=page===0;$("next").disabled=(page+1)*50>=total;
}
async function loadRows() {
  if(!latest||loading) return;
  if(kind==="orders") {showRows(latest.orders.slice(page*50,(page+1)*50),latest.orders.length);return;}
  if(demo) {showRows(kind==="recentTrades"?latest.recentTrades:[],kind==="recentTrades"?latest.recentTrades.length:0);return;}
  const requestedKind=kind,requestedAccount=$("account").value,requestedPage=page;
  loading=true;
  try {const data=await (await api("/api/rows/"+encodeURIComponent(requestedAccount)+"/"+kind+"?page="+page)).json();if(kind===requestedKind&&requestedAccount===$("account").value&&page===requestedPage)showRows(data.rows,data.total);}
  catch(error) {warning(error.message);} finally {loading=false;}
}
$("tabs").addEventListener("click",event=>{if(!event.target.dataset.kind)return;kind=event.target.dataset.kind;page=0;document.querySelectorAll("#tabs button").forEach(b=>b.classList.toggle("selected",b.dataset.kind===kind));loadRows();});
$("previous").onclick=()=>{page=Math.max(0,page-1);loadRows();};$("next").onclick=()=>{if((page+1)*50<total)page++;loadRows();};
for(const id of ["account","market0","market1","symbol0","symbol1"]) $(id).addEventListener("change",()=>{
  page=0;latest=null;$("tbody").replaceChildren();$("metrics").replaceChildren();
  for(const i of [0,1]) $("book"+i).replaceChildren(el("div","等待新快照…","bookfoot"));
  $("asof").textContent="等待新快照";warning("正在切换账户 / 行情，等待后端快照。");connect();
});
$("logout").onclick=async()=>{active=false;clearTimeout(timer);if(socket){socket.onclose=null;socket.close();}if(token)try{await api("/api/logout",{method:"POST"});}catch{}token="";if(demo){location.href=location.pathname;return;}$("dashboard").hidden=true;$("login").hidden=false;badge("已退出");};
$("download").onclick=async()=>{if(demo){warning("离线演示不提供真实账户日报下载。");return;}try {const account=$("account").value,day=latest.day,response=await api("/api/download/"+encodeURIComponent(account)+"/"+day+"/xlsx"),url=URL.createObjectURL(await response.blob()),a=el("a");a.href=url;a.download=account+"_"+day+".xlsx";a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}catch(error){warning(error.message+"；日报需由原有报告任务生成。");}};
setInterval(()=>{if(active)loadRows();},2000);
function demoSnapshot() {
  const now=Date.now();return {account:$("account").value,day:"20260922",collectorStale:false,depthErrors:{},status:{total_equity:3002.4851,actual_profit:2.4851,unimmr:94,private_streams:{pm:"CONNECTED",spot:"CONNECTED"}},summary:{tradeProfit:2.1038,pairCount:128,unmatchedCount:2,asOf:now,checkedAt:now,checkStatus:"演示"},orders:[{scope:"spot",market:"spot",symbol:"AAVEUSDT",side:"BUY",price:"103.67",remaining:.3,clientId:"demo-spot-order"},{scope:"um",market:"um",symbol:"AAVEUSDT",side:"SELL",price:"103.72",remaining:.3,clientId:"demo-future-order"}],recentTrades:[],books:legs().map(({market,symbol})=>({market,symbol,receivedTimeUs:now*1000,stale:false,bids:Array.from({length:15},(_,i)=>[(103.69-i*.01).toFixed(2),(2.15+i*1.31).toFixed(3)]),asks:Array.from({length:15},(_,i)=>[(103.70+i*.01).toFixed(2),(1.87+i*.93).toFixed(3)]),orders:[{scope:market,market,symbol,side:market==="spot"?"BUY":"SELL",price:market==="spot"?"103.67":"103.72",remaining:.3,clientId:"demo-order"}]}))};
}
if(demo){$("account").replaceChildren(...["zdl","mfx","dh"].map(a=>{const o=el("option",a);o.value=a;return o;}));active=true;$("login").hidden=true;$("dashboard").hidden=false;connect();}
