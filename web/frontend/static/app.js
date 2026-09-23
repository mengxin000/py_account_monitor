"use strict";
const $ = id => document.getElementById(id);
let base = location.origin, token = "", socket, latest, kind = "orders", page = 0, total = 0, timer, active = false, loading = false;
let subscription = 0, rowsKey = "", metricsKey = "", tableKey = "";
const el = (tag, text, cls) => { const n = document.createElement(tag); if(text != null) n.textContent = text; if(cls) n.className = cls; return n; };
const number = (v, digits = 4) => v == null || v === "-" || !Number.isFinite(Number(v)) ? "—" : Number(v).toLocaleString("en-US", {minimumFractionDigits:digits, maximumFractionDigits:digits});
const clock = v => v ? new Date(v).toLocaleTimeString("zh-CN", {hour12:false}) : "—";
const fillTime = v => {
  if(v == null) return "—";
  const date = new Date(Number(v));
  if(!Number.isFinite(date.getTime())) return "—";
  const pad = (n, length = 2) => String(n).padStart(length,"0");
  return `${date.getFullYear()}-${pad(date.getMonth()+1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(),3)}`;
};
function slippageBps(row) {
  const spread=row.quoted_spread, offset=row.offset;
  if(spread==null||offset==null||spread===""||offset==="")return null;
  if(!Number.isFinite(Number(spread))||!Number.isFinite(Number(offset)))return null;
  const side=String(row.quoted_spread_side||"").toUpperCase();
  if(side==="SELL")return (Number(spread)-Number(offset))*10000;
  if(side==="BUY")return (Number(offset)-Number(spread))*10000;
  return null;
}
const legs = () => [0,1].map(i => ({market:$("market"+i).value,symbol:$("symbol"+i).value.trim().toUpperCase()}));
$("backend").value = location.protocol === "http:" ? location.origin : (localStorage.getItem("monitorBackend") || location.origin);
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
    base=monitorBackendOrigin($("backend").value.trim(),location.origin);
    const data=await (await api("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:$("username").value,password:$("password").value})})).json();
    token=data.token; $("password").value=""; localStorage.setItem("monitorBackend",base);
    $("account").replaceChildren(...data.accounts.map(a => {const o=el("option",a);o.value=a;return o;}));
    active=true; $("login").hidden=true; $("dashboard").hidden=false; connect();
  } catch(error) { $("loginError").textContent=error.message; }
});
function connect() {
  clearTimeout(timer); if(socket) {socket.onclose=null;socket.close();}
  if(!active) return;
  badge("连接中");
  const ws=socket=new WebSocket(base.replace(/^http/,"ws")+"/api/live");
  ws.onopen=()=>ws.send(JSON.stringify({token,protocol:2,subscription,account:$("account").value,legs:legs()}));
  ws.onmessage=event=>{
    try {
      const message=JSON.parse(event.data);
      if(message.type==="update") {
        if(message.subscription===subscription) {
          render({...(!message.reset?latest:{}),...message.data});
          if(message.roundTripMs!=null)badge("实时连接 · 往返含渲染 "+message.roundTripMs+"ms",true);
          loadRows();
        }
        ws.send(JSON.stringify({type:"ack",sequence:message.sequence}));
      } else render(message);
    } catch {warning("收到无法解析的数据");}
  };
  ws.onclose=()=>{badge("已断开");warning("连接已断开，当前数值为最后快照，不代表实时状态。正在重连；若持续失败请重新登录。");timer=setTimeout(connect,3000);};
  ws.onerror=()=>badge("连接失败");
}
function render(data) {
  latest=data; badge("实时连接",true); $("day").textContent="交易日 "+data.day;
  const issues=[];
  if(data.collectorStale) issues.push("采集端状态已过期，权益和挂单仅供参考。");
  if(data.error) issues.push("实时计算失败："+data.error+"；保留上次结果。");
  for(const [market,error] of Object.entries(data.depthErrors||{})) issues.push(market+" 行情："+error);
  for(const [stream,status] of Object.entries(data.status.private_streams||{})) if(!["CONNECTED","DISABLED"].includes(status)) issues.push(stream+" 私有连接："+status);
  warning(issues.join(" "));
  const s=data.summary, a=data.status;
  const metrics=[["总权益 / U",data.collectorStale?null:a.total_equity],["实际损益 / U",data.collectorStale?null:a.actual_profit],["交易损益 · 暂算 / U",s.tradeProfit],["普通配对",s.pairCount,0],["未匹配条数",s.unmatchedCount,0],["MMR",a.unimmr,0]];
  const metricSignature=JSON.stringify(metrics);
  if(metricSignature!==metricsKey) {
    metricsKey=metricSignature;
    $("metrics").replaceChildren(...metrics.map(([label,value,digits])=>{const n=el("div",null,"metric");n.append(el("label",label),el("strong",number(value,digits??4),label.includes("损益")?(value<0?"negative":"positive"):""));return n;}));
  }
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
  // Preserve existing DOM; only update text/attributes which have changed.
  function reconcile(parent, desired) {
    if(parent.childNodes.length!==desired.length) {parent.replaceChildren(...desired);return;}
    desired.forEach((fresh,i)=>{
      const old=parent.childNodes[i];
      if(old.nodeType!==fresh.nodeType||old.nodeName!==fresh.nodeName) {old.replaceWith(fresh);return;}
      if(fresh.nodeType===Node.TEXT_NODE) {if(old.nodeValue!==fresh.nodeValue)old.nodeValue=fresh.nodeValue;return;}
      for(const attr of Array.from(old.attributes))if(!fresh.hasAttribute(attr.name))old.removeAttribute(attr.name);
      for(const attr of fresh.attributes)if(old.getAttribute(attr.name)!==attr.value)old.setAttribute(attr.name,attr.value);
      reconcile(old,Array.from(fresh.childNodes));
    });
  }
  reconcile(target,nodes);
}
const columns={
 orders:["来源","交易对","方向","挂单价格","剩余数量","订单ID"],
 recentTrades:["时间","来源","交易对","方向","本次成交量","本次成交价","手续费原值 / 币种"],
 matches:["交易对","方向","滑点（bps）","价差","成交价差","成交时间","订单ID","订单系统ID","订单方向","订单成交数量","订单平均成交价格","订单手续费","对冲订单ID","对冲订单系统ID","对冲订单方向","对冲订单成交数量","对冲订单平均成交价格","对冲订单手续费","收益"],
 unmatched:["时间","来源","交易对","订单ID","方向","数量","手续费 / U"],
 exposures:["基础币","匹配数量","买入金额","卖出金额","收益 / U"]
};
function cells(row) {
  if(kind==="orders") return [row.scope,row.symbol,row.side,row.price,number(row.remaining),row.clientId];
  if(kind==="recentTrades") {const e=row.data||row,o=typeof e.o==="object"?e.o:e; return [clock(o.T||e.T),row.accountScope||e.fs||"—",o.s,o.S,o.l,o.L,(o.n||"0")+" / "+(o.N||"—")];}
  if(kind==="matches") {
    const order=String(row.current_symbol||"").toUpperCase(),hedge=String(row.match_symbol||"").toUpperCase();
    const pair=order&&hedge?order+"_"+hedge:order||hedge;
    const direction=String(row.quoted_spread_side||"").toUpperCase()==="SELL"?"开仓":"平仓";
    return [pair,direction,number(slippageBps(row),3),number(row.quoted_spread,8),number(row.offset,8),fillTime(row.event_time_ms),row.current_id,row.current_system_id,row.current_side,number(row.quantity,8),number(row.current_price,8),number(row.current_fee,8),row.match_id,row.match_system_id,row.match_side,number(row.quantity,8),number(row.match_price,8),number(row.match_fee,8),number(row.profit,8)];
  }
  if(kind==="unmatched") return [clock(row.fill_time||row.time_ms),row.account_scope||row.market_type,row.symbol,row.id,row.side,number(row.quantity),number(row.fee,8)];
  return [row.base,number(row.quantity),number(row.buy_amount),number(row.sell_amount),number(row.profit_delta,8)];
}
function showRows(rows,count) {
  const signature=JSON.stringify([kind,page,count,rows]);
  if(signature===tableKey)return;
  tableKey=signature;
  total=count; const head=el("tr");columns[kind].forEach(t=>head.append(el("th",t)));$("thead").replaceChildren(head);
  $("tbody").replaceChildren(...rows.map(row=>{const tr=el("tr");cells(row).forEach(v=>tr.append(el("td",v??"—")));return tr;}));
  if(!rows.length) {const tr=el("tr"),td=el("td","暂无记录","empty");td.colSpan=columns[kind].length;tr.append(td);$("tbody").append(tr);}
  $("page").textContent=(page+1)+" / "+Math.max(1,Math.ceil(total/50));$("previous").disabled=page===0;$("next").disabled=(page+1)*50>=total;
}
async function loadRows() {
  if(!latest||loading) return;
  if(kind==="orders") {showRows(latest.orders.slice(page*50,(page+1)*50),latest.orders.length);return;}
  const requestedKind=kind,requestedAccount=$("account").value,requestedPage=page;
  const requestedKey=JSON.stringify([requestedAccount,latest.day,requestedKind,requestedPage,latest.summary.version]);
  if(rowsKey===requestedKey)return;
  loading=true;
  try {const data=await (await api("/api/rows/"+encodeURIComponent(requestedAccount)+"/"+kind+"?page="+page)).json();if(kind===requestedKind&&requestedAccount===$("account").value&&page===requestedPage){showRows(data.rows,data.total);rowsKey=requestedKey;}}
  catch(error) {warning(error.message);} finally {loading=false;}
}
$("tabs").addEventListener("click",event=>{if(!event.target.dataset.kind)return;kind=event.target.dataset.kind;page=0;document.querySelectorAll("#tabs button").forEach(b=>b.classList.toggle("selected",b.dataset.kind===kind));loadRows();});
$("previous").onclick=()=>{page=Math.max(0,page-1);loadRows();};$("next").onclick=()=>{if((page+1)*50<total)page++;loadRows();};
for(const id of ["account","market0","market1","symbol0","symbol1"]) $(id).addEventListener("change",()=>{
  page=0;latest=null;subscription++;rowsKey="";metricsKey="";tableKey="";$("tbody").replaceChildren();$("metrics").replaceChildren();
  for(const i of [0,1]) $("book"+i).replaceChildren(el("div","等待新快照…","bookfoot"));
  $("asof").textContent="等待新快照";warning("正在切换账户 / 行情，等待后端快照。");
  if(socket?.readyState===WebSocket.OPEN) socket.send(JSON.stringify({type:"subscribe",subscription,account:$("account").value,legs:legs()}));
  else connect();
});
$("logout").onclick=async()=>{active=false;clearTimeout(timer);if(socket){socket.onclose=null;socket.close();}if(token)try{await api("/api/logout",{method:"POST"});}catch{}token="";$("dashboard").hidden=true;$("login").hidden=false;badge("已退出");};
$("download").onclick=async()=>{try {const account=$("account").value,day=latest.day,response=await api("/api/download/"+encodeURIComponent(account)+"/"+day+"/xlsx"),url=URL.createObjectURL(await response.blob()),a=el("a");a.href=url;a.download=account+"_"+day+".xlsx";a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}catch(error){warning(error.message+"；日报需由原有报告任务生成。");}};
setInterval(()=>{if(active)loadRows();},2000);
