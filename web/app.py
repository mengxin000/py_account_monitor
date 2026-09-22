import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
from collections import deque
from datetime import datetime,timedelta
from pathlib import Path

from aiohttp import web, WSMsgType
try:
    from ..realtime.engine import AccountProjection, Journal
    from ..realtime.orders import Orders
    from ..realtime.depth import DepthService
except ImportError:
    from realtime.engine import AccountProjection, Journal
    from realtime.orders import Orders
    from realtime.depth import DepthService

LOGGER = logging.getLogger("binance_web")


def trading_day():
    now = datetime.now()
    if (now.hour,now.minute)<(9,30): now-=timedelta(days=1)
    return now.strftime("%Y%m%d")


class LiveData:
    def __init__(self,accounts,root,bridge,proxy=None):
        self.accounts = accounts
        self.root = Path(root)
        self.bridge = bridge
        self.depth = DepthService(proxy)
        self.projections = {}
        self.journals = {}
        self.orders = {a:Orders() for a in accounts}
        self.statuses = {}
        self.results = {}
        self.errors = {}
        self.watchers = {}
        self.day = None

    def refresh(self):
        day = trading_day()
        if self.day != day:
            self.projections = {a:AccountProjection(Path(path)/day) for a,path in self.accounts.items()}
            self.journals = {a:Journal(Path(path)/day/"all_callbacks.jsonl") for a,path in self.accounts.items()}
            self.day = day
        for account,projection in self.projections.items():
            try:
                result = projection.refresh()
                rows, reset = self.journals[account].read()
                if reset: self.orders[account] = Orders()
                for _,row in rows: self.orders[account].event(row)
                self.results[account] = result
                self.errors.pop(account,None)
            except Exception as exc:
                first_error = account not in self.errors
                self.errors[account] = type(exc).__name__
                if first_error: LOGGER.exception("live projection failed account=%s",account)

    async def run(self):
        import queue
        while True:
            # No concurrent mutation: bridge updates and refresh run serially.
            try:
                while True:
                    message = self.bridge.get_nowait()
                    account = message["account"]
                    if account not in self.accounts: continue
                    if message["kind"]=="status": self.statuses[account] = message
                    elif message["kind"]=="orders":
                        self.orders[account].reconcile(message["scope"],message["rows"],message["asOf"])
            except queue.Empty: pass
            await asyncio.to_thread(self.refresh)
            keys = {(x["market"],x["symbol"]) for orders in self.orders.values() for x in orders.rows.values() if x["market"] in {"spot","um"}}
            for watched in self.watchers.values(): keys.update(watched)
            self.depth.request(keys)
            await asyncio.sleep(.5)

    def snapshot(self,account,keys):
        status = self.statuses.get(account,{})
        now = time.time()*1000
        rows = list(self.orders[account].rows.copy().values())
        books = []
        for key in keys:
            book = self.depth.books.get(key)
            if book:
                books.append({**book,"stale":now-book["receivedTimeUs"]/1000>3000,
                              "orders":[x for x in rows if (x["market"],x["symbol"])==key]})
            else: books.append({"market":key[0],"symbol":key[1],"stale":True,"bids":[],"asks":[],"orders":[]})
        result = self.results.get(account,{})
        summary = {k:v for k,v in result.items() if k not in {"matches","unmatched","exposures","remain","recentTrades"}}
        return {"account":account,"day":self.day,"serverTime":now,"status":status.get("data",{}),
                "collectorStale":now-status.get("asOf",0)>15000,"orders":rows[:500],"books":books,
                "summary":summary,"recentTrades":result.get("recentTrades",[])[:30],
                "error":self.errors.get(account),"depthErrors":self.depth.errors}


def make_app(live,config):
    allowed = set(config.get("allowed_origins",[]))
    users = {}
    for row in config.get("users",[]):
        password = os.environ.get(row.get("password_env",""),"")
        if len(password)<16: raise ValueError("web password environment variable must contain at least 16 characters")
        accounts = set(row.get("accounts",[]))
        if not accounts or not accounts.issubset(live.accounts): raise ValueError("invalid web account permissions")
        salt = secrets.token_bytes(16)
        users[row["username"]] = (salt,hashlib.pbkdf2_hmac("sha256",password.encode(),salt,200000),accounts)
    if not users: raise ValueError("web.users required; no anonymous access")
    sessions = {}
    attempts = deque()
    sockets = set()
    def permission(request):
        token = request.headers.get("Authorization","").removeprefix("Bearer ")
        session = sessions.get(token)
        if not session or session[0]<time.time(): raise web.HTTPUnauthorized()
        return session[1]
    @web.middleware
    async def guard(request,handler):
        origin = request.headers.get("Origin")
        same = f"{request.scheme}://{request.host}"
        if origin and origin not in allowed and origin!=same: raise web.HTTPForbidden()
        if request.method=="OPTIONS": response=web.Response()
        else:
            try: response = await handler(request)
            except web.HTTPException as exc:
                response=web.Response(status=exc.status,text=exc.text,headers=exc.headers)
        if origin and (origin in allowed or origin==same):
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Vary"] = "Origin"
            response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Cache-Control"]="no-store"
        response.headers["X-Content-Type-Options"]="nosniff"
        return response
    app = web.Application(middlewares=[guard],client_max_size=16384)
    async def login(request):
        now=time.time()
        while attempts and now-attempts[0]>60: attempts.popleft()
        if len(attempts)>=20: raise web.HTTPTooManyRequests()
        attempts.append(now)
        try: data=await request.json()
        except (ValueError, UnicodeError): raise web.HTTPBadRequest()
        if not isinstance(data,dict): raise web.HTTPBadRequest()
        user=users.get(str(data.get("username","")))
        salt,expected,accounts=user or (b"invalid-user-salt",bytes(32),set())
        actual=await asyncio.to_thread(hashlib.pbkdf2_hmac,"sha256",str(data.get("password","")).encode(),salt,200000)
        if not user or not hmac.compare_digest(actual,expected): raise web.HTTPUnauthorized()
        for key,(expiry,_) in list(sessions.items()):
            if expiry<now: sessions.pop(key,None)
        if len(sessions)>=100: raise web.HTTPTooManyRequests()
        token=secrets.token_urlsafe(32)
        sessions[token]=(now+8*3600,accounts)
        return web.json_response({"token":token,"accounts":sorted(accounts)})
    async def logout(request):
        sessions.pop(request.headers.get("Authorization","").removeprefix("Bearer "),None)
        return web.json_response({"ok":True})
    async def rows(request):
        accounts=permission(request)
        account=request.match_info["account"]
        if account not in accounts: raise web.HTTPForbidden()
        kind=request.match_info["kind"]
        if kind not in {"matches","unmatched","exposures","remain","recentTrades"}: raise web.HTTPNotFound()
        try: page=max(0,int(request.query.get("page",0)))
        except ValueError: raise web.HTTPBadRequest()
        result=live.results.get(account,{})
        data=result.get(kind,[])
        return web.json_response({"rows":data[page*50:(page+1)*50],"total":len(data),"version":result.get("version")})
    async def download(request):
        accounts=permission(request)
        account=request.match_info["account"]
        day=request.match_info["day"]
        kind=request.match_info["kind"]
        if account not in accounts: raise web.HTTPForbidden()
        if not re.fullmatch(r"\d{8}",day): raise web.HTTPBadRequest()
        if kind=="xlsx":
            root=live.root/"output"/account/day
            path=root/f"{account}_{day}.xlsx"
        elif kind in {"trade_callbacks","unmatched","exposure_matches","exposure_remain","funding"}:
            root=Path(live.accounts[account])/day
            path=root/f"{kind}.jsonl"
        else: raise web.HTTPNotFound()
        if not path.resolve().is_relative_to(root.resolve()) or not path.is_file(): raise web.HTTPNotFound()
        return web.FileResponse(path,headers={"Content-Disposition":f'attachment; filename="{path.name}"'})
    async def socket(request):
        if len(sockets)>=20: raise web.HTTPTooManyRequests()
        ws=web.WebSocketResponse(heartbeat=20,max_msg_size=8192)
        await ws.prepare(request)
        sockets.add(ws)
        key=secrets.token_hex(8)
        reader=None
        try:
            # Token in first frame, never query string/access logs.
            hello=await asyncio.wait_for(ws.receive_json(),10)
            if not isinstance(hello,dict): return ws
            token=hello.get("token")
            session=sessions.get(token)
            if not session or session[0]<time.time(): return ws
            account=hello.get("account")
            if account not in session[1]: return ws
            keys=[]
            legs=hello.get("legs",[])
            if not isinstance(legs,list): return ws
            for leg in legs[:2]:
                if not isinstance(leg,dict): return ws
                market,symbol=leg.get("market"),str(leg.get("symbol","")).upper()
                if market not in {"spot","um"} or not re.fullmatch(r"[A-Z0-9]{2,30}",symbol): return ws
                keys.append((market,symbol))
            live.watchers[key]=set(keys)
            async def receive():
                async for message in ws:
                    if message.type==WSMsgType.ERROR: break
            reader=asyncio.create_task(receive())
            while not ws.closed and not reader.done() and sessions.get(token,(0,))[0]>time.time():
                await asyncio.wait_for(ws.send_json(live.snapshot(account,keys)),2)
                await asyncio.sleep(.25)
        except (asyncio.TimeoutError,ConnectionError,ValueError,RuntimeError): pass
        finally:
            if reader:
                reader.cancel()
                await asyncio.gather(reader,return_exceptions=True)
            live.watchers.pop(key,None)
            sockets.discard(ws)
            await ws.close()
        return ws
    app.router.add_post("/api/login",login)
    app.router.add_post("/api/logout",logout)
    app.router.add_get("/api/rows/{account}/{kind}",rows)
    app.router.add_get("/api/download/{account}/{day}/{kind}",download)
    app.router.add_get("/api/live",socket)
    frontend=Path(__file__).parent/"frontend"
    async def index(request): return web.FileResponse(frontend/"index.html")
    app.router.add_get("/",index)
    app.router.add_static("/static",frontend/"static",show_index=False)
    async def close_sockets(app):
        await asyncio.gather(*(ws.close() for ws in list(sockets)),return_exceptions=True)
    app.on_shutdown.append(close_sockets)
    return app
