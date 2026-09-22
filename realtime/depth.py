"""Disposable 20-level market snapshots; no depth history is written to disk."""
import asyncio
import json
import multiprocessing
import queue
import time
import uuid
from decimal import Decimal

import aiohttp
try:
    from ..collectors.market_ingress import stamp, take_batch
    from ..connections.diagnostics import connection_error
except ImportError:
    from collectors.market_ingress import stamp, take_batch
    from connections.diagnostics import connection_error


def normalize_depth(market, payload, envelope):
    stream = payload.get("stream", "")
    data = payload.get("data", payload)
    symbol = data.get("s") or stream.split("@")[0].upper()
    bids, asks = data.get("bids", data.get("b")), data.get("asks", data.get("a"))
    if not symbol or not isinstance(bids,list) or not isinstance(asks,list): return None
    def levels(rows, reverse):
        return sorted([[str(p),str(q)] for p,q in rows if Decimal(str(q)) > 0], key=lambda x:Decimal(x[0]),reverse=reverse)[:15]
    return {"market":market,"symbol":symbol,"bids":levels(bids,True),"asks":levels(asks,False),
            "receivedTimeUs":envelope["receivedTimeUs"],"receivedMonoNs":envelope["receivedMonoNs"],
            "connectionId":envelope["connectionId"],"sequence":envelope["receiveSequence"],
            "eventTime":data.get("E"),"transactionTime":data.get("T")}


def depth_worker(market, commands, output, stop, proxy):
    parent = multiprocessing.parent_process()
    def running():
        return not stop.is_set() and (parent is None or parent.is_alive())
    async def run():
        endpoint = "wss://stream.binance.com:443/stream" if market == "spot" else "wss://fstream.binance.com/stream"
        desired = set()
        dropped = 0
        def publish(value):
            nonlocal dropped
            value["dropped"] = dropped
            try: output.put_nowait(value)
            except queue.Full:
                dropped += 1
                # Depth is disposable. Prefer the newest snapshot over backlog.
                try: output.get_nowait()
                except queue.Empty: pass
                try: output.put_nowait(value)
                except queue.Full: pass
        while running():
            try:
                try:
                    while True: desired = set(commands.get_nowait())
                except queue.Empty: pass
                if not desired:
                    await asyncio.sleep(.2)
                    continue
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None,sock_connect=10),trust_env=True) as session:
                    async with session.ws_connect(endpoint,heartbeat=20,proxy=proxy) as ws:
                        connection = uuid.uuid4().hex
                        sequence = 0
                        subscribed = set()
                        pending = {}
                        request_id = 0
                        next_control = 0
                        while running():
                            try:
                                while True: desired = set(commands.get_nowait())
                            except queue.Empty: pass
                            if not desired: break
                            if pending and time.monotonic() - min(x[2] for x in pending.values()) > 10:
                                raise TimeoutError("subscription acknowledgment")
                            if not pending and time.monotonic() >= next_control:
                                additions, removals = desired-subscribed, subscribed-desired
                                method, symbols = ("UNSUBSCRIBE", removals) if removals else ("SUBSCRIBE",additions)
                                if symbols:
                                    request_id += 1
                                    await ws.send_json({"id":request_id,"method":method,"params":[f"{s.lower()}@depth20@100ms" for s in sorted(symbols)]})
                                    pending[request_id] = (method,symbols,time.monotonic())
                                    next_control = time.monotonic()+.3
                            try: msg = await ws.receive(timeout=.2)
                            except asyncio.TimeoutError: continue
                            sequence += 1
                            envelope = stamp(msg.data,connection,sequence)
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in {aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR}: raise ConnectionError()
                                continue
                            # Raw envelope crosses IPC before JSON interpretation.
                            publish({"market":market,"envelope":envelope})
                            data = json.loads(msg.data)
                            if data.get("id") in pending:
                                method,symbols,_ = pending.pop(data["id"])
                                if "result" not in data or data["result"] is not None: raise ValueError("depth subscription rejected")
                                subscribed = subscribed|symbols if method=="SUBSCRIBE" else subscribed-symbols
                            if data.get("data",data).get("e") == "serverShutdown": raise ConnectionError()
            except Exception as exc:
                publish({"market":market,"error":connection_error(exc,endpoint)})
                for _ in range(30):
                    if not running(): break
                    await asyncio.sleep(.1)
    try: asyncio.run(run())
    finally: output.cancel_join_thread()


class DepthService:
    def __init__(self, proxy=None):
        self.proxy = proxy
        self.workers = {}
        self.books = {}
        self.errors = {}
        self.closed = False

    def request(self, keys):
        ctx = multiprocessing.get_context("spawn")
        for market in ("spot","um"):
            symbols = sorted(s for m,s in keys if m==market)[:100]
            if symbols and market not in self.workers:
                commands, output, stop = ctx.Queue(2),ctx.Queue(4096),ctx.Event()
                process = ctx.Process(target=depth_worker,args=(market,commands,output,stop,self.proxy),daemon=True)
                process.start()
                self.workers[market] = (process,commands,output,stop)
            if market in self.workers:
                try: self.workers[market][1].put_nowait(symbols)
                except queue.Full: pass
        for key in list(self.books):
            if key not in keys: del self.books[key]

    async def run(self):
        while not self.closed:
            for market,(process,commands,output,stop) in list(self.workers.items()):
                if not process.is_alive():
                    self.errors[market] = "receiver exited; restarting on next request"
                    process.join(); process.close()
                    commands.close(); output.close()
                    del self.workers[market]
                    continue
                for row in await asyncio.to_thread(take_batch,output):
                    if "error" in row:
                        self.errors[market] = row["error"]
                        continue
                    try:
                        envelope = row["envelope"]
                        book = normalize_depth(market,json.loads(envelope["rawPayload"]),envelope)
                        if book:
                            book["dropped"] = row.get("dropped",0)
                            self.books[(market,book["symbol"])] = book
                            self.errors.pop(market,None)
                    except (ValueError,TypeError,KeyError): self.errors[market] = "invalid depth message"
            await asyncio.sleep(.02)

    async def close(self):
        self.closed = True
        for process,commands,output,stop in self.workers.values(): stop.set()
        for process,commands,output,stop in self.workers.values():
            await asyncio.to_thread(process.join,2)
            if process.is_alive(): process.terminate(); await asyncio.to_thread(process.join,2)
            process.close(); commands.close(); output.close()
