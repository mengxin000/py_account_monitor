"""Optional read-only sidecar; failure must not stop the trading journal."""
import asyncio
import json
import logging
import multiprocessing
import queue
import time
from pathlib import Path

LOGGER=logging.getLogger("binance_web_supervisor")


def worker(accounts,root,config,bridge,stop,proxy,matching_rules=None):
    from aiohttp import web
    try:
        from ..web.app import LiveData,make_app
        from .logging_support import TradingDayFileHandler
    except ImportError:
        from web.app import LiveData,make_app
        from service.logging_support import TradingDayFileHandler
    handler=TradingDayFileHandler(Path(root)/"log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().handlers=[handler]
    logging.getLogger().setLevel(logging.INFO)
    async def run():
        live=LiveData(accounts,root,bridge,proxy,matching_rules)
        runner=web.AppRunner(make_app(live,config),access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner,config.get("host","127.0.0.1"),int(config.get("port",8080))).start()
            LOGGER.info("read-only dashboard listening host=%s port=%s",config.get("host","127.0.0.1"),config.get("port",8080))
            tasks=[asyncio.create_task(live.run()),asyncio.create_task(live.depth.run())]
            try:
                while not stop.is_set():
                    for task in tasks:
                        if task.done(): await task
                    await asyncio.sleep(.2)
            finally:
                for task in tasks: task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                await live.depth.close()
        finally: await runner.cleanup()
    try: asyncio.run(run())
    except Exception: LOGGER.exception("web sidecar stopped")
    finally: handler.close()


async def supervise(monitors,root,config,proxy):
    """Keep sidecar startup/configuration failures out of the collector gather."""
    try:
        await _supervise(monitors,root,config,proxy)
    except Exception:
        LOGGER.exception("dashboard disabled after supervisor failure; analysis continues")


async def _supervise(monitors,root,config,proxy):
    if not config.get("enabled",False): return
    ctx=multiprocessing.get_context("spawn")
    while True:
        bridge,stop=ctx.Queue(64),ctx.Event()
        accounts={m.config.account_id:str(m.store.root) for m in monitors}
        rules={m.config.account_id:getattr(m.config,"matching_rules",None) for m in monitors}
        process=ctx.Process(target=worker,args=(accounts,str(root),config,bridge,stop,proxy,rules))
        process.start()
        def publish(row):
            try: bridge.put_nowait(row)
            except queue.Full: pass  # latest snapshots only; journal is durable
        async def orders():
            while True:
                for monitor in monitors:
                    routes=[]
                    if monitor.rest and monitor.config.spot_mode == "pm_margin":
                        routes.append(("pm_margin",monitor.rest,"/papi/v1/margin/openOrders"))
                    if monitor.rest and monitor.config.futures_mode == "pm_um":
                        routes.append(("um",monitor.rest,"/papi/v1/um/openOrders"))
                    if monitor.usdm_rest:
                        routes.append(("usdm",monitor.usdm_rest,"/fapi/v1/openOrders"))
                    if monitor.spot_stream: routes.append(("spot",monitor.spot_stream.rest,"/api/v3/openOrders"))
                    for scope,rest,path in routes:
                        at=int(time.time()*1000)
                        try:
                            response=await rest.get(path,signed=True)
                            rows=response if isinstance(response,list) else response.get("data",[])
                            if not isinstance(rows,list): raise ValueError("invalid openOrders")
                            publish({"kind":"orders","account":monitor.config.account_id,"scope":scope,"asOf":at,"rows":rows})
                        except Exception:
                            LOGGER.warning("dashboard order reconciliation failed account=%s scope=%s",monitor.config.account_id,scope)
                    if monitor.hyperliquid:
                        at=int(time.time()*1000)
                        try:
                            rows=await monitor.hyperliquid.open_orders()
                            publish({"kind":"orders","account":monitor.config.account_id,"scope":"hyperliquid","asOf":at,"rows":rows})
                        except Exception:
                            LOGGER.warning("dashboard Hyperliquid order reconciliation failed account=%s", monitor.config.account_id, exc_info=True)
                await asyncio.sleep(60)
        order_task=asyncio.create_task(orders())
        try:
            while process.is_alive():
                for monitor in monitors:
                    status=json.loads(json.dumps(monitor.status(),default=lambda x:x.isoformat()))
                    publish({"kind":"status","account":monitor.config.account_id,"asOf":int(time.time()*1000),"data":status})
                await asyncio.sleep(1)
        finally:
            order_task.cancel()
            await asyncio.gather(order_task,return_exceptions=True)
            stop.set()
            await asyncio.to_thread(process.join,8)
            if process.is_alive(): process.terminate(); await asyncio.to_thread(process.join,2)
            process.close();bridge.cancel_join_thread();bridge.close()
        LOGGER.error("dashboard sidecar exited; analysis continues; retry in 30s")
        await asyncio.sleep(30)
