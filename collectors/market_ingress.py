"""Spawn-safe public-market receiver. No credentials, reports or disk IO here."""
import asyncio
import multiprocessing
import queue
import time
import logging


def stamp(raw, connection_id, sequence):
    # Capture before JSON parsing, IPC or any business processing.
    wall_ns = time.time_ns()
    mono_ns = time.perf_counter_ns()
    return {"rawPayload": raw, "receivedTimeUs": wall_ns // 1000,
            "receivedMonoNs": mono_ns, "connectionId": connection_id,
            "receiveSequence": sequence, "timestampBasis": "application_ws_receive",
            "clockSyncStatus": "unverified"}


def worker(market, root, proxy, commands, output, stop, drops):
    # Child diagnostics are forwarded as metadata; do not corrupt the dashboard.
    logging.getLogger().handlers = [logging.NullHandler()]
    # Import inside the spawned child to avoid import cycles on Windows.
    from .market_collector import MarketCollector

    async def main():
        import aiohttp
        collector = MarketCollector(root, {"proxy": proxy})
        def publish(record):
            record["ingressDroppedTotal"] = drops.value
            try:
                output.put_nowait(record)
            except queue.Full:
                with drops.get_lock():
                    drops.value += 1
        collector.raw_sink = publish
        collector.quote_event = lambda *args, **kwargs: None
        collector.emit = lambda key, record, metadata=False: publish({"metadata": record, "symbol": key[1]})
        async def controls():
            while not stop.is_set():
                try:
                    desired = commands.get_nowait()
                    while True:
                        try: desired = commands.get_nowait()
                        except queue.Empty: break
                    for key in list(collector.states):
                        if key[1] not in desired: del collector.states[key]
                    for symbol in desired:
                        collector.ensure((market, symbol), time.time())
                except queue.Empty:
                    pass
                await asyncio.sleep(0.05)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15), trust_env=True) as session:
            collector.session = session
            controller = asyncio.create_task(controls())
            receiver = asyncio.create_task(collector.stream(market))
            try:
                while not stop.is_set():
                    if receiver.done():
                        await receiver
                        break
                    await asyncio.sleep(0.1)
            finally:
                collector.stopping = True
                receiver.cancel()
                controller.cancel()
                await asyncio.gather(receiver, controller, return_exceptions=True)
    try:
        asyncio.run(main())
    finally:
        # Parent continues draining during shutdown; flush the feeder before exit.
        output.close()
        output.join_thread()


def launch(market, root, proxy, capacity):
    ctx = multiprocessing.get_context("spawn")
    commands, output = ctx.Queue(8), ctx.Queue(capacity)
    stop, drops = ctx.Event(), ctx.Value("q", 0)
    process = ctx.Process(target=worker, args=(market, root, proxy, commands, output, stop, drops), daemon=True)
    process.start()
    return process, commands, output, stop, drops


def take_batch(output, limit=256):
    records = []
    try:
        records.append(output.get(timeout=0.2))
        while len(records) < limit:
            try: records.append(output.get_nowait())
            except queue.Empty: break
    except queue.Empty:
        pass
    return records
