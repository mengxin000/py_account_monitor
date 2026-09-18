# -*- coding: utf-8 -*-
# ========== 现货 key 探测脚本（只读，不产生交易） ==========
# 用途：只读比较普通 Spot、PM Margin、UM 的账户/挂单可见性
# 说明：支持 HMAC，以及 PEM/DER 格式的 Ed25519 私钥；不会创建或删除 listenKey
import base64
import argparse
import hashlib
import hmac
import json
import logging
from datetime import datetime
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from cryptography.hazmat.primitives.serialization import load_der_private_key, load_pem_private_key
    HAVE_ED = True
except ImportError:
    HAVE_ED = False

BASE_API = "https://api.binance.com"      # 现货
BASE_PAPI = "https://papi.binance.com"    # 统一账户

# 禁止在源码中保存密钥。运行时从账户配置 JSON 读取。
API_KEY = ""
API_SECRET = ""
_ed25519_key = None

# 与监控程序一致的本地代理
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 7897

_time_offset = 0


def load_credentials(path: Path):
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    api_key = str(data.get("api_key") or data.get("apiKey") or "").strip()
    secret = str(data.get("secret_key") or data.get("secretKey") or data.get("secKey") or "").strip()
    if not api_key or not secret:
        raise ValueError(f"配置缺少 api_key/secret_key: {path}")
    return api_key, secret


def detect_signer(secret):
    """Return an Ed25519 private key when the secret is PEM/DER; otherwise HMAC."""
    if not HAVE_ED:
        return None
    try:
        if secret.startswith("-----BEGIN"):
            return load_pem_private_key(secret.encode("utf-8"), password=None)
        return load_der_private_key(base64.b64decode(secret, validate=True), password=None)
    except Exception:
        return None


def _opener():
    if PROXY_HOST and PROXY_PORT:
        proxy = f"http://{PROXY_HOST}:{PROXY_PORT}"
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({'http': proxy, 'https': proxy}))
    return urllib.request.build_opener()


def _sign(query):
    """Ed25519 key → 私钥签名 + base64；HMAC key → sha256 hex"""
    if _ed25519_key is not None:
        return base64.b64encode(_ed25519_key.sign(query.encode('utf-8'))).decode()
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()


def sync_time():
    global _time_offset
    with _opener().open(f"{BASE_API}/api/v3/time", timeout=15) as r:
        server_time = json.loads(r.read().decode())["serverTime"]
    _time_offset = server_time - int(time.time() * 1000)


def request(base, path, params=None, method="GET"):
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000) + _time_offset
    params["recvWindow"] = 60000
    # Ed25519/HMAC 签名：signatureType 不放入 URL（API key 已绑定签名算法，服务器自动识别）
    unsigned_query = urllib.parse.urlencode(sorted(params.items()))
    params["signature"] = _sign(unsigned_query)
    # urlencode the signature too: Ed25519 base64 contains +, / and =.
    query = urllib.parse.urlencode(sorted(params.items()))
    url = f"{base}{path}?{query}"
    req = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": API_KEY})
    with _opener().open(req, timeout=20) as r:
        return json.loads(r.read().decode())


def test(name, fn):
    print(f"\n===== {name} =====")
    try:
        data = fn()
        print(">>> 成功")
        return data
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f">>> 失败 HTTP {e.code}: {body[:300]}")
    except Exception as e:
        print(f">>> 失败 {e}")
    return None


def show_account(acct):
    balances = acct.get("balances", [])
    print(f"    现货资产种类: {len(balances)} 项")
    for b in balances:
        free = float(b.get("free", 0) or 0)
        locked = float(b.get("locked", 0) or 0)
        if free != 0 or locked != 0:
            print(f"    {b['asset']}: free={free} locked={locked}")


def show_open_orders(orders, label="挂单"):
    if not orders:
        print(f"    当前无{label}（查询成功）")
        return
    print(f"    当前{label} {len(orders)} 笔:")
    for o in orders:
        print(f"    {o['symbol']} {o['side']} qty={o['origQty']} price={o['price']} "
              f"type={o['type']} status={o['status']} "
              f"clientOrderId={o.get('clientOrderId')} orderId={o.get('orderId')} "
              f"time={o.get('time')}")


def run_probe(symbol):
    print("签名类型: " + ("Ed25519" if _ed25519_key is not None else "HMAC-SHA256"))
    print(f"代理: {PROXY_HOST}:{PROXY_PORT}")
    sync_time()
    print(f"时间偏移: {_time_offset}ms")

    acct = test("1. GET /api/v3/account 现货账户/余额", lambda: request(BASE_API, "/api/v3/account"))
    if acct:
        show_account(acct)

    spot_orders = test(f"2. GET /api/v3/openOrders 普通Spot挂单 {symbol}",
                       lambda: request(BASE_API, "/api/v3/openOrders", {"symbol": symbol}))
    if spot_orders is not None:
        show_open_orders(spot_orders, "普通Spot挂单")

    margin_orders = test(f"3. GET /papi/v1/margin/openOrders PM Margin挂单 {symbol}",
                         lambda: request(BASE_PAPI, "/papi/v1/margin/openOrders", {"symbol": symbol}))
    if margin_orders is not None:
        show_open_orders(margin_orders, "PM Margin挂单")

    um_orders = test(f"4. GET /papi/v1/um/openOrders UM挂单 {symbol}",
                     lambda: request(BASE_PAPI, "/papi/v1/um/openOrders", {"symbol": symbol}))
    if um_orders is not None:
        show_open_orders(um_orders, "UM挂单")

    papi = test("5. GET /papi/v1/account 统一账户签名校验",
                lambda: request(BASE_PAPI, "/papi/v1/account"))
    if papi:
        print(f"    UMA 账户: actualEquity={papi.get('actualEquity')} "
              f"totalWalletBalance={papi.get('totalWalletBalance')}")

    print("\n===== 账本判断 =====")
    if isinstance(spot_orders, list) and spot_orders and isinstance(margin_orders, list) and not margin_orders:
        print("订单只出现在 /api/v3：属于普通 Spot，需要独立 Spot User Data Stream。")
    elif isinstance(margin_orders, list) and margin_orders and isinstance(spot_orders, list) and not spot_orders:
        print("订单只出现在 /papi/v1/margin：属于 PM Margin，应检查 PM 用户流推送。")
    elif isinstance(spot_orders, list) and not spot_orders and isinstance(margin_orders, list) and not margin_orders:
        print(f"两个现货账本当前都没有 {symbol} 挂单；请在挂单存续期间使用 --watch 复测。")
    elif isinstance(spot_orders, list) and isinstance(margin_orders, list):
        print("两个账本均有挂单，请根据订单ID/clientOrderId确认交易程序实际使用的下单通道。")
    else:
        print("至少一个关键请求失败，请先根据上面的 Binance 错误码处理签名、权限或IP白名单。")

    print("\n===== 探测完成 =====")


def watch_open_orders(symbol=None, interval=1.0, heartbeat=5.0):
    """持续轮询现货挂单（短间隔防错过挂撤）：有挂单打印完整明细，无挂单定时心跳"""
    print(f"开始持续监控现货挂单（间隔 {interval:.1f}s，币种: {symbol or '全部'}，Ctrl+C 停止）...")
    print("提示: 让交易侧挂单后保持几秒再撤，便于捕捉；脚本会完整打印每笔挂单明细")
    last_sig = None
    last_beat = 0.0
    while True:
        try:
            params = {"symbol": symbol} if symbol else {}
            orders = request(BASE_API, "/api/v3/openOrders", params)
            sig = tuple((o['symbol'], o['side'], o.get('price'), o.get('origQty'),
                         o['orderId'], o.get('clientOrderId'), o.get('status'),
                         o.get('time')) for o in orders)
            now = time.time()
            if sig != last_sig:
                last_sig = sig
                ts = time.strftime('%H:%M:%S')
                if orders:
                    print(f"[{ts}] 捕获到挂单 {len(orders)} 笔:")
                    for o in orders:
                        print(f"    {o['symbol']} {o['side']} qty={o['origQty']} "
                              f"price={o['price']} type={o['type']} "
                              f"clientOrderId={o.get('clientOrderId')} orderId={o['orderId']} "
                              f"status={o.get('status')} time={o.get('time')}")
                else:
                    print(f"[{ts}] 挂单已清空")
            elif now - last_beat >= heartbeat:
                last_beat = now
                print(f"[{time.strftime('%H:%M:%S')}] 无变化（当前 0 笔挂单，持续监听中...）")
        except KeyboardInterrupt:
            print("\n停止监控")
            break
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] 查询失败: {e}")
        time.sleep(interval)


def main():
    global API_KEY, API_SECRET, _ed25519_key, PROXY_HOST, PROXY_PORT
    parser = argparse.ArgumentParser(description="只读探测普通 Spot、PM Margin 与 UM 账户范围")
    parser.add_argument("--config", type=Path, default=Path(__file__).parent / "config" / "account_dh.local.json",
                        help="包含 api_key/secret_key 的账户配置")
    parser.add_argument("--symbol", default="AAVEUSDT")
    parser.add_argument("--watch", action="store_true", help="持续探测（默认行为，保留兼容）")
    parser.add_argument("--once", action="store_true", help="仅运行一次原有诊断")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--no-proxy", action="store_true")
    args = parser.parse_args()

    API_KEY, API_SECRET = load_credentials(args.config)
    _ed25519_key = detect_signer(API_SECRET)
    if args.no_proxy:
        PROXY_HOST, PROXY_PORT = "", 0
    if args.once:
        run_probe(args.symbol)
    else:
        continuous_probe(args.config, args.symbol, max(1.0, args.interval))


def continuous_probe(config_path, symbol, interval):
    """Persist each read-only REST snapshot, including empty results and errors."""
    log_dir = Path(__file__).parent / "log" / "probe"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("spot_key_probe")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = None
    active_day = None
    next_sync = 0.0
    endpoints = (
        ("SPOT", BASE_API, "/api/v3/openOrders"),
        ("PM_MARGIN", BASE_PAPI, "/papi/v1/margin/openOrders"),
        ("UM", BASE_PAPI, "/papi/v1/um/openOrders"),
    )
    print(f"持续REST探测 {symbol}，每轮间隔 {interval}s，Ctrl+C停止")
    print(f"代理: {PROXY_HOST}:{PROXY_PORT}；日志目录: {log_dir}")
    try:
        while True:
            day = datetime.now().strftime("%Y%m%d")
            if active_day != day:
                if handler:
                    logger.removeHandler(handler)
                    handler.close()
                handler = logging.FileHandler(log_dir / f"{day}_{config_path.stem}_spot_probe.txt", encoding="utf-8")
                handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
                logger.addHandler(handler)
                active_day = day
                logger.info("probe started symbol=%s proxy=%s:%s interval=%s", symbol, PROXY_HOST, PROXY_PORT, interval)
            if time.monotonic() >= next_sync:
                try:
                    sync_time()
                    logger.info("server time synchronized offset_ms=%s", _time_offset)
                    next_sync = time.monotonic() + 300
                except Exception as exc:
                    logger.error("time sync failed type=%s", type(exc).__name__)
                    print("时间同步失败，5秒后重试，详情见日志")
                    time.sleep(5)
                    continue
            status = []
            for label, base, path in endpoints:
                try:
                    result = request(base, path, {"symbol": symbol})
                    logger.info("market=%s path=%s symbol=%s response=%s", label, path, symbol,
                                json.dumps(result, ensure_ascii=False, separators=(",", ":")))
                    status.append(f"{label}={len(result) if isinstance(result, list) else '异常响应'}")
                except urllib.error.HTTPError as exc:
                    body = exc.read().decode(errors="replace")
                    logger.error("market=%s path=%s HTTP=%s response=%s", label, path, exc.code, body)
                    status.append(f"{label}=HTTP{exc.code}")
                    if '"code":-1021' in body.replace(" ", ""):
                        next_sync = 0
                except Exception as exc:
                    # Exception URLs may contain signed parameters; never log them.
                    reason = getattr(exc, "reason", None)
                    logger.error("market=%s path=%s error_type=%s reason_type=%s errno=%s", label, path,
                                 type(exc).__name__, type(reason).__name__, getattr(reason, "errno", None))
                    status.append(f"{label}=连接失败")
            print(f"{datetime.now():%H:%M:%S} " + " | ".join(status), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("probe stopped by user")
        print("探测已停止，日志已保存")
    finally:
        if handler:
            logger.removeHandler(handler)
            handler.close()


if __name__ == "__main__":
    main()
