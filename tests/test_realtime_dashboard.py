import json
import os
import queue
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer
from realtime.engine import Journal, AccountProjection
from realtime.depth import normalize_depth
from realtime.orders import Orders
from web.app import LiveData, make_app, trading_day
from replay.batch_replay import replay_day


def trade(ident,side,quantity,price,timestamp,client=None,status="FILLED",fee="0.01"):
    return {"accountId":"zdl","accountScope":"um","data":{"e":"ORDER_TRADE_UPDATE","fs":"UM","o":{
        "s":"AAVEUSDT","i":ident,"c":client or str(ident),"S":side,"X":status,"x":"TRADE",
        "q":str(quantity),"z":str(quantity),"l":str(quantity),"L":str(price),"p":str(price),"n":fee,"N":"USDT","T":timestamp,"t":timestamp}}}


class ProjectionTests(unittest.TestCase):
    def test_journal_partial_line_and_atomic_error(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"events.jsonl"
            path.write_bytes(b'{"x":1}\n{"x":')
            journal=Journal(path)
            self.assertEqual(journal.read()[0],[(0,{"x":1})])
            self.assertEqual(journal.offset,8)
            with path.open("ab") as f: f.write(b'2}\n')
            self.assertEqual(journal.read()[0][0][1],{"x":2})
            with path.open("ab") as f: f.write(b'{}\nbad\n')
            offset=journal.offset
            with self.assertRaises(ValueError): journal.read()
            self.assertEqual(journal.offset,offset)
            path.write_bytes(b'{}\n')
            rows,reset=journal.read()
            self.assertTrue(reset)
            self.assertEqual(rows,[(0,{})])

    def test_incremental_matches_fees_exposure_and_formal_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/"trade_callbacks.jsonl"
            events=[trade(123,"BUY",.3,100,1000,fee="0.03",status="PARTIALLY_FILLED"),
                    trade(123,"BUY",1,100,1000,fee="0.07"),trade(456,"SELL",1,101,1001,"hedge-123",fee="0.1"),
                    trade(777,"BUY",.3,100,1100,"exposure-buy",fee="0.03"),
                    trade(888,"SELL",.4,101,1200,"exposure-sell",fee="0.04")]
            path.write_text("",encoding="utf-8")
            projection=AccountProjection(root)
            for event in events:
                with path.open("a",encoding="utf-8") as f:f.write(json.dumps(event)+"\n")
                result=projection.refresh(now=900000)
            self.assertEqual(result["pairCount"],1)
            self.assertAlmostEqual(result["pairProfit"],.8)
            self.assertAlmostEqual(result["exposureProfit"],.24)
            self.assertEqual(result["unmatchedCount"],2)
            self.assertAlmostEqual(result["remain"][0]["quantity"],.1)
            self.assertAlmostEqual(result["remain"][0]["fee"],.01)
            self.assertFalse((root/"matches").exists())
            with patch.object(projection,"view",side_effect=AssertionError("unchanged must use cache")):
                self.assertIs(result,projection.refresh(now=900001))
            replay_day(root)
            formal=json.loads((root/"matches"/"AAVE.jsonl").read_text())
            self.assertEqual(formal,result["matches"][0])
            self.assertAlmostEqual(json.loads((root/"exposure_matches.jsonl").read_text())["profit_delta"],result["exposureProfit"])
            projection.last_check=-3600
            audited=projection.refresh(now=900001)
            self.assertEqual(audited["checkStatus"],"一致")

    def test_duplicate_late_event_recovery(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/"trade_callbacks.jsonl"
            early=trade(123,"BUY",1,100,1000)
            late=trade(456,"SELL",1,101,1100,"hedge-123")
            path.write_text(json.dumps(late)+"\n",encoding="utf-8")
            projection=AccountProjection(root);projection.refresh(now=1200)
            with path.open("a") as f:f.write(json.dumps(early)+"\n"+json.dumps(early)+"\n")
            result=projection.refresh(now=1200)
            self.assertEqual(result["pairCount"],1)
            self.assertEqual(len(projection.events),2)
            self.assertEqual(result["matches"],AccountProjection(root).refresh(now=1200)["matches"])

    def test_failed_application_does_not_acknowledge_trade(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            (root/"trade_callbacks.jsonl").write_text(json.dumps(trade(1,"BUY",1,100,1000))+"\n",encoding="utf-8")
            projection=AccountProjection(root)
            with patch.object(projection,"ingest",side_effect=ValueError("temporary failure")):
                with self.assertRaises(ValueError):projection.refresh(now=2000)
            self.assertEqual(projection.journal.offset,0)
            self.assertEqual(projection.refresh(now=2000)["unmatchedCount"],1)

    def test_concatenated_records_keep_numeric_order(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"events.jsonl"
            path.write_text('{"id":1}{"id":2}\n',encoding="utf-8")
            rows,_=Journal(path).read()
            self.assertEqual([r[1]["id"] for r in rows],[1,2])
            self.assertLess(rows[0][0],rows[1][0])

    def test_order_rest_race_and_scope(self):
        orders=Orders()
        event=trade(1,"BUY",1,100,2000,status="NEW")
        orders.event(event)
        orders.reconcile("um",[],1000)
        self.assertEqual(len(orders.rows),1)
        spot={**event,"accountScope":"spot"};orders.event(spot)
        self.assertEqual(len(orders.rows),2)
        orders.event(trade(1,"BUY",1,100,3000))
        orders.reconcile("um",[{"symbol":"AAVEUSDT","orderId":1,"side":"BUY","price":"100","origQty":"1"}],2500)
        self.assertEqual(len(orders.rows),1)

    def test_depth_15_levels_and_missing_exchange_time(self):
        envelope={"receivedTimeUs":123456789,"receivedMonoNs":10,"connectionId":"a","receiveSequence":1}
        book=normalize_depth("spot",{"stream":"aaveusdt@depth20@100ms","data":{"bids":[[str(i),"1"] for i in range(1,21)],"asks":[[str(i),"2"] for i in range(21,41)]}},envelope)
        self.assertEqual(len(book["bids"]),15)
        self.assertEqual(book["bids"][0],["20","1"])
        self.assertEqual(book["symbol"],"AAVEUSDT")
        self.assertIsNone(book["eventTime"])
        self.assertEqual(book["receivedTimeUs"],123456789)
        book=normalize_depth("um",{"s":"AAVEUSDT","b":[["1","2"]],"a":[["2","3"]],"E":10,"T":9},envelope)
        self.assertEqual(book["transactionTime"],9)


class WebTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder=tempfile.TemporaryDirectory();root=Path(self.folder.name)
        self.live=LiveData({"zdl":root/"zdl","dh":root/"dh"},root,queue.Queue())
        self.live.refresh()
        with patch.dict(os.environ,{"TEST_MONITOR_PASSWORD":"a-long-test-password"}):
            app=make_app(self.live,{"users":[{"username":"viewer","password_env":"TEST_MONITOR_PASSWORD","accounts":["zdl"]}],"allowed_origins":["https://example.vercel.app"]})
        self.client=TestClient(TestServer(app));await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close();self.folder.cleanup()

    async def login(self):
        response=await self.client.post("/api/login",json={"username":"viewer","password":"a-long-test-password"})
        self.assertEqual(response.status,200)
        return (await response.json())["token"]

    async def test_auth_permissions_cors_and_static(self):
        self.assertEqual((await self.client.get("/api/rows/zdl/matches")).status,401)
        self.assertEqual((await self.client.post("/api/login",json=[])).status,400)
        token=await self.login();headers={"Authorization":"Bearer "+token}
        self.assertEqual((await self.client.get("/api/rows/dh/matches",headers=headers)).status,403)
        self.assertEqual((await self.client.get("/api/rows/zdl/matches",headers=headers)).status,200)
        self.assertEqual((await self.client.get("/",headers={"Origin":"https://evil.test"})).status,403)
        response=await self.client.options("/api/login",headers={"Origin":"https://example.vercel.app"})
        self.assertEqual(response.headers["Access-Control-Allow-Origin"],"https://example.vercel.app")
        self.assertEqual((await self.client.get("/static/app.js")).status,200)
        self.assertEqual((await self.client.get("/api/download/zdl/20260922/equity",headers=headers)).status,404)

    async def test_websocket_auth_snapshot_logout(self):
        token=await self.login()
        ws=await self.client.ws_connect("/api/live")
        await ws.send_json({"token":token,"account":"zdl","legs":[{"market":"spot","symbol":"AAVEUSDT"}]})
        snapshot=await ws.receive_json(timeout=2)
        self.assertEqual(snapshot["account"],"zdl")
        self.assertTrue(snapshot["books"][0]["stale"])
        await self.client.post("/api/logout",headers={"Authorization":"Bearer "+token})
        await ws.receive(timeout=2)
        await ws.close()
        self.assertEqual((await self.client.get("/api/rows/zdl/matches",headers={"Authorization":"Bearer "+token})).status,401)

    async def test_day_boundary(self):
        from datetime import datetime
        with patch("web.app.datetime") as date:
            date.now.return_value=datetime(2026,9,22,9,29,59)
            self.assertEqual(trading_day(),"20260921")
            date.now.return_value=datetime(2026,9,22,9,30)
            self.assertEqual(trading_day(),"20260922")


if __name__=="__main__":unittest.main()
