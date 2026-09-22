"""Account-scoped order state; journal events newer than REST win."""
from datetime import datetime


class Orders:
    def __init__(self):
        self.rows = {}
        self.times = {}
        self.snapshot_at = {}

    @staticmethod
    def key(scope, symbol, order_id): return scope, symbol, str(order_id)

    def event(self,row):
        event = row.get("data",row)
        order = event.get("o") if isinstance(event.get("o"),dict) else event
        scope = row.get("accountScope") or str(event.get("fs", "unknown")).lower()
        if not order.get("s") or "i" not in order: return
        key = self.key(scope,order["s"],order["i"])
        try: at = int(datetime.fromisoformat(row["receivedTime"]).timestamp()*1000)
        except (KeyError,ValueError): at = int(order.get("T",0))
        if at < max(self.times.get(key,0),self.snapshot_at.get(scope,0)): return
        self.times[key] = at
        if order.get("X") in {"NEW","PARTIALLY_FILLED","PENDING_NEW"}:
            self.rows[key] = {"scope":scope,"market":"spot" if scope in {"spot","pm_margin"} else scope,
                              "symbol":order["s"],"orderId":str(order["i"]),"clientId":order.get("c",""),
                              "side":order.get("S"),"price":order.get("p"),
                              "remaining":max(0,float(order.get("q",0))-float(order.get("z",0))),"asOf":at}
        else: self.rows.pop(key,None)

    def reconcile(self,scope,rows,at):
        if at < self.snapshot_at.get(scope,0): return
        self.snapshot_at[scope] = at
        for key in list(self.rows):
            if key[0]==scope and self.times.get(key,0)<=at: self.rows.pop(key,None)
        for row in rows:
            key = self.key(scope,row["symbol"],row["orderId"])
            if self.times.get(key,0)>at: continue
            self.rows[key] = {"scope":scope,"market":"spot" if scope in {"spot","pm_margin"} else scope,
                              "symbol":row["symbol"],"orderId":str(row["orderId"]),"clientId":row.get("clientOrderId",""),
                              "side":row["side"],"price":row["price"],
                              "remaining":max(0,float(row["origQty"])-float(row.get("executedQty",0))),"asOf":at}
        self.times = {key:timestamp for key,timestamp in self.times.items() if key[0]!=scope or timestamp>at}
