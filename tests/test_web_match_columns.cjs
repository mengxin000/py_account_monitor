const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const root=path.resolve(__dirname,'..');
const context=vm.createContext({
  document:{getElementById:()=>({addEventListener(){}})},
  location:{origin:'http://localhost:8080',protocol:'http:'},setInterval(){}
});
vm.runInContext(fs.readFileSync(path.join(root,'web/frontend/static/app.js'),'utf8'),context);
test('columns match Excel with only slippage inserted after direction',()=>{
  const source=fs.readFileSync(path.join(root,'reports/excel_report.py'),'utf8');
  const headers=JSON.parse(source.match(/ORDER_HEADERS = (\[[\s\S]*?\])/)[1].replace(/,\s*]/,']'));
  headers.splice(2,0,'滑点（bps）');
  assert.deepEqual(JSON.parse(vm.runInContext('JSON.stringify(columns.matches)',context)),headers);
});
test('signed slippage bps, missing values and pair field alignment',()=>{
  const evaluate=row=>vm.runInContext(`slippageBps(${JSON.stringify(row)})`,context);
  for(const [side,spread,offset,expected] of [['SELL',.001,.0006,4],['SELL',.0006,.001,-4],['BUY',.0006,.001,4],['BUY',.001,.0006,-4]]) {
    assert.ok(Math.abs(evaluate({quoted_spread_side:side,quoted_spread:spread,offset})-expected)<1e-9);
  }
  assert.equal(evaluate({quoted_spread_side:'SELL',offset:0}),null);
  assert.equal(evaluate({quoted_spread:0,offset:0}),null);
  const row={current_symbol:'AAVEUSDC',match_symbol:'AAVEUSDT',quoted_spread_side:'SELL',quoted_spread:.001,offset:.0006,quantity:.3,current_side:'SELL',match_side:'BUY',current_price:100,match_price:99,current_fee:.01,match_fee:.02,profit:.27,event_time_ms:1787989150447};
  const cells=JSON.parse(vm.runInContext(`kind='matches';JSON.stringify(cells(${JSON.stringify(row)}))`,context));
  assert.equal(cells.length,19);
  assert.equal(cells[1],'开仓');assert.equal(cells[2],'4.000');
  assert.equal(cells[8],'SELL');assert.equal(cells[14],'BUY');
  assert.equal(cells[9],cells[15]);
  assert.match(cells[5],/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.447$/);
});
