const {test} = require("node:test");
const assert = require("node:assert/strict");
const {backendOrigin} = require("../web/frontend/static/network-policy.js");

test("same-origin private IPv4 and loopback HTTP", () => {
  for(const host of ["192.168.112.238","10.0.0.1","172.16.0.1","172.31.255.254","127.0.0.1","localhost","[::1]"]) {
    const origin = `http://${host}:8080`;
    assert.equal(backendOrigin(origin+"/",origin),origin);
  }
});
test("public HTTP, different origin and HTTPS mixed content are rejected", () => {
  for(const host of ["8.8.8.8","172.15.1.1","172.32.1.1","192.169.1.1","example.com"]) {
    const origin=`http://${host}:8080`;
    assert.throws(()=>backendOrigin(origin,origin));
  }
  assert.throws(()=>backendOrigin("http://192.168.112.238:8080","https://site.vercel.app"));
  assert.throws(()=>backendOrigin("http://192.168.112.238:8080","http://192.168.112.238:8765"));
  assert.throws(()=>backendOrigin("http://127.0.0.1:8080","https://site.vercel.app"));
});
test("HTTPS remains supported; credentials and invalid protocols rejected", () => {
  assert.equal(backendOrigin("https://api.example.com/","https://site.vercel.app"),"https://api.example.com");
  assert.throws(()=>backendOrigin("https://user:pass@api.example.com","https://site.vercel.app"));
  assert.throws(()=>backendOrigin("file:///abc","http://localhost:8080"));
});
