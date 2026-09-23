/* Plain HTTP is permitted only for a same-origin, private IPv4/loopback page. */
(function (root) {
  "use strict";
  function localHost(host) {
    if (["localhost", "127.0.0.1", "[::1]"].includes(host)) return true;
    const parts = host.split(".");
    if (parts.length !== 4 || parts.some(p => !/^\d{1,3}$/.test(p) || Number(p) > 255)) return false;
    const [a,b] = parts.map(Number);
    return a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
  }
  function backendOrigin(value, pageOrigin) {
    const url = new URL(value);
    if (!["http:","https:"].includes(url.protocol) || url.username || url.password)
      throw new Error("请输入 HTTP / HTTPS 后端地址，不要在地址中包含密码");
    if (url.protocol === "https:") return url.origin;
    const page = new URL(pageOrigin);
    if (page.protocol !== "http:" || page.origin !== url.origin || !localHost(url.hostname))
      throw new Error("HTTP仅允许从同地址的本机或局域网页面登录；请直接打开 http://服务器局域网IP:8080，不要从Vercel页面连接");
    return url.origin;
  }
  root.monitorBackendOrigin = backendOrigin;
  if (typeof module !== "undefined" && module.exports) module.exports = {backendOrigin};
})(typeof window === "undefined" ? globalThis : window);
