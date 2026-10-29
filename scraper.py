import asyncio
import ipaddress
import json
import logging
import os
import re
import time
import aiohttp
import asyncpg
from aiohttp_socks import ProxyConnector

DATABASE_URL = "postgres.jbchsqovebxpsfecevjy:kGepBVhNgInCkGlm@aws-0-sa-east-1.pooler.supabase.com"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1565501043855466547/I7Rl1P3jZNZE5dtgqh6P0trQBetv47ByOPwWY0xAZbYbu38Fht6beWvVc_mcKcJ13SQz"

SCRAPE_INTERVAL = 1800
REVERIFY_INTERVAL = 3600
FAST_REVERIFY = 300
FAST_TIMEOUT = 2
FULL_TIMEOUT = 4
WORKERS = 200
QUEUE_MAX = 5000
BATCH_PERSIST = 2000
ABORT_THRESHOLD = 0.90
WARMUP_CHECKS = 100
MAX_SCRAPE_CONCURRENCY = 8
DISCORD_RATE_LIMIT_DELAY = 2.0

TEST_URLS = [
    "https://api.ipify.org?format=json",
    "https://api64.ipify.org?format=json",
    "https://httpbin.org/ip",
    "https://ipinfo.io/json",
    "https://checkip.amazonaws.com",
    "https://icanhazip.com",
    "http://ip-api.com/json",
    "https://ifconfig.co/json",
]

SOURCES = {
    "proxyscrape": {
        "url": "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.json",
        "parser": "proxyscrape_json",
    },
    "speedx_http": {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "parser": "plain",
    },
    "speedx_socks4": {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "parser": "plain",
    },
    "speedx_socks5": {
        "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "parser": "plain",
    },
    "monosans_http": {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "parser": "plain",
    },
    "monosans_socks4": {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "parser": "plain",
    },
    "monosans_socks5": {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "parser": "plain",
    },
    "monosans_all": {
        "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt",
        "parser": "plain",
    },
    "openproxy_http": {
        "url": "https://api.openproxylist.xyz/http.txt",
        "parser": "plain",
    },
    "openproxy_socks4": {
        "url": "https://api.openproxylist.xyz/socks4.txt",
        "parser": "plain",
    },
    "openproxy_socks5": {
        "url": "https://api.openproxylist.xyz/socks5.txt",
        "parser": "plain",
    },
    "shiftytr": {
        "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt",
        "parser": "plain",
    },
    "clarketm": {
        "url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "parser": "plain",
    },
    "opsxcq": {
        "url": "https://raw.githubusercontent.com/opsxcq/proxy-list/master/list.txt",
        "parser": "plain",
    },
    "roosterkid_https": {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "parser": "plain",
    },
    "roosterkid_socks4": {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "parser": "plain",
    },
    "roosterkid_socks5": {
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "parser": "plain",
    },
    "jetkai_http": {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "parser": "plain",
    },
    "jetkai_https": {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
        "parser": "plain",
    },
    "jetkai_socks4": {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
        "parser": "plain",
    },
    "jetkai_socks5": {
        "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
        "parser": "plain",
    },
    "proxyhub_http": {
        "url": "https://raw.githubusercontent.com/ProxyHub/ProxyHub/main/proxy-list/http.txt",
        "parser": "plain",
    },
    "proxyhub_socks4": {
        "url": "https://raw.githubusercontent.com/ProxyHub/ProxyHub/main/proxy-list/socks4.txt",
        "parser": "plain",
    },
    "proxyhub_socks5": {
        "url": "https://raw.githubusercontent.com/ProxyHub/ProxyHub/main/proxy-list/socks5.txt",
        "parser": "plain",
    },
    "xroxy_http": {
        "url": "https://raw.githubusercontent.com/XR0X/Proxy-List/master/http.txt",
        "parser": "plain",
    },
    "xroxy_socks4": {
        "url": "https://raw.githubusercontent.com/XR0X/Proxy-List/master/socks4.txt",
        "parser": "plain",
    },
    "xroxy_socks5": {
        "url": "https://raw.githubusercontent.com/XR0X/Proxy-List/master/socks5.txt",
        "parser": "plain",
    },
    "proxyjudge_http": {
        "url": "https://raw.githubusercontent.com/proxyjudge/proxy-list/master/http.txt",
        "parser": "plain",
    },
    "proxyjudge_socks4": {
        "url": "https://raw.githubusercontent.com/proxyjudge/proxy-list/master/socks4.txt",
        "parser": "plain",
    },
    "proxyjudge_socks5": {
        "url": "https://raw.githubusercontent.com/proxyjudge/proxy-list/master/socks5.txt",
        "parser": "plain",
    },
    "hidemyna": {
        "url": "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-List/master/hidemyna.txt",
        "parser": "plain",
    },
    "abhinav": {
        "url": "https://raw.githubusercontent.com/abhinav78910/proxy-list/master/list.txt",
        "parser": "plain",
    },
    "rjrodriguez": {
        "url": "https://raw.githubusercontent.com/rjrodriguez/Proxy-List/master/proxies.txt",
        "parser": "plain",
    },
    "firehol_http": {
        "url": "https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/proxies_http.txt",
        "parser": "plain",
    },
    "firehol_socks4": {
        "url": "https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/proxies_socks4.txt",
        "parser": "plain",
    },
    "firehol_socks5": {
        "url": "https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/proxies_socks5.txt",
        "parser": "plain",
    },
    "blacksnow_https": {
        "url": "https://raw.githubusercontent.com/BlackSnowDot/proxy-list/master/https.txt",
        "parser": "plain",
    },
    "blacksnow_all": {
        "url": "https://raw.githubusercontent.com/BlackSnowDot/proxy-list/master/all.txt",
        "parser": "plain",
    },
    "mmpx12_http": {
        "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "parser": "plain",
    },
    "mmpx12_socks4": {
        "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        "parser": "plain",
    },
    "mmpx12_socks5": {
        "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        "parser": "plain",
    },
    "ahmad_chen_http": {
        "url": "https://raw.githubusercontent.com/ahmadchen/proxy-list/main/proxies/http.txt",
        "parser": "plain",
    },
    "ahmad_chen_socks4": {
        "url": "https://raw.githubusercontent.com/ahmadchen/proxy-list/main/proxies/socks4.txt",
        "parser": "plain",
    },
    "ahmad_chen_socks5": {
        "url": "https://raw.githubusercontent.com/ahmadchen/proxy-list/main/proxies/socks5.txt",
        "parser": "plain",
    },
    "proxylist_http": {
        "url": "https://raw.githubusercontent.com/roxylist/Proxy-List/main/http.txt",
        "parser": "plain",
    },
    "proxylist_socks4": {
        "url": "https://raw.githubusercontent.com/roxylist/Proxy-List/main/socks4.txt",
        "parser": "plain",
    },
    "proxylist_socks5": {
        "url": "https://raw.githubusercontent.com/roxylist/Proxy-List/main/socks5.txt",
        "parser": "plain",
    },
    "unixfreak_http": {
        "url": "https://raw.githubusercontent.com/UnixFreak/Proxy-List/master/http.txt",
        "parser": "plain",
    },
    "unixfreak_socks4": {
        "url": "https://raw.githubusercontent.com/UnixFreak/Proxy-List/master/socks4.txt",
        "parser": "plain",
    },
    "unixfreak_socks5": {
        "url": "https://raw.githubusercontent.com/UnixFreak/Proxy-List/master/socks5.txt",
        "parser": "plain",
    },
    "newlife_http": {
        "url": "https://raw.githubusercontent.com/NewLifeX/Proxy-List/master/http.txt",
        "parser": "plain",
    },
    "newlife_socks4": {
        "url": "https://raw.githubusercontent.com/NewLifeX/Proxy-List/master/socks4.txt",
        "parser": "plain",
    },
    "newlife_socks5": {
        "url": "https://raw.githubusercontent.com/NewLifeX/Proxy-List/master/socks5.txt",
        "parser": "plain",
    },
}

PROXY_RE = re.compile(r"(?:(?:https?|socks[45])://)?(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("proxy_pool")

_webhook_lock = asyncio.Lock()
_last_webhook_sent = 0.0
_notified_addrs = set()


async def db_open():
    conn = await asyncpg.connect(dsn=DATABASE_URL, statement_cache_size=0)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS proxies (
            addr TEXT PRIMARY KEY,
            protocol TEXT DEFAULT 'http',
            source TEXT,
            country TEXT,
            anonymity TEXT,
            ok INTEGER DEFAULT 0,
            score REAL DEFAULT 0.0,
            latency_ms INTEGER,
            fail_streak INTEGER DEFAULT 0,
            last_checked BIGINT,
            last_ok BIGINT,
            created BIGINT
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_ok ON proxies(ok)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON proxies(score DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_lastok ON proxies(last_ok)")
    return conn


async def db_persist_batch(conn, ok_rows, fail_rows, now):
    if ok_rows:
        await conn.executemany(
            """UPDATE proxies
               SET ok=1,
                   score=LEAST(score + 1.0, 20.0),
                   latency_ms=$1,
                   fail_streak=0,
                   last_checked=$2,
                   last_ok=$3
               WHERE addr=$4""",
            ok_rows,
        )
    if fail_rows:
        await conn.executemany(
            """UPDATE proxies
               SET ok=0,
                   score=GREATEST(score - 0.5, -20.0),
                   fail_streak=fail_streak + 1,
                   last_checked=$1
               WHERE addr=$2""",
            fail_rows,
        )


def is_valid_public_ip(ip_str):
    try:
        ip = ipaddress.ip_address(ip_str)
        return not (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_multicast)
    except ValueError:
        return False


def _parse_json_safe(text):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def parse_proxyscrape_json(text):
    data = _parse_json_safe(text)
    if not isinstance(data, list):
        return []
    out = []
    for p in data:
        if not isinstance(p, dict):
            continue
        ip = p.get("ip", "")
        port = p.get("port")
        if not is_valid_public_ip(ip):
            continue
        try:
            port = int(port)
            if not (1 <= port <= 65535):
                continue
        except (ValueError, TypeError):
            continue
        out.append((f"{ip}:{port}", str(p.get("protocol", "http")).lower(),
                    "proxyscrape", p.get("country_code"), p.get("anonymity")))
    return out


def parse_proxmint_json(text):
    data = _parse_json_safe(text)
    if data is None:
        return []
    if isinstance(data, dict):
        data = data.get("proxies", [])
    if not isinstance(data, list):
        return []
    out = []
    for p in data:
        if not isinstance(p, dict):
            continue
        ip = p.get("ip", "")
        port = p.get("port")
        if not is_valid_public_ip(ip):
            continue
        try:
            port = int(port)
            if not (1 <= port <= 65535):
                continue
        except (ValueError, TypeError):
            continue
        out.append((f"{ip}:{port}", str(p.get("protocol", "http")).lower(),
                    "proxmint", p.get("countryCode"), p.get("anonymity")))
    return out


def parse_plain(text):
    out = []
    seen = set()
    for ip, port in PROXY_RE.findall(text):
        if not is_valid_public_ip(ip):
            continue
        port = int(port)
        if not (1 <= port <= 65535):
            continue
        addr = f"{ip}:{port}"
        if addr in seen:
            continue
        seen.add(addr)
        out.append((addr, "http", None, None, None))
    return out


PARSERS = {
    "proxyscrape_json": parse_proxyscrape_json,
    "proxmint_json": parse_proxmint_json,
    "plain": parse_plain,
}


async def scrape_one(session, cfg):
    try:
        async with session.get(cfg["url"], timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            return PARSERS[cfg["parser"]](await r.text(errors="ignore"))
    except Exception as e:
        log.warning("scrape failed %s: %r", cfg["url"], e)
        return []


async def scrape_all(session, db_conn):
    sem = asyncio.Semaphore(MAX_SCRAPE_CONCURRENCY)

    async def bounded_scrape(name, cfg):
        async with sem:
            rows = await scrape_one(session, cfg)
            return name, rows

    tasks = [bounded_scrape(name, cfg) for name, cfg in SOURCES.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    rows = {}
    now = int(time.time())

    for res in results:
        if isinstance(res, Exception):
            continue
        name, batch = res
        for addr, proto, src, country, anon in batch:
            if addr not in rows:
                rows[addr] = (addr, proto or "http", name if src is None else src, country, anon, now)

    if not rows:
        return

    try:
        await db_conn.executemany(
            """INSERT INTO proxies(addr, protocol, source, country, anonymity, created, last_checked)
               VALUES($1,$2,$3,$4,$5,$6,0)
               ON CONFLICT(addr) DO UPDATE SET
                 protocol=COALESCE(EXCLUDED.protocol, proxies.protocol),
                 source=COALESCE(EXCLUDED.source, proxies.source),
                 country=COALESCE(EXCLUDED.country, proxies.country),
                 anonymity=COALESCE(EXCLUDED.anonymity, proxies.anonymity)""",
            list(rows.values()),
        )
        log.info("scrape: %d entries", len(rows))
    except Exception as e:
        log.error("scrape persist failed: %r", e)


def _extract_reported_ip(body):
    data = _parse_json_safe(body)
    if not isinstance(data, dict):
        return None
    reported = data.get("origin") or data.get("ip") or data.get("query")
    if not reported:
        return None
    return str(reported).split(",")[0].strip()


async def check_http(session, addr, timeout):
    proxy_ip = addr.split(":")[0]
    for url in TEST_URLS:
        try:
            t0 = time.perf_counter()
            async with session.get(
                url, proxy=f"http://{addr}",
                timeout=aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout)),
            ) as r:
                if r.status != 200:
                    continue
                reported = _extract_reported_ip(await r.text())
                if not reported or reported == proxy_ip:
                    continue
                return True, int((time.perf_counter() - t0) * 1000)
        except Exception:
            continue
    return False, 0


async def check_socks(addr, proto, timeout):
    proxy_ip = addr.split(":")[0]
    for url in TEST_URLS:
        connector = None
        try:
            t0 = time.perf_counter()
            connector = ProxyConnector.from_url(f"{proto}://{addr}", rdns=True)
            async with aiohttp.ClientSession(connector=connector) as sess:
                async with sess.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout)),
                ) as r:
                    if r.status != 200:
                        continue
                    reported = _extract_reported_ip(await r.text())
                    if not reported or reported == proxy_ip:
                        continue
                    return True, int((time.perf_counter() - t0) * 1000)
        except Exception:
            continue
        finally:
            if connector is not None:
                try:
                    await connector.close()
                except Exception:
                    pass
    return False, 0


async def verify_one(session, addr, protocol, timeout):
    try:
        if protocol in ("socks4", "socks5"):
            return addr, await check_socks(addr, protocol, timeout)
        return addr, await check_http(session, addr, timeout)
    except Exception:
        return addr, (False, 0)


async def notify_discord(session, addr, protocol, latency):
    global _last_webhook_sent
    if addr in _notified_addrs:
        return
    async with _webhook_lock:
        if addr in _notified_addrs:
            return
        now = time.monotonic()
        wait = DISCORD_RATE_LIMIT_DELAY - (now - _last_webhook_sent)
        if wait > 0:
            await asyncio.sleep(wait)
        payload = {
            "content": f"Proxy válida: `{addr}` | protocolo: `{protocol}` | latência: `{latency}ms`"
        }
        try:
            async with session.post(
                DISCORD_WEBHOOK, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    retry = (await r.json()).get("retry_after", 5)
                    await asyncio.sleep(float(retry) + 0.5)
                elif r.status in (200, 204):
                    _notified_addrs.add(addr)
        except Exception as e:
            log.warning("discord webhook failed: %r", e)
        finally:
            _last_webhook_sent = time.monotonic()


async def worker(session, queue, results, timeout, sem):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        addr, protocol = item
        async with sem:
            r = await verify_one(session, addr, protocol, timeout)
        ok, latency = r[1]
        if ok:
            await notify_discord(session, addr, protocol, latency)
        results.append(r)
        queue.task_done()


async def run_verification(session, addrs, timeout):
    queue = asyncio.Queue(maxsize=QUEUE_MAX)
    results = []
    sem = asyncio.Semaphore(WORKERS)

    workers = [asyncio.create_task(worker(session, queue, results, timeout, sem))
               for _ in range(WORKERS)]

    for item in addrs:
        await queue.put(item)
    for _ in range(WORKERS):
        await queue.put(None)
    await queue.join()
    await asyncio.gather(*workers, return_exceptions=True)
    return results


async def reverify(session, db_conn, only_ok=False, fast=False):
    q = "SELECT addr, protocol FROM proxies"
    if only_ok:
        q += " WHERE ok=1"
    all_rows = await db_conn.fetch(q)

    if not all_rows:
        return

    timeout = FAST_TIMEOUT if fast else FULL_TIMEOUT
    now = int(time.time())
    total = len(all_rows)

    log.info("reverify: %d proxies (fast=%s)", total, fast)
    results = await run_verification(session, [(r["addr"], r["protocol"]) for r in all_rows], timeout)

    dead = sum(1 for _, (ok, _) in results if not ok)
    if total >= WARMUP_CHECKS and dead / total > ABORT_THRESHOLD:
        log.error("abort reverify: %.0f%% dead - skipping persist", dead / total * 100)
        return

    ok_rows, fail_rows = [], []
    for addr, (ok, lat) in results:
        if ok:
            ok_rows.append((lat, now, now, addr))
        else:
            fail_rows.append((now, addr))

    for i in range(0, len(ok_rows), BATCH_PERSIST):
        await db_persist_batch(db_conn, ok_rows[i:i + BATCH_PERSIST], [], now)
    for i in range(0, len(fail_rows), BATCH_PERSIST):
        await db_persist_batch(db_conn, [], fail_rows[i:i + BATCH_PERSIST], now)

    await db_conn.execute(
        """DELETE FROM proxies
           WHERE score <= -10 AND fail_streak >= 3
           AND created < $1
           AND (last_ok IS NULL OR last_ok < $2)""",
        now - 3600, now - 86400)

    alive = await db_conn.fetchval("SELECT COUNT(*) FROM proxies WHERE ok=1")
    tot = await db_conn.fetchval("SELECT COUNT(*) FROM proxies")
    log.info("reverify: %d/%d alive", alive, tot)


async def main():
    db_conn = await db_open()
    connector = aiohttp.TCPConnector(
        limit=WORKERS,
        limit_per_host=0,
        use_dns_cache=True,
        ttl_dns_cache=300,
    )
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            await scrape_all(session, db_conn)

            next_scrape = time.monotonic() + SCRAPE_INTERVAL
            next_fast_reverify = time.monotonic()
            next_full_reverify = time.monotonic()

            while True:
                try:
                    now = time.monotonic()
                    if now >= next_scrape:
                        await scrape_all(session, db_conn)
                        next_scrape = now + SCRAPE_INTERVAL

                    if now >= next_fast_reverify:
                        await reverify(session, db_conn, only_ok=True, fast=True)
                        next_fast_reverify = now + FAST_REVERIFY

                    if now >= next_full_reverify:
                        await reverify(session, db_conn, only_ok=False, fast=False)
                        next_full_reverify = now + REVERIFY_INTERVAL
                except Exception as e:
                    log.exception("main loop error: %r", e)

                sleep_until = min(next_scrape, next_fast_reverify, next_full_reverify)
                await asyncio.sleep(max(0.05, sleep_until - time.monotonic()))
    finally:
        await db_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
