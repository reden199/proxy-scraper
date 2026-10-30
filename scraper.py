import asyncio
import ipaddress
import json
import logging
import os
import random
import re
import time
import uuid
import aiohttp
import asyncpg
from aiohttp import web
from aiohttp_socks import ProxyConnector

DATABASE_URL = "postgresql://postgres.jbchsqovebxpsfecevjy:kGepBVhNgInCkGlm@aws-0-us-east-1.pooler.supabase.com:5432/postgres"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1565501043855466547/I7Rl1P3jZNZE5dtgqh6P0trQBetv47ByOPwWY0xAZbYbu38Fht6beWvVc_mcKcJ13SQz"


INSTANCE_ID = os.environ.get("INSTANCE_ID", f"render-{uuid.uuid4().hex[:8]}")
PORT = int(os.environ.get("PORT", 10000))

SCRAPE_INTERVAL = 1800
REVERIFY_INTERVAL = 3600
FAST_REVERIFY = 300
FAST_TIMEOUT = 2
FULL_TIMEOUT = 4
WORKERS = 200
QUEUE_MAX = 5000
ABORT_THRESHOLD = 0.90
WARMUP_CHECKS = 100
MAX_SCRAPE_CONCURRENCY = 8
DISCORD_RATE_LIMIT_DELAY = 2.0
PERSIST_BUFFER_SIZE = 50
CLAIM_BATCH_SIZE = 50
CLAIM_TIMEOUT = 300
SCRAPE_LOCK_TIMEOUT = 300
WORKER_HARD_TIMEOUT = 30
BATCH_HARD_TIMEOUT = 600
RETRY_BATCH_SIZE = CLAIM_BATCH_SIZE
ZOMBIE_BACKLOG_PER_INSTANCE = 500
ZOMBIE_POLL_INTERVAL = 10
INSTANCE_HEARTBEAT_TIMEOUT = 120

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


class PersistBuffer:
    def __init__(self, conn, size=PERSIST_BUFFER_SIZE):
        self.conn = conn
        self.size = size
        self.ok_buf = []
        self.fail_buf = []
        self.lock = asyncio.Lock()

    async def add_ok(self, addr, latency):
        async with self.lock:
            now = int(time.time())
            self.ok_buf.append((latency, now, now, addr))
            if len(self.ok_buf) >= self.size:
                log.info("persist: flushing %d ok", len(self.ok_buf))
                await self._flush_ok()

    async def add_fail(self, addr):
        async with self.lock:
            now = int(time.time())
            self.fail_buf.append((now, addr))
            if len(self.fail_buf) >= self.size:
                log.info("persist: flushing %d fail", len(self.fail_buf))
                await self._flush_fail()

    async def _flush_ok(self):
        if not self.ok_buf:
            return
        batch = self.ok_buf
        await db_persist_batch(self.conn, batch, [])
        self.ok_buf = []

    async def _flush_fail(self):
        if not self.fail_buf:
            return
        batch = self.fail_buf
        await db_persist_batch(self.conn, [], batch)
        self.fail_buf = []

    async def flush_all(self):
        async with self.lock:
            await self._flush_ok()
            await self._flush_fail()


class DatabasePool:
    def __init__(self, pool):
        self.pool = pool

    async def execute(self, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(*args)

    async def executemany(self, *args):
        async with self.pool.acquire() as conn:
            return await conn.executemany(*args)

    async def fetch(self, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(*args)

    async def fetchrow(self, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(*args)

    async def fetchval(self, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(*args)

    async def close(self):
        await self.pool.close()


async def db_open():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required")

    log.info("db_open: connecting")
    try:
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=1,
            statement_cache_size=0,
            command_timeout=60,
        )
        log.info("db_open: connected")
    except Exception:
        log.exception("db_open: connect failed")
        raise

    try:
        async with pool.acquire() as conn:
            log.info("db_open: creating schema")
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
                    created BIGINT,
                    claimed_until BIGINT,
                    claimed_by TEXT,
                    notified INTEGER DEFAULT 0
                )
            """)
            log.info("db_open: table ok")
            await conn.execute("ALTER TABLE proxies ADD COLUMN IF NOT EXISTS claimed_until BIGINT")
            await conn.execute("ALTER TABLE proxies ADD COLUMN IF NOT EXISTS claimed_by TEXT")
            await conn.execute("ALTER TABLE proxies ADD COLUMN IF NOT EXISTS notified INTEGER DEFAULT 0")
            await conn.execute("ALTER TABLE proxies ADD COLUMN IF NOT EXISTS url_index INTEGER DEFAULT 0")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_url_index ON proxies(url_index)")
            log.info("db_open: alter ok")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_ok ON proxies(ok)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON proxies(score DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_lastok ON proxies(last_ok)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_claimed ON proxies(claimed_until)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notified ON proxies(notified)")
            log.info("db_open: indexes ok")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS scrape_lock (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    locked_until BIGINT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS instances (
                    instance_id TEXT PRIMARY KEY,
                    number INTEGER UNIQUE NOT NULL,
                    created BIGINT,
                    last_seen BIGINT
                )
            """)
            await conn.execute("ALTER TABLE instances ADD COLUMN IF NOT EXISTS last_seen BIGINT")
            log.info("db_open: aux tables ok")
    except Exception:
        await pool.close()
        raise
    return DatabasePool(pool)


async def get_or_create_instance_number(conn, instance_id):
    row = await conn.fetchrow(
        "SELECT number FROM instances WHERE instance_id=$1", instance_id)
    if row:
        await conn.execute(
            "UPDATE instances SET last_seen=$2 WHERE instance_id=$1",
            instance_id, int(time.time()))
        log.info("instance: %s has number %d", instance_id, row["number"])
        return row["number"]

    for _ in range(50):
        candidate = random.randint(1000, 9999)
        try:
            await conn.execute(
                """INSERT INTO instances(instance_id, number, created, last_seen)
                   VALUES($1,$2,$3,$3)""",
                instance_id, candidate, int(time.time()))
            log.info("instance: %s assigned number %d", instance_id, candidate)
            return candidate
        except asyncpg.UniqueViolationError:
            continue

    raise RuntimeError("could not allocate unique instance number")


async def try_scrape_lock(conn, timeout=SCRAPE_LOCK_TIMEOUT):
    now = int(time.time())
    result = await conn.fetchval("""
        INSERT INTO scrape_lock(id, locked_until) VALUES(1, $1)
        ON CONFLICT(id) DO UPDATE
        SET locked_until = $1
        WHERE scrape_lock.locked_until < $2
        RETURNING 1
    """, now + timeout, now)
    return result is not None


async def db_persist_batch(conn, ok_rows, fail_rows):
    if ok_rows:
        log.info("persist_batch: %d ok", len(ok_rows))
        await conn.executemany(
            """UPDATE proxies
               SET ok=1,
                   score=LEAST(score + 1.0, 20.0),
                   latency_ms=$1,
                   fail_streak=0,
                   last_checked=$2,
                   last_ok=$3,
                   url_index=0,
                   claimed_until=NULL,
                   claimed_by=NULL
               WHERE addr=$4""",
            ok_rows,
        )
    if fail_rows:
        log.info("persist_batch: %d fail", len(fail_rows))
        await conn.executemany(
            """UPDATE proxies
               SET ok=0,
                   score=GREATEST(score - 0.5, -20.0),
                   fail_streak=fail_streak + 1,
                   last_checked=$1,
                   url_index=url_index + 1,
                   claimed_until=NULL,
                   claimed_by=NULL
               WHERE addr=$2""",
            fail_rows,
        )

async def claim_new_batch(conn, batch_size=CLAIM_BATCH_SIZE, timeout=CLAIM_TIMEOUT):
    now = int(time.time())
    log.info("claim_new_batch: requesting %d", batch_size)
    rows = await conn.fetch("""
        WITH claimed AS (
            SELECT addr FROM proxies
            WHERE url_index = 0
              AND (claimed_until IS NULL OR claimed_until < $1)
              AND (last_checked IS NULL OR last_checked < $1)
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE proxies p
        SET claimed_until = $1 + $3,
            claimed_by = $4
        FROM claimed
        WHERE p.addr = claimed.addr
        RETURNING p.addr, p.protocol, p.url_index
    """, now, batch_size, timeout, INSTANCE_ID)
    log.info("claim_new_batch: got %d", len(rows))
    return rows


async def claim_retry_batch(conn, batch_size=RETRY_BATCH_SIZE, timeout=CLAIM_TIMEOUT):
    now = int(time.time())
    log.info("claim_retry_batch: requesting %d", batch_size)
    rows = await conn.fetch("""
        WITH claimed AS (
            SELECT addr FROM proxies
            WHERE url_index > 0
              AND url_index < $5
              AND (claimed_until IS NULL OR claimed_until < $1)
              AND (last_checked IS NULL OR last_checked < $1)
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE proxies p
        SET claimed_until = $1 + $3,
            claimed_by = $4
        FROM claimed
        WHERE p.addr = claimed.addr
        RETURNING p.addr, p.protocol, p.url_index
    """, now, batch_size, timeout, INSTANCE_ID, len(TEST_URLS))
    log.info("claim_retry_batch: got %d", len(rows))
    return rows


async def get_zombie_helper_assignment(conn, instance_id):
    now = int(time.time())
    row = await conn.fetchrow("""
        WITH active AS (
            SELECT instance_id,
                   ROW_NUMBER() OVER (ORDER BY last_seen ASC, number) AS instance_rank
            FROM instances
            WHERE last_seen >= $1
        ),
        pending AS (
            SELECT COUNT(*) AS total
            FROM proxies
            WHERE url_index > 0 AND url_index < $3
        )
        SELECT pending.total AS pending_zombies,
               (SELECT COUNT(*) FROM active) AS active_instances,
               (SELECT instance_rank FROM active WHERE instance_id=$2) AS instance_rank
        FROM pending
    """, now - INSTANCE_HEARTBEAT_TIMEOUT, instance_id, len(TEST_URLS))
    await conn.execute(
        "UPDATE instances SET last_seen=$2 WHERE instance_id=$1",
        instance_id, now)

    pending = row["pending_zombies"]
    active = row["active_instances"]
    rank = row["instance_rank"]
    desired_workers = min(
        active,
        (pending + ZOMBIE_BACKLOG_PER_INSTANCE - 1)
        // ZOMBIE_BACKLOG_PER_INSTANCE,
    )
    is_worker = (
        pending > 0
        and active > 0
        and rank is not None
        and rank <= desired_workers
    )
    log.debug(
        "zombie assignment: pending=%d active=%d helpers=%d rank=%s worker=%s",
        pending, active, desired_workers, rank, is_worker)
    return is_worker, pending, desired_workers, active


async def mark_notified(conn, addr):
    await conn.execute(
        "UPDATE proxies SET notified=1 WHERE addr=$1 AND notified=0",
        addr
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
        log.info("scrape_one: fetching %s", cfg["url"])
        async with session.get(cfg["url"], timeout=aiohttp.ClientTimeout(total=15)) as r:
            log.info("scrape_one: %s status %d", cfg["url"], r.status)
            if r.status != 200:
                return []
            text = await r.text(errors="ignore")
            parsed = PARSERS[cfg["parser"]](text)
            log.info("scrape_one: %s parsed %d", cfg["url"], len(parsed))
            return parsed
    except Exception as e:
        log.warning("scrape failed %s: %r", cfg["url"], e)
        return []


def protocol_for_source(source_name, parsed_protocol):
    source_protocol = source_name.rsplit("_", 1)[-1].lower()
    if source_protocol in {"http", "https", "socks4", "socks5"}:
        return source_protocol
    return parsed_protocol or "http"


async def scrape_all(session, db_conn):
    log.info("scrape_all: start")
    sem = asyncio.Semaphore(MAX_SCRAPE_CONCURRENCY)

    async def bounded_scrape(name, cfg):
        async with sem:
            rows = await scrape_one(session, cfg)
            return name, rows

    tasks = [bounded_scrape(name, cfg) for name, cfg in SOURCES.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    log.info("scrape_all: gathered %d results", len(results))
    rows = {}
    now = int(time.time())

    for res in results:
        if isinstance(res, Exception):
            log.warning("scrape_all: exception %r", res)
            continue
        name, batch = res
        for addr, proto, src, country, anon in batch:
            if addr not in rows:
                rows[addr] = (
                    addr,
                    protocol_for_source(name, proto) if src is None else proto or "http",
                    name if src is None else src,
                    country,
                    anon,
                    now,
                )

    if not rows:
        log.info("scrape_all: no rows")
        return

    try:
        log.info("scrape_all: inserting %d", len(rows))
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
        log.info("scrape_all: inserted %d", len(rows))
    except Exception:
        log.exception("scrape persist failed")
        raise


def _extract_reported_ip(body):
    data = _parse_json_safe(body)
    if not isinstance(data, dict):
        return None
    reported = data.get("origin") or data.get("ip") or data.get("query")
    if not reported:
        return None
    return str(reported).split(",")[0].strip()


async def check_http(session, addr, timeout, url_index):
    if url_index >= len(TEST_URLS):
        return False, 0
    url = TEST_URLS[url_index]
    proxy_ip = addr.split(":")[0]
    try:
        t0 = time.perf_counter()
        async with session.get(
            url, proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout)),
        ) as r:
            if r.status != 200:
                return False, 0
            reported = _extract_reported_ip(await r.text())
            if not reported or reported == proxy_ip:
                return False, 0
            return True, int((time.perf_counter() - t0) * 1000)
    except Exception:
        return False, 0

async def check_socks(addr, proto, timeout, url_index):
    if url_index >= len(TEST_URLS):
        return False, 0
    url = TEST_URLS[url_index]
    proxy_ip = addr.split(":")[0]
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
                    return False, 0
                reported = _extract_reported_ip(await r.text())
                if not reported or reported == proxy_ip:
                    return False, 0
                return True, int((time.perf_counter() - t0) * 1000)
    except Exception:
        return False, 0
    finally:
        if connector is not None:
            try:
                await asyncio.wait_for(connector.close(), timeout=2)
            except Exception:
                log.warning("SOCKS connector close failed for %s", addr, exc_info=True)

async def verify_one(session, addr, protocol, timeout, url_index):
    try:
        if protocol in ("socks4", "socks5"):
            return addr, await check_socks(addr, protocol, timeout, url_index)
        return addr, await check_http(session, addr, timeout, url_index)
    except Exception:
        log.exception("verification failed unexpectedly for %s", addr)
        return addr, (False, 0)


async def notify_discord(session, addr, protocol, latency, instance_number):
    if not DISCORD_WEBHOOK:
        return

    global _last_webhook_sent
    async with _webhook_lock:
        now = time.monotonic()
        wait = DISCORD_RATE_LIMIT_DELAY - (now - _last_webhook_sent)
        if wait > 0:
            await asyncio.sleep(wait)
        payload = {
            "content": f"Proxy: `{addr}` | tipo: `{protocol}` | ping: `{latency}ms` | by: `{instance_number}`"
        }
        try:
            log.info("discord: sending %s", addr)
            async with session.post(
                DISCORD_WEBHOOK, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                log.info("discord: %s status %d", addr, r.status)
                if r.status == 429:
                    retry = (await r.json()).get("retry_after", 5)
                    await asyncio.sleep(float(retry) + 0.5)
        except Exception as e:
            log.warning("discord webhook failed: %r", e)
        finally:
            _last_webhook_sent = time.monotonic()


async def worker(session, queue, results, timeout, buf, db_conn, instance_number):
    while True:
        item = await queue.get()
        try:
            if item is None:
                return

            addr, protocol, url_index = item
            try:
                r = await asyncio.wait_for(
                    verify_one(session, addr, protocol, timeout, url_index),
                    timeout=WORKER_HARD_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("worker: %s hard timeout", addr)
                r = (addr, (False, 0))
            except Exception as e:
                log.warning("worker: %s exception %r", addr, e)
                r = (addr, (False, 0))
            ok, latency = r[1]
            log.info(
                "worker: %s url_index=%d ok=%s lat=%d",
                addr, url_index, ok, latency)
            if ok:
                await buf.add_ok(addr, latency)
                try:
                    row = await db_conn.fetchrow(
                        "SELECT notified FROM proxies WHERE addr=$1", addr)
                    if row and row["notified"] == 0:
                        await notify_discord(
                            session, addr, protocol, latency, instance_number)
                        await mark_notified(db_conn, addr)
                except Exception as e:
                    log.warning("worker: db error for %s: %r", addr, e)
            else:
                await buf.add_fail(addr)
            results.append(r)
        finally:
            queue.task_done()


async def release_claims(db_conn, addrs):
    if not addrs:
        return
    await db_conn.execute(
        """UPDATE proxies
           SET claimed_until=NULL, claimed_by=NULL
           WHERE addr=ANY($1::text[]) AND claimed_by=$2""",
        addrs, INSTANCE_ID)


async def run_verification(session, addrs, timeout, db_conn, instance_number):
    log.info("run_verification: start with %d", len(addrs))
    queue = asyncio.Queue(maxsize=QUEUE_MAX)
    results = []
    buf = PersistBuffer(db_conn)

    workers = [asyncio.create_task(
        worker(session, queue, results, timeout, buf, db_conn, instance_number))
               for _ in range(WORKERS)]
    log.info("run_verification: %d workers started", len(workers))

    try:
        for item in addrs:
            await queue.put(item)
        for _ in range(WORKERS):
            await queue.put(None)
        log.info("run_verification: queue filled, joining")
        await asyncio.wait_for(queue.join(), timeout=BATCH_HARD_TIMEOUT)
    except asyncio.TimeoutError as exc:
        log.error("run_verification: batch hard timeout; cancelling workers")
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        try:
            await buf.flush_all()
        finally:
            await release_claims(db_conn, [item[0] for item in addrs])
        raise RuntimeError("verification batch exceeded hard timeout") from exc
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        try:
            await buf.flush_all()
        finally:
            await release_claims(db_conn, [item[0] for item in addrs])
        raise

    log.info("run_verification: queue joined, gathering workers")
    outcomes = await asyncio.gather(*workers, return_exceptions=True)
    try:
        await buf.flush_all()
    except BaseException:
        await release_claims(db_conn, [item[0] for item in addrs])
        raise
    worker_errors = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    if worker_errors:
        await release_claims(db_conn, [item[0] for item in addrs])
        raise RuntimeError(
            f"{len(worker_errors)} verification worker(s) failed") from worker_errors[0]
    log.info("run_verification: done")
    return results


async def reverify(session, db_conn, instance_number, fast=False):
    log.info("reverify: start (fast=%s)", fast)
    is_helper, pending, helpers, _ = await get_zombie_helper_assignment(
        db_conn, INSTANCE_ID)
    if is_helper:
        log.info(
            "reverify: yielding to zombie helper (pending=%d helpers=%d)",
            pending, helpers)
        return

    total_claimed = 0
    total_dead = 0

    while True:
        is_helper, pending, helpers, _ = await get_zombie_helper_assignment(
            db_conn, INSTANCE_ID)
        if is_helper:
            log.info(
                "reverify: yielding to zombie helper (pending=%d helpers=%d)",
                pending, helpers)
            break

        log.info("reverify: claiming new")
        rows = await claim_new_batch(db_conn)
        if not rows:
            log.info("reverify: no more new proxies")
            break

        batch = [(r["addr"], r["protocol"], r["url_index"]) for r in rows]
        total_claimed += len(batch)
        log.info("reverify: got new batch of %d", len(batch))

        timeout = FAST_TIMEOUT if fast else FULL_TIMEOUT
        results = await run_verification(session, batch, timeout, db_conn, instance_number)
        dead = sum(1 for _, (ok, _) in results if not ok)
        total_dead += dead
        log.info("reverify: new batch done, dead=%d", dead)

    log.info("reverify: claimed total=%d dead total=%d", total_claimed, total_dead)

    if total_claimed >= WARMUP_CHECKS and total_dead / total_claimed > ABORT_THRESHOLD:
        log.error("abort reverify: %.0f%% dead - skipping cleanup", total_dead / total_claimed * 100)
        return

    log.info("reverify: cleaning dead")
    await cleanup_dead_proxies(db_conn)

    alive = await db_conn.fetchval("SELECT COUNT(*) FROM proxies WHERE ok=1")
    tot = await db_conn.fetchval("SELECT COUNT(*) FROM proxies")
    log.info("reverify done: %d claimed, %d/%d alive", total_claimed, alive, tot)


async def verify_zombie_batch(session, db_conn, instance_number, fast=False):
    rows = await claim_retry_batch(db_conn)
    if not rows:
        return 0

    batch = [(r["addr"], r["protocol"], r["url_index"]) for r in rows]
    log.info("zombie cycle: verifying batch of %d", len(batch))
    timeout = FAST_TIMEOUT if fast else FULL_TIMEOUT
    results = await run_verification(session, batch, timeout, db_conn, instance_number)
    dead = sum(1 for _, (ok, _) in results if not ok)
    log.info("zombie cycle: batch done, dead=%d", dead)
    return len(batch)


async def cleanup_dead_proxies(db_conn):
    now = int(time.time())
    await db_conn.execute(
        """DELETE FROM proxies
           WHERE (url_index >= $1 OR fail_streak >= 10)
           AND created < $2
           AND (last_ok IS NULL OR last_ok < $3)""",
        len(TEST_URLS), now - 3600, now - 86400)


async def zombie_reverify_loop(session, db_conn, instance_number):
    completed_batches = 0
    while True:
        try:
            is_worker, pending, helpers, active = (
                await get_zombie_helper_assignment(db_conn, INSTANCE_ID)
            )
            if is_worker:
                log.info(
                    "zombie helper: pending=%d helpers=%d active=%d",
                    pending, helpers, active)
                claimed = await verify_zombie_batch(
                    session, db_conn, instance_number, fast=True)
                if claimed:
                    completed_batches += 1
                    if completed_batches % 10 == 0:
                        await cleanup_dead_proxies(db_conn)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("zombie helper cycle failed")
        await asyncio.sleep(ZOMBIE_POLL_INTERVAL)


async def keep_alive_handler(_request):
    return web.json_response({
        "status": "ok",
        "instance": _request.app["instance_id"],
        "uptime": time.time(),
    })


async def start_http_server():
    log.info("http: starting on port %d", PORT)
    app = web.Application()
    app["instance_id"] = INSTANCE_ID
    app.router.add_get("/keep-alive", keep_alive_handler)
    app.router.add_get("/", keep_alive_handler)
    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
    except Exception:
        await runner.cleanup()
        raise
    log.info("http: server on port %d", PORT)
    return runner


async def main():
    log.info("main: start")
    db_conn = await db_open()
    zombie_task = None
    http_runner = None
    try:
        instance_number = await get_or_create_instance_number(db_conn, INSTANCE_ID)
        log.info("main: db open, instance number %d", instance_number)
        if not DISCORD_WEBHOOK:
            log.warning("DISCORD_WEBHOOK is unset; notifications are disabled")

        connector = aiohttp.TCPConnector(
            limit=WORKERS,
            limit_per_host=0,
            use_dns_cache=True,
            ttl_dns_cache=300,
        )
        http_runner = await start_http_server()
        log.info("main: http up")

        async with aiohttp.ClientSession(connector=connector) as session:
            zombie_task = asyncio.create_task(
                zombie_reverify_loop(session, db_conn, instance_number))
            if await try_scrape_lock(db_conn):
                log.info("main: scrape lock acquired, scraping")
                await scrape_all(session, db_conn)
            else:
                log.info("main: scrape skipped (lock held)")
            log.info("main: scrape done")

            next_scrape = time.monotonic() + SCRAPE_INTERVAL
            next_fast_reverify = time.monotonic()
            next_full_reverify = time.monotonic()

            log.info("main: entering loop")
            while True:
                try:
                    now = time.monotonic()
                    if now >= next_scrape:
                        next_scrape = now + SCRAPE_INTERVAL
                        if await try_scrape_lock(db_conn):
                            log.info("main: interval scrape")
                            await scrape_all(session, db_conn)
                        else:
                            log.info("main: interval scrape skipped (lock held)")

                    if now >= next_fast_reverify:
                        next_fast_reverify = now + FAST_REVERIFY
                        is_helper, _, _, _ = await get_zombie_helper_assignment(
                            db_conn, INSTANCE_ID)
                        if is_helper:
                            log.info("main: fast reverify skipped (zombie helper)")
                        else:
                            log.info("main: fast reverify")
                            await reverify(
                                session, db_conn, instance_number, fast=True)

                    if now >= next_full_reverify:
                        next_full_reverify = now + REVERIFY_INTERVAL
                        is_helper, _, _, _ = await get_zombie_helper_assignment(
                            db_conn, INSTANCE_ID)
                        if is_helper:
                            log.info("main: full reverify skipped (zombie helper)")
                        else:
                            log.info("main: full reverify")
                            await reverify(
                                session, db_conn, instance_number, fast=False)
                except Exception as e:
                    log.exception("main loop error: %r", e)

                sleep_until = min(next_scrape, next_fast_reverify, next_full_reverify)
                await asyncio.sleep(max(0.05, sleep_until - time.monotonic()))
    finally:
        if zombie_task is not None:
            zombie_task.cancel()
            await asyncio.gather(zombie_task, return_exceptions=True)
        if http_runner is not None:
            await http_runner.cleanup()
        log.info("main: closing db")
        await db_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
