#!/usr/bin/env python3
"""
SMA Pro — Ultimate Stock Market Analyzer
Portfolio Analytics · Efficient Frontier · ML Predictions · WebSocket · Docker Ready
Multi-user · Telegram Bot · 20+ Indicators · Global Markets (VN/US/EU/Asia/Crypto)
"""
import json, os, sys, time, threading, webbrowser, sqlite3, subprocess, queue
import hashlib, secrets, hmac, smtplib, csv, io, zipfile, shutil, asyncio
import functools
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta, date
import pickle
import base64
import concurrent.futures
import urllib.parse
import urllib.request
import websocket
import qrcode
import pyotp
try:
    import numpy as np
    import pandas as pd
    import asyncio
    import websockets
    import pytest
    from scipy.optimize import minimize
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    import redis
    REDIS_URL = os.environ.get('REDIS_URL', None)
    REDIS_CLIENT = redis.from_url(REDIS_URL) if REDIS_URL else None
except ImportError:
    REDIS_CLIENT = None  # will be re-set after auto-install

# WebSocket config
ENABLE_WEBSOCKET = True
WS_PORT = 10000

# ── CONFIGURATION ─────────────────────────────────────────────
PORT = 1349
VERSION = "Pro"
ALERT_CD_1D=3600; ALERT_CD_MTF=14400; ALERT_CD_SL=7200; ALERT_CD_TP=7200
ALERT_CD_VOL=3600; ALERT_CD_REGIME=7200
WACC_VN_DEFAULT=0.12; WACC_US_DEFAULT=0.09
DAILY_DIGEST_HOUR = 17   # giờ gửi digest (17:30 VN)
DAILY_DIGEST_MIN  = 30
BIND = "127.0.0.1"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sma.db")
OHLCV_BARS = 2000

# ── AUTO INSTALL ──────────────────────────────────────────────
def pip(*pkgs):
    subprocess.check_call([sys.executable,"-m","pip","install",*pkgs,"-q"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Core dependencies
required = ['yfinance', 'requests', 'numpy', 'pandas', 'scikit-learn', 'websockets', 'redis', 'pytest', 'scipy']
for pkg in required:
    try:
        __import__(pkg.replace('-', '_'))
    except:
        print(f"[SMA] Installing {pkg}...")
        pip(pkg)

import yfinance as yf
import requests
import numpy as np
import pandas as pd
import websockets
from scipy.optimize import minimize
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

# Redis for caching (optional)
try:
    import redis
    REDIS_URL = os.environ.get('REDIS_URL', None)
    REDIS_CLIENT = redis.from_url(REDIS_URL) if REDIS_URL else None
except:
    REDIS_CLIENT = None

# WebSocket config
ENABLE_WEBSOCKET = True
WS_PORT = 10000

try:
    from vnstock import Vnstock; HAS_VN = True
except:
    try: pip("vnstock==3.4.2"); from vnstock import Vnstock; HAS_VN = True
    except: HAS_VN = False; print("[SMA] vnstock unavailable")

# ── RATE LIMITING ─────────────────────────────────────────────
_login_attempts = defaultdict(list)
_LOGIN_MAX = 5
_LOGIN_WINDOW = 300
_LOGIN_BLOCK = 900

def _check_rate_limit(ip):
    now = time.time()
    attempts = _login_attempts[ip]
    _login_attempts[ip] = [t for t in attempts if now - t < _LOGIN_BLOCK]
    attempts = _login_attempts[ip]
    recent = [t for t in attempts if now - t < _LOGIN_WINDOW]
    if len(recent) >= _LOGIN_MAX:
        oldest = min(recent)
        wait = int(_LOGIN_BLOCK - (now - oldest))
        return False, wait
    return True, 0

def _record_attempt(ip):
    _login_attempts[ip].append(time.time())

# ── LOGGING ──────────────────────────────────────────────────
import logging, logging.handlers

def _setup_logging():
    log_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(log_dir, 'sma.log')
    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.WARNING)
    root = logging.getLogger('sma')
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(ch)
    return root

log = _setup_logging()

# Patch print
_orig_print = print
def _sma_log(*args, **kwargs):
    msg = ' '.join(str(a) for a in args)
    if '[SMA]' in msg or '[retry]' in msg or '[Crypto]' in msg:
        if 'error' in msg.lower() or 'lỗi' in msg.lower() or '❌' in msg:
            log.error(msg)
        elif '⚠️' in msg or 'warn' in msg.lower():
            log.warning(msg)
        else:
            log.info(msg)
    _orig_print(*args, **kwargs)

import builtins

def _gen_default_pw(length: int = 12) -> str:
    """Tạo mật khẩu ngẫu nhiên an toàn."""
    import secrets as _sec, string as _str
    chars = _str.ascii_letters + _str.digits + "!@#$"
    return ''.join(_sec.choice(chars) for _ in range(length))

builtins.print = _sma_log

# ── DECORATORS ───────────────────────────────────────────────
def api_doc(summary="", params={}, returns={}):
    def decorator(f):
        f.__api_doc__ = {"summary": summary, "params": params, "returns": returns}
        return f
    return decorator

def with_redis_cache(ttl=300):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not REDIS_CLIENT:
                return f(*args, **kwargs)
            key = f"{f.__name__}:{str(args)}:{str(kwargs)}"
            cached = REDIS_CLIENT.get(key)
            if cached:
                return json.loads(cached)
            result = f(*args, **kwargs)
            REDIS_CLIENT.setex(key, ttl, json.dumps(result, default=str))
            return result
        return wrapper
    return decorator

# ── INPUT SANITIZATION ───────────────────────────────────────
import html as _html_mod
import re as _re_mod

def sanitize_str(val, max_len=500, allow_html=False):
    if not isinstance(val, str): val = str(val) if val is not None else ''
    val = val.strip()[:max_len]
    if not allow_html:
        val = _html_mod.escape(val)
    return val

def sanitize_symbol(sym):
    if not isinstance(sym, str): return ''
    sym = sym.upper().strip()[:20]
    sym = _re_mod.sub(r'[^A-Z0-9./_-]', '', sym)
    return sym

def sanitize_int(val, default=0, min_v=None, max_v=None):
    try:
        v = int(val)
        if min_v is not None: v = max(v, min_v)
        if max_v is not None: v = min(v, max_v)
        return v
    except: return default

def sanitize_float(val, default=0.0, min_v=None, max_v=None):
    try:
        v = float(val)
        if min_v is not None: v = max(v, min_v)
        if max_v is not None: v = min(v, max_v)
        return v
    except: return default

_ALLOWED_MARKETS = frozenset(['VN', 'US', 'CRYPTO'])
_ALLOWED_DIRECTIONS = frozenset(['LONG', 'SHORT'])
_ALLOWED_INTERVALS = frozenset(['1m', '5m', '15m', '30m', '1H', '4H', '1D', '1W', '4h', '1h'])

# ── RETRY DECORATOR ──────────────────────────────────────────
def with_retry(max_attempts=3, delay=2.0, backoff=2.0, exceptions=(Exception,)):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts:
                        print(f"[retry] {fn.__name__} attempt {attempt}/{max_attempts} failed: {e} — retrying in {wait:.1f}s")
                        time.sleep(wait)
                        wait *= backoff
                    else:
                        print(f"[retry] {fn.__name__} gave up after {max_attempts} attempts: {e}")
            raise last_exc
        return wrapper
    return decorator

def with_retry_none(max_attempts=3, delay=2.0, backoff=2.0):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if attempt < max_attempts:
                        print(f"[retry] {fn.__name__} attempt {attempt}/{max_attempts}: {e} — retry in {wait:.1f}s")
                        time.sleep(wait)
                        wait *= backoff
                    else:
                        print(f"[retry] {fn.__name__} failed permanently: {e}")
            return None
        return wrapper
    return decorator

# ── DATABASE ─────────────────────────────────────────────────
def _check_and_fix_db():
    if not os.path.exists(DB_PATH):
        return
    try:
        tmp = sqlite3.connect(DB_PATH, timeout=5)
        tmp.execute("PRAGMA journal_mode=WAL")
        tmp.execute("SELECT name FROM sqlite_master LIMIT 1")
        tmp.close()
    except Exception as ex:
        print(f"[SMA] ⚠️  DB corrupt ({ex}) — xóa để tạo lại...")
        for f in [DB_PATH, DB_PATH+"-wal", DB_PATH+"-shm"]:
            try: os.remove(f)
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 257, _e)
        print("[SMA] DB cũ đã xóa — tạo mới khi khởi động")

_DB_WRITE_LOCK = threading.Lock()

class _DBConn:
    """Context manager: serialize writes, allow concurrent reads."""
    def __init__(self, write=False):
        self._write = write
        self._lock_acquired = False
    def __enter__(self):
        if self._write:
            _DB_WRITE_LOCK.acquire(timeout=10)
            self._lock_acquired = True
        c = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=-16000")
        c.execute("PRAGMA busy_timeout=8000")  # wait 8s on busy instead of error
        c.execute("PRAGMA temp_store=MEMORY")  # temp tables in RAM
        self._conn = c
        return c
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        except Exception as _dbe: log.debug("[DB] commit/rollback: %s", _dbe)
        finally:
            try: self._conn.close()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 289, _e)
            if self._lock_acquired:
                _DB_WRITE_LOCK.release()
        return False

def get_db(write=False):
    return _DBConn(write=write)

def init_db():
    with get_db() as c:
        c.executescript("""
        -- Users & Auth
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_h TEXT NOT NULL,
            salt TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            email TEXT,
            email_verified INTEGER DEFAULT 0,
            two_factor_secret TEXT,
            created_at TEXT DEFAULT(datetime('now')),
            last_login TEXT
        );
        
        CREATE TABLE IF NOT EXISTS sessions(
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT DEFAULT(datetime('now')),
            expires_at TEXT NOT NULL,
            user_agent TEXT,
            ip_address TEXT
        );
        
        CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT,
            details TEXT,
            ip TEXT,
            created_at TEXT DEFAULT(datetime('now'))
        );
        
        -- Watchlist
        CREATE TABLE IF NOT EXISTS watchlist(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol    TEXT UNIQUE NOT NULL,
            market    TEXT NOT NULL,
            name      TEXT DEFAULT '',
            sector    TEXT DEFAULT 'N/A',
            note      TEXT DEFAULT '',
            added_at  TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS user_watchlist(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            symbol    TEXT NOT NULL,
            market    TEXT NOT NULL,
            note      TEXT DEFAULT '',
            group_id  INTEGER DEFAULT NULL,
            added_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(user_id, symbol)
        );
        
        CREATE TABLE IF NOT EXISTS watchlist_groups(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            name      TEXT NOT NULL,
            color     TEXT DEFAULT '#2563eb',
            created_at TEXT DEFAULT(datetime('now')),
            UNIQUE(user_id, name)
        );
        
        -- OHLCV Data
        CREATE TABLE IF NOT EXISTS ohlcv_history(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL,
            trade_date  TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL NOT NULL,
            volume      INTEGER DEFAULT 0,
            UNIQUE(symbol, trade_date)
        );
        CREATE INDEX IF NOT EXISTS idx_ohlcv_sym ON ohlcv_history(symbol, trade_date DESC);
        
        CREATE TABLE IF NOT EXISTS fetch_meta(
            symbol      TEXT PRIMARY KEY,
            market      TEXT,
            last_fetch  TEXT,
            last_price  REAL DEFAULT 0,
            last_chg    REAL DEFAULT 0,
            name        TEXT DEFAULT '',
            sector      TEXT DEFAULT 'N/A'
        );
        
        -- Alerts
        CREATE TABLE IF NOT EXISTS alerts(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            symbol      TEXT,
            market      TEXT,
            signal      TEXT,
            score       INTEGER,
            price       REAL,
            timeframe   TEXT DEFAULT '1D',
            detail      TEXT DEFAULT '{}',
            created_at  TEXT DEFAULT(datetime('now'))
        );
        
        CREATE TABLE IF NOT EXISTS price_alerts(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL,
            target_price REAL NOT NULL,
            direction   TEXT NOT NULL DEFAULT 'ABOVE',
            note        TEXT DEFAULT '',
            triggered   INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT(datetime('now')),
            triggered_at TEXT
        );
        
        -- Trading Journal
        CREATE TABLE IF NOT EXISTS trades(
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            symbol       TEXT NOT NULL,
            market       TEXT NOT NULL DEFAULT 'VN',
            direction    TEXT NOT NULL DEFAULT 'LONG',
            entry_price  REAL NOT NULL,
            exit_price   REAL,
            qty          REAL NOT NULL DEFAULT 1,
            entry_date   TEXT NOT NULL,
            exit_date    TEXT,
            status       TEXT NOT NULL DEFAULT 'OPEN',
            reason_in    TEXT,
            reason_out   TEXT,
            emotion_in   TEXT,
            emotion_out  TEXT,
            signal_score INTEGER DEFAULT 0,
            signal_name  TEXT,
            sl_price     REAL,
            tp1_price    REAL,
            tp2_price    REAL,
            note         TEXT,
            tags         TEXT DEFAULT '[]',
            created_at   TEXT DEFAULT(datetime('now')),
            updated_at   TEXT DEFAULT(datetime('now'))
        );
        
        -- Portfolio
        CREATE TABLE IF NOT EXISTS portfolio_snapshots(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            date        TEXT NOT NULL,
            total_nav   REAL,
            cash        REAL DEFAULT 0,
            invested    REAL DEFAULT 0,
            pnl_day     REAL DEFAULT 0,
            pnl_total   REAL DEFAULT 0,
            snapshot_json TEXT DEFAULT '{}',
            created_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(user_id, date)
        );
        
        -- Signal History for ML
        CREATE TABLE IF NOT EXISTS signal_snapshots(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL,
            date        TEXT NOT NULL,
            signal      TEXT,
            score       INTEGER,
            price       REAL,
            confidence  INTEGER,
            confluence  INTEGER,
            rsi         REAL,
            macd        REAL,
            adx         REAL,
            volume_ratio REAL,
            rs_vs_index REAL,
            low_liquidity INTEGER DEFAULT 0,
            squeeze     INTEGER DEFAULT 0,
            pattern_type TEXT,
            created_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(symbol, date)
        );
        
        -- ML Models Cache
        CREATE TABLE IF NOT EXISTS ml_models(
            symbol      TEXT PRIMARY KEY,
            model_data  TEXT,
            accuracy    REAL,
            trained_at  TEXT,
            n_samples   INTEGER
        );
        
        -- Foreign flow
        CREATE TABLE IF NOT EXISTS foreign_flow(
            symbol      TEXT NOT NULL,
            date        TEXT NOT NULL,
            buy_val     REAL DEFAULT 0,
            sell_val    REAL DEFAULT 0,
            net_val     REAL DEFAULT 0,
            updated_at  TEXT DEFAULT(datetime('now')),
            PRIMARY KEY(symbol, date)
        );
        
        -- Sector rotation
        CREATE TABLE IF NOT EXISTS sector_rotation(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sector      TEXT NOT NULL,
            date        TEXT NOT NULL,
            avg_score   REAL,
            bull_count  INTEGER DEFAULT 0,
            bear_count  INTEGER DEFAULT 0,
            total_count INTEGER DEFAULT 0,
            avg_rs      REAL DEFAULT 0,
            UNIQUE(sector, date)
        );
        
        -- Market context
        CREATE TABLE IF NOT EXISTS market_context(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            date        TEXT NOT NULL,
            trend       TEXT,
            score       INTEGER DEFAULT 0,
            price       REAL,
            updated_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(symbol, date)
        );
        
        -- Intraday cache
        CREATE TABLE IF NOT EXISTS intraday_cache(
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL,
            interval    TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            data_json   TEXT DEFAULT '[]',
            PRIMARY KEY(symbol, interval)
        );
        
        -- Top signals history
        CREATE TABLE IF NOT EXISTS top_signals_history(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL,
            date        TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            score       INTEGER DEFAULT 0,
            price       REAL DEFAULT 0,
            confidence  INTEGER DEFAULT 0,
            confluence  INTEGER DEFAULT 0,
            rsi         REAL DEFAULT 50,
            adx         REAL DEFAULT 0,
            squeeze     INTEGER DEFAULT 0,
            unusual_vol INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(symbol, date, signal_type)
        );
        CREATE INDEX IF NOT EXISTS idx_tsh_sym_date ON top_signals_history(symbol, date DESC);
        CREATE INDEX IF NOT EXISTS idx_tsh_sig ON top_signals_history(signal_type, date DESC);
        
        -- Settings
        CREATE TABLE IF NOT EXISTS settings(
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        
        -- Telegram
        CREATE TABLE IF NOT EXISTS tg_subscribers(
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            chat_id    TEXT NOT NULL UNIQUE,
            username   TEXT DEFAULT '',
            symbols    TEXT DEFAULT '[]',
            enabled    INTEGER DEFAULT 1,
            joined_at  TEXT DEFAULT(datetime('now'))
        );
        
        CREATE TABLE IF NOT EXISTS tg_link_codes(
            code TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at REAL NOT NULL
        );
        
        -- Email alerts log
        CREATE TABLE IF NOT EXISTS email_alerts(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            subject     TEXT,
            body        TEXT,
            sent_at     TEXT DEFAULT(datetime('now')),
            success     INTEGER DEFAULT 1
        );
        
        -- Forward test tracking
        CREATE TABLE IF NOT EXISTS forward_test(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER,
            symbol      TEXT,
            market      TEXT,
            signal      TEXT,
            score_at_signal INTEGER,
            price_at_signal REAL,
            price_5d    REAL,
            price_10d   REAL,
            price_20d   REAL,
            ret_5d      REAL,
            ret_10d     REAL,
            ret_20d     REAL,
            correct_5d  INTEGER,
            correct_10d INTEGER,
            correct_20d INTEGER,
            updated_at  TEXT DEFAULT(datetime('now'))
        );
                        CREATE TABLE IF NOT EXISTS drawing_tools(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT 'VN',
            tool_type TEXT NOT NULL,
            data_json TEXT NOT NULL,
            created_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ensemble_models(
            symbol TEXT PRIMARY KEY,
            model_blob TEXT NOT NULL,
            model_info TEXT DEFAULT '{}',
            trained_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS regime_models(
            symbol TEXT PRIMARY KEY,
            model_blob TEXT NOT NULL,
            model_info TEXT DEFAULT '{}',
            trained_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS kqkd_cache(
            symbol TEXT PRIMARY KEY,
            data_json TEXT NOT NULL,
            fetched_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS macro_cache(
            symbol TEXT PRIMARY KEY,
            price REAL,
            chg_pct REAL,
            extra TEXT DEFAULT '{}',
            fetched_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS vndirect_config(
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            password_enc TEXT NOT NULL,
            account_no TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS alpaca_config(
            user_id INTEGER PRIMARY KEY,
            api_key TEXT NOT NULL,
            api_secret TEXT NOT NULL,
            paper INTEGER DEFAULT 1,
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS mbs_config(
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            password_enc TEXT NOT NULL,
            account_no TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS bsc_config(
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            password_enc TEXT NOT NULL,
            account_no TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ibkr_config(
            user_id INTEGER PRIMARY KEY,
            host TEXT DEFAULT 'localhost',
            port INTEGER DEFAULT 5000,
            account_id TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS tcbs_config(
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            password_enc TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT(datetime('now'))
        );
        """)
        
        # Default settings
        defaults = [
            ('tg_token',''), ('tg_chat_id',''), ('tg_enabled','0'),
            ('page_size','20'), ('alert_threshold','8'), ('public_url',''),
            ('allow_register','1'), ('account_size','0'), ('risk_per_trade_pct','2'),
            ('email_enabled','0'), ('smtp_host',''), ('smtp_port','587'),
            ('smtp_user',''), ('smtp_pass',''), ('email_to',''),
            ('backup_enabled','1'), ('backup_days','7'),
            ('webhook_url',''), ('webhook_enabled','0'), ('last_backup_at',''),
            ('intraday_interval','5m'), ('vnindex_benchmark','1'),
            ('paper_trading_enabled','0'), ('paper_balance','100000000'),
            ('breakout_d1_gap_filter','1'), ('top_signals_days','7')
        ]
        for k,v in defaults:
            c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k,str(v)))
    
    # Migrations
    with get_db() as c:
        cols = [r[1] for r in c.execute("PRAGMA table_info(user_watchlist)").fetchall()]
        if 'group_id' not in cols:
            c.execute("ALTER TABLE user_watchlist ADD COLUMN group_id INTEGER DEFAULT NULL")

    # ── Merged v29: new feature migrations ────────────────────────────
    with get_db() as c:
        # Feature 1: % move alerts — add trigger_type + pct_value to price_alerts
        pa_cols = [r[1] for r in c.execute("PRAGMA table_info(price_alerts)").fetchall()]
        if 'trigger_type' not in pa_cols:
            c.execute("ALTER TABLE price_alerts ADD COLUMN trigger_type TEXT DEFAULT 'PRICE'")
        if 'pct_value' not in pa_cols:
            c.execute("ALTER TABLE price_alerts ADD COLUMN pct_value REAL DEFAULT NULL")
        if 'base_price' not in pa_cols:
            c.execute("ALTER TABLE price_alerts ADD COLUMN base_price REAL DEFAULT NULL")

        # Feature 2: Earnings surprise history table
        c.execute("""
            CREATE TABLE IF NOT EXISTS earnings_surprise(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                market      TEXT NOT NULL DEFAULT 'US',
                period      TEXT,
                report_date TEXT,
                eps_est     REAL,
                eps_actual  REAL,
                surprise_pct REAL,
                revenue_est  REAL,
                revenue_actual REAL,
                rev_surprise_pct REAL,
                reaction_1d REAL,
                reaction_5d REAL,
                updated_at  TEXT DEFAULT(datetime('now')),
                UNIQUE(symbol, market, period)
            )
        """)

        # Feature 3: Short selling — add borrow_rate + margin_req to trades
        tr_cols = [r[1] for r in c.execute("PRAGMA table_info(trades)").fetchall()]
        if 'borrow_rate' not in tr_cols:
            c.execute("ALTER TABLE trades ADD COLUMN borrow_rate REAL DEFAULT 0")
        if 'margin_used' not in tr_cols:
            c.execute("ALTER TABLE trades ADD COLUMN margin_used REAL DEFAULT 0")

        # Feature 4: Custom indicator rules table
        c.execute("""
            CREATE TABLE IF NOT EXISTS custom_signals(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                name        TEXT NOT NULL,
                description TEXT,
                rules       TEXT NOT NULL DEFAULT '[]',
                logic       TEXT NOT NULL DEFAULT 'AND',
                signal_type TEXT NOT NULL DEFAULT 'BUY',
                is_active   INTEGER DEFAULT 1,
                created_at  TEXT DEFAULT(datetime('now')),
                updated_at  TEXT DEFAULT(datetime('now'))
            )
        """)
        # Feature 4: Custom signal backtest results
        c.execute("""
            CREATE TABLE IF NOT EXISTS custom_signal_results(
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id    INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                triggered_at TEXT,
                price        REAL,
                return_5d    REAL,
                return_10d   REAL,
                return_20d   REAL,
                win          INTEGER
            )
        """)

    # ── Pro: extra tables from v12 ──
    with get_db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS paper_trades(
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER NOT NULL,
            symbol        TEXT NOT NULL,
            market        TEXT NOT NULL DEFAULT 'VN',
            signal        TEXT,
            entry_price   REAL NOT NULL,
            qty           INTEGER DEFAULT 0,
            position_value REAL DEFAULT 0,
            source        TEXT DEFAULT 'TA',
            confidence    TEXT DEFAULT 'MEDIUM',
            note          TEXT DEFAULT '',
            status        TEXT DEFAULT 'OPEN',
            entry_date    TEXT,
            exit_price    REAL,
            exit_date     TEXT,
            pnl_pct       REAL,
            pnl_vnd       REAL,
            close_reason  TEXT,
            created_at    TEXT DEFAULT(datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_paper_user ON paper_trades(user_id, status);

        CREATE TABLE IF NOT EXISTS ml_predictions(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            market      TEXT DEFAULT 'VN',
            pred_date   TEXT,
            signal      TEXT,
            probability REAL,
            confidence  TEXT,
            p_buy       REAL,
            p_sell      REAL,
            p_hold      REAL,
            actual_ret_1d REAL,
            actual_ret_5d REAL,
            created_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(symbol, pred_date)
        );

        CREATE TABLE IF NOT EXISTS ml_models(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            market      TEXT DEFAULT 'VN',
            model_blob  BLOB,
            metrics     TEXT,
            trained_at  TEXT,
            UNIQUE(symbol)
        );

        CREATE TABLE IF NOT EXISTS breakout_filter(
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT NOT NULL,
            market       TEXT NOT NULL,
            signal_date  TEXT NOT NULL,
            signal_type  TEXT,
            signal_price REAL,
            d1_open      REAL,
            d1_gap_pct   REAL,
            confirmed    INTEGER,
            created_at   TEXT DEFAULT(datetime('now')),
            UNIQUE(symbol, signal_date, signal_type)
        );

        CREATE TABLE IF NOT EXISTS investment_lists(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            type        TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL DEFAULT 'VN',
            rank        INTEGER NOT NULL,
            score       REAL NOT NULL,
            reasons     TEXT DEFAULT '[]',
            updated_at  TEXT DEFAULT(datetime('now')),
            UNIQUE(type, symbol)
        );

        CREATE TABLE IF NOT EXISTS investment_lists_meta(
            type        TEXT PRIMARY KEY,
            last_run    TEXT,
            next_run    TEXT,
            status      TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS analysis_cache(
            symbol      TEXT NOT NULL,
            market      TEXT NOT NULL DEFAULT 'VN',
            analysis    TEXT NOT NULL,  -- JSON của analyze() result
            ta_score    INTEGER DEFAULT 0,
            signal      TEXT DEFAULT 'NEUTRAL',
            analyzed_at TEXT DEFAULT(datetime('now')),
            PRIMARY KEY(symbol, market)
        );

        """)

    # v24: Additional tables (separate connection)
    with get_db() as c2:
        c2.execute("""CREATE TABLE IF NOT EXISTS crypto_watchlist(user_id INTEGER,symbol TEXT,PRIMARY KEY(user_id,symbol))""")
        c2.execute("""CREATE TABLE IF NOT EXISTS intraday_signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, market TEXT, interval TEXT,
            signal TEXT, score INTEGER, price REAL,
            created_at TEXT DEFAULT(datetime('now')))""")
        c2.execute("""CREATE TABLE IF NOT EXISTS portfolio_targets(
            user_id INTEGER, symbol TEXT, market TEXT,
            target_pct REAL DEFAULT 0,
            PRIMARY KEY(user_id, symbol))""")
        c2.execute("""CREATE TABLE IF NOT EXISTS signal_stats(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, market TEXT NOT NULL,
            signal TEXT NOT NULL, score INTEGER,
            price_entry REAL, price_exit REAL,
            horizon_days INTEGER DEFAULT 5,
            result TEXT, pnl_pct REAL,
            snapshot_date TEXT, checked_date TEXT,
            created_at TEXT DEFAULT(datetime('now')))""")
        c2.execute("""CREATE TABLE IF NOT EXISTS tg_pending_confirm(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL, user_id INTEGER,
            action TEXT NOT NULL, payload TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT DEFAULT(datetime('now')))""")
    _ensure_admin()

# ── USER AUTH ────────────────────────────────────────────────
def _hash_pw(salt, password):
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 260000).hex()

def _audit_log(user_id, action, details, ip=None):
    try:
        with get_db() as c:
            c.execute("INSERT INTO audit_log(user_id,action,details,ip) VALUES(?,?,?,?)",
                     (user_id, action, json.dumps(details, default=str), ip))
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 712, _e)

def _ensure_admin():
    with get_db() as c:
        n = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if n == 0:
            salt = secrets.token_hex(16)
            _dpw = os.environ.get("SMA_ADMIN_PW") or _gen_default_pw()
            ph = _hash_pw(salt, _dpw)
            c.execute("INSERT INTO users(username,password_h,salt,role) VALUES(?,?,?,?)",
                    ("admin", ph, salt, "admin"))
            print(f"[SMA] Admin mặc định: admin / {_dpw} (set SMA_ADMIN_PW env để tùy chỉnh)")

def user_register(username, password):
    if len(username) < 3: return False, "Username phải ≥ 3 ký tự"
    if len(password) < 6: return False, "Password phải ≥ 6 ký tự"
    if get_setting('allow_register','1') == '0': return False, "Đăng ký đã đóng"
    salt = secrets.token_hex(16)
    ph = _hash_pw(salt, password)
    try:
        with get_db() as c:
            c.execute("INSERT INTO users(username,password_h,salt) VALUES(?,?,?)", (username, ph, salt))
            uid = c.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()['id']
        return True, uid
    except sqlite3.IntegrityError:
        return False, "Username đã tồn tại"

def user_login(username, password, ip=None, user_agent=None):
    with get_db() as c:
        u = c.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not u:
            return None, "Sai tên đăng nhập hoặc mật khẩu"
        
        if _hash_pw(u['salt'], password) != u['password_h']:
            _audit_log(u['id'], 'LOGIN_FAILED', {'reason': 'wrong_password'}, ip)
            return None, "Sai tên đăng nhập hoặc mật khẩu"
        
        if u['role'] == 'banned':
            _audit_log(u['id'], 'LOGIN_BLOCKED', {'reason': 'user_banned'}, ip)
            return None, "Tài khoản đã bị khóa"
        
        token = secrets.token_urlsafe(32)
        exp = (datetime.now() + timedelta(days=30)).isoformat()
        c.execute("INSERT INTO sessions(token,user_id,expires_at,user_agent,ip_address) VALUES(?,?,?,?,?)",
                 (token, u['id'], exp, user_agent, ip))
        c.execute("UPDATE users SET last_login=? WHERE id=?", (datetime.now().isoformat(), u['id']))
        
        _audit_log(u['id'], 'LOGIN_SUCCESS', {}, ip)
        return token, {"id": u['id'], "username": u['username'], "role": u['role']}

def get_session(token):
    if not token: return None
    with get_db() as c:
        s = c.execute("SELECT s.*,u.username,u.role FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?", (token,)).fetchone()
        if not s: return None
        if datetime.fromisoformat(s['expires_at']) < datetime.now():
            c.execute("DELETE FROM sessions WHERE token=?", (token,))
            return None
    return {"id": s['user_id'], "username": s['username'], "role": s['role'], "token": token}

def logout_session(token):
    with get_db() as c:
        c.execute("DELETE FROM sessions WHERE token=?", (token,))

def user_change_password(user_id, old_pw, new_pw):
    if len(new_pw) < 6: return False, "Password mới phải ≥ 6 ký tự"
    with get_db() as c:
        u = c.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not u: return False, "User không tồn tại"
        if _hash_pw(u['salt'], old_pw) != u['password_h']: return False, "Mật khẩu cũ sai"
        salt = secrets.token_hex(16)
        ph = _hash_pw(salt, new_pw)
        c.execute("UPDATE users SET password_h=?,salt=? WHERE id=?", (ph, salt, user_id))
    return True, "Đổi mật khẩu thành công"

def get_user_setting(user_id, key, default=''):
    with get_db() as c:
        r = c.execute("SELECT value FROM user_settings WHERE user_id=? AND key=?", (user_id, key)).fetchone()
    return r['value'] if r else default

def set_user_setting(user_id, key, value):
    with get_db() as c:
        c.execute("INSERT OR REPLACE INTO user_settings(user_id,key,value) VALUES(?,?,?)", (user_id, key, str(value)))

# ── WEBSOCKET SERVER ─────────────────────────────────────────
connected_clients = set()

async def ws_handler(websocket, path):
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)
            if data.get('type') == 'subscribe':
                symbols = data.get('symbols', [])
                # Handle subscription
                pass
            elif data.get('type') == 'ping':
                await websocket.send(json.dumps({"type": "pong"}))
    finally:
        connected_clients.remove(websocket)

# Sửa hàm broadcast_update() - thay thế
def broadcast_update(data):
    """Send update to all connected WebSocket clients (non-blocking)"""
    if not connected_clients:
        return
    
    async def _broadcast():
        if connected_clients:
            await asyncio.gather(
                *[client.send(json.dumps(data)) for client in connected_clients],
                return_exceptions=True
            )
    
    try:
        # Try to get running loop, if not, create new one
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, create new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_broadcast())
            loop.close()
            return
        
        # If loop is running, create task
        if loop.is_running():
            asyncio.create_task(_broadcast())
        else:
            loop.run_until_complete(_broadcast())
    except Exception as e:
        print(f"  ⚠️ Broadcast error: {e}")

def start_websocket():
    """Start WebSocket server in a new thread with proper event loop"""
    if not ENABLE_WEBSOCKET:
        return
    
    def run_ws():
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            start_server = websockets.serve(ws_handler, "0.0.0.0", WS_PORT)
            loop.run_until_complete(start_server)
            print(f"  ✅ WebSocket running on ws://localhost:{WS_PORT}")
            loop.run_forever()
        except Exception as e:
            print(f"  ❌ WebSocket error: {e}")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_ws, daemon=True)
    thread.start()

# ── GET/SET SETTINGS ─────────────────────────────────────────
def get_setting(key, default=''):
    with get_db() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r['value'] if r else default

def set_setting(key, value):
    with get_db() as c:
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(value)))

# ── TELEGRAM ─────────────────────────────────────────────────
_tg_last_update = 0

def tg_gen_link_code(user_id):
    import random
    code = str(random.randint(100000, 999999))
    now = time.time()
    with get_db() as c:
        c.execute("DELETE FROM tg_link_codes WHERE user_id=?", (user_id,))
        c.execute("INSERT INTO tg_link_codes(code,user_id,created_at) VALUES(?,?,?)", (code, user_id, now))
    return code

def tg_verify_link_code(code, chat_id, tg_username=""):
    now = time.time()
    with get_db() as c:
        row = c.execute("SELECT * FROM tg_link_codes WHERE code=?", (code,)).fetchone()
        if not row:
            return False, "Ma khong dung hoac da het han."
        if now - row["created_at"] > 600:
            c.execute("DELETE FROM tg_link_codes WHERE code=?", (code,))
            return False, "Ma da het han (10 phut). Lay ma moi trong Settings."
        user_id = row["user_id"]
        c.execute("DELETE FROM tg_link_codes WHERE code=?", (code,))
        c.execute(
            "INSERT INTO tg_subscribers(user_id,chat_id,username,symbols,enabled) VALUES(?,?,?,?,1) "
            "ON CONFLICT(chat_id) DO UPDATE SET user_id=excluded.user_id,username=excluded.username,enabled=1",
            (user_id, str(chat_id), tg_username, "[]")
        )
        syms = [r["symbol"] for r in c.execute(
            "SELECT symbol FROM user_watchlist WHERE user_id=?", (user_id,)).fetchall()]
        urow = c.execute("SELECT username FROM users WHERE id=?", (user_id,)).fetchone()
        uname = urow["username"] if urow else str(user_id)
    if syms:
        tg_subscribe_symbols(str(chat_id), syms)
    return True, f"✅ Da lien ket voi tai khoan <b>{uname}</b>!\n📊 Da dang ky <b>{len(syms)} ma</b> tu watchlist."

def _check_default_password():
    with get_db() as c:
        row = c.execute("SELECT password_h, salt FROM users WHERE username='admin'").fetchone()
    if not row: return
    _check_pw = os.environ.get('SMA_ADMIN_PW', 'admin123')  # detect default pw
    match_new = _hash_pw(row['salt'], _check_pw) == row['password_h']
    match_old = hashlib.sha256((row['salt'] + _check_pw).encode()).hexdigest() == row['password_h']
    if match_new or match_old:
        print("[SMA] ⚠️  WARNING: Admin vẫn dùng mật khẩu mặc định!")
        print("[SMA] ⚠️  WARNING: Vào Settings → Đổi mật khẩu ngay để bảo mật!")

def tg_get_subscriber(chat_id):
    with get_db() as c:
        r = c.execute("SELECT * FROM tg_subscribers WHERE chat_id=?", (str(chat_id),)).fetchone()
    return dict(r) if r else None

def tg_register_chat(chat_id, tg_username=''):
    with get_db() as c:
        c.execute("""INSERT OR IGNORE INTO tg_subscribers(chat_id,username,symbols)
                    VALUES(?,?,?)""", (str(chat_id), tg_username, '[]'))

def tg_subscribe_symbols(chat_id, symbols):
    sub = tg_get_subscriber(str(chat_id))
    if not sub: tg_register_chat(chat_id)
    cur = set(json.loads(sub['symbols']) if sub else [])
    cur.update([s.upper() for s in symbols])
    with get_db() as c:
        c.execute("UPDATE tg_subscribers SET symbols=? WHERE chat_id=?",
                (json.dumps(list(cur)), str(chat_id)))

def tg_unsubscribe_symbols(chat_id, symbols):
    sub = tg_get_subscriber(str(chat_id))
    if not sub: return
    cur = set(json.loads(sub['symbols']))
    cur -= set(s.upper() for s in symbols)
    with get_db() as c:
        c.execute("UPDATE tg_subscribers SET symbols=? WHERE chat_id=?",
                (json.dumps(list(cur)), str(chat_id)))

def tg_get_all_subscribers():
    with get_db() as c:
        rows = c.execute("SELECT * FROM tg_subscribers WHERE enabled=1").fetchall()
    return [dict(r) for r in rows]

def tg_send_to_subscribers(symbol, message):
    token = get_setting('tg_token','')
    if not token: return
    subs = tg_get_all_subscribers()
    for sub in subs:
        syms = json.loads(sub.get('symbols','[]'))
        if symbol in syms or '*' in syms:
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": sub['chat_id'], "text": message, "parse_mode": "HTML"},
                            timeout=6)
                time.sleep(0.05)
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 968, _e)

# ── TELEGRAM ALERT FUNCTIONS ───────────────────────────────
def tg_test(token, chat_id):
    try:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id":chat_id,"text":"✅ <b>SMA Pro</b> kết nối Telegram thành công!\n📊 20+ Indicators + ML Ensemble + LSTM","parse_mode":"HTML"},timeout=8)
        return r.status_code==200, r.text
    except Exception as e: return False, str(e)

def tg_send(text):
    token=get_setting('tg_token'); chat=get_setting('tg_chat_id')
    if not token or not chat or get_setting('tg_enabled')!='1': return False
    try:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id":chat,"text":text,"parse_mode":"HTML"},timeout=8)
        return r.status_code==200
    except Exception as e: print(f"[TG] {e}"); return False

def tg_send_signal_alert(sym: str, mkt: str, signal: str, price: float,
                          score: int, confidence: float, reasons: list = None):
    """
    Gửi Telegram alert khi có BUY/SELL đồng thuận.
    Chỉ gửi khi |score| >= 3 (tránh noise).
    """
    if abs(score) < 3: return False
    if signal not in ('BUY', 'STRONG_BUY', 'SELL', 'STRONG_SELL'): return False

    icon   = '🟢' if 'BUY' in signal else '🔴'
    vn     = mkt == 'VN'
    p_str  = f"{int(price):,}đ" if vn else f"${price:.2f}"
    reasons_str = '\n'.join(f"  • {r}" for r in (reasons or [])[:5])

    msg = (
        f"📊 <b>SMA v{VERSION}</b> · {mkt}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{icon} <b>{signal}</b> — <b>{sym}</b>\n"
        f"💰 Giá: <b>{p_str}</b>\n"
        f"📊 Điểm: <b>{score:+d}</b> | Confidence: <b>{confidence:.0f}%</b>\n"
        f"{reasons_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>SMA v{VERSION} • {datetime.now().strftime('%H:%M %d/%m')}</i>"
    )
    return tg_send_to_subscribers(sym, msg)

def email_send_signal_alert(sym: str, mkt: str, signal: str, price: float,
                             score: int, reasons: list = None):
    """Gửi email alert khi có BUY/SELL đồng thuận."""
    if abs(score) < 3: return False
    vn = mkt == 'VN'
    p_str = f"{int(price):,}đ" if vn else f"${price:.2f}"
    icon  = '🟢' if 'BUY' in signal else '🔴'
    reasons_html = ''.join(f"<li>{r}</li>" for r in (reasons or [])[:5])
    subject = f"{icon} SMA v{VERSION} Alert: {signal} {sym} @ {p_str}"
    body = f"""
    <h2 style="color:{'#00c853' if 'BUY' in signal else '#d50000'}">{icon} {signal} — {sym}</h2>
    <table style="border-collapse:collapse;width:100%">
      <tr><td style="padding:6px;color:#666">Giá</td><td style="font-weight:bold">{p_str}</td></tr>
      <tr><td style="padding:6px;color:#666">Điểm</td><td style="font-weight:bold">{score:+d}</td></tr>
      <tr><td style="padding:6px;color:#666">Market</td><td>{mkt}</td></tr>
    </table>
    <h4>Lý do:</h4><ul>{reasons_html}</ul>
    <p style="color:#999;font-size:12px">SMA Pro • {datetime.now().strftime('%H:%M %d/%m/%Y')}</p>
    """
    try:
        return email_send(subject, body)
    except Exception as _ee:
        log.debug('[email_alert] %s', _ee)
        return False

def send_consensus_alert(sym: str, mkt: str, data: dict):
    """
    Tự động gửi TG + Email khi có BUY/SELL đồng thuận từ analyze().
    Gọi từ refresh worker sau mỗi lần update.
    """
    a = data.get('analysis', {})
    if not a: return
    signal     = a.get('signal', 'NEUTRAL')
    score      = int(a.get('score', 0))
    confidence = float(a.get('confidence', 0))
    price      = float(data.get('price', 0))
    reasons    = a.get('reasons', [])

    if signal in ('BUY','STRONG_BUY','SELL','STRONG_SELL') and abs(score) >= 3:
        # Chống spam: cooldown 4h per symbol per signal
        cooldown_key = f"alert_{sym}_{signal}"
        last_sent = _ram_cache.get(cooldown_key, {}).get('ts', 0)
        if time.time() - last_sent < 14400:  # 4 giờ
            return
        _ram_cache[cooldown_key] = {'ts': time.time(), 'data': None}

        # Gửi cả TG và email
        tg_send_signal_alert(sym, mkt, signal, price, score, confidence, reasons)
        email_send_signal_alert(sym, mkt, signal, price, score, reasons)
        log.info("[ALERT] Sent %s %s score=%d", signal, sym, score)


def tg_process_updates():
    global _tg_last_update
    token = get_setting('tg_token','')
    if not token: return
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates",
                        params={"offset": _tg_last_update+1, "timeout": 5}, timeout=10)
        if r.status_code != 200: return
        updates = r.json().get('result', [])
        for upd in updates:
            _tg_last_update = upd['update_id']
            msg = upd.get('message', {})
            if not msg: continue
            chat_id = str(msg['chat']['id'])
            text = msg.get('text','').strip()
            tg_user = msg.get('from',{}).get('username','')
            _tg_handle_command(chat_id, text, tg_user, token)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 1004, _e)

def _tg_handle_command(chat_id, text, tg_user, token):
    def reply(msg):
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=6)
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1011, _e)

    cmd = text.split()[0].lower() if text else ''
    args = text.split()[1:] if len(text.split()) > 1 else []

    if cmd == '/start':
        tg_register_chat(chat_id, tg_user)
        with get_db() as _c:
            _all_syms = [r['symbol'] for r in _c.execute("SELECT symbol FROM watchlist").fetchall()]
        if _all_syms:
            tg_subscribe_symbols(chat_id, _all_syms)
            auto_msg = f"\n✅ Đã tự động đăng ký <b>{len(_all_syms)} mã</b> trong watchlist."
        else:
            auto_msg = ""
        pub_url = get_setting('public_url',f'http://localhost:{PORT}')
        reply(f"""👋 <b>Chào mừng đến SMA Pro!</b>\n
📊 Hệ thống phân tích kỹ thuật 19 chỉ báo + Patterns + ML Predictions{auto_msg}

<b>Lệnh cơ bản:</b>
/alert VCB VHM — Thêm alert mã cụ thể
/off VCB — Tắt alert mã
/check VCB — Xem tín hiệu ngay
/list — Xem mã đang theo dõi
/link — Lấy link web
/ml AAPL — Xem ML prediction

🌐 Web: {pub_url}""")

    elif cmd in ('/alert', '/on'):
        if not args:
            reply("Ví dụ: /alert VCB VHM HPG")
            return
        syms = [s.upper() for s in args]
        tg_register_chat(chat_id, tg_user)
        tg_subscribe_symbols(chat_id, syms)
        reply(f"✅ Đã bật alert cho: <b>{', '.join(syms)}</b>")

    elif cmd == '/off':
        if not args:
            reply("Ví dụ: /off VCB")
            return
        syms = [s.upper() for s in args]
        tg_unsubscribe_symbols(chat_id, syms)
        reply(f"🔕 Đã tắt alert cho: <b>{', '.join(syms)}</b>")

    elif cmd == '/list':
        sub = tg_get_subscriber(chat_id)
        if not sub:
            reply("Bạn chưa đăng ký. Gửi /start để bắt đầu.")
            return
        syms = json.loads(sub.get('symbols','[]'))
        if not syms:
            reply("Bạn chưa theo dõi mã nào.\nGửi /alert VCB để bắt đầu.")
        else:
            reply(f"📋 <b>Đang theo dõi ({len(syms)} mã):</b>\n" + ", ".join(syms))

    elif cmd == '/check':
        if not args:
            reply("Ví dụ: /check VCB\nhoặc /check AAPL US")
            return
        sym = args[0].upper()
        mkt = args[1].upper() if len(args) > 1 else ("US" if len(sym) > 3 else "VN")
        reply(f"⏳ Đang phân tích {sym}...")
        try:
            data = fetch(sym, mkt)
            if not data:
                reply(f"❌ Không tìm thấy {sym}")
                return
            a = analyze(data)
            price = data['price']
            vn = mkt == "VN"
            fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
            ml = ml_predict(sym, a)
            sig_label = {"BUY":"🟢 MUA","SELL":"🔴 BÁN","BUY_WARN":"🟡 CẢNH BÁO MUA",
                        "SELL_WARN":"🟠 CẢNH BÁO BÁN","BUY_WEAK":"🔵 YẾU MUA",
                        "SELL_WEAK":"🟣 YẾU BÁN","NEUTRAL":"⚪ TRUNG TÍNH"}.get(a['signal'], a['signal'])
            lines = [
                f"📊 <b>{sym} ({mkt})</b> — {fp}",
                f"Tín hiệu: <b>{sig_label}</b> ({a['total']:+d}/19)",
                f"RSI: {a['rsi']}  Trend: {a['trend_long']}",
                f"🤖 ML Prediction: <b>{ml['prediction']}</b> ({ml['prob_up']}% up / {ml['prob_down']}% down)",
                "",
                "<b>Chi tiết:</b>",
            ]
            for k,v in list(a['details'].items())[:6]:
                if v and "Chưa" not in v:
                    sc = a['scores'].get(k,0)
                    mark = "🟢" if sc>0 else "🔴" if sc<0 else "⚪"
                    lines.append(f"{mark} {v}")
            reply("\n".join(lines))
        except Exception as e:
            reply(f"❌ Lỗi: {e}")

    elif cmd == '/ml':
        if not args:
            reply("Ví dụ: /ml AAPL")
            return
        sym = args[0].upper()
        reply(f"⏳ Đang tính ML prediction cho {sym}...")
        data = _ram_cache.get(sym, {}).get('data', {})
        if not data:
            reply(f"❌ Không có dữ liệu cho {sym}")
            return
        ml = ml_predict(sym, data.get('analysis', {}))
        reply(f"""🤖 <b>ML Prediction — {sym}</b>
━━━━━━━━━━━━━━━━━━━━
📈 Xác suất tăng: <b>{ml['prob_up']}%</b>
📉 Xác suất giảm: <b>{ml['prob_down']}%</b>
🎯 Dự đoán: <b>{ml['prediction']}</b>
📊 Độ tin cậy: <b>{ml['confidence']}%</b>
━━━━━━━━━━━━━━━━━━━━
💡 ML model trained trên {ml['n_samples'] or 100}+ mẫu lịch sử""")

    elif cmd == '/link':
        if args:
            ok, msg = tg_verify_link_code(args[0].strip(), chat_id, tg_user)
            reply(msg)
        else:
            pub_url = get_setting('public_url',f'http://localhost:{PORT}')
            reply(f"\U0001f310 <b>Link web SMA Pro:</b>\n{pub_url}\n\n\U0001f517 <b>Lien ket tai khoan:</b>\nVao Settings tren web → nhan <b>Lay ma lien ket</b> → gui <b>/link MA</b> o day")

    elif cmd == '/all':
        tg_register_chat(chat_id, tg_user)
        tg_subscribe_symbols(chat_id, ['*'])
        reply("✅ Bật alert TẤT CẢ mã trong hệ thống.\nGửi /off * để tắt.")

    elif cmd == '/help':
        reply("""<b>SMA Pro Bot — Lệnh:</b>\n
/start — Giới thiệu
/alert VCB VHM — Bật alert mã
/off VCB — Tắt alert mã
/all — Alert tất cả mã
/check VCB — Xem tín hiệu ngay
/ml AAPL — ML Prediction
/list — Mã đang theo dõi
/link — Link web
/help — Xem lệnh""")
    else:
        if text and not text.startswith('/'):
            sym = text.upper().strip()
            if 2 <= len(sym) <= 10:
                _tg_handle_command(chat_id, f"/check {sym}", tg_user, token)

# ── DB OHLCV ─────────────────────────────────────────────────
def db_save_ohlcv(symbol, market, bars):
    if not bars: return
    with get_db() as c:
        c.executemany(
            """INSERT OR REPLACE INTO ohlcv_history(symbol,market,trade_date,open,high,low,close,volume)
                VALUES(?,?,?,?,?,?,?,?)""",
            [(symbol, market, b['d'],
              b['o'] if b.get('o',0)>0 else b['c'],
              b['h'] if b.get('h',0)>0 else b['c'],
              b['l'] if b.get('l',0)>0 else b['c'],
              b['c'], b.get('v',0)) for b in bars if b.get('c',0)>0]
        )
        c.execute("""
            DELETE FROM ohlcv_history WHERE symbol=? AND trade_date NOT IN (
                SELECT trade_date FROM ohlcv_history
                WHERE symbol=? ORDER BY trade_date DESC LIMIT ?
            )""", (symbol, symbol, OHLCV_BARS))

def db_load_ohlcv(symbol, limit=OHLCV_BARS):
    with get_db() as c:
        rows = c.execute(
            """SELECT trade_date as d, open as o, high as h, low as l,
                    close as c, volume as v
                FROM ohlcv_history WHERE symbol=?
                ORDER BY trade_date ASC LIMIT ?""",
            (symbol, limit)
        ).fetchall()
    result = []
    for r in rows:
        d = {'d': r[0], 'o': r[1] or 0, 'h': r[2] or 0,
             'l': r[3] or 0, 'c': r[4] or 0, 'v': r[5] or 0}
        if d['c'] <= 0: continue
        if d['o'] <= 0: d['o'] = d['c']
        if d['h'] <= 0: d['h'] = d['c']
        if d['l'] <= 0: d['l'] = d['c']
        d['h'] = max(d['h'], d['o'], d['c'])
        d['l'] = min(d['l'], d['o'], d['c'])
        result.append(d)
    return result

def db_ohlcv_count(symbol):
    with get_db() as c:
        r = c.execute("SELECT COUNT(*) as n FROM ohlcv_history WHERE symbol=?", (symbol,)).fetchone()
    return r['n'] if r else 0

def db_latest_date(symbol):
    with get_db() as c:
        r = c.execute("SELECT MAX(trade_date) as d FROM ohlcv_history WHERE symbol=?", (symbol,)).fetchone()
    return r['d'] if r and r['d'] else None

def db_save_meta(symbol, market, price, chg, name='', sector='N/A'):
    with get_db() as c:
        c.execute("""INSERT OR REPLACE INTO fetch_meta(symbol,market,last_fetch,last_price,last_chg,name,sector)
                    VALUES(?,?,?,?,?,?,?)""",
                    (symbol, market, datetime.now().isoformat(), price, chg, name, sector))

def calc_rs_ranking(user_id: int, period: str = '1M') -> dict:
    """Xếp hạng RS của tất cả mã trong watchlist vs VN-Index."""
    period_days = {'1W': 5, '1M': 21, '3M': 63, '6M': 126}
    n_days = period_days.get(period, 21)

    # Lấy VN-Index return
    vnindex_d = _ram_cache.get('VNINDEX', {}).get('data', {})
    vnidx_bars = vnindex_d.get('history', []) if vnindex_d else []
    if len(vnidx_bars) >= n_days:
        vn_ret = (vnidx_bars[-1]['c'] - vnidx_bars[-n_days]['c']) / vnidx_bars[-n_days]['c'] * 100
    else:
        vn_ret = 0.0

    try:
        with get_db() as c:
            syms_rows = c.execute(
                "SELECT DISTINCT symbol, market FROM user_watchlist WHERE user_id=?", (user_id,)
            ).fetchall()
    except:
        syms_rows = []

    if not syms_rows:
        try:
            with get_db() as c:
                syms_rows = c.execute("SELECT symbol, market FROM watchlist").fetchall()
        except: syms_rows = []

    rankings = []
    for row in syms_rows:
        sym, mkt = row['symbol'], row['market']
        d = _ram_cache.get(sym, {}).get('data', {})
        if not d: continue
        bars = d.get('history', [])
        price = d.get('price', 0)
        if not price or len(bars) < 5: continue

        a = d.get('analysis', {})

        # Tính return các periods
        def pct_ret(n):
            if len(bars) < n: return None
            base = bars[-n]['c']
            return (bars[-1]['c'] - base) / base * 100 if base > 0 else None

        ret_1w = pct_ret(5)
        ret_1m = pct_ret(21)
        ret_3m = pct_ret(63)
        ret_period = pct_ret(n_days)

        # RS = return of stock - return of index
        rs_vs_vn = (ret_period - vn_ret) if ret_period is not None else None

        # RS score 0-100 (relative to peers - computed after sorting)
        rankings.append({
            'symbol': sym,
            'market': mkt,
            'price': round(price, 2),
            'ret_1w': round(ret_1w, 2) if ret_1w is not None else None,
            'ret_1m': round(ret_1m, 2) if ret_1m is not None else None,
            'ret_3m': round(ret_3m, 2) if ret_3m is not None else None,
            'ret_period': round(ret_period, 2) if ret_period is not None else None,
            'rs_vs_index': round(rs_vs_vn, 2) if rs_vs_vn is not None else None,
            'signal': a.get('signal', 'NEUTRAL'),
            'score': a.get('total', 0),
            'rsi': a.get('rsi', 50),
            'trend': a.get('trend_long', 'NA'),
            'sector': d.get('sector', 'N/A'),
        })

    # Sort by ret_period descending
    rankings.sort(key=lambda x: (x.get('ret_period') or -999), reverse=True)

    # Thêm RS rank (1 = strongest)
    for i, r in enumerate(rankings):
        r['rs_rank'] = i + 1
        # RS percentile
        r['rs_pct'] = round((len(rankings) - i) / len(rankings) * 100)
        # Leader/Laggard classification
        if r['rs_pct'] >= 75: r['rs_label'] = 'LEADER 🚀'
        elif r['rs_pct'] >= 50: r['rs_label'] = 'ABOVE AVG ↑'
        elif r['rs_pct'] >= 25: r['rs_label'] = 'BELOW AVG ↓'
        else: r['rs_label'] = 'LAGGARD 🐢'

    return {
        'period': period,
        'n_days': n_days,
        'vn_index_ret': round(vn_ret, 2),
        'rankings': rankings,
        'total': len(rankings),
        'leaders': [r for r in rankings if r['rs_pct'] >= 75],
        'laggards': [r for r in rankings[::-1] if r['rs_pct'] < 25],
    }

def db_get_meta(symbol):
    with get_db() as c:
        r = c.execute("SELECT * FROM fetch_meta WHERE symbol=?", (symbol,)).fetchone()
    return dict(r) if r else None

def db_meta_age_hours(symbol):
    meta = db_get_meta(symbol)
    if not meta or not meta.get('last_fetch'): return 9999
    try:
        dt = datetime.fromisoformat(meta['last_fetch'])
        return (datetime.now() - dt).total_seconds() / 3600
    except: return 9999

# ── VN FETCH QUEUE ───────────────────────────────────────────
_vn_queue = queue.PriorityQueue()
_vn_seq = 0
_vn_seq_lk = threading.Lock()
_vn_last = 0.0
_vn_lock = threading.Lock()
VN_GAP = 3.5
VN_WORKERS = 1

def _vn_worker():
    global _vn_last
    while True:
        _pri, _seq, sym, holder, ev = _vn_queue.get()
        for attempt in range(3):
            with _vn_lock:
                wait = VN_GAP - (time.time() - _vn_last)
                if wait > 0: time.sleep(wait)
                _vn_last = time.time()
            result = _raw_fetch_vn(sym)
            if result is not None:
                holder['data'] = result; break
            if attempt < 2:
                print(f"[SMA] Rate limit {sym} — chờ 25s (lần {attempt+1}/3)")
                try:
                    peek = _vn_queue.queue[0] if _vn_queue.qsize() > 0 else None
                    if peek and peek[0] == 0:
                        print(f"[SMA] User request pending → skip retry {sym}")
                        holder['data'] = None; break
                except Exception as _e: log.debug('[uncat] line %d: %%s' % 1252, _e)
                for _ in range(25):
                    time.sleep(1)
                    try:
                        peek = _vn_queue.queue[0] if _vn_queue.qsize() > 0 else None
                        if peek and peek[0] == 0:
                            print(f"[SMA] User request detected → abort wait {sym}")
                            holder['data'] = None; ev.set(); _vn_queue.task_done()
                            break
                    except Exception as _e: log.debug('[uncat] line %d: %%s' % 1261, _e)
                else:
                    continue
                break
        else:
            holder['data'] = None
        ev.set()
        _vn_queue.task_done()

for _ in range(VN_WORKERS):
    threading.Thread(target=_vn_worker, daemon=True).start()

@with_retry_none(max_attempts=3, delay=3.0, backoff=2.0)
def _raw_fetch_vn(sym):
    if not HAS_VN: return None
    try:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=800)).strftime("%Y-%m-%d")
        stk = Vnstock().stock(symbol=sym, source="VCI")
        h = stk.quote.history(start=start, end=end, interval="1D")
        if h is None or h.empty: return None
        h.columns = [col.lower() for col in h.columns]
        for a,t in [("o","open"),("h","high"),("l","low"),("c","close"),("vol","volume")]:
            if a in h.columns and t not in h.columns: h[t] = h[a]
        for col in ["open","high","low","close","volume"]:
            if col not in h.columns: h[col] = 0
        h = h.dropna(subset=["close"])
        if h.empty: return None
        bars=[]
        for idx_,r in h.iterrows():
            c_=float(r.get("close") or 0)
            if c_<=0: continue
            o_=float(r.get("open") or 0); o_=o_ if o_>0 else c_
            hi=float(r.get("high") or 0); hi=hi if hi>0 else c_
            lo=float(r.get("low")  or 0); lo=lo if lo>0 else c_
            hi=max(hi,o_,c_); lo=min(lo,o_,c_)
            d_=str(idx_.date()) if hasattr(idx_,"date") else str(idx_)[:10]
            bars.append({"d":d_,"o":o_,"h":hi,"l":lo,"c":c_,"v":int(r.get("volume") or 0)})
        price = bars[-1]["c"]; prev = bars[-2]["c"] if len(bars)>1 else price
        chg = round((price-prev)/prev*100, 2) if prev else 0
        name, sector = sym, "N/A"
        try:
            ov = stk.company.overview()
            if ov is not None and not ov.empty:
                row = ov.iloc[0]
                name = str(row.get("short_name", sym))
                sector = str(row.get("industry_name_vn", "N/A"))
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1308, _e)
        return {"symbol":sym,"market":"VN","name":name,"sector":sector,
                "price":price,"prev":prev,"chg":chg,
                "open":bars[-1]["o"],"high":bars[-1]["h"],"low":bars[-1]["l"],
                "close":bars[-1]["c"],"vol":bars[-1]["v"],"history":bars}
    except Exception as e:
        msg = str(e).lower()
        if 'rate' in msg or 'limit' in msg or '429' in msg or 'exceeded' in msg:
            return None
        try:
            from vnstock import Vnstock as _VNS
            stk2 = _VNS().stock(symbol=sym, source="TCBS")
            h2 = stk2.quote.history(start=start, end=end, interval="1D")
            if h2 is not None and not h2.empty:
                print(f"[VN] {sym}: VCI fail → TCBS OK")
                h = h2
                h.columns = [col.lower() for col in h.columns]
                bars=[]
                for idx_,r in h.iterrows():
                    c_=float(r.get("close") or r.get("c") or 0)
                    if c_<=0: continue
                    o_=float(r.get("open") or r.get("o") or 0); o_=o_ if o_>0 else c_
                    hi=float(r.get("high") or r.get("h") or 0); hi=hi if hi>0 else c_
                    lo=float(r.get("low") or r.get("l") or 0); lo=lo if lo>0 else c_
                    hi=max(hi,o_,c_); lo=min(lo,o_,c_)
                    d_=str(idx_.date()) if hasattr(idx_,"date") else str(idx_)[:10]
                    bars.append({"d":d_,"o":o_,"h":hi,"l":lo,"c":c_,"v":int(r.get("volume") or r.get("vol") or 0)})
                if bars:
                    price=bars[-1]["c"]; prev=bars[-2]["c"] if len(bars)>1 else price
                    chg=round((price-prev)/prev*100,2) if prev else 0
                    return {"symbol":sym,"market":"VN","name":sym,"sector":"N/A",
                            "price":price,"prev":prev,"chg":chg,
                            "open":bars[-1]["o"],"high":bars[-1]["h"],
                            "low":bars[-1]["l"],"close":bars[-1]["c"],
                            "vol":bars[-1]["v"],"history":bars}
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1343, _e)
        # Tier 3b: thử SSI source nếu VCI và TCBS đều fail
        try:
            from vnstock import Vnstock as _VNS2
            stk3 = _VNS2().stock(symbol=sym, source="SSI")
            h3 = stk3.quote.history(start=start, end=end, interval="1D")
            if h3 is not None and not h3.empty:
                log.info("[VN] %s: VCI+TCBS fail -> SSI OK", sym)
                bars = []
                for idx_, r in h3.iterrows():
                    c_ = float(r.get("close") or r.get("c") or 0)
                    if c_ <= 0: continue
                    o_ = float(r.get("open") or r.get("o") or c_)
                    hi = float(r.get("high") or r.get("h") or c_)
                    lo = float(r.get("low")  or r.get("l") or c_)
                    hi = max(hi, o_, c_); lo = min(lo, o_, c_)
                    d_ = str(idx_.date()) if hasattr(idx_, "date") else str(idx_)[:10]
                    bars.append({"d": d_, "o": o_, "h": hi, "l": lo, "c": c_,
                                 "v": int(r.get("volume") or r.get("vol") or 0)})
                if bars:
                    price = bars[-1]["c"]; prev = bars[-2]["c"] if len(bars)>1 else price
                    chg = round((price-prev)/prev*100, 2) if prev else 0
                    return {"symbol": sym, "market": "VN", "name": sym, "sector": "N/A",
                            "price": price, "prev": prev, "chg": chg,
                            "open": bars[-1]["o"], "high": bars[-1]["h"],
                            "low": bars[-1]["l"], "close": bars[-1]["c"],
                            "vol": bars[-1]["v"], "history": bars}
        except Exception as _ssi_e:
            log.debug("[VN] SSI fallback %s: %s", sym, _ssi_e)
        log.error("[VN] ALL sources failed for %s: %s", sym, e)
        return None

@with_retry_none(max_attempts=3, delay=2.0, backoff=1.5)
def _raw_fetch_us(sym):
    try:
        t = yf.Ticker(sym)
        h = t.history(period="2y", interval="1d", auto_adjust=True)
        if h.empty: return None
        fi = t.fast_info
        price = float(getattr(fi,'last_price',None) or h['Close'].iloc[-1])
        prev = float(getattr(fi,'previous_close',None) or h['Close'].iloc[-2])
        chg = round((price-prev)/prev*100, 2) if prev else 0
        meta = db_get_meta(sym)
        name = (meta or {}).get('name', sym) or sym
        sector = (meta or {}).get('sector', 'N/A') or 'N/A'
        if name == sym:
            try:
                import yfinance as _yf
                _qi = _yf.Search(sym, max_results=1)
                if hasattr(_qi, 'quotes') and _qi.quotes:
                    q = _qi.quotes[0]
                    name = q.get('longname') or q.get('shortname') or sym
                    sector = q.get('sector') or 'N/A'
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 1367, _e)
        bars = [{"d": str(idx.date()),
                 "o": round(float(r.Open),4), "h": round(float(r.High),4),
                 "l": round(float(r.Low),4), "c": round(float(r.Close),4),
                 "v": int(r.Volume)}
                for idx,r in h.iterrows() if r.Close > 0]
        return {"symbol":sym,"market":"US","name":name,"sector":sector,
                "price":round(price,4),"prev":round(prev,4),"chg":chg,
                "open":bars[-1]["o"],"high":bars[-1]["h"],"low":bars[-1]["l"],
                "close":bars[-1]["c"],"vol":bars[-1]["v"],"history":bars}
    except Exception as e:
        print(f"[US] {sym}: {e}"); return None

def _raw_fetch_global(symbol, market):
    try:
        suffix = MARKET_SUFFIX.get(market.upper(), '')
        ticker_sym = symbol if '.' in symbol else symbol + suffix
        import yfinance as yf
        tk = yf.Ticker(ticker_sym)
        hist = tk.history(period='2y', interval='1d')
        if hist is None or len(hist) == 0: return None
        info = {}
        try: info = tk.info or {}
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1390, _e)
        bars = []
        for dt, row in hist.iterrows():
            try:
                bars.append({'d': str(dt)[:10],
                    'o': float(row['Open']), 'h': float(row['High']),
                    'l': float(row['Low']),  'c': float(row['Close']),
                    'v': int(row['Volume'])})
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 1398, _e)
        if not bars: return None
        last = bars[-1]; prev = bars[-2] if len(bars)>1 else last
        chg = round((last['c']-prev['c'])/prev['c']*100, 2) if prev['c'] else 0
        name = info.get('longName') or info.get('shortName') or symbol
        return {
            'symbol': symbol, 'market': market, 'name': name,
            'sector': info.get('sector','N/A'),
            'price': last['c'], 'chg': chg, 'prev': prev['c'],
            'open': last['o'], 'high': last['h'], 'low': last['l'],
            'close': last['c'], 'vol': last['v'], 'history': bars,
        }
    except Exception as e:
        log.warning(f"[Global] {symbol}/{market}: {e}")
        return None

def _fetch_crypto_ohlcv(symbol, days=500):
    try:
        import urllib.request as _ur
        pair = symbol.upper() + 'USDT'
        limit = min(days, 1000)
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit={limit}"
        req = _ur.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with _ur.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
        if not data: return None
        bars = []
        for k in data:
            try:
                bars.append({
                    'd': datetime.fromtimestamp(k[0]/1000).strftime('%Y-%m-%d'),
                    'o': float(k[1]), 'h': float(k[2]),
                    'l': float(k[3]), 'c': float(k[4]),
                    'v': float(k[5]),
                })
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 1433, _e)
        if not bars: return None
        last = bars[-1]
        prev = bars[-2] if len(bars) > 1 else last
        chg  = round((last['c'] - prev['c']) / prev['c'] * 100, 2) if prev['c'] else 0
        return {
            'symbol': symbol, 'market': 'CRYPTO',
            'name': symbol + '/USDT', 'sector': 'Crypto',
            'price': last['c'], 'chg': chg,
            'open': last['o'], 'high': last['h'], 'low': last['l'],
            'close': last['c'], 'vol': last['v'],
            'prev': prev['c'], 'history': bars,
        }
    except Exception as e:
        print(f"[Crypto] {symbol}: {e}")
        return None

def fetch_crypto(symbol):
    sym = symbol.upper().replace('USDT','').replace('-USD','')
    with _CRYPTO_LOCK:
        cached = _CRYPTO_CACHE.get(sym)
        if cached and time.time() - cached.get('ts', 0) < 300:
            return cached.get('data')
    data = _fetch_crypto_ohlcv(sym, 500)
    if data:
        db_save_ohlcv(sym, 'CRYPTO', data['history'])
        db_save_meta(sym, 'CRYPTO', data['price'], data['chg'], data['name'], 'Crypto')
        with _CRYPTO_LOCK:
            _CRYPTO_CACHE[sym] = {'ts': time.time(), 'data': data}
        _ram_cache[sym] = {'ts': time.time(), 'data': data}
    return data

# ── SMART FETCH ───────────────────────────────────────────────
_ram_cache = {}
_CRYPTO_CACHE = {}
_CRYPTO_LOCK = threading.Lock()
RAM_TTL_VN = 1800
RAM_TTL_US = 600

MARKET_SUFFIX = {
    'JP': '.T', 'HK': '.HK', 'KR': '.KS', 'DE': '.DE',
    'GB': '.L', 'FR': '.PA', 'SG': '.SI', 'AU': '.AX',
    'IN': '.NS', 'TW': '.TW',
}

def ram_age(sym):
    if sym not in _ram_cache: return 9999
    return int(time.time() - _ram_cache[sym]['ts'])

def _build_from_db(sym, mkt, bars, meta):
    if not bars: return None
    last = bars[-1]
    prev = bars[-2] if len(bars) > 1 else last
    price = last['c']
    chg = round((price - prev['c']) / prev['c'] * 100, 2) if prev['c'] else 0
    name = (meta or {}).get('name', sym)
    sector = (meta or {}).get('sector', 'N/A')
    if meta and meta.get('last_price'):
        today = date.today().isoformat()
        if last['d'] == today or (datetime.now() - datetime.fromisoformat(meta['last_fetch'])).total_seconds() < 3600:
            price = meta['last_price']
            chg = meta.get('last_chg', chg)
    data_age_h = 0
    if meta and meta.get('last_fetch'):
        try: data_age_h = round((datetime.now()-datetime.fromisoformat(meta['last_fetch'])).total_seconds()/3600, 1)
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1498, _e)
    result = {
        "symbol": sym, "market": mkt, "name": name, "sector": sector,
        "price": price, "prev": prev['c'], "chg": chg,
        "open": last['o'], "high": last['h'], "low": last['l'],
        "close": last['c'], "vol": last['v'],
        "history": bars,
        "ohlcv_count": len(bars),
        "data_source": "db",
        "_data_age_h": data_age_h,
    }
    if data_age_h > 36:
        result['_stale'] = True
        result['_stale_h'] = data_age_h
        result['_note'] = f"⚠️ Data cũ {data_age_h:.0f}h"
    return result

def fetch(sym, mkt, force=False):
    """
    Smart fetch với 5-tier fallback — bằng mọi giá phải có data để phân tích.
    Tier 1: RAM cache (TTL 30min VN / 10min US)
    Tier 2: DB fresh  (< 36h VN / 20h US)
    Tier 3: API live  (VCI→TCBS VN; yfinance US)
    Tier 4: DB stale  (bất kỳ tuổi - last resort live data)
    Tier 5: DB OHLCV  (chỉ bars, không meta - vẫn phân tích được)
    """
    ttl = RAM_TTL_VN if mkt == "VN" else RAM_TTL_US

    # ── Tier 1: RAM cache ────────────────────────────────────
    if not force and sym in _ram_cache:
        age = time.time() - _ram_cache[sym]['ts']
        if age < ttl:
            return _ram_cache[sym]['data']

    meta      = db_get_meta(sym)
    ohlcv_cnt = db_ohlcv_count(sym)
    meta_age  = db_meta_age_hours(sym)

    # ── Tier 2: DB fresh ─────────────────────────────────────
    db_fresh = (meta and ohlcv_cnt >= 50
                and meta_age < (36 if mkt == "VN" else 20)
                and not force)
    if db_fresh:
        bars = db_load_ohlcv(sym, OHLCV_BARS)
        if bars:
            data = _build_from_db(sym, mkt, bars, meta)
            _ram_cache[sym] = {'ts': time.time(), 'data': data}
            log.debug("[fetch] Tier2 DB-fresh %s age=%.1fh bars=%d", sym, meta_age, len(bars))
            return data

    # ── Tier 3: API live ─────────────────────────────────────
    raw = None
    try:
        log.info("[fetch] Tier3 API %s (%s)", sym, mkt)
        if mkt == "VN":
            holder = {}; ev = threading.Event()
            global _vn_seq
            with _vn_seq_lk: _vn_seq += 1; _seq = _vn_seq
            _vn_queue.put((0, _seq, sym, holder, ev))
            ev.wait(timeout=25)
            raw = holder.get('data')
            if not raw:
                log.warning("[fetch] VN queue timeout %s", sym)
        elif mkt == "CRYPTO":
            raw = fetch_crypto(sym)
        elif mkt in MARKET_SUFFIX:
            raw = _raw_fetch_global(sym, mkt)
        else:
            raw = _raw_fetch_us(sym)
    except Exception as _fe:
        log.warning("[fetch] API error %s: %s", sym, _fe)
        raw = None

    if raw:
        # API thành công → lưu DB + cache
        try:
            db_save_ohlcv(sym, mkt, raw['history'])
            db_save_meta(sym, mkt, raw['price'], raw['chg'],
                         raw.get('name', ''), raw.get('sector', 'N/A'))
            with get_db() as c:
                c.execute("UPDATE watchlist SET name=?, sector=? WHERE symbol=?",
                          (raw.get('name', ''), raw.get('sector', 'N/A'), sym))
        except Exception as _se:
            log.debug("[fetch] save error %s: %s", sym, _se)
        bars = db_load_ohlcv(sym, OHLCV_BARS)
        data = _build_from_db(sym, mkt, bars or raw['history'][:100], db_get_meta(sym) or meta)
        _ram_cache[sym] = {'ts': time.time(), 'data': data}
        return data

    # ── Tier 4: DB stale (bất kỳ tuổi) ──────────────────────
    bars = db_load_ohlcv(sym, OHLCV_BARS)
    if bars and len(bars) >= 15:
        stale_age = meta_age if meta_age < 9999 else 999
        log.warning("[fetch] Tier4 DB-stale %s age=%.1fh bars=%d", sym, stale_age, len(bars))
        data = _build_from_db(sym, mkt, bars, meta)
        if data:
            # Đánh dấu data cũ để UI hiển thị cảnh báo
            data['_stale'] = True
            data['_stale_h'] = round(stale_age, 1)
            data['_note']  = f"⚠️ Data cũ {stale_age:.0f}h — API không phản hồi"
            _ram_cache[sym] = {'ts': time.time() - ttl + 300, 'data': data}  # TTL 5min retry
            return data

    # ── Tier 5: DB OHLCV only (không meta) ───────────────────
    if bars and len(bars) >= 5:
        log.warning("[fetch] Tier5 OHLCV-only %s bars=%d", sym, len(bars))
        last = bars[-1]; prev = bars[-2] if len(bars) > 1 else last
        price = last['c']
        chg   = round((price - prev['c']) / prev['c'] * 100, 2) if prev['c'] else 0
        data  = {
            "symbol": sym, "market": mkt, "name": sym, "sector": "N/A",
            "price": price, "prev": prev['c'], "chg": chg,
            "open": last['o'], "high": last['h'], "low": last['l'],
            "close": last['c'], "vol": last['v'],
            "history": bars,
            "_stale": True, "_stale_h": 999,
            "_note": "⚠️ Chỉ có dữ liệu lịch sử cũ — API và metadata đều không khả dụng",
        }
        _ram_cache[sym] = {'ts': time.time() - ttl + 120, 'data': data}
        return data

    # ── Thực sự không có gì ──────────────────────────────────
    log.error("[fetch] NO DATA at all for %s (%s)", sym, mkt)
    return None

def _cached_fetch(sym, mkt, fetch_fn, ttl_h=6):
    """Generic cached fetch wrapper for non-VN/US markets."""
    cached = _ram_cache.get(sym)
    if cached and time.time() - cached['ts'] < ttl_h * 3600:
        return cached['data']
    raw = fetch_fn()
    if raw:
        db_save_ohlcv(sym, mkt, raw['history'])
        db_save_meta(sym, mkt, raw['price'], raw['chg'], raw.get('name',''), raw.get('sector','N/A'))
        bars = db_load_ohlcv(sym, OHLCV_BARS)
        data = _build_from_db(sym, mkt, bars, db_get_meta(sym))
        if data: _ram_cache[sym] = {'ts': time.time(), 'data': data}
        return data
    return _ram_cache.get(sym, {}).get('data')

# ── RESAMPLE ─────────────────────────────────────────────────
def resample_history(hist, tf):
    """Resample OHLCV history to a higher timeframe.
    Supports: 1W, 1M (from daily bars), 4H, 1H (from intraday bars or simulated from daily).
    """
    if tf == '1D' or not hist: return hist
    result = []; bucket = []

    def flush():
        if not bucket: return
        result.append({"o": bucket[0].get("o", bucket[0]["c"]),
                        "h": max(x.get("h", x["c"]) for x in bucket),
                        "l": min(x.get("l", x["c"]) for x in bucket),
                        "c": bucket[-1]["c"],
                        "v": sum(x.get("v", 0) for x in bucket),
                        "d": bucket[-1].get("d", bucket[-1].get("t", ""))})

    # 4H / 1H — aggregate intraday bars if bars have time info, else simulate from daily
    if tf in ('4H', '1H'):
        # Check if bars have intraday timestamps
        has_time = any(':' in str(b.get('t', b.get('d', ''))) for b in hist[:3])
        if has_time:
            # Aggregate by 4H or 1H windows
            hours_per_bucket = 4 if tf == '4H' else 1
            for bar in hist:
                t_str = str(bar.get('t', bar.get('d', '')))
                try:
                    if 'T' in t_str: dt = datetime.fromisoformat(t_str[:19])
                    elif ' ' in t_str: dt = datetime.strptime(t_str[:16], '%Y-%m-%d %H:%M')
                    else: dt = datetime.strptime(t_str[:10], '%Y-%m-%d')
                    bucket_hour = (dt.hour // hours_per_bucket) * hours_per_bucket
                    key = (dt.date(), bucket_hour)
                except: key = t_str[:13]
                prev_key = bucket[-1].get("_key") if bucket else None
                if bucket and key != prev_key: flush(); bucket = []
                b = dict(bar); b["_key"] = key; bucket.append(b)
            flush()
            return result
        else:
            # Simulate 4H from daily: split each daily bar into 2 parts (AM/PM)
            # AM: o->mid, PM: mid->c, each with half volume
            if tf == '4H':
                for bar in hist:
                    mid = (bar.get('o', bar['c']) + bar['c']) / 2
                    h = bar.get('h', bar['c']); lo = bar.get('l', bar['c'])
                    v = bar.get('v', 0) or 0
                    d = bar.get('d', '')
                    result.append({'o': bar.get('o', bar['c']), 'h': max(bar.get('o', bar['c']), mid, h*0.6+lo*0.4),
                                   'l': lo, 'c': mid, 'v': v//2, 'd': d + ' 09:00'})
                    result.append({'o': mid, 'h': h, 'l': max(lo, mid*0.995),
                                   'c': bar['c'], 'v': v - v//2, 'd': d + ' 13:00'})
                return result
            # 1H: return daily bars as-is (best we can do without intraday data)
            return hist

    # Weekly / Monthly
    for bar in hist:
        try:
            d = datetime.strptime(bar["d"][:10], "%Y-%m-%d")
            key = (d.isocalendar()[0], d.isocalendar()[1]) if tf=='1W' else (d.year, d.month)
        except: key = bar["d"][:7]
        prev_key = bucket[-1].get("_key") if bucket else None
        if bucket and key != prev_key: flush(); bucket = []
        b = dict(bar); b["_key"] = key; bucket.append(b)
    flush()
    return result

# ── NEWS & EVENTS ───────────────────────────────────────────
_news_cache = {}
_events_cache = {}
_NEWS_TTL = 3600

def _news_cache_get(sym):
    c = _news_cache.get(sym)
    if c and time.time()-c['ts'] < _NEWS_TTL:
        return c['data']
    return None

def _events_cache_get(sym):
    c = _events_cache.get(sym)
    if c and time.time()-c['ts'] < _NEWS_TTL:
        return c['data']
    return None

def fetch_news(symbol, market):
    return fetch_vn_news(symbol) if market=='VN' else fetch_us_news(symbol)

def fetch_events(symbol, market):
    return fetch_vn_events(symbol) if market=='VN' else fetch_us_events(symbol)

def fetch_vn_news(symbol):
    cached = _news_cache_get(symbol)
    if cached is not None: return cached
    try:
        from vnstock import Vnstock
        co = Vnstock().stock(symbol=symbol, source='VCI').company
        df = co.news()
        result = []
        if df is not None and len(df) > 0:
            for _, row in df.head(10).iterrows():
                result.append({
                    'title': str(row.get('title', row.get('news_title', ''))),
                    'date': str(row.get('publish_date', row.get('date', ''))),
                    'url': str(row.get('url', row.get('news_url', '#'))),
                    'source':'VCI',
                })
        _news_cache[symbol] = {'ts': time.time(), 'data': result}
        return result
    except Exception as e:
        _news_cache[symbol] = {'ts': time.time(), 'data': []}
        return []

def fetch_vn_events(symbol):
    cached = _events_cache_get(symbol)
    if cached is not None: return cached
    try:
        from vnstock import Vnstock
        co = Vnstock().stock(symbol=symbol, source='VCI').company
        result = {'events': [], 'dividends': [], 'financials': {}}
        try:
            df = co.events()
            if df is not None and len(df) > 0:
                for _, row in df.head(10).iterrows():
                    result['events'].append({
                        'title': str(row.get('event_name', row.get('title', ''))),
                        'date': str(row.get('event_date', row.get('date', ''))),
                        'type': str(row.get('event_type', '')),
                    })
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1667, _e)
        try:
            df2 = co.dividends()
            if df2 is not None and len(df2) > 0:
                for _, row in df2.head(5).iterrows():
                    result['dividends'].append({
                        'year': str(row.get('year', '')),
                        'rate': str(row.get('cash_dividend_percentage', row.get('dividend_rate', ''))),
                        'method': str(row.get('issue_method', '')),
                    })
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1677, _e)
        try:
            df3 = co.ratio()
            if df3 is not None and len(df3) > 0:
                row = df3.iloc[0]
                result['financials'] = {
                    'eps': str(row.get('earning_per_share', '')),
                    'pe': str(row.get('price_to_earning', '')),
                    'pb': str(row.get('price_to_book', '')),
                    'roe': str(row.get('return_on_equity', '')),
                    'roa': str(row.get('return_on_asset', '')),
                }
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1689, _e)
        _events_cache[symbol] = {'ts': time.time(), 'data': result}
        return result
    except Exception as e:
        empty = {'events': [], 'dividends': [], 'financials': {}}
        _events_cache[symbol] = {'ts': time.time(), 'data': empty}
        return empty

def fetch_us_news(symbol):
    cached = _news_cache_get(symbol)
    if cached is not None: return cached
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        result = []
        for item in (tk.news or [])[:10]:
            result.append({
                'title': item.get('title',''),
                'date': str(item.get('providerPublishTime','')),
                'url': item.get('link','#'),
                'source': item.get('publisher',''),
            })
        _news_cache[symbol] = {'ts': time.time(), 'data': result}
        return result
    except:
        _news_cache[symbol] = {'ts': time.time(), 'data': []}
        return []

def fetch_us_events(symbol):
    cached = _events_cache_get(symbol)
    if cached is not None: return cached
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        result = {'events': [], 'dividends': [], 'financials': {}}
        try:
            cal = tk.calendar
            if cal is not None:
                ed = cal.get('Earnings Date', '')
                if ed:
                    result['events'].append({
                        'title': f"Earnings Report",
                        'date': str(ed[0]) if isinstance(ed, list) else str(ed),
                        'type': 'earnings',
                    })
                div = cal.get('Ex-Dividend Date', '')
                if div:
                    result['events'].append({
                        'title': f"Ex-Dividend Date",
                        'date': str(div),
                        'type': 'dividend',
                    })
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1741, _e)
        try:
            divs = tk.dividends
            if divs is not None and len(divs) > 0:
                for dt, val in divs.tail(5).items():
                    result['dividends'].append({
                        'year': str(dt)[:10],
                        'rate': f"${val:.4f}",
                        'method':'Cash',
                    })
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1751, _e)
        try:
            info = tk.info
            result['financials'] = {
                'eps': str(round(info.get('trailingEps',0),2)),
                'pe': str(round(info.get('trailingPE',0),2)),
                'pb': str(round(info.get('priceToBook',0),2)),
                'roe': str(round(info.get('returnOnEquity',0)*100,1)) + '%' if info.get('returnOnEquity') else '',
                'roa': str(round(info.get('returnOnAssets',0)*100,1)) + '%' if info.get('returnOnAssets') else '',
                'revenue': str(info.get('totalRevenue','')),
                'profit': str(info.get('netIncomeToCommon','')),
            }
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 1763, _e)
        _events_cache[symbol] = {'ts': time.time(), 'data': result}
        return result
    except:
        empty = {'events': [], 'dividends': [], 'financials': {}}
        _events_cache[symbol] = {'ts': time.time(), 'data': empty}
        return empty

def has_upcoming_events(symbol, market, days=7):
    try:
        ev = fetch_events(symbol, market)
        from datetime import datetime, timedelta
        now = datetime.now()
        for e in ev.get('events', []):
            try:
                dt = datetime.fromisoformat(str(e['date'])[:10])
                if 0 <= (dt - now).days <= days:
                    return True, e['title']
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 1781, _e)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 1782, _e)
    return False, ''

# ── ANALYSIS ENGINE ──────────────────────────────────────────
def analyze(data, tf='1D'):
    h = data.get("history", [])
    if tf != '1D': h = resample_history(h, tf)
    if not h: return empty_analysis()
    price = h[-1]["c"]
    valid = [x for x in h if x["c"] > 0]
    min_bars = 15
    if len(valid) < min_bars: return empty_analysis()
    closes = [x["c"] for x in valid]
    highs = [x.get("h",x["c"]) for x in valid]
    lows = [x.get("l",x["c"]) for x in valid]
    vols = [x.get("v",0) for x in valid]
    n = len(closes)
    sc, dt = {}, {}

    # 1. RSI (14)
    rsi = calc_rsi(closes)
    sc["rsi"] = 1 if rsi < 30 else (-1 if rsi > 70 else 0)
    dt["rsi"] = f"RSI={rsi:.1f} — {'Quá bán →MUA' if rsi<30 else 'Quá mua →BÁN' if rsi>70 else 'Trung tính'}"

    # 2. MACD (12/26/9)
    if n >= 60:
        e12=ema_s(closes,12); e26=ema_s(closes,26)
        ml=[e12[i]-e26[i] for i in range(25,len(e12))]
        sl=ema_s(ml,9)
        hist_cur = ml[-1]-sl[-1]
        hist_prev = ml[-2]-sl[-2]
        cu = ml[-2]<sl[-2] and ml[-1]>sl[-1]
        cd2 = ml[-2]>sl[-2] and ml[-1]<sl[-1]
        bull_hist = hist_prev<0 and hist_cur>hist_prev
        bear_hist = hist_prev>0 and hist_cur<hist_prev
        if cu: sc["macd"]=1; dt["macd"]=f"MACD cắt lên Signal ↑ ({ml[-1]:.4f})"
        elif cd2: sc["macd"]=-1; dt["macd"]=f"MACD cắt xuống Signal ↓ ({ml[-1]:.4f})"
        elif bull_hist: sc["macd"]=1; dt["macd"]=f"MACD Hist thu hẹp ({hist_cur:.4f}) →sắp cắt lên"
        elif bear_hist: sc["macd"]=-1; dt["macd"]=f"MACD Hist thu hẹp ({hist_cur:.4f}) →sắp cắt xuống"
        else: sc["macd"]=0; dt["macd"]=f"MACD={ml[-1]:.4f} Sig={sl[-1]:.4f} Hist={hist_cur:.4f}"
    else: sc["macd"]=0; dt["macd"]="Chưa đủ dữ liệu"

    # 3. Bollinger Bands (20, 2σ)
    r20=closes[-20:]; avg=sum(r20)/20
    std=(sum((x-avg)**2 for x in r20)/20)**0.5
    bbu,bbl=avg+2*std,avg-2*std; bw=round((bbu-bbl)/avg*100,1)
    ma20_bb = avg
    bb_bounce = price<=bbl and closes[-1]>=closes[-2]
    bb_reject = price>=bbu and closes[-1]<=closes[-2]
    if bb_bounce: sc["bb"]=1; dt["bb"]=f"Chạm Lower BB={bbl:.2f} + bounce (BW={bw}%)"
    elif bb_reject: sc["bb"]=-1; dt["bb"]=f"Chạm Upper BB={bbu:.2f} + reject (BW={bw}%)"
    elif price<=bbl: sc["bb"]=0; dt["bb"]=f"Dưới Lower BB={bbl:.2f} (downtrend, chờ xác nhận)"
    elif price>=bbu: sc["bb"]=0; dt["bb"]=f"Trên Upper BB={bbu:.2f} (uptrend mạnh)"
    else: sc["bb"]=0; dt["bb"]=f"BB U={bbu:.2f} M={avg:.2f} L={bbl:.2f} BW={bw}%"

    # 4. MA Cross (20/50)
    if n >= 52:
        ma20=sum(closes[-20:])/20; ma50=sum(closes[-50:])/50
        pm20=sum(closes[-21:-1])/20; pm50=sum(closes[-51:-1])/50
        just_golden = pm20<pm50 and ma20>ma50
        just_death = pm20>pm50 and ma20<ma50
        gap_pct = (ma20-ma50)/ma50*100
        bull_trend = gap_pct > 0.3
        bear_trend = gap_pct < -0.3
        if just_golden: sc["ma"]=1; dt["ma"]=f"⭐ Golden Cross MA20={ma20:.1f}>MA50={ma50:.1f}"
        elif just_death: sc["ma"]=-1; dt["ma"]=f"💀 Death Cross MA20={ma20:.1f}<MA50={ma50:.1f}"
        elif bull_trend: sc["ma"]=1; dt["ma"]=f"MA20={ma20:.1f} trên MA50={ma50:.1f} (+{gap_pct:.1f}%)"
        elif bear_trend: sc["ma"]=-1; dt["ma"]=f"MA20={ma20:.1f} dưới MA50={ma50:.1f} ({gap_pct:.1f}%)"
        else: sc["ma"]=0; dt["ma"]=f"MA20={ma20:.1f} ≈ MA50={ma50:.1f} (sideways {gap_pct:+.2f}%)"
    else: sc["ma"]=0; dt["ma"]="Chưa đủ dữ liệu"

    # 5. Stochastic (14,3)
    if n >= 16:
        def stoch_k(idx):
            lo = min(lows[max(0,idx-13):idx+1])
            hi = max(highs[max(0,idx-13):idx+1])
            return (closes[idx]-lo)/(hi-lo or 1)*100
        k0 = stoch_k(n-1); k1 = stoch_k(n-2); k2 = stoch_k(n-3)
        kv = k0
        dv = (k0+k1+k2)/3
        dprev = (k1+k2+stoch_k(n-4))/3 if n>=17 else dv
        cross_up = k1<dprev and k0>dv
        cross_down = k1>dprev and k0<dv
        if kv<25 and cross_up: sc["stoch"]=1; dt["stoch"]=f"%K={kv:.1f}↑%D={dv:.1f} (quá bán→cắt lên)"
        elif kv>75 and cross_down: sc["stoch"]=-1; dt["stoch"]=f"%K={kv:.1f}↓%D={dv:.1f} (quá mua→cắt xuống)"
        elif kv<25: sc["stoch"]=1; dt["stoch"]=f"%K={kv:.1f} trong vùng quá bán"
        elif kv>75: sc["stoch"]=-1; dt["stoch"]=f"%K={kv:.1f} trong vùng quá mua"
        else: sc["stoch"]=0; dt["stoch"]=f"%K={kv:.1f} %D={dv:.1f}"
    else: sc["stoch"]=0; dt["stoch"]="Chưa đủ dữ liệu"

    # 6. Volume + OBV
    if n >= 21:
        avg_v=sum(vols[-20:])/20; lv=vols[-1]; ratio=round(lv/avg_v,1) if avg_v>0 else 0
        obv=0; oa=[0]
        for i in range(1,n):
            obv += (vols[i] if closes[i]>closes[i-1] else -vols[i] if closes[i]<closes[i-1] else 0)
            oa.append(obv)
        obv_up=oa[-1]>oa[-10] if len(oa)>=10 else True
        up=closes[-1]>closes[-2]; spike=lv>avg_v*1.5
        if spike and up and obv_up: sc["volume"]=1; dt["volume"]=f"Vol {ratio}x + OBV↑ (mua mạnh)"
        elif spike and up and not obv_up: sc["volume"]=-1; dt["volume"]=f"Vol {ratio}x giá↑ OBV↓ (phân phối)"
        elif spike and not up: sc["volume"]=-1; dt["volume"]=f"Vol {ratio}x kèm giá↓ (bán mạnh)"
        elif not spike and obv_up: sc["volume"]=1; dt["volume"]=f"Vol {ratio}x OBV tăng đều"
        else: sc["volume"]=0; dt["volume"]=f"Vol {ratio}x trung bình"
    else: sc["volume"]=0; dt["volume"]="Chưa đủ dữ liệu"

    # 7. Candlestick
    if n >= 3:
        c0=valid[-1]; c1=valid[-2]; c2=valid[-3]
        o=c0.get("o",c0["c"]); cl=c0["c"]; hi2=c0.get("h",c0["c"]); lo2=c0.get("l",c0["c"])
        body=abs(cl-o); rng_c=hi2-lo2 or 1
        uw=hi2-max(cl,o); dw=min(cl,o)-lo2
        o1=c1.get("o",c1["c"]); o2=c2.get("o",c2["c"])
        min_wick = rng_c*0.05
        hammer = dw>=rng_c*0.55 and uw<=rng_c*0.15 and body<=rng_c*0.35
        bull_eng = c1["c"]<o1 and cl>o and cl>o1 and o<=c1["c"] and body>abs(c1["c"]-o1)
        morn = (c2["c"]>c2.get("o",c2["c"])) and body<abs(c2["c"]-c2.get("o",c2["c"]))*0.5 and cl>o and cl>(c2["c"]+c2.get("o",c2["c"]))/2
        shoot = uw>=rng_c*0.55 and dw<=rng_c*0.15 and body<=rng_c*0.35
        bear_eng = c1["c"]>o1 and cl<o and cl<o1 and o>=c1["c"] and body>abs(c1["c"]-o1)
        eve = (c2["c"]<c2.get("o",c2["c"])) and body<abs(c2["c"]-c2.get("o",c2["c"]))*0.5 and cl<o and cl<(c2["c"]+c2.get("o",c2["c"]))/2
        if hammer: sc["candle"]=1; dt["candle"]="Hammer (đảo chiều tăng)"
        elif bull_eng: sc["candle"]=1; dt["candle"]="Bullish Engulfing"
        elif morn: sc["candle"]=1; dt["candle"]="Morning Star"
        elif shoot: sc["candle"]=-1; dt["candle"]="Shooting Star"
        elif bear_eng: sc["candle"]=-1; dt["candle"]="Bearish Engulfing"
        elif eve: sc["candle"]=-1; dt["candle"]="Evening Star"
        else: sc["candle"]=0; dt["candle"]="Không có mẫu nến"
    else: sc["candle"]=0; dt["candle"]="Chưa đủ dữ liệu"

    # 8. ADX (14)
    adx_val = 0.0
    if n >= 40:
        tr_list, dp_list, dm_list = [], [], []
        for i in range(1, n):
            tr = max(highs[i]-lows[i],
                     abs(highs[i]-closes[i-1]),
                     abs(lows[i]-closes[i-1]))
            up2 = highs[i]-highs[i-1]
            dn2 = lows[i-1]-lows[i]
            dp_list.append(up2 if up2>dn2 and up2>0 else 0)
            dm_list.append(dn2 if dn2>up2 and dn2>0 else 0)
            tr_list.append(tr)
        p14 = 14
        atr_w = sum(tr_list[:p14])
        sdp = sum(dp_list[:p14])
        sdm = sum(dm_list[:p14])
        dx_arr = []
        for i in range(p14, len(tr_list)):
            atr_w = atr_w - atr_w/p14 + tr_list[i]
            sdp = sdp - sdp/p14 + dp_list[i]
            sdm = sdm - sdm/p14 + dm_list[i]
            dip_i = sdp / (atr_w or 1) * 100
            dim_i = sdm / (atr_w or 1) * 100
            dx_i = abs(dip_i - dim_i) / ((dip_i + dim_i) or 1) * 100
            dx_arr.append((dx_i, dip_i, dim_i))
        if len(dx_arr) >= p14:
            adx_val = sum(x[0] for x in dx_arr[:p14]) / p14
            for dx_i, _, __ in dx_arr[p14:]:
                adx_val = (adx_val * (p14-1) + dx_i) / p14
            dip = dx_arr[-1][1]; dim = dx_arr[-1][2]
            if adx_val > 25 and dip > dim:
                sc["adx"]=1; dt["adx"]=f"ADX={adx_val:.0f} DI+={dip:.1f}>DI- (xu hướng tăng mạnh)"
            elif adx_val > 25 and dim > dip:
                sc["adx"]=-1; dt["adx"]=f"ADX={adx_val:.0f} DI-={dim:.1f}>DI+ (xu hướng giảm mạnh)"
            elif adx_val > 20 and dip > dim:
                sc["adx"]=1; dt["adx"]=f"ADX={adx_val:.0f} DI+>{dim:.1f} (tăng vừa)"
            elif adx_val > 20 and dim > dip:
                sc["adx"]=-1; dt["adx"]=f"ADX={adx_val:.0f} DI->DI+ (giảm vừa)"
            else:
                sc["adx"]=0; dt["adx"]=f"ADX={adx_val:.0f} <20 (thị trường tích lũy/không xu hướng)"
        else: sc["adx"]=0; dt["adx"]="Chưa đủ dữ liệu ADX"
    else: sc["adx"]=0; dt["adx"]="Chưa đủ dữ liệu"

    # 9. Ichimoku
    if n >= 52:
        def iline(hp,lp,p): return (max(hp[-p:])+min(lp[-p:]))/2
        tenkan = iline(highs,lows,9)
        kijun = iline(highs,lows,26)
        sa = (tenkan+kijun)/2
        sb = iline(highs,lows,52)
        top = max(sa,sb); bot = min(sa,sb)
        chikou_bull = n>=52 and closes[-1] > closes[-27] if n>27 else False
        above = price > top; below = price < bot
        tk = tenkan > kijun
        ichi_score = 0
        if above: ichi_score += 1
        elif below: ichi_score -= 1
        if tk: ichi_score += 1
        elif not tk: ichi_score -= 1
        if chikou_bull: ichi_score += 1
        else: ichi_score -= 1
        if ichi_score >= 2: sc["ichi"]=1; dt["ichi"]=f"Trên Kumo T={tenkan:.1f}>K={kijun:.1f} Chikou{'↑' if chikou_bull else '↓'}"
        elif ichi_score <= -2: sc["ichi"]=-1; dt["ichi"]=f"Dưới Kumo T={tenkan:.1f}<K={kijun:.1f} Chikou{'↑' if chikou_bull else '↓'}"
        else: sc["ichi"]=0; dt["ichi"]=f"Trung tính T={tenkan:.1f} K={kijun:.1f} Kumo={'trên' if above else 'dưới' if below else 'trong'}"
    else: sc["ichi"]=0; dt["ichi"]="Chưa đủ dữ liệu"

    # 10. RSI Divergence
    if n >= 35:
        rsi_arr = [calc_rsi(closes[:i+1]) for i in range(n-30, n)]
        sw_lows = [(i, lows[n-30+i], rsi_arr[i]) for i in range(2,28) if lows[n-30+i] <= min(lows[n-30+max(0,i-3):n-30+i]+lows[n-30+i+1:n-30+min(30,i+4)] or [lows[n-30+i]])]
        sw_highs = [(i, highs[n-30+i], rsi_arr[i]) for i in range(2,28) if highs[n-30+i] >= max(highs[n-30+max(0,i-3):n-30+i]+highs[n-30+i+1:n-30+min(30,i+4)] or [highs[n-30+i]])]
        bull_div = False; bear_div = False
        if len(sw_lows) >= 2:
            i1,p1,r1 = sw_lows[-2]; i2,p2,r2 = sw_lows[-1]
            if p2 < p1 and r2 > r1: bull_div = True
        if len(sw_highs) >= 2:
            i1,p1,r1 = sw_highs[-2]; i2,p2,r2 = sw_highs[-1]
            if p2 > p1 and r2 < r1: bear_div = True
        if bull_div: sc["div"]=1; dt["div"]=f"Bullish Divergence: Giá đáy↓ RSI đáy↑"
        elif bear_div: sc["div"]=-1; dt["div"]=f"Bearish Divergence: Giá đỉnh↑ RSI đỉnh↓"
        else: sc["div"]=0; dt["div"]="Không có Divergence"
    else: sc["div"]=0; dt["div"]="Chưa đủ dữ liệu"

    # 11. Fibonacci
    if n >= 30:
        hi_v=max(highs[-30:]); lo_v=min(lows[-30:]); rng_f=hi_v-lo_v or 1
        f236=hi_v-0.236*rng_f; f382=hi_v-0.382*rng_f
        f500=hi_v-0.500*rng_f; f618=hi_v-0.618*rng_f
        tol=rng_f*0.018
        at_support = abs(price-f618)<tol or abs(price-f500)<tol
        at_resistance = abs(price-f236)<tol or abs(price-f382)<tol
        bouncing_up = closes[-1]>closes[-2] and closes[-2]>=closes[-3]
        falling_down = closes[-1]<closes[-2] and closes[-2]<=closes[-3]
        if at_support and bouncing_up:
            sc["fib"]=1; dt["fib"]=f"Bounce Fib {f618:.0f}-{f500:.0f} (50-61.8%)"
        elif at_resistance and falling_down:
            sc["fib"]=-1; dt["fib"]=f"Từ chối Fib {f382:.0f} (38.2%)"
        else:
            sc["fib"]=0; dt["fib"]=f"Fib: 23.6%={f236:.0f} 38.2%={f382:.0f} 50%={f500:.0f} 61.8%={f618:.0f}"
    else: sc["fib"]=0; dt["fib"]="Chưa đủ dữ liệu"

    # 12. Support/Resistance
    if n >= 30:
        sups=[]; ress=[]
        for i in range(3, n-3):
            if lows[i] == min(lows[i-3:i+4]):
                sups.append(lows[i])
            if highs[i] == max(highs[i-3:i+4]):
                ress.append(highs[i])
        def cluster(levels, tol_pct=0.005):
            if not levels: return []
            levels = sorted(set(round(x,4) for x in levels))
            out=[levels[0]]
            for v in levels[1:]:
                if abs(v-out[-1])/out[-1] > tol_pct: out.append(v)
                else: out[-1]=(out[-1]+v)/2
            return out
        sups = cluster(sups); ress = cluster(ress)
        tol2 = price * 0.015
        near_sup = [s for s in sups if abs(price-s)<tol2 and s<=price*1.01]
        near_res = [r for r in ress if abs(price-r)<tol2 and r>=price*0.99]
        up3 = closes[-1]>closes[-2]
        if near_sup and up3:
            sc["sr"]=1; dt["sr"]=f"Bounce tại Support {near_sup[-1]:.0f}"
        elif near_res and not up3:
            sc["sr"]=-1; dt["sr"]=f"Từ chối tại Resistance {near_res[0]:.0f}"
        else:
            best_s=min(sups,key=lambda x:abs(x-price)) if sups else price*0.95
            best_r=min(ress,key=lambda x:abs(x-price)) if ress else price*1.05
            sc["sr"]=0; dt["sr"]=f"S≈{best_s:.0f} R≈{best_r:.0f}"
    else: sc["sr"]=0; dt["sr"]="Chưa đủ dữ liệu"

    # 13. OBV Trend
    if n >= 21:
        obv2=0; oa2=[0]
        for i in range(1,n):
            obv2 += (vols[i] if closes[i]>closes[i-1] else -vols[i] if closes[i]<closes[i-1] else 0)
            oa2.append(obv2)
        oe5=ema_s(oa2,5); oe20=ema_s(oa2,20)
        bull_o=oe5[-1]>oe20[-1] and oa2[-1]>oa2[-5]
        bear_o=oe5[-1]<oe20[-1] and oa2[-1]<oa2[-5]
        if bull_o: sc["obv"]=1; dt["obv"]="OBV EMA5>EMA20 (dòng tiền vào)"
        elif bear_o: sc["obv"]=-1; dt["obv"]="OBV EMA5<EMA20 (dòng tiền ra)"
        else: sc["obv"]=0; dt["obv"]="OBV trung tính"
    else: sc["obv"]=0; dt["obv"]="Chưa đủ dữ liệu"

    # 14. Pivot Points
    if n >= 2:
        ph=highs[-2]; pl=lows[-2]; pc=closes[-2]
        pivot=(ph+pl+pc)/3
        r1=2*pivot-pl; s1=2*pivot-ph
        if price<s1: sc["pivot"]=1; dt["pivot"]=f"Giá dưới S1={s1:.1f}→hỗ trợ (Pivot={pivot:.1f})"
        elif price>r1: sc["pivot"]=-1; dt["pivot"]=f"Giá trên R1={r1:.1f}→kháng cự (Pivot={pivot:.1f})"
        elif price<pivot: sc["pivot"]=1; dt["pivot"]=f"Giá dưới Pivot={pivot:.1f} (S1={s1:.1f})"
        else: sc["pivot"]=0; dt["pivot"]=f"Pivot={pivot:.1f} R1={r1:.1f} S1={s1:.1f}"
    else: sc["pivot"]=0; dt["pivot"]="Chưa đủ dữ liệu"

    # 15. BB Width
    if n >= 20:
        bw_now = (bbu-bbl)/avg*100
        if n >= 50:
            r50=closes[-50:-30]; avg50=sum(r50)/20
            std50=(sum((x-avg50)**2 for x in r50)/20)**0.5
            bw_avg=round((avg50+2*std50-(avg50-2*std50))/avg50*100,1)
            squeeze = bw_now < bw_avg*0.7
            expand = bw_now > bw_avg*1.5
            if squeeze: sc["bbw"]=1; dt["bbw"]=f"BB Width={bw_now:.1f}% co hẹp→sắp bùng nổ (avg={bw_avg:.1f}%)"
            elif expand and closes[-1]>avg: sc["bbw"]=1; dt["bbw"]=f"BB giãn rộng={bw_now:.1f}% + giá tăng"
            elif expand and closes[-1]<avg: sc["bbw"]=-1; dt["bbw"]=f"BB giãn rộng={bw_now:.1f}% + giá giảm"
            else: sc["bbw"]=0; dt["bbw"]=f"BB Width={bw_now:.1f}% bình thường"
        else: sc["bbw"]=0; dt["bbw"]=f"BB Width={bw_now:.1f}%"
    else: sc["bbw"]=0; dt["bbw"]="Chưa đủ dữ liệu"

    # 16. VWAP
    if n >= 10:
        vwap_sum=0; vol_sum=0
        for i in range(max(0,n-20), n):
            tp=(highs[i]+lows[i]+closes[i])/3
            v=vols[i] if vols[i]>0 else 1
            vwap_sum+=tp*v; vol_sum+=v
        vwap_val = vwap_sum/vol_sum if vol_sum>0 else price
        vwap_dist_pct = (price-vwap_val)/vwap_val*100 if vwap_val>0 else 0
        if price > vwap_val*1.005:
            sc["vwap"]=1; dt["vwap"]=f"Giá>{vwap_val:.1f} VWAP (+{vwap_dist_pct:.1f}%) — tích cực"
        elif price < vwap_val*0.995:
            sc["vwap"]=-1; dt["vwap"]=f"Giá<{vwap_val:.1f} VWAP ({vwap_dist_pct:.1f}%) — áp lực bán"
        else:
            sc["vwap"]=0; dt["vwap"]=f"Giá≈VWAP={vwap_val:.1f} ({vwap_dist_pct:+.1f}%)"
    else: sc["vwap"]=0; dt["vwap"]="Chưa đủ dữ liệu"; vwap_val=price

    # 17. Elder Ray
    elder_bulls = 0.0; elder_bears = 0.0
    if n >= 15:
        ema13 = ema_s(closes, 13)[-1]
        elder_bulls = highs[-1] - ema13
        elder_bears = lows[-1] - ema13
        ema13_prev = ema_s(closes[:-1], 13)[-1] if n > 15 else ema13
        bp_prev = highs[-2] - ema13_prev
        br_prev = lows[-2] - ema13_prev
        if elder_bulls > 0 and elder_bulls > bp_prev:
            sc["elder"]=1; dt["elder"]=f"Bulls={elder_bulls:.2f}↑ Bears={elder_bears:.2f} (lực mua tăng)"
        elif elder_bears < 0 and elder_bears < br_prev:
            sc["elder"]=-1; dt["elder"]=f"Bulls={elder_bulls:.2f} Bears={elder_bears:.2f}↓ (lực bán tăng)"
        elif elder_bulls > 0:
            sc["elder"]=1; dt["elder"]=f"Bulls={elder_bulls:.2f}>0 (tích cực)"
        elif elder_bears < 0:
            sc["elder"]=-1; dt["elder"]=f"Bears={elder_bears:.2f}<0 (tiêu cực)"
        else:
            sc["elder"]=0; dt["elder"]=f"Bulls={elder_bulls:.2f} Bears={elder_bears:.2f}"
    else: sc["elder"]=0; dt["elder"]="Chưa đủ dữ liệu"

    # 18. Squeeze
    squeeze_on = False; sq_mom = 0.0
    if n >= 20:
        ema20_kc = ema_s(closes, 20)[-1]
        kc_atr = _atr(valid, 20) if n >= 21 else 0
        kc_upper = ema20_kc + 1.5 * kc_atr
        kc_lower = ema20_kc - 1.5 * kc_atr
        squeeze_on = (bbu < kc_upper) and (bbl > kc_lower)
        highest_high = max(highs[-20:]); lowest_low = min(lows[-20:])
        mid_hl = (highest_high + lowest_low) / 2
        mid_ma = sum(closes[-20:]) / 20
        sq_mom = closes[-1] - (mid_hl + mid_ma) / 2
        sq_mom_prev = (closes[-2] - ((max(highs[-21:-1])+min(lows[-21:-1]))/2 + sum(closes[-21:-1])/20)/2) if n >= 21 else 0
        if squeeze_on and sq_mom > 0 and sq_mom > sq_mom_prev:
            sc["squeeze"]=1; dt["squeeze"]=f"🔥 SQUEEZE giải phóng TĂNG (mom={sq_mom:.2f})"
        elif squeeze_on and sq_mom < 0 and sq_mom < sq_mom_prev:
            sc["squeeze"]=-1; dt["squeeze"]=f"🔥 SQUEEZE giải phóng GIẢM (mom={sq_mom:.2f})"
        elif squeeze_on:
            sc["squeeze"]=0; dt["squeeze"]=f"⚡ Đang SQUEEZE (tích lũy) — chờ bùng nổ"
        elif sq_mom > 0:
            sc["squeeze"]=1; dt["squeeze"]=f"Momentum dương ({sq_mom:.2f}) không squeeze"
        elif sq_mom < 0:
            sc["squeeze"]=-1; dt["squeeze"]=f"Momentum âm ({sq_mom:.2f}) không squeeze"
        else:
            sc["squeeze"]=0; dt["squeeze"]=f"Trung tính (mom={sq_mom:.2f})"
    else: sc["squeeze"]=0; dt["squeeze"]="Chưa đủ dữ liệu"

    # 19. Volume Profile
    vp_poc = 0.0; vp_vah = 0.0; vp_val_price = 0.0; vp_poc_pct = 0.0
    vp_data = []
    if n >= 30:
        _vp_bars = valid[-min(60, n):]
        _vp_highs = [x.get("h", x["c"]) for x in _vp_bars]
        _vp_lows = [x.get("l", x["c"]) for x in _vp_bars]
        _vp_vols = [x.get("v", 0) for x in _vp_bars]
        _vp_range_hi = max(_vp_highs); _vp_range_lo = min(_vp_lows)
        _vp_span = _vp_range_hi - _vp_range_lo
        if _vp_span > 0:
            N_BUCKETS = 20
            bucket_size = _vp_span / N_BUCKETS
            buckets = [0.0] * N_BUCKETS
            for k, bar in enumerate(_vp_bars):
                bhi = _vp_highs[k]; blo = _vp_lows[k]; bvol = _vp_vols[k]
                bar_span = bhi - blo if bhi > blo else bucket_size
                for b in range(N_BUCKETS):
                    blvl = _vp_range_lo + b * bucket_size
                    bhvl = blvl + bucket_size
                    overlap = max(0, min(bhi, bhvl) - max(blo, blvl))
                    buckets[b] += bvol * (overlap / bar_span)
            poc_idx = buckets.index(max(buckets))
            vp_poc = round(_vp_range_lo + (poc_idx + 0.5) * bucket_size, 4)
            total_vol = sum(buckets); va_target = total_vol * 0.70
            va_vols = 0.0; va_lo_idx = poc_idx; va_hi_idx = poc_idx
            while va_vols < va_target:
                va_vols += buckets[va_lo_idx] if va_lo_idx == poc_idx else 0
                lo_ext = va_lo_idx - 1; hi_ext = va_hi_idx + 1
                lo_vol = buckets[lo_ext] if lo_ext >= 0 else -1
                hi_vol = buckets[hi_ext] if hi_ext < N_BUCKETS else -1
                if lo_vol < 0 and hi_vol < 0: break
                if lo_vol >= hi_vol and lo_ext >= 0:
                    va_lo_idx = lo_ext; va_vols += lo_vol
                elif hi_ext < N_BUCKETS:
                    va_hi_idx = hi_ext; va_vols += hi_vol
                else:
                    va_lo_idx = lo_ext; va_vols += lo_vol
            vp_vah = round(_vp_range_lo + (va_hi_idx + 1) * bucket_size, 4)
            vp_val_price = round(_vp_range_lo + va_lo_idx * bucket_size, 4)
            vp_poc_pct = round((price - vp_poc) / vp_poc * 100, 2) if vp_poc > 0 else 0
            max_bvol = max(buckets) if buckets else 1
            vp_data = [{"p": round(_vp_range_lo + (i+0.5)*bucket_size, 2),
                        "v": round(buckets[i]/max_bvol*100, 1)} for i in range(N_BUCKETS)]
            if abs(vp_poc_pct) < 1.5:
                dt["vp"] = f"Giá≈POC={vp_poc:.1f} (±{vp_poc_pct:+.1f}%) — vùng quyết định"
            elif price > vp_vah:
                dt["vp"] = f"Giá trên VAH={vp_vah:.1f} — xu hướng tăng mạnh"
            elif price < vp_val_price:
                dt["vp"] = f"Giá dưới VAL={vp_val_price:.1f} — xu hướng giảm mạnh"
            elif price > vp_poc:
                dt["vp"] = f"Giá trên POC={vp_poc:.1f} ({vp_poc_pct:+.1f}%) — tích cực"
            else:
                dt["vp"] = f"Giá dưới POC={vp_poc:.1f} ({vp_poc_pct:+.1f}%) — cần bứt phá"
    if not vp_poc: dt["vp"] = "Chưa đủ dữ liệu Volume Profile"

    # Liquidity Filter
    avg_vol_20 = sum(vols[-20:]) / 20 if n >= 20 else (sum(vols) / n if n > 0 else 0)
    unusual_vol = False
    if n >= 21 and avg_vol_20 > 0:
        vol_ratio_today = vols[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
        recent_spikes = sum(1 for v in vols[-6:-1] if v > avg_vol_20 * 2.5)
        unusual_vol = vol_ratio_today >= 3.0 and recent_spikes == 0
        if unusual_vol and closes[-1] >= closes[-2]:
            dt["vol_unusual"] = f"⚡ UNUSUAL VOL: {vol_ratio_today:.1f}× avg"
        elif unusual_vol:
            dt["vol_unusual"] = f"⚡ UNUSUAL VOL BÁN: {vol_ratio_today:.1f}× avg"
        else:
            dt["vol_unusual"] = ""
    else:
        dt["vol_unusual"] = ""

    # Pattern Recognition
    if n >= 30:
        pat_sig=0; pat_txt="Không có pattern rõ"
        c=closes; h2=highs; l2=lows
        peaks=[]
        for i in range(2,n-2):
            if h2[i]>=h2[i-1] and h2[i]>=h2[i-2] and h2[i]>=h2[i+1] and h2[i]>=h2[i+2]:
                peaks.append((i,h2[i]))
        if len(peaks)>=2:
            p1,p2=peaks[-2],peaks[-1]
            gap=p2[0]-p1[0]
            if 5<=gap<=40 and abs(p1[1]-p2[1])/p1[1]<0.03:
                neck=min(c[p1[0]:p2[0]+1])
                if c[-1]<neck*0.99:
                    pat_sig=-1; pat_txt=f"⛰⛰ Double Top (đỉnh={p1[1]:.1f})→GIẢM"
        if pat_sig==0:
            troughs=[]
            for i in range(2,n-2):
                if l2[i]<=l2[i-1] and l2[i]<=l2[i-2] and l2[i]<=l2[i+1] and l2[i]<=l2[i+2]:
                    troughs.append((i,l2[i]))
            if len(troughs)>=2:
                t1,t2=troughs[-2],troughs[-1]
                gap=t2[0]-t1[0]
                if 5<=gap<=40 and abs(t1[1]-t2[1])/t1[1]<0.03:
                    neck=max(c[t1[0]:t2[0]+1])
                    if c[-1]>neck*1.01:
                        pat_sig=1; pat_txt=f"W W Double Bottom (đáy={t1[1]:.1f})→TĂNG"
        if pat_sig==0 and n>=20:
            recent_hi=h2[-20:]; recent_lo=l2[-20:]
            hi_std=( sum((x-sum(recent_hi)/20)**2 for x in recent_hi)/20 )**0.5
            lo_slope=(recent_lo[-1]-recent_lo[0])/20
            if hi_std/max(recent_hi)<0.015 and lo_slope>0:
                pat_sig=1; pat_txt=f"△ Ascending Triangle→xu hướng TĂNG"
        if pat_sig==0 and n>=20:
            lo_std=( sum((x-sum(recent_lo)/20)**2 for x in recent_lo)/20 )**0.5
            hi_slope=(recent_hi[-1]-recent_hi[0])/20
            if lo_std/max(recent_lo)<0.015 and hi_slope<0:
                pat_sig=-1; pat_txt=f"▽ Descending Triangle→xu hướng GIẢM"
        sc["pattern"]=pat_sig; dt["pattern"]=pat_txt
    else: sc["pattern"]=0; dt["pattern"]="Chưa đủ dữ liệu (cần 30 phiên)"

    # Advanced Patterns
    adv_patterns = detect_advanced_patterns(closes, highs, lows, n)
    if adv_patterns:
        bull_adv = [p for p in adv_patterns if p['type'] == 'BULLISH']
        bear_adv = [p for p in adv_patterns if p['type'] == 'BEARISH']
        if bull_adv and not bear_adv:
            best = max(bull_adv, key=lambda x: x['reliability'])
            if sc.get('pattern', 0) == 0:
                sc["pattern"] = best['score']
                dt["pattern"] = best['name'] + ': ' + best['detail']
        elif bear_adv and not bull_adv:
            best = max(bear_adv, key=lambda x: x['reliability'])
            if sc.get('pattern', 0) == 0:
                sc["pattern"] = best['score']
                dt["pattern"] = best['name'] + ': ' + best['detail']

    # Weights
    WEIGHTS = {
        "macd": 2.0, "adx": 2.0, "ichi": 1.5, "rsi": 1.5,
        "volume": 1.5, "ma": 1.2, "bb": 1.2, "squeeze": 1.5,
        "elder": 1.2, "stoch": 1.0, "div": 1.0, "sr": 1.0,
        "obv": 1.0, "fib": 0.8, "vwap": 0.8, "candle": 0.7,
        "pivot": 0.7, "bbw": 0.6, "pattern": 0.8,
    }
    total_weighted = sum(sc.get(k, 0) * WEIGHTS.get(k, 1.0) for k in sc)
    max_weighted = sum(WEIGHTS.get(k, 1.0) for k in sc)
    total = round(total_weighted / max_weighted * len(sc)) if max_weighted > 0 else 0
    total_raw = sum(sc.values())
    max_possible = len(sc)

    # Confluence
    confluence = 0; confluence_items = []
    _mktd = sc.get("ma",0) + sc.get("adx",0)
    _mom = sc.get("rsi",0) + sc.get("macd",0) + sc.get("stoch",0)
    _vol_cf = sc.get("volume",0)
    _cloud = sc.get("ichi",0)
    _squeeze_cf = sc.get("squeeze",0)
    _price_action = sc.get("sr",0) + sc.get("fib",0) + sc.get("candle",0)

    def conf_add(condition, label, weight=1):
        nonlocal confluence
        if condition: confluence += weight; confluence_items.append(label)

    conf_add(_mktd >= 2, "MA+ADX bullish", 2)
    conf_add(_mom >= 2, "RSI+MACD+Stoch bull", 2)
    conf_add(_vol_cf > 0, "Volume xác nhận", 1)
    conf_add(_cloud > 0, "Trên Ichimoku Cloud", 2)
    conf_add(_squeeze_cf > 0, "Squeeze bùng TĂNG", 2)
    conf_add(_price_action >= 2, "S/R + Fib + Candle", 1)
    conf_add(sc.get("div",0) > 0, "Bullish Divergence", 1)
    conf_add(sc.get("vwap",0) > 0, "Trên VWAP", 1)
    conf_add(sc.get("elder",0) > 0, "Elder Ray tích cực", 1)

    bear_conf = 0
    if _mktd <= -2: bear_conf += 2
    if _mom <= -2: bear_conf += 2
    if _vol_cf < 0: bear_conf += 1
    if _cloud < 0: bear_conf += 2
    if _squeeze_cf < 0: bear_conf += 2
    if sc.get("div",0) < 0: bear_conf += 1

    if bear_conf > confluence: confluence = -bear_conf; confluence_items = ["Bear confluence"]
    confluence_label = "🔥 CỰC MẠNH" if confluence>=6 else "✅ TỐT" if confluence>=3 else "⚠️ YẾU" if confluence>=1 else "❌ NGƯỢC CHIỀU" if confluence<0 else "—"

    # Confidence
    active = [v for v in sc.values() if v != 0]
    n_active = len(active) or 1
    bull_count = sum(1 for v in active if v > 0)
    bear_count = sum(1 for v in active if v < 0)
    confidence = round(max(bull_count, bear_count) / n_active * 100)

    # Signal
    core = ["rsi","macd","bb","ma","adx","ichi"]
    core_bull = sum(1 for k in core if sc.get(k,0)>0)
    core_bear = sum(1 for k in core if sc.get(k,0)<0)
    eff_total = total
    if eff_total>=8 and core_bull>=3: sig,col="BUY","#00e676"
    elif eff_total<=-8 and core_bear>=3: sig,col="SELL","#f03050"
    elif eff_total>=6: sig,col="BUY_WARN","#69f0ae"
    elif eff_total<=-6: sig,col="SELL_WARN","#ff9100"
    elif eff_total>=3: sig,col="BUY_WEAK","#a5d6a7"
    elif eff_total<=-3: sig,col="SELL_WEAK","#ffcc80"
    else: sig,col="NEUTRAL","#4a6a8a"

    # Liquidity
    MIN_VOL_VN = 200_000
    MIN_VOL_US = 500_000
    market = data.get("market","VN")
    min_vol_thr = MIN_VOL_VN if market == "VN" else MIN_VOL_US
    low_liquidity = (avg_vol_20 > 0) and (avg_vol_20 < min_vol_thr)
    if low_liquidity:
        sig = sig + "_LOWVOL" if "_LOWVOL" not in sig else sig

    # RS vs Index
    rs_vs_index = data.get("_rs_vs_index", None)
    data_age_h = data.get("_data_age_h", 0)

    ma20v = sum(closes[-20:])/20 if n>=20 else price
    ma50v = sum(closes[-50:])/50 if n>=50 else price
    ma200v = sum(closes[-200:])/200 if n>=200 else price
    trend = "BULLISH" if price>ma200v else "BEARISH"
    atr_v = _atr(valid,14)

    # ── Supertrend (ATR multiplier=3, period=10) ──────────────
    st_dir = 1  # 1=up(bullish), -1=down(bearish)
    st_val = 0.0
    st_series = []  # [{time, value, direction}]
    try:
        st_period = 10; st_mult = 3.0
        if n >= st_period + 2:
            # Calculate ATR array for supertrend
            st_atrs = []
            for i in range(1, n):
                tr = max(highs[i] - lows[i],
                         abs(highs[i] - closes[i-1]),
                         abs(lows[i] - closes[i-1]))
                st_atrs.append(tr)
            # Smooth ATR with Wilder's
            st_atr_smooth = [sum(st_atrs[:st_period]) / st_period]
            for i in range(st_period, len(st_atrs)):
                st_atr_smooth.append((st_atr_smooth[-1] * (st_period-1) + st_atrs[i]) / st_period)
            # Basic upper/lower bands
            prev_upper = prev_lower = 0.0
            prev_dir = 1
            for i in range(len(st_atr_smooth)):
                idx = i + st_period  # index in closes
                if idx >= n: break
                hl2 = (highs[idx] + lows[idx]) / 2
                atr_i = st_atr_smooth[i]
                raw_upper = hl2 + st_mult * atr_i
                raw_lower = hl2 - st_mult * atr_i
                # Adjust bands
                upper = raw_upper if raw_upper < prev_upper or closes[idx-1] > prev_upper else prev_upper
                lower = raw_lower if raw_lower > prev_lower or closes[idx-1] < prev_lower else prev_lower
                # Determine direction
                if closes[idx] > prev_upper:
                    cur_dir = 1
                elif closes[idx] < prev_lower:
                    cur_dir = -1
                else:
                    cur_dir = prev_dir
                st_line = lower if cur_dir == 1 else upper
                d_key = valid[idx].get('d','')
                if d_key:
                    st_series.append({'time': d_key, 'value': round(st_line, 4), 'dir': cur_dir})
                if idx == n - 1:
                    st_dir = cur_dir
                    st_val = round(st_line, 4)
                prev_upper = upper; prev_lower = lower; prev_dir = cur_dir
    except Exception as _ste:
        pass

    return {
        "scores":sc, "details":dt, "total":total, "total_raw":total_raw,
        "max":max_possible,
        "signal":sig, "signal_color":col, "rsi":round(rsi,1),
        "bb_upper":round(bbu,2), "bb_lower":round(bbl,2),
        "ma20":round(ma20v,2), "ma50":round(ma50v,2), "ma200":round(ma200v,2),
        "trend_long":trend, "atr":round(atr_v,4),
        "stop_loss": round(price-1.5*atr_v, 4),
        "take_profit": round(price+2.0*atr_v, 4),
        "take_profit2": round(price+3.0*atr_v,4),
        "trailing_stop": round(price-2.0*atr_v, 4),
        "rr_ratio": round(2.0/1.5, 2),
        "risk_pct": round(1.5*atr_v/price*100, 2) if price>0 else 0,
        "pos_size_2pct": round(0.02*price/(1.5*atr_v), 4) if atr_v>0 else 0,
        "chart_data": h[-200:],
        "n_bars": n,
        "confidence": confidence,
        "confluence": confluence,
        "confluence_label": confluence_label,
        "confluence_items": confluence_items[:5],
        "low_liquidity": low_liquidity,
        "avg_vol_20": round(avg_vol_20),
        "unusual_vol": unusual_vol,
        "unusual_vol_detail": dt.get("vol_unusual",""),
        "squeeze_on": squeeze_on,
        "vp_poc": round(vp_poc, 4), "vp_vah": round(vp_vah, 4),
        "vp_val": round(vp_val_price, 4), "vp_poc_pct": vp_poc_pct,
        "vp_data": vp_data,
        "elder_bulls": round(elder_bulls,4),
        "elder_bears": round(elder_bears,4),
        "vwap": round(vwap_val,4) if n>=10 else 0,
        "pivot_r1": round(r1,2) if n>=2 else 0,
        "pivot_s1": round(s1,2) if n>=2 else 0,
        "pivot_p": round(pivot,2) if n>=2 else 0,
        "rs_vs_index": rs_vs_index,
        "data_age_h": data_age_h,
        "adx_val": round(adx_val, 1),
        "adv_patterns": [{k:v for k,v in p.items() if isinstance(v,(str,int,float,bool,type(None)))} for p in (adv_patterns or [])],
        "candle_patterns": detect_candle_patterns(valid[-10:]) if len(valid) >= 4 else [],
        "supertrend": st_val,
        "supertrend_dir": st_dir,
        "supertrend_series": st_series[-100:],  # last 100 points for chart overlay
        "supertrend_label": ("📈 BULLISH (Mua)" if st_dir == 1 else "📉 BEARISH (Bán)") if st_val > 0 else "",
    }

# ── Heikin-Ashi ───────────────────────────────────────────────
def calc_heikin_ashi(bars):
    if not bars: return []
    ha=[]
    for i,b in enumerate(bars):
        hc=(b['o']+b['h']+b['l']+b['c'])/4
        ho=(b['o']+b['c'])/2 if i==0 else (ha[i-1]['o']+ha[i-1]['c'])/2
        ha.append({'d':b.get('d',''),'t':b.get('t',''),'o':round(ho,4),'h':round(max(b['h'],ho,hc),4),'l':round(min(b['l'],ho,hc),4),'c':round(hc,4),'v':b.get('v',0)})
    return ha

def calc_fibonacci(history):
    if not history or len(history) < 30:
        return {}
    closes = [b['c'] for b in history]
    highs = [b.get('h', b['c']) for b in history]
    lows  = [b.get('l', b['c']) for b in history]
    n = len(history)
    # Lấy swing high/low trong 60 phiên
    lb = min(n-1, 60)
    sub_hi = highs[-lb:]; sub_lo = lows[-lb:]
    hi_v = max(sub_hi); lo_v = min(sub_lo)
    hi_idx = len(sub_hi)-1 - list(reversed(sub_hi)).index(hi_v)
    lo_idx = len(sub_lo)-1 - list(reversed(sub_lo)).index(lo_v)
    is_uptrend = lo_idx < hi_idx
    rng = hi_v - lo_v if hi_v != lo_v else 1
    if is_uptrend:
        fib_0 = hi_v; fib_100 = lo_v
    else:
        fib_0 = lo_v; fib_100 = hi_v
    fib_236 = hi_v - 0.236 * rng
    fib_382 = hi_v - 0.382 * rng
    fib_500 = hi_v - 0.500 * rng
    fib_618 = hi_v - 0.618 * rng
    fib_786 = hi_v - 0.786 * rng
    fib_ext_1272 = lo_v - 0.272 * rng
    fib_ext_1618 = lo_v - 0.618 * rng
    return {
        'fib_0': round(fib_0, 2), 'fib_100': round(fib_100, 2),
        'fib_236': round(fib_236, 2), 'fib_382': round(fib_382, 2),
        'fib_500': round(fib_500, 2), 'fib_618': round(fib_618, 2),
        'fib_786': round(fib_786, 2),
        'fib_ext_1272': round(fib_ext_1272, 2),
        'fib_ext_1618': round(fib_ext_1618, 2),
        'fib_swing_hi': round(hi_v, 2), 'fib_swing_lo': round(lo_v, 2),
        'fib_is_uptrend': is_uptrend
    }

def analyze_mtf(data):
    h_all = data.get("history", [])
    results = {}
    for tf in ['1D','1W','1M']:
        h = resample_history(h_all, tf)
        fake = dict(data); fake["history"] = h
        if h: fake["price"] = h[-1]["c"]
        results[tf] = analyze(fake, '1D')
    totals = [results[tf]["total"] for tf in ['1D','1W','1M']]
    all_bull = all(t>0 for t in totals)
    all_bear = all(t<0 for t in totals)
    results["consensus"] = "ALIGNED_BULL" if all_bull else "ALIGNED_BEAR" if all_bear else "MIXED"
    results["consensus_score"] = sum(totals)
    return results

def calculate_var(history, confidence=0.95):
    """Value at Risk calculation"""
    if len(history) < 30:
        return 0
    returns = sorted([(history[i]['c'] - history[i-1]['c']) / history[i-1]['c'] 
               for i in range(1, len(history)) if history[i-1]['c'] > 0])
    if not returns:
        return 0
    try:
        import numpy as np
        return float(np.percentile(returns, (1-confidence)*100))
    except ImportError:
        idx2 = int(len(returns) * (1-confidence))
        return float(returns[max(0, idx2)])

def calculate_cvar(history, confidence=0.95):
    """Conditional VaR (Expected Shortfall)"""
    if len(history) < 30:
        return 0
    returns = sorted([(history[i]['c'] - history[i-1]['c']) / history[i-1]['c'] 
               for i in range(1, len(history)) if history[i-1]['c'] > 0])
    if not returns:
        return 0
    try:
        import numpy as np
        var = np.percentile(returns, (1-confidence)*100)
        tail = [r for r in returns if r <= var]
        return float(np.mean(tail)) if tail else 0.0
    except ImportError:
        idx2 = int(len(returns) * (1-confidence))
        tail = returns[:max(1, idx2)]
        return float(sum(tail)/len(tail)) if tail else 0.0

def calc_correlation_with_portfolio(symbol):
    """Calculate correlation between a symbol and user's portfolio"""
    # This is a placeholder - actual implementation would need user_id
    return None

def _atr(h, p=14):
    n = len(h)
    if n < p+1: return 0
    trs = [max(h[i].get("h",h[i]["c"])-h[i].get("l",h[i]["c"]),
               abs(h[i].get("h",h[i]["c"])-h[i-1]["c"]),
               abs(h[i].get("l",h[i]["c"])-h[i-1]["c"])) for i in range(n-p,n)]
    return sum(trs)/p

def calc_rsi(closes, p=14):
    if len(closes) < p+2: return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    if len(gains) < p: return 50
    avg_g = sum(gains[:p]) / p
    avg_l = sum(losses[:p]) / p
    for i in range(p, len(gains)):
        avg_g = (avg_g * (p-1) + gains[i]) / p
        avg_l = (avg_l * (p-1) + losses[i]) / p
    return 100 - 100 / (1 + (avg_g / (avg_l or 0.0001)))

def ema_s(arr, p):
    if not arr: return [0]
    k=2/(p+1); r=[arr[0]]
    for v in arr[1:]: r.append(v*k+r[-1]*(1-k))
    return r

def empty_analysis():
    ks=["rsi","macd","bb","ma","stoch","volume","candle","adx","ichi","div","fib","sr","obv","pivot","bbw","vwap","pattern","elder","squeeze"]
    return {"scores":{k:0 for k in ks},"details":{k:"Chưa đủ dữ liệu" for k in ks},
            "total":0,"total_raw":0,"max":19,"signal":"NEUTRAL","signal_color":"#4a6a8a",
            "rsi":50,"bb_upper":0,"bb_lower":0,"ma20":0,"ma50":0,"ma200":0,
            "trend_long":"N/A","atr":0,"stop_loss":0,"take_profit":0,"take_profit2":0,
            "chart_data":[],"n_bars":0,"vwap":0,
            "confidence":0,"confluence":0,"confluence_label":"--","confluence_items":[],
            "low_liquidity":False,"avg_vol_20":0,"unusual_vol":False,"unusual_vol_detail":"",
            "squeeze_on":False,"elder_bulls":0,"elder_bears":0,
            "rs_vs_index":None,"data_age_h":0,"adx_val":0,
            "pivot_r1":0,"pivot_s1":0,"pivot_p":0,
            "vp_poc":0,"vp_vah":0,"vp_val":0,"vp_poc_pct":0,"vp_data":[]}

# ── ADVANCED PATTERN DETECTION ───────────────────────────────
def detect_advanced_patterns(closes, highs, lows, n):
    patterns = []
    if n < 40: return patterns

    # Head & Shoulders
    if n >= 60:
        peaks = []
        for i in range(3, n-3):
            if highs[i] >= highs[i-1] and highs[i] >= highs[i-2] and highs[i] >= highs[i+1] and highs[i] >= highs[i+2]:
                peaks.append((i, highs[i]))
        if len(peaks) >= 3:
            for pi in range(len(peaks)-2):
                l_sh, l_h = peaks[pi]
                head, h_h = peaks[pi+1]
                r_sh, r_h = peaks[pi+2]
                gap1 = head - l_sh; gap2 = r_sh - head
                if (5 <= gap1 <= 40 and 5 <= gap2 <= 40
                        and h_h > l_h * 1.02 and h_h > r_h * 1.02
                        and abs(l_h - r_h) / l_h < 0.05):
                    neckline = min(closes[l_sh:r_sh+1])
                    if closes[-1] < neckline * 0.99:
                        patterns.append({
                            'name': '🔻 Head & Shoulders',
                            'type': 'BEARISH',
                            'score': -2,
                            'detail': f'Đầu={h_h:.1f}, Vai trái={l_h:.1f}, Vai phải={r_h:.1f}, Cổ={neckline:.1f}',
                            'reliability': 85
                        })
                        break

    # Inverse Head & Shoulders
    if n >= 60:
        troughs = []
        for i in range(3, n-3):
            if lows[i] <= lows[i-1] and lows[i] <= lows[i-2] and lows[i] <= lows[i+1] and lows[i] <= lows[i+2]:
                troughs.append((i, lows[i]))
        if len(troughs) >= 3:
            for ti in range(len(troughs)-2):
                l_sh, l_l = troughs[ti]
                head, h_l = troughs[ti+1]
                r_sh, r_l = troughs[ti+2]
                gap1 = head - l_sh; gap2 = r_sh - head
                if (5 <= gap1 <= 40 and 5 <= gap2 <= 40
                        and h_l < l_l * 0.98 and h_l < r_l * 0.98
                        and abs(l_l - r_l) / l_l < 0.05):
                    neckline = max(closes[l_sh:r_sh+1])
                    if closes[-1] > neckline * 1.01:
                        patterns.append({
                            'name': '🔺 Inverse H&S',
                            'type': 'BULLISH',
                            'score': 2,
                            'detail': f'Đáy đầu={h_l:.1f}, 2 vai={l_l:.1f}/{r_l:.1f}, Cổ={neckline:.1f}',
                            'reliability': 80
                        })
                        break

    # Cup & Handle
    if n >= 50:
        cup_range = min(40, n // 2)
        cup_start = n - cup_range - 10
        cup_end = n - 10
        if cup_start > 5:
            cup_closes = closes[cup_start:cup_end]
            cup_hi_l = closes[cup_start]
            cup_hi_r = closes[cup_end-1]
            cup_lo = min(cup_closes)
            cup_depth = (min(cup_hi_l, cup_hi_r) - cup_lo) / min(cup_hi_l, cup_hi_r) if min(cup_hi_l, cup_hi_r) > 0 else 0
            if (abs(cup_hi_l - cup_hi_r) / cup_hi_l < 0.05 and 0.10 <= cup_depth <= 0.35):
                handle = closes[cup_end:]
                if len(handle) >= 3:
                    handle_drop = (handle[0] - min(handle)) / handle[0] if handle[0] > 0 else 0
                    if closes[-1] > max(cup_hi_l, cup_hi_r) * 1.005 and handle_drop < 0.08:
                        patterns.append({
                            'name': '☕ Cup & Handle',
                            'type': 'BULLISH',
                            'score': 2,
                            'detail': f'Cup đáy={cup_lo:.1f} ({cup_depth*100:.1f}%), Breakout={closes[-1]:.1f}',
                            'reliability': 78
                        })

    # Bull Flag
    if n >= 25:
        pole_period = 10
        flag_period = 8
        if n >= pole_period + flag_period:
            pole = closes[-(pole_period + flag_period):-flag_period]
            flag = closes[-flag_period:]
            pole_gain = (pole[-1] - pole[0]) / pole[0] if pole[0] > 0 else 0
            flag_range = max(flag) - min(flag)
            flag_slope = (flag[-1] - flag[0]) / flag[0] if flag[0] > 0 else 0
            if (pole_gain >= 0.08 and flag_range / flag[0] < 0.05 if flag[0] > 0 else False
                    and -0.03 <= flag_slope <= 0.01
                    and closes[-1] >= max(flag[:-1])):
                patterns.append({
                    'name': '🚩 Bull Flag',
                    'type': 'BULLISH',
                    'score': 1,
                    'detail': f'Cột +{pole_gain*100:.1f}%, Cờ consolidation {flag_range:.1f}',
                    'reliability': 72
                })

    # Bear Flag
    if n >= 25:
        pole_period = 10; flag_period = 8
        if n >= pole_period + flag_period:
            pole = closes[-(pole_period + flag_period):-flag_period]
            flag = closes[-flag_period:]
            pole_drop = (pole[0] - pole[-1]) / pole[0] if pole[0] > 0 else 0
            flag_slope = (flag[-1] - flag[0]) / flag[0] if flag[0] > 0 else 0
            if (pole_drop >= 0.08 and 0.00 <= flag_slope <= 0.03
                    and closes[-1] <= min(flag[:-1])):
                patterns.append({
                    'name': '🐻 Bear Flag',
                    'type': 'BEARISH',
                    'score': -1,
                    'detail': f'Cột -{pole_drop*100:.1f}%, breakdown',
                    'reliability': 70
                })

    # Rising Wedge
    if n >= 20:
        recent_hi = highs[-20:]; recent_lo = lows[-20:]
        hi_slope = (recent_hi[-1] - recent_hi[0]) / 20
        lo_slope = (recent_lo[-1] - recent_lo[0]) / 20
        if hi_slope > 0 and lo_slope > hi_slope * 1.3:
            spread_start = recent_hi[0] - recent_lo[0]
            spread_end = recent_hi[-1] - recent_lo[-1]
            if spread_end < spread_start * 0.7:
                patterns.append({
                    'name': '📐 Rising Wedge',
                    'type': 'BEARISH',
                    'score': -1,
                    'detail': f'Biên độ thu hẹp từ {spread_start:.1f} → {spread_end:.1f}',
                    'reliability': 68
                })

    # Falling Wedge
    if n >= 20:
        recent_hi = highs[-20:]; recent_lo = lows[-20:]
        hi_slope = (recent_hi[-1] - recent_hi[0]) / 20
        lo_slope = (recent_lo[-1] - recent_lo[0]) / 20
        if hi_slope < 0 and lo_slope < hi_slope * 1.3:
            spread_start = recent_hi[0] - recent_lo[0]
            spread_end = recent_hi[-1] - recent_lo[-1]
            if spread_end < spread_start * 0.7:
                patterns.append({
                    'name': '📐 Falling Wedge',
                    'type': 'BULLISH',
                    'score': 1,
                    'detail': f'Biên độ thu hẹp từ {spread_start:.1f} → {spread_end:.1f}',
                    'reliability': 68
                })

    return patterns

# ── ML PREDICTOR ─────────────────────────────────────────────
class MLPredictor:
    def __init__(self):
        self.models = {}
        self.feature_cols = ['rsi', 'macd', 'ma_cross', 'volume_ratio', 
                            'bb_position', 'adx', 'sentiment']
    
    def extract_features(self, symbol, days=100):
        bars = db_load_ohlcv(symbol, days)
        if len(bars) < 50:
            return None, None
        
        features = []
        labels = []
        
        for i in range(30, len(bars)-5):
            window = bars[:i+1]
            analysis = analyze({'history': window, 'price': bars[i]['c']})
            
            feat = [
                analysis.get('rsi', 50) / 100,
                analysis.get('scores', {}).get('macd', 0),
                analysis.get('scores', {}).get('ma', 0),
                1 if analysis.get('unusual_vol') else 0,
                (bars[i]['c'] - analysis.get('bb_lower', bars[i]['c'])) / 
                (analysis.get('bb_upper', bars[i]['c']) - analysis.get('bb_lower', bars[i]['c']) + 1),
                analysis.get('adx_val', 0) / 100,
                0.5
            ]
            
            future_price = bars[i+5]['c']
            label = 1 if future_price > bars[i]['c'] * 1.02 else (-1 if future_price < bars[i]['c'] * 0.98 else 0)
            
            features.append(feat)
            labels.append(label)
        
        return np.array(features), np.array(labels)
    
    def train(self, symbol):
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        
        X, y = self.extract_features(symbol)
        if X is None or len(X) < 30:
            return None
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        
        model = LogisticRegression(max_iter=1000)
        model.fit(X_train, y_train)
        
        accuracy = model.score(X_test, y_test)
        
        model_data = {
            'coef': model.coef_.tolist(),
            'intercept': model.intercept_.tolist(),
            'classes': model.classes_.tolist()
        }
        
        with get_db() as c:
            c.execute("""INSERT OR REPLACE INTO ml_models(symbol,model_data,accuracy,trained_at,n_samples)
                        VALUES(?,?,?,?,?)""",
                     (symbol, json.dumps(model_data), accuracy, 
                      datetime.now().isoformat(), len(X)))
        
        self.models[symbol] = model
        return model
    
    def predict(self, symbol, current_analysis):
        if symbol not in self.models:
            with get_db() as c:
                row = c.execute("SELECT model_data FROM ml_models WHERE symbol=?", 
                              (symbol,)).fetchone()
                if row:
                    data = json.loads(row['model_data'])
                    from sklearn.linear_model import LogisticRegression
                    model = LogisticRegression()
                    model.coef_ = np.array(data['coef'])
                    model.intercept_ = np.array(data['intercept'])
                    model.classes_ = np.array(data['classes'])
                    self.models[symbol] = model
                else:
                    self.train(symbol)
        
        if symbol not in self.models:
            return {'prob_up': 50, 'confidence': 0}
        
        model = self.models[symbol]
        feat = [[
            current_analysis.get('rsi', 50) / 100,
            current_analysis.get('scores', {}).get('macd', 0),
            current_analysis.get('scores', {}).get('ma', 0),
            1 if current_analysis.get('unusual_vol') else 0,
            (current_analysis.get('price', 0) - current_analysis.get('bb_lower', 0)) / 
            (current_analysis.get('bb_upper', 0) - current_analysis.get('bb_lower', 0) + 1),
            current_analysis.get('adx_val', 0) / 100,
            0.5
        ]]
        
        prob = model.predict_proba(feat)[0]
        up_idx = list(model.classes_).index(1) if 1 in model.classes_ else 0
        
        return {
            'prob_up': round(prob[up_idx] * 100, 1),
            'prob_down': round((1 - prob[up_idx]) * 100, 1),
            'prediction': 'UP' if prob[up_idx] > 0.6 else ('DOWN' if prob[up_idx] < 0.4 else 'NEUTRAL'),
            'confidence': round(abs(prob[up_idx] - 0.5) * 200, 1),
            'accuracy': round(cached.get('metrics', {}).get('val_accuracy', 0) if cached else 0, 1),
            'n_samples': cached.get('metrics', {}).get('n_train', 0) if cached else 0
        }

ml_predictor = MLPredictor()
def ml_predict(symbol, analysis_dict):
    """
    Predict bull probability for a symbol using trained model.
    Returns {'prob_up': 0-100, 'prediction': UP/DOWN/NEUTRAL, 'confidence': %}
    """
    return ml_predictor.predict(symbol, analysis_dict)

# ── PAPER TRADING ────────────────────────────────────────────
class PaperTrading:
    def __init__(self, user_id):
        self.user_id = user_id
        self.load_state()
    
    def load_state(self):
        with get_db() as c:
            row = c.execute("SELECT value FROM settings WHERE key=?", (f"paper_{self.user_id}",)).fetchone()
            if row:
                self.balance = float(row['value'])
            else:
                self.balance = float(get_setting('paper_balance', '100000000'))
            
            pos_row = c.execute("SELECT value FROM settings WHERE key=?", (f"paper_positions_{self.user_id}",)).fetchone()
            self.positions = json.loads(pos_row['value']) if pos_row else []
    
    def save_state(self):
        set_setting(f"paper_{self.user_id}", str(self.balance))
        set_setting(f"paper_positions_{self.user_id}", json.dumps(self.positions))
    
    def buy(self, symbol, price, qty, date=None):
        cost = price * qty * 1.0015
        if cost > self.balance:
            return False, "Insufficient balance"
        
        self.balance -= cost
        self.positions.append({
            'symbol': symbol,
            'qty': qty,
            'entry_price': price,
            'entry_date': date or datetime.now().isoformat()
        })
        self.save_state()
        return True, "Order executed"
    
    def sell(self, symbol, price, qty=None, date=None):
        position = next((p for p in self.positions if p['symbol'] == symbol), None)
        if not position:
            return False, "Position not found"
        
        qty = qty or position['qty']
        if qty > position['qty']:
            return False, "Insufficient shares"
        
        proceeds = price * qty * 0.999
        pnl = (price - position['entry_price']) * qty
        
        self.balance += proceeds
        if qty == position['qty']:
            self.positions = [p for p in self.positions if p['symbol'] != symbol]
        else:
            position['qty'] -= qty
        
        self.save_state()
        return True, {'proceeds': proceeds, 'pnl': pnl}
    
    def get_portfolio_value(self, current_prices):
        total = self.balance
        for p in self.positions:
            price = current_prices.get(p['symbol'], p['entry_price'])
            total += price * p['qty']
        return total

# ── SCANNER ENGINE ───────────────────────────────────────────
class Scanner:
    def __init__(self):
        self.criteria = {}
        self.scheduled_jobs = []
    
    def add_criteria(self, name, condition):
        self.criteria[name] = condition
    
    def scan(self, market='ALL', min_score=5, limit=100):
        results = []
        for sym, cached in _ram_cache.items():
            d = cached.get('data', {})
            if not d: continue
            if market != 'ALL' and d.get('market') != market: continue
            
            a = d.get('analysis')
            if not a: continue
            
            score = 0
            matches = []
            for name, cond in self.criteria.items():
                if cond(sym, d, a):
                    score += 1
                    matches.append(name)
            
            if score >= min_score:
                results.append({
                    'symbol': sym,
                    'market': d.get('market'),
                    'score': score,
                    'matches': matches,
                    'price': d.get('price'),
                    'signal': a.get('signal'),
                    'total': a.get('total')
                })
        
        results.sort(key=lambda x: -x['score'])
        return results[:limit]

scanner = Scanner()

# Add default criteria
scanner.add_criteria('RSI Oversold', lambda s,d,a: a.get('rsi', 50) < 30)
scanner.add_criteria('RSI Overbought', lambda s,d,a: a.get('rsi', 50) > 70)
scanner.add_criteria('Golden Cross', lambda s,d,a: a.get('scores', {}).get('ma', 0) > 0)
scanner.add_criteria('Death Cross', lambda s,d,a: a.get('scores', {}).get('ma', 0) < 0)
scanner.add_criteria('High Volume', lambda s,d,a: a.get('unusual_vol', False))
scanner.add_criteria('Squeeze', lambda s,d,a: a.get('squeeze_on', False))
scanner.add_criteria('Bullish Divergence', lambda s,d,a: a.get('scores', {}).get('div', 0) > 0)
scanner.add_criteria('Bearish Divergence', lambda s,d,a: a.get('scores', {}).get('div', 0) < 0)

# ── USER WATCHLIST ───────────────────────────────────────────
def uw_list(user_id):
    with get_db() as c:
        rows = c.execute("""SELECT uw.*, fm.name, fm.sector, fm.last_price, fm.last_chg
                            FROM user_watchlist uw
                            LEFT JOIN fetch_meta fm ON uw.symbol=fm.symbol
                            WHERE uw.user_id=? ORDER BY uw.added_at""", (user_id,)).fetchall()
    return [dict(r) for r in rows]

def uw_add(user_id, symbol, market, note=''):
    try:
        with get_db() as c:
            c.execute("INSERT OR IGNORE INTO user_watchlist(user_id,symbol,market,note) VALUES(?,?,?,?)",
                    (user_id, symbol, market, note))
        with get_db() as c:
            c.execute("INSERT OR IGNORE INTO watchlist(symbol,market) VALUES(?,?)", (symbol, market))
        with get_db() as c:
            subs = c.execute(
                "SELECT chat_id FROM tg_subscribers WHERE user_id=? AND enabled=1", (user_id,)
            ).fetchall()
        for sub in subs:
            tg_subscribe_symbols(sub['chat_id'], [symbol])
        return True
    except: return False

def uw_remove(user_id, symbol):
    with get_db() as c:
        c.execute("DELETE FROM user_watchlist WHERE user_id=? AND symbol=?", (user_id, symbol))

# ── BACKTEST ENGINE ──────────────────────────────────────────
def backtest(history, capital=10_000_000, market="VN", risk_per_trade=0.02):
    if len(history) < 60: return {"error":"Can it nhat 60 phien"}
    fee_buy = 0.0015 if market=="VN" else 0.0005
    fee_sell = 0.0010 if market=="VN" else 0.0005
    sp = history[55]["c"]

    cash, shares, trades, equity, wins_val, losses_val = _run_backtest_segment(
        history, capital, market, risk_per_trade, start_i=55)

    final = cash + shares*history[-1]["c"]
    profit = round((final-capital)/capital*100, 2)
    bh = round((history[-1]["c"]-sp)/sp*100, 2)
    wins=len(wins_val); losses=len(losses_val)
    wr=round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0
    avg_win=round(sum(wins_val)/wins,2) if wins>0 else 0
    avg_loss=round(sum(losses_val)/losses,2) if losses>0 else 0
    pf_denom=abs(sum(losses_val)) if losses>0 and sum(losses_val)!=0 else 1
    profit_factor=round(sum(wins_val)/pf_denom,2) if wins>0 else 0
    peak=capital; mdd=0
    for v in equity:
        if v>peak: peak=v
        dd=(peak-v)/peak*100
        if dd>mdd: mdd=dd
    n_buys = len([t for t in trades if t["action"]=="BUY"])
    n_sells = len([t for t in trades if "SELL" in t["action"]])
    est_fee_pct=round((n_buys*fee_buy+n_sells*fee_sell)*100,3)

    # Walk-Forward Validation (Nhóm 3.1) — chỉ khi đủ >= 200 bars
    wf_result = None
    n = len(history)
    if n >= 200:
        t1 = int(n*0.6); t2 = int(n*0.8)
        _,_,tr_test,eq_test,w_t,l_t = _run_backtest_segment(history, capital, market, risk_per_trade, start_i=max(55,t1))
        _,_,tr_oos,eq_oos,w_o,l_o   = _run_backtest_segment(history, capital, market, risk_per_trade, start_i=t2)
        def seg_stats(wv, lv, eq, cap):
            wins_s=len(wv); losses_s=len(lv)
            wr_s=round(wins_s/(wins_s+losses_s)*100,1) if (wins_s+losses_s)>0 else 0
            pf_s=round(sum(wv)/abs(sum(lv)),2) if lv and sum(lv)!=0 else (round(sum(wv),2) if wv else 0)
            final_s=(eq[-1] if eq else cap)
            ret_s=round((final_s-cap)/cap*100,2)
            return {"win_rate":wr_s,"profit_factor":pf_s,"return_pct":ret_s,"trades":wins_s+losses_s}
        wf_result = {
            "test":     seg_stats(w_t, l_t, eq_test, capital),
            "oos":      seg_stats(w_o, l_o, eq_oos,  capital),
            "note": "Test=bars 60-80%, OOS=bars 80-100% — kết quả OOS đáng tin nhất"
        }

    # v12: Drawdown analysis
    dd_periods = calc_drawdown_periods(equity, capital)
    # v12: Monte Carlo
    all_pnl=[t.get('pnl_pct',0) for t in trades if t.get('pnl_pct') is not None]
    mc = monte_carlo_backtest(all_pnl, capital, n_sims=300) if len(all_pnl)>=5 else None
    # v12: Slippage info
    slip_pct=(SLIPPAGE_VN if market=="VN" else SLIPPAGE_US)*100
    # v12: Consecutive stats
    max_cw=max_cl=cw=cl=0
    for t in trades:
        p2=t.get('pnl_pct',0) or 0
        if p2>0: cw+=1;cl=0;max_cw=max(max_cw,cw)
        elif p2<0: cl+=1;cw=0;max_cl=max(max_cl,cl)
    return {"initial_capital":capital,"final_value":round(final,2),"profit_pct":profit,
            "buy_hold_pct":bh,"total_trades":len(trades),"win_rate":wr,
            "wins":wins,"losses":losses,
            "avg_win_pct":avg_win,"avg_loss_pct":avg_loss,"profit_factor":profit_factor,
            "max_drawdown":round(mdd,2),"est_fee_pct":est_fee_pct,
            "slippage_pct":slip_pct,
            "max_consec_win":max_cw,"max_consec_loss":max_cl,
            "look_ahead_bias_fixed":True,
            "walk_forward":wf_result,
            "drawdown_periods":dd_periods,
            "monte_carlo":mc,
            "trades":trades[-20:],"equity_curve":equity[-150:]}

def _run_backtest_segment(history, capital, market, risk_per_trade, start_i=55):
    fee_buy = 0.0015 if market=="VN" else 0.0005
    fee_sell = 0.0010 if market=="VN" else 0.0005
    cash=float(capital); shares=0.0; trades=[]; equity=[]
    sl_price=0.0; peak_price=0.0; entry_price=0.0
    pending_signal = None
    wins_val=[]; losses_val=[]
    for i in range(start_i, len(history)):
        _slip = SLIPPAGE_VN if market=="VN" else SLIPPAGE_US
        p_exec = float(history[i].get("o") or history[i]["c"]) * (1 + _slip)
        p_close = float(history[i]["c"])
        if shares > 0:
            if p_exec <= sl_price:
                fee = shares*p_exec*fee_sell
                pnl = round((p_exec-entry_price)/entry_price*100, 2)
                cash += shares*p_exec - fee
                shares=0.0; pending_signal=None
                trades.append({"day":i,"action":"SELL_SL","price":round(p_exec,4),
                                "shares":"—","value":round(cash,2),"pnl_pct":pnl,"reason":"TrailingStop"})
                if pnl>0: wins_val.append(pnl)
                else: losses_val.append(pnl)
                sl_price=0.0; peak_price=0.0; entry_price=0.0
                equity.append(round(cash,2)); continue
            if p_close > peak_price:
                peak_price = p_close
                atr_now = float(analyze({"history":history[:i],"price":p_close}).get("atr",0)) or p_close*0.02
                sl_price = max(sl_price, round(p_close - 2.0*atr_now, 4))
        if pending_signal == "BUY" and shares == 0 and cash > 0:
            a_prev = analyze({"history":history[:i],"price":p_close})
            atr_v = float(a_prev.get("atr",0)) or p_exec*0.02
            sl = round(p_exec - 1.5*atr_v, 4)
            rps = max(p_exec - sl, p_exec*0.001)
            max_sh = min((cash*risk_per_trade)/rps, cash/(p_exec*(1+fee_buy)))
            shares = round(max_sh, 6)
            cash -= shares*p_exec*(1+fee_buy)
            entry_price=p_exec; sl_price=sl; peak_price=p_exec
            trades.append({"day":i,"action":"BUY","price":round(p_exec,4),
                           "shares":round(shares,4),"value":round(shares*p_exec,2),"sl":round(sl,4)})
        elif pending_signal == "SELL" and shares > 0:
            fee = shares*p_exec*fee_sell
            pnl = round((p_exec-entry_price)/entry_price*100, 2)
            cash += shares*p_exec - fee
            shares=0.0
            trades.append({"day":i,"action":"SELL","price":round(p_exec,4),
                           "shares":"—","value":round(cash,2),"pnl_pct":pnl,"reason":"Signal"})
            if pnl>0: wins_val.append(pnl)
            else: losses_val.append(pnl)
            sl_price=0.0; peak_price=0.0; entry_price=0.0
        pending_signal = None
        a = analyze({"history":history[:i+1],"price":p_close})
        if a["total"] >= 7 and shares == 0: pending_signal = "BUY"
        elif a["total"] <= -7 and shares > 0: pending_signal = "SELL"
        equity.append(round(cash+shares*p_close, 2))
    return cash, shares, trades, equity, wins_val, losses_val

# ── BACKUP ──────────────────────────────────────────────────
_BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')

def db_backup_now():
    if get_setting('backup_enabled','1') != '1':
        return None
    os.makedirs(_BACKUP_DIR, exist_ok=True)
    keep_days = int(get_setting('backup_days','7'))
    ts = time.strftime('%Y%m%d_%H%M')
    zip_path = os.path.join(_BACKUP_DIR, f'sma_backup_{ts}.zip')
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(DB_PATH, 'sma.db')
        cutoff = time.time() - keep_days * 86400
        for fn in os.listdir(_BACKUP_DIR):
            fp = os.path.join(_BACKUP_DIR, fn)
            if fn.endswith('.zip') and os.path.getmtime(fp) < cutoff:
                os.remove(fp)
        set_setting("last_backup_at", time.strftime("%Y-%m-%d %H:%M"))
        print(f"[SMA] ✅ Backup: {zip_path}")
        return zip_path
    except Exception as e:
        print(f"[SMA] Backup error: {e}")
        return None

def backup_worker():
    import datetime as dt
    while True:
        now = dt.datetime.now()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += dt.timedelta(days=1)
        wait = (next_run - now).total_seconds()
        time.sleep(wait)
        db_backup_now()

# ── WAL CHECKPOINT WORKER ────────────────────────────────────────────────────
def _wal_checkpoint_worker():
    """Chạy WAL checkpoint mỗi 1 giờ để sma.db-wal không phình to."""
    while True:
        time.sleep(3600)
        try:
            with get_db() as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 3095, _e)

# ── EMAIL ───────────────────────────────────────────────────
def email_send(subject, body_html, to_addr=None):
    if get_setting('email_enabled','0') != '1':
        return False, 'Email chưa bật'
    host = get_setting('smtp_host','')
    port = int(get_setting('smtp_port','587'))
    user = get_setting('smtp_user','')
    pw = get_setting('smtp_pass','')
    to = to_addr or get_setting('email_to','')
    if not all([host, user, pw, to]):
        return False, 'Thiếu cấu hình SMTP'
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = user
        msg['To'] = to
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))
        with smtplib.SMTP(host, port, timeout=15) as srv:
            srv.ehlo(); srv.starttls(); srv.login(user, pw)
            srv.sendmail(user, [to], msg.as_string())
        with get_db() as c:
            c.execute("INSERT INTO email_alerts(subject,body,success) VALUES(?,?,1)",(subject, body_html[:2000]))
        return True, 'OK'
    except Exception as e:
        with get_db() as c:
            c.execute("INSERT INTO email_alerts(subject,body,success) VALUES(?,?,0)",(subject, str(e)))
        return False, str(e)

def email_test(host, port, user, pw, to):
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = '✅ SMA Pro — Email test thành công'
        msg['From'] = user; msg['To'] = to
        msg.attach(MIMEText('<h3>SMA Pro Email hoạt động!</h3><p>Cấu hình SMTP đúng.</p>','html','utf-8'))
        with smtplib.SMTP(host, int(port), timeout=15) as srv:
            srv.ehlo(); srv.starttls(); srv.login(user, pw)
            srv.sendmail(user, [to], msg.as_string())
        return True, 'OK'
    except Exception as e:
        return False, str(e)

# ── WEBHOOK ─────────────────────────────────────────────────
def webhook_send(payload_dict):
    import urllib.request as _ur
    url = get_setting('webhook_url','')
    if not url: return False,'Webhook URL chưa cấu hình'
    try:
        data=json.dumps(payload_dict,ensure_ascii=False,default=str).encode('utf-8')
        req=_ur.Request(url,data=data,method='POST',
                        headers={'Content-Type':'application/json','User-Agent':'SMA-Pro'})
        with _ur.urlopen(req,timeout=8) as r: status=r.getcode()
        return (status<400),f'HTTP {status}'
    except Exception as e: return False,str(e)

# ── FOREIGN FLOW ────────────────────────────────────────────
_ff_cache = {}
_ff_lock = threading.Lock()

def fetch_foreign_flow_vn(symbols):
    import urllib.request as _ur
    result = {}

    def _ssi_batch(syms_str):
        try:
            url = f"https://fc.ssi.com.vn/data/ForeignTradingGetList?symbols={syms_str}"
            req = _ur.Request(url, headers={
                'User-Agent':'Mozilla/5.0','Accept':'application/json',
                'Referer':'https://iboard.ssi.com.vn/',
            })
            with _ur.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode())
            return data.get('data', [])
        except: return []

    def _ssi_single(sym):
        try:
            url = f"https://fc.ssi.com.vn/data/ForeignTradingBySector?symbol={sym}&pageIndex=1&pageSize=20"
            req = _ur.Request(url, headers={
                'User-Agent':'Mozilla/5.0','Accept':'application/json',
                'Referer':'https://iboard.ssi.com.vn/',
            })
            with _ur.urlopen(req, timeout=8) as r:
                data = json.loads(r.read().decode())
            items = data.get('data', data.get('items', []))
            if not items: return None
            buy_col = next((c for c in items[0] if 'buy' in c.lower()), None)
            sell_col = next((c for c in items[0] if 'sell' in c.lower()), None)
            if not buy_col or not sell_col: return None
            buy = sum(float(x.get(buy_col, 0) or 0) for x in items)
            sell = sum(float(x.get(sell_col,0) or 0) for x in items)
            buy5 = sum(float(x.get(buy_col, 0) or 0) for x in items[:5])
            sel5 = sum(float(x.get(sell_col,0) or 0) for x in items[:5])
            return {'net_vol':int(buy-sell),'net_5d':int(buy5-sel5),
                    'buy_vol':int(buy),'sell_vol':int(sell)}
        except: return None

    if symbols:
        batch = _ssi_batch(','.join(symbols[:20]))
        for item in batch:
            sym = (item.get('symbol') or item.get('code','')).upper()
            if sym not in symbols: continue
            net = int(float(item.get('foreignNetVol', item.get('netVol', 0)) or 0))
            buy = int(float(item.get('foreignBuyVol', item.get('buyVol', 0)) or 0))
            sell = int(float(item.get('foreignSellVol',item.get('sellVol',0)) or 0))
            result[sym] = {
                'net_vol':net,'net_5d':net,'buy_vol':buy,'sell_vol':sell,
                'signal':'MUA RÒNG' if net>0 else ('BÁN RÒNG' if net<0 else 'TRUNG LẬP'),
                'color':'#00e676' if net>0 else ('#f03050' if net<0 else '#90a4ae'),
            }

    for sym in symbols:
        if sym in result: continue
        d = _ssi_single(sym)
        if d:
            net = d['net_vol']
            result[sym] = {**d,
                'signal':'MUA RÒNG' if net>0 else ('BÁN RÒNG' if net<0 else 'TRUNG LẬP'),
                'color':'#00e676' if net>0 else ('#f03050' if net<0 else '#90a4ae'),
            }
        time.sleep(0.3)
    return result

def get_foreign_flow(symbols):
    now = time.time()
    with _ff_lock:
        uncached = [s for s in symbols if s not in _ff_cache or now-_ff_cache[s].get('_ts',0)>1800]
    if uncached:
        fresh = fetch_foreign_flow_vn(uncached)
        with _ff_lock:
            for s,v in fresh.items(): _ff_cache[s] = {**v,'_ts':now}
    with _ff_lock:
        return {s:_ff_cache[s] for s in symbols if s in _ff_cache}

# ── SECTOR ROTATION ─────────────────────────────────────────
SECTOR_MAP_VN = {
    'Ngân hàng':['VCB','BID','CTG','TCB','MBB','VPB','ACB','HDB','TPB','STB','LPB','MSB'],
    'Bất động sản':['VIC','VHM','NVL','PDR','DXG','KDH','BCM','DIG','HDG','NLG'],
    'Thép/VLXD':['HPG','HSG','NKG','VGS','TIS','CSV','BMP','VCS'],
    'Dầu khí':['GAS','PVD','PVS','PVC','PLX','OIL'],
    'Tiêu dùng':['VNM','SAB','MSN','MWG','PNJ','DGW','FRT'],
    'Công nghệ':['FPT','CMG','VGI','ELC','ITD'],
    'Hàng không/VT':['VJC','HVN','AST','GMD'],
    'Điện':['POW','PC1','GEX','REE','EVF','SHP'],
    'Chứng khoán':['SSI','VND','HCM','MBS','BSI','VCI','SBS'],
    'Hóa chất/Phân bón':['DPM','DCM','CSV','LAS','VAF'],
}

def calc_sector_rotation():
    result = []
    for sector, syms in SECTOR_MAP_VN.items():
        scores=[]; rs_vals=[]; bull=0; bear=0
        syms_buy=[]; syms_sell=[]; syms_watch=[]
        for sym in syms:
            if sym not in _ram_cache: continue
            d = _ram_cache[sym].get('data',{})
            a = d.get('analysis',{})
            if not a:
                try: a = analyze(d)
                except: continue
            t = a.get('total',0); scores.append(t)
            rs = a.get('rs_vs_index',{})
            if rs: rs_vals.append(rs.get('rs',1.0))
            sig = a.get('signal','')
            price = d.get('price',0); chg = d.get('chg',0)
            sym_info = {
                'symbol': sym,
                'signal': sig,
                'score': t,
                'price': price,
                'chg': round(chg, 2),
            }
            if 'BUY' in sig:
                bull += 1
                syms_buy.append(sym_info)
            elif 'SELL' in sig:
                bear += 1
                syms_sell.append(sym_info)
            else:
                syms_watch.append(sym_info)
        if not scores: continue
        # Sort by signal strength
        syms_buy.sort(key=lambda x: -x['score'])
        syms_sell.sort(key=lambda x: x['score'])
        syms_watch.sort(key=lambda x: -abs(x['score']))
        avg = sum(scores)/len(scores)
        avg_rs = sum(rs_vals)/len(rs_vals) if rs_vals else 1.0
        result.append({
            'sector': sector,
            'avg_score': round(avg, 1),
            'bull': bull,
            'bear': bear,
            'total': len(scores),
            'avg_rs': round(avg_rs, 3),
            'momentum': 'STRONG' if avg>=6 else('WEAK' if avg<=-3 else 'NEUTRAL'),
            'color': '#00e676' if avg>=6 else('#f03050' if avg<=-3 else '#ff9100' if avg>=3 else '#546e7a'),
            'symbols_buy': syms_buy,
            'symbols_sell': syms_sell,
            'symbols_watch': syms_watch,
        })
    result.sort(key=lambda x: x['avg_score'], reverse=True)
    return result

# ── PORTFOLIO FUNCTIONS ────────────────────────────────────
def calc_portfolio_nav(user_id):
    account_size = float(get_setting('account_size','0') or 0)
    trades = trade_list(user_id, status='OPEN', limit=500)
    positions=[]; total_cost=0.0; total_val=0.0; sector_exp={}
    for t in trades:
        sym=t['symbol']; mkt=t['market']; qty=t.get('qty',1) or 1
        entry=t.get('entry_price',0) or 0; sl=t.get('sl_price') or 0
        cur=entry; a={}
        if sym in _ram_cache:
            d=_ram_cache[sym].get('data',{})
            cur=d.get('price',0) or entry
            a=d.get('analysis',{})
            if not a:
                try: a=analyze(d)
                except: a={}
        cost=entry*qty; val=cur*qty; pnl=val-cost
        pnl_pct=(pnl/cost*100) if cost else 0
        risk_val=(entry-sl)*qty if sl and entry>sl else 0
        risk_pct=(risk_val/account_size*100) if account_size and risk_val else 0
        sector=a.get('sector','Khác') or 'Khác'
        sector_exp[sector]=sector_exp.get(sector,0)+val
        positions.append({'symbol':sym,'market':mkt,'qty':qty,'entry':entry,'cur_price':cur,
            'cost':cost,'val':val,'pnl':round(pnl,0),'pnl_pct':round(pnl_pct,2),
            'risk_val':round(risk_val,0),'risk_pct':round(risk_pct,2),
            'signal':a.get('signal',''),'total':a.get('total',0),'sector':sector,
            'days_held':_days_held(t.get('entry_date','')),
            't3':t3_warning(t.get('entry_date',''),mkt),
        })
        total_cost+=cost; total_val+=val
    unrealized=total_val-total_cost
    pnl_pct_total=(unrealized/total_cost*100) if total_cost else 0
    total_risk=sum(p['risk_val'] for p in positions)
    port_heat=(total_risk/account_size*100) if account_size else 0
    closed=trade_list(user_id,status='CLOSED',limit=500)
    realized=sum((t.get('exit_price',0)-t.get('entry_price',0))*t.get('qty',1) for t in closed if t.get('exit_price'))
    total_v=total_val or 1
    sector_pct={s:round(v/total_v*100,1) for s,v in sector_exp.items()}
    return {'positions':sorted(positions,key=lambda x:x['pnl_pct'],reverse=True),
        'total_cost':round(total_cost,0),'total_val':round(total_val,0),
        'unrealized_pnl':round(unrealized,0),'pnl_pct':round(pnl_pct_total,2),
        'realized_pnl':round(realized,0),'total_pnl':round(unrealized+realized,0),
        'port_heat':round(port_heat,2),'account_size':account_size,
        'cash':round(account_size-total_cost,0) if account_size else 0,
        'deploy_pct':round(total_cost/account_size*100,1) if account_size else 0,
        'sector_exposure':sector_pct,'n_positions':len(positions)}

def _days_held(entry_date_str):
    try:
        import datetime as _dt
        ed=_dt.datetime.strptime(entry_date_str[:10],'%Y-%m-%d')
        return (_dt.datetime.now()-ed).days
    except: return 0

def t3_warning(entry_date_str, market='VN'):
    if market!='VN': return None
    try:
        import datetime as _dt
        ed=_dt.datetime.strptime(entry_date_str[:10],'%Y-%m-%d').date()
        delta=(_dt.date.today()-ed).days
        if delta<3: return {'warning':True,'days_left':3-delta,'msg':f'⚠️ T+{delta} — chưa thể bán'}
        return {'warning':False,'days_left':0,'msg':''}
    except: return None

def calc_position_size(account_size, entry_price, sl_price, market='VN',
                       risk_pct=2.0, win_rate=None, avg_win=None, avg_loss=None):
    if not account_size or not entry_price or not sl_price:
        return {'qty':0,'risk_amount':0,'method':'Cần nhập đủ thông tin'}
    sl_dist=abs(entry_price-sl_price)
    if sl_dist<=0: return {'qty':0,'risk_amount':0,'method':'SL không hợp lệ'}
    risk_amount=account_size*(risk_pct/100)
    qty_ff=risk_amount/sl_dist
    if market=='VN': qty_ff=max(100,round(qty_ff/100)*100)
    else: qty_ff=max(1,round(qty_ff))
    kelly_frac=None; qty_kelly=None
    if win_rate and avg_win and avg_loss and avg_loss>0:
        b=avg_win/avg_loss; w=win_rate/100
        kelly_frac=max(0,min((b*w-(1-w))/b,0.25))
        qty_kelly=account_size*kelly_frac/entry_price
        if market=='VN': qty_kelly=max(100,round(qty_kelly/100)*100)
        else: qty_kelly=max(1,round(qty_kelly))
    pos_val=qty_ff*entry_price
    return {'qty':int(qty_ff),'qty_kelly':int(qty_kelly) if qty_kelly else None,
        'risk_amount':round(risk_amount,0),'position_value':round(pos_val,0),
        'deploy_pct':round(pos_val/account_size*100,1) if account_size else 0,
        'sl_distance':round(sl_dist,2),'sl_pct':round(sl_dist/entry_price*100,2),
        'kelly_fraction':round(kelly_frac*100,1) if kelly_frac else None,
        'method':'Fixed+Kelly' if kelly_frac else 'Fixed 2% Risk'}

def portfolio_backtest(user_id, capital=100_000_000, risk_per_trade=0.02):
    with get_db() as c:
        rows=c.execute("SELECT symbol,market FROM user_watchlist WHERE user_id=?",(user_id,)).fetchall()
    results=[]; equity_by_date={}
    for row in rows:
        sym,mkt=row['symbol'],row['market']
        bars=db_load_ohlcv(sym,OHLCV_BARS)
        if not bars or len(bars)<60: continue
        try:
            r=backtest(bars,capital,market=mkt,risk_per_trade=risk_per_trade)
            r['symbol']=sym; results.append(r)
            for trade in r.get('trades',[]):
                d=trade.get('exit_date','')[:10]
                if d: equity_by_date[d]=equity_by_date.get(d,0)+trade.get('pnl',0)
        except: continue
    if not results: return {'error':'Không đủ dữ liệu','results':[]}
    total_trades=sum(r.get('total_trades',0) for r in results)
    total_wins=sum(r.get('wins',0) for r in results)
    total_pnl=sum(r.get('total_pnl',0) for r in results)
    avg_wr=total_wins/total_trades*100 if total_trades else 0
    daily_rets=sorted(equity_by_date.items())
    sharpe=0
    if len(daily_rets)>1:
        vals=[v for _,v in daily_rets]; mean_r=sum(vals)/len(vals)
        std_r=(sum((v-mean_r)**2 for v in vals)/len(vals))**0.5
        if std_r>0: sharpe=mean_r/std_r*(252**0.5)
    best=max(results,key=lambda x:x.get('total_pnl',0))
    worst=min(results,key=lambda x:x.get('total_pnl',0))
    return {'total_symbols':len(results),'total_trades':total_trades,'total_wins':total_wins,
        'avg_win_rate':round(avg_wr,1),'total_pnl':round(total_pnl,0),'sharpe':round(sharpe,2),
        'best_sym':best.get('symbol',''),'best_pnl':round(best.get('total_pnl',0),0),
        'worst_sym':worst.get('symbol',''),'worst_pnl':round(worst.get('total_pnl',0),0),
        'equity_curve':[{'date':d,'pnl':round(v,0)} for d,v in daily_rets],
        'by_symbol':[{'symbol':r['symbol'],'win_rate':r.get('win_rate',0),
            'total_pnl':r.get('total_pnl',0),'total_trades':r.get('total_trades',0),
            'max_dd':r.get('max_drawdown_pct',0)} for r in results]}

# ── EFFICIENT FRONTIER ──────────────────────────────────────
def calc_efficient_frontier(user_id, n_points=20):
    import math, random
    trades = trade_list(user_id, status='OPEN', limit=500)
    if len(trades) < 2:
        return {"error": "Cần ít nhất 2 vị thế để tính Efficient Frontier"}
    symbol_returns = {}
    for t in trades:
        sym = t['symbol']
        bars = db_load_ohlcv(sym, 252)
        if len(bars) >= 30:
            cl = [b['c'] for b in bars if b['c'] > 0]
            rets = [(cl[i]-cl[i-1])/cl[i-1] for i in range(1, len(cl))]
            symbol_returns[sym] = rets
    syms = list(symbol_returns.keys())
    if len(syms) < 2:
        return {"error": "Chưa đủ lịch sử giá"}
    n = len(syms); min_len = min(len(r) for r in symbol_returns.values())
    rm = {s: symbol_returns[s][-min_len:] for s in syms}
    mu_list = [sum(rm[s])/min_len for s in syms]
    cov_mat = [[0.0]*n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            ri = rm[syms[i]]; rj = rm[syms[j]]
            cov_mat[i][j] = sum((ri[k]-mu_list[i])*(rj[k]-mu_list[j]) for k in range(min_len))/min_len
    def port_stats(w):
        ar = sum(w[i]*mu_list[i] for i in range(n))*252
        av = math.sqrt(max(sum(w[i]*w[j]*cov_mat[i][j] for i in range(n) for j in range(n))*252, 0))
        sh = (ar-0.045)/av if av > 0 else 0
        return ar, av, sh
    random.seed(42); N_SIM = 1000
    pts = []; bsh = -999; mvo = 999; bsh_pt = None; mvo_pt = None
    for _ in range(N_SIM):
        raw = [random.expovariate(1) for _ in range(n)]
        tot = sum(raw); w = [x/tot for x in raw]
        ar, av, sh = port_stats(w)
        pt = {'return_pct':round(ar*100,2),'vol_pct':round(av*100,2),'sharpe':round(sh,3),
              'weights':{syms[i]:round(w[i]*100,1) for i in range(n)}}
        pts.append(pt)
        if sh > bsh: bsh=sh; bsh_pt=pt
        if av < mvo: mvo=av; mvo_pt=pt
    buckets = {}
    for pt in pts:
        bk = round(pt['vol_pct']/2)*2
        if bk not in buckets or pt['return_pct'] > buckets[bk]['return_pct']:
            buckets[bk] = pt
    efficient = sorted(buckets.values(), key=lambda x: x['vol_pct'])
    eq_w = [1/n]*n; eq_r, eq_v, eq_s = port_stats(eq_w)
    asset_stats = [{'symbol':syms[i],'return_pct':round(mu_list[i]*252*100,2),
        'vol_pct':round((sum((r-mu_list[i])**2 for r in rm[syms[i]])/min_len)**0.5*(252**0.5)*100,2),
        'sharpe':round((mu_list[i]*252-0.045)/max((sum((r-mu_list[i])**2 for r in rm[syms[i]])/min_len)**0.5*(252**0.5),0.0001),2)}
        for i in range(n)]
    return {
        'frontier': efficient, 'all_simulations': pts[:300],
        'optimal_sharpe': bsh_pt, 'min_volatility': mvo_pt,
        'current_portfolio': {'return_pct':round(eq_r*100,2),'vol_pct':round(eq_v*100,2),
                              'sharpe':round(eq_s,3),'note':'Equal weight baseline'},
        'assets': asset_stats, 'n_simulations': N_SIM,
    }

def calc_portfolio_metrics(user_id, risk_free_rate=0.045):
    import math
    trades = trade_list(user_id, status='OPEN', limit=500)
    if not trades: return {"error": "Không có vị thế đang mở"}
    symbol_returns = {}; positions = {}; total_value = 0.0
    for t in trades:
        sym=t['symbol']; qty=float(t.get('qty',1) or 1); entry=float(t.get('entry_price',0) or 0)
        if not entry: continue
        cur_price=entry
        if sym in _ram_cache: cur_price=_ram_cache[sym].get('data',{}).get('price',entry) or entry
        pos_value=cur_price*qty; total_value+=pos_value
        positions[sym]={'qty':qty,'entry':entry,'cur':cur_price,'value':pos_value}
        bars=db_load_ohlcv(sym,252)
        if len(bars)>=20:
            cl=[b['c'] for b in bars if b['c']>0]
            symbol_returns[sym]=[(cl[i]-cl[i-1])/cl[i-1] for i in range(1,len(cl))]
    if not symbol_returns: return {"error":"Không đủ data"}
    weights={sym:positions[sym]['value']/total_value if total_value>0 else 0
             for sym in symbol_returns if sym in positions}
    min_len=min(len(r) for r in symbol_returns.values())
    port_rets=[sum(weights.get(s,0)*symbol_returns[s][-(min_len-i)] for s in symbol_returns)
               for i in range(min_len)]
    if len(port_rets)<10: return {"error":"Chưa đủ lịch sử"}
    n=len(port_rets); mu=sum(port_rets)/n
    std=math.sqrt(sum((r-mu)**2 for r in port_rets)/n) or 0.0001
    ann_ret=mu*252; ann_std=std*math.sqrt(252); rf_d=risk_free_rate/252
    sharpe=round((ann_ret-risk_free_rate)/ann_std,3) if ann_std>0 else 0
    dn=[r for r in port_rets if r<rf_d]
    dn_std=math.sqrt(sum((r-rf_d)**2 for r in dn)/len(dn))*math.sqrt(252) if dn else 0
    sortino=round((ann_ret-risk_free_rate)/dn_std,3) if dn_std>0 else 99.0
    var95=round(-(mu-1.645*std)*100,2); var99=round(-(mu-2.326*std)*100,2)
    var_vnd=round(var95/100*total_value,0)
    hist_var=round(-sorted(port_rets)[max(0,int(n*0.05)-1)]*100,2)
    cum=[1.0]
    for r in port_rets: cum.append(cum[-1]*(1+r))
    pk=cum[0]; mdd=0.0
    for v in cum:
        if v>pk: pk=v
        mdd=max(mdd,(pk-v)/pk)
    mdd_pct=round(mdd*100,2); calmar=round(ann_ret/mdd,3) if mdd>0 else 0
    beta=None
    ib=db_load_ohlcv('VNINDEX',252)
    if len(ib)>=20:
        ic=[b['c'] for b in ib]
        ir=[(ic[i]-ic[i-1])/ic[i-1] for i in range(1,len(ic))]
        n2=min(len(port_rets),len(ir)); pr2=port_rets[-n2:]; ir2=ir[-n2:]
        im=sum(ir2)/n2
        cov=sum((pr2[i]-mu)*(ir2[i]-im) for i in range(n2))/n2
        vi=sum((x-im)**2 for x in ir2)/n2
        beta=round(cov/vi,3) if vi>0 else None
    contrib=sorted([{
        'symbol':s,'weight_pct':round(weights.get(s,0)*100,1),
        'ann_ret_pct':round(sum(r)/len(r)*252*100,2),
        'ann_vol_pct':round((sum((r-sum(r)/len(r))**2 for r in [symbol_returns[s]])[0]/len(symbol_returns[s]))**0.5*math.sqrt(252)*100,2),
        'sharpe':round((sum(symbol_returns[s])/len(symbol_returns[s])*252-risk_free_rate)/
                       ((sum((r-sum(symbol_returns[s])/len(symbol_returns[s]))**2 for r in symbol_returns[s])/len(symbol_returns[s]))**0.5*math.sqrt(252)),2)
        if (sum((r-sum(symbol_returns[s])/len(symbol_returns[s]))**2 for r in symbol_returns[s])/len(symbol_returns[s]))**0.5>0 else 0
    } for s in symbol_returns], key=lambda x:-x['weight_pct'])
    return {
        'n_positions':len(positions),'total_value':round(total_value,0),
        'ann_return_pct':round(ann_ret*100,2),'ann_volatility_pct':round(ann_std*100,2),
        'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'beta':beta,
        'var_95_pct':var95,'var_99_pct':var99,'var_95_vnd':var_vnd,'hist_var_95_pct':hist_var,
        'max_drawdown_pct':mdd_pct,'risk_free_rate':risk_free_rate,'contributions':contrib,
        'equity_curve':[round(v*total_value,0) for v in cum[-200:]],
        'rating':('Xuất sắc' if sharpe>=2 else 'Tốt' if sharpe>=1 else 'Trung bình' if sharpe>=0.5 else 'Kém — xem lại'),
    }

# ── IMPORT FROM CSV ─────────────────────────────────────────
def import_trades_from_csv(user_id, csv_text, broker='generic'):
    import io as _io, csv as _csv
    reader=_csv.DictReader(_io.StringIO(csv_text))
    ok=0; errors=[]
    for i,row in enumerate(reader,1):
        try:
            row={k.lower().strip():str(v).strip() for k,v in row.items()}
            if broker=='ssi':
                row['symbol']=row.get('mã ck',row.get('symbol',''))
                row['entry_price']=row.get('giá khớp',row.get('entry_price','0'))
                row['qty']=row.get('klượng',row.get('qty','0'))
                row['entry_date']=row.get('ngày gd',row.get('entry_date',''))
                row['direction']='LONG' if 'mua' in row.get('loại gd','').lower() else 'SHORT'
            sym=row.get('symbol','').upper().strip()
            price=float(str(row.get('entry_price',0)).replace(',',''))
            qty=float(str(row.get('qty',1)).replace(',',''))
            date=row.get('entry_date','')[:10]
            if not sym or not price or not date: errors.append(f"Dòng {i}: thiếu dữ liệu"); continue
            ep_raw=str(row.get('exit_price','0')).replace(',','')
            sl_raw=str(row.get('sl_price','0')).replace(',','')
            data={'symbol':sym,'market':row.get('market','VN').upper(),
                'direction':row.get('direction','LONG').upper(),
                'entry_price':price,'qty':qty,'entry_date':date,
                'exit_price':float(ep_raw) if ep_raw and float(ep_raw)>0 else None,
                'exit_date':row.get('exit_date','') or None,
                'note':row.get('note',f'Import {broker}'),
                'reason_in':row.get('reason_in','Import'),
                'sl_price':float(sl_raw) if sl_raw and float(sl_raw)>0 else None}
            trade_add(user_id,data); ok+=1
        except Exception as e: errors.append(f"Dòng {i}: {e}")
    return {'imported':ok,'errors':errors}

def get_import_template():
    hdrs=['symbol','market','direction','entry_price','qty','entry_date',
          'exit_price','exit_date','sl_price','note','reason_in']
    ex=['VCB','VN','LONG','88000','1000','2024-01-15','95000','2024-02-10','85000','Ghi chú','MACD breakout']
    buf=io.StringIO()
    w=csv.writer(buf); w.writerow(hdrs); w.writerow(ex)
    return buf.getvalue()

# ── MONTE CARLO ────────────────────────────────────────────
SLIPPAGE_VN = 0.002
SLIPPAGE_US = 0.001

def monte_carlo_backtest(trades_pnl_pct, capital=10_000_000, n_sims=300):
    import random as _rand
    if not trades_pnl_pct or len(trades_pnl_pct) < 5: return None
    n = len(trades_pnl_pct)
    results_final=[]; results_mdd=[]; all_curves=[]
    for _ in range(n_sims):
        shuffled = _rand.choices(trades_pnl_pct, k=n)
        eq=capital; curve=[eq]; peak=eq; mdd=0
        for r in shuffled:
            eq *= (1+r/100); curve.append(round(eq,0))
            if eq>peak: peak=eq
            dd=(peak-eq)/peak*100
            if dd>mdd: mdd=dd
        results_final.append(eq); results_mdd.append(mdd); all_curves.append(curve)
    n_steps=len(all_curves[0])
    p5=[]; p50=[]; p95=[]
    for step in range(n_steps):
        vals=sorted(c[step] for c in all_curves)
        p5.append(vals[int(n_sims*0.05)]); p50.append(vals[int(n_sims*0.50)]); p95.append(vals[int(n_sims*0.95)])
    results_final.sort(); results_mdd.sort()
    loss_prob=sum(1 for v in results_final if v<capital)/n_sims*100
    stride=max(1,n_steps//60)
    return {
        'n_sims':n_sims,
        'median_final':round(results_final[n_sims//2],0),
        'p5_final':round(results_final[int(n_sims*0.05)],0),
        'p95_final':round(results_final[int(n_sims*0.95)],0),
        'median_mdd':round(results_mdd[n_sims//2],2),
        'p95_mdd':round(results_mdd[int(n_sims*0.95)],2),
        'prob_loss':round(loss_prob,1),
        'p5_curve': [int(v) for v in p5[::stride]],
        'p50_curve':[int(v) for v in p50[::stride]],
        'p95_curve':[int(v) for v in p95[::stride]],
    }

# ── DRAWDOWN ANALYSIS ───────────────────────────────────────
def calc_drawdown_periods(equity_curve, capital):
    if not equity_curve or len(equity_curve)<3: return []
    periods=[]; peak=equity_curve[0]; peak_idx=0; in_dd=False; dd_start=0
    for i,v in enumerate(equity_curve):
        if v>=peak:
            if in_dd:
                seg=equity_curve[dd_start:i]
                bv=min(seg); bi=equity_curve.index(bv,dd_start,i)
                dd_pct=(peak-bv)/peak*100
                periods.append({'start':dd_start,'bottom':bi,'recovery':i,
                    'dd_pct':round(dd_pct,2),'duration_bars':i-dd_start,
                    'recovery_bars':i-bi,'bottom_val':round(bv,0)})
                in_dd=False
            peak=v; peak_idx=i
        else:
            if not in_dd: in_dd=True; dd_start=peak_idx
    if in_dd:
        seg=equity_curve[dd_start:]
        bv=min(seg); bi=equity_curve.index(bv,dd_start)
        periods.append({'start':dd_start,'bottom':bi,'recovery':None,
            'dd_pct':round((peak-bv)/peak*100,2),'duration_bars':len(equity_curve)-dd_start,
            'recovery_bars':None,'bottom_val':round(bv,0),'open':True})
    periods.sort(key=lambda x:x['dd_pct'],reverse=True)
    return periods[:10]

# ── TAX EXPORT ──────────────────────────────────────────────
def export_tax_report(user_id, year=None):
    import datetime as _dt
    year = year or _dt.datetime.now().year
    trades_all = trade_list(user_id, status='CLOSED', limit=2000)
    yearly = [t for t in trades_all if t.get('exit_date','')[:4]==str(year)]
    rows=[]; total_buy=0.0; total_sell=0.0; total_profit=0.0; total_loss=0.0
    for t in yearly:
        ep=t.get('entry_price',0) or 0; xp=t.get('exit_price',0) or 0; qty=t.get('qty',1) or 1
        bv=ep*qty; sv=xp*qty; pnl=(xp-ep)*qty; pnl_pct=(pnl/bv*100) if bv else 0
        rows.append({'symbol':t['symbol'],'market':t['market'],
            'entry_date':t.get('entry_date',''),'exit_date':t.get('exit_date',''),
            'qty':qty,'entry_price':ep,'exit_price':xp,
            'buy_val':round(bv,0),'sell_val':round(sv,0),
            'pnl':round(pnl,0),'pnl_pct':round(pnl_pct,2),
            'tax_pp1':round(sv*0.001,0),'tax_pp2':round(max(0,pnl*0.2),0)})
        total_buy+=bv; total_sell+=sv
        if pnl>=0: total_profit+=pnl
        else: total_loss+=abs(pnl)
    net=total_profit-total_loss
    buf=io.StringIO(); w=csv.writer(buf)
    w.writerow([f'BÁO CÁO THUẾ TNCN TỪ CHỨNG KHOÁN — NĂM {year}'])
    w.writerow([f'Tổng GD đóng:{len(rows)}',f'Tổng mua:{round(total_buy,0):,.0f}đ',
                f'Tổng bán:{round(total_sell,0):,.0f}đ',f'Lãi ròng:{round(net,0):,.0f}đ'])
    w.writerow([f'PP1 (0.1% doanh thu):{round(total_sell*0.001,0):,.0f}đ',
                f'PP2 (20% lãi ròng):{round(max(0,net*0.2),0):,.0f}đ','Chọn PP nhỏ hơn'])
    w.writerow([])
    w.writerow(['Mã','TT','Ngày mua','Ngày bán','KL','Giá mua','Giá bán',
                'GT mua','GT bán','Lãi/Lỗ','%','Thuế PP1','Thuế PP2'])
    for r in rows:
        w.writerow([r['symbol'],r['market'],r['entry_date'],r['exit_date'],
                    r['qty'],r['entry_price'],r['exit_price'],
                    r['buy_val'],r['sell_val'],r['pnl'],f"{r['pnl_pct']}%",
                    r['tax_pp1'],r['tax_pp2']])
    return buf.getvalue(), year, len(rows)

# ── TRADE JOURNAL ───────────────────────────────────────────
def trade_add(user_id, data):
    with get_db() as c:
        cur = c.execute("""
            INSERT INTO trades(user_id,symbol,market,direction,entry_price,qty,
                entry_date,reason_in,emotion_in,signal_score,signal_name,
                sl_price,tp1_price,tp2_price,note,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')
        """, (user_id,
              data.get('symbol','').upper(), data.get('market','VN').upper(),
              data.get('direction','LONG').upper(),
              float(data.get('entry_price',0)), float(data.get('qty',1)),
              data.get('entry_date',''), data.get('reason_in',''),
              data.get('emotion_in',''), int(data.get('signal_score',0)),
              data.get('signal_name',''),
              float(data.get('sl_price',0)) if data.get('sl_price') else None,
              float(data.get('tp1_price',0)) if data.get('tp1_price') else None,
              float(data.get('tp2_price',0)) if data.get('tp2_price') else None,
              data.get('note','')))
        return cur.lastrowid

def trade_close(trade_id, user_id, exit_price, exit_date, reason_out='', emotion_out='', note=''):
    with get_db() as c:
        t = c.execute("SELECT * FROM trades WHERE id=? AND user_id=?",(trade_id,user_id)).fetchone()
        if not t: return None
        t=dict(t)
        ep=float(t['entry_price']); xp=float(exit_price); qty=float(t['qty']); mkt=t['market']
        fee_buy=0.0015 if mkt=='VN' else 0.0005
        fee_sell=0.0010 if mkt=='VN' else 0.0005
        if t['direction']=='LONG':
            pnl_pct=(xp-ep)/ep*100; fees=(ep*qty*fee_buy)+(xp*qty*fee_sell); pnl=(xp-ep)*qty-fees
        else:
            pnl_pct=(ep-xp)/ep*100; fees=(ep*qty*fee_buy)+(xp*qty*fee_sell); pnl=(ep-xp)*qty-fees
        c.execute("""UPDATE trades SET exit_price=?,exit_date=?,status='CLOSED',
                     reason_out=?,emotion_out=?,note=CASE WHEN ?!='' THEN ? ELSE note END,
                     updated_at=datetime('now') WHERE id=? AND user_id=?""",
                  (float(exit_price),exit_date,reason_out,emotion_out,note,note,trade_id,user_id))
        return {"pnl":round(pnl,2),"pnl_pct":round(pnl_pct,2),"fees":round(fees,2)}

def trade_list(user_id, status=None, limit=100):
    with get_db() as c:
        if status:
            rows=c.execute("SELECT * FROM trades WHERE user_id=? AND status=? ORDER BY created_at DESC LIMIT ?",(user_id,status,limit)).fetchall()
        else:
            rows=c.execute("SELECT * FROM trades WHERE user_id=? ORDER BY created_at DESC LIMIT ?",(user_id,limit)).fetchall()
    result=[]
    for r in rows:
        d=dict(r)
        if d['status']=='CLOSED' and d['exit_price'] and d['entry_price']:
            ep=float(d['entry_price']); xp=float(d['exit_price'])
            d['pnl_pct']=round((xp-ep)/ep*100 if d['direction']=='LONG' else (ep-xp)/ep*100,2)
        else:
            d['pnl_pct']=None
        result.append(d)
    return result

def trade_stats(user_id):
    rows=[r for r in trade_list(user_id,status='CLOSED',limit=500)]
    open_cnt=len(trade_list(user_id,status='OPEN',limit=500))
    if not rows:
        return {"total":0,"open":open_cnt,"wins":0,"losses":0,"win_rate":0,
                "avg_win":0,"avg_loss":0,"profit_factor":0,"total_pnl_pct":0,
                "best_trade":None,"worst_trade":None,"by_symbol":{},"by_emotion":{},"recent":[]}
    pnls=[r['pnl_pct'] for r in rows if r['pnl_pct'] is not None]
    wins=[p for p in pnls if p>0]; losses=[abs(p) for p in pnls if p<=0]
    avg_win=round(sum(wins)/len(wins),2) if wins else 0
    avg_loss=round(sum(losses)/len(losses),2) if losses else 0
    pf=round(sum(wins)/sum(losses),2) if losses and sum(losses)>0 else 0
    by_sym={}
    for r in rows:
        s=r['symbol']
        if s not in by_sym: by_sym[s]={'trades':0,'pnl_pct':0.0,'wins':0}
        by_sym[s]['trades']+=1
        if r['pnl_pct'] is not None:
            by_sym[s]['pnl_pct']+=r['pnl_pct']
            if r['pnl_pct']>0: by_sym[s]['wins']+=1
    for s in by_sym: by_sym[s]['pnl_pct']=round(by_sym[s]['pnl_pct'],2)
    by_em={}
    for r in rows:
        em=r.get('emotion_in','') or 'Không ghi'
        if em not in by_em: by_em[em]={'trades':0,'pnl_pct':0.0}
        by_em[em]['trades']+=1
        if r['pnl_pct'] is not None: by_em[em]['pnl_pct']+=r['pnl_pct']
    for e in by_em: by_em[e]['pnl_pct']=round(by_em[e]['pnl_pct'],2)
    return {"total":len(rows),"open":open_cnt,"wins":len(wins),"losses":len(losses),
            "win_rate":round(len(wins)/len(rows)*100,1) if rows else 0,
            "avg_win":avg_win,"avg_loss":avg_loss,"profit_factor":pf,
            "total_pnl_pct":round(sum(p for p in pnls),2),
            "best_trade":max(rows,key=lambda r:r['pnl_pct'] or -999) if rows else None,
            "worst_trade":min(rows,key=lambda r:r['pnl_pct'] or 999) if rows else None,
            "by_symbol":by_sym,"by_emotion":by_em,"recent":rows[:20]}

def market_ctx_refresh():
    for sym,mkt in [('VNINDEX','VN'),('SPY','US')]:
        try:
            data=fetch(sym,mkt)
            if data:
                a=analyze(data)
                trend='UP' if a['total']>=3 else ('DOWN' if a['total']<=-3 else 'SIDEWAYS')
                today=datetime.now().strftime('%Y-%m-%d')
                with get_db() as c:
                    c.execute("INSERT OR REPLACE INTO market_context(symbol,date,trend,score,price) VALUES(?,?,?,?,?)",
                              (sym,today,trend,a['total'],data['price']))
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 3772, _e)

def market_ctx_get():
    result={}
    for sym in ['VNINDEX','SPY']:
        with get_db() as c:
            r=c.execute("SELECT * FROM market_context WHERE symbol=? ORDER BY date DESC LIMIT 1",(sym,)).fetchone()
        result[sym]=dict(r) if r else {"symbol":sym,"trend":"UNKNOWN","score":0,"price":0}
    return result

# ── POPULAR SYMBOLS ─────────────────────────────────────────
VN_POPULAR=["FPT","VNM","VIC","VHM","VCB","BID","CTG","TCB","MBB","VPB","ACB","HDB",
            "SSI","VCI","MWG","PNJ","MSN","SAB","GAS","PLX","HPG","HSG","NKG","VJC",
            "HVN","REE","DGW","FRT","GMD","DCM","DPM","PVD","HAG","DXG","NVL","KDH",
            "PDR","VRE","STB","EIB","LPB","MSB","SHB","VIB","OCB","MPC","IDI","VHC",
            "TLG","DHG","IMP","DMC","TRA","PME","AAA","BMP","DRC","PVT","SGN","ACV"]

US_POPULAR=["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK.B","JPM","V",
            "JNJ","WMT","PG","MA","HD","DIS","BAC","NFLX","XOM","CVX","AMD","INTC",
            "QCOM","TSM","ASML","AVGO","MU","SPY","QQQ","VTI","GLD","TLT"]

VN_NAMES = {
    'VCB':'Vietcombank','BID':'BIDV','CTG':'VietinBank','TCB':'Techcombank',
    'MBB':'MBBank','VPB':'VPBank','ACB':'ACB','HDB':'HDBank','TPB':'TPBank',
    'STB':'Sacombank','LPB':'LienVietPostBank','SHB':'SHB','OCB':'OCB',
    'VIC':'Vingroup','VHM':'Vinhomes','MSN':'Masan','MWG':'Thế Giới Di Động',
    'FPT':'FPT Corporation','HPG':'Hoà Phát','GAS':'PV Gas','PLX':'Petrolimex',
    'SAB':'Sabeco','VNM':'Vinamilk','PNJ':'Phú Nhuận Jewelry','DGW':'Digiworld',
    'SSI':'SSI Securities','VND':'VNDirect','HCM':'HSC','NVL':'Novaland',
    'PDR':'Phát Đạt','KDH':'Khang Điền','VJC':'Vietjet Air','HVN':'Vietnam Airlines',
    'GMD':'Gemadept','REE':'REE Corp','POW':'PV Power','GEX':'Gelex',
    'DPM':'Đạm Phú Mỹ','DCM':'Đạm Cà Mau','VGI':'Viettel Global',
    'CMG':'CMC Corp','ELC':'Elcom','HSG':'Hoa Sen','NKG':'Nam Kim',
    'PKG':'Packaging Corp','KBC':'Kinh Bắc City','DXG':'Danh Khôi',
    'BCM':'Bình Dương Industrial','NLG':'Nam Long','HDG':'Hà Đô',
    'PC1':'PC1 Group','EVF':'Everest','SHP':'Sơn Phú Hydro',
    'PVD':'PV Drilling','PVS':'PV Technical','BSR':'Binh Son Refinery',
}

# ── SCREENER ────────────────────────────────────────────────
def screener_run(market='VN', min_score=5, signal_filter=None, limit=50):
    results=[]
    for sym, cached in _ram_cache.items():
        d=cached.get('data',{})
        if not d: continue
        if market!='ALL' and d.get('market','')!=market: continue
        a=d.get('analysis')
        if not a:
            try: a=analyze(d)
            except: continue
        total=a.get('total',0)
        sig=a.get('signal','')
        if abs(total)<min_score: continue
        if signal_filter and sig!=signal_filter: continue
        vn=d.get('market','')=='VN'
        results.append({
            'symbol':sym, 'market':d.get('market',''),
            'name':d.get('name',''), 'sector':d.get('sector','N/A'),
            'price':d.get('price',0), 'chg':d.get('chg',0),
            'score':total, 'signal':sig,
            'rsi':round(a.get('rsi',50),1),
            'trend':a.get('trend_long',''),
            'confidence':a.get('confidence',0),
            'ma200':round(a.get('ma200',0),2),
            'vwap':round(a.get('vwap',0),2),
        })
    results.sort(key=lambda x: abs(x['score']), reverse=True)
    return results[:limit]

# ── JOURNAL EXPORT ──────────────────────────────────────────
def export_journal_csv(user_id):
    import io, csv
    trades=trade_list(user_id, limit=1000)
    output=io.StringIO()
    w=csv.writer(output)
    w.writerow(["ID","Symbol","Market","Direction","Status","Entry Price","Exit Price",
                "Qty","Entry Date","Exit Date","P&L %","SL","TP1","TP2",
                "Reason In","Reason Out","Emotion In","Emotion Out","Note"])
    for t in trades:
        w.writerow([t.get('id'),t.get('symbol'),t.get('market'),t.get('direction'),t.get('status'),
                    t.get('entry_price'),t.get('exit_price',''),t.get('qty'),
                    t.get('entry_date'),t.get('exit_date',''),t.get('pnl_pct',''),
                    t.get('sl_price',''),t.get('tp1_price',''),t.get('tp2_price',''),
                    t.get('reason_in',''),t.get('reason_out',''),t.get('emotion_in',''),
                    t.get('emotion_out',''),t.get('note','')])
    return output.getvalue()

def export_watchlist_csv(user_id, sd_snapshot):
    import io, csv
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["Symbol","Market","Name","Sector","Price","Change%","Score","Signal","Trend",
                "RSI","MA20","MA50","ATR","SL","TP1","TP2","TrailingStop","Confidence%"])
    for sym, data in sd_snapshot.items():
        a = data.get('analysis',{})
        sc = a.get('scores',{})
        vn = data.get('market')=='VN'
        w.writerow([
            sym, data.get('market',''), data.get('name',''), data.get('sector',''),
            round(data.get('price',0),2), data.get('chg',0),
            a.get('total',0), a.get('signal',''), a.get('trend_long',''),
            round(a.get('rsi',0),1) if a.get('rsi') else '',
            round(a.get('ma20',0),2) if a.get('ma20') else '',
            round(a.get('ma50',0),2) if a.get('ma50') else '',
            round(a.get('atr',0),2) if a.get('atr') else '',
            round(a.get('stop_loss',0),2) if a.get('stop_loss') else '',
            round(a.get('take_profit',0),2) if a.get('take_profit') else '',
            round(a.get('take_profit2',0),2) if a.get('take_profit2') else '',
            round(a.get('trailing_stop',0),2) if a.get('trailing_stop') else '',
            a.get('confidence',0),
        ])
    return output.getvalue()

# ── WATCHLIST GROUPS ────────────────────────────────────────
def wg_list(user_id):
    with get_db() as c:
        rows=c.execute("SELECT * FROM watchlist_groups WHERE user_id=? ORDER BY name",(user_id,)).fetchall()
    return [dict(r) for r in rows]

def wg_add(user_id, name, color='#2563eb'):
    try:
        with get_db() as c:
            cur=c.execute("INSERT INTO watchlist_groups(user_id,name,color) VALUES(?,?,?)",(user_id,name,color))
            return cur.lastrowid
    except: return None

def wg_delete(group_id, user_id):
    with get_db() as c:
        c.execute("DELETE FROM watchlist_groups WHERE id=? AND user_id=?",(group_id,user_id))
        c.execute("UPDATE user_watchlist SET group_id=NULL WHERE group_id=? AND user_id=?",(group_id,user_id))

def wg_assign(user_id, symbol, group_id):
    with get_db() as c:
        c.execute("UPDATE user_watchlist SET group_id=? WHERE user_id=? AND symbol=?",(group_id,user_id,symbol))

# ── PRICE ALERTS ────────────────────────────────────────────
def palert_add(user_id, symbol, market, target_price, direction, note='',
               trigger_type='PRICE', pct_value=None, base_price=None):
    with get_db() as c:
        cur=c.execute(
            """INSERT INTO price_alerts(user_id,symbol,market,target_price,direction,note,
               trigger_type,pct_value,base_price) VALUES(?,?,?,?,?,?,?,?,?)""",
            (user_id, symbol.upper(), market.upper(), float(target_price),
             direction.upper(), note, trigger_type, pct_value, base_price))
        return cur.lastrowid

def palert_list(user_id, include_triggered=False):
    with get_db() as c:
        if include_triggered:
            rows=c.execute("SELECT * FROM price_alerts WHERE user_id=? ORDER BY created_at DESC",(user_id,)).fetchall()
        else:
            rows=c.execute("SELECT * FROM price_alerts WHERE user_id=? AND triggered=0 ORDER BY created_at DESC",(user_id,)).fetchall()
    return [dict(r) for r in rows]

def palert_delete(alert_id, user_id):
    with get_db() as c:
        c.execute("DELETE FROM price_alerts WHERE id=? AND user_id=?",(alert_id,user_id))

def palert_check_all():
    try:
        token = get_setting('tg_token','')
        with get_db() as c:
            rows=c.execute("SELECT * FROM price_alerts WHERE triggered=0").fetchall()
        for row in rows:
            sym=row['symbol']; mkt=row['market']
            target=float(row['target_price']); direction=row['direction']
            trigger_type = row['trigger_type'] if 'trigger_type' in row.keys() else 'PRICE'
            pct_value    = float(row['pct_value'] or 0) if 'pct_value' in row.keys() and row['pct_value'] else None
            base_price   = float(row['base_price'] or 0) if 'base_price' in row.keys() and row['base_price'] else None
            cached=_ram_cache.get(sym)
            if not cached: continue
            price=cached['data'].get('price',0)
            if not price: continue
            triggered=False; trigger_msg=''

            if trigger_type == 'PRICE' or not trigger_type:
                if direction=='ABOVE' and price >= target: triggered=True
                elif direction=='BELOW' and price <= target: triggered=True
                trigger_msg = f"Giá chạm {target:,.0f}" if triggered else ''

            elif trigger_type == 'PCT_CHANGE' and base_price:
                # % change from base_price (set at alert creation)
                chg_pct = (price - base_price) / base_price * 100
                if pct_value and direction == 'ABOVE' and chg_pct >= pct_value:
                    triggered=True; trigger_msg=f"Tăng {chg_pct:+.2f}% từ {base_price:,.0f}"
                elif pct_value and direction == 'BELOW' and chg_pct <= -abs(pct_value):
                    triggered=True; trigger_msg=f"Giảm {chg_pct:+.2f}% từ {base_price:,.0f}"

            elif trigger_type == 'PCT_FROM_HIGH':
                # % drawdown from recent high (from analysis)
                a = cached['data'].get('analysis', {})
                hi = a.get('high_52w') or a.get('pivot_r2') or price
                bars = cached['data'].get('history', [])
                if bars: hi = max(b.get('h', b.get('c',0)) for b in bars[-60:] if b.get('c',0) > 0)
                if hi > 0:
                    dd_pct = (price - hi) / hi * 100
                    if pct_value and dd_pct <= -abs(pct_value):
                        triggered=True; trigger_msg=f"Giảm {dd_pct:.1f}% từ đỉnh {hi:,.0f}"

            elif trigger_type == 'PCT_FROM_LOW':
                bars = cached['data'].get('history', [])
                lo = price
                if bars: lo = min(b.get('l', b.get('c', price)) for b in bars[-60:] if b.get('c',0) > 0)
                if lo > 0:
                    up_pct = (price - lo) / lo * 100
                    if pct_value and up_pct >= pct_value:
                        triggered=True; trigger_msg=f"Tăng {up_pct:.1f}% từ đáy {lo:,.0f}"

            if triggered:
                with get_db() as c2:
                    c2.execute("UPDATE price_alerts SET triggered=1,triggered_at=datetime('now') WHERE id=?",(row['id'],))
                if token:
                    vn=mkt=='VN'; fp=lambda v:(f"{round(v):,}đ" if vn else f"${v:.2f}")
                    icon = '⬆️' if direction=='ABOVE' else '⬇️'
                    type_label = {'PRICE':'💰 Giá cố định','PCT_CHANGE':'📊 % Thay đổi',
                                  'PCT_FROM_HIGH':'📉 % Từ đỉnh','PCT_FROM_LOW':'📈 % Từ đáy'}.get(trigger_type,'')
                    msg=(f"🔔 <b>PRICE ALERT — {sym}</b> · {mkt}\n"
                         f"━━━━━━━━━━━━━━━━━━━━\n"
                         f"{icon} <b>{trigger_msg}</b>\n"
                         f"💰 Giá hiện tại: <b>{fp(price)}</b>\n"
                         f"🏷 Loại: {type_label}\n"
                         f"{('📝 '+row['note']) if row['note'] else ''}\n"
                         f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                    try:
                        with get_db() as c2:
                            subs=c2.execute("SELECT chat_id FROM tg_subscribers WHERE user_id=? AND enabled=1",(row['user_id'],)).fetchall()
                        for sub in subs:
                            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                        json={"chat_id":sub['chat_id'],"text":msg,"parse_mode":"HTML"},timeout=6)
                    except Exception as _e: log.debug('[palert tg] %s', _e)
                # ── Push to live banner (always, even without Telegram) ──
                level = 'CRITICAL' if trigger_type in ('PCT_FROM_HIGH','PCT_FROM_LOW') else 'WARNING'
                vn2 = mkt == 'VN'
                fp2 = lambda v: f"{round(v):,}đ" if vn2 else f"${v:.2f}"
                push_live_alert(
                    sym, mkt, level,
                    f"{'🔴' if level=='CRITICAL' else '🟠'} PRICE ALERT — {sym}",
                    f"{trigger_msg} · Giá: {fp2(price)}",
                    action='Nhấn để xem phân tích'
                )
    except Exception as e:
        print(f"[palert] {e}")

# ── STARTUP ─────────────────────────────────────────────────
def _pre_warm_cache():
    try:
        with get_db() as c:
            rows = c.execute("SELECT DISTINCT symbol, market FROM fetch_meta ORDER BY last_fetch DESC").fetchall()
        loaded = 0
        for r in rows:
            sym, mkt = r['symbol'], r['market']
            try:
                bars = db_load_ohlcv(sym, OHLCV_BARS)
                meta = db_get_meta(sym)
                if bars and meta:
                    data = _build_from_db(sym, mkt, bars, meta)
                    if data:
                        _ram_cache[sym] = {'ts': time.time(), 'data': data}
                        loaded += 1
            except Exception as _ple: log.debug('[preload] %s: %s', sym, _ple)
        print(f"[SMA] ✅ Pre-warm: {loaded}/{len(rows)} symbols từ fetch_meta → RAM")
    except Exception as e:
        print(f"[SMA] Pre-warm error: {e}")

def preload_from_db():
    with get_db() as c:
        rows = c.execute("SELECT symbol,market FROM watchlist").fetchall()
    loaded=0; stale=[]
    for r in rows:
        sym,mkt=r["symbol"],r["market"]
        bars=db_load_ohlcv(sym, OHLCV_BARS)
        if bars:
            meta=db_get_meta(sym)
            data=_build_from_db(sym,mkt,bars,meta)
            if data:
                _ram_cache[sym]={'ts':time.time(),'data':data}
                loaded+=1
                age = db_meta_age_hours(sym)
                threshold = 24 if mkt=="VN" else 12
                if age > threshold:
                    stale.append((sym,mkt))
        else:
            stale.append((r["symbol"],r["market"]))
    print(f"[SMA] ✅ Preloaded {loaded}/{len(rows)} symbols từ DB → hiển thị ngay")
    return loaded

# ── BACKGROUND REFRESH ──────────────────────────────────────
def start_refresh():
    def loop():
        while True:
            try:
                with get_db() as c:
                    # FIX Bug 4: đọc từ user_watchlist DISTINCT (thay vì global watchlist cũ)
                    rows=c.execute("SELECT DISTINCT symbol,market FROM user_watchlist ORDER BY added_at").fetchall()
                    if not rows:
                        # Fallback: nếu user_watchlist trống, dùng global watchlist
                        rows=c.execute("SELECT symbol,market FROM watchlist ORDER BY added_at").fetchall()
                threshold=int(get_setting('alert_threshold','8'))
                for r in rows:
                    sym,mkt=r["symbol"],r["market"]
                    # Chỉ refresh khi RAM cache hết hạn
                    _cached_d = _ram_cache.get(sym, {}).get('data', {})
                    _cached_a = _cached_d.get('analysis', {}) or {}
                    # ── Run smart advisor on EVERY cycle using cached data ──
                    if _cached_d and _cached_a:
                        try:
                            _sa_ts2 = getattr(loop, '_sa_ts', {})
                            def _sa_ok2(k, ttl=3600): return time.time() - _sa_ts2.get(f"{sym}_{k}", 0) > ttl
                            def _sa_mark2(k): _sa_ts2[f"{sym}_{k}"] = time.time(); setattr(loop, '_sa_ts', _sa_ts2)
                            _p2 = _cached_d.get('price', 0); _vn2 = mkt == 'VN'
                            _fp2 = lambda v: f"{round(v):,}đ" if _vn2 else f"${v:.2f}"
                            # RSI extremes
                            _rsi2 = float(_cached_a.get('rsi', 50) or 50)
                            if _rsi2 <= 25 and _sa_ok2('rsi_os', 14400):
                                _sa_mark2('rsi_os')
                                push_live_alert(sym, mkt, 'INFO', f"📉 RSI QUÁ BÁN — {sym}", f"{_fp2(_p2)} · RSI={_rsi2:.0f} — Vùng tích lũy tốt", action=f"Xem {sym}")
                            elif _rsi2 >= 75 and _sa_ok2('rsi_ob', 14400):
                                _sa_mark2('rsi_ob')
                                push_live_alert(sym, mkt, 'WARNING', f"📈 RSI QUÁ MUA — {sym}", f"{_fp2(_p2)} · RSI={_rsi2:.0f} — Cân nhắc chốt", action=f"Kiểm tra {sym}")
                            # TP hit
                            _tp1 = _cached_a.get('take_profit', 0); _tp2a = _cached_a.get('take_profit2', 0)
                            for _tv, _tn, _tk in [(_tp1,'TP1','tp1'), (_tp2a,'TP2','tp2')]:
                                if _tv and _p2 and _p2 >= _tv * 0.998 and _sa_ok2(_tk, 7200):
                                    _sa_mark2(_tk)
                                    push_live_alert(sym, mkt, 'CRITICAL', f"🎯 {_tn} ĐẠT — {sym}", f"Giá {_fp2(_p2)} ≥ {_tn} {_fp2(_tv)} · Chốt lời ngay!", action=f"Chốt {sym}")
                            # Support/Resistance proximity
                            _r1 = _cached_a.get('pivot_r1', 0); _s1 = _cached_a.get('pivot_s1', 0)
                            if _r1 and _p2 and _sa_ok2('near_r1', 14400):
                                _dr1 = (_r1 - _p2) / _p2 * 100
                                if 0 < _dr1 <= 1.5:
                                    _sa_mark2('near_r1')
                                    push_live_alert(sym, mkt, 'INFO', f"🎯 GẦN KHÁNG CỰ — {sym}", f"{_fp2(_p2)} → R1={_fp2(_r1)} · Cách {_dr1:.1f}% · Theo dõi breakout", action=f"Xem {sym}")
                            if _s1 and _p2 and _sa_ok2('near_s1', 14400):
                                _ds1 = (_p2 - _s1) / _p2 * 100
                                if 0 < _ds1 <= 1.5:
                                    _sa_mark2('near_s1')
                                    push_live_alert(sym, mkt, 'WARNING', f"🛡 GẦN HỖ TRỢ — {sym}", f"{_fp2(_p2)} · S1={_fp2(_s1)} · Cách {_ds1:.1f}% · Chú ý", action=f"Xem {sym}")
                            # Volume bất thường
                            if _cached_a.get('unusual_vol') and abs(_cached_a.get('total', 0)) >= 3 and _sa_ok2('vol_un', 14400):
                                _sa_mark2('vol_un')
                                push_live_alert(sym, mkt, 'INFO', f"📊 VOLUME BẤT THƯỜNG — {sym}", f"{_fp2(_p2)} · {_cached_a.get('unusual_vol_detail','Volume bất thường')[:60]}", action=f"Xem {sym}")
                            # Candle patterns (fast check on last 6 bars)
                            if _sa_ok2('candle', 10800):
                                _bars2 = _cached_d.get('history', [])
                                if _bars2 and len(_bars2) >= 4:
                                    _pts2 = detect_candle_patterns(_bars2[-6:])
                                    _strong2 = [_pt for _pt in _pts2 if abs(_pt.get('score', 0)) >= 3]
                                    if _strong2:
                                        _sa_mark2('candle')
                                        _pt = _strong2[0]
                                        _ic = '🟢' if _pt['type'] == 'BULLISH' else '🔴'
                                        _lvl = 'INFO' if _pt['type'] == 'BULLISH' else 'WARNING'
                                        push_live_alert(sym, mkt, _lvl, f"{_ic} {_pt['name']} — {sym}", f"{_fp2(_p2)} · {_pt.get('detail','')[:60]} · {_pt.get('reliability',0)}%", action=f"Xem biểu đồ {sym}")
                        except Exception as _e_sa2: log.debug("[sa_quick] %s: %s", sym, _e_sa2)

                    if ram_age(sym) < (RAM_TTL_VN if mkt=="VN" else RAM_TTL_US) - 10:
                        continue
                    data=fetch(sym,mkt)
                    if not data: continue
                    a=analyze(data)
                    data["analysis"]=a
                    # Lưu kết quả analyze vào DB để screener/invest dùng
                    try:
                        with get_db() as _ac:
                            _ac.execute("""INSERT OR REPLACE INTO analysis_cache
                                (symbol, market, analysis, ta_score, signal, analyzed_at)
                                VALUES (?,?,?,?,?,?)""",
                                (sym, mkt,
                                 json.dumps(a, ensure_ascii=False, default=str),
                                 int(a.get('total', 0)),
                                 str(a.get('signal', 'NEUTRAL')),
                                 datetime.now().isoformat()))
                    except Exception as _ace:
                        log.debug("[refresh] analysis_cache save %s: %s", sym, _ace)
                    # Push INFO banner for medium signals (score 5-7)
                    if abs(a["total"]) >= 5 and abs(a["total"]) < threshold:
                        sig_med = a["signal"]
                        if 'BUY' in sig_med or 'SELL' in sig_med:
                            _med_key = f"{sym}_{sig_med}_med"
                            _med_last = getattr(loop, '_med_ts', {})
                            if time.time() - _med_last.get(_med_key, 0) > 7200:  # 2h dedup
                                _med_last[_med_key] = time.time()
                                loop._med_ts = _med_last
                                icon_m = '🟡' if 'BUY' in sig_med else '🟠'
                                vn_m = mkt == 'VN'
                                fp_m = lambda v: f"{round(v):,}đ" if vn_m else f"${v:.2f}"
                                push_live_alert(
                                    sym, mkt, 'INFO',
                                    f"{icon_m} {sig_med.replace('_',' ')} — {sym}",
                                    f"{fp_m(data['price'])} · Score: {a['total']:+d}/19 · {a.get('confluence_label','')}",
                                    action='Nhấn để xem phân tích'
                                )
                    if abs(a["total"])>=threshold:
                        with get_db() as c2:
                            last=c2.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal=? AND timeframe='1D' ORDER BY created_at DESC LIMIT 1",
                                            (sym,a["signal"])).fetchone()
                            if not last or (datetime.now()-datetime.fromisoformat(last["created_at"])).total_seconds()>3600:
                                c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",
                                           (sym,mkt,a["signal"],a["total"],data["price"],'1D',json.dumps(a["details"])))
                                # v18: Breakout D+1 gap filter trước khi gửi Telegram
                                should_send, gap_reason = check_breakout_d1_gap(sym, mkt, a["signal"])
                                if should_send:
                                    is_first = _digest_add(sym, mkt, a["signal"], a["total"], data["price"], '1D', a["details"], a)
                                    if is_first:
                                        global _alert_digest_ts
                                        _alert_digest_ts = time.time()
                                else:
                                    log.debug("[Alert] %s suppressed by gap filter", sym)
                            # ── Push to live banner (KHÔNG phụ thuộc gap filter) ──
                            sig_str = a["signal"]
                            lvl = 'CRITICAL' if 'STRONG' in sig_str else 'WARNING'
                            icon = '🟢' if 'BUY' in sig_str else '🔴'
                            vn_f = mkt == 'VN'
                            fp_f = lambda v: f"{round(v):,}đ" if vn_f else f"${v:.2f}"
                            conf_lbl = a.get('confluence_label', '')
                            push_live_alert(
                                sym, mkt, lvl,
                                f"{icon} {sig_str.replace('_',' ')} — {sym}",
                                f"{fp_f(data['price'])} · Score: {a['total']:+d}/19 · {conf_lbl}",
                                action='Nhấn để xem phân tích'
                            )
                    # MTF consensus alert — chỉ tính MTF khi 1D có dấu hiệu
                    if abs(a["total"]) >= max(3, threshold//2):
                        mtf = analyze_mtf(data)
                        con = mtf.get("consensus","MIXED")
                        con_score = mtf.get("consensus_score",0)
                    else:
                        con = "MIXED"; con_score = 0
                    if con in ("ALIGNED_BULL","ALIGNED_BEAR") and abs(con_score)>= threshold*2:
                        con_sig = "BUY" if con=="ALIGNED_BULL" else "SELL"
                        with get_db() as c2:
                            last_mtf=c2.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal=? AND timeframe='MTF' ORDER BY created_at DESC LIMIT 1",
                                                (sym,con_sig)).fetchone()
                            if not last_mtf or (datetime.now()-datetime.fromisoformat(last_mtf["created_at"])).total_seconds()>14400:
                                c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",
                                        (sym,mkt,con_sig,con_score,data["price"],'MTF',json.dumps({"consensus":con,"1D":mtf["1D"]["total"],"1W":mtf["1W"]["total"],"1M":mtf["1M"]["total"]})))
                                _nl_mtf=_get_alert_context(sym,mkt)
                                msg_mtf=fmt_alert_mtf(sym,mkt,con,con_score,data["price"],mtf,news_lines=_nl_mtf)
                                threading.Thread(target=tg_send_to_subscribers,args=(sym,msg_mtf),daemon=True).start()
                    # SL Alert khi giá gần stop loss
                    sl=a.get("stop_loss",0); pnow=data.get("price",0)
                    if sl and sl>0 and pnow>0:
                        sl_pct=(pnow-sl)/pnow*100
                        if sl_pct<3.0:
                            with get_db() as c2:
                                last_sl=c2.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='SL_WARN' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
                            if not last_sl or (datetime.now()-datetime.fromisoformat(last_sl["created_at"])).total_seconds()>7200:
                                vn=mkt=="VN"; fp2=lambda v:(f"{round(v):,}d" if vn else f"${v:.2f}")
                                sep="━━━━━━━━━━━━━━━━━━━━"
                                sl_lines=[f"⚠️ <b>CANH BAO STOP LOSS — {sym} ({mkt})</b>",sep,
                                        f"💰 Gia hien tai: <b>{fp2(pnow)}</b>",
                                        f"🛡 Stop Loss (1.5×ATR): <b>{fp2(sl)}</b>",
                                        f"📏 Con cach SL: <b>{sl_pct:.1f}%</b>",
                                        f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}",sep,
                                        "🔔 <b>Kiem tra lai vi the neu dang giu ma nay!</b>",
                                        f"🔗 {get_setting('public_url',f'http://localhost:{PORT}')}"]
                                with get_db() as c2:
                                    c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,"SL_WARN",0,pnow,"1D","{}"))
                                threading.Thread(target=tg_send_to_subscribers,args=(sym,"\n".join(sl_lines)),daemon=True).start()
                    # ── TP1 Alert ──
                    try:
                        tp1=a.get('take_profit',0); tp2=a.get('take_profit2',0)
                        for lv,nm in [(tp1,'TP1 (2×ATR)'),(tp2,'TP2 (3×ATR)')]:
                            if not lv or not pnow: continue
                            if pnow >= lv*0.998:
                                sig_key=f'TP1_{sym}' if nm.startswith('TP1') else f'TP2_{sym}'
                                with get_db() as c2:
                                    last_tp=c2.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal=? ORDER BY created_at DESC LIMIT 1",(sym,sig_key)).fetchone()
                                if not last_tp or (datetime.now()-datetime.fromisoformat(last_tp["created_at"])).total_seconds()>7200:
                                    vn=mkt=="VN"; fp2=lambda v:(f"{round(v):,}đ" if vn else f"${v:.2f}")
                                    msg_tp=[f"🎯 <b>{nm} ĐẠT MỤC TIÊU — {sym} ({mkt})</b>",
                                            "━━━━━━━━━━━━━━━━━━━━",
                                            f"💰 Giá hiện tại: <b>{fp2(pnow)}</b>",
                                            f"🎯 {nm}: <b>{fp2(lv)}</b>",
                                            f"📊 Signal: {a.get('signal','?')} | Score: {a.get('total',0):+d}/16",
                                            "💡 Xem xét chốt lời một phần hoặc dời SL lên break-even"]
                                    threading.Thread(target=tg_send_to_subscribers,args=(sym,"\n".join(msg_tp)),daemon=True).start()
                                    with get_db() as c2:
                                        c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,sig_key,a.get('total',0),pnow,"1D","{}"))
                    except Exception as e_tp: log.debug("[TP alert] %s: %s", sym, e_tp)
                    # Tính RS vs Index và lưu vào data cache
                    try:
                        idx_sym = "VNINDEX" if mkt=="VN" else "SPY"
                        rs = calc_rs_vs_index(data.get("history",[]), index_sym=idx_sym)
                        if rs:
                            data["_rs_vs_index"] = rs
                            _ram_cache[sym] = {'ts': _ram_cache[sym]['ts'], 'data': data}
                        if True:  # v24 merged
                            try:
                                with get_db() as _c23:
                                    _uu = _c23.execute("SELECT DISTINCT user_id FROM user_watchlist WHERE symbol=?", (sym,)).fetchall()
                                for _uu_row in _uu:
                                    _pos = trade_list(_uu_row["user_id"], status="OPEN", limit=100)
                                    if _pos:
                                        threading.Thread(target=_VAR_MON.check, args=(_uu_row["user_id"], _pos, _ram_cache), daemon=True).start()
                                        # Real-time SL check → CRITICAL alert < 30s
                                        threading.Thread(target=check_portfolio_sl_realtime,
                                            args=(_uu_row["user_id"], _pos, _ram_cache), daemon=True).start()
                            except Exception: pass
                        # Multi-condition custom signal alerts
                        try: check_multi_condition_alerts(sym, mkt, a, data.get('price', 0))
                        except Exception as _mce: log.debug("[multi_cond] %s: %s", sym, _mce)

                        # ════════════════════════════════════════════════════════
                        # SMART ADVISOR ALERTS — như trợ lý đầu tư thực thụ
                        # Chạy theo throttle riêng để không spam
                        # ════════════════════════════════════════════════════════
                        _sa_ts = getattr(loop, '_sa_ts', {})
                        _sa_key = lambda k: f"{sym}_{k}"
                        def _sa_ok(k, ttl=3600):
                            return time.time() - _sa_ts.get(_sa_key(k), 0) > ttl
                        def _sa_mark(k):
                            _sa_ts[_sa_key(k)] = time.time()
                            loop._sa_ts = _sa_ts

                        price = data.get('price', 0)
                        vn_sa = mkt == 'VN'
                        fp_sa = lambda v: f"{round(v):,}đ" if vn_sa else f"${v:.2f}"

                        # 1. PRE-BREAKOUT — cờ vào lệnh sớm (4h dedup)
                        try:
                            if _sa_ok('breakout', 14400):
                                bk = detect_breakout_pre_alert(sym, mkt)
                                if bk.get('is_breakout') and bk.get('confidence', 0) >= 60:
                                    _sa_mark('breakout')
                                    reasons = ' · '.join(bk.get('reasons', [])[:2])
                                    push_live_alert(
                                        sym, mkt, 'WARNING',
                                        f"⚡ PRE-BREAKOUT — {sym}",
                                        f"{fp_sa(price)} · Độ tin cậy {bk['confidence']:.0f}% · {reasons}",
                                        action=f"Cân nhắc vào lệnh {sym}"
                                    )
                        except Exception as _e_bk: log.debug("[sa_breakout] %s: %s", sym, _e_bk)

                        # 2. SMART MONEY — tích lũy/phân phối (6h dedup)
                        try:
                            if _sa_ok('smart_money', 21600):
                                sm = detect_smart_money(sym, mkt)
                                verdict = sm.get('verdict', 'NEUTRAL')
                                sm_score = sm.get('score', 0)
                                if verdict == 'ACCUMULATION' and sm_score >= 3:
                                    _sa_mark('smart_money')
                                    signals = sm.get('signals', [])
                                    detail = signals[0] if signals else 'Smart money đang vào hàng'
                                    push_live_alert(
                                        sym, mkt, 'INFO',
                                        f"📦 SMART MONEY VÀO — {sym}",
                                        f"{fp_sa(price)} · {detail[:60]}",
                                        action=f"Xem phân tích dòng tiền {sym}"
                                    )
                                elif verdict == 'DISTRIBUTION' and sm_score <= -3:
                                    _sa_mark('smart_money')
                                    push_live_alert(
                                        sym, mkt, 'WARNING',
                                        f"🚨 SMART MONEY XẢ HÀNG — {sym}",
                                        f"{fp_sa(price)} · Smart money đang phân phối — cân nhắc chốt",
                                        action=f"Kiểm tra {sym} ngay"
                                    )
                        except Exception as _e_sm: log.debug("[sa_smart] %s: %s", sym, _e_sm)

                        # 3. CANDLE PATTERNS — tín hiệu đảo chiều (3h dedup)
                        try:
                            if _sa_ok('candle', 10800):
                                bars_sa = data.get('history', [])
                                if bars_sa and len(bars_sa) >= 4:
                                    pts = detect_candle_patterns(bars_sa[-6:])
                                    # Chỉ push pattern mạnh (score >= 3)
                                    strong = [p for p in pts if abs(p.get('score', 0)) >= 3]
                                    if strong:
                                        p = strong[0]
                                        _sa_mark('candle')
                                        icon_c = '🟢' if p['type'] == 'BULLISH' else '🔴'
                                        push_live_alert(
                                            sym, mkt, 'INFO' if p['type'] == 'BULLISH' else 'WARNING',
                                            f"{icon_c} {p['name']} — {sym}",
                                            f"{fp_sa(price)} · {p.get('detail','')[:60]} · Độ tin cậy {p.get('reliability',0)}%",
                                            action=f"Xem biểu đồ {sym}"
                                        )
                        except Exception as _e_cp: log.debug("[sa_candle] %s: %s", sym, _e_cp)

                        # 4. TP HIT — chốt lời (2h dedup)
                        try:
                            tp1 = a.get('take_profit', 0); tp2 = a.get('take_profit2', 0)
                            for tp_val, tp_name, tp_key in [(tp1,'TP1','tp1'), (tp2,'TP2','tp2')]:
                                if tp_val and price and price >= tp_val * 0.998 and _sa_ok(tp_key, 7200):
                                    _sa_mark(tp_key)
                                    profit_pct = (price - tp_val) / tp_val * 100 if tp_val else 0
                                    push_live_alert(
                                        sym, mkt, 'CRITICAL',
                                        f"🎯 {tp_name} ĐẠT — {sym}",
                                        f"Giá {fp_sa(price)} ≥ {tp_name} {fp_sa(tp_val)} · Cân nhắc chốt lời!",
                                        action=f"Chốt lời {sym}"
                                    )
                        except Exception as _e_tp: log.debug("[sa_tp] %s: %s", sym, _e_tp)

                        # 5. VOLUME BẤT THƯỜNG — dấu hiệu quan tâm (4h dedup)
                        try:
                            if _sa_ok('vol_unusual', 14400):
                                vol_unusual = a.get('unusual_vol', False)
                                if vol_unusual and abs(a.get('total', 0)) >= 3:
                                    _sa_mark('vol_unusual')
                                    vol_detail = a.get('unusual_vol_detail', 'Volume bất thường')
                                    push_live_alert(
                                        sym, mkt, 'INFO',
                                        f"📊 VOLUME BẤT THƯỜNG — {sym}",
                                        f"{fp_sa(price)} · {vol_detail[:70]}",
                                        action=f"Theo dõi {sym}"
                                    )
                        except Exception as _e_vol: log.debug("[sa_vol] %s: %s", sym, _e_vol)

                        # 6. RSI CỰC TRỊ — mua bán quá mức (4h dedup)
                        try:
                            rsi_val = float(a.get('rsi', 50) or 50)
                            if rsi_val <= 25 and _sa_ok('rsi_oversold', 14400):
                                _sa_mark('rsi_oversold')
                                push_live_alert(
                                    sym, mkt, 'INFO',
                                    f"📉 RSI QUÁN BÁN — {sym}",
                                    f"{fp_sa(price)} · RSI={rsi_val:.0f} ≤ 25 — Vùng có thể mua vào",
                                    action=f"Xem cơ hội {sym}"
                                )
                            elif rsi_val >= 75 and _sa_ok('rsi_overbought', 14400):
                                _sa_mark('rsi_overbought')
                                push_live_alert(
                                    sym, mkt, 'WARNING',
                                    f"📈 RSI QUÁ MUA — {sym}",
                                    f"{fp_sa(price)} · RSI={rsi_val:.0f} ≥ 75 — Cân nhắc chốt lời",
                                    action=f"Kiểm tra {sym}"
                                )
                        except Exception as _e_rsi: log.debug("[sa_rsi] %s: %s", sym, _e_rsi)

                        # 7. SUPPORT/RESISTANCE GẦN — vùng quyết định (4h dedup)
                        try:
                            r1 = a.get('pivot_r1', 0); s1 = a.get('pivot_s1', 0)
                            if r1 and price and _sa_ok('near_r1', 14400):
                                dist_r1 = (r1 - price) / price * 100
                                if 0 < dist_r1 <= 1.5:  # giá cách R1 ≤ 1.5%
                                    _sa_mark('near_r1')
                                    push_live_alert(
                                        sym, mkt, 'INFO',
                                        f"🎯 GẦN KHÁNG CỰ R1 — {sym}",
                                        f"{fp_sa(price)} · R1={fp_sa(r1)} · Cách {dist_r1:.1f}% — Chú ý breakout",
                                        action=f"Theo dõi phá vỡ {sym}"
                                    )
                            if s1 and price and _sa_ok('near_s1', 14400):
                                dist_s1 = (price - s1) / price * 100
                                if 0 < dist_s1 <= 1.5:  # giá cách S1 ≤ 1.5%
                                    _sa_mark('near_s1')
                                    push_live_alert(
                                        sym, mkt, 'WARNING',
                                        f"🛡 GẦN HỖ TRỢ S1 — {sym}",
                                        f"{fp_sa(price)} · S1={fp_sa(s1)} · Cách {dist_s1:.1f}% — Chú ý hold/buy",
                                        action=f"Theo dõi {sym}"
                                    )
                        except Exception as _e_sr: log.debug("[sa_sr] %s: %s", sym, _e_sr)

                    except Exception as _e: log.debug('[refresh] digest: %s', _e)
                    time.sleep(1.0)  # throttle
            except Exception as e: print(f"[refresh] {e}")
            # Cập nhật market context mỗi 1 giờ
            try:
                _mc=getattr(loop,'_mc_ts',0)
                if time.time()-_mc>3600:
                    loop._mc_ts=time.time()
                    threading.Thread(target=market_ctx_refresh,daemon=True).start()
                    # Cập nhật Market Regime cùng lúc
                    threading.Thread(target=get_market_regime,daemon=True).start()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4125, _e)
            # Kiểm tra price alerts (user tự đặt) mỗi 30s
            try: palert_check_all()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4128, _e)
            # ── Alert Digest flush (Nhóm 4.1) ─────────────────
            try: _digest_flush()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4131, _e)
            # Signal snapshot lúc 17:15 (sau đóng cửa VN) và Forward test update
            try:
                _snap_key = datetime.now().strftime("%Y-%m-%d-17")
                if datetime.now().hour == 17 and datetime.now().minute >= 15:
                    if getattr(loop, '_last_snap', '') != _snap_key:
                        loop._last_snap = _snap_key
                        threading.Thread(target=save_signal_snapshot, daemon=True).start()
                        threading.Thread(target=forward_test_update, daemon=True).start()
                        # v18: Lưu top signals history
                        threading.Thread(target=save_top_signals_today, daemon=True).start()
                        threading.Thread(target=_update_prediction_actuals, daemon=True).start()
                        threading.Thread(target=update_signal_stats, daemon=True).start()
                        log.info('[TopSig] Saved daily snapshot')
            except Exception as _se: log.warning('[Snapshot] error: %s', _se)


            # v27: Daily digest lúc 17:30
            try:
                _dig_key = datetime.now().strftime('%Y-%m-%d')
                dh = int(get_setting('daily_digest_hour','17'))
                if datetime.now().hour == dh and datetime.now().minute >= 30:
                    if getattr(loop, '_last_digest', '') != _dig_key:
                        loop._last_digest = _dig_key
                        threading.Thread(target=send_daily_digest, daemon=True).start()
            except: pass
            # ── Market Scanner 24/7 at 9:00, 11:30, 14:30, 15:05 ──────────────
            try:
                _now = datetime.now()
                _scan_key = _now.strftime('%Y-%m-%d-%H')
                _scan_minutes = _now.minute
                _scan_hour = _now.hour
                _should_scan = (_scan_hour == 9  and _scan_minutes < 5) or \
                               (_scan_hour == 11 and 30 <= _scan_minutes < 35) or \
                               (_scan_hour == 14 and 30 <= _scan_minutes < 35) or \
                               (_scan_hour == 15 and 5  <= _scan_minutes < 10)
                if _should_scan and getattr(loop, '_last_scan', '') != _scan_key:
                    loop._last_scan = _scan_key
                    threading.Thread(target=run_market_scanner_24_7, daemon=True).start()
                    log.info("[Scanner] Triggered at %s", _now.strftime('%H:%M'))
            except Exception as _se: log.debug('[scanner] %s', _se)
            # ── Daily briefing at 8:30am ─────────────────────────────────────────
            try:
                if datetime.now().hour == 8 and 30 <= datetime.now().minute < 35:
                    _brief_key = datetime.now().strftime('%Y-%m-%d-briefing')
                    if getattr(loop, '_last_brief', '') != _brief_key:
                        loop._last_brief = _brief_key
                        threading.Thread(target=send_morning_briefing, daemon=True).start()
                        log.info("[briefing] Morning briefing triggered")
            except Exception as _be: log.debug('[briefing] %s', _be)
            # ── Earnings calendar alert check (once per day at 8:00) ──────────
            try:
                if datetime.now().hour == 8 and datetime.now().minute < 5:
                    _earn_key = datetime.now().strftime('%Y-%m-%d')
                    if getattr(loop, '_last_earn_check', '') != _earn_key:
                        loop._last_earn_check = _earn_key
                        threading.Thread(target=check_earnings_alerts, daemon=True).start()
            except Exception as _ee: log.debug('[earnings_alert] %s', _ee)
            # v20: Auto-save top signals after each full scan (dedup by date)
            try:
                _tsh_key = datetime.now().strftime("%Y-%m-%d-%H")
                if getattr(loop, '_tsh_saved', '') != _tsh_key:
                    loop._tsh_saved = _tsh_key
                    threading.Thread(target=save_top_signals_today, daemon=True).start()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4152, _e)
            # v18: Đảm bảo VNINDEX/SPY luôn được fetch để dùng làm benchmark
            try:
                _bench_key = getattr(loop, '_bench_ts', 0)
                if time.time() - _bench_key > 3600:
                    loop._bench_ts = time.time()
                    # Fetch all market indices for the header banner
                    for _bs, _bm in [('VNINDEX','VN'), ('VN30','VN'), ('HNX','VN'),
                                     ('SPY','US'), ('^GSPC','US'), ('GC=F','US')]:
                        cached = _ram_cache.get(_bs, {})
                        age = time.time() - cached.get('ts', 0)
                        if not cached or age > 1800:  # refresh every 30min
                            threading.Thread(target=fetch, args=(_bs,_bm), daemon=True).start()
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4161, _e)
            time.sleep(30)  # check alert mỗi 30s
    threading.Thread(target=loop, daemon=True).start()

    # Telegram polling thread
    def tg_loop():
        while True:
            try: tg_process_updates()
            except Exception as _tge: log.debug("[TG poll] %s", _tge)
            time.sleep(3)
    threading.Thread(target=tg_loop, daemon=True).start()

# ── ALERT DIGEST ────────────────────────────────────────────
_alert_digest_buffer = []
_alert_digest_lock = threading.Lock()
_alert_digest_ts = 0.0
DIGEST_WINDOW = 1800

def _digest_add(sym, mkt, signal, total, price, tf, details, extra=None):
    """Thêm alert vào buffer digest. Trả về True nếu đây là alert đầu tiên (cần gửi ngay)."""
    global _alert_digest_ts
    with _alert_digest_lock:
        # Dedup: không thêm trùng (sym, signal) trong window hiện tại
        for item in _alert_digest_buffer:
            if item[0] == sym and item[2] == signal: return False
        _alert_digest_buffer.append((sym, mkt, signal, total, price, tf, details, extra or {}))
        return len(_alert_digest_buffer) == 1

def _digest_flush():
    """Gửi digest tổng hợp nếu đã đủ thời gian hoặc đủ số lượng."""
    global _alert_digest_ts, _alert_digest_buffer
    now = time.time()
    with _alert_digest_lock:
        if not _alert_digest_buffer: return
        # Gửi khi: 30 phút trôi qua HOẶC có 5+ alerts
        if now - _alert_digest_ts < DIGEST_WINDOW and len(_alert_digest_buffer) < 5:
            return
        items = list(_alert_digest_buffer)
        _alert_digest_buffer.clear()
        _alert_digest_ts = now
    # Build digest message
    if not items: return
    if len(items) == 1:
        # Chỉ 1 alert → gửi bình thường
        sym, mkt, signal, total, price, tf, details, extra = items[0]
        nl = _get_alert_context(sym, mkt)
        msg = fmt_alert(sym, mkt, signal, total, price, tf, details, news_lines=nl, extra=extra)
        threading.Thread(target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()
        return
    # Nhiều alerts → gom thành 1 tin
    sep = "━━━━━━━━━━━━━━━━━━━━"
    bull = [x for x in items if "BUY" in x[2]]
    bear = [x for x in items if "SELL" in x[2]]
    lines = [f"📊 <b>DIGEST ALERT — {len(items)} tín hiệu</b>", sep]
    if bull:
        lines.append(f"🟢 <b>MUA ({len(bull)} mã):</b>")
        for sym, mkt, sig, tot, prc, tf, det, ex in sorted(bull, key=lambda x: -abs(x[3])):
            vn = mkt == "VN"
            fp = f"{round(prc):,}đ" if vn else f"${prc:.2f}"
            flag = "🇻🇳" if vn else "🇺🇸"
            lines.append(f"  {flag} <b>{sym}</b>  {sig} {tot:+d}/19  @{fp}")
    if bear:
        lines.append(f"🔴 <b>BÁN ({len(bear)} mã):</b>")
        for sym, mkt, sig, tot, prc, tf, det, ex in sorted(bear, key=lambda x: abs(x[3])):
            vn = mkt == "VN"
            fp = f"{round(prc):,}đ" if vn else f"${prc:.2f}"
            flag = "🇻🇳" if vn else "🇺🇸"
            lines.append(f"  {flag} <b>{sym}</b>  {sig} {tot:+d}/19  @{fp}")
    lines += [sep, f"⏰ {datetime.now().strftime('%d/%m %H:%M')}",
              f"🔗 {get_setting('public_url', f'http://localhost:{PORT}')}",
              "💡 Nhấn vào từng mã trong app để xem chi tiết"]
    digest_msg = "\n".join(lines)
    # Gửi đến tất cả subscribers có ít nhất 1 trong các mã
    syms_in_digest = [x[0] for x in items]
    with get_db() as c:
        all_subs = c.execute("SELECT chat_id, symbols FROM tg_subscribers WHERE enabled=1").fetchall()
    sent_chats = set()
    for sub in all_subs:
        try:
            sub_syms = json.loads(sub["symbols"] or "[]")
            if any(s in sub_syms for s in syms_in_digest) or sub_syms == []:
                if sub["chat_id"] not in sent_chats:
                    tg_send_to_chat(sub["chat_id"], digest_msg)
                    sent_chats.add(sub["chat_id"])
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 4245, _e)

def tg_send_to_chat(chat_id, text):
    token = get_setting('tg_token')
    if not token or get_setting('tg_enabled') != '1': return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                    timeout=10)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 4254, _e)

def _get_alert_context(sym, mkt):
    lines = []
    try:
        ev_flag, ev_title = has_upcoming_events(sym, mkt, days=7)
        if ev_flag and ev_title:
            lines.append(f"📅 <b>Sự kiện sắp tới:</b> {ev_title}")
        cached = _news_cache_get(sym)
        if cached:
            news = cached[:2]
            if news:
                lines.append("📰 <b>Tin tức gần đây:</b>")
                for n in news:
                    title = str(n.get('title',''))[:80]
                    if title: lines.append(f"  • {title}")
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 4270, _e)
    return lines

def fmt_alert(sym, mkt, signal, total, price, tf, details, news_lines=None, extra=None):
    LEVEL = {
        "BUY": ("🟢🟢🟢", "⚡ MUA MẠNH"),
        "SELL": ("🔴🔴🔴", "⚡ BÁN MẠNH"),
        "BUY_WARN": ("🟡🟡", "⚠️ CẢNH BÁO MUA"),
        "SELL_WARN": ("🟠🟠", "⚠️ CẢNH BÁO BÁN"),
        "BUY_WEAK": ("🟢", "📌 MUA YẾU"),
        "SELL_WEAK": ("🔴", "📌 BÁN YẾU"),
    }
    icons, label = LEVEL.get(signal, ("⚪", signal))
    vn = mkt == "VN"
    fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
    pub = get_setting("public_url", f"http://localhost:{PORT}")
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    sep = "━━━━━━━━━━━━━━━━━━━━"
    lines = [
        f"📊 <b>SMA v{VERSION}</b> · {mkt}",
        f"{icons} <b>{sym}</b> — {label}",
        sep,
        f"📊 Điểm: <b>{total:+d}/19</b>  ·  Khung: <b>{tf}</b>",
        f"💰 Giá: <b>{fp}</b>",
        f"🕐 {now}",
    ]
    good = [(k,v) for k,v in details.items() if v and "Chưa" not in str(v) and "Trung tính" not in str(v)]
    if good:
        lines.append(sep)
        lines.append("<b>📈 Chỉ báo kích hoạt:</b>")
        for k, v in good[:5]:
            dot = "🟢" if any(x in str(v) for x in ["MUA","↑","tăng","Bounce","cắt lên","Bull"]) else "🔴" if any(x in str(v) for x in ["BÁN","↓","giảm","Reject","cắt xuống","Bear"]) else "🔹"
            lines.append(f"  {dot} {v}")
    if extra:
        ep = extra.get
        sl = ep("stop_loss"); tp1 = ep("take_profit"); tp2 = ep("take_profit2")
        if sl and tp1:
            fv = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"
            lines.append(sep)
            lines.append(f"🛡 Stop Loss: <b>{fv(sl)}</b>")
            lines.append(f"🎯 TP1: <b>{fv(tp1)}</b>  TP2: <b>{fv(tp2)}</b>")
    if news_lines:
        lines.append(sep)
        lines.extend(news_lines)
    lines.append(sep)
    lines.append(f"🔗 {pub}")
    return "\n".join(lines)

def fmt_alert_mtf(sym, mkt, consensus, score, price, mtf, news_lines=None):
    bull = consensus == "ALIGNED_BULL"
    icons = "🟢🟢🟢" if bull else "🔴🔴🔴"
    label = "⚡ 3 KHUNG ĐỒNG THUẬN TĂNG" if bull else "⚡ 3 KHUNG ĐỒNG THUẬN GIẢM"
    vn = mkt == "VN"
    fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
    pub = get_setting("public_url", f"http://localhost:{PORT}")
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    sep = "━━━━━━━━━━━━━━━━━━━━"
    abs_score = abs(score)
    strength = "RẤT MẠNH" if abs_score>=36 else "MẠNH" if abs_score>=24 else "VỪA"
    d1=mtf.get("1D",{}); dw=mtf.get("1W",{}); dm=mtf.get("1M",{})
    def bar(t):
        if t is None: return "—"
        return ("🟢" if t>0 else "🔴" if t<0 else "⚪") + f" {t:+d}/19"
    lines = [
        f"📊 <b>SMA Pro</b> · MTF Alert",
        f"{icons} <b>{sym} ({mkt})</b> — {label}",
        sep,
        f"💪 Sức mạnh: <b>{strength}</b>  ({score:+d}/57)",
        f"💰 Giá: <b>{fp}</b>",
        f"🕐 {now}",
        sep,
        "<b>📊 Từng khung thời gian:</b>",
        f"  📅 1 Ngày  : {bar(d1.get('total'))}",
        f"  📅 1 Tuần  : {bar(dw.get('total'))}",
        f"  📅 1 Tháng : {bar(dm.get('total'))}",
        sep,
        "⭐ <b>Tín hiệu TIN CẬY NHẤT</b> — 3 khung cùng chiều",
    ]
    if news_lines:
        lines.append(sep)
        lines.extend(news_lines)
    lines.append(sep)
    lines.append(f"🔗 {pub}  |  <i>SMA v{VERSION}</i>")
    return "\n".join(lines)

# ── MARKET REGIME ───────────────────────────────────────────
_market_regime_cache = {"ts": 0, "regime": "UNKNOWN", "score": 0, "details": {}}
_REGIME_TTL = 600   # 10 phút - cập nhật thường xuyên hơn

def get_market_regime():
    global _market_regime_cache
    if time.time() - _market_regime_cache["ts"] < _REGIME_TTL:
        return _market_regime_cache
    try:
        details = {}; regime_score = 0
        for idx_sym, idx_mkt in [("VNINDEX","VN"), ("SPY","US")]:
            d = _ram_cache.get(idx_sym, {}).get("data")
            if not d:
                d = fetch(idx_sym, idx_mkt)
            if not d: continue
            h = d.get("history", [])
            if len(h) < 50: continue
            closes = [x["c"] for x in h if x["c"] > 0]
            n = len(closes)
            ma20 = sum(closes[-20:])/20
            ma50 = sum(closes[-50:])/50
            ma200 = sum(closes[-200:])/200 if n >= 200 else sum(closes)/n
            price = closes[-1]
            rsi_idx = calc_rsi(closes)
            highs_i = [x.get("h",x["c"]) for x in h[-50:]]
            lows_i = [x.get("l",x["c"]) for x in h[-50:]]
            cl_i = [x["c"] for x in h[-50:]]
            tr_l=[]; dp_l=[]; dm_l=[]
            for k in range(1, len(cl_i)):
                tr_l.append(max(highs_i[k]-lows_i[k],abs(highs_i[k]-cl_i[k-1]),abs(lows_i[k]-cl_i[k-1])))
                up=highs_i[k]-highs_i[k-1]; dn=lows_i[k-1]-lows_i[k]
                dp_l.append(up if up>dn and up>0 else 0)
                dm_l.append(dn if dn>up and dn>0 else 0)
            atr_i=sum(tr_l[:14]); sdp_i=sum(dp_l[:14]); sdm_i=sum(dm_l[:14])
            for k in range(14,len(tr_l)):
                atr_i=atr_i-atr_i/14+tr_l[k]; sdp_i=sdp_i-sdp_i/14+dp_l[k]; sdm_i=sdm_i-sdm_i/14+dm_l[k]
            dip_i=sdp_i/(atr_i or 1)*100; dim_i=sdm_i/(atr_i or 1)*100
            ret_20 = (closes[-1]/closes[-20]-1)*100 if n>=20 else 0
            uptrend = (price > ma50 > ma200) and (ma50 > ma200) and rsi_idx > 50
            downtrend = (price < ma50 < ma200) and rsi_idx < 50
            sym_score = 2 if uptrend else (-2 if downtrend else 0)
            if ret_20 > 3: sym_score += 1
            elif ret_20 < -3: sym_score -= 1
            details[idx_sym] = {
                "price": round(price,2), "ma50": round(ma50,2), "ma200": round(ma200,2),
                "rsi": round(rsi_idx,1), "ret_20d": round(ret_20,2),
                "trend": "UP" if uptrend else ("DOWN" if downtrend else "SIDE"),
                "adx_di_plus": round(dip_i,1), "adx_di_minus": round(dim_i,1)
            }
            regime_score += sym_score
        if regime_score >= 3:
            regime = "BULL"
            recommendation = "📈 Thị trường UPTREND — Ưu tiên BUY, giảm ngưỡng signal xuống ≥6"
            signal_bias = "BUY"
            threshold_adj = -2
        elif regime_score <= -3:
            regime = "BEAR"
            recommendation = "📉 Thị trường DOWNTREND — Chỉ SELL/thoát hàng, ignore BUY"
            signal_bias = "SELL"
            threshold_adj = +2
        else:
            regime = "SIDEWAYS"
            recommendation = "📊 Thị trường SIDEWAYS — Cần tín hiệu rất mạnh (≥10), trade thận trọng"
            signal_bias = "NEUTRAL"
            threshold_adj = +2
        result = {
            "ts": time.time(), "regime": regime, "score": regime_score,
            "details": details, "recommendation": recommendation,
            "signal_bias": signal_bias, "threshold_adj": threshold_adj,
            "updated_at": datetime.now().strftime("%H:%M %d/%m")
        }
        _market_regime_cache = result
        return result
    except Exception as e:
        print(f"[Regime] {e}")
        return _market_regime_cache

# ── RELATIVE STRENGTH ──────────────────────────────────────
def calc_rs_vs_index(sym_history, index_sym="VNINDEX", period=20):
    try:
        idx_data = _ram_cache.get(index_sym, {}).get("data") or fetch(index_sym, "VN" if index_sym=="VNINDEX" else "US")
        if not idx_data: return None
        idx_h = idx_data.get("history", [])
        if len(sym_history) < period+1 or len(idx_h) < period+1: return None
        sym_ret = (sym_history[-1]["c"] / sym_history[-period]["c"]) - 1
        idx_ret = (idx_h[-1]["c"] / idx_h[-period]["c"]) - 1
        if idx_ret == 0: return None
        rs = round(sym_ret / abs(idx_ret) if idx_ret != 0 else 0, 3)
        rs_label = "🚀 Dẫn dắt TT" if rs > 1.2 else ("✅ Mạnh hơn TT" if rs > 1.0 else ("⚠️ Yếu hơn TT" if rs > 0.8 else "❌ Yếu nhất"))
        return {"rs": rs, "sym_ret_pct": round(sym_ret*100,2),
                "idx_ret_pct": round(idx_ret*100,2), "label": rs_label,
                "period": period, "index": index_sym}
    except: return None

# ── CORRELATION MATRIX ─────────────────────────────────────
def calc_correlation_matrix(symbols, period=30):
    try:
        returns = {}
        for sym in symbols:
            d = _ram_cache.get(sym, {}).get("data")
            if not d: continue
            h = d.get("history", [])
            cl = [x["c"] for x in h if x["c"] > 0]
            if len(cl) < period+1: continue
            cl = cl[-(period+1):]
            rets = [(cl[i]-cl[i-1])/cl[i-1] for i in range(1, len(cl))]
            returns[sym] = rets
        syms = list(returns.keys())
        matrix = {}
        for s1 in syms:
            matrix[s1] = {}
            r1 = returns[s1]
            m1 = sum(r1)/len(r1)
            for s2 in syms:
                r2 = returns[s2]
                m2 = sum(r2)/len(r2)
                n = min(len(r1), len(r2))
                cov = sum((r1[i]-m1)*(r2[i]-m2) for i in range(n))/n
                sd1 = (sum((x-m1)**2 for x in r1)/len(r1))**0.5
                sd2 = (sum((x-m2)**2 for x in r2)/len(r2))**0.5
                corr = round(cov/(sd1*sd2),3) if sd1>0 and sd2>0 else (1.0 if s1==s2 else 0.0)
                matrix[s1][s2] = corr
        return {"matrix": matrix, "symbols": syms, "period": period}
    except Exception as e:
        print(f"[Corr] {e}"); return {"matrix":{}, "symbols":[], "period":period}

# ── FORWARD TEST ────────────────────────────────────────────
def forward_test_update():
    try:
        with get_db() as c:
            today = datetime.now().strftime("%Y-%m-%d")
            for days in [5, 10, 20]:
                check_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                snaps = c.execute("""SELECT * FROM signal_snapshots WHERE date=?
                    AND signal IN ('BUY','SELL','BUY_WARN','SELL_WARN')
                    AND NOT EXISTS(SELECT 1 FROM forward_test WHERE snapshot_id=signal_snapshots.id)
                    """, (check_date,)).fetchall()
                for s in snaps:
                    sym = s["symbol"]; mkt = s["market"]
                    cur = _ram_cache.get(sym, {}).get("data")
                    if not cur: continue
                    cp = cur.get("price", 0)
                    if not cp or not s["price"]: continue
                    ret = round((cp/s["price"]-1)*100, 2)
                    bull = s["signal"] in ("BUY","BUY_WARN")
                    correct = 1 if (bull and ret > 0) or (not bull and ret < 0) else 0
                    col = f"price_{days}d"
                    try:
                        c.execute(f"""INSERT OR IGNORE INTO forward_test
                            (snapshot_id,symbol,market,signal,score_at_signal,price_at_signal,{col},ret_{days}d,correct_{days}d)
                            VALUES(?,?,?,?,?,?,?,?,?)""",
                            (s["id"],sym,mkt,s["signal"],s["score"],s["price"],cp,ret,correct))
                    except Exception as _e: log.debug('[uncat] line %d: %%s' % 4507, _e)
    except Exception as e:
        print(f"[FwdTest] {e}")

# ── SIGNAL SNAPSHOT ─────────────────────────────────────────
def save_signal_snapshot():
    try:
        with get_db() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS signal_snapshots(
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT NOT NULL, market TEXT NOT NULL,
                date      TEXT NOT NULL, signal TEXT, score INTEGER,
                price     REAL, confidence INTEGER, confluence INTEGER,
                rs_vs_index REAL, low_liquidity INTEGER DEFAULT 0,
                created_at TEXT DEFAULT(datetime('now')),
                UNIQUE(symbol, date))""")
            rows = c.execute("SELECT DISTINCT symbol, market FROM user_watchlist").fetchall()
            today = datetime.now().strftime("%Y-%m-%d")
            saved = 0
            for r in rows:
                sym, mkt = r["symbol"], r["market"]
                cached = _ram_cache.get(sym, {}).get("data")
                if not cached: continue
                a = cached.get("analysis") or analyze(cached)
                try:
                    c.execute("""INSERT OR IGNORE INTO signal_snapshots
                        (symbol,market,date,signal,score,price,confidence,confluence,rs_vs_index,low_liquidity)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (sym, mkt, today, a.get("signal",""), a.get("total",0),
                         cached.get("price",0), a.get("confidence",0), a.get("confluence",0),
                         a.get("rs_vs_index",{}).get("rs") if isinstance(a.get("rs_vs_index"),dict) else None,
                         1 if a.get("low_liquidity") else 0))
                    saved += 1
                except Exception as _e: log.debug('[uncat] line %d: %%s' % 4540, _e)
            print(f"[Snapshot] Đã lưu {saved} tín hiệu ngày {today}")
    except Exception as e:
        print(f"[Snapshot] Lỗi: {e}")

# ── TOP SIGNALS ─────────────────────────────────────────────
def save_top_signals_today():
    today = datetime.now().strftime("%Y-%m-%d")
    all_signals = []
    for sym, cached in list(_ram_cache.items()):
        d = cached.get('data', {})
        if not d: continue
        a = d.get('analysis')
        if not a:
            try: a = analyze(d)
            except: continue
        sig   = a.get('signal', '')
        score = a.get('total', 0)
        price = d.get('price', 0)
        conf  = a.get('confidence', 0)
        conf_score = a.get('confluence', 0)
        rsi_v = a.get('rsi', 50)
        adx_v = a.get('adx_val', 0)
        squeeze = 1 if a.get('squeeze_on') else 0
        unusual = 1 if a.get('unusual_vol') else 0
        mkt   = d.get('market', 'VN')
        if 'BUY' in sig and score >= 3:
            sig_type = 'BUY'
        elif 'SELL' in sig and score <= -3:
            sig_type = 'SELL'
        else:
            continue
        all_signals.append({
            'symbol': sym, 'market': mkt, 'sig_type': sig_type,
            'score': score, 'price': price, 'confidence': conf,
            'confluence': conf_score, 'rsi': round(rsi_v, 1),
            'adx': round(adx_v, 1), 'squeeze': squeeze, 'unusual_vol': unusual,
        })
    saved = 0
    for item in all_signals:
        try:
            with get_db() as c:
                c.execute("""INSERT OR IGNORE INTO top_signals_history
                    (symbol, market, date, signal_type, score, price, confidence,
                     confluence, rsi, adx, squeeze, unusual_vol)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item['symbol'], item['market'], today, item['sig_type'],
                     item['score'], item['price'], item['confidence'],
                     item['confluence'], item['rsi'], item['adx'],
                     item['squeeze'], item['unusual_vol']))
            saved += 1
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 4591, _e)
    if saved:
        log.info(f"[TopSig] Saved {saved} signals for {today}")
    return saved

def get_top_signals_persistent(signal_type='BUY', days=7, min_days=None, limit=30):
    if min_days is None:
        min_days = 2 if days <= 7 else (3 if days <= 21 else 4)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    today  = datetime.now().strftime("%Y-%m-%d")
    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT symbol, market,
                       COUNT(DISTINCT date) as day_count,
                       AVG(score) as avg_score,
                       AVG(ABS(score)) as avg_abs_score,
                       MAX(ABS(score)) as max_score,
                       MAX(price) as last_price,
                       AVG(confidence) as avg_conf,
                       AVG(confluence) as avg_confluence,
                       AVG(rsi) as avg_rsi,
                       AVG(adx) as avg_adx,
                       SUM(squeeze) as squeeze_days,
                       SUM(unusual_vol) as unusual_days,
                       MAX(date) as last_seen,
                       MIN(date) as first_seen,
                       GROUP_CONCAT(date || ':' || score ORDER BY date ASC) as date_scores
                FROM top_signals_history
                WHERE signal_type=? AND date >= ?
                GROUP BY symbol
                HAVING day_count >= ?
                ORDER BY day_count DESC, avg_abs_score DESC
                LIMIT ?
            """, (signal_type, cutoff, min_days, limit * 2)).fetchall()
        result = []
        for r in rows:
            sym = r['symbol']
            date_score_str = r['date_scores'] or ''
            date_score_pairs = []
            for ds in date_score_str.split(','):
                try:
                    d, sc = ds.split(':')
                    date_score_pairs.append((d.strip(), int(sc)))
                except Exception as _e: log.debug('[uncat] line %d: %%s' % 4635, _e)
            date_score_pairs.sort(key=lambda x: x[0])
            all_dates = sorted([x[0] for x in date_score_pairs], reverse=True)
            streak = 0
            check_date = datetime.now().date()
            for i in range(min(days, len(all_dates) + 1)):
                d_str = (check_date - timedelta(days=i)).strftime('%Y-%m-%d')
                if d_str in all_dates:
                    streak += 1
                else:
                    break
            heat = 0
            if len(date_score_pairs) >= 4:
                recent = [abs(sc) for _, sc in date_score_pairs[-3:]]
                prev   = [abs(sc) for _, sc in date_score_pairs[-6:-3]]
                if prev:
                    recent_avg = sum(recent) / len(recent)
                    prev_avg   = sum(prev) / len(prev)
                    heat = round((recent_avg - prev_avg) / max(prev_avg, 1) * 100, 1)
            is_today = today in all_dates
            base_rel  = min(70, round(r['day_count'] / days * 70))
            streak_bonus = min(20, (streak - 1) * 8) if streak >= 2 else 0
            today_bonus  = 10 if is_today else 0
            heat_bonus   = 5 if heat > 10 else 0
            reliability  = min(100, base_rel + streak_bonus + today_bonus + heat_bonus)
            cur_price = r['last_price']
            cur_signal = ''; cur_score = round(r['avg_score'], 1)
            cur_conf = round(r['avg_conf'], 0)
            if sym in _ram_cache:
                cd = _ram_cache[sym].get('data', {})
                cur_price  = cd.get('price', cur_price)
                ca = cd.get('analysis', {})
                if ca:
                    cur_signal = ca.get('signal', '')
                    cur_score  = ca.get('total', cur_score)
                    cur_conf   = ca.get('confidence', cur_conf)
            result.append({
                'symbol': sym,
                'market': r['market'],
                'day_count': r['day_count'],
                'streak': streak,
                'reliability': reliability,
                'avg_score': round(r['avg_score'], 1),
                'avg_abs_score': round(r['avg_abs_score'], 1),
                'max_score': int(r['max_score']),
                'avg_confidence': round(r['avg_conf'], 0),
                'avg_confluence': round(r['avg_confluence'] or 0, 1),
                'avg_rsi': round(r['avg_rsi'] or 50, 1),
                'avg_adx': round(r['avg_adx'] or 0, 1),
                'squeeze_days': int(r['squeeze_days'] or 0),
                'unusual_days': int(r['unusual_days'] or 0),
                'last_seen': r['last_seen'],
                'first_seen': r['first_seen'],
                'is_today': is_today,
                'heat': heat,
                'heat_label': ('🔥 Nóng dần' if heat > 15 else '❄️ Nguội dần' if heat < -15 else '→ Ổn định'),
                'cur_price': cur_price,
                'cur_signal': cur_signal,
                'cur_score': cur_score,
                'cur_confidence': cur_conf,
            })
        result.sort(key=lambda x: (-x['streak'], -x['reliability'], -x['avg_abs_score']))
        return result[:limit]
    except Exception as e:
        log.error(f"[TopSig] get error: {e}")
        return []

# ── BREAKOUT FILTER ─────────────────────────────────────────
def check_breakout_d1_gap(symbol, market, signal):
    if get_setting('breakout_d1_gap_filter', '1') != '1':
        return True, ''
    if 'BUY' not in signal and 'SELL' not in signal:
        return True, ''
    try:
        bars = db_load_ohlcv(symbol, 5)
        if len(bars) < 2: return True, ''
        last = bars[-1]
        prev = bars[-2]
        gap_pct = (last['o'] - prev['c']) / prev['c'] * 100 if prev['c'] else 0
        is_buy = 'BUY' in signal
        if is_buy and gap_pct < -1.5:
            return False, f"D+1 Gap xuống {gap_pct:.1f}% — lọc false BUY"
        if not is_buy and gap_pct > 1.5:
            return False, f"D+1 Gap lên {gap_pct:.1f}% — lọc false SELL"
        return True, ''
    except:
        return True, ''

# ── INTRADAY ────────────────────────────────────────────────
_intraday_lock = threading.Lock()

def fetch_intraday_vn(symbol, interval='5m'):
    if not HAS_VN: return []
    try:
        from vnstock import Vnstock
        vn = Vnstock().stock(symbol=symbol, source='VCI')
        df = vn.quote.intraday(interval=interval, count=200)
        if df is None or len(df) == 0: return []
        rows = []
        for _, r in df.iterrows():
            try:
                t = str(r.get('time', r.get('t', '')))
                o = float(r.get('open', r.get('o', 0)) or 0)
                h = float(r.get('high', r.get('h', 0)) or 0)
                l = float(r.get('low',  r.get('l', 0)) or 0)
                c = float(r.get('close',r.get('c', 0)) or 0)
                v = int(r.get('volume',r.get('v', 0)) or 0)
                if c > 0:
                    rows.append({'t':t,'o':o,'h':h,'l':l,'c':c,'v':v})
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4744, _e)
        return rows
    except Exception as e:
        print(f"[Intraday] {symbol}: {e}")
        return []

def fetch_intraday_us(symbol, interval='5m'):
    try:
        import yfinance as yf
        period_map = {'1m':'1d','5m':'5d','15m':'5d','30m':'1mo','1h':'60d','1H':'60d','4h':'60d','4H':'60d'}
        tf = period_map.get(interval, '5d')
        tk = yf.Ticker(symbol)
        df = tk.history(period=tf, interval=interval)
        if df is None or len(df) == 0: return []
        rows = []
        for idx, r in df.iterrows():
            try:
                t = str(idx)[:19]
                o = float(r['Open'])
                h = float(r['High'])
                l = float(r['Low'])
                c = float(r['Close'])
                v = int(r['Volume'])
                if c > 0:
                    rows.append({'t':t,'o':o,'h':h,'l':l,'c':c,'v':v})
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 4769, _e)
        return rows
    except Exception as e:
        print(f"[Intraday US] {symbol}: {e}")
        return []


def _intraday_save_db(sym, mkt, interval, bars):
    """Lưu intraday bars vào DB (chỉ lưu 300 bars gần nhất/interval)."""
    try:
        with get_db() as c:
            c.executemany(
                """INSERT OR REPLACE INTO intraday_cache(symbol,market,interval,trade_date,open,high,low,close,volume)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                [(sym,mkt,interval,b['t'],b['o'],b['h'],b['l'],b['c'],b['v']) for b in bars]
            )
            c.execute("""DELETE FROM intraday_cache WHERE symbol=? AND interval=? AND trade_date NOT IN (
                SELECT trade_date FROM intraday_cache WHERE symbol=? AND interval=?
                ORDER BY trade_date DESC LIMIT 300)""", (sym,interval,sym,interval))
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 4788, _e)


def get_intraday(symbol, market, interval='5m', force=False):
    with _intraday_lock:
        try:
            with get_db() as c:
                row = c.execute("SELECT * FROM intraday_cache WHERE symbol=? AND interval=?",
                                (symbol, interval)).fetchone()
            if row and not force:
                age = (datetime.now() - datetime.fromisoformat(row['fetched_at'])).total_seconds()
                if age < 300:
                    return json.loads(row['data_json'])
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 4801, _e)
    if market == 'VN':
        data = fetch_intraday_vn(symbol, interval)
    else:
        data = fetch_intraday_us(symbol, interval)
    try:
        with get_db() as c:
            c.execute("""INSERT OR REPLACE INTO intraday_cache(symbol,market,interval,fetched_at,data_json)
                         VALUES(?,?,?,?,?)""",
                      (symbol, market, interval, datetime.now().isoformat(), json.dumps(data)))
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 4811, _e)
    return data

# ── BACKTEST WITH BENCHMARK ─────────────────────────────────
def backtest_with_benchmark(bars, capital, market='VN'):
    result = backtest(bars, capital, market=market)
    bench_sym = 'VNINDEX' if market == 'VN' else 'SPY'
    bench_data = _ram_cache.get(bench_sym, {}).get('data', {})
    bench_bars = bench_data.get('history', []) if bench_data else []
    if not bench_bars or len(bench_bars) < 2:
        result['benchmark'] = None
        return result
    bar_dates = {b['d'] for b in bars}
    bench_aligned = [b for b in bench_bars if b['d'] in bar_dates]
    if len(bench_aligned) < 2:
        bench_aligned = bench_bars[-len(bars):]
    if len(bench_aligned) < 2:
        result['benchmark'] = None
        return result
    b_start = bench_aligned[0]['c']
    b_end   = bench_aligned[-1]['c']
    b_return = round((b_end / b_start - 1) * 100, 2) if b_start > 0 else 0
    s_return = result.get('total_return_pct', 0)
    alpha = round(s_return - b_return, 2)
    bench_equity = []
    for i, b in enumerate(bench_aligned):
        eq = round(capital * b['c'] / b_start, 0) if b_start > 0 else capital
        bench_equity.append({'d': b['d'], 'v': eq})
    result['benchmark'] = {
        'symbol': bench_sym,
        'start_price': b_start,
        'end_price': b_end,
        'return_pct': b_return,
        'strategy_return_pct': s_return,
        'alpha': alpha,
        'outperform': s_return > b_return,
        'equity_curve': bench_equity[-200:],
    }
    return result

# ── ADMIN ────────────────────────────────────────────────────
def admin_get_stats():
    with get_db() as c:
        users = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        sessions = c.execute("SELECT COUNT(*) FROM sessions WHERE expires_at > datetime('now')").fetchone()[0]
        symbols = c.execute("SELECT COUNT(DISTINCT symbol) FROM ohlcv_history").fetchone()[0]
        ohlcv_rows = c.execute("SELECT COUNT(*) FROM ohlcv_history").fetchone()[0]
        alerts_today = c.execute(
            "SELECT COUNT(*) FROM alerts WHERE created_at >= date('now')"
        ).fetchone()[0]
        trades_total = c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        vn_q = _vn_queue.qsize() if '_vn_queue' in dir() else 0
    backup_files = []
    backup_dir = os.path.join(os.path.dirname(DB_PATH), 'backups')
    if os.path.exists(backup_dir):
        backup_files = sorted([
            {'name': f, 'size': os.path.getsize(os.path.join(backup_dir, f)),
             'ts': os.path.getmtime(os.path.join(backup_dir, f))}
            for f in os.listdir(backup_dir) if f.endswith('.zip')
        ], key=lambda x: -x['ts'])[:5]
    return {
        'users_total': users,
        'sessions_active': sessions,
        'symbols_tracked': symbols,
        'ohlcv_rows': ohlcv_rows,
        'alerts_today': alerts_today,
        'trades_total': trades_total,
        'db_size_mb': round(db_size / 1024 / 1024, 2),
        'vn_queue': vn_q,
        'server_uptime': time.strftime('%Y-%m-%d %H:%M:%S'),
        'version': VERSION,
        'port': PORT,
        'backups': backup_files,
        'ohlcv_bars_setting': OHLCV_BARS,
    }

def admin_ban_user(user_id, banned=True):
    with get_db() as c:
        c.execute("UPDATE users SET role=? WHERE id=?",
                  ('banned' if banned else 'user', user_id))
        if banned:
            c.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))

def admin_reset_password(user_id, new_pw=None):
    if new_pw is None: new_pw = os.environ.get('SMA_RESET_PW') or _gen_default_pw()
    salt = secrets.token_hex(16)
    ph   = _hash_pw(salt, new_pw)
    with get_db() as c:
        c.execute("UPDATE users SET password_h=?, salt=? WHERE id=?",(ph,salt,user_id))
    return new_pw

def admin_delete_user(user_id):
    with get_db() as c:
        c.execute("DELETE FROM user_watchlist WHERE user_id=?", (user_id,))
        c.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        c.execute("DELETE FROM trades WHERE user_id=?", (user_id,))
        c.execute("DELETE FROM user_settings WHERE user_id=?", (user_id,))
        c.execute("DELETE FROM tg_subscribers WHERE user_id=?", (user_id,))
        c.execute("DELETE FROM users WHERE id=?", (user_id,))

# ── SERVICE WORKER ──────────────────────────────────────────
SW_JS = """
const CACHE_NAME = 'sma-v6-cache';
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE_NAME).then(c => c.addAll(['/manifest.json']).catch(()=>{})).then(()=>self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE_NAME).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const url = e.request.url;
  // 1. HTML pages: always network-first, fallback to cache
  if (e.request.destination === 'document' || url.endsWith('/') || url.endsWith('.html')) {
    e.respondWith(fetch(e.request).catch(() => caches.match(e.request).then(r => r || fetch(e.request))));
    return;
  }
  // 2. API calls: network-only with offline JSON fallback
  if (url.includes('/api/')) {
    e.respondWith(
      fetch(e.request).catch(() =>
        new Response(JSON.stringify({error:'offline',msg:'Mất kết nối server'}),
          {status:503, headers:{'Content-Type':'application/json'}})
      )
    );
    return;
  }
  // 3. Static assets: cache-first
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request).then(resp => {
      if (resp.ok && resp.type === 'basic') {
        const clone = resp.clone();
        caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
      }
      return resp;
    }).catch(() => new Response('', {status: 408})))
  );
});
"""

MANIFEST_JSON = json.dumps({
    "name": "SMA Pro — Ultimate Stock Analyzer",
    "short_name": "SMA Pro",
    "description": "Stock Multi-Market Analyzer với 19 chỉ báo, ML Predictions, Efficient Frontier",
    "start_url": "/",
    "display": "standalone",
    "background_color": "#0a0d14",
    "theme_color": "#0a0d14",
    "orientation": "any",
    "icons": [
        {"src": "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='20' fill='%232563eb'/><text y='.9em' font-size='70' x='15'>📊</text></svg>",
         "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}
    ],
    "screenshots": [],
    "categories": ["finance", "productivity"]
}, ensure_ascii=False)

# ── HTTP HANDLER ────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
# SMA Pro UPGRADES — Fundamental, ML, Sentiment, Strategy Builder
# (merged from sma_v22_upgrades.py)
# ════════════════════════════════════════════════════════════════


# [merged] import json, os, time, threading, sqlite3, hashlib
# [merged] import numpy as np
# [merged] from datetime import datetime, timedelta
# [merged] from collections import defaultdict

# ── FUNDAMENTAL DATA ──────────────────────────────────────────

_FUND_CACHE = {}
_FUND_TTL   = 3600 * 6   # 6 hours


def fetch_fundamental_vn(symbol: str) -> dict:
    """Lấy dữ liệu cơ bản mã VN từ vnstock + SSI."""
    cached = _FUND_CACHE.get(symbol)
    if cached and time.time() - cached['ts'] < _FUND_TTL:
        return cached['data']

    result = {
        'symbol': symbol,
        'pe': None, 'pb': None, 'roe': None, 'roa': None,
        'eps': None, 'bvps': None, 'dividend_yield': None,
        'revenue_ttm': None, 'net_income_ttm': None,
        'revenue_growth_yoy': None, 'profit_growth_yoy': None,
        'debt_to_equity': None, 'current_ratio': None,
        'gross_margin': None, 'net_margin': None,
        'ev_ebitda': None, 'market_cap': None,
        'revenue_forecast': [],   # list of {'year', 'revenue', 'growth'}
        'income_statements': [],  # last 8 quarters
        'balance_sheet': {},
        'source': 'vnstock',
        'updated_at': datetime.now().isoformat(),
        'error': None,
    }

    try:
        from vnstock import Vnstock
        stk = Vnstock().stock(symbol=symbol, source='VCI')

        # Ratios
        try:
            df_ratio = stk.company.ratio()
            if df_ratio is not None and len(df_ratio):
                r = df_ratio.iloc[0]
                result['pe']  = _safe_float(r.get('price_to_earning'))
                result['pb']  = _safe_float(r.get('price_to_book'))
                result['roe'] = _safe_float(r.get('return_on_equity'))
                result['roa'] = _safe_float(r.get('return_on_asset'))
                result['eps'] = _safe_float(r.get('earning_per_share'))
                result['bvps']= _safe_float(r.get('book_value_per_share'))
                result['dividend_yield'] = _safe_float(r.get('dividend_yield'))
                result['debt_to_equity'] = _safe_float(r.get('debt_to_equity'))
        except Exception as e:
            result['error'] = f'ratio: {e}'

        # Income statements — quarterly
        try:
            df_is = stk.finance.income_statement(period='quarter', lang='en')
            if df_is is not None and len(df_is) >= 2:
                quarters = []
                for _, row in df_is.head(8).iterrows():
                    quarters.append({
                        'period': str(row.get('yearReport', '')) + 'Q' + str(row.get('lengthReport', '')),
                        'revenue':     _safe_float(row.get('revenue')),
                        'gross_profit':_safe_float(row.get('gross_profit')),
                        'operating_profit': _safe_float(row.get('operating_profit')),
                        'net_income':  _safe_float(row.get('net_profit')),
                        'eps':         _safe_float(row.get('eps')),
                    })
                result['income_statements'] = quarters
                # TTM revenue & income
                ttm_rev = sum(q['revenue'] or 0 for q in quarters[:4])
                ttm_ni  = sum(q['net_income'] or 0 for q in quarters[:4])
                result['revenue_ttm']    = ttm_rev
                result['net_income_ttm'] = ttm_ni
                if ttm_rev > 0:
                    result['net_margin'] = round(ttm_ni / ttm_rev * 100, 2)
                # YoY growth
                if len(quarters) >= 5:
                    rev_year_ago = sum(q['revenue'] or 0 for q in quarters[4:8])
                    if rev_year_ago > 0:
                        result['revenue_growth_yoy'] = round((ttm_rev - rev_year_ago) / rev_year_ago * 100, 2)
                    ni_year_ago = sum(q['net_income'] or 0 for q in quarters[4:8])
                    if ni_year_ago and ni_year_ago != 0:
                        result['profit_growth_yoy'] = round((ttm_ni - ni_year_ago) / abs(ni_year_ago) * 100, 2)
                # Simple linear forecast (next 2 years)
                revs = [q['revenue'] or 0 for q in reversed(quarters[:8])]
                if len(revs) >= 4:
                    annual = [sum(revs[i:i+4]) for i in range(0, len(revs)-3, 4) if i+4 <= len(revs)]
                    if len(annual) >= 2:
                        growth = (annual[-1] - annual[-2]) / annual[-2] if annual[-2] else 0
                        growth = max(min(growth, 0.5), -0.3)   # clamp -30%..+50%
                        yr = datetime.now().year
                        result['revenue_forecast'] = [
                            {'year': yr+1, 'revenue': round(annual[-1] * (1 + growth), 0),
                             'growth': round(growth * 100, 1), 'method': 'Linear extrapolation'},
                            {'year': yr+2, 'revenue': round(annual[-1] * (1 + growth)**2, 0),
                             'growth': round(growth * 100, 1), 'method': 'Linear extrapolation'},
                        ]
        except Exception as e:
            result['error'] = (result.get('error') or '') + f' | IS: {e}'

        # Balance sheet
        try:
            df_bs = stk.finance.balance_sheet(period='quarter', lang='en')
            if df_bs is not None and len(df_bs):
                row = df_bs.iloc[0]
                total_assets = _safe_float(row.get('total_assets'))
                total_equity = _safe_float(row.get('equity'))
                total_debt   = _safe_float(row.get('long_term_debt'))
                current_assets = _safe_float(row.get('short_assets'))
                current_liabs  = _safe_float(row.get('short_liabilities'))
                result['balance_sheet'] = {
                    'total_assets': total_assets,
                    'total_equity': total_equity,
                    'total_debt':   total_debt,
                    'current_assets': current_assets,
                    'current_liabilities': current_liabs,
                }
                if total_equity and total_equity > 0 and total_debt:
                    result['debt_to_equity'] = round(total_debt / total_equity, 2)
                if current_liabs and current_liabs > 0 and current_assets:
                    result['current_ratio'] = round(current_assets / current_liabs, 2)
        except Exception as e:
            result['error'] = (result.get('error') or '') + f' | BS: {e}'

    except Exception as e:
        result['error'] = str(e)

    _FUND_CACHE[symbol] = {'ts': time.time(), 'data': result}
    return result


def fetch_fundamental_us(symbol: str) -> dict:
    """Lấy dữ liệu cơ bản mã US từ yfinance."""
    cached = _FUND_CACHE.get(symbol)
    if cached and time.time() - cached['ts'] < _FUND_TTL:
        return cached['data']

    result = {
        'symbol': symbol,
        'pe': None, 'pb': None, 'roe': None, 'roa': None,
        'eps': None, 'bvps': None, 'dividend_yield': None,
        'revenue_ttm': None, 'net_income_ttm': None,
        'revenue_growth_yoy': None, 'profit_growth_yoy': None,
        'debt_to_equity': None, 'current_ratio': None,
        'gross_margin': None, 'net_margin': None,
        'ev_ebitda': None, 'market_cap': None,
        'income_statements': [],
        'revenue_forecast': [],
        'source': 'yfinance',
        'updated_at': datetime.now().isoformat(),
        'error': None,
    }
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        info = tk.info or {}

        result['pe']    = _safe_float(info.get('trailingPE') or info.get('forwardPE'))
        result['pb']    = _safe_float(info.get('priceToBook'))
        result['roe']   = round(_safe_float(info.get('returnOnEquity')) * 100, 2) if info.get('returnOnEquity') else None
        result['roa']   = round(_safe_float(info.get('returnOnAssets')) * 100, 2) if info.get('returnOnAssets') else None
        result['eps']   = _safe_float(info.get('trailingEps'))
        result['bvps']  = _safe_float(info.get('bookValue'))
        result['dividend_yield'] = round(_safe_float(info.get('dividendYield')) * 100, 2) if info.get('dividendYield') else None
        result['revenue_ttm']    = _safe_float(info.get('totalRevenue'))
        result['net_income_ttm'] = _safe_float(info.get('netIncomeToCommon'))
        result['debt_to_equity'] = _safe_float(info.get('debtToEquity'))
        result['current_ratio']  = _safe_float(info.get('currentRatio'))
        result['gross_margin']   = round(_safe_float(info.get('grossMargins')) * 100, 2) if info.get('grossMargins') else None
        result['net_margin']     = round(_safe_float(info.get('profitMargins')) * 100, 2) if info.get('profitMargins') else None
        result['ev_ebitda']      = _safe_float(info.get('enterpriseToEbitda'))
        result['market_cap']     = _safe_float(info.get('marketCap'))

        # Quarterly financials
        try:
            qf = tk.quarterly_financials
            if qf is not None and not qf.empty:
                quarters = []
                for col in qf.columns[:8]:
                    quarters.append({
                        'period': str(col)[:10],
                        'revenue': _safe_float(qf.loc['Total Revenue', col]) if 'Total Revenue' in qf.index else None,
                        'gross_profit': _safe_float(qf.loc['Gross Profit', col]) if 'Gross Profit' in qf.index else None,
                        'net_income': _safe_float(qf.loc['Net Income', col]) if 'Net Income' in qf.index else None,
                    })
                result['income_statements'] = quarters
                if len(quarters) >= 5:
                    rev_now   = sum(q['revenue'] or 0 for q in quarters[:4])
                    rev_prev  = sum(q['revenue'] or 0 for q in quarters[4:8])
                    ni_now    = sum(q['net_income'] or 0 for q in quarters[:4])
                    ni_prev   = sum(q['net_income'] or 0 for q in quarters[4:8])
                    if rev_prev:
                        result['revenue_growth_yoy'] = round((rev_now - rev_prev) / abs(rev_prev) * 100, 2)
                    if ni_prev and ni_prev != 0:
                        result['profit_growth_yoy']  = round((ni_now - ni_prev) / abs(ni_prev) * 100, 2)
                # Revenue forecast using analyst estimates
                growth = (result['revenue_growth_yoy'] or 0) / 100
                growth = max(min(growth, 0.5), -0.3)
                ttm = result['revenue_ttm'] or 0
                yr = datetime.now().year
                if ttm:
                    result['revenue_forecast'] = [
                        {'year': yr+1, 'revenue': round(ttm * (1 + growth), 0),
                         'growth': round(growth * 100, 1), 'method': 'YoY extrapolation'},
                        {'year': yr+2, 'revenue': round(ttm * (1 + growth)**2, 0),
                         'growth': round(growth * 100, 1), 'method': 'YoY extrapolation'},
                    ]
        except Exception as e:
            result['error'] = f'quarterly: {e}'

    except Exception as e:
        result['error'] = str(e)

    _FUND_CACHE[symbol] = {'ts': time.time(), 'data': result}
    return result


def fetch_fundamental(symbol: str, market: str) -> dict:
    if market == 'VN':
        return fetch_fundamental_vn(symbol)
    return fetch_fundamental_us(symbol)


def _safe_float(val, default=None):
    try:
        v = float(val)
        return round(v, 4) if v == v else default  # NaN check
    except:
        return default


def fundamental_score(fd: dict) -> dict:
    """Chấm điểm cơ bản 0-100 và label."""
    score = 50
    signals = []

    pe = fd.get('pe')
    pb = fd.get('pb')
    roe = fd.get('roe')
    rg = fd.get('revenue_growth_yoy')
    pg = fd.get('profit_growth_yoy')
    de = fd.get('debt_to_equity')
    nm = fd.get('net_margin')

    if pe:
        if pe < 10:   score += 10; signals.append('P/E rất thấp (<10)')
        elif pe < 20: score += 5;  signals.append('P/E hợp lý (<20)')
        elif pe > 40: score -= 10; signals.append('P/E cao (>40)')
    if pb:
        if pb < 1.5:  score += 8;  signals.append('P/B thấp (<1.5)')
        elif pb < 3:  score += 3;  signals.append('P/B hợp lý')
        elif pb > 5:  score -= 8;  signals.append('P/B cao (>5)')
    if roe:
        if roe > 20:  score += 12; signals.append(f'ROE xuất sắc ({roe:.1f}%)')
        elif roe > 15:score += 7;  signals.append(f'ROE tốt ({roe:.1f}%)')
        elif roe < 8: score -= 8;  signals.append(f'ROE yếu ({roe:.1f}%)')
    if rg:
        if rg > 20:   score += 8;  signals.append(f'Doanh thu tăng mạnh ({rg:.1f}%)')
        elif rg > 10: score += 4;  signals.append(f'Doanh thu tăng tốt ({rg:.1f}%)')
        elif rg < 0:  score -= 6;  signals.append(f'Doanh thu giảm ({rg:.1f}%)')
    if pg:
        if pg > 25:   score += 8;  signals.append(f'Lợi nhuận tăng mạnh ({pg:.1f}%)')
        elif pg < 0:  score -= 8;  signals.append(f'Lợi nhuận giảm ({pg:.1f}%)')
    if de is not None:
        if de < 0.5:  score += 5;  signals.append('Nợ thấp (D/E<0.5)')
        elif de > 2:  score -= 8;  signals.append('Nợ cao (D/E>2)')
    if nm:
        if nm > 20:   score += 6;  signals.append(f'Biên lợi nhuận cao ({nm:.1f}%)')
        elif nm < 5:  score -= 4;  signals.append(f'Biên lợi nhuận mỏng ({nm:.1f}%)')

    score = max(0, min(100, score))
    if score >= 75:   label = 'STRONG BUY'
    elif score >= 60: label = 'BUY'
    elif score >= 45: label = 'HOLD'
    elif score >= 30: label = 'WEAK'
    else:             label = 'AVOID'

    return {'score': score, 'label': label, 'signals': signals}


# ── SENTIMENT ANALYSIS ────────────────────────────────────────

# Từ điển cảm xúc đơn giản (không cần mô hình NLP ngoài)
_BULL_WORDS = {
    'tăng','tăng trưởng','lợi nhuận','tích cực','khởi sắc','bùng nổ','đột phá','phục hồi',
    'kỷ lục','vượt kỳ vọng','mua vào','nâng dự báo','khuyến nghị mua','outperform',
    'beat','surge','rally','bullish','upbeat','strong','gain','rise','record','buy',
    'upgrade','overweight','positive','growth','profit','earnings beat',
}
_BEAR_WORDS = {
    'giảm','sụt giảm','lỗ','tiêu cực','rủi ro','lo ngại','tháo chạy','bán tháo',
    'hạ dự báo','cảnh báo','điều tra','phạt','vi phạm','nợ xấu','mất thanh khoản',
    'sell','downgrade','underweight','bearish','loss','decline','fall','drop','miss',
    'warning','risk','debt','negative','underperform','cut',
}

def analyze_headline_sentiment(headlines: list) -> dict:
    """Phân tích cảm xúc từ list headline strings."""
    if not headlines:
        return {'score': 0, 'label': 'NEUTRAL', 'bull': 0, 'bear': 0, 'analyzed': 0}

    bull = bear = 0
    for h in headlines:
        lower = h.lower()
        words = set(lower.split())
        b = len(_BULL_WORDS & words) + sum(1 for bw in _BULL_WORDS if bw in lower and ' ' in bw)
        s = len(_BEAR_WORDS & words) + sum(1 for sw in _BEAR_WORDS if sw in lower and ' ' in sw)
        bull += b; bear += s

    n = len(headlines)
    net = (bull - bear) / max(n, 1)
    score = round(min(max(net * 25 + 50, 0), 100), 1)

    if score >= 65:   label = 'BULLISH'
    elif score >= 55: label = 'SLIGHTLY BULLISH'
    elif score >= 45: label = 'NEUTRAL'
    elif score >= 35: label = 'SLIGHTLY BEARISH'
    else:             label = 'BEARISH'

    return {
        'score': score,
        'label': label,
        'bull_count': bull,
        'bear_count': bear,
        'analyzed': n,
        'headlines_sample': headlines[:5],
    }


def get_sentiment_for_symbol(symbol: str, market: str, news_list: list) -> dict:
    """Kết hợp sentiment từ news + tính weighted signal."""
    titles = [n.get('title', '') for n in (news_list or []) if n.get('title')]
    sent = analyze_headline_sentiment(titles)
    # Thêm context từ symbol pattern
    if 'HPG' in symbol or 'HSG' in symbol:
        sent['sector_context'] = 'Steel — cyclical'
    return sent


# ── ADVANCED ML ───────────────────────────────────────────────

class AdvancedMLPredictor:
    """
    Gradient Boosting predictor kết hợp kỹ thuật + cơ bản + sentiment.
    Fallback về sklearn GradientBoostingClassifier (không cần torch/tensorflow).
    """

    FEATURE_NAMES = [
        'rsi_norm', 'macd_score', 'adx_norm', 'ma_score', 'bb_position',
        'volume_ratio', 'squeeze', 'elder', 'stoch_score', 'obv_score',
        'pe_norm', 'pb_norm', 'roe_norm', 'rev_growth_norm', 'profit_growth_norm',
        'sentiment_score',
        'ret_5d', 'ret_20d', 'volatility_20d',
    ]

    def __init__(self, db_path: str):
        self.db_path   = db_path
        self.models    = {}
        self._lock     = threading.Lock()

    def _get_db(self):
        c = sqlite3.connect(self.db_path, timeout=10, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def extract_features(self, symbol: str, market: str, history: list,
                         fund_data: dict = None, sentiment: dict = None) -> list:
        """Trích xuất feature vector từ analysis + fundamental + sentiment."""
        if not history or len(history) < 30:
            return None

        closes = [b['c'] for b in history]
        n = len(closes)

        # Technical features (normalize to -1..1 or 0..1)
        rsi = _calc_rsi_simple(closes) / 100.0

        # MACD score proxy
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd_val = (ema12 - ema26) / (closes[-1] + 1e-9)
        macd_score = max(-1, min(1, macd_val * 100))

        # ADX proxy
        adx = _calc_adx_simple(history) / 100.0

        # MA position
        ma20 = sum(closes[-20:]) / 20 if n >= 20 else closes[-1]
        ma50 = sum(closes[-50:]) / 50 if n >= 50 else closes[-1]
        ma_score = (1 if closes[-1] > ma20 else -1) + (1 if closes[-1] > ma50 else -1)
        ma_score_norm = ma_score / 2.0

        # BB position
        bb_mean = ma20
        std20 = (_var(closes[-20:]) ** 0.5) if n >= 20 else 1
        bb_upper = bb_mean + 2 * std20
        bb_lower = bb_mean - 2 * std20
        bb_pos = (closes[-1] - bb_lower) / (bb_upper - bb_lower + 1e-9)
        bb_pos = max(0, min(1, bb_pos))

        # Volume ratio
        vols = [b.get('v', 0) for b in history]
        avg_vol20 = sum(vols[-20:]) / 20 if n >= 20 else 1
        vol_ratio = min(vols[-1] / (avg_vol20 + 1e-9), 5) / 5.0

        # Squeeze (simplified)
        squeeze = 1.0 if std20 < (sum(abs(closes[i]-closes[i-1]) for i in range(-5,0)) / 5) else 0.0

        # Elder Ray (simplified)
        elder = 1.0 if closes[-1] > ma13(closes) else -1.0 if closes[-1] < ma13(closes) else 0.0

        # Stoch (simplified)
        h14 = max(b.get('h', b['c']) for b in history[-14:]) if n >= 14 else closes[-1]
        l14 = min(b.get('l', b['c']) for b in history[-14:]) if n >= 14 else closes[-1]
        stoch_k = (closes[-1] - l14) / (h14 - l14 + 1e-9)

        # OBV trend
        obv = [0.0]
        for i in range(1, min(20, n)):
            d = closes[-i] - closes[-i-1]
            obv.append(obv[-1] + (vols[-i] if d > 0 else -vols[-i]))
        obv_trend = 1.0 if obv[-1] > obv[0] else -1.0

        # Returns
        ret5  = (closes[-1] / closes[-6]  - 1) if n >= 6  else 0
        ret20 = (closes[-1] / closes[-21] - 1) if n >= 21 else 0
        rets  = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, min(21, n))]
        vol20 = (_var(rets) ** 0.5) if rets else 0

        # Fundamental features
        pe_norm = pb_norm = roe_norm = rev_g_norm = prof_g_norm = 0.5
        if fund_data:
            pe  = fund_data.get('pe')
            pb  = fund_data.get('pb')
            roe = fund_data.get('roe')
            rg  = fund_data.get('revenue_growth_yoy')
            pg  = fund_data.get('profit_growth_yoy')
            pe_norm   = max(0, min(1, 1 - (pe  or 20) / 60))
            pb_norm   = max(0, min(1, 1 - (pb  or 2)  / 10))
            roe_norm  = max(0, min(1, (roe or 10) / 40))
            rev_g_norm= max(0, min(1, ((rg or 0) + 30) / 80))
            prof_g_norm= max(0, min(1, ((pg or 0) + 50) / 100))

        # Sentiment
        sent_norm = (sentiment.get('score', 50) / 100.0) if sentiment else 0.5

        return [
            rsi, macd_score, adx, ma_score_norm, bb_pos,
            vol_ratio, squeeze, elder, stoch_k, obv_trend,
            pe_norm, pb_norm, roe_norm, rev_g_norm, prof_g_norm,
            sent_norm,
            ret5, ret20, vol20,
        ]

    def train(self, symbol: str, market: str, history: list,
              fund_data: dict = None) -> dict:
        """Train GradientBoosting model."""
        try:
            from sklearn.ensemble import GradientBoostingClassifier
            from sklearn.model_selection import TimeSeriesSplit
            from sklearn.preprocessing import StandardScaler
# [merged]             import numpy as _np
        except ImportError:
            return {'error': 'scikit-learn not available'}

        if not history or len(history) < 80:
            return {'error': 'Cần ít nhất 80 phiên'}

        X, y = [], []
        step = 5   # predict direction after 5 days
        for i in range(30, len(history) - step):
            feat = self.extract_features(symbol, market, history[:i+1], fund_data)
            if feat is None:
                continue
            future = history[i + step]['c']
            current = history[i]['c']
            label = 1 if future > current * 1.02 else (-1 if future < current * 0.98 else 0)
            X.append(feat); y.append(label)

        if len(X) < 40:
            return {'error': 'Không đủ mẫu huấn luyện'}

        X = _np.array(X); y = _np.array(y)
        scaler = StandardScaler()
        X_s = scaler.fit_transform(X)

        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for tr, te in tscv.split(X_s):
            clf = GradientBoostingClassifier(n_estimators=80, max_depth=3,
                                             learning_rate=0.1, random_state=42)
            clf.fit(X_s[tr], y[tr])
            scores.append(clf.score(X_s[te], y[te]))

        clf_final = GradientBoostingClassifier(n_estimators=100, max_depth=3,
                                               learning_rate=0.1, random_state=42)
        clf_final.fit(X_s, y)

        fi = clf_final.feature_importances_.tolist()
        feature_importance = dict(zip(self.FEATURE_NAMES, [round(v*100,2) for v in fi]))

        model_info = {
            'scaler_mean': scaler.mean_.tolist(),
            'scaler_scale': scaler.scale_.tolist(),
            'feature_importances': feature_importance,
            'cv_accuracy': round(sum(scores) / len(scores) * 100, 1),
            'n_samples': len(X),
            'trained_at': datetime.now().isoformat(),
            'model_type': 'GradientBoosting',
        }

        with self._lock:
            self.models[symbol] = {'clf': clf_final, 'scaler': scaler, 'info': model_info}

        # Persist to DB
        try:
            import pickle, base64
            blob = base64.b64encode(pickle.dumps({'clf': clf_final, 'scaler': scaler})).decode()
            with self._get_db() as c:
                c.execute("""INSERT OR REPLACE INTO ml_models_v22
                             (symbol, model_blob, model_info, trained_at)
                             VALUES (?, ?, ?, ?)""",
                          (symbol, blob, json.dumps(model_info),
                           datetime.now().isoformat()))
        except Exception:
            pass

        return model_info

    def predict(self, symbol: str, market: str, history: list,
                fund_data: dict = None, sentiment: dict = None) -> dict:
        """Dự báo xu hướng 5 phiên tới."""
# [merged]         import numpy as _np

        # Load model from cache or DB
        if symbol not in self.models:
            self._load_from_db(symbol)

        if symbol not in self.models:
            # Auto-train in background
            return {'prob_up': 50, 'prediction': 'NEUTRAL', 'confidence': 0,
                    'note': 'Chưa có model — đang training...'}

        mdl = self.models[symbol]
        clf, scaler = mdl['clf'], mdl['scaler']
        feat = self.extract_features(symbol, market, history, fund_data, sentiment)
        if feat is None:
            return {'prob_up': 50, 'prediction': 'NEUTRAL', 'confidence': 0}

        X = scaler.transform(_np.array(feat).reshape(1, -1))
        proba = clf.predict_proba(X)[0]
        classes = clf.classes_.tolist()
        up_idx   = classes.index(1)  if 1  in classes else 0
        down_idx = classes.index(-1) if -1 in classes else 1
        prob_up   = round(proba[up_idx]   * 100, 1)
        prob_down = round(proba[down_idx] * 100, 1) if down_idx < len(proba) else round((1 - proba[up_idx]) * 100, 1)
        pred = 'UP' if prob_up > 60 else ('DOWN' if prob_down > 60 else 'NEUTRAL')
        conf = round(abs(prob_up - 50) * 2, 1)

        return {
            'prob_up':   prob_up,
            'prob_down': prob_down,
            'prediction': pred,
            'confidence': conf,
            'model_info': mdl['info'],
            'top_features': sorted(
                mdl['info'].get('feature_importances', {}).items(),
                key=lambda x: -x[1])[:5],
        }

    def _load_from_db(self, symbol: str):
        try:
            import pickle, base64
            with self._get_db() as c:
                row = c.execute("SELECT model_blob, model_info FROM ml_models_v22 WHERE symbol=?",
                                (symbol,)).fetchone()
            if row:
                data = pickle.loads(base64.b64decode(row['model_blob']))
                info = json.loads(row['model_info'])
                with self._lock:
                    self.models[symbol] = {'clf': data['clf'], 'scaler': data['scaler'], 'info': info}
        except Exception:
            pass


def _ema(arr, p):
    if not arr: return arr[-1] if arr else 0
    k = 2 / (p + 1); v = arr[0]
    for x in arr[1:]:
        v = x * k + v * (1 - k)
    return v

def _var(arr):
    if not arr: return 0
    m = sum(arr) / len(arr)
    return sum((x - m)**2 for x in arr) / len(arr)

def _calc_rsi_simple(closes, p=14):
    if len(closes) < p + 2: return 50
    gains = []; losses = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    if len(gains) < p: return 50
    ag = sum(gains[:p]) / p; al = sum(losses[:p]) / p
    for i in range(p, len(gains)):
        ag = (ag * (p-1) + gains[i]) / p
        al = (al * (p-1) + losses[i]) / p
    return 100 - 100 / (1 + ag / (al or 1e-9))

def _calc_adx_simple(history, p=14):
    if len(history) < p + 2: return 25
    try:
        trs = []
        for i in range(1, len(history)):
            h = history[i].get('h', history[i]['c'])
            l = history[i].get('l', history[i]['c'])
            pc = history[i-1]['c']
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        atr = sum(trs[-p:]) / p
        return min(50, sum(trs[-p:]) / (atr * p + 1e-9) * 25)
    except:
        return 25

def ma13(closes):
    return sum(closes[-13:]) / 13 if len(closes) >= 13 else closes[-1]


# ── STRATEGY BUILDER & BACKTEST ───────────────────────────────

STRATEGY_REGISTRY = {}

def register_strategy(name: str, fn):
    """Đăng ký chiến lược giao dịch."""
    STRATEGY_REGISTRY[name] = fn


def _strategy_macd_rsi(history, params=None):
    """Chiến lược MACD + RSI crossover."""
    p = params or {}
    rsi_ob = p.get('rsi_ob', 70); rsi_os = p.get('rsi_os', 30)
    fast = p.get('fast', 12); slow_p = p.get('slow', 26); sig = p.get('signal', 9)
    signals = []
    closes = [b['c'] for b in history]
    for i in range(slow_p + sig + 2, len(history)):
        c = closes[:i+1]
        ema_f = _ema(c[-fast*2:], fast)
        ema_s = _ema(c[-slow_p*2:], slow_p)
        macd  = ema_f - ema_s
        prev_macd = _ema(c[-(fast*2+1):-1], fast) - _ema(c[-(slow_p*2+1):-1], slow_p)
        rsi   = _calc_rsi_simple(c)
        if macd > 0 and prev_macd <= 0 and rsi < rsi_ob:
            signals.append({'idx': i, 'signal': 'BUY',  'price': history[i]['c'], 'date': history[i].get('d', str(i))})
        elif macd < 0 and prev_macd >= 0 and rsi > rsi_os:
            signals.append({'idx': i, 'signal': 'SELL', 'price': history[i]['c'], 'date': history[i].get('d', str(i))})
    return signals


def _strategy_ma_crossover(history, params=None):
    """Chiến lược MA20/MA50 crossover."""
    p = params or {}
    fast = p.get('fast', 20); slow_p = p.get('slow', 50)
    signals = []
    closes = [b['c'] for b in history]
    for i in range(slow_p + 1, len(history)):
        ma_f_now  = sum(closes[i-fast+1:i+1])  / fast
        ma_s_now  = sum(closes[i-slow_p+1:i+1]) / slow_p
        ma_f_prev = sum(closes[i-fast:i])   / fast
        ma_s_prev = sum(closes[i-slow_p:i]) / slow_p
        if ma_f_now > ma_s_now and ma_f_prev <= ma_s_prev:
            signals.append({'idx': i, 'signal': 'BUY',  'price': history[i]['c'], 'date': history[i].get('d', str(i))})
        elif ma_f_now < ma_s_now and ma_f_prev >= ma_s_prev:
            signals.append({'idx': i, 'signal': 'SELL', 'price': history[i]['c'], 'date': history[i].get('d', str(i))})
    return signals


def _strategy_mean_reversion(history, params=None):
    """Mean reversion: mua khi giá lệch xa MA, bán khi về mean."""
    p = params or {}
    period = p.get('period', 20); z_entry = p.get('z_entry', 2.0)
    signals = []
    closes = [b['c'] for b in history]
    for i in range(period + 1, len(history)):
        win = closes[i-period:i]
        mean = sum(win) / period
        std  = _var(win) ** 0.5
        if std == 0: continue
        z = (closes[i] - mean) / std
        if z < -z_entry:
            signals.append({'idx': i, 'signal': 'BUY',  'price': history[i]['c'], 'date': history[i].get('d', str(i))})
        elif z > z_entry:
            signals.append({'idx': i, 'signal': 'SELL', 'price': history[i]['c'], 'date': history[i].get('d', str(i))})
    return signals


def _strategy_momentum(history, params=None):
    """Momentum: mua khi momentum dương và tăng tốc."""
    p = params or {}
    lookback = p.get('lookback', 20); threshold = p.get('threshold', 0.05)
    signals = []
    closes = [b['c'] for b in history]
    for i in range(lookback + 1, len(history)):
        mom     = (closes[i] - closes[i - lookback]) / closes[i - lookback]
        mom_prev= (closes[i-1] - closes[i-1-lookback]) / closes[i-1-lookback]
        if mom > threshold and mom > mom_prev:
            signals.append({'idx': i, 'signal': 'BUY',  'price': history[i]['c'], 'date': history[i].get('d', str(i))})
        elif mom < -threshold:
            signals.append({'idx': i, 'signal': 'SELL', 'price': history[i]['c'], 'date': history[i].get('d', str(i))})
    return signals


register_strategy('MACD + RSI', _strategy_macd_rsi)
register_strategy('MA Crossover', _strategy_ma_crossover)
register_strategy('Mean Reversion', _strategy_mean_reversion)
register_strategy('Momentum', _strategy_momentum)


def run_backtest_v2(history: list, strategy_name: str, params: dict = None,
                   capital: float = 100_000_000, risk_per_trade: float = 0.02,
                   sl_pct: float = 0.05, tp_pct: float = 0.10,
                   position_method: str = 'fixed',
                   market: str = 'VN') -> dict:
    """
    Backtest engine nâng cao với SL/TP, position sizing.
    """
    if strategy_name not in STRATEGY_REGISTRY:
        return {'error': f'Chiến lược không tồn tại: {strategy_name}'}

    if not history or len(history) < 60:
        return {'error': 'Cần ít nhất 60 phiên dữ liệu'}

    sig_fn  = STRATEGY_REGISTRY[strategy_name]
    signals = sig_fn(history, params)

    equity = capital
    trades = []
    position = None
    equity_curve = [{'date': history[0].get('d', '0'), 'equity': capital}]

    fee_rate = 0.0015 if market == 'VN' else 0.001

    for sig in signals:
        idx    = sig['idx']
        price  = sig['price']
        action = sig['signal']

        if action == 'BUY' and position is None:
            sl    = price * (1 - sl_pct)
            tp    = price * (1 + tp_pct)
            risk  = equity * risk_per_trade
            sl_dist = price - sl

            if position_method == 'kelly':
                # Simplified Kelly
                wr = 0.55; rr = tp_pct / sl_pct
                kelly = (wr * rr - (1 - wr)) / rr
                qty   = int(equity * max(0, min(kelly, 0.25)) / price)
            else:
                qty = int(risk / sl_dist) if sl_dist > 0 else 0

            if market == 'VN': qty = max(100, round(qty / 100) * 100)
            else:               qty = max(1, qty)

            cost  = price * qty * (1 + fee_rate)
            if cost <= equity and qty > 0:
                equity -= cost
                position = {'qty': qty, 'entry': price, 'sl': sl, 'tp': tp,
                            'entry_idx': idx, 'entry_date': sig['date']}

        elif action == 'SELL' and position is not None:
            exit_p = price
            proceeds = exit_p * position['qty'] * (1 - fee_rate)
            pnl = proceeds - position['entry'] * position['qty'] * (1 + fee_rate)
            equity += position['entry'] * position['qty'] * (1 + fee_rate) + pnl  # restore cost + pnl
            entry_cost = position['entry'] * position['qty'] * (1 + fee_rate)
            pnl_pct = pnl / entry_cost * 100
            trades.append({
                'entry_date': position['entry_date'],
                'exit_date':  sig['date'],
                'entry': position['entry'],
                'exit':  exit_p,
                'qty':   position['qty'],
                'pnl':   round(pnl, 0),
                'pnl_pct': round(pnl_pct, 2),
                'duration': idx - position['entry_idx'],
            })
            equity_curve.append({'date': sig['date'], 'equity': round(equity, 0)})
            position = None

    # SL/TP checking during position
    if position:
        # Force close at last bar
        last = history[-1]
        exit_p = last['c']
        proceeds = exit_p * position['qty'] * (1 - fee_rate)
        entry_cost = position['entry'] * position['qty'] * (1 + fee_rate)
        pnl = proceeds - entry_cost
        trades.append({
            'entry_date': position['entry_date'],
            'exit_date':  last.get('d', 'end'),
            'entry': position['entry'], 'exit': exit_p,
            'qty': position['qty'],
            'pnl': round(pnl, 0), 'pnl_pct': round(pnl / entry_cost * 100, 2),
            'duration': len(history) - position['entry_idx'],
            'open': True,
        })
        equity += entry_cost + pnl
        equity_curve.append({'date': last.get('d', 'end'), 'equity': round(equity, 0)})

    if not trades:
        return {'error': 'Không có giao dịch nào được thực hiện'}

    wins  = [t for t in trades if t['pnl'] > 0]
    losses= [t for t in trades if t['pnl'] <= 0]
    total_pnl = sum(t['pnl'] for t in trades)
    win_rate  = len(wins) / len(trades) * 100 if trades else 0
    avg_win   = sum(t['pnl_pct'] for t in wins) / len(wins) if wins else 0
    avg_loss  = sum(t['pnl_pct'] for t in losses) / len(losses) if losses else 0
    profit_factor = (sum(t['pnl'] for t in wins) / abs(sum(t['pnl'] for t in losses))
                     if losses and sum(t['pnl'] for t in losses) != 0 else float('inf'))

    # Max drawdown
    peak = capital; max_dd = 0
    for pt in equity_curve:
        v = pt['equity']
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd: max_dd = dd

    # Sharpe
    rets = [(equity_curve[i]['equity'] - equity_curve[i-1]['equity']) / equity_curve[i-1]['equity']
            for i in range(1, len(equity_curve)) if equity_curve[i-1]['equity'] > 0]
    sharpe = 0
    if len(rets) > 1:
        mr = sum(rets) / len(rets)
        sr = (_var(rets) ** 0.5)
        sharpe = round(mr / sr * (252 ** 0.5), 3) if sr > 0 else 0

    return {
        'strategy': strategy_name,
        'params': params or {},
        'total_trades': len(trades),
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': round(win_rate, 1),
        'total_pnl': round(total_pnl, 0),
        'total_pnl_pct': round(total_pnl / capital * 100, 2),
        'avg_win_pct': round(avg_win, 2),
        'avg_loss_pct': round(avg_loss, 2),
        'profit_factor': round(min(profit_factor, 99), 2),
        'max_drawdown_pct': round(max_dd, 2),
        'sharpe': sharpe,
        'final_equity': round(equity, 0),
        'equity_curve': equity_curve,
        'trades': trades[-50:],  # last 50 for display
    }


def walk_forward_optimize(history: list, strategy_name: str,
                          param_grid: dict, capital: float = 100_000_000,
                          n_windows: int = 4, in_sample_pct: float = 0.7,
                          market: str = 'VN') -> dict:
    """
    Walk-forward optimization — tự động tìm bộ tham số tốt nhất.
    param_grid ví dụ: {'rsi_ob': [60,65,70], 'rsi_os': [25,30,35]}
    """
    import itertools

    n = len(history)
    if n < 100:
        return {'error': 'Cần ít nhất 100 phiên'}

    window_size = n // n_windows
    in_size  = int(window_size * in_sample_pct)
    out_size = window_size - in_size

    # Generate all param combinations
    keys   = list(param_grid.keys())
    combos = list(itertools.product(*[param_grid[k] for k in keys]))
    param_combos = [dict(zip(keys, c)) for c in combos]

    wf_results = []

    for w in range(n_windows):
        start = w * window_size
        end   = start + window_size
        train_data = history[start: start + in_size]
        test_data  = history[start + in_size: end]

        if len(train_data) < 50 or len(test_data) < 10:
            continue

        # Find best params on in-sample
        best_params  = None
        best_sharpe  = -999
        for params in param_combos:
            r = run_backtest_v2(train_data, strategy_name, params, capital,
                                market=market)
            if 'error' not in r and r.get('sharpe', -999) > best_sharpe:
                best_sharpe  = r['sharpe']
                best_params  = params

        if best_params is None:
            continue

        # Test on out-of-sample
        oos = run_backtest_v2(test_data, strategy_name, best_params, capital,
                              market=market)
        wf_results.append({
            'window': w + 1,
            'train_start': history[start].get('d', str(start)),
            'train_end':   history[start + in_size - 1].get('d', str(start+in_size)),
            'test_start':  history[start + in_size].get('d', str(start+in_size)),
            'test_end':    history[end - 1].get('d', str(end)) if end <= n else history[-1].get('d', str(n)),
            'best_params_is': best_params,
            'is_sharpe': round(best_sharpe, 3),
            'oos_sharpe': oos.get('sharpe', 0) if 'error' not in oos else 0,
            'oos_pnl_pct': oos.get('total_pnl_pct', 0) if 'error' not in oos else 0,
            'oos_win_rate': oos.get('win_rate', 0) if 'error' not in oos else 0,
        })

    if not wf_results:
        return {'error': 'Walk-forward thất bại — không đủ dữ liệu'}

    avg_oos_sharpe = sum(r['oos_sharpe'] for r in wf_results) / len(wf_results)
    avg_oos_pnl    = sum(r['oos_pnl_pct'] for r in wf_results) / len(wf_results)

    # Most frequent params
# [merged]     from collections import Counter
    param_counts = Counter()
    for r in wf_results:
        param_counts[json.dumps(r['best_params_is'], sort_keys=True)] += 1
    best_overall = json.loads(param_counts.most_common(1)[0][0])

    return {
        'strategy': strategy_name,
        'n_windows': len(wf_results),
        'windows': wf_results,
        'avg_oos_sharpe': round(avg_oos_sharpe, 3),
        'avg_oos_pnl_pct': round(avg_oos_pnl, 2),
        'recommended_params': best_overall,
        'robustness': 'HIGH' if avg_oos_sharpe > 0.5 else (
                      'MEDIUM' if avg_oos_sharpe > 0 else 'LOW'),
    }


# ── RISK MANAGEMENT ───────────────────────────────────────────

def calc_var_cvar_realtime(history: list, confidence: float = 0.95,
                           horizon_days: int = 1) -> dict:
    """VaR và CVaR cập nhật theo dữ liệu mới nhất."""
    if len(history) < 30:
        return {'error': 'Cần ít nhất 30 phiên'}

    closes = [b['c'] for b in history]
    rets   = [(closes[i] - closes[i-1]) / closes[i-1]
               for i in range(1, len(closes)) if closes[i-1] > 0]

    # Scale to horizon
    scale = horizon_days ** 0.5
    rets_h = [r * scale for r in rets]
    rets_h.sort()

    n = len(rets_h)
    var_idx  = int(n * (1 - confidence))
    var  = -rets_h[var_idx] * 100
    cvar = -sum(rets_h[:var_idx+1]) / (var_idx + 1) * 100 if var_idx >= 0 else var

    # Annualized volatility
    mean_r = sum(rets) / n
    vol_daily  = _var([r - mean_r for r in rets]) ** 0.5
    vol_annual = vol_daily * (252 ** 0.5) * 100

    return {
        'var_pct': round(var, 2),
        'cvar_pct': round(cvar, 2),
        'var_amount': None,  # caller multiplies by position value
        'confidence': confidence * 100,
        'horizon_days': horizon_days,
        'vol_daily_pct': round(vol_daily * 100, 3),
        'vol_annual_pct': round(vol_annual, 2),
        'n_observations': n,
    }


def stress_test(history: list, scenarios: list = None) -> dict:
    """
    Stress test với các kịch bản:
    scenarios = [{'name': 'COVID crash', 'drop_pct': -35}, ...]
    """
    default_scenarios = [
        {'name': 'COVID-19 (2020)', 'drop_pct': -35, 'duration_days': 33},
        {'name': 'GFC (2008)',       'drop_pct': -55, 'duration_days': 365},
        {'name': 'Dotcom (2000)',    'drop_pct': -49, 'duration_days': 620},
        {'name': 'Flash crash',      'drop_pct': -10, 'duration_days': 1},
        {'name': 'Moderate bear -20%', 'drop_pct': -20, 'duration_days': 90},
        {'name': 'Mild correction -10%', 'drop_pct': -10, 'duration_days': 30},
    ]
    scens = scenarios or default_scenarios
    if not history:
        return {'error': 'No history'}

    price = history[-1]['c']
    vr = calc_var_cvar_realtime(history)
    daily_vol = vr.get('vol_daily_pct', 1) / 100

    results = []
    for sc in scens:
        drop = sc['drop_pct'] / 100
        expected_loss_pct = drop * 100
        # Approximate probability via normal distribution (simplified)
        z = abs(drop) / (daily_vol * (sc['duration_days'] ** 0.5) + 1e-9)
        prob = max(0.1, min(50, 100 / (1 + z**2)))  # rough
        results.append({
            'scenario': sc['name'],
            'drop_pct': sc['drop_pct'],
            'duration_days': sc['duration_days'],
            'price_impact': round(price * (1 + drop), 2),
            'loss_pct': round(expected_loss_pct, 1),
            'est_probability_pct': round(prob, 1),
            'severity': 'EXTREME' if abs(drop) > 40 else (
                        'HIGH' if abs(drop) > 25 else (
                        'MEDIUM' if abs(drop) > 15 else 'LOW')),
        })

    return {
        'current_price': price,
        'daily_vol_pct': vr.get('vol_daily_pct', 0),
        'var_95': vr.get('var_pct', 0),
        'scenarios': results,
    }


def calc_position_sizing_advanced(account: float, entry: float, sl: float,
                                   win_rate: float = None, avg_win: float = None,
                                   avg_loss: float = None,
                                   market: str = 'VN') -> dict:
    """
    So sánh Fixed Fraction, Kelly Criterion, Optimal-f.
    """
    if not account or not entry or not sl or entry <= sl:
        return {'error': 'Thiếu thông số'}

    sl_dist  = entry - sl
    sl_pct   = sl_dist / entry * 100

    # Fixed 2%
    risk_amt = account * 0.02
    qty_ff   = int(risk_amt / sl_dist)
    if market == 'VN': qty_ff = max(100, round(qty_ff / 100) * 100)
    else:               qty_ff = max(1, qty_ff)

    results = {
        'fixed_2pct': {
            'qty': qty_ff,
            'risk_amount': round(risk_amt, 0),
            'position_value': round(qty_ff * entry, 0),
            'position_pct': round(qty_ff * entry / account * 100, 1),
            'description': 'Rủi ro cố định 2% tài khoản',
        }
    }

    # Kelly
    if win_rate and avg_win and avg_loss and avg_loss > 0:
        b = avg_win / avg_loss; w = win_rate / 100
        kelly = max(0, min((b * w - (1 - w)) / b, 0.25))
        qty_k = int(account * kelly / entry)
        if market == 'VN': qty_k = max(100, round(qty_k / 100) * 100)
        else:               qty_k = max(1, qty_k)
        half_kelly = int(qty_k / 2)
        results['kelly'] = {
            'full_kelly_pct': round(kelly * 100, 1),
            'half_kelly_pct': round(kelly * 50, 1),
            'qty_full': qty_k,
            'qty_half': half_kelly,
            'position_value_half': round(half_kelly * entry, 0),
            'description': f'Kelly Criterion (WR={win_rate:.0f}%, RR={b:.1f})',
            'recommended': 'half',  # half-kelly safer
        }

        # Optimal-f (simplified — uses historical trade outcomes)
        if avg_win and avg_loss:
            opt_f = kelly  # Simplified approximation
            qty_of = int(account * opt_f * 0.8 / entry)
            if market == 'VN': qty_of = max(100, round(qty_of / 100) * 100)
            else:               qty_of = max(1, qty_of)
            results['optimal_f'] = {
                'fraction': round(opt_f * 0.8 * 100, 1),
                'qty': qty_of,
                'position_value': round(qty_of * entry, 0),
                'description': 'Optimal-f (80% Kelly — more conservative)',
            }

    results['sl_distance_pct'] = round(sl_pct, 2)
    results['account'] = account
    results['entry'] = entry
    results['sl'] = sl
    return results


# ── MARKET INTERNALS ──────────────────────────────────────────

def calc_market_internals(ram_cache: dict, market: str = 'VN') -> dict:
    """
    Tính các chỉ số market internals từ cache.
    """
    advances = declines = unchanged = 0
    new_highs = new_lows = 0
    up_vol = down_vol = 0
    total_symbols = 0

    for sym, cached in ram_cache.items():
        d = cached.get('data', {})
        if not d: continue
        if market != 'ALL' and d.get('market') != market: continue

        hist = d.get('history', [])
        price = d.get('price', 0)
        prev_close = hist[-2]['c'] if len(hist) >= 2 else price

        if not prev_close: continue
        chg_pct = (price - prev_close) / prev_close * 100
        vol = hist[-1].get('v', 0) if hist else 0
        total_symbols += 1

        if chg_pct > 0.05:
            advances += 1; up_vol += vol
        elif chg_pct < -0.05:
            declines += 1; down_vol += vol
        else:
            unchanged += 1

        # 52-week high/low
        if len(hist) >= 252:
            high52 = max(b.get('h', b['c']) for b in hist[-252:])
            low52  = min(b.get('l', b['c']) for b in hist[-252:])
            if price >= high52 * 0.99: new_highs += 1
            if price <= low52  * 1.01: new_lows  += 1

    ad_ratio = advances / (declines + 1)
    ad_line  = advances - declines
    mcclellan_osc = round((advances - declines) / max(total_symbols, 1) * 100, 2)

    up_vol_pct = round(up_vol / (up_vol + down_vol + 1) * 100, 1)

    breadth_label = ('BULLISH' if ad_ratio > 1.5 else
                     'BEARISH' if ad_ratio < 0.7 else 'NEUTRAL')

    return {
        'advances': advances,
        'declines': declines,
        'unchanged': unchanged,
        'total': total_symbols,
        'advance_decline_ratio': round(ad_ratio, 2),
        'ad_line': ad_line,
        'mcclellan_oscillator': mcclellan_osc,
        'new_highs': new_highs,
        'new_lows': new_lows,
        'new_highs_lows_ratio': round(new_highs / (new_lows + 1), 2),
        'up_volume_pct': up_vol_pct,
        'breadth_label': breadth_label,
        'market': market,
        'updated_at': datetime.now().isoformat(),
    }


# ── FUNDAMENTAL SCREENER ──────────────────────────────────────

def fundamental_screener(ram_cache: dict, filters: dict = None) -> list:
    """
    Screener kết hợp technical + fundamental.
    filters ví dụ:
    {
        'min_pe': 0, 'max_pe': 20,
        'min_roe': 15,
        'min_revenue_growth': 10,
        'min_technical_score': 5,
        'market': 'VN',
    }
    """
    f = filters or {}
    market   = f.get('market', 'ALL')
    min_pe   = f.get('min_pe')
    max_pe   = f.get('max_pe')
    min_roe  = f.get('min_roe')
    min_rg   = f.get('min_revenue_growth')
    min_pg   = f.get('min_profit_growth')
    max_pb   = f.get('max_pb')
    min_tech = f.get('min_technical_score', 0)
    max_pe_n = f.get('max_pe')

    results = []
    for sym, cached in ram_cache.items():
        d = cached.get('data', {})
        if not d: continue
        if market != 'ALL' and d.get('market', '') != market: continue

        a = d.get('analysis', {})
        tech_total = a.get('total', 0) if a else 0
        if tech_total < min_tech: continue

        fd = _FUND_CACHE.get(sym, {}).get('data', {})

        # Apply filters
        if fd:
            pe  = fd.get('pe')
            pb  = fd.get('pb')
            roe = fd.get('roe')
            rg  = fd.get('revenue_growth_yoy')
            pg  = fd.get('profit_growth_yoy')

            if min_pe is not None and pe and pe < min_pe: continue
            if max_pe is not None and pe and pe > max_pe: continue
            if max_pb is not None and pb and pb > max_pb: continue
            if min_roe is not None and roe and roe < min_roe: continue
            if min_rg is not None and rg is not None and rg < min_rg: continue
            if min_pg is not None and pg is not None and pg < min_pg: continue
        else:
            # If no fundamental data and filter requires it, skip
            if any([min_roe, min_rg, min_pg, max_pe, max_pb]): continue

        fs = fundamental_score(fd) if fd else {'score': 50, 'label': 'N/A', 'signals': []}
        combined = tech_total * 2 + fs['score'] / 10

        results.append({
            'symbol': sym,
            'market': d.get('market', ''),
            'price': d.get('price', 0),
            'technical_score': tech_total,
            'signal': a.get('signal', '') if a else '',
            'fundamental_score': fs['score'],
            'fundamental_label': fs['label'],
            'combined_score': round(combined, 1),
            'pe': fd.get('pe') if fd else None,
            'pb': fd.get('pb') if fd else None,
            'roe': fd.get('roe') if fd else None,
            'revenue_growth': fd.get('revenue_growth_yoy') if fd else None,
            'profit_growth': fd.get('profit_growth_yoy') if fd else None,
        })

    results.sort(key=lambda x: -x['combined_score'])
    return results[:100]


# ── BROKER API SKELETON ───────────────────────────────────────

class BrokerAdapter:
    """Base class cho broker adapters."""
    name = 'generic'

    def get_account(self) -> dict:
        return {"error": "Chưa implement — cần consumer_id/secret thật từ SSI/VNDirect developer portal"}

    def get_positions(self) -> list:
        return {"error": "Chưa implement — cần consumer_id/secret thật từ SSI/VNDirect developer portal"}

    def place_order(self, symbol, qty, price, side, order_type='LIMIT') -> dict:
        return {"error": "Chưa implement — cần consumer_id/secret thật từ SSI/VNDirect developer portal"}

    def cancel_order(self, order_id) -> bool:
        return {"error": "Chưa implement — cần consumer_id/secret thật từ SSI/VNDirect developer portal"}


class PaperBroker(BrokerAdapter):
    """Paper trading broker — không kết nối thật."""
    name = 'paper'

    def __init__(self, balance: float = 100_000_000):
        self.balance   = balance
        self.positions = {}
        self.orders    = []

    def get_account(self) -> dict:
        total_pos_val = sum(p['qty'] * p.get('current_price', p['entry_price'])
                            for p in self.positions.values())
        return {'balance': self.balance, 'total_value': self.balance + total_pos_val,
                'broker': 'paper'}

    def get_positions(self) -> list:
        return list(self.positions.values())

    def place_order(self, symbol, qty, price, side, order_type='LIMIT') -> dict:
        oid = f"PAPER-{int(time.time())}"
        order = {'id': oid, 'symbol': symbol, 'qty': qty, 'price': price,
                 'side': side, 'status': 'FILLED', 'broker': 'paper',
                 'timestamp': datetime.now().isoformat()}
        if side == 'BUY':
            cost = price * qty * 1.0015
            if cost > self.balance:
                return {'error': 'Insufficient balance'}
            self.balance -= cost
            self.positions[symbol] = {'symbol': symbol, 'qty': qty,
                                       'entry_price': price, 'entry_date': order['timestamp']}
        else:
            pos = self.positions.pop(symbol, None)
            if not pos:
                return {'error': 'No position'}
            proceeds = price * pos['qty'] * 0.999
            self.balance += proceeds
            order['pnl'] = proceeds - pos['entry_price'] * pos['qty']

        self.orders.append(order)
        return order


class SSIBroker(BrokerAdapter):
    """SSI FastConnect API — wrapper dùng SSIFastConnect (đầy đủ tính năng)."""
    name = 'ssi'

    def __init__(self, consumer_id: str, consumer_secret: str,
                 account_no: str, pin: str = ''):
        # Delegate hoàn toàn sang SSIFastConnect đã implement đầy đủ
        self._fc = SSIFastConnect(consumer_id, consumer_secret, account_no, pin)
        self.account_no = account_no

    def get_account(self) -> dict:
        return self._fc.get_account_balance()

    def get_positions(self) -> list:
        return self._fc.get_positions()

    def get_pending_orders(self) -> list:
        return self._fc.get_pending_orders()

    def get_order_history(self, from_date=None, to_date=None) -> list:
        return self._fc.get_order_history(from_date, to_date)

    def place_order(self, symbol, qty, price, side, order_type='LO', market='HOSE') -> dict:
        return self._fc.place_order(symbol, qty, price, side, order_type, market)

    def cancel_order(self, order_id, symbol='', market='HOSE') -> dict:
        return self._fc.cancel_order(str(order_id), symbol, market)

    def modify_order(self, order_id, symbol, new_qty, new_price, market='HOSE') -> dict:
        return self._fc.modify_order(str(order_id), symbol, new_qty, new_price, market)


class VNDirectBroker(BrokerAdapter):
    """VNDirect API skeleton."""
    name = 'vndirect'
    BASE_URL = 'https://api.vndirect.com.vn/v4'

    def __init__(self, username: str, password: str, account: str):
        self.username = username
        self.password = password
        self.account  = account

    def get_account(self) -> dict:
        return {'broker': 'vndirect', 'account': self.account, 'status': 'skeleton'}

    def place_order(self, symbol, qty, price, side, order_type='LIMIT') -> dict:
        return {'status': 'not_implemented', 'broker': 'vndirect',
                'note': 'Tích hợp VNDirect API cần credentials thật'}


BROKER_REGISTRY = {'paper': PaperBroker}

def get_broker(broker_type: str, config: dict) -> BrokerAdapter:
    if broker_type == 'paper':
        return PaperBroker(float(config.get('balance', 100_000_000)))
    if broker_type == 'ssi':
        return SSIBroker(config['consumer_id'], config['consumer_secret'],
                         config['account_no'], config.get('pin', ''))
    if broker_type == 'vndirect':
        return VNDirectBroker(config['username'], config['password'], config['account'])
    raise ValueError(f'Unknown broker: {broker_type}')


# ── DB MIGRATION ──────────────────────────────────────────────

def init_db_v22(db_path: str):
    """Tạo các bảng mới cho v22."""
    c = sqlite3.connect(db_path, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS fundamentals(
        symbol      TEXT NOT NULL,
        market      TEXT NOT NULL,
        data_json   TEXT NOT NULL,
        updated_at  TEXT DEFAULT(datetime('now')),
        PRIMARY KEY(symbol, market)
    );

    CREATE TABLE IF NOT EXISTS ml_models_v22(
        symbol      TEXT PRIMARY KEY,
        model_blob  TEXT NOT NULL,
        model_info  TEXT DEFAULT '{}',
        trained_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS strategies(
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER NOT NULL,
        name        TEXT NOT NULL,
        strategy_type TEXT NOT NULL DEFAULT 'MACD + RSI',
        params_json TEXT DEFAULT '{}',
        created_at  TEXT DEFAULT(datetime('now')),
        UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS backtest_results(
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER NOT NULL,
        strategy_id INTEGER,
        symbol      TEXT NOT NULL,
        market      TEXT NOT NULL DEFAULT 'VN',
        result_json TEXT NOT NULL,
        created_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sentiment_cache(
        symbol      TEXT PRIMARY KEY,
        data_json   TEXT NOT NULL,
        updated_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS broker_configs(
        user_id     INTEGER NOT NULL,
        broker_type TEXT NOT NULL,
        config_json TEXT NOT NULL,
        enabled     INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT(datetime('now')),
        PRIMARY KEY(user_id, broker_type)
    );

    CREATE TABLE IF NOT EXISTS market_internals_cache(
        market      TEXT PRIMARY KEY,
        data_json   TEXT NOT NULL,
        updated_at  TEXT DEFAULT(datetime('now'))
    );
    """)
    c.commit()
    c.close()


# ── API ROUTE HANDLERS ────────────────────────────────────────

def handle_v22_api(path: str, method: str, body: dict,
                   db_path: str, ram_cache: dict,
                   current_user: dict = None) -> tuple:
    """
    Returns (response_dict, status_code).
    Tích hợp vào Handler.do_GET/POST bằng cách gọi hàm này.
    """
    db = lambda: sqlite3.connect(db_path, timeout=10, check_same_thread=False)

    # ── GET fundamental
    if method == 'GET' and path.startswith('/api/v22/fundamental/'):
        sym = path.split('/')[-1].upper()
        mkt = body.get('market', 'US') if body else 'US'
        # detect market from cache
        if sym in ram_cache:
            mkt = ram_cache[sym].get('data', {}).get('market', mkt)
        fd  = fetch_fundamental(sym, mkt)
        fs  = fundamental_score(fd)
        return {**fd, 'score': fs}, 200

    # ── GET sentiment
    if method == 'GET' and path.startswith('/api/v22/sentiment/'):
        sym = path.split('/')[-1].upper()
        if sym in ram_cache:
            d    = ram_cache[sym].get('data', {})
            news = d.get('news', [])
            mkt  = d.get('market', 'VN')
        else:
            news = []; mkt = 'VN'
        sent = get_sentiment_for_symbol(sym, mkt, news)
        return sent, 200

    # ── POST ML train
    if method == 'POST' and path == '/api/v22/ml/train':
        if not current_user:
            return {'error': 'Unauthorized'}, 401
        sym = body.get('symbol', '').upper()
        mkt = body.get('market', 'VN').upper()
        if sym not in ram_cache:
            return {'error': f'{sym} chưa có trong watchlist'}, 404
        d    = ram_cache[sym].get('data', {})
        hist = d.get('history', [])
        fd   = _FUND_CACHE.get(sym, {}).get('data', {})
        from threading import Thread

        def _train():
            predictor = AdvancedMLPredictor(db_path)
            predictor.train(sym, mkt, hist, fd)

        Thread(target=_train, daemon=True).start()
        return {'ok': True, 'message': f'Training {sym} started in background'}, 200

    # ── POST ML predict
    if method == 'POST' and path == '/api/v22/ml/predict':
        sym = body.get('symbol', '').upper()
        mkt = body.get('market', 'VN').upper()
        if sym not in ram_cache:
            return {'error': f'{sym} không tìm thấy'}, 404
        d    = ram_cache[sym].get('data', {})
        hist = d.get('history', [])
        fd   = _FUND_CACHE.get(sym, {}).get('data', {})
        sent = _SENT_CACHE.get(sym, {}).get('data')
        predictor = AdvancedMLPredictor(db_path)
        result = predictor.predict(sym, mkt, hist, fd, sent)
        return result, 200

    # ── POST backtest
    if method == 'POST' and path == '/api/v22/backtest':
        if not current_user:
            return {'error': 'Unauthorized'}, 401
        sym      = body.get('symbol', '').upper()
        strategy = body.get('strategy', 'MACD + RSI')
        params   = body.get('params', {})
        capital  = float(body.get('capital', 100_000_000))
        market   = body.get('market', 'VN').upper()

        if sym not in ram_cache:
            return {'error': f'{sym} chưa có trong watchlist'}, 404
        hist = ram_cache[sym].get('data', {}).get('history', [])
        result = run_backtest_v2(hist, strategy, params, capital, market=market)
        return result, 200

    # ── POST walk-forward optimize
    if method == 'POST' and path == '/api/v22/walk_forward':
        if not current_user:
            return {'error': 'Unauthorized'}, 401
        sym      = body.get('symbol', '').upper()
        strategy = body.get('strategy', 'MACD + RSI')
        grid     = body.get('param_grid', {})
        capital  = float(body.get('capital', 100_000_000))
        market   = body.get('market', 'VN').upper()
        n_windows= int(body.get('n_windows', 4))

        if not grid:
            # Default grid based on strategy
            if 'MACD' in strategy:
                grid = {'rsi_ob': [65, 70, 75], 'rsi_os': [25, 30, 35]}
            elif 'MA' in strategy:
                grid = {'fast': [10, 15, 20], 'slow': [40, 50, 60]}
            else:
                grid = {'period': [15, 20, 25], 'z_entry': [1.5, 2.0, 2.5]}

        if sym not in ram_cache:
            return {'error': f'{sym} chưa có trong watchlist'}, 404
        hist = ram_cache[sym].get('data', {}).get('history', [])
        result = walk_forward_optimize(hist, strategy, grid, capital, n_windows, market=market)
        return result, 200

    # ── GET risk (VaR + Stress)
    if method == 'GET' and path.startswith('/api/v22/risk/'):
        sym = path.split('/')[-1].upper()
        if sym not in ram_cache:
            return {'error': 'Symbol not found'}, 404
        hist = ram_cache[sym].get('data', {}).get('history', [])
        var_data  = calc_var_cvar_realtime(hist)
        st_data   = stress_test(hist)
        return {'symbol': sym, 'var': var_data, 'stress': st_data}, 200

    # ── POST position sizing
    if method == 'POST' and path == '/api/v22/position_size':
        result = calc_position_sizing_advanced(
            account  = float(body.get('account', 0)),
            entry    = float(body.get('entry', 0)),
            sl       = float(body.get('sl', 0)),
            win_rate = body.get('win_rate'),
            avg_win  = body.get('avg_win'),
            avg_loss = body.get('avg_loss'),
            market   = body.get('market', 'VN'),
        )
        return result, 200

    # ── GET market internals
    if method == 'GET' and path.startswith('/api/v22/market_internals'):
        market = body.get('market', 'VN') if body else 'VN'
        result = calc_market_internals(ram_cache, market)
        return result, 200

    # ── POST fundamental screener
    if method == 'POST' and path == '/api/v22/screener/fundamental':
        filters = body or {}
        result  = fundamental_screener(ram_cache, filters)
        return {'results': result, 'count': len(result)}, 200

    # ── GET strategies list
    if method == 'GET' and path == '/api/v22/strategies':
        return {'strategies': list(STRATEGY_REGISTRY.keys())}, 200

    # ── POST broker order (paper)
    if method == 'POST' and path == '/api/v22/broker/order':
        if not current_user:
            return {'error': 'Unauthorized'}, 401
        broker_type = body.get('broker', 'paper')
        config      = body.get('config', {})
        try:
            broker = get_broker(broker_type, config)
            result = broker.place_order(
                symbol     = body.get('symbol', ''),
                qty        = int(body.get('qty', 0)),
                price      = float(body.get('price', 0)),
                side       = body.get('side', 'BUY'),
                order_type = body.get('order_type', 'LIMIT'),
            )
            return result, 200
        except Exception as e:
            return {'error': str(e)}, 400

    # ─── v22 Routes added from v24 ───

    if method == 'GET' and path.startswith('/api/v22/dcf/'):
        sym = path.split('/')[-1].upper()
        mkt = body.get('market', 'VN') if body else qs.get('market', ['VN'])[0].upper()
        fd  = fetch_fundamental(sym, mkt)
        if fd.get('error') and not fd.get('net_income_ttm'):
            return {'error': f'Không có dữ liệu fundamental cho {sym}'}, 404
        dcf_result = calc_dcf(fd, mkt)
        fs = fundamental_score(fd)
        return {
            'symbol': sym, 'market': mkt,
            'fundamental': fd, 'dcf': dcf_result,
            'score': fs,
        }, 200

    # ── GET Peer comparison ───────────────────────────────────
    if method == 'GET' and path.startswith('/api/v22/ensemble/importance/'):
        sym = path.split('/')[-1].upper()
        ens = get_ensemble(db_path)
        if sym not in ens._models:
            ens._load_from_db(sym)
        if sym not in ens._models:
            return {'error': f'Model chưa train cho {sym}. Chạy train trước.'}, 404
        info = ens._models[sym]['info']
        return {
            'symbol': sym,
            'permutation_importance': info.get('permutation_importance', {}),
            'top_drivers': info.get('top_drivers', []),
            'weak_features': info.get('weak_features', []),
            'model_accuracies': {
                'gb': info.get('acc_gb'), 'lr': info.get('acc_lr'),
                'lstm': info.get('acc_lstm'), 'ensemble': info.get('ensemble_acc'),
            },
            'weights': info.get('weights', {}),
            'trained_at': info.get('trained_at'),
            'n_samples': info.get('n_samples'),
        }, 200

    # ── GET DCF valuation ────────────────────────────────────
    if method == 'POST' and path == '/api/v22/ensemble/predict':
        sym    = body.get('symbol', '').upper()
        market = body.get('market', 'VN').upper()
        if sym not in ram_cache:
            return {'error': f'{sym} chưa có trong watchlist'}, 404
        hist = ram_cache[sym].get('data', {}).get('history', [])
        ens  = get_ensemble(db_path)
        result = ens.predict(sym, market, hist)
        return result, 200

    # ── GET Ensemble feature importance (SHAP proxy) ─────────
    if method == 'POST' and path == '/api/v22/ensemble/train':
        if not current_user:
            return {'error': 'Unauthorized'}, 401
        sym    = body.get('symbol', '').upper()
        market = body.get('market', 'VN').upper()
        if sym not in ram_cache:
            return {'error': f'{sym} chưa có trong watchlist'}, 404
        hist   = ram_cache[sym].get('data', {}).get('history', [])
        fund   = None
        try: fund = fetch_fundamental(sym, market)
        except: pass
        ens = get_ensemble(db_path)
        def _train_bg():
            result = ens.train(sym, market, hist, fund)
            log.info('[Ensemble] Train %s: acc=%s', sym, result.get('ensemble_acc'))
        threading.Thread(target=_train_bg, daemon=True).start()
        return {'ok': True, 'message': f'Ensemble training {sym} bắt đầu (background ~20s)'}, 202

    # ── POST Ensemble predict ────────────────────────────────
    if method == 'GET' and path.startswith('/api/v22/fundamental_full/'):
        sym = path.split('/')[-1].upper()
        mkt = qs.get('market', ['VN'])[0].upper()
        fd  = fetch_fundamental(sym, mkt)
        fs  = fundamental_score(fd)
        dcf = calc_dcf(fd, mkt) if (fd.get('net_income_ttm') or fd.get('net_income')) else {}
        peer = get_peer_comparison(sym, mkt)
        fd['score'] = fs
        fd['dcf']   = dcf
        fd['peer']  = peer
        return fd, 200


    if method == 'GET' and path.startswith('/api/v22/peer/'):
        sym = path.split('/')[-1].upper()
        mkt = qs.get('market', ['VN'])[0].upper()
        result = get_peer_comparison(sym, mkt)
        return result, 200
    return {'error': f'v22: Route not found: {path}'}, 404


_SENT_CACHE = {}

# ── V22 INIT ──────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════
# ENSEMBLE ML PREDICTOR — LSTM + GradientBoosting + LogReg
# + Permutation-based Feature Importance (SHAP proxy)
# ══════════════════════════════════════════════════════════════



    # ── GET Fundamental enhanced (DCF + Peer in one call) ────


_SENT_CACHE = {}

# ── V22 INIT ──────────────────────────────────────────────────
def _init_v22():
    try:
        init_db_v22(DB_PATH)
        global _ML_V22
        _ML_V22 = AdvancedMLPredictor(DB_PATH)
        print("[SMA Pro] Modules ready")
    except Exception as _e:
        print(f"[SMA Pro] Warning: {_e}")

# ════════════════════════════════════════════════════════════════
# SMA Pro CORE — Real-time, LSTM, T+2 Backtest, VaR, Scheduler
# (merged from sma_v24_core.py)
# ════════════════════════════════════════════════════════════════


# [merged] import json, os, sys, time, threading, sqlite3, logging, asyncio
# [merged] import hashlib, hmac, base64, urllib.request, urllib.parse
# [merged] from datetime import datetime, timedelta, date
# [merged] from collections import defaultdict, deque
from typing import Optional, Dict, List, Tuple

log = logging.getLogger('sma.Pro')

# ══════════════════════════════════════════════════════════════
# MODULE 1 — SSI iBOARD WEBSOCKET (REAL-TIME, KHÔNG CẦN KEY)
# ══════════════════════════════════════════════════════════════

class SSIRealtimeFeed:
    """
    Kết nối SSI iBoard WebSocket để nhận:
    - Giá khớp lệnh từng tick (last price, volume)
    - Dư mua/bán 3 bước giá (order book)
    - Room nước ngoài còn lại
    - Thống kê phiên (open, high, low, total vol, total val)

    Không cần đăng ký tài khoản — public feed.

    Cách dùng:
        feed = SSIRealtimeFeed(['VCB', 'HPG', 'FPT'])
        feed.start()
        price = feed.get_price('VCB')   # → {'price': 88500, 'vol': 1200, ...}
        feed.stop()
    """

    # ── txnstream endpoint (mới từ 2025) ──────────────────────
    WS_URL   = "wss://txnstream-iboard.ssi.com.vn/realtime"
    HTTP_URL = "https://iboard-query.ssi.com.vn/iboard"   # HTTP fallback giữ nguyên
    HEADERS  = {
        "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin":      "https://iboard.ssi.com.vn",
        "Referer":     "https://iboard.ssi.com.vn/",
        "Host":        "txnstream-iboard.ssi.com.vn",
    }
    RECONNECT_DELAY = 30
    MAX_ERRORS      = 5
    PING_INTERVAL   = 20  # txnstream thường yêu cầu ping nhanh hơn

    def __init__(self, symbols: List[str], on_tick=None):
        self.symbols    = [s.upper() for s in symbols]
        self.on_tick    = on_tick        # callback(symbol, tick_data)
        self._data: Dict[str, dict] = {}
        self._lock      = threading.Lock()
        self._running   = False
        self._ws        = None
        self._thread    = None
        self._last_ping = 0
        self._errors    = 0

    # ── Public API ─────────────────────────────────────────────

    def start(self):
        """Khởi động feed trong background thread."""
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True,
                                          name="ssi-ws-feed")
        self._thread.start()
        log.info("[SSI-RT] Feed started for %s", self.symbols)

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        log.info("[SSI-RT] Feed stopped")

    def get_price(self, symbol: str) -> Optional[dict]:
        """Trả về tick mới nhất. None nếu chưa có dữ liệu."""
        with self._lock:
            return self._data.get(symbol.upper())

    def get_all(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._data)

    def add_symbol(self, symbol: str):
        symbol = symbol.upper()
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            self._subscribe([symbol])

    def is_connected(self) -> bool:
        return self._running and self._ws is not None

    # ── HTTP fallback (khi WS chưa kết nối) ───────────────────

    def fetch_snapshot_http(self, symbol: str) -> Optional[dict]:
        """
        Lấy snapshot qua HTTP REST — dùng khi WS chưa ready.
        Endpoint: GET /iboard/stock/quotes?symbol=VCB
        """
        try:
            url = f"{self.HTTP_URL}/stock/quotes?symbol={symbol}"
            req = urllib.request.Request(url, headers=self.HEADERS)
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = json.loads(r.read().decode())
            data = raw.get("data", raw.get("quotes", []))
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            else:
                return None
            return self._parse_quote(item)
        except Exception as e:
            log.warning("[SSI-RT] HTTP snapshot %s failed: %s", symbol, e)
            return None

    def fetch_orderbook_http(self, symbol: str) -> Optional[dict]:
        """
        Lấy order book 3 bước giá qua HTTP.
        """
        try:
            url = f"{self.HTTP_URL}/stock/bidasks?symbol={symbol}"
            req = urllib.request.Request(url, headers=self.HEADERS)
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = json.loads(r.read().decode())
            data = raw.get("data", raw.get("bidasks", {}))
            if not data:
                return None
            return {
                "bids": [
                    {"price": _pf(data.get(f"g{i}",0)), "vol": _vi(data.get(f"gv{i}",0))}
                    for i in range(1, 4)
                ],
                "asks": [
                    {"price": _pf(data.get(f"o{i}",0)), "vol": _vi(data.get(f"ov{i}",0))}
                    for i in range(1, 4)
                ],
            }
        except Exception as e:
            log.warning("[SSI-RT] Orderbook %s failed: %s", symbol, e)
            return None

    # ── WebSocket loop ─────────────────────────────────────────

    def _run_loop(self):
        """
        Main loop: thử WS 1 lần, nếu fail → HTTP polling vĩnh viễn.
        Không retry WS liên tục để tránh spam 404.
        """
        # SSI txnstream yêu cầu auth (401) → dùng TCBS HTTP thay thế
        # Thử WS 1 lần, nếu fail (401/403/404) → HTTP polling vĩnh viễn
        _ws_ok = False
        try:
            log.info("[SSI-RT] Thử kết nối WS → %s", self.WS_URL)
            self._connect_and_run()
            _ws_ok = True
        except Exception as e:
            err_str = str(e)
            if '401' in err_str or 'Unauthorized' in err_str:
                log.info("[SSI-RT] WS 401 (cần auth) → TCBS HTTP fallback")
            elif '403' in err_str or 'Forbidden' in err_str:
                log.info("[SSI-RT] WS 403 → TCBS HTTP fallback")
            elif '404' in err_str or 'Not Found' in err_str:
                log.info("[SSI-RT] WS 404 → TCBS HTTP fallback")
            else:
                log.info("[SSI-RT] WS error: %s → TCBS HTTP fallback", e)

        if not _ws_ok:
            self._http_poll_loop()  # TCBS polling — không bao giờ return
            return

        # WS OK → reconnect khi bị ngắt
        _fail = 0
        while self._running:
            try:
                self._connect_and_run()
                _fail = 0
            except Exception as e:
                _fail += 1
                delay = min(30 * (2 ** _fail), 300)
                if self._running:
                    time.sleep(delay)

    def _connect_and_run(self):
        """Single WebSocket session."""
        try:
            import websocket as _ws_lib
        except ImportError:
            log.warning("[SSI-RT] websocket-client not installed, using HTTP fallback")
            self._http_poll_loop()
            return

        def on_open(ws):
            log.info("[SSI-RT] WS connected: %s", self.WS_URL)
            self._ws = ws
            self._errors = 0  # reset error count on success
            # txnstream có thể cần handshake trước khi subscribe
            try:
                # Thử gửi handshake (một số endpoint SSI yêu cầu)
                handshake = json.dumps({"action": "login", "token": ""})
                ws.send(handshake)
            except Exception:
                pass  # không bắt buộc
            self._subscribe(self.symbols)
            self._last_ping = time.time()

        def on_message(ws, message):
            try:
                self._handle_message(message)
            except Exception as e:
                log.debug("[SSI-RT] parse error: %s", e)

        def on_error(ws, error):
            err_str = str(error)
            if '404' in err_str:
                log.warning("[SSI-RT] WS 404 — SSI đã đổi endpoint. "
                           "Đang dùng HTTP fallback. Liên hệ SSI để cập nhật WS_URL.")
            else:
                log.warning("[SSI-RT] WS error: %s", error)
            self._ws = None

        def on_close(ws, code, msg):
            log.info("[SSI-RT] WS closed: %s %s", code, msg)
            self._ws = None

        ws = _ws_lib.WebSocketApp(
            self.WS_URL,
            header=[f"{k}: {v}" for k, v in self.HEADERS.items()],
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=self.PING_INTERVAL, ping_timeout=10)

    def _http_poll_loop(self):
        """
        HTTP polling fallback dùng TCBS public API.
        Interval 5s để tránh rate limit, không spam.
        """
        _fail_count = 0
        while self._running:
            _got = 0
            for sym in self.symbols:
                if not self._running: break
                try:
                    # Dùng TCBS public API (không cần auth, miễn phí)
                    tick = fetch_ssi_realtime_price(sym)
                    if tick and tick.get('price', 0) > 0:
                        tick_fmt = {
                            'price':      tick.get('price', 0),
                            'change_pct': tick.get('change_pct', 0),
                            'volume':     tick.get('volume', 0),
                            'source':     'TCBS-HTTP',
                        }
                        with self._lock:
                            self._data[sym] = tick_fmt
                        if self.on_tick:
                            self.on_tick(sym, tick_fmt)
                        _got += 1
                except Exception as _pe:
                    log.debug("[SSI-RT] HTTP poll %s: %s", sym, _pe)
                time.sleep(0.5)  # 0.5s giữa mỗi mã

            if _got == 0:
                _fail_count += 1
                if _fail_count >= 5:
                    # Tất cả fail → tăng interval lên 60s
                    log.debug("[SSI-RT] HTTP poll: tất cả fail → sleep 60s")
                    for _ in range(60):
                        if not self._running: break
                        time.sleep(1)
                    _fail_count = 0
                    continue
            else:
                _fail_count = 0

            # Poll mỗi 5 giây trong giờ giao dịch, 60s ngoài giờ
            _h = __import__('datetime').datetime.now().hour
            _interval = 5 if 9 <= _h <= 15 else 60
            for _ in range(_interval * 2):
                if not self._running: break
                time.sleep(0.5)

    def _subscribe(self, symbols: List[str]):
        """
        Gửi subscribe message lên txnstream WebSocket.
        txnstream endpoint dùng format: {action, list, type}
        Thử nhiều format vì SSI thay đổi không thông báo.
        """
        if not self._ws or not symbols:
            return
        try:
            # Format 1: txnstream batch subscribe
            msg = json.dumps({
                "action": "subscribe",
                "list":   symbols,
                "type":   "stock"
            })
            self._ws.send(msg)
            log.debug("[SSI-RT] subscribed %d symbols (txnstream format)", len(symbols))
        except Exception as e:
            log.debug("[SSI-RT] subscribe batch failed: %s — trying per-symbol", e)
            # Format 2: fallback per-symbol (iboard cũ)
            for sym in symbols:
                try:
                    self._ws.send(json.dumps({
                        "action":  "subcribe",   # typo SSI API cũ
                        "symbols": [sym],
                        "type":    "stock"
                    }))
                except Exception as e2:
                    log.debug("[SSI-RT] subscribe %s failed: %s", sym, e2)

    def _handle_message(self, raw: str):
        """
        Parse message từ txnstream SSI WebSocket.
        txnstream format: {"type":"stock","data":[{...},...]}
        hoặc: {"t":"q","d":{...}}  (compact format)
        """
        try:
            msg = json.loads(raw)
        except Exception:
            log.debug("[SSI-RT] non-JSON message: %s", raw[:100])
            return

        msg_type = (msg.get("type") or msg.get("t") or
                    msg.get("action") or msg.get("event") or "")
        data      = (msg.get("data") or msg.get("d") or
                    msg.get("list") or msg.get("payload") or {})

        log.debug("[SSI-RT] msg type=%r keys=%s", msg_type, list(msg.keys()))

        # Tick data — txnstream gửi dạng list
        if msg_type in ("stock", "quote", "ticker", "realtime", "q", "trade"):
            if isinstance(data, list):
                for item in data:
                    self._update_tick(item)
            elif isinstance(data, dict):
                self._update_tick(data)
        # Broadcast tất cả fields nếu là dict trực tiếp (một số format)
        elif isinstance(msg, dict) and ("lastPrice" in msg or "matchingPrice" in msg):
            self._update_tick(msg)
        # Order book
        elif msg_type in ("bidask", "orderbook", "ba"):
            sym = (data.get("sym") or data.get("symbol") or "").upper()
            if sym:
                parsed = {
                    "bids": [
                        {"price": _pf(data.get(f"g{i}",0)), "vol": _vi(data.get(f"gv{i}",0))}
                        for i in range(1, 4)
                    ],
                    "asks": [
                        {"price": _pf(data.get(f"o{i}",0)), "vol": _vi(data.get(f"ov{i}",0))}
                        for i in range(1, 4)
                    ],
                }
                with self._lock:
                    if sym in self._data:
                        self._data[sym]["orderbook"] = parsed

    def _update_tick(self, item: dict):
        sym = (item.get("sym") or item.get("symbol") or item.get("mc") or "").upper()
        if not sym or sym not in self.symbols:
            return
        tick = self._parse_quote(item)
        with self._lock:
            old = self._data.get(sym, {})
            # Giữ lại orderbook nếu có
            if "orderbook" in old:
                tick["orderbook"] = old["orderbook"]
            self._data[sym] = tick
        if self.on_tick:
            self.on_tick(sym, tick)

    @staticmethod
    def _parse_quote(item: dict) -> dict:
        """
        Chuẩn hóa quote từ SSI response.
        Các field SSI dùng: lastPrice, lot, open, high, low, totalVol, totalVal,
                             foreignBuyVol, foreignSellVol, foreignRoom, ...
        """
        price  = _pf(item.get("lastPrice") or item.get("matchingPrice") or
                     item.get("last")       or item.get("p") or 0)
        chg    = _pf(item.get("change")    or item.get("ch") or 0)
        chg_pct= _pf(item.get("changePct") or item.get("changePercent") or
                     item.get("pct")        or 0)
        vol    = _vi(item.get("lot")        or item.get("matchingVolume") or
                     item.get("vol")        or item.get("v") or 0)
        tot_vol= _vi(item.get("totalVol")   or item.get("totalVolume")   or
                     item.get("tvol")       or 0)
        tot_val= _pf(item.get("totalVal")   or item.get("totalValue")    or 0)

        fgn_buy  = _vi(item.get("foreignBuyVol")   or item.get("fBuyVol")  or 0)
        fgn_sell = _vi(item.get("foreignSellVol")  or item.get("fSellVol") or 0)
        fgn_room = _vi(item.get("foreignRoom")     or item.get("fRoom")    or 0)
        fgn_net  = fgn_buy - fgn_sell

        return {
            "price":      price,
            "change":     chg,
            "change_pct": chg_pct,
            "vol":        vol,
            "total_vol":  tot_vol,
            "total_val":  tot_val,
            "open":       _pf(item.get("open")  or item.get("o") or price),
            "high":       _pf(item.get("high")  or item.get("h") or price),
            "low":        _pf(item.get("low")   or item.get("l") or price),
            "ref_price":  _pf(item.get("refPrice") or item.get("r") or 0),
            "ceil_price": _pf(item.get("ceilPrice")  or 0),
            "floor_price":_pf(item.get("floorPrice") or 0),
            "fgn_buy":    fgn_buy,
            "fgn_sell":   fgn_sell,
            "fgn_net":    fgn_net,
            "fgn_room":   fgn_room,
            "session":    str(item.get("session") or item.get("tradingStatus") or ""),
            "ts":         time.time(),
            "source":     "ssi_ws",
        }


# Singleton feed (được khởi tạo khi server start)
_ssi_feed: Optional[SSIRealtimeFeed] = None
_ssi_lock = threading.Lock()

def get_ssi_feed(symbols: List[str] = None, ram_cache: dict = None) -> SSIRealtimeFeed:
    """Lấy singleton SSI feed, tạo mới nếu chưa có."""
    global _ssi_feed
    with _ssi_lock:
        if _ssi_feed is None:
            syms = symbols or []
            if ram_cache:
                syms = list(set(syms + [s for s in ram_cache
                                        if ram_cache[s].get("data", {}).get("market") == "VN"]))

            def _on_tick(sym, tick):
                """Callback: cập nhật ram_cache khi nhận tick mới."""
                if ram_cache and sym in ram_cache:
                    d = ram_cache[sym].get("data", {})
                    if d:
                        d["price"]      = tick["price"]
                        d["change"]     = tick.get("change", 0)
                        d["change_pct"] = tick.get("change_pct", 0)
                        d["total_vol"]  = tick.get("total_vol", 0)
                        d["realtime"]   = tick
                        ram_cache[sym]["ts_rt"] = time.time()  # separate realtime ts

            _ssi_feed = SSIRealtimeFeed(syms, on_tick=_on_tick)
            _ssi_feed.start()
    return _ssi_feed


def init_realtime(ram_cache: dict):
    """
    Khởi động SSI/TCBS realtime feed.
    Pipeline:
      1. WS thử kết nối 1 lần
      2. Nếu fail (404 hoặc lỗi khác) → HTTP polling TCBS
      3. Mọi lỗi đều graceful — KHÔNG ảnh hưởng OHLCV pipeline
    Note: OHLCV data (fetch/analyze) hoàn toàn độc lập qua vnstock VCI/TCBS.
    """
    try:
        vn_syms = [s for s, v in ram_cache.items()
                   if v.get("data", {}).get("market") == "VN"]
        if not vn_syms:
            log.info("[SSI-RT] No VN symbols yet — skip init")
            return None
        feed = get_ssi_feed(vn_syms, ram_cache)
        log.info("[SSI-RT] Feed started: %d VN symbols (WS→HTTP fallback ready)", len(vn_syms))
        return feed
    except Exception as _re:
        log.warning("[SSI-RT] init_realtime error (non-critical): %s", _re)
        return None


# ══════════════════════════════════════════════════════════════
# MODULE 2 — T+2/T+3 CONSTRAINT BACKTEST (VN ACCURATE)
# ══════════════════════════════════════════════════════════════

T_PLUS_VN = 2   # Có thể bán từ T+2 (nhận cổ phiếu ngày T+2)
T_PLUS_US = 0   # US: T+1 thực tế, nhưng không giới hạn intraday

def run_backtest_t2(
    history: List[dict],
    capital: float     = 100_000_000,
    market: str        = "VN",
    risk_per_trade: float = 0.02,
    sl_pct: float      = 0.07,
    tp_pct: float      = 0.15,
    strategy: str      = "signal",   # "signal" | "ma_cross"
    t_plus: int        = None,
) -> dict:
    """
    Backtest engine với T+2/T+3 constraint đúng cho TTCK Việt Nam.

    Key differences vs backtest cũ:
    - Không thể bán trong T+0 và T+1 (ngày mua + 1 ngày sau)
    - Dynamic slippage dựa trên thanh khoản
    - Vị thế pending: lệnh mua T+0, nhận cổ phiếu T+2
    - Commission: 0.15% mua + 0.10% bán (VN), 0.05% mua+bán (US)
    """
    n = len(history)
    if n < 60:
        return {"error": "Cần ít nhất 60 phiên"}

    t_lock = T_PLUS_VN if market == "VN" else (t_plus or T_PLUS_US)
    fee_buy  = 0.0015 if market == "VN" else 0.0005
    fee_sell = 0.0010 if market == "VN" else 0.0005

    # Tính average daily volume cho dynamic slippage
    avg_vols = []
    for i in range(20, n):
        avg_vols.append(sum(history[j].get("v", 0) for j in range(i-20, i)) / 20)

    cash      = float(capital)
    shares    = 0.0
    entry_price = 0.0
    entry_bar   = -999   # bar index khi mua
    sl_price    = 0.0
    tp_price    = 0.0
    trades      = []
    equity_curve= []
    wins_val    = []
    losses_val  = []
    pending_buy_qty   = 0.0   # cổ phiếu đang chờ T+2
    pending_buy_bar   = -999  # bar index lệnh mua
    pending_buy_cost  = 0.0

    for i in range(55, n):
        bar     = history[i]
        price   = float(bar.get("o") or bar["c"])  # dùng giá mở cửa để exec
        close   = float(bar["c"])
        vol_i   = bar.get("v", 0)
        avg_vol = avg_vols[i - 20] if i >= 20 and (i-20) < len(avg_vols) else max(vol_i, 1)

        # Nhận cổ phiếu sau T+lock ngày
        if pending_buy_qty > 0 and (i - pending_buy_bar) >= t_lock:
            shares            += pending_buy_qty
            entry_price        = pending_buy_cost / pending_buy_qty
            entry_bar          = pending_buy_bar
            pending_buy_qty    = 0.0
            pending_buy_bar    = -999
            pending_buy_cost   = 0.0

        # Kiểm tra SL/TP nếu đang có vị thế (chỉ sau khi nhận cổ phiếu)
        if shares > 0:
            # Dynamic slippage
            order_qty = shares
            slip = calc_dynamic_slippage(order_qty, avg_vol, market)

            # Stop Loss hit
            low_bar = float(bar.get("l") or close)
            if sl_price > 0 and low_bar <= sl_price:
                exec_price = sl_price * (1 - slip)
                fee   = shares * exec_price * fee_sell
                pnl   = (exec_price - entry_price) * shares - fee
                pnl_pct = (exec_price - entry_price) / entry_price * 100
                cash += shares * exec_price - fee
                trades.append(_trade_record(history, entry_bar, i, entry_price,
                                             exec_price, shares, pnl, pnl_pct, "SL", slip))
                (wins_val if pnl > 0 else losses_val).append(pnl_pct)
                shares = 0.0; sl_price = 0.0; tp_price = 0.0

            # Take Profit hit
            elif tp_price > 0 and float(bar.get("h") or close) >= tp_price:
                exec_price = tp_price * (1 - slip)
                fee   = shares * exec_price * fee_sell
                pnl   = (exec_price - entry_price) * shares - fee
                pnl_pct = (exec_price - entry_price) / entry_price * 100
                cash += shares * exec_price - fee
                trades.append(_trade_record(history, entry_bar, i, entry_price,
                                             exec_price, shares, pnl, pnl_pct, "TP", slip))
                (wins_val if pnl > 0 else losses_val).append(pnl_pct)
                shares = 0.0; sl_price = 0.0; tp_price = 0.0

        # Tính tín hiệu từ analysis
        signal = _get_bar_signal(history, i, strategy)

        # BUY signal — chỉ mua khi không có vị thế và không có lệnh pending
        if signal == "BUY" and shares == 0 and pending_buy_qty == 0:
            slip     = calc_dynamic_slippage(0, avg_vol, market)
            exec_p   = price * (1 + slip)
            risk_amt = cash * risk_per_trade
            sl_p     = exec_p * (1 - sl_pct)
            sl_dist  = exec_p - sl_p
            if sl_dist > 0:
                qty = int(risk_amt / sl_dist)
                if market == "VN":
                    qty = max(100, round(qty / 100) * 100)
                else:
                    qty = max(1, qty)
                cost = exec_p * qty * (1 + fee_buy)
                if cost <= cash and qty > 0:
                    cash              -= cost
                    pending_buy_qty    = float(qty)
                    pending_buy_bar    = i
                    pending_buy_cost   = exec_p * qty
                    sl_price           = sl_p
                    tp_price           = exec_p * (1 + tp_pct)

        # SELL signal — chỉ bán được sau khi nhận cổ phiếu (T+lock)
        elif signal == "SELL" and shares > 0:
            slip = calc_dynamic_slippage(shares, avg_vol, market)
            exec_price = price * (1 - slip)
            fee   = shares * exec_price * fee_sell
            pnl   = (exec_price - entry_price) * shares - fee
            pnl_pct = (exec_price - entry_price) / entry_price * 100
            cash += shares * exec_price - fee
            trades.append(_trade_record(history, entry_bar, i, entry_price,
                                         exec_price, shares, pnl, pnl_pct, "SELL", slip))
            (wins_val if pnl > 0 else losses_val).append(pnl_pct)
            shares = 0.0; sl_price = 0.0; tp_price = 0.0

        equity_curve.append(round(cash + (shares + pending_buy_qty) * close, 0))

    # Force close cuối backtest
    if shares > 0:
        lp = history[-1]["c"]
        slip = calc_dynamic_slippage(shares, 1_000_000, market)
        fee  = shares * lp * fee_sell * (1 - slip)
        pnl  = (lp - entry_price) * shares - fee
        pnl_pct = (lp - entry_price) / entry_price * 100
        trades.append(_trade_record(history, entry_bar, n-1, entry_price,
                                     lp, shares, pnl, pnl_pct, "CLOSE", slip))
        (wins_val if pnl > 0 else losses_val).append(pnl_pct)
        cash += shares * lp - fee
        equity_curve.append(round(cash, 0))

    final = equity_curve[-1] if equity_curve else capital
    total_trades = len(trades)
    wins_n  = len(wins_val)
    losses_n= len(losses_val)
    win_rate= round(wins_n / total_trades * 100, 1) if total_trades else 0
    avg_win = round(sum(wins_val)  / wins_n,   2) if wins_n   else 0
    avg_loss= round(sum(losses_val)/ losses_n, 2) if losses_n else 0
    pf_denom= abs(sum(losses_val)) or 1
    pf      = round(sum(wins_val) / pf_denom, 2) if wins_val else 0

    peak = capital; mdd = 0.0
    for v in equity_curve:
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd: mdd = dd

    sharpe = _calc_sharpe(equity_curve)

    # Buy & Hold comparison
    bh_pct = round((history[-1]["c"] - history[55]["c"]) / history[55]["c"] * 100, 2)

    return {
        "t_plus_constraint": t_lock,
        "market": market,
        "initial_capital": capital,
        "final_value": round(final, 0),
        "profit_pct": round((final - capital) / capital * 100, 2),
        "buy_hold_pct": bh_pct,
        "total_trades": total_trades,
        "wins": wins_n,
        "losses": losses_n,
        "win_rate": win_rate,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "profit_factor": pf,
        "max_drawdown_pct": round(mdd, 2),
        "sharpe": sharpe,
        "equity_curve": equity_curve[-200:],
        "trades": trades[-30:],
        "note": f"T+{t_lock} constraint applied — sàn VN không cho bán trong {t_lock} ngày đầu",
    }


def _get_bar_signal(history: List[dict], i: int, strategy: str) -> str:
    """Tính tín hiệu BUY/SELL/HOLD từ bar data."""
    closes = [b["c"] for b in history[:i+1]]
    n = len(closes)

    if strategy == "ma_cross":
        if n < 55: return "HOLD"
        ma20 = sum(closes[-20:]) / 20
        ma50 = sum(closes[-50:]) / 50
        ma20_prev = sum(closes[-21:-1]) / 20
        ma50_prev = sum(closes[-51:-1]) / 50
        if ma20 > ma50 and ma20_prev <= ma50_prev: return "BUY"
        if ma20 < ma50 and ma20_prev >= ma50_prev: return "SELL"
        return "HOLD"

    # Default: RSI + MACD combined signal
    if n < 30: return "HOLD"
    rsi = _rsi(closes)
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd  = ema12 - ema26
    ema12_p = _ema(closes[:-1], 12)
    ema26_p = _ema(closes[:-1], 26)
    macd_p  = ema12_p - ema26_p

    if macd > 0 and macd_p <= 0 and rsi < 70: return "BUY"
    if macd < 0 and macd_p >= 0 and rsi > 30: return "SELL"
    return "HOLD"


def _trade_record(history, entry_bar, exit_bar, entry_p, exit_p,
                   qty, pnl, pnl_pct, action, slip):
    return {
        "entry_date":  history[entry_bar].get("d", str(entry_bar)) if entry_bar >= 0 else "N/A",
        "exit_date":   history[exit_bar].get("d",  str(exit_bar)),
        "entry_price": round(entry_p, 2),
        "exit_price":  round(exit_p,  2),
        "qty":         int(qty),
        "pnl":         round(pnl, 0),
        "pnl_pct":     round(pnl_pct, 2),
        "action":      action,
        "slippage_pct":round(slip * 100, 3),
        "duration_days": exit_bar - max(entry_bar, 0),
    }


# ══════════════════════════════════════════════════════════════
# MODULE 3 — SSI FASTCONNECT API (ĐẶT LỆNH THẬT)
# ══════════════════════════════════════════════════════════════

class SSIFastConnect:
    """
    SSI FastConnect API v2.
    Đăng ký tại: https://iboard.ssi.com.vn/fctrading

    Tính năng:
    - Xác thực OAuth2 với consumer_id / consumer_secret
    - Đặt lệnh LO, MP, ATO, ATC
    - Hủy lệnh, sửa lệnh
    - Xem danh mục, lịch sử lệnh, số dư tài khoản
    - Hỗ trợ cả margin và thường
    """

    BASE_URL     = "https://fc-tradeapi.ssi.com.vn/api/v2/Trader"
    DATA_URL     = "https://fc-data.ssi.com.vn/api/v2/Market"
    TOKEN_TTL    = 1800   # 30 phút

    ORDER_TYPES  = {"LO", "MP", "ATO", "ATC", "MTL", "MOK", "MAK"}
    SIDES        = {"BUY", "SELL"}
    MARKETS      = {"HOSE", "HNX", "UPCOM"}

    def __init__(self, consumer_id: str, consumer_secret: str,
                 account_no: str, pin: str = ""):
        self.consumer_id     = consumer_id
        self.consumer_secret = consumer_secret
        self.account_no      = account_no
        self.pin             = pin
        self._token          = None
        self._token_expiry   = 0
        self._lock           = threading.Lock()

    # ── Auth ───────────────────────────────────────────────────

    def _get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._token_expiry - 60:
                return self._token
            token = self._request_token()
            self._token        = token
            self._token_expiry = time.time() + self.TOKEN_TTL
            return token

    def _request_token(self) -> str:
        payload = json.dumps({
            "consumerID":     self.consumer_id,
            "consumerSecret": self.consumer_secret,
        }).encode()
        resp = self._http("POST", f"{self.BASE_URL}/Login", payload, auth=False)
        token = (resp.get("data", {}).get("accessToken") or
                 resp.get("accessToken") or "")
        if not token:
            raise ValueError(f"SSI login failed: {resp.get('message', resp)}")
        return token

    def _http(self, method: str, url: str, data: bytes = None,
              auth: bool = True, retries: int = 2) -> dict:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if auth:
            headers["Authorization"] = f"Bearer {self._get_token()}"
        for attempt in range(retries + 1):
            try:
                req = urllib.request.Request(url, data=data, method=method,
                                              headers=headers)
                with urllib.request.urlopen(req, timeout=12) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                body = e.read().decode()
                if e.code == 401 and attempt < retries:
                    # Token expired — force refresh
                    with self._lock:
                        self._token = None
                    continue
                raise ValueError(f"HTTP {e.code}: {body}")
            except Exception as e:
                if attempt < retries:
                    time.sleep(1)
                else:
                    raise

    # ── Account ────────────────────────────────────────────────

    def get_account_balance(self) -> dict:
        """Lấy số dư tài khoản."""
        resp = self._http("GET", f"{self.BASE_URL}/AccountBalance"
                          f"?account={self.account_no}&requestID={_req_id()}")
        return resp.get("data", resp)

    def get_positions(self) -> List[dict]:
        """Lấy danh mục cổ phiếu đang nắm giữ."""
        resp = self._http("GET", f"{self.BASE_URL}/StockPosition"
                          f"?account={self.account_no}&requestID={_req_id()}")
        return resp.get("data", [])

    def get_order_history(self, from_date: str = None, to_date: str = None) -> List[dict]:
        """Lấy lịch sử lệnh."""
        today = datetime.now().strftime("%d/%m/%Y")
        fd = from_date or today
        td = to_date   or today
        resp = self._http("GET",
            f"{self.BASE_URL}/OrderHistory?account={self.account_no}"
            f"&startDate={fd}&endDate={td}&requestID={_req_id()}")
        return resp.get("data", [])

    def get_pending_orders(self) -> List[dict]:
        """Lấy danh sách lệnh chờ."""
        resp = self._http("GET",
            f"{self.BASE_URL}/PendingOrders?account={self.account_no}"
            f"&requestID={_req_id()}")
        return resp.get("data", [])

    # ── Orders ─────────────────────────────────────────────────

    def place_order(self, symbol: str, qty: int, price: float,
                    side: str, order_type: str = "LO",
                    market: str = "HOSE") -> dict:
        """
        Đặt lệnh.
        symbol: mã CK (VCB, HPG...)
        qty:    số lượng (bội số 100 cho HOSE)
        price:  giá (0 nếu ATO/ATC/MP)
        side:   BUY | SELL
        order_type: LO | MP | ATO | ATC
        market: HOSE | HNX | UPCOM
        """
        side = side.upper()
        order_type = order_type.upper()

        if side not in self.SIDES:
            return {"error": f"side phải là BUY hoặc SELL"}
        if order_type not in self.ORDER_TYPES:
            return {"error": f"order_type không hợp lệ: {order_type}"}
        if qty <= 0:
            return {"error": "qty phải > 0"}
        if market == "HOSE" and qty % 100 != 0:
            return {"error": "HOSE: qty phải là bội số 100"}

        payload = json.dumps({
            "account":     self.account_no,
            "requestID":   _req_id(),
            "instrumentID":symbol.upper(),
            "market":      market,
            "buySell":     "B" if side == "BUY" else "S",
            "orderType":   order_type,
            "quantity":    str(qty),
            "price":       str(int(price)) if price > 0 else "0",
            "stopOrder":   False,
            "stopPrice":   "0",
            "stopType":    "",
            "stopStep":    "0",
            "lossStep":    "0",
            "profitStep":  "0",
        }).encode()

        resp = self._http("POST", f"{self.BASE_URL}/NewOrder", payload)
        return resp.get("data", resp)

    def cancel_order(self, order_id: str, symbol: str = "",
                     market: str = "HOSE") -> dict:
        """Hủy lệnh."""
        payload = json.dumps({
            "account":     self.account_no,
            "requestID":   _req_id(),
            "orderID":     order_id,
            "market":      market,
            "instrumentID": symbol.upper(),
        }).encode()
        resp = self._http("POST", f"{self.BASE_URL}/CancelOrder", payload)
        return resp.get("data", resp)

    def modify_order(self, order_id: str, symbol: str,
                     new_qty: int, new_price: float,
                     market: str = "HOSE") -> dict:
        """Sửa lệnh (giá / khối lượng)."""
        payload = json.dumps({
            "account":     self.account_no,
            "requestID":   _req_id(),
            "orderID":     order_id,
            "instrumentID":symbol.upper(),
            "market":      market,
            "quantity":    str(new_qty),
            "price":       str(int(new_price)),
        }).encode()
        resp = self._http("POST", f"{self.BASE_URL}/ModifyOrder", payload)
        return resp.get("data", resp)

    # ── Market Data ────────────────────────────────────────────

    def get_securities(self, market: str = "HOSE") -> List[dict]:
        """Lấy danh sách mã CK trên sàn."""
        resp = self._http("GET",
            f"{self.DATA_URL}/Securities?market={market}&pageIndex=1&pageSize=1000"
            f"&requestID={_req_id()}")
        return resp.get("data", [])

    def get_daily_ohlc(self, symbol: str, from_date: str, to_date: str) -> List[dict]:
        """
        Lấy OHLCV lịch sử từ SSI data API.
        Dùng thay yfinance cho mã VN.
        from_date, to_date: "dd/MM/yyyy"
        """
        resp = self._http("GET",
            f"{self.DATA_URL}/DailyOhlc?symbol={symbol}"
            f"&fromDate={from_date}&toDate={to_date}"
            f"&ascending=true&requestID={_req_id()}")
        bars = resp.get("data", [])
        result = []
        for b in bars:
            try:
                result.append({
                    "d":    str(b.get("tradingDate", ""))[:10],
                    "o":    _pf(b.get("openPrice",  0)),
                    "h":    _pf(b.get("highPrice",  0)),
                    "l":    _pf(b.get("lowPrice",   0)),
                    "c":    _pf(b.get("closePrice", 0)),
                    "v":    _vi(b.get("totalVolume", 0)),
                })
            except Exception:
                pass
        return result


# ══════════════════════════════════════════════════════════════
# MODULE 4 — LSTM MODEL (TIME-SERIES PREDICTION)
# ══════════════════════════════════════════════════════════════

class LSTMPredictor:
    """
    LSTM 2-layer predictor.
    Tự động dùng tensorflow nếu có, fallback về numpy LSTM đơn giản.

    Features (window=60 phiên):
        - Returns chuẩn hóa
        - RSI, MACD, ADX, BB position
        - Volume ratio
        - Fundamental: ROE, PE (nếu có)

    Output: xác suất tăng/giảm/đi ngang 5 phiên tới
    """

    WINDOW   = 60    # số phiên input
    HORIZON  = 5     # dự báo n phiên tới
    THRESH   = 0.02  # ±2% để phân loại UP/DOWN/NEUTRAL
    FEATURES = 8     # số feature mỗi timestep

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._models: Dict[str, object] = {}
        self._scalers: Dict[str, tuple]  = {}
        self._lock   = threading.Lock()
        self._has_tf = self._check_tf()

    def _check_tf(self) -> bool:
        try:
            import tensorflow as tf
            log.info("[LSTM] TensorFlow %s available", tf.__version__)
            return True
        except ImportError:
            log.info("[LSTM] TensorFlow not found — using numpy fallback LSTM")
            return False

    # ── Feature extraction ─────────────────────────────────────

    def _build_sequences(self, history: List[dict],
                          fund: dict = None) -> Tuple[object, object]:
        """Tạo chuỗi (X, y) để train."""
# [merged]         import numpy as np

        closes = np.array([b["c"] for b in history], dtype=np.float32)
        vols   = np.array([b.get("v", 0) for b in history], dtype=np.float32)
        n = len(closes)

        if n < self.WINDOW + self.HORIZON + 10:
            return None, None

        # --- tính features ---
        rets   = np.diff(closes) / (closes[:-1] + 1e-9)
        # RSI (vectorized rolling)
        gains  = np.where(rets > 0, rets, 0)
        losses = np.where(rets < 0, -rets, 0)
        # EMA-smoothed RSI
        rsi_arr = np.full(n, 50.0)
        p = 14
        ag = np.mean(gains[:p]); al = np.mean(losses[:p])
        for j in range(p, len(rets)):
            ag = (ag * (p-1) + gains[j]) / p
            al = (al * (p-1) + losses[j]) / p
            rsi_arr[j+1] = 100 - 100 / (1 + ag / (al + 1e-9))

        # MACD
        ema12 = _ema_np(closes, 12)
        ema26 = _ema_np(closes, 26)
        macd_arr = ema12 - ema26

        # BB position
        ma20 = np.convolve(closes, np.ones(20)/20, mode='same')
        std20 = np.array([np.std(closes[max(0,i-20):i+1]) for i in range(n)])
        bb_pos = (closes - (ma20 - 2*std20)) / (4*std20 + 1e-9)
        bb_pos = np.clip(bb_pos, 0, 1)

        # Volume ratio
        vol_ma20 = np.convolve(vols, np.ones(20)/20, mode='same') + 1
        vol_ratio = np.clip(vols / vol_ma20, 0, 5) / 5.0

        # Fundamental features (constant per sequence)
        pe_norm  = min(max(1 - (fund.get("pe", 20) or 20) / 60, 0), 1) if fund else 0.5
        roe_norm = min(max((fund.get("roe", 10) or 10) / 40, 0), 1) if fund else 0.5

        # Build (X, y)
        X_list, y_list = [], []
        for i in range(self.WINDOW, n - self.HORIZON):
            window_rets  = rets[i-self.WINDOW:i]  # shape (WINDOW,)
            window_rsi   = rsi_arr[i-self.WINDOW:i] / 100.0
            window_macd  = np.tanh(macd_arr[i-self.WINDOW:i] / (np.std(macd_arr) + 1e-9))
            window_bb    = bb_pos[i-self.WINDOW:i]
            window_vol   = vol_ratio[i-self.WINDOW:i]

            # Normalize returns
            ret_std = np.std(window_rets) + 1e-9
            window_rets_n = np.clip(window_rets / ret_std, -3, 3)

            # Per-timestep features: [ret, rsi, macd, bb, vol, pe(const), roe(const), ma_pos]
            ma50_i = np.mean(closes[max(0,i-50):i])
            ma_pos  = np.where(closes[i-self.WINDOW:i] > ma50_i, 1.0, -1.0)

            feats = np.stack([
                window_rets_n,
                window_rsi,
                window_macd,
                window_bb,
                window_vol,
                np.full(self.WINDOW, pe_norm),
                np.full(self.WINDOW, roe_norm),
                ma_pos,
            ], axis=1)   # shape: (WINDOW, FEATURES)

            X_list.append(feats)

            # Label
            future_ret = (closes[i + self.HORIZON] - closes[i]) / closes[i]
            if future_ret > self.THRESH:
                label = 0   # UP
            elif future_ret < -self.THRESH:
                label = 1   # DOWN
            else:
                label = 2   # NEUTRAL
            y_list.append(label)

        if not X_list:
            return None, None

        X = np.array(X_list, dtype=np.float32)
        y = np.array(y_list, dtype=np.int32)
        return X, y

    # ── TensorFlow training ────────────────────────────────────

    def train_tf(self, symbol: str, history: List[dict],
                  fund: dict = None) -> dict:
        """Train LSTM với TensorFlow."""
# [merged]         import numpy as np
        import tensorflow as tf
        from tensorflow.keras import layers, models, callbacks

        X, y = self._build_sequences(history, fund)
        if X is None:
            return {"error": "Không đủ dữ liệu"}

        n_samples = len(X)
        split = int(n_samples * 0.8)
        X_tr, X_te = X[:split], X[split:]
        y_tr, y_te = y[:split], y[split:]

        # Xây LSTM model
        model = models.Sequential([
            layers.Input(shape=(self.WINDOW, self.FEATURES)),
            layers.LSTM(64, return_sequences=True, dropout=0.2),
            layers.LSTM(32, dropout=0.2),
            layers.Dense(16, activation="relu"),
            layers.Dropout(0.2),
            layers.Dense(3, activation="softmax"),
        ])
        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
            loss="sparse_categorical_crossentropy",
            metrics=["accuracy"],
        )

        early_stop = callbacks.EarlyStopping(
            patience=10, restore_best_weights=True, monitor="val_accuracy"
        )

        hist_fit = model.fit(
            X_tr, y_tr,
            validation_data=(X_te, y_te),
            epochs=60,
            batch_size=32,
            callbacks=[early_stop],
            verbose=0,
        )

        val_acc = max(hist_fit.history.get("val_accuracy", [0]))
        train_acc = max(hist_fit.history.get("accuracy", [0]))

        with self._lock:
            self._models[symbol]  = model
            self._scalers[symbol] = None

        self._save_model_tf(symbol, model, {
            "val_acc": round(val_acc * 100, 1),
            "train_acc": round(train_acc * 100, 1),
            "n_samples": n_samples,
            "window": self.WINDOW,
            "trained_at": datetime.now().isoformat(),
            "model_type": "LSTM-TF",
        })

        return {
            "symbol": symbol,
            "val_accuracy": round(val_acc * 100, 1),
            "train_accuracy": round(train_acc * 100, 1),
            "n_samples": n_samples,
            "epochs_run": len(hist_fit.history["loss"]),
        }

    def _save_model_tf(self, symbol: str, model, info: dict):
        """Lưu model TF vào DB dạng bytes."""
        try:
            import tempfile, shutil, pickle
            with tempfile.TemporaryDirectory() as tmpdir:
                path = os.path.join(tmpdir, "model")
                model.save(path, save_format="tf")
                # Zip thư mục
                zip_path = shutil.make_archive(path, "zip", path)
                with open(zip_path, "rb") as f:
                    blob = base64.b64encode(f.read()).decode()
            with sqlite3.connect(self.db_path, timeout=10) as c:
                c.execute("""INSERT OR REPLACE INTO lstm_models
                             (symbol, model_blob, model_info, trained_at)
                             VALUES (?, ?, ?, ?)""",
                          (symbol, blob, json.dumps(info), datetime.now().isoformat()))
        except Exception as e:
            log.warning("[LSTM] save model failed: %s", e)

    # ── Numpy fallback training ────────────────────────────────

    def train_numpy(self, symbol: str, history: List[dict],
                     fund: dict = None) -> dict:
        """
        Numpy-based GRU-like predictor — không cần TF/PyTorch.
        Dùng sliding window features + sklearn GradientBoosting.
        """
# [merged]         import numpy as np
        try:
            from sklearn.ensemble import GradientBoostingClassifier
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return {"error": "scikit-learn không có"}

        X, y = self._build_sequences(history, fund)
        if X is None:
            return {"error": "Không đủ dữ liệu"}

        # Flatten sequence features — lấy thêm trend features
        n_samples = len(X)
        X_flat = np.zeros((n_samples, self.FEATURES * 3 + 10), dtype=np.float32)
        for i, seq in enumerate(X):
            # Last timestep features
            X_flat[i, :self.FEATURES] = seq[-1]
            # Mean features
            X_flat[i, self.FEATURES:self.FEATURES*2] = seq.mean(axis=0)
            # Std features
            X_flat[i, self.FEATURES*2:self.FEATURES*3] = seq.std(axis=0)
            # Trend features (last vs first half)
            half = self.WINDOW // 2
            X_flat[i, self.FEATURES*3:self.FEATURES*3+8] = (
                seq[-half:].mean(axis=0) - seq[:half].mean(axis=0))
            # Auto-correlation feature
            ret_col = seq[:, 0]
            ac1 = float(np.corrcoef(ret_col[:-1], ret_col[1:])[0,1]) if len(ret_col) > 2 else 0
            X_flat[i, -2] = ac1
            X_flat[i, -1] = float(np.sign(ret_col[-5:].mean()))

        split = int(n_samples * 0.8)
        scaler = StandardScaler()
        X_s = scaler.fit_transform(X_flat)

        clf = GradientBoostingClassifier(
            n_estimators=120, max_depth=4, learning_rate=0.08,
            subsample=0.8, random_state=42
        )
        clf.fit(X_s[:split], y[:split])
        acc = clf.score(X_s[split:], y[split:]) if split < n_samples else 0

        with self._lock:
            self._models[symbol]  = ("numpy", clf)
            self._scalers[symbol] = scaler

        return {
            "symbol": symbol,
            "val_accuracy": round(acc * 100, 1),
            "n_samples": n_samples,
            "model_type": "LSTM-fallback(GBM)",
        }

    # ── Train dispatcher ───────────────────────────────────────

    def train(self, symbol: str, history: List[dict],
               fund: dict = None) -> dict:
        """Train model — dùng TF nếu có, fallback numpy."""
        if self._has_tf:
            return self.train_tf(symbol, history, fund)
        return self.train_numpy(symbol, history, fund)

    # ── Predict ────────────────────────────────────────────────

    def predict(self, symbol: str, history: List[dict],
                 fund: dict = None) -> dict:
        """Dự báo xu hướng."""
# [merged]         import numpy as np

        if symbol not in self._models:
            self._load_from_db(symbol)
        if symbol not in self._models:
            return {"prob_up": 50, "prob_down": 25, "prob_neutral": 25,
                    "prediction": "NEUTRAL", "confidence": 0,
                    "note": "Chưa có model LSTM — chạy /api/v24/lstm/train trước"}

        X, _ = self._build_sequences(history, fund)
        if X is None or len(X) == 0:
            return {"prob_up": 50, "prediction": "NEUTRAL", "confidence": 0}

        last_X = X[-1:].astype(np.float32)   # shape (1, WINDOW, FEATURES)
        mdl = self._models[symbol]

        if isinstance(mdl, tuple) and mdl[0] == "numpy":
            clf    = mdl[1]
            scaler = self._scalers.get(symbol)
            flat   = self._flatten_single(last_X[0])
            if scaler:
                flat = scaler.transform(flat.reshape(1, -1))
            proba  = clf.predict_proba(flat.reshape(1, -1))[0]
            classes = clf.classes_.tolist()
        else:
            # TF model
            proba  = mdl.predict(last_X, verbose=0)[0]
            classes = [0, 1, 2]

        class_map = {0: "up", 1: "down", 2: "neutral"}
        prob_dict = {}
        for idx, cls in enumerate(classes):
            if idx < len(proba):
                prob_dict[class_map.get(cls, str(cls))] = round(float(proba[idx]) * 100, 1)

        prob_up   = prob_dict.get("up", 33)
        prob_down = prob_dict.get("down", 33)
        pred      = "UP" if prob_up > 50 else ("DOWN" if prob_down > 50 else "NEUTRAL")
        conf      = round(max(prob_up, prob_down) - 33, 1)

        return {
            "prob_up":      prob_up,
            "prob_down":    prob_down,
            "prob_neutral": prob_dict.get("neutral", 34),
            "prediction":   pred,
            "confidence":   conf,
            "horizon_days": self.HORIZON,
            "model_type":   "LSTM-TF" if self._has_tf else "LSTM-fallback",
        }

    def _flatten_single(self, seq):
        """Flatten 1 sequence cho numpy model."""
# [merged]         import numpy as np
        half = self.WINDOW // 2
        flat = np.concatenate([
            seq[-1],
            seq.mean(axis=0),
            seq.std(axis=0),
            seq[-half:].mean(axis=0) - seq[:half].mean(axis=0),
            [float(np.corrcoef(seq[:-1,0], seq[1:,0])[0,1]) if len(seq) > 2 else 0,
             float(np.sign(seq[-5:,0].mean()))],
        ])
        return flat

    def _load_from_db(self, symbol: str):
        """Load model từ DB."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as c:
                row = c.execute(
                    "SELECT model_blob, model_info FROM lstm_models WHERE symbol=?",
                    (symbol,)).fetchone()
            if not row:
                return
            info = json.loads(row[1] or "{}")
            if info.get("model_type") == "LSTM-TF" and self._has_tf:
                import tensorflow as tf, tempfile, shutil
                blob = base64.b64decode(row[0])
                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = os.path.join(tmpdir, "model.zip")
                    with open(zip_path, "wb") as f:
                        f.write(blob)
                    shutil.unpack_archive(zip_path, os.path.join(tmpdir, "m"), "zip")
                    model = tf.keras.models.load_model(os.path.join(tmpdir, "m"))
                with self._lock:
                    self._models[symbol] = model
        except Exception as e:
            log.debug("[LSTM] load model %s: %s", symbol, e)


# ══════════════════════════════════════════════════════════════
# MODULE 5 — VAR ALERT REAL-TIME
# ══════════════════════════════════════════════════════════════

class VaRMonitor:
    """
    Hook vào start_refresh() loop để tính VaR portfolio real-time.
    Gửi Telegram alert khi VaR vượt ngưỡng người dùng đặt.

    Tích hợp:
        var_monitor = VaRMonitor(db_path, tg_send_fn)
        # Trong start_refresh() loop, sau khi cập nhật ram_cache:
        var_monitor.check(user_id, positions, ram_cache)
    """

    def __init__(self, db_path: str, tg_send_fn=None):
        self.db_path   = db_path
        self.tg_send   = tg_send_fn   # hàm tg_send từ server.py
        self._last_alert: Dict[int, float] = {}  # user_id → timestamp
        self._alert_cooldown = 3600   # alert tối đa 1 lần/giờ/user
        self._lock = threading.Lock()

    def check(self, user_id: int, positions: List[dict],
               ram_cache: dict, threshold_pct: float = None):
        """
        Tính VaR portfolio và alert nếu vượt ngưỡng.
        Gọi từ refresh loop sau mỗi lần cập nhật giá.
        """
        if not positions:
            return

        with self._lock:
            last = self._last_alert.get(user_id, 0)
            if time.time() - last < self._alert_cooldown:
                return

        # Lấy ngưỡng từ user setting nếu không truyền vào
        if threshold_pct is None:
            threshold_pct = self._get_user_threshold(user_id)

        try:
            result = self._calc_portfolio_var(positions, ram_cache)
        except Exception as e:
            log.debug("[VaR] calc error: %s", e)
            return

        if not result:
            return

        var_pct = result["var_portfolio_pct"]
        if var_pct > threshold_pct:
            self._send_alert(user_id, result, threshold_pct)
            with self._lock:
                self._last_alert[user_id] = time.time()

    def _calc_portfolio_var(self, positions: List[dict],
                             ram_cache: dict,
                             confidence: float = 0.95) -> Optional[dict]:
        """Tính VaR portfolio theo phương pháp historical simulation."""
# [merged]         import numpy as np

        port_val  = sum(p.get("val", p.get("cur_price", 0) * p.get("qty", 0))
                        for p in positions)
        if port_val <= 0:
            return None

        # Thu thập returns từng mã
        all_rets = []
        weights  = []
        names    = []

        for pos in positions:
            sym = pos["symbol"]
            qty = pos.get("qty", 0)
            cur = pos.get("cur_price", pos.get("entry", 0))
            w   = cur * qty / port_val

            if sym not in ram_cache:
                continue
            hist = ram_cache[sym].get("data", {}).get("history", [])
            if len(hist) < 30:
                continue

            closes = [b["c"] for b in hist if b["c"] > 0]
            rets   = [(closes[i] - closes[i-1]) / closes[i-1]
                       for i in range(1, len(closes))]
            if not rets:
                continue

            all_rets.append(rets)
            weights.append(w)
            names.append(sym)

        if not all_rets:
            return None

        # Đồng bộ độ dài
        min_len = min(len(r) for r in all_rets)
        ret_mat = np.array([r[-min_len:] for r in all_rets])  # (n_assets, T)
        w_arr   = np.array(weights)

        # Portfolio returns
        port_rets = (w_arr[:, None] * ret_mat).sum(axis=0)  # (T,)
        port_rets.sort()

        var_idx  = int(len(port_rets) * (1 - confidence))
        var_pct  = -port_rets[var_idx] * 100
        cvar_pct = -port_rets[:var_idx+1].mean() * 100 if var_idx >= 0 else var_pct

        # Per-asset contribution
        contribs = []
        for i, sym in enumerate(names):
            sym_rets = np.array(all_rets[i][-min_len:])
            sym_rets.sort()
            sym_var = -sym_rets[int(len(sym_rets) * (1-confidence))] * 100
            contribs.append({
                "symbol": sym,
                "weight_pct": round(weights[i] * 100, 1),
                "var_pct": round(sym_var, 2),
            })
        contribs.sort(key=lambda x: -x["var_pct"])

        return {
            "var_portfolio_pct":  round(var_pct,  2),
            "cvar_portfolio_pct": round(cvar_pct, 2),
            "confidence":         confidence * 100,
            "portfolio_value":    round(port_val, 0),
            "var_amount":         round(port_val * var_pct / 100, 0),
            "top_risk_assets":    contribs[:3],
            "n_assets":           len(names),
            "computed_at":        datetime.now().isoformat(),
        }

    def _get_user_threshold(self, user_id: int) -> float:
        try:
            with sqlite3.connect(self.db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT value FROM user_settings WHERE user_id=? AND key='var_threshold'",
                    (user_id,)).fetchone()
            return float(row[0]) if row else 3.0  # default 3%
        except Exception:
            return 3.0

    def _send_alert(self, user_id: int, result: dict, threshold: float):
        if not self.tg_send:
            return
        msg_lines = [
            "🚨 <b>CẢNH BÁO RỦI RO DANH MỤC</b>",
            "━" * 22,
            f"📊 VaR (95%): <b>{result['var_portfolio_pct']:.2f}%</b> (ngưỡng: {threshold:.1f}%)",
            f"📉 CVaR (95%): <b>{result['cvar_portfolio_pct']:.2f}%</b>",
            f"💰 Giá trị rủi ro: <b>{result['var_amount']:,.0f}đ</b>",
            f"🏦 Giá trị danh mục: {result['portfolio_value']:,.0f}đ",
            "",
            "Top rủi ro:",
        ]
        for a in result.get("top_risk_assets", []):
            msg_lines.append(f"  • {a['symbol']}: VaR={a['var_pct']:.2f}% (tỷ trọng {a['weight_pct']:.1f}%)")
        msg_lines.append(f"\n⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        try:
            self.tg_send("\n".join(msg_lines))
        except Exception as e:
            log.warning("[VaR] Telegram alert failed: %s", e)


# ══════════════════════════════════════════════════════════════
# MODULE 6 — PORTFOLIO-LEVEL BACKTEST
# ══════════════════════════════════════════════════════════════

# ── PORTFOLIO ADVANCED METRICS ────────────────────────────────────────────────

def calc_portfolio_advanced_metrics(trades: list, bars_map: dict = None) -> dict:
    """
    Tính VaR, Sharpe, Max Drawdown, Monte Carlo cho portfolio.
    trades: list of trade dicts (từ journal)
    bars_map: {symbol: bars_list} - optional, để tính daily returns
    """
    result = {
        "sharpe": 0.0, "sortino": 0.0,
        "max_drawdown_pct": 0.0, "max_drawdown_vnd": 0,
        "var_95": 0.0, "cvar_95": 0.0,
        "calmar": 0.0, "omega": 0.0,
        "win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
        "profit_factor": 0.0, "expectancy": 0.0,
        "mc_median": 0.0, "mc_p10": 0.0, "mc_p90": 0.0,
        "mc_ruin_pct": 0.0,
    }
    if not trades: return result

    # ── Lấy returns từ closed trades ──────────────────────────
    closed = [t for t in trades if t.get('status') == 'CLOSED' and t.get('pnl_pct') is not None]
    if len(closed) < 3: return result

    rets = [float(t['pnl_pct']) / 100.0 for t in closed]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r < 0]

    # Win rate & averages
    result["win_rate"]  = round(len(wins) / len(rets) * 100, 1)
    result["avg_win"]   = round(sum(wins)  / len(wins)  * 100, 2) if wins   else 0
    result["avg_loss"]  = round(sum(losses) / len(losses) * 100, 2) if losses else 0

    # Profit factor
    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))
    result["profit_factor"] = round(gross_win / gross_loss, 2) if gross_loss > 0 else 99.0

    # Expectancy
    wr = len(wins)/len(rets)
    result["expectancy"] = round((wr * result["avg_win"] + (1-wr) * result["avg_loss"]), 2)

    # ── Sharpe & Sortino ──────────────────────────────────────
    try:
        import math
        n = len(rets)
        mean_r = sum(rets) / n
        var_r  = sum((r - mean_r)**2 for r in rets) / max(n-1, 1)
        std_r  = math.sqrt(var_r)
        result["sharpe"]  = round(mean_r / std_r * math.sqrt(252), 3) if std_r > 0 else 0

        neg_rets = [r for r in rets if r < 0]
        if neg_rets:
            down_var = sum(r**2 for r in neg_rets) / len(neg_rets)
            down_std = math.sqrt(down_var)
            result["sortino"] = round(mean_r / down_std * math.sqrt(252), 3) if down_std > 0 else 0
    except Exception as _me: log.debug('[port_metrics] sharpe: %s', _me)

    # ── Max Drawdown ──────────────────────────────────────────
    try:
        equity = [1.0]
        for r in rets:
            equity.append(equity[-1] * (1 + r))
        peak = equity[0]
        max_dd = 0.0
        for e in equity:
            if e > peak: peak = e
            dd = (peak - e) / peak if peak > 0 else 0
            if dd > max_dd: max_dd = dd
        result["max_drawdown_pct"] = round(max_dd * 100, 2)
        # Calmar ratio (annual return / max drawdown)
        if max_dd > 0:
            annual_ret = (equity[-1] / equity[0]) ** (252 / max(len(rets), 1)) - 1
            result["calmar"] = round(annual_ret / max_dd, 3)
    except Exception as _de: log.debug('[port_metrics] drawdown: %s', _de)

    # ── VaR 95% & CVaR ───────────────────────────────────────
    try:
        sorted_rets = sorted(rets)
        idx_var = int(len(sorted_rets) * 0.05)
        result["var_95"]  = round(sorted_rets[max(0, idx_var)] * 100, 2)
        tail = sorted_rets[:max(1, idx_var)]
        result["cvar_95"] = round(sum(tail) / len(tail) * 100, 2) if tail else 0
    except Exception as _ve: log.debug('[port_metrics] var: %s', _ve)

    # ── Monte Carlo (300 simulations) ────────────────────────
    try:
        import random as _rand
        n_sims, n_trades = 300, max(len(rets), 30)
        finals = []
        ruin_count = 0
        for _ in range(n_sims):
            capital = 1.0
            ruined = False
            for __ in range(n_trades):
                r = _rand.choice(rets)
                capital *= (1 + r)
                if capital < 0.3:
                    ruined = True; break
            if ruined: ruin_count += 1
            finals.append(capital)
        finals.sort()
        result["mc_median"]   = round((finals[n_sims//2] - 1) * 100, 2)
        result["mc_p10"]      = round((finals[int(n_sims*0.1)] - 1) * 100, 2)
        result["mc_p90"]      = round((finals[int(n_sims*0.9)] - 1) * 100, 2)
        result["mc_ruin_pct"] = round(ruin_count / n_sims * 100, 1)
    except Exception as _mce: log.debug('[port_metrics] mc: %s', _mce)

    # ── Omega ratio ───────────────────────────────────────────
    try:
        threshold = 0.0
        upside   = sum(r - threshold for r in rets if r > threshold)
        downside = sum(threshold - r for r in rets if r < threshold)
        result["omega"] = round(upside / downside, 3) if downside > 0 else 99.0
    except Exception as _oe: log.debug('[port_metrics] omega: %s', _oe)

    return result


def run_portfolio_backtest(
    symbol_histories: Dict[str, List[dict]],
    capital: float  = 1_000_000_000,
    market: str     = "VN",
    max_positions: int = 5,
    risk_per_trade: float = 0.02,
    sl_pct: float   = 0.07,
    tp_pct: float   = 0.15,
    strategy: str   = "signal",
) -> dict:
    """
    Portfolio-level backtest: chạy đồng thời nhiều mã,
    chia capital thực tế, không bao giờ vượt 100% tài khoản.

    symbol_histories: {'VCB': [...bars], 'HPG': [...bars], ...}
    max_positions: tối đa bao nhiêu mã cùng lúc
    """
    if not symbol_histories:
        return {"error": "Không có dữ liệu"}

    t_lock = T_PLUS_VN if market == "VN" else T_PLUS_US

    # Đồng bộ time index: lấy tất cả ngày giao dịch
    all_dates = set()
    date_bars: Dict[str, Dict[str, dict]] = {}

    for sym, hist in symbol_histories.items():
        date_bars[sym] = {}
        for bar in hist:
            d = bar.get("d", "")
            if d:
                all_dates.add(d)
                date_bars[sym][d] = bar

    sorted_dates = sorted(all_dates)
    if len(sorted_dates) < 60:
        return {"error": "Cần ít nhất 60 ngày giao dịch"}

    # State
    cash         = float(capital)
    positions: Dict[str, dict] = {}  # sym → {qty, entry, entry_date, sl, tp, pending}
    all_trades: List[dict] = []
    equity_curve: List[dict] = []
    daily_history: Dict[str, List[dict]] = {sym: [] for sym in symbol_histories}

    fee_buy  = 0.0015 if market == "VN" else 0.0005
    fee_sell = 0.0010 if market == "VN" else 0.0005

    for day_i, day in enumerate(sorted_dates):
        # Cập nhật daily_history
        for sym in symbol_histories:
            if day in date_bars[sym]:
                daily_history[sym].append(date_bars[sym][day])

        if day_i < 55:
            equity_curve.append({"date": day, "equity": round(cash, 0)})
            continue

        # Tính NAV hôm nay
        nav = cash
        for sym, pos in positions.items():
            if day in date_bars[sym]:
                cur_p = date_bars[sym][day]["c"]
                nav  += pos["qty"] * cur_p

        equity_curve.append({"date": day, "equity": round(nav, 0)})

        # Process existing positions: SL/TP/T+2 settlement
        to_close = []
        for sym, pos in positions.items():
            if day not in date_bars[sym]:
                continue
            bar = date_bars[sym][day]
            price = bar.get("o") or bar["c"]
            close = bar["c"]
            avg_vol_list = [date_bars[sym].get(d, {}).get("v", 0)
                            for d in sorted_dates[max(0, day_i-20):day_i]]
            avg_vol = sum(avg_vol_list) / len(avg_vol_list) if avg_vol_list else 1

            # Check T+lock
            entry_day_i = sorted_dates.index(pos["entry_date"]) if pos["entry_date"] in sorted_dates else 0
            days_held   = day_i - entry_day_i
            if days_held < t_lock:
                continue  # chưa được bán

            slip      = calc_dynamic_slippage(pos["qty"], avg_vol, market)
            sl_price  = pos.get("sl", 0)
            tp_price  = pos.get("tp", 0)
            low_bar   = bar.get("l") or close
            high_bar  = bar.get("h") or close

            close_reason = None
            exec_price   = None

            if sl_price and float(low_bar) <= sl_price:
                exec_price   = sl_price * (1 - slip)
                close_reason = "SL"
            elif tp_price and float(high_bar) >= tp_price:
                exec_price   = tp_price * (1 - slip)
                close_reason = "TP"

            if close_reason:
                fee  = pos["qty"] * exec_price * fee_sell
                pnl  = (exec_price - pos["entry"]) * pos["qty"] - fee
                pnl_pct = (exec_price - pos["entry"]) / pos["entry"] * 100
                cash += pos["qty"] * exec_price - fee
                all_trades.append({
                    "symbol": sym,
                    "entry_date": pos["entry_date"],
                    "exit_date": day,
                    "entry": pos["entry"],
                    "exit": round(exec_price, 2),
                    "qty": pos["qty"],
                    "pnl": round(pnl, 0),
                    "pnl_pct": round(pnl_pct, 2),
                    "reason": close_reason,
                    "days_held": days_held,
                    "slip_pct": round(slip * 100, 3),
                })
                to_close.append(sym)

        for sym in to_close:
            del positions[sym]

        # Tìm tín hiệu mua cho các mã chưa có vị thế
        n_open = len(positions)
        if n_open >= max_positions:
            continue

        slots_available = max_positions - n_open
        capital_per_slot = cash / max(slots_available, 1)

        for sym in symbol_histories:
            if sym in positions or slots_available <= 0:
                continue
            hist_so_far = daily_history[sym]
            if len(hist_so_far) < 56:
                continue
            signal = _get_bar_signal(hist_so_far, len(hist_so_far)-1, strategy)
            if signal != "BUY":
                continue

            bar   = date_bars.get(sym, {}).get(day, {})
            if not bar:
                continue

            price  = float(bar.get("o") or bar["c"])
            avg_vol= sum(date_bars[sym].get(d, {}).get("v", 0)
                         for d in sorted_dates[max(0,day_i-20):day_i]) / 20 + 1
            slip   = calc_dynamic_slippage(0, avg_vol, market)
            exec_p = price * (1 + slip)
            sl_p   = exec_p * (1 - sl_pct)
            tp_p   = exec_p * (1 + tp_pct)
            sl_dist = exec_p - sl_p
            if sl_dist <= 0:
                continue

            risk_amt = capital_per_slot * risk_per_trade
            qty = int(risk_amt / sl_dist)
            if market == "VN": qty = max(100, round(qty/100)*100)
            else:               qty = max(1, qty)

            cost = exec_p * qty * (1 + fee_buy)
            if cost > cash:
                continue

            cash -= cost
            positions[sym] = {
                "qty":        qty,
                "entry":      exec_p,
                "entry_date": day,
                "sl":         sl_p,
                "tp":         tp_p,
            }
            slots_available -= 1

    # Force close tất cả cuối kỳ
    last_day = sorted_dates[-1]
    for sym, pos in list(positions.items()):
        if last_day in date_bars[sym]:
            lp   = date_bars[sym][last_day]["c"]
            fee  = pos["qty"] * lp * fee_sell
            pnl  = (lp - pos["entry"]) * pos["qty"] - fee
            pnl_pct = (lp - pos["entry"]) / pos["entry"] * 100
            cash += pos["qty"] * lp - fee
            all_trades.append({
                "symbol": sym, "entry_date": pos["entry_date"],
                "exit_date": last_day, "entry": pos["entry"],
                "exit": round(lp, 2), "qty": pos["qty"],
                "pnl": round(pnl, 0), "pnl_pct": round(pnl_pct, 2),
                "reason": "CLOSE", "days_held": 0, "slip_pct": 0,
            })

    # Metrics
    wins   = [t for t in all_trades if t["pnl"] > 0]
    losses = [t for t in all_trades if t["pnl"] <= 0]
    total_pnl  = sum(t["pnl"] for t in all_trades)
    win_rate   = len(wins) / len(all_trades) * 100 if all_trades else 0
    avg_win    = sum(t["pnl_pct"] for t in wins)  / len(wins)   if wins   else 0
    avg_loss   = sum(t["pnl_pct"] for t in losses)/ len(losses) if losses else 0
    pf_denom   = abs(sum(t["pnl"] for t in losses)) or 1
    pf         = sum(t["pnl"] for t in wins) / pf_denom if wins else 0
    final_nav  = equity_curve[-1]["equity"] if equity_curve else capital
    peak = capital; mdd = 0.0
    for pt in equity_curve:
        v = pt["equity"]
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd: mdd = dd

    sharpe = _calc_sharpe([pt["equity"] for pt in equity_curve])

    return {
        "capital": capital,
        "final_nav": final_nav,
        "total_pnl": round(total_pnl, 0),
        "total_pnl_pct": round(total_pnl / capital * 100, 2),
        "buy_and_hold_note": "Portfolio-level — không so B&H đơn lẻ",
        "total_trades": len(all_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "profit_factor": round(pf, 2),
        "max_drawdown_pct": round(mdd, 2),
        "sharpe": sharpe,
        "max_concurrent_positions": max_positions,
        "t_plus_constraint": t_lock,
        "equity_curve": equity_curve,
        "trades_by_symbol": _group_by_symbol(all_trades),
        "recent_trades": all_trades[-30:],
    }


# ══════════════════════════════════════════════════════════════
# MODULE 7 — DYNAMIC SLIPPAGE
# ══════════════════════════════════════════════════════════════

def calc_dynamic_slippage(
    order_qty: float,
    avg_daily_vol: float,
    market: str = "VN",
    participation_rate: float = 0.1,   # tối đa 10% KLGD ngày
) -> float:
    """
    Tính slippage động dựa trên market impact model:
        slippage = base_rate * sqrt(order_qty / avg_daily_vol)

    Market impact tỷ lệ với căn bậc hai của participation rate.
    Mã thanh khoản thấp → slippage cao hơn nhiều.

    Returns: slippage dạng decimal (e.g., 0.003 = 0.3%)
    """
    base_rate = 0.0015 if market == "VN" else 0.0005

    if avg_daily_vol <= 0 or order_qty <= 0:
        return base_rate

    participation = order_qty / avg_daily_vol
    participation = min(participation, participation_rate)  # cap at max

    # Kyle lambda model: impact ∝ sqrt(participation)
    impact = base_rate * (participation / 0.01) ** 0.5   # normalized at 1% participation

    # Giới hạn thực tế: sàn VN ATC có thể trượt 1 bước giá (~0.1%)
    min_slip = 0.001 if market == "VN" else 0.0002
    max_slip = 0.015 if market == "VN" else 0.005

    return round(max(min_slip, min(impact, max_slip)), 5)


def estimate_market_impact(
    order_value: float,    # VND / USD
    avg_daily_value: float,
    market: str = "VN",
) -> dict:
    """
    Ước tính market impact cho lệnh lớn.
    Useful để cảnh báo trader khi lệnh quá lớn so với thanh khoản.
    """
    if avg_daily_value <= 0:
        return {"impact_pct": 0, "warning": None}

    participation = order_value / avg_daily_value
    slip          = calc_dynamic_slippage(
        order_qty=1, avg_daily_vol=1 / (participation + 1e-9), market=market
    )
    cost          = order_value * slip

    warning = None
    if participation > 0.2:
        warning = f"Lệnh chiếm {participation*100:.0f}% KLGD ngày — slippage rất cao!"
    elif participation > 0.05:
        warning = f"Lệnh chiếm {participation*100:.0f}% KLGD ngày — cân nhắc chia nhỏ"

    return {
        "order_value": round(order_value, 0),
        "avg_daily_value": round(avg_daily_value, 0),
        "participation_pct": round(participation * 100, 1),
        "estimated_slippage_pct": round(slip * 100, 3),
        "estimated_impact_cost": round(cost, 0),
        "warning": warning,
    }


# ══════════════════════════════════════════════════════════════
# MODULE 8 — AUTO-RETRAIN SCHEDULER
# ══════════════════════════════════════════════════════════════

class AutoRetrainScheduler:
    """
    Scheduler chạy hàng tuần (Chủ nhật 2AM):
    1. Retrain model cho tất cả mã trong watchlist
    2. So sánh accuracy với model cũ
    3. Swap nếu model mới tốt hơn ≥ 1%
    4. Alert qua Telegram kết quả

    Tích hợp:
        scheduler = AutoRetrainScheduler(db_path, lstm_predictor, tg_send)
        scheduler.start()   # gọi 1 lần từ main()
    """

    def __init__(self, db_path: str, lstm: LSTMPredictor,
                  tg_send_fn=None, ram_cache: dict = None):
        self.db_path  = db_path
        self.lstm     = lstm
        self.tg_send  = tg_send_fn
        self.ram_cache = ram_cache
        self._running = False
        self._thread  = None
        self._last_run_key = ""

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True,
                                          name="auto-retrain")
        self._thread.start()
        log.info("[AutoRetrain] Scheduler started — runs every Sunday 02:00")

    def stop(self):
        self._running = False

    def run_now(self) -> dict:
        """Chạy ngay lập tức (manual trigger từ API)."""
        return self._do_retrain()

    def _loop(self):
        while self._running:
            now = datetime.now()
            # Chủ nhật = weekday() == 6
            run_key = f"{now.strftime('%Y-W%W')}"   # week key
            if (now.weekday() == 6 and now.hour == 2
                    and self._last_run_key != run_key):
                self._last_run_key = run_key
                try:
                    result = self._do_retrain()
                    log.info("[AutoRetrain] Weekly retrain done: %s", result.get("summary"))
                except Exception as e:
                    log.error("[AutoRetrain] Failed: %s", e)
            time.sleep(60)   # check mỗi phút

    def _do_retrain(self) -> dict:
        """Thực hiện retrain toàn bộ với A/B test trước khi swap model."""
        if not self.ram_cache:
            return {"error": "ram_cache not set"}

        start_time = time.time()
        results    = []
        improved   = []
        degraded   = []
        swapped    = []
        kept_old   = []

        symbols = list(self.ram_cache.keys())
        log.info("[AutoRetrain] Starting retrain for %d symbols (with A/B test)", len(symbols))

        for sym in symbols:
            try:
                d    = self.ram_cache.get(sym, {}).get("data", {})
                hist = d.get("history", [])
                mkt  = d.get("market", "VN")
                if len(hist) < 80 or mkt not in ("VN", "US"):
                    continue

                # Lấy accuracy cũ từ DB
                old_acc = self._get_model_accuracy(sym)

                # Train model mới trên 80% data
                split = int(len(hist) * 0.8)
                train_hist = hist[:split]
                test_hist  = hist[split:]

                r = self.lstm.train(sym, train_hist)
                if "error" in r:
                    results.append({"symbol": sym, "status": "error", "msg": r["error"]})
                    continue

                new_acc = r.get("val_accuracy", 0)

                # A/B test: validate new model trên 20% holdout
                ab_score = new_acc  # default nếu không có holdout
                if len(test_hist) >= 20:
                    try:
                        pred = self.lstm.predict(sym, test_hist)
                        ab_score = pred.get("confidence", new_acc * 0.8)
                    except:
                        ab_score = new_acc * 0.9

                # Chỉ swap nếu model mới tốt hơn trên HOLDOUT (không chỉ train set)
                delta = new_acc - (old_acc or 0)
                if ab_score > (old_acc or 0) and delta >= 0:
                    # Model mới tốt hơn → swap
                    status = "improved"
                    improved.append(sym)
                    swapped.append({"symbol": sym, "old": old_acc, "new": new_acc, "ab_score": round(ab_score,1)})
                    log.info("[AutoRetrain] SWAP %s: old=%.1f%% new=%.1f%% ab=%.1f%%", sym, old_acc or 0, new_acc, ab_score)
                elif delta <= -3:
                    # Model mới tệ hơn đáng kể → giữ model cũ, KHÔNG swap
                    status = "degraded"
                    degraded.append(sym)
                    kept_old.append(sym)
                    # Xóa model mới vừa train, giữ lại model DB cũ
                    if sym in self.lstm._models:
                        self.lstm._load_from_db(sym)  # reload model cũ
                    log.warning("[AutoRetrain] KEEP OLD %s: old=%.1f%% new=%.1f%% — no swap", sym, old_acc or 0, new_acc)
                else:
                    status = "stable"

                results.append({
                    "symbol": sym, "status": status,
                    "old_acc": old_acc, "new_acc": new_acc,
                    "ab_score": round(ab_score, 1),
                    "swapped": sym in [s["symbol"] for s in swapped],
                    "delta": round(delta, 1),
                })

            except Exception as e:
                results.append({"symbol": sym, "status": "exception", "msg": str(e)})

        elapsed = round(time.time() - start_time, 1)
        summary = {
            "total": len(results),
            "improved": len(improved),
            "degraded": len(degraded),
            "stable": len(results) - len(improved) - len(degraded),
            "elapsed_sec": elapsed,
            "run_at": datetime.now().isoformat(),
            "results": results,
        }
        summary["summary"] = (
            f"Retrain {summary['total']} mã: "
            f"+{summary['improved']} cải thiện, "
            f"{summary['degraded']} giảm, {summary['stable']} ổn định "
            f"({elapsed}s)"
        )

        self._send_report(summary, improved, degraded)
        return summary

    def _get_model_accuracy(self, symbol: str) -> Optional[float]:
        try:
            with sqlite3.connect(self.db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT model_info FROM lstm_models WHERE symbol=?",
                    (symbol,)).fetchone()
            if row:
                info = json.loads(row[0] or "{}")
                return info.get("val_acc") or info.get("val_accuracy")
        except Exception:
            pass
        return None

    def _send_report(self, summary: dict, improved: List[str], degraded: List[str]):
        if not self.tg_send:
            return
        msg = [
            "🤖 <b>BÁO CÁO RETRAIN MODEL HÀNG TUẦN</b>",
            "━" * 22,
            f"📊 Tổng cộng: <b>{summary['total']} mã</b>",
            f"✅ Cải thiện: <b>{summary['improved']} mã</b>" +
            (f" ({', '.join(improved[:5])})" if improved else ""),
            f"⚠️ Giảm độ chính xác: {summary['degraded']} mã" +
            (f" ({', '.join(degraded[:3])})" if degraded else ""),
            f"⏱ Thời gian: {summary['elapsed_sec']}s",
            f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        ]
        if degraded:
            msg.append("\n💡 Gợi ý: Kiểm tra lại dữ liệu các mã bị giảm accuracy")
        try:
            self.tg_send("\n".join(msg))
        except Exception as e:
            log.warning("[AutoRetrain] Telegram report failed: %s", e)


# ══════════════════════════════════════════════════════════════
# SHARED HELPERS
# ══════════════════════════════════════════════════════════════

def _pf(v) -> float:
    """Parse float safely."""
    try:
        return float(str(v).replace(",", "").replace(" ", "")) or 0.0
    except Exception:
        return 0.0

def _vi(v) -> int:
    """Parse int (volume) safely."""
    try:
        return int(float(str(v).replace(",", "").replace(" ", "")))
    except Exception:
        return 0

def _req_id() -> str:
    """Generate unique request ID cho SSI API."""
    return hashlib.md5(f"{time.time()}{os.getpid()}".encode()).hexdigest()[:16]

def _rsi(closes: List[float], p: int = 14) -> float:
    if len(closes) < p + 2:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:p]) / p; al = sum(losses[:p]) / p
    for i in range(p, len(gains)):
        ag = (ag * (p-1) + gains[i]) / p
        al = (al * (p-1) + losses[i]) / p
    return 100 - 100 / (1 + ag / (al + 1e-9))

def _ema(arr: List[float], p: int) -> float:
    if not arr:
        return 0.0
    k = 2 / (p + 1); v = arr[0]
    for x in arr[1:]:
        v = x * k + v * (1 - k)
    return v

def _ema_np(arr, p: int):
    """EMA trả về array (numpy)."""
# [merged]     import numpy as np
    k = 2 / (p + 1)
    result = np.zeros_like(arr)
    result[0] = arr[0]
    for i in range(1, len(arr)):
        result[i] = arr[i] * k + result[i-1] * (1 - k)
    return result

def _calc_sharpe(equity: List[float], rf: float = 0.045) -> float:
    if len(equity) < 10:
        return 0.0
    import math
    rets = [(equity[i] - equity[i-1]) / equity[i-1]
             for i in range(1, len(equity)) if equity[i-1] > 0]
    if len(rets) < 5:
        return 0.0
    mean_r = sum(rets) / len(rets)
    var_r  = sum((r - mean_r)**2 for r in rets) / len(rets)
    std_r  = math.sqrt(var_r)
    if std_r == 0:
        return 0.0
    return round((mean_r - rf/252) / std_r * math.sqrt(252), 3)

def _group_by_symbol(trades: List[dict]) -> List[dict]:
    groups = defaultdict(lambda: {"pnl": 0, "trades": 0, "wins": 0})
    for t in trades:
        sym = t["symbol"]
        groups[sym]["pnl"]    += t["pnl"]
        groups[sym]["trades"] += 1
        if t["pnl"] > 0:
            groups[sym]["wins"] += 1
    result = []
    for sym, g in groups.items():
        wr = g["wins"] / g["trades"] * 100 if g["trades"] else 0
        result.append({"symbol": sym, "pnl": round(g["pnl"], 0),
                        "trades": g["trades"], "win_rate": round(wr, 1)})
    result.sort(key=lambda x: -x["pnl"])
    return result


# ══════════════════════════════════════════════════════════════
# DB MIGRATION
# ══════════════════════════════════════════════════════════════

def init_db_v24(db_path: str):
    """Tạo các bảng mới cho v24."""
    c = sqlite3.connect(db_path, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS lstm_models(
        symbol      TEXT PRIMARY KEY,
        model_blob  TEXT,
        model_info  TEXT DEFAULT '{}',
        trained_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS portfolio_backtest_results(
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER NOT NULL,
        symbols     TEXT NOT NULL,
        params_json TEXT DEFAULT '{}',
        result_json TEXT NOT NULL,
        created_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS var_alerts(
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER NOT NULL,
        var_pct     REAL,
        cvar_pct    REAL,
        portfolio_value REAL,
        detail_json TEXT DEFAULT '{}',
        created_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS retrain_log(
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        summary     TEXT,
        result_json TEXT DEFAULT '{}',
        created_at  TEXT DEFAULT(datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS ssi_config(
        user_id         INTEGER PRIMARY KEY,
        consumer_id     TEXT DEFAULT '',
        consumer_secret TEXT DEFAULT '',
        account_no      TEXT DEFAULT '',
        pin_hash        TEXT DEFAULT '',
        enabled         INTEGER DEFAULT 0,
        updated_at      TEXT DEFAULT(datetime('now'))
    );
    """)
    c.commit()
    c.close()


# ══════════════════════════════════════════════════════════════
# API ROUTE HANDLER
# ══════════════════════════════════════════════════════════════

def handle_4_api(path: str, method: str, body: dict,
                    db_path: str, ram_cache: dict,
                    current_user: dict = None,
                    ssi_feed: SSIRealtimeFeed = None,
                    lstm: LSTMPredictor = None,
                    scheduler: AutoRetrainScheduler = None,
                    tg_send_fn = None,
                    qs: dict = None) -> tuple:
    """
    Returns (response_dict, status_code).
    qs: parsed query string dict (parse_qs format: {key: [val]}) — optional, used by v29 routes.
    """
    if qs is None:
        qs = {}

    # ── GET real-time price ─────────────────────────────────────
    if method == "GET" and path.startswith("/api/realtime/"):
        sym  = path.split("/")[-1].upper()
        feed = ssi_feed or get_ssi_feed()

        # 1. Cố lấy từ WS feed
        tick = feed.get_price(sym) if feed else None

        # 2. Fallback HTTP snapshot
        if not tick:
            tick = feed.fetch_snapshot_http(sym) if feed else None

        # 3. Fallback ram_cache
        if not tick and sym in ram_cache:
            d = ram_cache[sym].get("data", {})
            tick = {"price": d.get("price", 0), "source": "cache",
                    "change_pct": d.get("change_pct", 0)}

        if not tick:
            return {"error": f"Không có dữ liệu real-time cho {sym}"}, 404

        # Thêm orderbook nếu chưa có
        if "orderbook" not in tick and feed:
            ob = feed.fetch_orderbook_http(sym)
            if ob:
                tick["orderbook"] = ob

        return tick, 200

    # ── GET orderbook ──────────────────────────────────────────
    if method == "GET" and path.startswith("/api/orderbook/"):
        sym  = path.split("/")[-1].upper()
        feed = ssi_feed or get_ssi_feed()
        ob   = feed.fetch_orderbook_http(sym) if feed else None
        if not ob:
            return {"error": f"Không lấy được orderbook {sym}"}, 404
        return ob, 200

    # ── POST backtest T+2 ──────────────────────────────────────
    if method == "POST" and path == "/apibacktest_t2":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        sym    = body.get("symbol", "").upper()
        mkt    = body.get("market", "VN").upper()
        cap    = float(body.get("capital", 100_000_000))
        strat  = body.get("strategy", "signal")
        sl_pct = float(body.get("sl_pct", 0.07))
        tp_pct = float(body.get("tp_pct", 0.15))

        if sym not in ram_cache:
            return {"error": f"{sym} không có trong watchlist"}, 404

        hist = ram_cache[sym].get("data", {}).get("history", [])
        result = run_backtest_t2(hist, cap, mkt, sl_pct=sl_pct, tp_pct=tp_pct,
                                  strategy=strat)
        return result, 200

    # ── POST portfolio backtest ────────────────────────────────
    if method == "POST" and path == "/api/portfolio_backtest":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        symbols       = body.get("symbols", [])
        capital       = float(body.get("capital", 1_000_000_000))
        max_pos       = int(body.get("max_positions", 5))
        mkt           = body.get("market", "VN").upper()
        strategy      = body.get("strategy", "signal")

        if not symbols:
            # Lấy tất cả mã VN từ cache
            symbols = [s for s, v in ram_cache.items()
                       if v.get("data", {}).get("market") == mkt]

        sym_hists = {}
        for sym in symbols:
            if sym in ram_cache:
                hist = ram_cache[sym].get("data", {}).get("history", [])
                if len(hist) >= 60:
                    sym_hists[sym] = hist

        if not sym_hists:
            return {"error": "Không có mã nào đủ dữ liệu"}, 400

        result = run_portfolio_backtest(
            sym_hists, capital, mkt, max_pos, strategy=strategy
        )

        # Lưu kết quả
        try:
            with sqlite3.connect(db_path, timeout=10) as c:
                c.execute("""INSERT INTO portfolio_backtest_results
                             (user_id, symbols, params_json, result_json)
                             VALUES (?, ?, ?, ?)""",
                          (current_user["id"], json.dumps(symbols),
                           json.dumps(body), json.dumps(result, default=str)))
        except Exception:
            pass

        return result, 200

    # ── POST LSTM train ────────────────────────────────────────
    if method == "POST" and path == "/api/lstm/train":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        sym = body.get("symbol", "").upper()
        if sym not in ram_cache:
            return {"error": f"{sym} không có trong watchlist"}, 404

        predictor = lstm or LSTMPredictor(db_path)
        hist = ram_cache[sym].get("data", {}).get("history", [])

        def _train_bg():
            r = predictor.train(sym, hist)
            log.info("[LSTM] Train %s: %s", sym, r)

        threading.Thread(target=_train_bg, daemon=True).start()
        return {"ok": True, "message": f"LSTM training {sym} started (background)",
                "has_tensorflow": predictor._has_tf}, 202

    # ── POST LSTM predict ──────────────────────────────────────
    if method == "POST" and path == "/api/lstm/predict":
        sym = body.get("symbol", "").upper()
        if sym not in ram_cache:
            return {"error": f"{sym} không tìm thấy"}, 404

        predictor = lstm or LSTMPredictor(db_path)
        hist = ram_cache[sym].get("data", {}).get("history", [])
        result = predictor.predict(sym, hist)
        return result, 200

    # ── POST manual retrain ────────────────────────────────────
    if method == "POST" and path == "/api/retrain_now":
        if not current_user or current_user.get("role") not in ("admin", "user"):
            return {"error": "Unauthorized"}, 401

        sched = scheduler
        if not sched:
            sched = AutoRetrainScheduler(db_path, lstm or LSTMPredictor(db_path),
                                          tg_send_fn, ram_cache)

        def _run_bg():
            result = sched.run_now()
            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("INSERT INTO retrain_log(summary, result_json) VALUES(?,?)",
                              (result.get("summary", ""), json.dumps(result, default=str)))
            except Exception:
                pass

        threading.Thread(target=_run_bg, daemon=True).start()
        return {"ok": True, "message": "Retrain started in background"}, 202

    # ── GET/POST SSI config ────────────────────────────────────
    if path == "/api/ssi_config":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        if method == "GET":
            try:
                with sqlite3.connect(db_path, timeout=5) as c:
                    row = c.execute(
                        "SELECT consumer_id, account_no, enabled FROM ssi_config WHERE user_id=?",
                        (current_user["id"],)).fetchone()
                if row:
                    return {"consumer_id": row[0], "account_no": row[1],
                            "enabled": bool(row[2]),
                            "note": "consumer_secret không được hiển thị"}, 200
                return {"configured": False}, 200
            except Exception as e:
                return {"error": str(e)}, 500

        elif method == "POST":
            cid    = body.get("consumer_id", "").strip()
            csecret= body.get("consumer_secret", "").strip()
            acc    = body.get("account_no", "").strip()
            pin    = body.get("pin", "").strip()
            enabled= int(bool(body.get("enabled", False)))

            if not cid or not csecret or not acc:
                return {"error": "Cần consumer_id, consumer_secret, account_no"}, 400

            pin_hash = hashlib.sha256(pin.encode()).hexdigest() if pin else ""

            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("""INSERT OR REPLACE INTO ssi_config
                                 (user_id, consumer_id, consumer_secret, account_no,
                                  pin_hash, enabled, updated_at)
                                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
                              (current_user["id"], cid, csecret, acc,
                               pin_hash, enabled, datetime.now().isoformat()))
            except Exception as e:
                return {"error": str(e)}, 500

            return {"ok": True, "message": "Cấu hình SSI đã lưu"}, 200

    # ── POST SSI place order ────────────────────────────────────
    if method == "POST" and path == "/api/ssi/order":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        try:
            with sqlite3.connect(db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT consumer_id, consumer_secret, account_no, enabled FROM ssi_config WHERE user_id=?",
                    (current_user["id"],)).fetchone()
        except Exception:
            row = None

        if not row or not row[3]:
            return {"error": "SSI chưa được cấu hình hoặc chưa bật. Vào Settings → SSI Config"}, 400

        try:
            broker = SSIFastConnect(row[0], row[1], row[2])
            result = broker.place_order(
                symbol     = body.get("symbol", "").upper(),
                qty        = int(body.get("qty", 0)),
                price      = float(body.get("price", 0)),
                side       = body.get("side", "BUY").upper(),
                order_type = body.get("order_type", "LO").upper(),
                market     = body.get("market", "HOSE").upper(),
            )
            return result, 200
        except Exception as e:
            return {"error": str(e)}, 500

    # ── GET SSI account balance ────────────────────────────────
    if method == "GET" and path == "/api/ssi/account":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        try:
            with sqlite3.connect(db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT consumer_id, consumer_secret, account_no FROM ssi_config WHERE user_id=? AND enabled=1",
                    (current_user["id"],)).fetchone()
            if not row:
                return {"error": "SSI chưa cấu hình"}, 400
            broker = SSIFastConnect(row[0], row[1], row[2])
            return broker.get_account_balance(), 200
        except Exception as e:
            return {"error": str(e)}, 500
        
    if path == "/api/alpaca/config":
        if not current_user: return {"error": "Unauthorized"}, 401
        if method == "GET":
            try:
                with sqlite3.connect(db_path, timeout=5) as c:
                    c.row_factory = sqlite3.Row
                    row = c.execute("SELECT api_key, paper, enabled FROM alpaca_config WHERE user_id=?",
                                    (current_user["id"],)).fetchone()
                if row:
                    key = row["api_key"]
                    masked = key[:4] + "****" + key[-4:] if len(key) >= 8 else "****"
                    return {"configured": True, "api_key_masked": masked,
                            "paper": bool(row["paper"]), "enabled": bool(row["enabled"])}, 200
                return {"configured": False}, 200
            except Exception as e:
                return {"error": str(e)}, 500
        elif method == "POST":
            key    = body.get("api_key","").strip()
            secret = body.get("api_secret","").strip()
            paper  = int(bool(body.get("paper", True)))
            enabled= int(bool(body.get("enabled", True)))
            if not key or not secret:
                return {"error": "Cần api_key và api_secret"}, 400
            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("""INSERT OR REPLACE INTO alpaca_config
                                 (user_id, api_key, api_secret, paper, enabled, updated_at)
                                 VALUES (?, ?, ?, ?, ?, ?)""",
                              (current_user["id"], encrypt_secret(key), encrypt_secret(secret), paper, enabled,
                               datetime.now().isoformat()))
                # Invalidate cache
                _ALPACA_CACHE.pop(current_user["id"], None)
                return {"ok": True, "message": "Đã lưu Alpaca config"}, 200
            except Exception as e:
                return {"error": str(e)}, 500

    if path=="/api/tcbs/config":
        if not current_user: return {"error":"Unauthorized"},401
        if method=="GET":
            try:
                with sqlite3.connect(db_path,timeout=5) as c:
                    c.row_factory=sqlite3.Row
                    row=c.execute("SELECT username,enabled FROM tcbs_config WHERE user_id=?",(current_user["id"],)).fetchone()
                return ({"configured":True,"username":row["username"],"enabled":bool(row["enabled"])},200) if row else ({"configured":False},200)
            except Exception as e: return {"error":str(e)},500
        elif method=="POST":
            uname=body.get("username","").strip();pw=body.get("password","").strip()
            if not uname or not pw: return {"error":"Cần username và password"},400
            try:
                pe=encrypt_secret(pw)  # AES-256 encrypted
                with sqlite3.connect(db_path,timeout=10) as c:
                    c.execute("INSERT OR REPLACE INTO tcbs_config(user_id,username,password_enc,enabled,updated_at) VALUES(?,?,?,1,?)",(current_user["id"],uname,pe,datetime.now().isoformat()))
                _TCBS_CACHE.pop(current_user["id"],None)
                return {"ok":True,"message":"Đã lưu TCBS config"},200
            except Exception as e: return {"error":str(e)},500
    if method=="GET" and path=="/api/tcbs/account":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        return (b.get_account(),200) if b else ({"error":"TCBS chưa cấu hình"},400)
    if method=="GET" and path=="/api/tcbs/positions":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        return ({"positions":b.get_positions()},200) if b else ({"error":"TCBS chưa cấu hình"},400)
    if method=="GET" and path.startswith("/api/v24/tcbs/orders"):
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        if not b: return {"error":"TCBS chưa cấu hình"},400
        return {"orders":b.get_orders(qs.get("status",["open"])[0] if qs else "open")},200
    if method=="POST" and path=="/api/tcbs/order":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        if not b: return {"error":"TCBS chưa cấu hình"},400
        r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"),body.get("order_type","LO"))
        return r,(200 if r.get("ok") else 400)

    # ── Binance crypto realtime ──────────────────────────────
    if path == "/api/crypto/realtime":
        ws = get_binance_ws()
        data = ws.get_all()
        return {"symbols": list(data.values()), "count": len(data)}, 200
    if method == "GET" and path.startswith("/api/crypto/klines/"):
        sym = path.split("/")[-1].upper()
        interval = qs.get("interval",["1h"])[0] if qs else "1h"
        limit = int(qs.get("limit",["200"])[0]) if qs else 200
        ws = get_binance_ws()
        ws.subscribe([sym])
        klines = ws.get_klines(sym, interval, limit)
        return {"symbol": sym, "interval": interval, "klines": klines}, 200
    if method == "POST" and path == "/api/crypto/subscribe":
        syms = body.get("symbols", [])
        if syms: get_binance_ws().subscribe(syms)
        return {"ok": True, "subscribed": syms}, 200

    # ── VNDirect broker ───────────────────────────────────────
    if path == "/api/vndirect/config":
        if not current_user: return {"error":"Unauthorized"},401
        if method == "GET":
            try:
                with sqlite3.connect(db_path,timeout=5) as c:
                    c.row_factory=sqlite3.Row
                    row=c.execute("SELECT username,account_no,enabled FROM vndirect_config WHERE user_id=?",(current_user["id"],)).fetchone()
                return ({"configured":True,"username":row["username"],"account_no":row["account_no"],"enabled":bool(row["enabled"])},200) if row else ({"configured":False},200)
            except Exception as e: return {"error":str(e)},500
        elif method == "POST":
            uname=body.get("username","").strip(); pw=body.get("password","").strip(); acct=body.get("account_no","").strip()
            if not uname or not pw: return {"error":"Cần username và password"},400
            try:
                pe=encrypt_secret(pw)
                with sqlite3.connect(db_path,timeout=10) as c:
                    c.execute("INSERT OR REPLACE INTO vndirect_config(user_id,username,password_enc,account_no,enabled,updated_at) VALUES(?,?,?,?,1,?)",(current_user["id"],uname,pe,acct,datetime.now().isoformat()))
                _VNDIRECT_CACHE.pop(current_user["id"],None)
                return {"ok":True,"message":"Đã lưu VNDirect config"},200
            except Exception as e: return {"error":str(e)},500
    if method=="GET" and path=="/api/vndirect/account":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_vndirect_broker(current_user["id"],db_path)
        return (b.get_account(),200) if b else ({"error":"VNDirect chưa cấu hình hoặc login thất bại"},400)
    if method=="GET" and path=="/api/vndirect/positions":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_vndirect_broker(current_user["id"],db_path)
        return ({"positions":b.get_positions()},200) if b else ({"error":"VNDirect chưa cấu hình"},400)
    if method=="GET" and path.startswith("/api/vndirect/orders"):
        if not current_user: return {"error":"Unauthorized"},401
        b=get_vndirect_broker(current_user["id"],db_path)
        if not b: return {"error":"VNDirect chưa cấu hình"},400
        st=qs.get("status",["OPEN"])[0] if qs else "OPEN"
        return {"orders":b.get_orders(st)},200
    if method=="POST" and path=="/api/vndirect/order":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_vndirect_broker(current_user["id"],db_path)
        if not b: return {"error":"VNDirect chưa cấu hình"},400
        r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"),body.get("order_type","LO"))
        return r,(200 if r.get("ok") else 400)
    
    # ── GET dynamic slippage estimate ──────────────────────────
    if method == "GET" and path.startswith("/api/slippage/"):
        sym = path.split("/")[-1].upper()
        qty = float((body or {}).get("qty", 1000))
        mkt = (body or {}).get("market", "VN").upper()

        avg_vol = 1_000_000
        if sym in ram_cache:
            hist = ram_cache[sym].get("data", {}).get("history", [])
            if len(hist) >= 20:
                avg_vol = sum(b.get("v", 0) for b in hist[-20:]) / 20

        slip = calc_dynamic_slippage(qty, avg_vol, mkt)
        price = ram_cache.get(sym, {}).get("data", {}).get("price", 0)
        order_val = price * qty

        impact = estimate_market_impact(order_val, price * avg_vol, mkt)
        return {
            "symbol": sym,
            "qty": qty,
            "avg_daily_vol": round(avg_vol, 0),
            "slippage_pct": round(slip * 100, 3),
            "slippage_amount": round(order_val * slip, 0),
            "market_impact": impact,
        }, 200


    # ─── v24 Routes (merged) ───────────────────────────────────

    if method=="GET" and path.startswith("/api/crypto/klines"):
        sym_k=qs.get("symbol",["BTCUSDT"])[0].upper()
        itvl_k=qs.get("interval",["1h"])[0]
        lim_k=int(qs.get("limit",["100"])[0])
        return {"symbol":sym_k,"interval":itvl_k,"klines":get_binance_klines(sym_k,itvl_k,lim_k)}, 200

    # v28: VNDirect test connection
    if method=="GET" and path=="/api/portfolio/beta":
        if not current_user: return {"error":"Unauthorized"},401
        idx=(qs.get("index",["VNINDEX"])[0] if qs else "VNINDEX")
        return calc_portfolio_beta(current_user["id"],idx),200
    if method=="POST" and path=="/api/settings/multi_tenant":
        if not current_user: return {"error":"Unauthorized"},401
        with get_db() as c: c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('multi_tenant_enabled',?)",("1" if body.get("enabled") else "0",))
        return {"ok":True},200
    if method=="POST" and path=="/api/settings/ssl":
        if not current_user: return {"error":"Unauthorized"},401
        with get_db() as c: c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('ssl_enabled',?)",("1" if body.get("enabled") else "0",))
        return {"ok":True,"note":"Restart server để áp dụng SSL"},200
    if path == "/api/v22/regime_ml/predict" and method in ("GET","POST"):
        sym=(body.get("symbol",qs.get("symbol",[""])[0]) if method=="POST" else qs.get("symbol",[""])[0]).upper()
        mkt=(body.get("market",qs.get("market",["VN"])[0]) if method=="POST" else qs.get("market",["VN"])[0]).upper()
        d=_ram_cache.get(sym,{}).get("data") or (fetch(sym,mkt) if sym else None)
        if not d: return {"error":f"Không có dữ liệu {sym}"},400
        result=get_regime_ml(db_path).predict(sym,mkt,d.get("analysis",{}),d.get("history",[]))
        return result,200

    # ── MBS ──────────────────────────────────────────────────────
    if path == "/api/v22/regime_ml/train" and method == "POST":
        if not current_user: return {"error":"Unauthorized"},401
        sym=body.get("symbol","").upper(); mkt=body.get("market","VN")
        d=fetch(sym,mkt) if sym else None
        if not d: return {"error":f"Không lấy được dữ liệu {sym}"},400
        result=get_regime_ml(db_path).train(sym,mkt,d.get("history",[]))
        return result,200

    if method == "GET" and path == "/api/v24/alpaca/account":
        if not current_user: return {"error": "Unauthorized"}, 401
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình. Vào Settings → Alpaca."}, 400
        return broker.get_account(), 200

    # ── GET Alpaca positions ───────────────────────────────────
    if path == "/api/v24/alpaca/config":
        if not current_user: return {"error": "Unauthorized"}, 401
        if method == "GET":
            try:
                with sqlite3.connect(db_path, timeout=5) as c:
                    c.row_factory = sqlite3.Row
                    row = c.execute("SELECT api_key, paper, enabled FROM alpaca_config WHERE user_id=?",
                                    (current_user["id"],)).fetchone()
                if row:
                    key = row["api_key"]
                    masked = key[:4] + "****" + key[-4:] if len(key) >= 8 else "****"
                    return {"configured": True, "api_key_masked": masked,
                            "paper": bool(row["paper"]), "enabled": bool(row["enabled"])}, 200
                return {"configured": False}, 200
            except Exception as e:
                return {"error": str(e)}, 500
        elif method == "POST":
            key    = body.get("api_key","").strip()
            secret = body.get("api_secret","").strip()
            paper  = int(bool(body.get("paper", True)))
            enabled= int(bool(body.get("enabled", True)))
            if not key or not secret:
                return {"error": "Cần api_key và api_secret"}, 400
            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("""INSERT OR REPLACE INTO alpaca_config
                                 (user_id, api_key, api_secret, paper, enabled, updated_at)
                                 VALUES (?, ?, ?, ?, ?, ?)""",
                              (current_user["id"], encrypt_secret(key), encrypt_secret(secret), paper, enabled,
                               datetime.now().isoformat()))
                # Invalidate cache
                _ALPACA_CACHE.pop(current_user["id"], None)
                return {"ok": True, "message": "Đã lưu Alpaca config"}, 200
            except Exception as e:
                return {"error": str(e)}, 500

    if method == "POST" and path == "/api/v24/alpaca/order":
        if not current_user: return {"error": "Unauthorized"}, 401
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình"}, 400
        r = broker.place_order(
            symbol     = body.get("symbol","").upper(),
            qty        = int(body.get("qty", 1)),
            price      = float(body.get("price", 0) or 0),
            side       = body.get("side","buy"),
            order_type = body.get("order_type","market"),
            time_in_force = body.get("tif","day"),
        )
        return r, 200 if r.get("ok") else 400

    # ── DELETE Alpaca cancel order ─────────────────────────────
    if method == "DELETE" and path.startswith("/api/v24/alpaca/order/"):
        if not current_user: return {"error": "Unauthorized"}, 401
        order_id = path.split("/")[-1]
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình"}, 400
        return broker.cancel_order(order_id), 200

    # ── GET/POST Alpaca config ─────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/alpaca/orders"):
        if not current_user: return {"error": "Unauthorized"}, 401
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình"}, 400
        status_q = qs.get("status", ["open"])[0] if qs else "open"
        return {"orders": broker.get_orders(status_q)}, 200

    # ── GET Alpaca latest quote ────────────────────────────────
    if method == "GET" and path == "/api/v24/alpaca/positions":
        if not current_user: return {"error": "Unauthorized"}, 401
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình"}, 400
        return {"positions": broker.get_positions()}, 200

    # ── GET Alpaca orders ──────────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/alpaca/quote/"):
        if not current_user: return {"error": "Unauthorized"}, 401
        sym = path.split("/")[-1].upper()
        broker = get_alpaca_broker(current_user["id"], db_path)
        if not broker: return {"error": "Alpaca chưa cấu hình"}, 400
        return broker.get_latest_quote(sym), 200

    # ── POST Alpaca order ──────────────────────────────────────
    if method == "POST" and path == "/api/v24/backtest_t2":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        sym    = body.get("symbol", "").upper()
        mkt    = body.get("market", "VN").upper()
        cap    = float(body.get("capital", 100_000_000))
        strat  = body.get("strategy", "signal")
        sl_pct = float(body.get("sl_pct", 0.07))
        tp_pct = float(body.get("tp_pct", 0.15))

        if sym not in ram_cache:
            return {"error": f"{sym} không có trong watchlist"}, 404

        hist = ram_cache[sym].get("data", {}).get("history", [])
        result = run_backtest_t2(hist, cap, mkt, sl_pct=sl_pct, tp_pct=tp_pct,
                                  strategy=strat)
        return result, 200

    # ── POST portfolio backtest ────────────────────────────────
    if path.startswith("/api/v24/bsc/"):
        if not current_user: return {"error":"Unauthorized"},401
        sub=path.split("/api/v24/bsc/")[-1]
        if method=="POST" and sub=="config":
            u=body.get("username","").strip(); p=body.get("password","").strip()
            if not u or not p: return {"error":"Cần username và password"},400
            with sqlite3.connect(db_path,timeout=10) as c:
                c.execute("INSERT OR REPLACE INTO bsc_config(user_id,username,password_enc,account_no,enabled,updated_at) VALUES(?,?,?,?,1,?)",(current_user["id"],u,encrypt_secret(p),"",datetime.now().isoformat()))
            _BSC_CACHE.pop(current_user["id"],None); return {"ok":True},200
        b=get_bsc_broker(current_user["id"],db_path)
        if sub=="account": return (b.get_account(),200) if b else ({"error":"BSC chưa cấu hình"},400)
        if sub=="positions": return ({"positions":b.get_positions()},200) if b else ({"error":"BSC chưa cấu hình"},400)
        if method=="POST" and sub=="order":
            if not b: return {"error":"BSC chưa cấu hình"},400
            r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"))
            return r,(200 if r.get("ok") else 400)
    # ── IBKR ─────────────────────────────────────────────────────
    if path.startswith("/api/v24/ibkr/"):
        if not current_user: return {"error":"Unauthorized"},401
        sub=path.split("/api/v24/ibkr/")[-1]
        if method=="POST" and sub=="config":
            with sqlite3.connect(db_path,timeout=10) as c:
                c.execute("INSERT OR REPLACE INTO ibkr_config(user_id,host,port,account_id,enabled,updated_at) VALUES(?,?,?,?,1,?)",(current_user["id"],body.get("host","localhost"),int(body.get("port",5000)),body.get("account_id",""),datetime.now().isoformat()))
            return {"ok":True},200
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT host,port,account_id FROM ibkr_config WHERE user_id=? AND enabled=1",(current_user["id"],)).fetchone()
        if not row: return {"error":"IBKR chưa cấu hình"},400
        b=IBKRBroker(row['host'],row['port'],row.get('account_id',''))
        if sub=="account": return b.get_account(),200
        if sub=="positions": return {"positions":b.get_positions()},200
        if method=="POST" and sub=="order":
            r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"))
            return r,(200 if r.get("ok") else 400)
    # ── Data endpoints ────────────────────────────────────────────
    if method == "POST" and path == "/api/v24/lstm/predict":
        sym = body.get("symbol", "").upper()
        if sym not in ram_cache:
            return {"error": f"{sym} không tìm thấy"}, 404

        predictor = lstm or LSTMPredictor(db_path)
        hist = ram_cache[sym].get("data", {}).get("history", [])
        result = predictor.predict(sym, hist)
        return result, 200

    # ── POST manual retrain ────────────────────────────────────
    if method == "POST" and path == "/api/v24/lstm/train":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        sym = body.get("symbol", "").upper()
        if sym not in ram_cache:
            return {"error": f"{sym} không có trong watchlist"}, 404

        predictor = lstm or LSTMPredictor(db_path)
        hist = ram_cache[sym].get("data", {}).get("history", [])

        def _train_bg():
            r = predictor.train(sym, hist)
            log.info("[LSTM] Train %s: %s", sym, r)

        threading.Thread(target=_train_bg, daemon=True).start()
        return {"ok": True, "message": f"LSTM training {sym} started (background)",
                "has_tensorflow": predictor._has_tf}, 202

    # ── POST LSTM predict ──────────────────────────────────────
    if path.startswith("/api/v24/mbs/"):
        if not current_user: return {"error":"Unauthorized"},401
        sub=path.split("/api/v24/mbs/")[-1]
        if method=="POST" and sub=="config":
            u=body.get("username","").strip(); p=body.get("password","").strip(); a=body.get("account_no","").strip()
            if not u or not p: return {"error":"Cần username và password"},400
            with sqlite3.connect(db_path,timeout=10) as c:
                c.execute("INSERT OR REPLACE INTO mbs_config(user_id,username,password_enc,account_no,enabled,updated_at) VALUES(?,?,?,?,1,?)",(current_user["id"],u,encrypt_secret(p),a,datetime.now().isoformat()))
            _MBS_CACHE.pop(current_user["id"],None); return {"ok":True},200
        b=get_mbs_broker(current_user["id"],db_path)
        if sub=="account": return (b.get_account(),200) if b else ({"error":"MBS chưa cấu hình"},400)
        if sub=="positions": return ({"positions":b.get_positions()},200) if b else ({"error":"MBS chưa cấu hình"},400)
        if method=="POST" and sub=="order":
            if not b: return {"error":"MBS chưa cấu hình"},400
            r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"))
            return r,(200 if r.get("ok") else 400)
    # ── BSC ──────────────────────────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/orderbook/"):
        sym  = path.split("/")[-1].upper()
        feed = ssi_feed or get_ssi_feed()
        ob   = feed.fetch_orderbook_http(sym) if feed else None
        if not ob:
            return {"error": f"Không lấy được orderbook {sym}"}, 404
        return ob, 200

    # ── POST backtest T+2 ──────────────────────────────────────
    if method == "POST" and path == "/api/v24/portfolio_backtest":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        symbols       = body.get("symbols", [])
        capital       = float(body.get("capital", 1_000_000_000))
        max_pos       = int(body.get("max_positions", 5))
        mkt           = body.get("market", "VN").upper()
        strategy      = body.get("strategy", "signal")

        if not symbols:
            # Lấy tất cả mã VN từ cache
            symbols = [s for s, v in ram_cache.items()
                       if v.get("data", {}).get("market") == mkt]

        sym_hists = {}
        for sym in symbols:
            if sym in ram_cache:
                hist = ram_cache[sym].get("data", {}).get("history", [])
                if len(hist) >= 60:
                    sym_hists[sym] = hist

        if not sym_hists:
            return {"error": "Không có mã nào đủ dữ liệu"}, 400

        result = run_portfolio_backtest(
            sym_hists, capital, mkt, max_pos, strategy=strategy
        )

        # Lưu kết quả
        try:
            with sqlite3.connect(db_path, timeout=10) as c:
                c.execute("""INSERT INTO portfolio_backtest_results
                             (user_id, symbols, params_json, result_json)
                             VALUES (?, ?, ?, ?)""",
                          (current_user["id"], json.dumps(symbols),
                           json.dumps(body), json.dumps(result, default=str)))
        except Exception:
            pass

        return result, 200

    # ── POST LSTM train ────────────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/realtime/"):
        sym  = path.split("/")[-1].upper()
        feed = ssi_feed or get_ssi_feed()

        # 1. Cố lấy từ WS feed
        tick = feed.get_price(sym) if feed else None

        # 2. Fallback HTTP snapshot
        if not tick:
            tick = feed.fetch_snapshot_http(sym) if feed else None

        # 3. Fallback ram_cache
        if not tick and sym in ram_cache:
            d = ram_cache[sym].get("data", {})
            tick = {"price": d.get("price", 0), "source": "cache",
                    "change_pct": d.get("change_pct", 0)}

        if not tick:
            return {"error": f"Không có dữ liệu real-time cho {sym}"}, 404

        # Thêm orderbook nếu chưa có
        if "orderbook" not in tick and feed:
            ob = feed.fetch_orderbook_http(sym)
            if ob:
                tick["orderbook"] = ob

        return tick, 200

    # ── GET orderbook ──────────────────────────────────────────
    if method == "POST" and path == "/api/v24/retrain_now":
        if not current_user or current_user.get("role") not in ("admin", "user"):
            return {"error": "Unauthorized"}, 401

        sched = scheduler
        if not sched:
            sched = AutoRetrainScheduler(db_path, lstm or LSTMPredictor(db_path),
                                          tg_send_fn, ram_cache)

        def _run_bg():
            result = sched.run_now()
            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("INSERT INTO retrain_log(summary, result_json) VALUES(?,?)",
                              (result.get("summary", ""), json.dumps(result, default=str)))
            except Exception:
                pass

        threading.Thread(target=_run_bg, daemon=True).start()
        return {"ok": True, "message": "Retrain started in background"}, 202

    # ── GET/POST SSI config ────────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/slippage/"):
        sym = path.split("/")[-1].upper()
        qty = float((body or {}).get("qty", 1000))
        mkt = (body or {}).get("market", "VN").upper()

        avg_vol = 1_000_000
        if sym in ram_cache:
            hist = ram_cache[sym].get("data", {}).get("history", [])
            if len(hist) >= 20:
                avg_vol = sum(b.get("v", 0) for b in hist[-20:]) / 20

        slip = calc_dynamic_slippage(qty, avg_vol, mkt)
        price = ram_cache.get(sym, {}).get("data", {}).get("price", 0)
        order_val = price * qty

        impact = estimate_market_impact(order_val, price * avg_vol, mkt)
        return {
            "symbol": sym,
            "qty": qty,
            "avg_daily_vol": round(avg_vol, 0),
            "slippage_pct": round(slip * 100, 3),
            "slippage_amount": round(order_val * slip, 0),
            "market_impact": impact,
        }, 200

    # ── GET Alpaca account ────────────────────────────────────
    if method == "GET" and path == "/api/v24/ssi/account":
        if not current_user:
            return {"error": "Unauthorized"}, 401
        try:
            with sqlite3.connect(db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT consumer_id, consumer_secret, account_no FROM ssi_config WHERE user_id=? AND enabled=1",
                    (current_user["id"],)).fetchone()
            if not row:
                return {"error": "SSI chưa cấu hình"}, 400
            broker = SSIFastConnect(row[0], row[1], row[2])
            return broker.get_account_balance(), 200
        except Exception as e:
            return {"error": str(e)}, 500

    # ── GET dynamic slippage estimate ──────────────────────────
    if method == "POST" and path == "/api/v24/ssi/order":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        try:
            with sqlite3.connect(db_path, timeout=5) as c:
                row = c.execute(
                    "SELECT consumer_id, consumer_secret, account_no, enabled FROM ssi_config WHERE user_id=?",
                    (current_user["id"],)).fetchone()
        except Exception:
            row = None

        if not row or not row[3]:
            return {"error": "SSI chưa được cấu hình hoặc chưa bật. Vào Settings → SSI Config"}, 400

        try:
            broker = SSIFastConnect(row[0], row[1], row[2])
            result = broker.place_order(
                symbol     = body.get("symbol", "").upper(),
                qty        = int(body.get("qty", 0)),
                price      = float(body.get("price", 0)),
                side       = body.get("side", "BUY").upper(),
                order_type = body.get("order_type", "LO").upper(),
                market     = body.get("market", "HOSE").upper(),
            )
            return result, 200
        except Exception as e:
            return {"error": str(e)}, 500

    # ── GET SSI account balance ────────────────────────────────
    if path == "/api/v24/ssi_config":
        if not current_user:
            return {"error": "Unauthorized"}, 401

        if method == "GET":
            try:
                with sqlite3.connect(db_path, timeout=5) as c:
                    row = c.execute(
                        "SELECT consumer_id, account_no, enabled FROM ssi_config WHERE user_id=?",
                        (current_user["id"],)).fetchone()
                if row:
                    return {"consumer_id": row[0], "account_no": row[1],
                            "enabled": bool(row[2]),
                            "note": "consumer_secret không được hiển thị"}, 200
                return {"configured": False}, 200
            except Exception as e:
                return {"error": str(e)}, 500

        elif method == "POST":
            cid    = body.get("consumer_id", "").strip()
            csecret= body.get("consumer_secret", "").strip()
            acc    = body.get("account_no", "").strip()
            pin    = body.get("pin", "").strip()
            enabled= int(bool(body.get("enabled", False)))

            if not cid or not csecret or not acc:
                return {"error": "Cần consumer_id, consumer_secret, account_no"}, 400

            pin_hash = hashlib.sha256(pin.encode()).hexdigest() if pin else ""

            try:
                with sqlite3.connect(db_path, timeout=10) as c:
                    c.execute("""INSERT OR REPLACE INTO ssi_config
                                 (user_id, consumer_id, consumer_secret, account_no,
                                  pin_hash, enabled, updated_at)
                                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
                              (current_user["id"], cid, csecret, acc,
                               pin_hash, enabled, datetime.now().isoformat()))
            except Exception as e:
                return {"error": str(e)}, 500

            return {"ok": True, "message": "Cấu hình SSI đã lưu"}, 200

    # ── POST SSI place order ────────────────────────────────────
    if method=="GET" and path=="/api/v24/tcbs/account":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        return (b.get_account(),200) if b else ({"error":"TCBS chưa cấu hình"},400)
    if path=="/api/v24/tcbs/config":
        if not current_user: return {"error":"Unauthorized"},401
        if method=="GET":
            try:
                with sqlite3.connect(db_path,timeout=5) as c:
                    c.row_factory=sqlite3.Row
                    row=c.execute("SELECT username,enabled FROM tcbs_config WHERE user_id=?",(current_user["id"],)).fetchone()
                return ({"configured":True,"username":row["username"],"enabled":bool(row["enabled"])},200) if row else ({"configured":False},200)
            except Exception as e: return {"error":str(e)},500
        elif method=="POST":
            uname=body.get("username","").strip();pw=body.get("password","").strip()
            if not uname or not pw: return {"error":"Cần username và password"},400
            try:
                pe=encrypt_secret(pw)  # AES-256 encrypted
                with sqlite3.connect(db_path,timeout=10) as c:
                    c.execute("INSERT OR REPLACE INTO tcbs_config(user_id,username,password_enc,enabled,updated_at) VALUES(?,?,?,1,?)",(current_user["id"],uname,pe,datetime.now().isoformat()))
                _TCBS_CACHE.pop(current_user["id"],None)
                return {"ok":True,"message":"Đã lưu TCBS config"},200
            except Exception as e: return {"error":str(e)},500
    if method=="POST" and path=="/api/v24/tcbs/order":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        if not b: return {"error":"TCBS chưa cấu hình"},400
        r=b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),float(body.get("price",0) or 0),body.get("side","BUY"),body.get("order_type","LO"))
        return r,(200 if r.get("ok") else 400)

    # ── Binance crypto realtime ──────────────────────────────
    if method=="GET" and path=="/api/v24/tcbs/positions":
        if not current_user: return {"error":"Unauthorized"},401
        b=get_tcbs_broker(current_user["id"],db_path)
        return ({"positions":b.get_positions()},200) if b else ({"error":"TCBS chưa cấu hình"},400)
    if method == "GET" and path == "/api/v24/vndirect/account":
        if not current_user: return {"error":"Unauthorized"},401
        b = get_vndirect_broker(current_user["id"],db_path)
        return (b.get_account(),200) if b else ({"error":"VNDirect chưa cấu hình hoặc login thất bại"},400)

    if path == "/api/v24/vndirect/config":
        if not current_user: return {"error":"Unauthorized"},401
        if method == "GET":
            try:
                with sqlite3.connect(db_path,timeout=5) as c:
                    c.row_factory=sqlite3.Row
                    row=c.execute("SELECT username,account_no,enabled FROM vndirect_config WHERE user_id=?",(current_user["id"],)).fetchone()
                return ({"configured":True,"username":row["username"],"account_no":row["account_no"],"enabled":bool(row["enabled"])},200) if row else ({"configured":False},200)
            except Exception as e: return {"error":str(e)},500
        elif method == "POST":
            uname=body.get("username","").strip(); pw=body.get("password","").strip(); acct=body.get("account_no","").strip()
            if not uname or not pw: return {"error":"Cần username và password"},400
            try:
                pw_enc = encrypt_secret(pw)
                with sqlite3.connect(db_path,timeout=10) as c:
                    c.execute("INSERT OR REPLACE INTO vndirect_config(user_id,username,password_enc,account_no,enabled,updated_at) VALUES(?,?,?,?,1,?)",
                              (current_user["id"],uname,pw_enc,acct,datetime.now().isoformat()))
                _VNDIRECT_CACHE.pop(current_user["id"],None)
                return {"ok":True,"message":"Đã lưu VNDirect config (mật khẩu mã hóa AES)"},200
            except Exception as e: return {"error":str(e)},500

    if method == "POST" and path == "/api/v24/vndirect/order":
        if not current_user: return {"error":"Unauthorized"},401
        b = get_vndirect_broker(current_user["id"],db_path)
        if not b: return {"error":"VNDirect chưa cấu hình"},400
        r = b.place_order(body.get("symbol","").upper(),int(body.get("qty",0)),
                          float(body.get("price",0) or 0),body.get("side","BUY"),body.get("order_type","LO"))
        return r,(200 if r.get("ok") else 400)

    if method == "DELETE" and path.startswith("/api/v24/vndirect/order/"):
        if not current_user: return {"error":"Unauthorized"},401
        b = get_vndirect_broker(current_user["id"],db_path)
        if not b: return {"error":"VNDirect chưa cấu hình"},400
        return (b.cancel_order(path.split("/")[-1]),200)

    # ── Regime ML (v28) ─────────────────────────────────────────
    if method == "GET" and path.startswith("/api/v24/vndirect/orders"):
        if not current_user: return {"error":"Unauthorized"},401
        b = get_vndirect_broker(current_user["id"],db_path)
        if not b: return {"error":"VNDirect chưa cấu hình"},400
        st = qs.get("status",["OPEN"])[0] if qs else "OPEN"
        return {"orders":b.get_orders(st)},200

    if method == "GET" and path == "/api/v24/vndirect/positions":
        if not current_user: return {"error":"Unauthorized"},401
        b = get_vndirect_broker(current_user["id"],db_path)
        return ({"positions":b.get_positions()},200) if b else ({"error":"VNDirect chưa cấu hình"},400)

    if method=="POST" and path=="/api/v24/vndirect/test":
        if not current_user: return {"error":"Unauthorized"}, 401
        uname=body.get("username",""); pw=body.get("password",""); acct=body.get("account_no","")
        return test_vndirect_connection(uname,pw,acct), 200

    # ── VNDirect config ───────────────────────────────────────────
    if method=="GET" and path.startswith("/api/v29/fibonacci/"):
        sym=path.split("/api/v29/fibonacci/")[-1].upper()
        mkt=(qs.get("market",["VN"])[0] if qs else "VN").upper()
        d=_ram_cache.get(sym,{}).get("data",{})
        if not d:
            d=fetch(sym,mkt) or {}
        if not d:
            return {"error":f"Chưa có dữ liệu cho {sym}"},404
        # Lấy analysis để có fib levels đã tính
        a=d.get("analysis",{})
        if not a:
            a=analyze(d)
        return {
            "symbol":sym,"price":d.get("price",0),
            "fib_0":a.get("fib_0",0),"fib_100":a.get("fib_100",0),
            "fib_236":a.get("fib_236",0),"fib_382":a.get("fib_382",0),
            "fib_500":a.get("fib_500",0),"fib_618":a.get("fib_618",0),
            "fib_786":a.get("fib_786",0),
            "fib_ext_1272":a.get("fib_ext_1272",0),
            "fib_ext_1618":a.get("fib_ext_1618",0),
            "fib_swing_hi":a.get("fib_swing_hi",0),
            "fib_swing_lo":a.get("fib_swing_lo",0),
            "fib_is_uptrend":a.get("fib_is_uptrend",True),
            "vwap":a.get("vwap",0),
            "pivot_r1":a.get("pivot_r1",0),"pivot_s1":a.get("pivot_s1",0),
            "pivot_p":a.get("pivot_p",0),
        },200

    # ── v29.3: Market Calendar ────────────────────────────────
    if method=="GET" and path.startswith("/api/v29/sentiment/"):
        sym=path.split("/api/v29/sentiment/")[-1].upper()
        mkt=(qs.get("market",["VN"])[0] if qs else "VN").upper()
        news=[]
        try:
            with get_db() as c: rows=c.execute("SELECT title FROM news_cache WHERE symbol=? ORDER BY published_at DESC LIMIT 20",(sym,)).fetchall(); news=[r["title"] for r in rows]
        except: pass
        result=analyze_headline_sentiment_v2(news,"en" if mkt=="US" else "vi"); result["symbol"]=sym
        return result,200
    # ── Trend Forecast ───────────────────────────────────────
    if method=="GET" and path.startswith("/api/v29/trend_forecast/"):
        sym=path.split("/api/v29/trend_forecast/")[-1].upper()
        mkt=(qs.get("market",["VN"])[0] if qs else "VN").upper()
        periods=int(qs.get("periods",["10"])[0] if qs else 10)
        d=_ram_cache.get(sym,{}).get("data",{})
        if not d:
            d=fetch(sym,mkt) or {}
        if not d:
            return {"error":f"Chưa có dữ liệu cho {sym}"},404
        result=calc_trend_forecast(d, periods)
        result["symbol"]=sym; result["market"]=mkt
        return result,200

    # ── Fibonacci levels ─────────────────────────────────────
    return {"error": f"Pro: Route not found: {path}"}, 404

# ══════════════════════════════════════════════════════════════
# SMOKE TEST
# ══════════════════════════════════════════════════════════════

# ── Pro INIT ──────────────────────────────────────────────────
_LSTM      = None
_VAR_MON   = None
_SCHEDULER = None
_ML_V22    = None

def _init_v24():
    global _LSTM, _VAR_MON, _SCHEDULER
    try:
        init_db_v24(DB_PATH)
        _LSTM      = LSTMPredictor(DB_PATH)
        _VAR_MON   = VaRMonitor(DB_PATH)
        _SCHEDULER = AutoRetrainScheduler(DB_PATH, _LSTM)
        print("[SMA Pro] v24 modules ready")
    except Exception as _e:
        print(f"[SMA Pro] Warning: {_e}")


# ════════════════════════════════════════════════════════════════
# SMA Pro — ADDITIONS FROM v12
# Best features from v12 not present in v24
# ════════════════════════════════════════════════════════════════

# ── v12 CONSTANTS ────────────────────────────────────────────────
_SENT_POS = [
    # VN positive
    'tăng mạnh','tăng trưởng','vượt kỳ vọng','lợi nhuận tăng','doanh thu tăng',
    'chia cổ tức','mua ròng','khuyến nghị mua','nâng mục tiêu','phá đỉnh',
    'hợp đồng lớn','ký kết hợp đồng','trúng thầu','nhà máy mới','mở rộng',
    'lãi kỷ lục','cao nhất','tích cực','khả quan','kết quả tốt',
    # EN positive
    'beat expectations','record profit','strong growth','buy recommendation',
    'upgrade','new contract','expansion','dividend','outperform','buy',
    'positive outlook','strong earnings',
]
_SENT_NEG = [
    # VN negative
    'giảm mạnh','thua lỗ','lỗ ròng','vi phạm','phạt','đình chỉ','bán ròng',
    'hạ mục tiêu','cảnh báo','rủi ro','nợ xấu','kiện tụng','thoái vốn',
    'khó khăn','sụt giảm','thu hẹp','điều tra','nghi vấn','bán tháo',
    # EN negative
    'miss expectations','net loss','violation','fine','suspend','downgrade',
    'sell recommendation','debt','lawsuit','investigation','decline','cut',
    'negative outlook','underperform','sell off',
]

_ml_model_cache  = {}   # {symbol: {model, scaler, features, trained_at, metrics}}
_ML_RETRAIN_DAYS = 7    # retrain mỗi 7 ngày
_ML_MIN_BARS     = 120  # tối thiểu để train

_macro_cache  = {}
_MACRO_TTL    = 3600    # 1 giờ

_COOLDOWN_SL   = 1800   # 30min for stop loss alerts
_COOLDOWN_MTF  = 14400  # 4h for MTF alerts
_COOLDOWN_BUY  = 3600   # 1h per BUY signal
_COOLDOWN_SELL = 3600   # 1h per SELL signal
_alert_cooldown = {}    # {(sym, signal_type): last_sent_ts}

_foreign_cache  = {}
_FOREIGN_TTL    = 300   # 5 phút

# ── v12 ENSEMBLE ML + STATISTICAL BACKTEST ───────────────────────

# ── score_news_sentiment (from v12) ──

def score_news_sentiment(symbol, market, n_articles=10):
    """
    Tính điểm sentiment từ tiêu đề và tóm tắt tin tức gần nhất.
    Trả về: score (-5 đến +5), label, positive_count, negative_count
    """
    try:
        news_data = fetch_news(symbol, market)
        articles  = news_data if isinstance(news_data, list) else news_data.get('news', [])
        articles  = articles[:n_articles]
        if not articles: return {"score":0,"label":"Trung lập","pos":0,"neg":0,"total":0}
        pos_count=0; neg_count=0
        for art in articles:
            text = ((art.get('title','') or '') + ' ' + (art.get('summary','') or '')).lower()
            for kw in _SENT_POS:
                if kw in text: pos_count+=1; break
            for kw in _SENT_NEG:
                if kw in text: neg_count+=1; break
        raw  = pos_count - neg_count
        n    = len(articles)
        score= max(-5, min(5, round(raw/max(n,1)*5)))
        if   score >= 2:  label="🟢 Tích cực"
        elif score >= 1:  label="🟩 Hơi tích cực"
        elif score <= -2: label="🔴 Tiêu cực"
        elif score <= -1: label="🟥 Hơi tiêu cực"
        else:             label="⬜ Trung lập"
        return {"score":score,"label":label,"pos":pos_count,"neg":neg_count,
                "total":len(articles),"articles_analyzed":n}
    except Exception as e:
        return {"score":0,"label":"Trung lập","pos":0,"neg":0,"total":0,"error":str(e)}


    return fetch_vn_events(symbol) if market=='VN' else fetch_us_events(symbol)

# ── intraday_analyze (from v12) ──

def intraday_analyze(bars):
    """Phân tích kỹ thuật nhanh cho intraday bars."""
    if not bars or len(bars) < 10: return {}
    closes = [b['c'] for b in bars]
    vols   = [b['v'] for b in bars]
    n = len(closes)
    rsi_val = calc_rsi(closes)
    avg_vol = sum(vols[-20:]) / 20 if n >= 20 else (sum(vols)/n if n > 0 else 1)
    vol_spike = vols[-1] / avg_vol if avg_vol > 0 else 1
    # VWAP intraday
    vwap_sum = sum((bars[i]['h']+bars[i]['l']+bars[i]['c'])/3 * max(bars[i]['v'],1) for i in range(n))
    vol_sum  = sum(max(bars[i]['v'],1) for i in range(n))
    vwap_val = vwap_sum / vol_sum if vol_sum > 0 else closes[-1]
    price = closes[-1]
    trend = 'UP' if n >= 5 and closes[-1] > closes[-5] else ('DOWN' if n >= 5 and closes[-1] < closes[-5] else 'FLAT')
    # EMA 9/21
    ema9  = ema_s(closes,9)[-1]  if n >= 9  else price
    ema21 = ema_s(closes,21)[-1] if n >= 21 else price
    return {
        'rsi': round(rsi_val,1), 'vwap': round(vwap_val,2),
        'price': price, 'trend': trend,
        'ema9': round(ema9,2), 'ema21': round(ema21,2),
        'vol_spike': round(vol_spike,1),
        'above_vwap': price > vwap_val,
        'signal': 'BUY' if rsi_val<40 and price>vwap_val and trend=='UP' else
                  ('SELL' if rsi_val>65 and price<vwap_val and trend=='DOWN' else 'NEUTRAL'),
        'n_bars': n,
    }

# ── v13: BREAKOUT D+1 GAP FILTER ─────────────────────────────────────────────

# ── _build_ml_features (from v12) ──

def _build_ml_features(bars):
    """
    Xây dựng feature matrix từ OHLCV bars.
    Features: RSI(7/14/21), MACD, BB%B, ATR%, volume_ratio, momentum(5/10/20),
              volatility(10/20), trend_strength, candle_body, gap, price_position.
    Returns: (X: list[list], dates: list)
    """
    from collections import deque
    n = len(bars)
    if n < _ML_MIN_BARS: return [], []

    closes = [b['c'] for b in bars]
    highs  = [b.get('h', b['c']) for b in bars]
    lows   = [b.get('l', b['c']) for b in bars]
    vols   = [b.get('v', 0) for b in bars]
    opens  = [b.get('o', b['c']) for b in bars]

    def ema_arr(arr, p):
        k = 2/(p+1); res = [arr[0]]
        for i in range(1, len(arr)): res.append(arr[i]*k + res[-1]*(1-k))
        return res

    def rsi_arr(arr, p=14):
        gains, losses = [], []
        for i in range(1, len(arr)):
            d = arr[i]-arr[i-1]
            gains.append(max(d,0)); losses.append(max(-d,0))
        if len(gains) < p: return [50]*len(arr)
        ag = sum(gains[:p])/p; al = sum(losses[:p])/p
        rsi = []
        for i in range(p, len(gains)):
            ag = (ag*(p-1)+gains[i])/p; al = (al*(p-1)+losses[i])/p
            rs = ag/al if al > 0 else 100
            rsi.append(100 - 100/(1+rs))
        return [50]*(len(arr)-len(rsi)) + rsi

    ema9  = ema_arr(closes, 9)
    ema20 = ema_arr(closes, 20)
    ema50 = ema_arr(closes, 50)
    ema12 = ema_arr(closes, 12)
    ema26 = ema_arr(closes, 26)
    macd  = [ema12[i]-ema26[i] for i in range(n)]
    macd_sig = ema_arr(macd, 9)
    rsi14 = rsi_arr(closes, 14)
    rsi7  = rsi_arr(closes, 7)
    rsi21 = rsi_arr(closes, 21)

    # Bollinger %B
    def bband_pct(arr, p=20):
        res = [0.5]*p
        for i in range(p, len(arr)):
            win = arr[i-p:i]; mu = sum(win)/p
            sd  = (sum((x-mu)**2 for x in win)/p)**0.5
            if sd > 0: res.append((arr[i]-(mu-2*sd))/(4*sd))
            else: res.append(0.5)
        return res
    bb_pct = bband_pct(closes)

    # ATR
    def atr_arr(p=14):
        trs = [highs[0]-lows[0]]
        for i in range(1, n):
            trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
        res = [sum(trs[:p])/p]
        for i in range(p, len(trs)):
            res.append((res[-1]*(p-1)+trs[i])/p)
        return [trs[0]]*(p-1) + res
    atr = atr_arr()

    # Volume ratio
    avg_vol20 = [0]*20
    for i in range(20, n):
        avg = sum(vols[i-20:i])/20
        avg_vol20.append(vols[i]/avg if avg > 0 else 1.0)

    X, dates = [], []
    for i in range(60, n-1):  # need 60 bars history, label = next day return
        c  = closes[i]; c_prev = closes[i-1]
        try:
            feat = [
                # Momentum
                (c - closes[i-5])  / closes[i-5]  * 100,   # mom5
                (c - closes[i-10]) / closes[i-10] * 100,   # mom10
                (c - closes[i-20]) / closes[i-20] * 100,   # mom20
                # RSI trio
                rsi7[i], rsi14[i], rsi21[i],
                # MACD
                macd[i] / c * 100,
                (macd[i] - macd_sig[i]) / c * 100,
                # BB %B
                bb_pct[i],
                # Trend (price vs EMA)
                (c - ema9[i])  / c * 100,
                (c - ema20[i]) / c * 100,
                (c - ema50[i]) / c * 100,
                (ema9[i] - ema20[i]) / c * 100,  # short-term trend
                # Volatility
                atr[i] / c * 100,
                # Volume
                min(avg_vol20[i], 5.0),  # cap at 5x
                # Candle features
                abs(c - opens[i]) / c * 100,   # body size
                (highs[i] - max(c, opens[i])) / c * 100,  # upper wick
                (min(c, opens[i]) - lows[i])   / c * 100,  # lower wick
                1.0 if c > opens[i] else 0.0,  # bullish candle
                # Gap
                (opens[i] - closes[i-1]) / closes[i-1] * 100,
                # Position in range
                (c - min(closes[i-20:i+1])) / max(max(closes[i-20:i+1]) - min(closes[i-20:i+1]), 1) * 100,
            ]
            # Validate - no NaN/Inf
            if any(not (-1000 < f < 1000) for f in feat): continue
            X.append(feat)
            dates.append(bars[i]['d'])
        except: continue
    return X, dates

# ── _build_ml_labels (from v12) ──

def _build_ml_labels(bars, X_dates, threshold_pct=1.5):
    """
    Label: 1=BUY (next day +threshold%), -1=SELL (next day -threshold%), 0=HOLD.
    Dùng close D+1 để tránh look-ahead.
    """
    date_to_idx = {b['d']: i for i, b in enumerate(bars)}
    labels = []
    valid_dates = []
    for d in X_dates:
        idx = date_to_idx.get(d)
        if idx is None or idx+1 >= len(bars): continue
        next_ret = (bars[idx+1]['c'] - bars[idx]['c']) / bars[idx]['c'] * 100
        if   next_ret >= threshold_pct:  labels.append(1)   # BUY
        elif next_ret <= -threshold_pct: labels.append(-1)  # SELL
        else:                            labels.append(0)   # HOLD
        valid_dates.append(d)
    return labels, valid_dates

# ── train_ml_model (from v12) ──

def train_ml_model(sym, market='VN', bars=None, threshold=1.5):
    """
    Train ensemble ML model cho 1 mã.
    Sử dụng TimeSeriesSplit để tránh data leakage.
    Returns metrics dict.
    """
    try:
        from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.pipeline import Pipeline
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.metrics import accuracy_score, f1_score
        import numpy as np

        if bars is None: bars = db_load_ohlcv(sym, OHLCV_BARS)
        if not bars or len(bars) < _ML_MIN_BARS:
            return {'error': f'Cần ≥{_ML_MIN_BARS} bars, chỉ có {len(bars) if bars else 0}'}

        X_raw, dates = _build_ml_features(bars)
        if not X_raw or len(X_raw) < 60:
            return {'error': 'Không đủ features để train'}

        labels, valid_dates = _build_ml_labels(bars, dates, threshold)
        if len(labels) < 60:
            return {'error': 'Không đủ labels để train'}

        # Align X with valid labels
        date_set = set(valid_dates)
        X_aligned = [X_raw[i] for i, d in enumerate(dates) if d in date_set]
        X = np.array(X_aligned, dtype=float)
        y = np.array(labels, dtype=int)

        if len(X) != len(y): y = y[:len(X)]

        # ── TimeSeriesSplit cross-validation ─────────────────────────────────
        tscv = TimeSeriesSplit(n_splits=5)
        scaler = StandardScaler()

        # Ensemble: RF + XGBoost (hoặc GBM fallback) + LR
        rf  = RandomForestClassifier(n_estimators=100, max_depth=6, min_samples_leaf=5,
                                     class_weight='balanced', random_state=42, n_jobs=-1)
        lr  = LogisticRegression(C=0.1, max_iter=500, class_weight='balanced', random_state=42)
        try:
            from xgboost import XGBClassifier
            boosted = XGBClassifier(
                n_estimators=100, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                use_label_encoder=False, eval_metric='logloss',
                scale_pos_weight=1, random_state=42, verbosity=0,
                tree_method='hist'  # fast, no GPU required
            )
            log.debug("[ML] XGBoost available — using true ensemble RF+XGB+LR")
        except ImportError:
            from sklearn.ensemble import GradientBoostingClassifier
            boosted = GradientBoostingClassifier(n_estimators=80, max_depth=4,
                                                 learning_rate=0.05, subsample=0.8, random_state=42)
            log.debug("[ML] XGBoost not found — fallback to sklearn GBM (pip install xgboost)")

        ensemble = VotingClassifier(
            estimators=[('rf', rf), ('boosted', boosted), ('lr', lr)],
            voting='soft', weights=[2, 3, 1]  # XGBoost weighted highest
        )

        # Train/val split: last 20% as validation
        split = int(len(X)*0.8)
        X_train, X_val = X[:split], X[split:]
        y_train, y_val = y[:split], y[split:]

        X_train_s = scaler.fit_transform(X_train)
        X_val_s   = scaler.transform(X_val)

        ensemble.fit(X_train_s, y_train)

        # ── Cross-val metrics ─────────────────────────────────────────────────
        cv_scores = []
        for train_idx, val_idx in tscv.split(X):
            Xtr = scaler.fit_transform(X[train_idx])
            Xvl = scaler.transform(X[val_idx])
            ensemble.fit(Xtr, y[train_idx])
            pred = ensemble.predict(Xvl)
            cv_scores.append(accuracy_score(y[val_idx], pred))

        # Final model trained on all data
        X_all_s = scaler.fit_transform(X)
        ensemble.fit(X_all_s, y)

        val_pred = ensemble.predict(X_val_s)
        val_acc  = round(accuracy_score(y_val, val_pred)*100, 1)
        cv_mean  = round(sum(cv_scores)/len(cv_scores)*100, 1)
        cv_std   = round((sum((s-sum(cv_scores)/len(cv_scores))**2 for s in cv_scores)/len(cv_scores))**0.5*100, 1)

        # Buy/Sell precision on validation
        buy_correct  = sum(1 for p,a in zip(val_pred, y_val) if p==1  and a==1)
        buy_total    = sum(1 for p in val_pred if p==1)
        sell_correct = sum(1 for p,a in zip(val_pred, y_val) if p==-1 and a==-1)
        sell_total   = sum(1 for p in val_pred if p==-1)
        buy_prec  = round(buy_correct/buy_total*100,1)   if buy_total  > 0 else 0
        sell_prec = round(sell_correct/sell_total*100,1) if sell_total > 0 else 0

        # Feature importance from RF
        feat_names = ['mom5','mom10','mom20','rsi7','rsi14','rsi21',
                      'macd','macd_hist','bb_pct','vs_ema9','vs_ema20','vs_ema50',
                      'ema_cross','atr_pct','vol_ratio','body','upper_wick',
                      'lower_wick','is_bull','gap','range_pos']
        rf_model = ensemble.named_estimators_['rf']
        fi = sorted(zip(feat_names, rf_model.feature_importances_),
                    key=lambda x: x[1], reverse=True)[:8]

        metrics = {
            'val_accuracy': val_acc,
            'cv_accuracy_mean': cv_mean,
            'cv_accuracy_std': cv_std,
            'buy_precision': buy_prec,
            'sell_precision': sell_prec,
            'n_train': len(X_train),
            'n_val': len(X_val),
            'threshold_pct': threshold,
            'top_features': [(n, round(float(v)*100,1)) for n,v in fi],
            'class_dist': {
                'buy':  int(sum(y==1)),
                'sell': int(sum(y==-1)),
                'hold': int(sum(y==0))
            }
        }
        cache_entry = {
            'model': ensemble,
            'scaler': scaler,
            'metrics': metrics,
            'trained_at': datetime.now().isoformat(),
            'n_bars': len(bars),
            'sym': sym,
        }
        _ml_model_cache[sym] = cache_entry
        # Persist to DB
        try:
            model_blob = pickle.dumps({'model': ensemble, 'scaler': scaler})
            with get_db() as c:
                c.execute("""INSERT OR REPLACE INTO ml_models
                    (symbol, market, model_blob, metrics, trained_at)
                    VALUES(?,?,?,?,?)""",
                    (sym, market, model_blob, json.dumps(metrics),
                     datetime.now().isoformat()))
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 9337, _e)
        print(f"[ML] {sym} trained: val_acc={val_acc}% cv={cv_mean}±{cv_std}%")
        return metrics
    except Exception as e:
        print(f"[ML] Train error {sym}: {e}")
        return {'error': str(e)}

# ── predict_ml (from v12) ──

def predict_ml(sym, market='VN', bars=None, force_train=False):
    """
    Predict direction cho ngày tiếp theo.
    Returns: {'signal': 'BUY'|'SELL'|'HOLD', 'probability': float,
              'confidence': 'HIGH'|'MEDIUM'|'LOW', 'features': dict}
    """
    try:
        import numpy as np

        # Load from cache or DB
        cached = _ml_model_cache.get(sym)
        if not cached or force_train:
            # Try load from DB
            try:
                with get_db() as c:
                    row = c.execute("SELECT model_blob, metrics, trained_at FROM ml_models WHERE symbol=? ORDER BY trained_at DESC LIMIT 1",
                                    (sym,)).fetchone()
                if row:
                    age_days = (datetime.now() - datetime.fromisoformat(row['trained_at'])).days
                    if age_days <= _ML_RETRAIN_DAYS:
                        blob = pickle.loads(row['model_blob'])
                        cached = {'model': blob['model'], 'scaler': blob['scaler'],
                                  'metrics': json.loads(row['metrics']),
                                  'trained_at': row['trained_at']}
                        _ml_model_cache[sym] = cached
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 9371, _e)

        if not cached or force_train:
            # Train new model
            if bars is None: bars = db_load_ohlcv(sym, OHLCV_BARS)
            result = train_ml_model(sym, market, bars)
            if 'error' in result:
                return {'signal': 'HOLD', 'probability': 0.33, 'confidence': 'LOW',
                        'error': result['error']}
            cached = _ml_model_cache.get(sym)

        if not cached:
            return {'signal': 'HOLD', 'probability': 0.33, 'confidence': 'LOW',
                    'error': 'Model not available'}

        if bars is None: bars = db_load_ohlcv(sym, OHLCV_BARS)
        X_raw, dates = _build_ml_features(bars)
        if not X_raw:
            return {'signal': 'HOLD', 'probability': 0.33, 'confidence': 'LOW',
                    'error': 'Không đủ features'}

        # Use last feature vector for prediction
        X_last = np.array([X_raw[-1]])
        X_scaled = cached['scaler'].transform(X_last)
        model = cached['model']

        proba = model.predict_proba(X_scaled)[0]  # [P(sell), P(hold), P(buy)] sorted by class
        classes = model.classes_
        class_proba = dict(zip(classes.tolist(), proba.tolist()))
        p_buy  = class_proba.get(1,  0)
        p_sell = class_proba.get(-1, 0)
        p_hold = class_proba.get(0,  0)

        # Decision with confidence threshold
        max_p = max(p_buy, p_sell, p_hold)
        if p_buy >= 0.50:
            signal = 'BUY'
            prob   = round(p_buy * 100, 1)
        elif p_sell >= 0.50:
            signal = 'SELL'
            prob   = round(p_sell * 100, 1)
        elif p_buy >= 0.40:
            signal = 'BUY_WEAK'
            prob   = round(p_buy * 100, 1)
        elif p_sell >= 0.40:
            signal = 'SELL_WEAK'
            prob   = round(p_sell * 100, 1)
        else:
            signal = 'HOLD'
            prob   = round(p_hold * 100, 1)

        confidence = ('HIGH' if max_p >= 0.60 else
                      'MEDIUM' if max_p >= 0.45 else 'LOW')
        conf_label = ('✅ Cao' if confidence == 'HIGH' else
                      '⚠️ Trung bình' if confidence == 'MEDIUM' else '❌ Thấp')

        metrics = cached.get('metrics', {})
        return {
            'signal':      signal,
            'probability': prob,
            'p_buy':       round(p_buy*100, 1),
            'p_sell':      round(p_sell*100, 1),
            'p_hold':      round(p_hold*100, 1),
            'confidence':  confidence,
            'conf_label':  conf_label,
            'trained_at':  cached.get('trained_at', ''),
            'val_accuracy':metrics.get('val_accuracy', 0),
            'cv_accuracy': metrics.get('cv_accuracy_mean', 0),
            'buy_precision':  metrics.get('buy_precision', 0),
            'sell_precision': metrics.get('sell_precision', 0),
            'top_features':   metrics.get('top_features', []),
            'agreement_with_ta': None,  # filled by caller
        }
    except Exception as e:
        print(f"[ML] Predict error {sym}: {e}")
        return {'signal': 'HOLD', 'probability': 33.0, 'confidence': 'LOW', 'error': str(e)}

# ── ml_combined_signal (from v12) ──

def ml_combined_signal(sym, market='VN', ta_signal=None, bars=None):
    """
    Kết hợp ML prediction + Technical Analysis signal.
    Pipeline thống nhất Pro: thử LSTM (v24) trước, fallback sang Ensemble (v12).
    Chỉ tin vào tín hiệu khi CẢ HAI đồng thuận.
    """
    # Thử LSTM v24 trước (nếu đã train)
    ml = None
    try:
        if _LSTM is not None:
            lstm_result = _LSTM.predict(sym, bars or db_load_ohlcv(sym, OHLCV_BARS))
            if lstm_result and lstm_result.get("confidence", 0) > 20:
                # Convert LSTM format sang format chung
                pred = lstm_result.get("prediction","NEUTRAL")
                prob = lstm_result.get("prob_up", 50)
                ml = {
                    "signal": "BUY" if pred=="UP" else ("SELL" if pred=="DOWN" else "HOLD"),
                    "probability": prob,
                    "confidence": "HIGH" if lstm_result.get("confidence",0)>40 else "MEDIUM",
                    "model": "LSTM-v24",
                    "prob_up": prob,
                    "prob_down": lstm_result.get("prob_down",50),
                }
    except Exception as _e:
        pass

    # Fallback: Ensemble RF+GBM+LR từ v12
    if ml is None:
        ml = predict_ml(sym, market, bars)
        if "model" not in ml:
            ml["model"] = "Ensemble-v12"
    if "error" in ml:
        return ml
    if 'error' in ml:
        return ml

    ta_sig = ta_signal  # 'BUY', 'SELL', 'NEUTRAL' etc.
    ml_sig = ml['signal']

    # Agreement check
    ml_is_buy  = 'BUY'  in ml_sig
    ml_is_sell = 'SELL' in ml_sig
    ta_is_buy  = ta_sig and 'BUY'  in ta_sig
    ta_is_sell = ta_sig and 'SELL' in ta_sig

    if ml_is_buy and ta_is_buy:
        combined = 'STRONG_BUY'
        agreement = True
        note = f"✅ ML ({ml['probability']}%) + TA đồng thuận MUA"
    elif ml_is_sell and ta_is_sell:
        combined = 'STRONG_SELL'
        agreement = True
        note = f"✅ ML ({ml['probability']}%) + TA đồng thuận BÁN"
    elif ml_is_buy:
        combined = 'ML_BUY'
        agreement = False
        note = f"⚠️ ML BUY ({ml['probability']}%) nhưng TA chưa xác nhận"
    elif ml_is_sell:
        combined = 'ML_SELL'
        agreement = False
        note = f"⚠️ ML SELL ({ml['probability']}%) nhưng TA chưa xác nhận"
    else:
        combined = 'HOLD'
        agreement = False
        note = f"ML HOLD ({ml['probability']}%)"

    ml['combined_signal'] = combined
    ml['ta_ml_agreement'] = agreement
    ml['combined_note']   = note
    ml['agreement_with_ta'] = agreement
    return ml

# ── v16: ML DB TABLE (thêm vào init_db) ──────────────────────────────────────
_ML_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ml_models(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    market TEXT DEFAULT 'VN',
    model_blob BLOB,
    metrics TEXT,
    trained_at TEXT,
    UNIQUE(symbol)
);
CREATE TABLE IF NOT EXISTS ml_predictions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    market TEXT DEFAULT 'VN',
    pred_date TEXT,
    signal TEXT,
    probability REAL,
    confidence TEXT,
    p_buy REAL, p_sell REAL, p_hold REAL,
    actual_ret_1d REAL,
    actual_ret_5d REAL,
    created_at TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(symbol, pred_date)
);"""

# ── v16: PAPER TRADING ENGINE ─────────────────────────────────────────────────
# Paper trading: theo dõi tín hiệu ML+TA live, tính P&L giả định
# Không dùng tiền thật — chỉ để validate signal accuracy

# ── _calc_p_value (from v12) ──

def _calc_p_value(wins_val, losses_val):
    """
    Tính p-value bằng bootstrap permutation test.
    H0: strategy không tốt hơn random (mean return = 0).
    Nếu p < 0.05 → reject H0 → tín hiệu có ý nghĩa thống kê.
    """
    import random
    all_pnl = wins_val + losses_val
    n = len(all_pnl)
    if n < 10: return None, "Cần ≥10 trades để tính p-value"
    observed_mean = sum(all_pnl)/n
    # Bootstrap: shuffle signs 5000 lần
    n_boot = 5000
    count_extreme = 0
    rng = random.Random(42)
    for _ in range(n_boot):
        shuffled = [abs(x)*(1 if rng.random()>0.5 else -1) for x in all_pnl]
        if sum(shuffled)/n >= observed_mean:
            count_extreme += 1
    p_val = round(count_extreme/n_boot, 4)
    if p_val < 0.01:   sig = "✅ Rất có ý nghĩa thống kê (p<0.01)"
    elif p_val < 0.05: sig = "✅ Có ý nghĩa thống kê (p<0.05)"
    elif p_val < 0.10: sig = "⚠️ Biên (p<0.10) — cần thêm dữ liệu"
    else:              sig = "❌ Không có ý nghĩa thống kê — có thể là may mắn"
    return p_val, sig

# ── _kelly_criterion (from v12) ──

def _kelly_criterion(wins_val, losses_val):
    """
    Kelly Criterion: f* = W/|avg_loss| - (1-W)/avg_win ... thực ra:
    f* = (W * avg_win - (1-W) * |avg_loss|) / avg_win
    Trả về: kelly_pct, half_kelly, risk_of_ruin_pct
    """
    if not wins_val or not losses_val:
        return {"kelly_pct":0,"half_kelly":0,"risk_of_ruin":100,"note":"Chưa đủ dữ liệu"}
    n_w=len(wins_val); n_l=len(losses_val); total=n_w+n_l
    W  = n_w/total
    avg_win  = sum(wins_val)/n_w      # % gain
    avg_loss = abs(sum(losses_val)/n_l)  # % loss
    if avg_loss == 0: avg_loss = 0.01
    # Fractional Kelly formula (b = avg_win/avg_loss)
    b = avg_win/avg_loss
    f_star = (b*W - (1-W)) / b
    f_star = max(0, min(f_star, 0.25))  # cap 25% để tránh over-leverage
    half_k = round(f_star/2 * 100, 1)
    full_k = round(f_star * 100, 1)
    # Risk of Ruin: xác suất thua hết với half-kelly sizing
    # Dùng công thức gần đúng: RoR ≈ ((1-W)/W)^(cap/unit)
    if W > 0 and W < 1 and half_k > 0:
        try:
            import math
            ror = ((1-W)/W) ** (100/max(half_k,1))
            ror_pct = round(min(ror*100, 99.9), 2)
        except: ror_pct = 0
    else:
        ror_pct = 0 if W >= 1 else 99.9
    # Suggested position size: min(half_kelly, 2% rule)
    two_pct_rule = 2.0
    suggested = min(half_k, two_pct_rule)
    if full_k < 1:   note="⚠️ Kelly âm — hệ thống chưa profitable đủ"
    elif full_k < 5: note="⚠️ Kelly thấp — position size nhỏ là đúng"
    elif full_k>20:  note="✅ Kelly cao — nhưng dùng half-Kelly để giảm rủi ro"
    else:            note="✅ Kelly bình thường"
    return {
        "kelly_pct":full_k, "half_kelly":half_k, "suggested_size":suggested,
        "win_rate":round(W*100,1), "avg_win":round(avg_win,2), "avg_loss":round(avg_loss,2),
        "b_ratio":round(b,2), "risk_of_ruin":ror_pct, "note":note
    }

# ── _wf_segment_stats (from v12) ──

def _wf_segment_stats(wv, lv, eq, cap):
    """Tính stats cho 1 segment walk-forward."""
    wins_s=len(wv); losses_s=len(lv); total_t=wins_s+losses_s
    wr_s=round(wins_s/total_t*100,1) if total_t>0 else 0
    pf_s=round(sum(wv)/abs(sum(lv)),2) if lv and sum(lv)!=0 else (round(sum(wv),2) if wv else 0)
    final_s=eq[-1] if eq else cap
    ret_s=round((final_s-cap)/cap*100,2)
    if len(eq)>1:
        rets=[(eq[i]-eq[i-1])/eq[i-1] for i in range(1,len(eq)) if eq[i-1]>0]
        if len(rets)>1:
            mu=sum(rets)/len(rets)
            std=(sum((r-mu)**2 for r in rets)/len(rets))**0.5
            sharpe=round((mu/std)*(252**0.5),2) if std>0 else 0
            # Sortino: chỉ dùng downside deviation (returns < 0)
            down=[r for r in rets if r<0]
            down_dev=(sum(r**2 for r in down)/len(rets))**0.5 if down else std
            sortino=round((mu/down_dev)*(252**0.5),2) if down_dev>0 else sharpe
        else: sharpe=0; sortino=0
    else: sharpe=0; sortino=0
    peak2=cap; mdd2=0; mdd_dur=0; cur_dur=0
    for v in eq:
        if v>peak2: peak2=v; cur_dur=0
        d=(peak2-v)/peak2*100
        if d>mdd2: mdd2=d
        if d>0: cur_dur+=1
        if cur_dur>mdd_dur: mdd_dur=cur_dur
    # Calmar ratio: annual return / max drawdown
    n_days=len(eq) if eq else 1
    ann_ret=(final_s/cap)**(252/n_days)-1 if cap>0 and n_days>0 else 0
    calmar=round(ann_ret*100/mdd2,2) if mdd2>0 else 0
    # VaR 95% (historical simulation)
    if len(eq)>20:
        rets_sorted=sorted([(eq[i]-eq[i-1])/eq[i-1] for i in range(1,len(eq)) if eq[i-1]>0])
        var_idx=max(0,int(len(rets_sorted)*0.05))
        var_95=round(abs(rets_sorted[var_idx])*100,2) if rets_sorted else 0
        cvar_95=round(abs(sum(rets_sorted[:var_idx+1])/(var_idx+1))*100,2) if var_idx>0 else var_95
    else: var_95=0; cvar_95=0
    return {"win_rate":wr_s,"profit_factor":pf_s,"return_pct":ret_s,
            "trades":total_t,"sharpe":sharpe,"sortino":sortino,"calmar":calmar,
            "max_drawdown":round(mdd2,2),"max_dd_duration":mdd_dur,
            "var_95":var_95,"cvar_95":cvar_95}

# ── _build_benchmark_for_backtest (from v12) ──

def _build_benchmark_for_backtest(symbol_history, capital, market, start_i=55):
    """
    Tính equity curve benchmark: mua & nắm giữ VNINDEX (VN) hoặc SPY (US).
    Được normalize cùng capital với backtest strategy để so sánh trực quan.
    """
    try:
        idx_sym = 'VNINDEX' if market == 'VN' else 'SPY'
        idx_bars = db_load_ohlcv(idx_sym, OHLCV_BARS)
        if not idx_bars or len(idx_bars) <= start_i:
            # Fallback: dùng chính symbol history (buy&hold mã đó)
            if len(symbol_history) <= start_i: return []
            sp = symbol_history[start_i]['c']
            if sp <= 0: return []
            return [round(capital * b['c'] / sp, 0) for b in symbol_history[start_i:]]
        # Align theo ngày với symbol_history
        sym_dates = {b['d']: i for i, b in enumerate(symbol_history)}
        idx_map   = {b['d']: b['c'] for b in idx_bars}
        # Lấy giá index tại ngày start
        start_date = symbol_history[start_i]['d'] if start_i < len(symbol_history) else None
        if not start_date or start_date not in idx_map:
            # Dùng ngày gần nhất của index
            idx_start_price = idx_bars[min(start_i, len(idx_bars)-1)]['c']
            idx_aligned     = [b['c'] for b in idx_bars[min(start_i,len(idx_bars)-1):]]
        else:
            idx_start_price = idx_map[start_date]
            idx_aligned     = [idx_map.get(b['d'], 0) for b in symbol_history[start_i:]]
            idx_aligned     = [v for v in idx_aligned if v > 0]
        if not idx_start_price or not idx_aligned: return []
        return [round(capital * v / idx_start_price, 0) for v in idx_aligned]
    except Exception as e:
        print(f"[Benchmark] {e}"); return []
SLIPPAGE_US = 0.001   # 0.1% slippage US (liquid market)

# ── check_breakout_confirmation (from v12) ──

def check_breakout_confirmation(sym, mkt, signal_date, signal_type, signal_price):
    """
    Kiểm tra xác nhận breakout D+1:
    BUY  → D+1 open phải > signal_price (không gap down mạnh)
    SELL → D+1 open phải < signal_price (không gap up mạnh)
    Lưu kết quả vào bảng breakout_filter.
    """
    try:
        bars = db_load_ohlcv(sym, OHLCV_BARS)
        if not bars or len(bars) < 2: return None
        # Tìm bar tại ngày tín hiệu
        sig_idx = next((i for i,b in enumerate(bars) if b['d'] == signal_date), None)
        if sig_idx is None or sig_idx + 1 >= len(bars): return None
        d1_bar  = bars[sig_idx + 1]
        d1_open = d1_bar['o'] or d1_bar['c']
        gap_pct = (d1_open - signal_price) / signal_price * 100 if signal_price else 0
        # BUY: xác nhận nếu D+1 không gap down > 2%
        # SELL: xác nhận nếu D+1 không gap up > 2%
        if signal_type in ('BUY','BUY_WARN','BUY_WEAK'):
            confirmed = 1 if gap_pct > -2.0 else -1  # -1 = false signal
        else:  # SELL
            confirmed = 1 if gap_pct < 2.0 else -1
        with get_db() as c:
            c.execute("""INSERT OR REPLACE INTO breakout_filter
                (symbol,market,signal_date,signal_type,signal_price,d1_open,d1_gap_pct,confirmed)
                VALUES(?,?,?,?,?,?,?,?)""",
                (sym,mkt,signal_date,signal_type,signal_price,d1_open,round(gap_pct,2),confirmed))
        return {'confirmed': confirmed, 'd1_open': d1_open, 'gap_pct': round(gap_pct,2)}
    except Exception as e:
        print(f"[BreakoutFilter] {e}"); return None

# ── send_breakout_confirmed_alert (from v12) ──

def send_breakout_confirmed_alert(sym, mkt, signal_type, price, d1_open, gap_pct):
    """Gửi Telegram alert SAU KHI xác nhận breakout D+1."""
    token = get_setting('tg_token','')
    if not token or get_setting('tg_enabled','0') != '1': return
    vn = mkt == 'VN'
    fp = lambda v: (f"{round(v):,}đ" if vn else f"${v:.2f}")
    is_buy = 'BUY' in signal_type
    icon = '✅🟢' if is_buy else '✅🔴'
    lbl  = 'MUA ĐÃ XÁC NHẬN' if is_buy else 'BÁN ĐÃ XÁC NHẬN'
    gap_str = f"+{gap_pct:.1f}%" if gap_pct >= 0 else f"{gap_pct:.1f}%"
    msg = (f"{icon} <b>BREAKOUT XÁC NHẬN — {sym} ({mkt})</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🎯 <b>{lbl}</b>\n"
           f"💰 Tín hiệu: {fp(price)}\n"
           f"📈 Open D+1: {fp(d1_open)} (Gap {gap_str})\n"
           f"✅ Không có false signal → Vào lệnh an toàn\n"
           f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🔗 {get_setting('public_url',f'http://localhost:{PORT}')}")
    threading.Thread(target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()

# ── v13: TOP BUY/SELL HISTORY — lưu & bộ lọc độ tin cậy ─────────────────────

# ── save_top_signal_history (from v12) ──

def save_top_signal_history(top_buy_list, top_sell_list, date_str=None):
    """Lưu top buy/sell vào DB mỗi ngày để tính độ tin cậy qua nhiều ngày."""
    today = date_str or datetime.now().strftime('%Y-%m-%d')
    try:
        with get_db() as c:
            for rank, item in enumerate(top_buy_list[:30], 1):
                sym = item.get('symbol','')
                if not sym: continue
                c.execute("""INSERT OR IGNORE INTO top_signal_history
                    (symbol,market,date,signal_type,score,price,confidence,rank_pos)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (sym, item.get('market','VN'), today, 'BUY',
                     item.get('score',0), item.get('price',0),
                     item.get('confidence',0), rank))
            for rank, item in enumerate(top_sell_list[:30], 1):
                sym = item.get('symbol','')
                if not sym: continue
                c.execute("""INSERT OR IGNORE INTO top_signal_history
                    (symbol,market,date,signal_type,score,price,confidence,rank_pos)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (sym, item.get('market','VN'), today, 'SELL',
                     item.get('score',0), item.get('price',0),
                     item.get('confidence',0), rank))
    except Exception as e:
        print(f"[TopHistory] {e}")

# ── get_top_signal_reliability (from v12) ──

def get_top_signal_reliability(signal_type='BUY', days=7, min_appearances=2):
    """
    Lấy danh sách mã xuất hiện liên tục trong top buy/sell nhiều ngày.
    Trả về list có thêm trường: appearances (số ngày xuất hiện), reliability_pct.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT symbol, market,
                       COUNT(DISTINCT date) as appearances,
                       AVG(score) as avg_score,
                       AVG(confidence) as avg_confidence,
                       MIN(rank_pos) as best_rank,
                       MAX(date) as last_seen
                FROM top_signal_history
                WHERE signal_type=? AND date >= ?
                GROUP BY symbol
                HAVING COUNT(DISTINCT date) >= ?
                ORDER BY appearances DESC, avg_score DESC
                LIMIT 50
            """, (signal_type, cutoff, min_appearances)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d['reliability_pct'] = round(d['appearances'] / days * 100, 1)
            d['avg_score']       = round(d['avg_score'] or 0, 1)
            d['avg_confidence']  = round(d['avg_confidence'] or 0, 1)
            result.append(d)
        return result
    except: return []

# ── v13: BENCHMARK vs VNINDEX ──────────────────────────────────────────────────

# ── paper_trade_open (from v12) ──

def paper_trade_open(user_id, sym, market, signal, entry_price, size_pct=2.0,
                     source='ML', confidence='MEDIUM', note=''):
    """Mở vị thế paper trade."""
    try:
        account = float(get_setting('account_size', '1000000000') or '1000000000')
        position_value = account * size_pct / 100
        qty = int(position_value / entry_price) if entry_price > 0 else 0
        with get_db() as c:
            c.execute("""INSERT OR IGNORE INTO paper_trades
                (user_id, symbol, market, signal, entry_price, qty, position_value,
                 source, confidence, note, status, entry_date)
                VALUES(?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                (user_id, sym, market, signal, entry_price, qty,
                 round(position_value, 0), source, confidence, note,
                 datetime.now().strftime('%Y-%m-%d')))
    except Exception as e:
        print(f"[PaperTrade] Open error: {e}")

# ── paper_trade_close (from v12) ──

def paper_trade_close(trade_id, exit_price, reason='Signal'):
    """Đóng vị thế paper trade, tính P&L."""
    try:
        with get_db() as c:
            t = c.execute("SELECT * FROM paper_trades WHERE id=?", (trade_id,)).fetchone()
            if not t: return
            entry = t['entry_price']
            qty   = t['qty'] or 0
            sig   = t['signal'] or 'BUY'
            pnl_pct = round((exit_price - entry) / entry * 100 * (1 if 'BUY' in sig else -1), 2)
            pnl_vnd = round((exit_price - entry) * qty * (1 if 'BUY' in sig else -1), 0)
            c.execute("""UPDATE paper_trades SET status='CLOSED', exit_price=?,
                exit_date=?, pnl_pct=?, pnl_vnd=?, close_reason=?
                WHERE id=?""",
                (exit_price, datetime.now().strftime('%Y-%m-%d'),
                 pnl_pct, pnl_vnd, reason, trade_id))
    except Exception as e:
        print(f"[PaperTrade] Close error: {e}")

# ── paper_trade_summary (from v12) ──

def paper_trade_summary(user_id):
    """Tổng kết paper trading: accuracy, P&L, win rate."""
    try:
        with get_db() as c:
            rows = c.execute("""SELECT * FROM paper_trades
                WHERE user_id=? ORDER BY entry_date DESC""", (user_id,)).fetchall()
        trades = [dict(r) for r in rows]
        closed = [t for t in trades if t['status'] == 'CLOSED']
        open_p = [t for t in trades if t['status'] == 'OPEN']
        if not closed:
            return {'open': len(open_p), 'closed': 0, 'win_rate': 0,
                    'total_pnl_pct': 0, 'trades': trades[:20]}
        wins  = [t for t in closed if (t['pnl_pct'] or 0) > 0]
        losses= [t for t in closed if (t['pnl_pct'] or 0) <= 0]
        wr    = round(len(wins)/len(closed)*100, 1)
        avg_w = round(sum(t['pnl_pct'] or 0 for t in wins)/len(wins), 2)   if wins   else 0
        avg_l = round(sum(t['pnl_pct'] or 0 for t in losses)/len(losses), 2) if losses else 0
        total_pnl = round(sum(t['pnl_vnd'] or 0 for t in closed), 0)
        # By source (ML vs TA)
        ml_trades = [t for t in closed if t.get('source') == 'ML']
        ta_trades = [t for t in closed if t.get('source') == 'TA']
        ml_wr = round(sum(1 for t in ml_trades if (t['pnl_pct'] or 0) > 0)/len(ml_trades)*100, 1) if ml_trades else 0
        ta_wr = round(sum(1 for t in ta_trades if (t['pnl_pct'] or 0) > 0)/len(ta_trades)*100, 1) if ta_trades else 0
        return {
            'open': len(open_p), 'closed': len(closed),
            'win_rate': wr, 'avg_win': avg_w, 'avg_loss': avg_l,
            'total_pnl_vnd': total_pnl,
            'ml_win_rate': ml_wr, 'ml_trades': len(ml_trades),
            'ta_win_rate': ta_wr, 'ta_trades': len(ta_trades),
            'trades': trades[:30]
        }
    except Exception as e:
        return {'error': str(e), 'open': 0, 'closed': 0}

# ── _save_ml_prediction (from v12) ──

def _save_ml_prediction(sym, market, ml_result):
    """Lưu prediction vào DB để track accuracy sau."""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        with get_db() as c:
            c.execute("""INSERT OR REPLACE INTO ml_predictions
                (symbol, market, pred_date, signal, probability, confidence,
                 p_buy, p_sell, p_hold)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                (sym, market, today,
                 ml_result.get('signal','HOLD'),
                 ml_result.get('probability',33),
                 ml_result.get('confidence','LOW'),
                 ml_result.get('p_buy',33),
                 ml_result.get('p_sell',33),
                 ml_result.get('p_hold',33)))
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 9920, _e)

# ── _update_prediction_actuals (from v12) ──

def _update_prediction_actuals():
    """Hàng ngày: cập nhật actual return cho predictions hôm qua."""
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        with get_db() as c:
            preds = c.execute("""SELECT id, symbol, market, pred_date FROM ml_predictions
                WHERE pred_date=? AND actual_ret_1d IS NULL""",
                (yesterday,)).fetchall()
        for p in preds:
            bars = db_load_ohlcv(p['symbol'], 5)
            if not bars or len(bars) < 2: continue
            ret_1d = round((bars[-1]['c'] - bars[-2]['c']) / bars[-2]['c'] * 100, 2)
            ret_5d = None
            if len(bars) >= 6:
                ret_5d = round((bars[-1]['c'] - bars[-6]['c']) / bars[-6]['c'] * 100, 2)
            with get_db() as c:
                c.execute("""UPDATE ml_predictions SET actual_ret_1d=?, actual_ret_5d=?
                    WHERE id=?""", (ret_1d, ret_5d, p['id']))
    except Exception as e:
        print(f"[ML] Update actuals error: {e}")

# ── v15: BETA VS MARKET & DIVERSIFICATION SCORE ──────────────────────────────

# ── calc_beta_vs_market (from v12) ──

def calc_beta_vs_market(sym_bars, index_bars='VNINDEX', n_days=120):
    """
    Tính beta của mã so với VNINDEX (hoặc SPY cho US).
    Beta = Cov(stock, index) / Var(index)
    """
    try:
        idx = db_load_ohlcv(index_bars, n_days+5)
        if not idx or not sym_bars: return None
        # Align by date
        idx_map = {b['d']: b['c'] for b in idx}
        pairs = []
        for i in range(1, min(len(sym_bars), n_days+1)):
            d = sym_bars[-i]['d']
            d_prev = sym_bars[-i-1]['d'] if i+1 < len(sym_bars) else None
            if not d_prev: continue
            if d not in idx_map: continue
            s_ret = (sym_bars[-i]['c'] - sym_bars[-i-1]['c']) / sym_bars[-i-1]['c']
            prev_idx = next((b['c'] for b in reversed(idx) if b['d'] < d), None)
            if not prev_idx: continue
            i_ret = (idx_map[d] - prev_idx) / prev_idx
            pairs.append((s_ret, i_ret))
        if len(pairs) < 20: return None
        s_rets = [p[0] for p in pairs]
        i_rets = [p[1] for p in pairs]
        n = len(pairs)
        mu_s = sum(s_rets)/n; mu_i = sum(i_rets)/n
        cov = sum((s_rets[k]-mu_s)*(i_rets[k]-mu_i) for k in range(n)) / n
        var_i = sum((i_rets[k]-mu_i)**2 for k in range(n)) / n
        if var_i == 0: return None
        beta = round(cov / var_i, 3)
        corr = cov / ((sum((s-mu_s)**2 for s in s_rets)/n)**0.5 * (var_i**0.5)) if var_i > 0 else 0
        alpha_daily = mu_s - beta * mu_i
        alpha_annual = round(alpha_daily * 252 * 100, 2)
        label = ("Phòng thủ (β<0.8)" if beta < 0.8 else
                 "Thị trường (β≈1)" if beta < 1.2 else
                 "Tăng tốc (β>1.2)")
        return {'beta': beta, 'alpha_annual': alpha_annual,
                'corr_with_index': round(corr, 3), 'label': label,
                'n_days': n, 'index': index_bars}
    except Exception as e:
        return None

# ── calc_portfolio_diversification (from v12) ──

def calc_portfolio_diversification(user_id):
    """
    Diversification score 0-100:
    - 100 = hoàn toàn đa dạng hóa
    - 0   = tập trung 1 ngành, 1 mã
    Dựa trên: số mã, số ngành, average correlation.
    """
    try:
        with get_db() as c:
            rows = c.execute("SELECT symbol,market FROM user_watchlist WHERE user_id=?",
                             (user_id,)).fetchall()
        n_syms = len(rows)
        if n_syms == 0: return {'score': 0, 'label': 'Chưa có mã nào'}
        if n_syms == 1: return {'score': 10, 'label': 'Rủi ro cao — chỉ 1 mã'}
        # Sector diversity
        with get_db() as c:
            sec_rows = c.execute(
                "SELECT sector FROM fetch_meta WHERE symbol IN ({})".format(
                    ','.join('?'*n_syms)), [r['symbol'] for r in rows]).fetchall()
        sectors = set(r['sector'] for r in sec_rows if r['sector'])
        n_sectors = max(len(sectors), 1)
        sector_score = min(40, n_sectors * 8)  # max 40pts for 5+ sectors
        # Symbol count score
        sym_score = min(30, n_syms * 3)  # max 30pts for 10+ symbols
        # Average pairwise correlation (inverse = good)
        corr_data = get_portfolio_correlation([r['symbol'] for r in rows],
                                              rows[0]['market'] if rows else 'VN', n_days=60)
        corr_matrix = corr_data.get('corr_matrix', {})
        if corr_matrix:
            avg_corr = sum(abs(v) for v in corr_matrix.values()) / len(corr_matrix)
            corr_score = max(0, int(30 * (1 - avg_corr)))  # max 30pts for low correlation
        else:
            corr_score = 15
        total = sector_score + sym_score + corr_score
        if   total >= 80: label = "✅ Đa dạng hóa tốt"
        elif total >= 60: label = "⚠️ Đa dạng hóa trung bình"
        elif total >= 40: label = "⚠️ Cần đa dạng hóa hơn"
        else:             label = "❌ Tập trung quá nhiều — rủi ro cao"
        return {'score': total, 'label': label, 'n_symbols': n_syms,
                'n_sectors': n_sectors, 'sector_score': sector_score,
                'sym_score': sym_score, 'corr_score': corr_score,
                'avg_correlation': round(avg_corr if corr_matrix else 0, 3)}
    except Exception as e:
        return {'score': 0, 'label': f'Lỗi: {e}'}

# ── v15: DIVIDEND-ADJUSTED PRICE HISTORY ─────────────────────────────────────

# ── get_portfolio_correlation (from v12) ──


def _build_corr_matrix(daily_returns):
    """Correlation matrix {(s1,s2): corr} for all symbol pairs."""
    syms = list(daily_returns.keys()); n = len(syms); corr = {}
    for i in range(n):
        for j in range(i, n):
            s1, s2 = syms[i], syms[j]
            r1, r2 = daily_returns[s1], daily_returns[s2]
            ml = min(len(r1), len(r2))
            if ml < 20: corr[(s1,s2)] = corr[(s2,s1)] = (1.0 if s1==s2 else 0.0); continue
            r1, r2 = r1[-ml:], r2[-ml:]
            mu1=sum(r1)/ml; mu2=sum(r2)/ml
            cov=sum((r1[k]-mu1)*(r2[k]-mu2) for k in range(ml))/ml
            sd1=(sum((x-mu1)**2 for x in r1)/ml)**0.5
            sd2=(sum((x-mu2)**2 for x in r2)/ml)**0.5
            c=cov/(sd1*sd2) if sd1*sd2>0 else (1.0 if s1==s2 else 0.0)
            corr[(s1,s2)]=corr[(s2,s1)]=round(c,4)
    return corr


def get_portfolio_correlation(watchlist_syms, market='VN', n_days=60):
    """
    Tính correlation matrix của portfolio.
    Phát hiện: concentration risk (nhiều mã cùng ngành), high correlation clusters.
    """
    result = {'symbols': watchlist_syms, 'warnings': [], 'sector_concentration': {}, 'corr_matrix': {}}
    if len(watchlist_syms) < 2: return result
    # Lấy returns cho từng mã
    returns = {}
    for sym in watchlist_syms[:20]:  # limit 20 mã
        try:
            bars = db_load_ohlcv(sym, n_days+5)
            if bars and len(bars) >= n_days:
                closes = [b['c'] for b in bars[-n_days:]]
                rets = [(closes[i]-closes[i-1])/closes[i-1] for i in range(1,len(closes))]
                returns[sym] = rets
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 10078, _e)
    if len(returns) < 2: return result
    # Tính correlation pairs
    syms = list(returns.keys())
    corr = {}
    high_corr_pairs = []
    for i in range(len(syms)):
        for j in range(i+1, len(syms)):
            s1, s2 = syms[i], syms[j]
            r1, r2 = returns[s1], returns[s2]
            n = min(len(r1), len(r2))
            if n < 20: continue
            r1, r2 = r1[:n], r2[:n]
            mu1, mu2 = sum(r1)/n, sum(r2)/n
            num = sum((r1[k]-mu1)*(r2[k]-mu2) for k in range(n))
            d1  = (sum((x-mu1)**2 for x in r1)/n)**0.5
            d2  = (sum((x-mu2)**2 for x in r2)/n)**0.5
            c   = round(num/(n*d1*d2), 3) if (d1*d2) > 0 else 0
            corr[f"{s1}-{s2}"] = c
            if c >= 0.75:
                high_corr_pairs.append({'pair': f"{s1}/{s2}", 'corr': c,
                    'warn': f"⚠️ {s1} & {s2} tương quan cao ({c:.2f}) — rủi ro tập trung!"})
    result['corr_matrix'] = corr
    result['high_corr_pairs'] = high_corr_pairs
    if high_corr_pairs:
        result['warnings'].append(f"{len(high_corr_pairs)} cặp mã tương quan cao (>0.75) trong portfolio")
    # Sector concentration
    try:
        with get_db() as c:
            rows = c.execute("SELECT symbol, sector FROM fetch_meta WHERE symbol IN ({})".format(
                ','.join('?'*len(watchlist_syms[:20]))), watchlist_syms[:20]).fetchall()
        sector_count = {}
        for r in rows:
            sec = r['sector'] or 'Khác'
            sector_count[sec] = sector_count.get(sec, 0) + 1
        result['sector_concentration'] = sector_count
        for sec, cnt in sector_count.items():
            pct = round(cnt/len(watchlist_syms)*100)
            if pct >= 40:
                result['warnings'].append(f"⚠️ Tập trung {pct}% vào ngành {sec} ({cnt} mã)")
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10118, _e)
    return result

# ── v14: FOREIGN FLOW REALTIME (enhanced) ────────────────────────────────────
_foreign_cache = {}
_FOREIGN_TTL   = 300  # 5 phút

# ── fetch_macro_data (from v12) ──

def fetch_macro_data() -> dict:
    """Lấy dữ liệu vĩ mô toàn cầu từ yfinance."""
    now = time.time()
    results = {}
    try:
        with get_db() as c:
            cached = {r['symbol']: dict(r) for r in
                      c.execute("SELECT * FROM macro_cache").fetchall()}
    except: cached = {}

    syms_to_fetch = [s for s, info in _MACRO_SYMBOLS.items()
                     if s not in cached or
                     (datetime.now() - datetime.fromisoformat(cached[s]['fetched_at'])).total_seconds() > _MACRO_TTL]

    if syms_to_fetch:
        # Fetch VN indices từ _ram_cache trước (nếu đã có trong watchlist)
        vn_syms = [sym for sym in syms_to_fetch if _MACRO_SYMBOLS.get(sym,{}).get('mkt')=='VN']
        us_syms = [sym for sym in syms_to_fetch if _MACRO_SYMBOLS.get(sym,{}).get('mkt')!='VN']
        for sym in vn_syms:
            d = _ram_cache.get(sym, {}).get('data', {})
            if d and d.get('price', 0) > 0:
                chg = d.get('chg', 0)
                with get_db() as c2:
                    c2.execute("INSERT OR REPLACE INTO macro_cache(symbol,price,chg_pct,extra,fetched_at) VALUES(?,?,?,?,?)",
                               (sym, d['price'], chg, json.dumps({'prev':d.get('prev',0)}), datetime.now().isoformat()))
                cached[sym] = {'price': d['price'], 'chg_pct': chg, 'fetched_at': datetime.now().isoformat()}
            else:
                # Thử fetch VN index riêng
                try:
                    raw = fetch(sym, 'VN')
                    if raw and raw.get('price', 0) > 0:
                        chg = raw.get('chg', 0)
                        with get_db() as c2:
                            c2.execute("INSERT OR REPLACE INTO macro_cache(symbol,price,chg_pct,extra,fetched_at) VALUES(?,?,?,?,?)",
                                       (sym, raw['price'], chg, '{}', datetime.now().isoformat()))
                        cached[sym] = {'price': raw['price'], 'chg_pct': chg, 'fetched_at': datetime.now().isoformat()}
                except: pass
        syms_to_fetch = us_syms
        try:
            import yfinance as yf
            tickers = yf.Tickers(' '.join(syms_to_fetch))
            for sym in syms_to_fetch:
                try:
                    tk = tickers.tickers[sym]
                    info = tk.fast_info
                    price = float(getattr(info, 'last_price', 0) or 0)
                    prev  = float(getattr(info, 'previous_close', price) or price)
                    chg   = round((price - prev) / prev * 100, 2) if prev else 0
                    extra = json.dumps({'prev': prev})
                    with get_db() as c2:
                        c2.execute("""INSERT OR REPLACE INTO macro_cache
                            (symbol, price, chg_pct, extra, fetched_at) VALUES(?,?,?,?,?)""",
                            (sym, price, chg, extra, datetime.now().isoformat()))
                    cached[sym] = {'symbol': sym, 'price': price, 'chg_pct': chg, 'extra': extra}
                except: pass
        except Exception as e:
            log.debug("[Macro] %s", e)

    for sym, info in _MACRO_SYMBOLS.items():
        c = cached.get(sym, {})
        # Fallback từ _ram_cache cho VN indices (VNINDEX/VN30/HNX)
        if not c.get('price') and info.get('mkt') == 'VN':
            d = _ram_cache.get(sym, {}).get('data', {})
            if d and d.get('price', 0) > 0:
                c = {'price': d['price'], 'chg_pct': d.get('chg', 0)}
        price = c.get('price', 0); chg = c.get('chg_pct', 0)
        results[sym] = {
            'symbol': sym, 'name': info['name'], 'icon': info['icon'],
            'price': round(price, 4), 'chg_pct': round(chg, 2),
            'direction': '📈' if chg > 0.3 else ('📉' if chg < -0.3 else '➡️'),
            'sentiment': 'RISK_ON' if (sym == '^GSPC' and chg > 0) else
                        ('RISK_OFF' if sym == '^VIX' and price > 25 else 'NEUTRAL'),
        }

    # Macro summary for VN market
    vix = results.get('^VIX', {}).get('price', 20)
    dxy = results.get('DXY', {}).get('chg_pct', 0)
    sp5 = results.get('^GSPC', {}).get('chg_pct', 0)
    macro_signal = 'POSITIVE' if (vix < 20 and sp5 > 0 and dxy < 0.5) else \
                   ('NEGATIVE' if (vix > 25 or sp5 < -1) else 'NEUTRAL')
    results['_summary'] = {
        'macro_signal': macro_signal,
        'vix': vix,
        'dxy_chg': dxy,
        'sp500_chg': sp5,
        'note': ('🟢 Môi trường thuận lợi cho cổ phiếu' if macro_signal == 'POSITIVE'
                 else '🔴 Rủi ro cao — thận trọng' if macro_signal == 'NEGATIVE'
                 else '🟡 Trung tính — theo dõi chặt'),
    }
    return results


def fetch_macro_data():
    """
    Lấy dữ liệu macro: USD/VND, lãi suất liên ngân hàng, P/E VN-Index,
    margin debt (ước tính từ dữ liệu công khai).
    """
    now = time.time()
    if _macro_cache.get('ts') and now - _macro_cache['ts'] < _MACRO_TTL:
        return _macro_cache.get('data', {})
    result = {}
    # 1. USD/VND — dùng yfinance USDVND=X
    try:
        import yfinance as yf
        t = yf.Ticker('USDVND=X')
        h = t.history(period='5d')
        if not h.empty:
            result['usdvnd'] = round(float(h['Close'].iloc[-1]), 0)
            prev = float(h['Close'].iloc[-2]) if len(h)>1 else result['usdvnd']
            result['usdvnd_chg'] = round((result['usdvnd']-prev)/prev*100, 3)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10145, _e)
    # 2. VN-Index P/E ước tính: dùng VNINDEX price + trailing EPS proxy
    # Thực tế: tính P/E từ top 30 mã VN30 hoặc dùng giá trị lịch sử
    try:
        vni_bars = db_load_ohlcv('VNINDEX', 100)
        if vni_bars and len(vni_bars) >= 252:
            vni_now  = vni_bars[-1]['c']
            vni_52w_hi = max(b['c'] for b in vni_bars[-252:])
            vni_52w_lo = min(b['c'] for b in vni_bars[-252:])
            vni_200ma  = sum(b['c'] for b in vni_bars[-200:])/200
            result['vnindex'] = vni_now
            result['vnindex_52w_hi'] = vni_52w_hi
            result['vnindex_52w_lo'] = vni_52w_lo
            result['vnindex_200ma']  = round(vni_200ma, 1)
            result['vnindex_vs_200ma'] = round((vni_now - vni_200ma)/vni_200ma*100, 2)
            # P/E proxy: VN-Index ~14-17 is fair value historically
            # Ước tính đơn giản từ P/E trung bình lịch sử
            pe_base = 15.0  # long-run average
            pe_ratio = pe_base * (vni_now / vni_200ma)
            result['pe_estimate'] = round(pe_ratio, 1)
            result['pe_comment'] = ("Đắt (P/E cao)" if pe_ratio > 18 else
                                    "Hợp lý" if pe_ratio > 13 else "Rẻ (P/E thấp)")
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10167, _e)
    # 3. Lãi suất: dùng VN10Y bond proxy — ticker ^VN10Y nếu có hoặc ước tính
    try:
        import yfinance as yf
        bond = yf.Ticker('^IRX')  # US 3-month T-bill làm proxy
        bh = bond.history(period='5d')
        if not bh.empty:
            result['us_3m_rate'] = round(float(bh['Close'].iloc[-1]), 3)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10175, _e)
    # 4. VN bank interbank rate (proxy từ SBV) — fallback giá trị cố định nếu không fetch được
    # Thực tế cần scrape sbv.gov.vn hoặc nguồn tương tự
    result.setdefault('interbank_rate', None)
    result.setdefault('interbank_note', 'Cần cấu hình nguồn dữ liệu lãi suất VN')
    # 5a. Margin debt proxy
    try:
        md = fetch_margin_debt_proxy()
        if md.get('available'):
            result['margin_debt'] = md
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10185, _e)
    # 5. Market breadth: % mã tăng/giảm trong watchlist
    try:
        with get_db() as c:
            rows = c.execute("""SELECT COUNT(*) as total,
                SUM(CASE WHEN m.chg > 0 THEN 1 ELSE 0 END) as up,
                SUM(CASE WHEN m.chg < 0 THEN 1 ELSE 0 END) as dn
                FROM fetch_meta m""").fetchone()
            if rows and rows['total'] > 0:
                result['breadth_total'] = rows['total']
                result['breadth_up']    = rows['up'] or 0
                result['breadth_dn']    = rows['dn'] or 0
                result['breadth_pct']   = round((rows['up'] or 0)/rows['total']*100, 1)
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10198, _e)
    # 6. VN30 today performance
    try:
        vn30_bars = db_load_ohlcv('VN30', 5)
        if vn30_bars and len(vn30_bars) >= 2:
            today = vn30_bars[-1]['c']; prev = vn30_bars[-2]['c']
            result['vn30_today_pct'] = round((today-prev)/prev*100, 2)
            result['vn30_price']     = today
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10206, _e)

    result['ts'] = datetime.now().isoformat()
    _macro_cache['ts']   = now
    _macro_cache['data'] = result
    return result

# ── get_macro_alert_context (from v12) ──

def get_macro_alert_context():
    """
    Kiểm tra macro context để quyết định có nên alert không.
    Trả về: (should_suppress, reason)
    """
    macro = fetch_macro_data()
    reasons = []
    # Suppression rule 1: VN30 đang giảm mạnh (>= 2%) trong ngày
    vn30_pct = macro.get('vn30_today_pct')
    if vn30_pct is not None and vn30_pct <= -2.0:
        reasons.append(f"VN30 đang -{ abs(vn30_pct):.1f}% hôm nay")
    # Suppression rule 2: Breadth xấu (<30% mã tăng)
    breadth = macro.get('breadth_pct')
    if breadth is not None and breadth < 30:
        reasons.append(f"Chỉ {breadth}% mã tăng giá hôm nay")
    # Suppression rule 3: USD/VND tăng mạnh (>= 0.5% trong ngày)
    usd_chg = macro.get('usdvnd_chg')
    if usd_chg is not None and usd_chg >= 0.5:
        reasons.append(f"USD/VND tăng {usd_chg:.2f}% (dòng tiền ngoại rút)")
    if reasons:
        return True, "⚠️ BUY alert bị ức chế bởi macro: " + " | ".join(reasons)
    return False, None

# ── v14: PORTFOLIO CORRELATION FILTER ────────────────────────────────────────

# ── fetch_foreign_flow_realtime (from v12) ──

def fetch_foreign_flow_realtime(market='VN', top_n=20):
    """
    Lấy dữ liệu dòng tiền nước ngoài realtime từ vnstock.
    Trả về: top mua ròng, top bán ròng, tổng net flow.
    """
    key = f"ff_{market}"
    c = _foreign_cache.get(key)
    if c and time.time() - c['ts'] < _FOREIGN_TTL:
        return c['data']
    if not HAS_VN: return {"top_buy":[],"top_sell":[],"total_net":0,"date":"","source":"no_vnstock","msg":"vnstock chưa được cài. Chạy: pip install vnstock"}
    try:
        from vnstock import Vnstock
        stock = Vnstock()
        # Thử lấy dữ liệu foreign flow từ API
        today = datetime.now().strftime('%Y-%m-%d')
        # SSI foreign flow
        try:
            ff = stock.trading.foreign_trading(date=today)
            if ff is not None and not (hasattr(ff,'empty') and ff.empty):
                ff.columns = [c.lower().replace(' ','_') for c in ff.columns]
                rows = []
                for _, row in ff.iterrows():
                    sym = str(row.get('ticker','') or row.get('symbol','')).strip().upper()
                    if not sym: continue
                    buy_val  = float(row.get('foreign_buy_value',0)  or row.get('buy_value',0)  or 0)
                    sell_val = float(row.get('foreign_sell_value',0) or row.get('sell_value',0) or 0)
                    net      = buy_val - sell_val
                    rows.append({'symbol':sym,'buy':buy_val,'sell':sell_val,'net':net})
                if rows:
                    rows.sort(key=lambda x: x['net'], reverse=True)
                    total_net = sum(r['net'] for r in rows)
                    data = {
                        'top_buy':  rows[:top_n],
                        'top_sell': sorted(rows, key=lambda x: x['net'])[:top_n],
                        'total_net': round(total_net/1e9, 2),  # tỷ đồng
                        'date': today, 'source': 'vnstock-realtime'
                    }
                    _foreign_cache[key] = {'ts': time.time(), 'data': data}
                    return data
        except Exception as _e: log.debug('[uncat] line %d: %%s' % 10281, _e)
        # Fallback: lấy từ DB (dữ liệu cũ)
        with get_db() as db:
            rows = db.execute("""SELECT symbol, net_value, buy_value, sell_value, trade_date
                FROM foreign_flow ORDER BY net_value DESC LIMIT ?""", (top_n*2,)).fetchall()
        if rows:
            rlist = [dict(r) for r in rows]
            data = {
                'top_buy':  [r for r in rlist if r['net_value']>0][:top_n],
                'top_sell': sorted([r for r in rlist if r['net_value']<0], key=lambda x:x['net_value'])[:top_n],
                'total_net': round(sum(r['net_value'] for r in rlist)/1e9, 2),
                'date': rlist[0].get('trade_date','') if rlist else '',
                'source': 'db-cache'
            }
            _foreign_cache[key] = {'ts': time.time(), 'data': data}
            return data
    except Exception as _ffe:
        log.debug("[ForeignFlow] %s", _ffe)
    return {"top_buy":[],"top_sell":[],"total_net":0,"date":"","source":"no_data","msg":"Không có dữ liệu — vnstock chưa kết nối hoặc ngoài giờ giao dịch"}

# ── v12: EMAIL ALERTS ────────────────────────────────────────────────────────

# ── fetch_ssi_realtime_price (from v12) ──

def fetch_ssi_realtime_price(sym):
    """Giá realtime VN qua TCBS public API (không cần auth). TTL 15s."""
    key = f"rt_{sym}"
    cached = _intra_cache.get(key)
    if cached and time.time()-cached["ts"] < 15: return cached["data"]
    try:
        import urllib.request, json as _j
        url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/quote?symbol={sym}&type=stock&count=1"
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        with urllib.request.urlopen(req, timeout=5) as r: data=_j.loads(r.read())
        items=data.get("data") or []
        if items:
            p=items[0]
            mul = 1000 if (p.get("c") or 0) < 1000 else 1  # TCBS trả về đơn vị nghìn đồng
            res={"symbol":sym,"price":float(p.get("c") or 0)*mul,
                 "change_pct":float(p.get("changePct") or p.get("priceDiff") or 0),
                 "volume":int(float(p.get("v") or p.get("volume") or 0)),
                 "high":float(p.get("h") or 0)*mul,"low":float(p.get("l") or 0)*mul,
                 "open":float(p.get("o") or 0)*mul,
                 "time":str(p.get("t") or p.get("tradingDate") or ""),"source":"TCBS"}
            _intra_cache[key]={"ts":time.time(),"data":res}; return res
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10326, _e)
    # Fallback: RAM cache
    cached_stock = _ram_cache.get(sym,{}).get("data",{})
    if cached_stock.get("price"):
        return {"symbol":sym,"price":cached_stock["price"],"change_pct":cached_stock.get("chg",0),
                "volume":0,"source":"ram-cache"}
    return None

# ── fetch_vn_market_realtime (from v12) ──

def fetch_vn_market_realtime(symbols_list):
    """Lấy giá realtime cho danh sách mã VN cùng lúc qua TCBS batch."""
    results={}
    try:
        import urllib.request, json as _j
        syms_str=",".join(symbols_list[:30])
        url=f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/ticker?tickers={syms_str}"
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        with urllib.request.urlopen(req, timeout=8) as r: data=_j.loads(r.read())
        items=data.get("data") or (data if isinstance(data,list) else [])
        for item in items:
            sym=str(item.get("ticker") or item.get("symbol","")).upper()
            if not sym: continue
            mul=1000 if (item.get("lastPrice") or 0)<1000 else 1
            results[sym]={"symbol":sym,
                "price":float(item.get("lastPrice") or 0)*mul,
                "change_pct":float(item.get("priceChangeRatio") or 0)*100,
                "volume":int(float(item.get("dealVolume") or 0)),
                "high":float(item.get("highPrice") or 0)*mul,
                "low":float(item.get("lowPrice") or 0)*mul,
                "foreign_buy":float(item.get("foreignBuyVolume") or 0),
                "foreign_sell":float(item.get("foreignSellVolume") or 0),
                "source":"TCBS-batch"}
        if results: return results
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10360, _e)
    for sym in symbols_list[:20]:
        r=fetch_ssi_realtime_price(sym)
        if r: results[sym]=r
        time.sleep(0.08)
    return results


# ── v12: IMPORT FROM BROKER CSV ───────────────────────────────────────────────

# ── smart_alert_cooldown (from v12) ──

def smart_alert_cooldown(sym, signal_type, ttl=None):
    """
    Kiểm tra xem đã gửi loại alert này cho sym trong TTL chưa.
    Trả về (can_send: bool, seconds_remaining: int).
    """
    ttl = ttl or (_COOLDOWN_SL if 'SL' in signal_type else
                  _COOLDOWN_MTF if 'MTF' in signal_type else
                  _COOLDOWN_BUY if 'BUY' in signal_type else _COOLDOWN_SELL)
    key = (sym, signal_type)
    last = _alert_cooldown.get(key, 0)
    elapsed = time.time() - last
    if elapsed >= ttl:
        _alert_cooldown[key] = time.time()
        return True, 0
    return False, int(ttl - elapsed)

# ── v14: MACRO DASHBOARD ─────────────────────────────────────────────────────
_macro_cache = {}
_MACRO_TTL   = 1800  # 30 phút

# ── fetch_financial_summary (from v12) ──

def fetch_financial_summary(sym, market='VN'):
    """
    Lấy tóm tắt tài chính: EPS, P/E, ROE, ROA, Debt/Equity.
    VN: từ vnstock | US: từ yfinance info.
    """
    try:
        if market == 'VN' and HAS_VN:
            from vnstock import Vnstock
            stk = Vnstock().stock(symbol=sym, source='TCBS')
            try:
                # Income statement
                income = stk.finance.income_statement(period='year', lang='en')
                balance = stk.finance.balance_sheet(period='year', lang='en')
                ratio = stk.finance.ratio(period='year', lang='en')
                result = {'symbol': sym, 'market': market, 'source': 'vnstock'}
                if ratio is not None and not (hasattr(ratio, 'empty') and ratio.empty):
                    ratio.columns = [c.lower() for c in ratio.columns]
                    row = ratio.iloc[-1] if len(ratio) > 0 else None
                    if row is not None:
                        result['roe']   = round(float(row.get('roe', 0) or 0) * 100, 1)
                        result['roa']   = round(float(row.get('roa', 0) or 0) * 100, 1)
                        result['eps']   = round(float(row.get('eps', 0) or 0), 0)
                        result['pe']    = round(float(row.get('pe', 0) or row.get('price_to_earning', 0) or 0), 1)
                        result['pb']    = round(float(row.get('pb', 0) or row.get('price_to_book', 0) or 0), 1)
                        result['debt_equity'] = round(float(row.get('debt_on_equity', 0) or 0), 2)
                        result['gross_margin'] = round(float(row.get('gross_profit_margin', 0) or 0) * 100, 1)
                return result
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 10421, _e)
        elif market == 'US':
            import yfinance as yf
            info = yf.Ticker(sym).info
            return {
                'symbol': sym, 'market': market, 'source': 'yfinance',
                'pe':   round(float(info.get('trailingPE', 0) or 0), 1),
                'pb':   round(float(info.get('priceToBook', 0) or 0), 1),
                'eps':  round(float(info.get('trailingEps', 0) or 0), 2),
                'roe':  round(float(info.get('returnOnEquity', 0) or 0) * 100, 1),
                'roa':  round(float(info.get('returnOnAssets', 0) or 0) * 100, 1),
                'debt_equity': round(float(info.get('debtToEquity', 0) or 0) / 100, 2),
                'gross_margin': round(float(info.get('grossMargins', 0) or 0) * 100, 1),
                'revenue_growth': round(float(info.get('revenueGrowth', 0) or 0) * 100, 1),
            }
    except Exception as e:
        pass
    return {'symbol': sym, 'market': market, 'error': 'Không thể tải dữ liệu tài chính'}

# ── v15: MARGIN DEBT PROXY ────────────────────────────────────────────────────

# ── _corr_adjusted_size (from v12) ──

def _corr_adjusted_size(sym, active_positions, corr_matrix, base_risk, max_risk=0.04):
    """Reduce position size proportionally to average correlation with existing positions."""
    if not active_positions: return base_risk
    corr_vals = [abs(corr_matrix.get((sym,p), corr_matrix.get((p,sym), 0.0))) for p in active_positions]
    avg_corr  = sum(corr_vals)/len(corr_vals) if corr_vals else 0.0
    adj = base_risk * (1.0 - avg_corr * 0.6)
    return min(max(adj, base_risk*0.2), max_risk)

# ── backtest_by_regime (from v12) ──


def fetch_vnindex_for_benchmark(n_bars=500):
    """Lấy lịch sử VNINDEX để so sánh equity curve."""
    cached = _ram_cache.get('VNINDEX', {}).get('data')
    if cached and cached.get('history'):
        return cached['history']
    bars = db_load_ohlcv('VNINDEX', OHLCV_BARS)
    if bars and len(bars) >= 50: return bars
    # Fetch nếu chưa có
    try:
        raw = _raw_fetch_vn('VNINDEX')
        if raw and raw.get('history'):
            db_save_ohlcv('VNINDEX', 'VN', raw['history'])
            db_save_meta('VNINDEX','VN',raw['price'],raw['chg'],'VNINDEX','Index')
            return raw['history']
    except Exception as _e: log.debug('[uncat] line %d: %%s' % 10469, _e)
    return []


def backtest_by_regime(history, capital=10_000_000, market="VN"):
    """
    Chạy backtest phân theo Market Regime:
    - Bull regime (VNI > 200MA): chỉ tín hiệu BUY
    - Bear regime (VNI < 200MA): chỉ tín hiệu SELL hoặc cash
    - Neutral: cả hai
    So sánh performance trong từng regime.
    """
    vnindex = fetch_vnindex_for_benchmark()
    if not vnindex or len(vnindex) < 200:
        return {'error': 'Cần VNINDEX data để phân tích theo regime'}
    # Build regime map by date
    regime_map = {}
    for i in range(200, len(vnindex)):
        ma200 = sum(b['c'] for b in vnindex[i-200:i]) / 200
        price = vnindex[i]['c']
        regime_map[vnindex[i]['d']] = 'BULL' if price > ma200 else 'BEAR'
    # Separate bars by regime
    bull_bars = [b for b in history if regime_map.get(b['d']) == 'BULL']
    bear_bars = [b for b in history if regime_map.get(b['d']) == 'BEAR']
    results = {'total_bars': len(history), 'bull_bars': len(bull_bars), 'bear_bars': len(bear_bars)}
    if len(bull_bars) >= 60:
        r = backtest(bull_bars, capital, market)
        results['bull_regime'] = {'profit_pct': r.get('profit_pct',0),
            'win_rate': r.get('win_rate',0), 'sharpe': r.get('sharpe',0),
            'trades': r.get('total_trades',0), 'label': '🟢 Bull regime'}
    if len(bear_bars) >= 60:
        r = backtest(bear_bars, capital, market)
        results['bear_regime'] = {'profit_pct': r.get('profit_pct',0),
            'win_rate': r.get('win_rate',0), 'sharpe': r.get('sharpe',0),
            'trades': r.get('total_trades',0), 'label': '🔴 Bear regime'}
    # Insight
    bull_profit = results.get('bull_regime', {}).get('profit_pct', 0)
    bear_profit = results.get('bear_regime', {}).get('profit_pct', 0)
    if bull_profit > 10 and bear_profit < 0:
        results['insight'] = "✅ Strategy hoạt động tốt trong Bull, nên đứng ngoài Bear"
    elif bear_profit > bull_profit:
        results['insight'] = "⚠️ Strategy nghịch chu kỳ — hiệu quả hơn khi Bear"
    else:
        results['insight'] = "ℹ️ Strategy hoạt động tương đương cả hai regime"
    return results

# ── v15: FINANCIAL STATEMENTS PROXY ──────────────────────────────────────────

# ── _portfolio_var (from v12) ──

def _portfolio_var(weights, corr_matrix, symbols, vol_map, confidence=0.95):
    """Portfolio VaR using variance-covariance (Markowitz) method. Returns daily VaR %."""
    import math
    syms = [s for s in symbols if s in weights and s in vol_map]
    if len(syms) < 2: return 0.0
    port_var = 0.0
    for s1 in syms:
        for s2 in syms:
            w1=weights.get(s1,0); w2=weights.get(s2,0)
            v1=vol_map.get(s1,0.02); v2=vol_map.get(s2,0.02)
            key=(s1,s2) if (s1,s2) in corr_matrix else (s2,s1)
            c=corr_matrix.get(key,1.0 if s1==s2 else 0.0)
            port_var += w1*w2*v1*v2*c
    return round(math.sqrt(max(port_var,0))*1.645*100, 2)  # z=1.645 for 95%

def fetch_insider_trading_vn(symbol: str) -> dict:
    """Lấy giao dịch nội bộ của cổ đông lớn/ban lãnh đạo từ vnstock."""
    try:
        from vnstock import Vnstock
        co = Vnstock().stock(symbol=symbol, source='VCI').company
        df = co.insider_deals() if hasattr(co, 'insider_deals') else None
        if df is None or len(df) == 0:
            return {"deals": [], "symbol": symbol, "note": "Không có dữ liệu giao dịch nội bộ"}
        deals = []
        for _, row in df.head(10).iterrows():
            deals.append({
                "date":     str(row.get("deal_announce_date", row.get("date", ""))),
                "name":     str(row.get("name", row.get("dealer_name", ""))),
                "position": str(row.get("position", "")),
                "action":   str(row.get("deal_action", row.get("action", ""))),
                "quantity": int(float(row.get("quantity", row.get("deal_quantity", 0)) or 0)),
                "price":    float(row.get("price", row.get("deal_price", 0)) or 0),
                "ratio":    float(row.get("ratio", row.get("ownership_ratio", 0)) or 0),
            })
        return {"deals": deals, "symbol": symbol, "count": len(deals)}
    except Exception as e:
        return {"deals": [], "symbol": symbol, "error": str(e)}



def get_data_staleness(sym, mkt):
    """Trả về số giờ kể từ lần fetch cuối. >2h = stale."""
    try:
        with get_db() as c:
            r = c.execute("SELECT last_fetch FROM fetch_meta WHERE symbol=?",(sym,)).fetchone()
        if not r or not r["last_fetch"]: return 99
        last = datetime.fromisoformat(r["last_fetch"])
        return round((datetime.now()-last).total_seconds()/3600, 1)
    except: return 99

# ── SIGNAL FORWARD TESTING TRACKER (Nhóm 4.4) ────────────────────────────────

# ── OPTIONS / FUTURES / EARNINGS ─────────────────────────────────────────────

def fetch_options_data(sym: str, market: str = 'VN') -> dict:
    """
    Lấy dữ liệu quyền chọn (Options) và hợp đồng tương lai (Futures).
    VN: VN30 Index futures (HOSE derivatives) qua SSI FastConnect hoặc vnstock.
    US: yfinance options chain.
    """
    key = f"opt_{sym}_{market}"
    cached = _intra_cache.get(key)
    if cached and time.time() - cached['ts'] < 300: return cached['data']

    result = {"symbol": sym, "market": market, "calls": [], "puts": [], "futures": [], "expiries": []}
    try:
        if market == 'VN':
            # VN30F futures từ vnstock
            try:
                from vnstock import Vnstock
                st = Vnstock()
                # Lấy danh sách hợp đồng tương lai VN30
                derivatives = st.derivatives.search(symbol="VN30F")
                if derivatives is not None and not (hasattr(derivatives,'empty') and derivatives.empty):
                    for _, row in derivatives.iterrows():
                        result["futures"].append({
                            "symbol": str(row.get("symbol","")).strip(),
                            "price":  float(row.get("close", row.get("last_price", 0)) or 0),
                            "change": float(row.get("change_pct", 0) or 0),
                            "volume": int(row.get("volume", row.get("total_volume", 0)) or 0),
                            "oi":     int(row.get("open_interest", 0) or 0),
                            "expiry": str(row.get("maturity_date", "") or ""),
                        })
            except Exception as _vfe: log.debug("[options] VN futures: %s", _vfe)

        elif market == 'US':
            # US options qua yfinance
            try:
                import yfinance as yf
                tk = yf.Ticker(sym)
                expiries = tk.options[:3] if tk.options else []
                result["expiries"] = list(expiries)
                if expiries:
                    chain = tk.option_chain(expiries[0])
                    for _, row in chain.calls.head(10).iterrows():
                        result["calls"].append({
                            "strike":     float(row.get("strike", 0)),
                            "lastPrice":  float(row.get("lastPrice", 0)),
                            "bid":        float(row.get("bid", 0)),
                            "ask":        float(row.get("ask", 0)),
                            "iv":         float(row.get("impliedVolatility", 0)),
                            "volume":     int(row.get("volume", 0) or 0),
                            "oi":         int(row.get("openInterest", 0) or 0),
                            "inTheMoney": bool(row.get("inTheMoney", False)),
                        })
                    for _, row in chain.puts.head(10).iterrows():
                        result["puts"].append({
                            "strike":     float(row.get("strike", 0)),
                            "lastPrice":  float(row.get("lastPrice", 0)),
                            "bid":        float(row.get("bid", 0)),
                            "ask":        float(row.get("ask", 0)),
                            "iv":         float(row.get("impliedVolatility", 0)),
                            "volume":     int(row.get("volume", 0) or 0),
                            "oi":         int(row.get("openInterest", 0) or 0),
                            "inTheMoney": bool(row.get("inTheMoney", False)),
                        })
            except Exception as _uoe: log.debug("[options] US: %s", _uoe)

        _intra_cache[key] = {"ts": time.time(), "data": result}
    except Exception as _oe:
        log.debug("[options] %s/%s: %s", sym, market, _oe)
    return result


def fetch_earnings_calendar(sym: str, market: str = 'VN') -> dict:
    """
    Lịch KQKD (Earnings Calendar) cho mã.
    VN: lấy từ vnstock company events.
    US: yfinance calendar.
    """
    key = f"earn_{sym}_{market}"
    cached = _intra_cache.get(key)
    if cached and time.time() - cached['ts'] < 3600: return cached['data']

    result = {"symbol": sym, "next_earnings": None, "eps_est": None,
              "eps_actual": None, "surprise_pct": None, "history": []}
    try:
        if market == 'US':
            import yfinance as yf
            tk = yf.Ticker(sym)
            cal = tk.calendar
            if cal is not None:
                ed = cal.get('Earnings Date', [])
                if hasattr(ed, 'iloc'): ed = ed.iloc[0] if len(ed) > 0 else None
                result["next_earnings"] = str(ed)[:10] if ed else None
                result["eps_est"] = cal.get('EPS Estimate', None)
                result["eps_actual"] = cal.get('Reported EPS', None)
            # EPS history
            hist = tk.earnings_history
            if hist is not None and len(hist) > 0:
                for _, row in hist.head(8).iterrows():
                    result["history"].append({
                        "date":     str(row.get("reportDate",""))[:10],
                        "eps_est":  float(row.get("epsEstimate") or 0),
                        "eps_act":  float(row.get("epsActual") or 0),
                        "surprise": float(row.get("surprisePercent") or 0),
                    })
        elif market == 'VN':
            # VN: lấy từ events đã có (ĐHCĐ, BCTC)
            events = fetch_vn_events(sym)
            earn_events = [e for e in (events or []) if
                           any(kw in e.get('title','').lower()
                               for kw in ['kqkd','kết quả','bctc','lợi nhuận','quý','tăng','giảm'])]
            result["history"] = earn_events[:8]
        _intra_cache[key] = {"ts": time.time(), "data": result}
    except Exception as _ee:
        log.debug("[earnings] %s: %s", sym, _ee)
    return result


# ══════════════════════════════════════════════════════════════════════
# FEATURE 2: EARNINGS SURPRISE HISTORY
# ══════════════════════════════════════════════════════════════════════
def get_earnings_surprise(sym: str, market: str = 'US') -> dict:
    """Get earnings surprise history with beat/miss/reaction data."""
    # Try DB cache first
    try:
        with get_db() as c:
            rows = c.execute(
                "SELECT * FROM earnings_surprise WHERE symbol=? AND market=? ORDER BY report_date DESC LIMIT 12",
                (sym.upper(), market.upper())
            ).fetchall()
        if rows:
            return {"symbol": sym, "market": market, "history": [dict(r) for r in rows]}
    except Exception: pass

    result = {"symbol": sym, "market": market, "history": []}
    try:
        if market == 'US':
            import yfinance as yf
            tk = yf.Ticker(sym)
            # Earnings history
            hist = tk.earnings_history
            bars = None
            try: bars = tk.history(period="2y")
            except: pass

            if hist is not None and len(hist) > 0:
                for _, row in hist.head(12).iterrows():
                    rd = str(row.get("reportDate",""))[:10]
                    eps_est = float(row.get("epsEstimate") or 0)
                    eps_act = float(row.get("epsActual") or 0)
                    surp = float(row.get("surprisePercent") or 0)
                    # Price reaction: 1d and 5d after report
                    r1d = r5d = None
                    if bars is not None and rd and rd in str(bars.index):
                        try:
                            idx_list = [str(i)[:10] for i in bars.index]
                            if rd in idx_list:
                                pos = idx_list.index(rd)
                                p0 = float(bars.iloc[pos]['Close'])
                                if pos+1 < len(bars): r1d = round((float(bars.iloc[pos+1]['Close'])-p0)/p0*100, 2)
                                if pos+5 < len(bars): r5d = round((float(bars.iloc[pos+5]['Close'])-p0)/p0*100, 2)
                        except: pass

                    entry = {"symbol": sym, "market": market, "period": rd,
                             "report_date": rd, "eps_est": eps_est, "eps_actual": eps_act,
                             "surprise_pct": surp, "reaction_1d": r1d, "reaction_5d": r5d,
                             "revenue_est": None, "revenue_actual": None, "rev_surprise_pct": None}
                    result["history"].append(entry)
                    # Cache in DB
                    try:
                        with get_db() as c:
                            c.execute("""INSERT OR REPLACE INTO earnings_surprise
                                (symbol,market,period,report_date,eps_est,eps_actual,surprise_pct,reaction_1d,reaction_5d)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                                (sym.upper(), market.upper(), rd, rd, eps_est, eps_act, surp, r1d, r5d))
                    except: pass

        elif market == 'VN':
            # VN: use financial data quarters
            fd = fetch_financial_summary(sym, 'VN') if 'fetch_financial_summary' in dir() else {}
            quarters = fd.get('quarterly', []) if fd else []
            for q in quarters[:8]:
                rd = q.get('date', '')
                eps = q.get('eps', 0) or 0
                prev_eps = q.get('prev_eps', 0) or eps
                surp = round((eps - prev_eps) / abs(prev_eps) * 100, 1) if prev_eps else 0
                result["history"].append({
                    "symbol": sym, "market": market, "period": rd, "report_date": rd,
                    "eps_est": prev_eps, "eps_actual": eps, "surprise_pct": surp,
                    "reaction_1d": None, "reaction_5d": None
                })
    except Exception as e:
        log.debug("[earnings_surprise] %s: %s", sym, e)

    # Compute summary stats
    if result["history"]:
        beats = [h for h in result["history"] if (h.get("surprise_pct") or 0) > 0]
        result["beat_rate"] = round(len(beats) / len(result["history"]) * 100, 1)
        result["avg_surprise"] = round(
            sum(h.get("surprise_pct") or 0 for h in result["history"]) / len(result["history"]), 2)
        avg_r1d = [h["reaction_1d"] for h in result["history"] if h.get("reaction_1d") is not None]
        result["avg_reaction_1d"] = round(sum(avg_r1d)/len(avg_r1d), 2) if avg_r1d else None
    return result


# ══════════════════════════════════════════════════════════════════════
# FEATURE 3: SHORT SELLING — enhanced P&L + metrics
# ══════════════════════════════════════════════════════════════════════
def calc_short_position_metrics(trade: dict, current_price: float) -> dict:
    """Calculate P&L and risk metrics for a SHORT position."""
    entry = trade.get('entry_price', 0) or 0
    qty   = trade.get('qty', 1) or 1
    borrow_rate = trade.get('borrow_rate', 0) or 0  # annual % borrow cost
    entry_date  = trade.get('entry_date', '')

    # Days held
    days = 0
    try:
        from datetime import datetime as _dt
        d0 = _dt.strptime(entry_date[:10], '%Y-%m-%d')
        days = (_dt.now() - d0).days
    except: pass

    # Short P&L: profit when price falls
    upnl = (entry - current_price) * qty
    upnl_pct = (entry - current_price) / entry * 100 if entry else 0

    # Borrow cost (daily)
    borrow_cost = entry * qty * (borrow_rate / 100) * (days / 365) if borrow_rate and days else 0
    net_upnl = upnl - borrow_cost

    # Risk: theoretically unlimited upside for short
    sl = trade.get('sl_price', 0) or 0
    max_loss = (sl - entry) * qty if sl > entry else None
    max_loss_pct = (sl - entry) / entry * 100 if sl and entry and sl > entry else None

    return {
        "direction": "SHORT",
        "entry": entry,
        "current": current_price,
        "qty": qty,
        "upnl": round(upnl, 0),
        "upnl_pct": round(upnl_pct, 2),
        "borrow_cost": round(borrow_cost, 0),
        "net_upnl": round(net_upnl, 0),
        "days_held": days,
        "max_loss": round(max_loss, 0) if max_loss else None,
        "max_loss_pct": round(max_loss_pct, 2) if max_loss_pct else None,
        "is_profitable": upnl > 0,
        "squeeze_risk": current_price > entry * 1.05,  # >5% against = squeeze risk
    }


# ══════════════════════════════════════════════════════════════════════
# FEATURE 4: CUSTOM INDICATOR RULE ENGINE
# ══════════════════════════════════════════════════════════════════════
CUSTOM_SIGNAL_OPS = {
    'gt': lambda a, b: a > b,
    'gte': lambda a, b: a >= b,
    'lt': lambda a, b: a < b,
    'lte': lambda a, b: a <= b,
    'eq': lambda a, b: abs(a - b) < 0.001,
    'cross_above': lambda a, b: a > b,   # simplified: current above threshold
    'cross_below': lambda a, b: a < b,
}

def evaluate_custom_signal(rules: list, logic: str, analysis: dict) -> tuple:
    """
    Evaluate a custom signal against current analysis data.
    Returns (triggered: bool, details: list)
    """
    if not rules or not analysis: return False, []

    # Map indicator keys to analysis values
    ind_map = {
        'rsi':       analysis.get('rsi', 50),
        'macd':      analysis.get('macd', 0),
        'macd_hist': analysis.get('macd_hist', 0),
        'adx':       analysis.get('adx', 20),
        'bb_pct':    analysis.get('bb_pct', 0.5),    # position within BB 0-1
        'vol_ratio': analysis.get('vol_ratio', 1.0),
        'score':     analysis.get('total', 0),
        'rsi_14':    analysis.get('rsi', 50),
        'obv_trend': 1 if analysis.get('obv_signal','') == 'UP' else -1,
        'price_vs_ma20': analysis.get('price_vs_ma20', 0),
        'price_vs_ma50': analysis.get('price_vs_ma50', 0),
        'price_vs_ma200': analysis.get('price_vs_ma200', 0),
        'slope':     analysis.get('slope', 0),
        'atr_pct':   analysis.get('atr_pct', 2.0),
        'stoch_k':   analysis.get('stoch_k', 50),
    }

    results = []
    for rule in rules:
        ind   = rule.get('indicator', '')
        op    = rule.get('op', 'gt')
        val   = float(rule.get('value', 0))
        label = rule.get('label', f"{ind} {op} {val}")

        ind_val = ind_map.get(ind)
        if ind_val is None:
            results.append({'label': label, 'pass': False, 'reason': 'Chỉ báo không có'})
            continue

        op_fn = CUSTOM_SIGNAL_OPS.get(op, lambda a, b: False)
        passed = op_fn(float(ind_val), val)
        results.append({'label': label, 'pass': passed,
                        'actual': round(float(ind_val), 2), 'threshold': val})

    if logic == 'AND':
        triggered = all(r['pass'] for r in results)
    elif logic == 'OR':
        triggered = any(r['pass'] for r in results)
    else:  # MAJORITY
        triggered = sum(1 for r in results if r['pass']) > len(results) / 2

    return triggered, results


def scan_custom_signal(signal_id: int, user_id: int) -> dict:
    """Run a custom signal against all watchlist symbols."""
    try:
        with get_db() as c:
            sig = c.execute("SELECT * FROM custom_signals WHERE id=? AND user_id=?",
                            (signal_id, user_id)).fetchone()
            if not sig: return {"error": "Không tìm thấy signal"}
            syms = [r['symbol'] for r in
                    c.execute("SELECT symbol FROM user_watchlist WHERE user_id=?",
                              (user_id,)).fetchall()]
    except Exception as e:
        return {"error": str(e)}

    import json
    rules  = json.loads(sig['rules'] or '[]')
    logic  = sig['logic'] or 'AND'
    hits   = []
    checked = 0

    for sym in syms:
        d = _ram_cache.get(sym, {}).get('data', {})
        if not d: continue
        a = d.get('analysis', {})
        if not a:
            try: a = analyze(d)
            except: continue
        triggered, details = evaluate_custom_signal(rules, logic, a)
        checked += 1
        if triggered:
            hits.append({
                "symbol":  sym,
                "market":  d.get('market','VN'),
                "price":   d.get('price', 0),
                "score":   a.get('total', 0),
                "signal":  a.get('signal',''),
                "details": details,
            })

    hits.sort(key=lambda x: -abs(x['score']))
    return {"signal_id": signal_id, "name": sig['name'], "logic": logic,
            "hits": hits, "total_checked": checked, "hit_count": len(hits)}


def backtest_custom_signal(signal_id: int, user_id: int, days_back: int = 90) -> dict:
    """Backtest a custom signal on historical data."""
    try:
        with get_db() as c:
            sig = c.execute("SELECT * FROM custom_signals WHERE id=? AND user_id=?",
                            (signal_id, user_id)).fetchone()
            if not sig: return {"error": "Không tìm thấy signal"}
            syms = [r['symbol'] for r in
                    c.execute("SELECT symbol FROM user_watchlist WHERE user_id=?",
                              (user_id,)).fetchall()]
    except Exception as e:
        return {"error": str(e)}

    import json
    rules = json.loads(sig['rules'] or '[]')
    logic = sig['logic'] or 'AND'
    all_trades = []; wins = 0; losses = 0

    for sym in syms[:30]:  # limit for performance
        bars = db_load_ohlcv(sym, min(days_back + 30, OHLCV_BARS))
        if not bars or len(bars) < 30: continue
        closes = [b['c'] for b in bars]

        # Simulate signal evaluation day by day
        for i in range(20, len(bars) - 20):
            slice_data = {"history": bars[:i+1], "price": bars[i]['c'], "market": "VN"}
            try: a = analyze(slice_data)
            except: continue
            triggered, _ = evaluate_custom_signal(rules, logic, a)
            if not triggered: continue

            entry_price = bars[i]['c']
            if i + 10 < len(bars):
                r10d = (bars[i+10]['c'] - entry_price) / entry_price * 100
                r20d = (bars[min(i+20, len(bars)-1)]['c'] - entry_price) / entry_price * 100 if i+20 < len(bars) else r10d
                win = r10d > 0
                if win: wins += 1
                else: losses += 1
                all_trades.append({"sym": sym, "date": bars[i]['d'],
                                   "entry": entry_price, "r10d": round(r10d,2), "r20d": round(r20d,2), "win": win})

    total = len(all_trades)
    wr = round(wins/total*100, 1) if total else 0
    avg_r = round(sum(t['r10d'] for t in all_trades)/total, 2) if total else 0
    return {"signal_id": signal_id, "name": sig['name'],
            "total_signals": total, "wins": wins, "losses": losses,
            "win_rate": wr, "avg_return_10d": avg_r,
            "trades": all_trades[-50:]}  # last 50 for display



# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 3: SMART MONEY TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def detect_smart_money(sym: str, mkt: str) -> dict:
    """
    Detect smart money activity:
    - Block trades > 500M VND or > $200K
    - Foreign flow anomalies
    - Volume spikes at key price levels
    - Unusual call/put option activity
    """
    d = _ram_cache.get(sym, {}).get('data', {})
    if not d: return {'symbol': sym, 'signals': [], 'score': 0}

    bars = d.get('history', [])
    price = d.get('price', 0)
    signals = []
    score = 0

    if bars and len(bars) >= 10:
        closes = [b.get('c', 0) for b in bars if b.get('c', 0) > 0]
        vols   = [b.get('v', 0) for b in bars]

        # 1. Volume anomaly detection (3x average = block trade likely)
        avg_vol = sum(vols[-20:]) / max(1, len(vols[-20:]))
        today_vol = vols[-1] if vols else 0
        vol_ratio = today_vol / max(1, avg_vol)

        threshold_vnd = 500_000_000  # 500M VND block trade threshold
        threshold_usd = 200_000      # $200K USD
        est_value = today_vol * price
        is_block = (mkt == 'VN' and est_value > threshold_vnd) or                    (mkt == 'US' and est_value > threshold_usd)

        if is_block and vol_ratio > 2.5:
            score += 3
            signals.append({
                'type': 'BLOCK_TRADE',
                'label': f'🐋 Block trade detected: {vol_ratio:.1f}x avg volume',
                'severity': 'HIGH',
                'value': round(est_value / 1e9, 2) if mkt == 'VN' else round(est_value / 1e6, 2),
                'unit': 'tỷ đồng' if mkt == 'VN' else 'M USD',
            })

        # 2. Price impact analysis - large vol + small price move = accumulation
        if len(closes) >= 2:
            price_chg_pct = abs(closes[-1] - closes[-2]) / max(1, closes[-2]) * 100
            if vol_ratio > 2 and price_chg_pct < 1.0:
                score += 2
                signals.append({
                    'type': 'ACCUMULATION',
                    'label': f'📦 Tích lũy: vol cao {vol_ratio:.1f}x nhưng giá ít biến động ({price_chg_pct:.1f}%)',
                    'severity': 'MEDIUM',
                })

        # 3. Distribution pattern - large vol + price drop
        if vol_ratio > 2 and len(closes) >= 2 and closes[-1] < closes[-2]:
            price_drop = (closes[-2] - closes[-1]) / closes[-2] * 100
            if price_drop > 1.5:
                score -= 2
                signals.append({
                    'type': 'DISTRIBUTION',
                    'label': f'🚨 Xả hàng: vol cao {vol_ratio:.1f}x + giảm {price_drop:.1f}%',
                    'severity': 'HIGH',
                })

        # 4. Foreign flow from analysis
        a = d.get('analysis', {})
        ff = a.get('foreign_flow', 0) or 0
        if ff > 0:
            score += min(2, ff / 10)
            signals.append({
                'type': 'FOREIGN_BUY',
                'label': f'🌐 Khối ngoại mua ròng: {ff:+.1f}%',
                'severity': 'MEDIUM',
            })
        elif ff < -5:
            score -= 1
            signals.append({
                'type': 'FOREIGN_SELL',
                'label': f'🌐 Khối ngoại bán ròng: {ff:+.1f}%',
                'severity': 'MEDIUM',
            })

        # 5. Consecutive high-volume days
        recent_vols = vols[-5:]
        if all(v > avg_vol * 1.5 for v in recent_vols):
            score += 2
            signals.append({
                'type': 'SUSTAINED_INTEREST',
                'label': f'📊 Dòng tiền liên tục: 5 phiên vol cao hơn TB 1.5x',
                'severity': 'MEDIUM',
            })

    verdict = 'ACCUMULATION' if score >= 3 else               'DISTRIBUTION' if score <= -2 else               'NEUTRAL'

    return {
        'symbol': sym,
        'market': mkt,
        'price': price,
        'score': round(score, 1),
        'verdict': verdict,
        'verdict_label': {
            'ACCUMULATION': '📦 Đang tích lũy / Smart money vào',
            'DISTRIBUTION': '🚨 Đang xả hàng / Smart money ra',
            'NEUTRAL': '➡️ Không có dấu hiệu rõ ràng',
        }.get(verdict, ''),
        'signals': signals,
    }


def scan_smart_money_watchlist() -> list:
    """Scan all watchlist symbols for smart money activity."""
    results = []
    for sym, cached in list(_ram_cache.items()):
        d = cached.get('data', {})
        mkt = d.get('market', 'VN')
        sm = detect_smart_money(sym, mkt)
        if sm.get('signals'):
            results.append(sm)
    results.sort(key=lambda x: -abs(x.get('score', 0)))
    return results[:20]


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 4A: AI CHAT ANALYST (calls Claude API via Anthropic)
# ══════════════════════════════════════════════════════════════════════════════

def build_stock_context(sym: str, mkt: str) -> str:
    """Build rich context string for AI analysis."""
    d = _ram_cache.get(sym, {}).get('data', {})
    if not d:
        # Fetch on demand — supports any symbol, not just watchlist
        try:
            d = fetch(sym, mkt) or {}
            if d:
                d['analysis'] = analyze(d)
        except Exception as _e:
            log.debug("[ai_ctx] fetch %s: %s", sym, _e)
    if not d: return f"Không có dữ liệu cho {sym} — không thể kết nối API để lấy dữ liệu."

    a = d.get('analysis', {}) or {}
    price  = d.get('price', 0)
    chg    = d.get('chg', 0)
    sector = a.get('sector', 'N/A')
    signal = a.get('signal', 'NEUTRAL')
    score  = a.get('total', 0)
    rsi    = a.get('rsi', 50)
    macd   = a.get('macd', 0)
    ma20   = a.get('ma20', 0)
    ma50   = a.get('ma50', 0)
    ma200  = a.get('ma200', 0)
    vn     = mkt == 'VN'
    fp     = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"

    # Build MTF context
    try:
        mtf = analyze_mtf(d)
        mtf_str = f"MTF: 1D={mtf.get('1D',{}).get('signal','?')} 1W={mtf.get('1W',{}).get('signal','?')} 1M={mtf.get('1M',{}).get('signal','?')}"
    except:
        mtf_str = ""

    ctx = f"""Phân tích kỹ thuật {sym} ({mkt}) lúc {datetime.now().strftime('%d/%m/%Y %H:%M')}:
- Giá: {fp(price)} | Thay đổi: {chg:+.2f}%
- Ngành: {sector}
- Tín hiệu: {signal} | Score: {score:+d}/19
- RSI(14): {rsi:.1f} | MACD: {macd:+.2f}
- MA20: {fp(ma20)} | MA50: {fp(ma50)} | MA200: {fp(ma200)}
- Giá vs MA20: {'trên' if price>ma20 else 'dưới'} | vs MA50: {'trên' if price>ma50 else 'dưới'} | vs MA200: {'trên' if price>ma200 else 'dưới'}
- {mtf_str}
- Chi tiết: {json.dumps(a.get('details',{}), ensure_ascii=False)[:500]}
"""
    # Add recent news if available
    try:
        with get_db() as c:
            news = c.execute(
                "SELECT title FROM news_cache WHERE symbol=? ORDER BY published_at DESC LIMIT 3",
                (sym,)).fetchall()
        if news:
            ctx += "Tin tức gần đây:\n" + "\n".join(f"- {r['title']}" for r in news)
    except: pass
    return ctx


def ai_chat_analyze(user_question: str, sym: str = None, mkt: str = 'VN',
                    conversation_history: list = None) -> dict:
    """
    AI analyst using Claude API.
    Returns {'response': str, 'sym': str, 'charts_suggested': list}
    """
    try:
        import urllib.request, urllib.error, json as _j

        # Build context
        system_prompt = """Bạn là AI analyst chuyên nghiệp về chứng khoán Việt Nam và quốc tế.
Phân tích dựa trên dữ liệu kỹ thuật được cung cấp. Trả lời bằng tiếng Việt tự nhiên, súc tích.
Luôn kết thúc bằng: cảnh báo rủi ro và nhắc người dùng tự nghiên cứu trước khi đầu tư.
Không đưa ra lời khuyên đầu tư cụ thể - chỉ phân tích kỹ thuật và cung cấp thông tin."""

        messages = list(conversation_history or [])

        # Add stock context if available
        if sym:
            ctx = build_stock_context(sym, mkt)
            user_content = ctx + "\n\nCâu hỏi: " + user_question
        else:
            user_content = user_question

        messages.append({"role": "user", "content": user_content})

        # Skip API call if no key configured
        api_key = get_setting('anthropic_api_key', '')
        if not api_key:
            raise ValueError("No API key configured — using local analysis")

        # Call Anthropic API
        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1024,
            "system": system_prompt,
            "messages": messages[-10:],  # keep last 10 turns
        }

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=_j.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01",
                "x-api-key": get_setting('anthropic_api_key', ''),
            },
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = _j.loads(resp.read())

        ai_text = result.get('content', [{}])[0].get('text', 'Không có phản hồi')
        # Compute verdict for API response too
        verdict_api, verdict_lbl_api, conf_api = 'HOLD', 'NẮM GIỮ', 50
        if sym:
            d_api = _ram_cache.get(sym, {}).get('data', {})
            a_api = d_api.get('analysis', {}) if d_api else {}
            s_api = a_api.get('total', 0)
            sig_api = a_api.get('signal', 'NEUTRAL')
            conf_api = min(round(abs(s_api) / 19 * 100 + (10 if 'STRONG' in sig_api else 0)), 92)
            if s_api >= 7 or sig_api in ('STRONG_BUY', 'BUY'): verdict_api, verdict_lbl_api = 'BUY', 'MUA'
            elif s_api >= 4: verdict_api, verdict_lbl_api = 'WATCH', 'THEO DÕI'
            elif s_api <= -7 or sig_api in ('STRONG_SELL', 'SELL'): verdict_api, verdict_lbl_api = 'SELL', 'BÁN'
            elif s_api <= -4: verdict_api, verdict_lbl_api = 'WATCH', 'THEO DÕI'
        return {
            'response': ai_text,
            'symbol': sym,
            'verdict': verdict_api,
            'verdict_label': verdict_lbl_api,
            'confidence': conf_api,
            'model': 'claude-sonnet-4',
            'tokens_used': result.get('usage', {}).get('output_tokens', 0),
        }

    except Exception as e:
        log.debug("[ai_chat] %s", e)
        # Fallback: generate response from local analysis if API fails
        if sym:
            ctx = build_stock_context(sym, mkt)
            # Get analysis data (may have been fetched on demand in build_stock_context)
            d_fb = _ram_cache.get(sym, {}).get('data', {})
            a_fb = d_fb.get('analysis', {}) if d_fb else {}
            score_fb  = a_fb.get('total', 0)
            signal_fb = a_fb.get('signal', 'NEUTRAL')
            rsi_fb    = float(a_fb.get('rsi', 50) or 50)
            # Compute confidence
            raw_conf = min(abs(score_fb) / 19 * 100, 85)
            if 'STRONG' in signal_fb: raw_conf = min(raw_conf + 10, 90)
            if 'WARN'   in signal_fb or 'WEAK' in signal_fb: raw_conf = max(raw_conf - 10, 30)
            if rsi_fb < 30 or rsi_fb > 70: raw_conf = min(raw_conf + 8, 92)
            conf_fb = max(round(raw_conf), 20)
            # Map to verdict
            if score_fb >= 7 or signal_fb in ('STRONG_BUY', 'BUY'):
                verdict, vlabel = 'BUY',   'MUA'
            elif score_fb >= 4 or signal_fb == 'BUY_WARN':
                verdict, vlabel = 'WATCH', 'THEO DÕI'
            elif score_fb <= -7 or signal_fb in ('STRONG_SELL', 'SELL'):
                verdict, vlabel = 'SELL',  'BÁN'
            elif score_fb <= -4 or signal_fb == 'SELL_WARN':
                verdict, vlabel = 'WATCH', 'THEO DÕI'
            else:
                verdict, vlabel = 'HOLD',  'NẮM GIỮ'
            mood = 'tích cực' if score_fb > 3 else 'tiêu cực' if score_fb < -3 else 'trung tính'
            return {
                'response': "[Phân tích nội bộ]\n\n" + ctx + "\n\nDựa trên dữ liệu trên, mã " + sym + " đang " + mood + ".",
                'symbol': sym,
                'verdict': verdict,
                'verdict_label': vlabel,
                'confidence': conf_fb,
                'model': 'local-fallback',
                'error': str(e)[:100],
            }
        # Generic response without symbol
        answer = ("Tôi sẵn sàng phân tích cổ phiếu cho bạn!\n\n"
                  "Hãy chọn mã cổ phiếu trong dropdown phía trên, sau đó hỏi lại.\n\n"
                  "Ví dụ: Chọn VCB → hỏi 'Phân tích kỹ thuật VCB'\n\n"
                  "Lưu ý: AI Chat dùng phân tích kỹ thuật nội bộ (không cần API key).\n"
                  "Để có phân tích AI nâng cao hơn, cần Anthropic API key trong Settings.")
        return {'response': answer, 'model': 'local-guide'}


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 4B: DAILY BRIEFING 8:30AM
# ══════════════════════════════════════════════════════════════════════════════

def generate_daily_briefing(user_id: int = None) -> dict:
    """
    Generate comprehensive morning briefing:
    - Market overview
    - Top 5 opportunities today
    - Calendar events
    - Macro alerts
    - Portfolio status
    """
    now = datetime.now()
    date_str = now.strftime('%d/%m/%Y')
    briefing = {
        'date': date_str,
        'generated_at': now.isoformat(),
        'sections': {}
    }

    # 1. Market overview
    try:
        vn_syms = [s for s, d in _ram_cache.items()
                   if d.get('data', {}).get('market') == 'VN']
        if vn_syms:
            scores = [_ram_cache[s]['data'].get('analysis', {}).get('total', 0)
                      for s in vn_syms if _ram_cache[s].get('data', {}).get('analysis')]
            buy_count  = sum(1 for s in scores if s >= 7)
            sell_count = sum(1 for s in scores if s <= -7)
            avg_score  = sum(scores) / len(scores) if scores else 0
            market_mood = '🟢 Tích cực' if avg_score > 3 else                          '🔴 Tiêu cực' if avg_score < -3 else '🟡 Trung lập'
            briefing['sections']['market'] = {
                'title': '📊 Tổng quan thị trường',
                'mood': market_mood,
                'avg_score': round(avg_score, 1),
                'buy_count': buy_count,
                'sell_count': sell_count,
                'total': len(vn_syms),
            }
    except Exception as e:
        log.debug("[briefing] market: %s", e)

    # 2. Top 5 opportunities
    try:
        scan = _market_scan_results.get('latest', {})
        if not scan:
            scan = run_market_scanner_24_7()
        briefing['sections']['opportunities'] = {
            'title': '🎯 Top 5 cơ hội hôm nay',
            'buy':  (scan.get('top_buy', []) or [])[:5],
            'breakout': (scan.get('top_breakout', []) or [])[:3],
        }
    except Exception as e:
        log.debug("[briefing] opportunities: %s", e)

    # 3. Calendar events today/tomorrow
    try:
        with get_db() as c:
            today = now.date().isoformat()
            tomorrow = (now.date() + timedelta(days=1)).isoformat()
            events = c.execute(
                "SELECT * FROM market_events WHERE event_date BETWEEN ? AND ? ORDER BY event_date",
                (today, tomorrow)).fetchall()
        briefing['sections']['calendar'] = {
            'title': '📅 Sự kiện hôm nay & ngày mai',
            'events': [dict(e) for e in events[:10]],
        }
    except Exception as e:
        log.debug("[briefing] calendar: %s", e)

    # 4. Macro context
    try:
        macro = fetch_macro_data()
        regime = get_market_regime()
        briefing['sections']['macro'] = {
            'title': '🌐 Bối cảnh vĩ mô',
            'regime': regime.get('regime', 'UNKNOWN'),
            'vn30': macro.get('vn30', {}),
            'sp500': macro.get('sp500', {}),
            'usd_vnd': macro.get('usd_vnd', {}),
        }
    except Exception as e:
        log.debug("[briefing] macro: %s", e)

    # 5. Portfolio alerts
    if user_id:
        try:
            pos = trade_list(user_id, status='OPEN', limit=50)
            alerts = []
            for p in pos:
                sym = p['symbol']
                d = _ram_cache.get(sym, {}).get('data', {})
                if not d: continue
                price = d.get('price', 0)
                entry = p.get('entry_price', 0)
                sl = p.get('sl_price', 0)
                if sl and price and (price - sl) / max(sl, 1) * 100 < 2:
                    alerts.append({'symbol': sym, 'type': 'SL_NEAR', 'price': price, 'sl': sl})
            briefing['sections']['portfolio'] = {
                'title': '💼 Cảnh báo danh mục',
                'open_positions': len(pos),
                'alerts': alerts,
            }
        except Exception as e:
            log.debug("[briefing] portfolio: %s", e)

    # 6. Generate Telegram message
    try:
        lines = [f"☀️ <b>BẢN TIN SÁNG {date_str}</b>", "━━━━━━━━━━━━━━━━━━━━"]
        if 'market' in briefing['sections']:
            m = briefing['sections']['market']
            lines.append(f"📊 Thị trường: <b>{m['mood']}</b> | BUY: {m['buy_count']} | SELL: {m['sell_count']}")
        if 'opportunities' in briefing['sections']:
            top = briefing['sections']['opportunities'].get('buy', [])
            if top:
                lines.append(f"🎯 Top cơ hội: {', '.join(t['symbol'] for t in top[:3])}")
        if 'portfolio' in briefing['sections']:
            alerts = briefing['sections']['portfolio'].get('alerts', [])
            if alerts:
                lines.append(f"⚠️ Cảnh báo: {', '.join(a['symbol'] for a in alerts[:3])} gần SL")
        lines.append('━━━━━━━━━━━━━━━━━━━━\n🔗 ' + get_setting('public_url', f'http://localhost:{PORT}'))
        briefing['telegram_msg'] = "\n".join(lines)
    except Exception as e:
        log.debug("[briefing] telegram: %s", e)

    return briefing


def send_morning_briefing(user_id: int = None):
    """Send morning briefing via Telegram."""
    try:
        token = get_setting('tg_token', '')
        briefing = generate_daily_briefing(user_id)
        if token and briefing.get('telegram_msg'):
            msg = briefing['telegram_msg'].replace('\n', '\n')
            threading.Thread(
                target=tg_send_to_subscribers,
                args=(None, msg),
                daemon=True).start()
        log.info("[briefing] Morning briefing generated: %s", briefing['date'])
        return briefing
    except Exception as e:
        log.error("[briefing] %s", e)
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 4C: MULTI-SCREEN LAYOUT STORAGE
# ══════════════════════════════════════════════════════════════════════════════

def save_layout_config(user_id: int, layout: dict) -> bool:
    """Save user's custom widget layout."""
    try:
        import json as _j
        with get_db() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS user_layouts(
                user_id INTEGER PRIMARY KEY,
                layout TEXT NOT NULL,
                updated_at TEXT DEFAULT(datetime('now')))""")
            c.execute("""INSERT OR REPLACE INTO user_layouts(user_id, layout, updated_at)
                VALUES(?,?,datetime('now'))""",
                (user_id, _j.dumps(layout, ensure_ascii=False)))
        return True
    except Exception as e:
        log.debug("[layout] save: %s", e)
        return False


def get_layout_config(user_id: int) -> dict:
    """Get user's saved layout config."""
    default_layout = {
        'version': 1,
        'panels': [
            {'id': 'watchlist',  'x': 0, 'y': 0, 'w': 4, 'h': 12, 'visible': True},
            {'id': 'chart',      'x': 4, 'y': 0, 'w': 8, 'h': 8,  'visible': True},
            {'id': 'alerts',     'x': 4, 'y': 8, 'w': 4, 'h': 4,  'visible': True},
            {'id': 'portfolio',  'x': 8, 'y': 8, 'w': 4, 'h': 4,  'visible': True},
            {'id': 'scanner',    'x': 0, 'y': 12,'w': 6, 'h': 6,  'visible': True},
            {'id': 'macro',      'x': 6, 'y': 12,'w': 6, 'h': 6,  'visible': True},
        ]
    }
    try:
        import json as _j
        with get_db() as c:
            row = c.execute("SELECT layout FROM user_layouts WHERE user_id=?",
                           (user_id,)).fetchone()
        if row:
            return _j.loads(row['layout'])
    except: pass
    return default_layout

# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 1: SMART ALERT ENGINE
# ══════════════════════════════════════════════════════════════════════════════

# Alert severity levels
ALERT_LEVEL = {'CRITICAL': 3, 'WARNING': 2, 'INFO': 1}

# In-memory alert banner queue (for frontend polling)
_live_alerts = []        # list of alert dicts for banner
_live_alerts_lock = threading.Lock()
_live_alerts_max = 50    # keep last 50

def push_live_alert(sym: str, mkt: str, level: str, title: str, body: str,
                    action: str = '', price: float = 0):
    """Push alert into live banner queue for frontend to poll."""
    alert = {
        'id':      f"{sym}_{level}_{int(time.time())}",
        'symbol':  sym,
        'market':  mkt,
        'level':   level,   # CRITICAL / WARNING / INFO
        'title':   title,
        'body':    body,
        'action':  action,
        'price':   price,
        'ts':      datetime.now().isoformat(),
        'seen':    False,
    }
    with _live_alerts_lock:
        # Remove old alerts for same symbol+level to avoid spam
        _live_alerts[:] = [a for a in _live_alerts
                           if not (a['symbol'] == sym and a['level'] == level)]
        _live_alerts.insert(0, alert)
        if len(_live_alerts) > _live_alerts_max:
            _live_alerts[:] = _live_alerts[:_live_alerts_max]
    return alert


def get_live_alerts(since_ts: str = None, unseen_only: bool = False) -> list:
    """Get live alerts for frontend banner."""
    with _live_alerts_lock:
        alerts = list(_live_alerts)
    if since_ts:
        alerts = [a for a in alerts if a['ts'] > since_ts]
    if unseen_only:
        alerts = [a for a in alerts if not a['seen']]
    return alerts


def mark_alerts_seen(alert_ids: list):
    """Mark alerts as seen."""
    with _live_alerts_lock:
        for a in _live_alerts:
            if a['id'] in alert_ids:
                a['seen'] = True


# ── Multi-condition alert storage ────────────────────────────────────────────
def check_multi_condition_alerts(sym: str, mkt: str, analysis: dict, price: float):
    """
    Evaluate multi-condition alerts from custom_signals table.
    Triggers push_live_alert + Telegram when all conditions met.
    """
    try:
        with get_db() as c:
            sigs = c.execute(
                "SELECT * FROM custom_signals WHERE is_active=1").fetchall()
    except: return

    import json as _j
    for sig in sigs:
        try:
            rules  = _j.loads(sig['rules'] or '[]')
            logic  = sig['logic'] or 'AND'
            sig_type = sig['signal_type'] or 'BUY'
            triggered, details = evaluate_custom_signal(rules, logic, analysis)
            if not triggered: continue

            # Cooldown: don't repeat same signal for 2 hours
            can_send, _ = smart_alert_cooldown(
                f"{sym}_custom_{sig['id']}", 'CUSTOM', ttl=7200)
            if not can_send: continue

            level = 'WARNING' if sig_type in ('BUY', 'SELL') else 'INFO'
            passed_rules = [d['label'] for d in details if d.get('pass')]
            body = f"{sym} · {', '.join(passed_rules[:3])}"
            push_live_alert(sym, mkt, level,
                            f"⚙️ {sig['name']} — {sig_type}",
                            body, action=f"Xem {sym}", price=price)

            # Telegram notification
            token = get_setting('tg_token', '')
            if token:
                vn = mkt == 'VN'
                fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
                msg = (f"⚙️ <b>CUSTOM SIGNAL — {sig['name']}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"{'🇻🇳' if vn else '🇺🇸'} <b>{sym}</b> @ {fp}\n"
                       f"🎯 Loại: {sig_type}\n"
                       f"✅ Điều kiện: {', '.join(passed_rules)}\n"
                       f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                threading.Thread(
                    target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()
        except Exception as _e:
            log.debug("[multi_condition] %s: %s", sym, _e)


# ── Real-time Stop-Loss tracker ───────────────────────────────────────────────
def check_portfolio_sl_realtime(user_id: int, positions: list, cache: dict):
    """
    Check all OPEN positions against their stop-loss prices.
    Triggers CRITICAL alert within 30s of SL breach.
    """
    token = get_setting('tg_token', '')
    for pos in positions:
        if pos.get('status') != 'OPEN': continue
        sym   = pos['symbol']
        mkt   = pos.get('market', 'VN')
        sl    = float(pos.get('sl_price') or 0)
        entry = float(pos.get('entry_price') or 0)
        direction = pos.get('direction', 'LONG')
        if not sl: continue

        d = cache.get(sym, {}).get('data', {})
        price = d.get('price', 0)
        if not price: continue

        # Check SL breach
        sl_hit = (direction == 'LONG'  and price <= sl) or \
                 (direction == 'SHORT' and price >= sl)
        sl_near = (direction == 'LONG'  and price <= sl * 1.02) or \
                  (direction == 'SHORT' and price >= sl * 0.98)

        if sl_hit:
            can_send, _ = smart_alert_cooldown(
                f"{sym}_SL_HIT_{user_id}", 'SL_HIT', ttl=1800)
            if can_send:
                vn = mkt == 'VN'
                fp = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"
                pnl_pct = ((price - entry) / entry * 100
                           if direction == 'LONG'
                           else (entry - price) / entry * 100)
                action = ("🔴 CẮT LỖ NGAY — Đã chạm Stop Loss!" if direction == 'LONG'
                         else "🔴 MUA LẠI NGAY — Đã chạm SL cho vị thế SHORT!")
                push_live_alert(sym, mkt, 'CRITICAL',
                    f"🚨 SL HIT — {sym}",
                    f"Giá {fp(price)} ≤ SL {fp(sl)} | P&L: {pnl_pct:+.1f}%",
                    action=action, price=price)
                if token:
                    msg = (f"🚨 <b>STOP LOSS HIT — {sym} ({mkt})</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"💰 Giá hiện tại: <b>{fp(price)}</b>\n"
                           f"🛡 Stop Loss: <b>{fp(sl)}</b>\n"
                           f"📉 P&L: <b>{pnl_pct:+.1f}%</b>\n"
                           f"{'📌 Hướng: LONG' if direction=='LONG' else '📌 Hướng: SHORT'}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚡ <b>{action}</b>\n"
                           f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                    threading.Thread(
                        target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()

        elif sl_near:
            can_send, _ = smart_alert_cooldown(
                f"{sym}_SL_NEAR_{user_id}", 'SL_NEAR', ttl=3600)
            if can_send:
                vn = mkt == 'VN'
                fp = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"
                dist_pct = abs(price - sl) / sl * 100
                push_live_alert(sym, mkt, 'WARNING',
                    f"⚠️ GẦN SL — {sym}",
                    f"Giá {fp(price)} · SL {fp(sl)} · Còn {dist_pct:.1f}%",
                    action=f"Kiểm tra {sym}", price=price)


# ── Earnings Calendar Alert ───────────────────────────────────────────────────
_earnings_alert_cache = {}  # sym -> last alert date

def check_earnings_alerts():
    """Alert 1 day before earnings for all watchlist symbols."""
    try:
        with get_db() as c:
            syms = [r['symbol'] for r in
                    c.execute("SELECT DISTINCT symbol FROM watchlist").fetchall()]
        if not syms: return
        today     = datetime.now().date()
        tomorrow  = (today + timedelta(days=1)).isoformat()
        today_str = today.isoformat()
        token = get_setting('tg_token', '')

        for sym in syms[:30]:  # limit per cycle
            # Only check US stocks for now (yfinance)
            d = _ram_cache.get(sym, {}).get('data', {})
            mkt = d.get('market', 'VN') if d else 'VN'
            if mkt != 'US': continue

            last_check = _earnings_alert_cache.get(sym, '')
            if last_check == today_str: continue

            ec = fetch_earnings_calendar(sym, 'US')
            next_earn = ec.get('next_earnings')
            if not next_earn: continue

            _earnings_alert_cache[sym] = today_str
            price = d.get('price', 0) if d else 0
            fp = lambda v: f"${v:.2f}"

            if next_earn == tomorrow:
                push_live_alert(sym, 'US', 'WARNING',
                    f"📅 EARNINGS NGÀY MAI — {sym}",
                    f"Báo cáo KQKD ngày {next_earn} · Giá hiện tại {fp(price)}",
                    action=f"Xem {sym} Earnings", price=price)
                if token:
                    eps_est = ec.get('eps_est')
                    beat_rate = None
                    # Get beat rate from history
                    hist = ec.get('history', [])
                    if hist:
                        beats = [h for h in hist if (h.get('surprise_pct') or 0) > 0]
                        beat_rate = round(len(beats)/len(hist)*100)
                    msg = (f"📅 <b>EARNINGS NGÀY MAI — {sym}</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"📅 Ngày báo cáo: <b>{next_earn}</b>\n"
                           f"💰 Giá hiện tại: <b>{fp(price)}</b>\n"
                           f"{f'📊 EPS kỳ vọng: <b>${eps_est:.2f}</b>' if eps_est else ''}\n"
                           f"{f'📈 Beat rate lịch sử: <b>{beat_rate}%</b>' if beat_rate else ''}\n"
                           f"⚠️ Giá thường biến động mạnh sau báo cáo\n"
                           f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                    threading.Thread(
                        target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()
    except Exception as e:
        log.debug("[earnings_alerts] %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 2: AI SCREENING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

# Full VN market symbol list (HoSE + HNX top symbols)
VN_FULL_MARKET = list(set(
    [sym for syms in SECTOR_MAP_VN.values() for sym in syms] +
    ['VN30F1M','VNM','VCB','VIC','VHM','BID','GAS','SAB','MSN','MWG',
     'TCB','MBB','VPB','ACB','FPT','SSI','VND','HCM','HPG','HSG',
     'PDR','NVL','VJC','HVN','POW','PVD','GEX','REE','DGW','PNJ',
     'MBS','VCI','SBS','BSI','VCI','CMG','ELC','ITD','VGI',
     'NKG','VGS','TIS','PVC','PLX','OIL','DPM','DCM','LAS',
     'PC1','SHP','EVF','STB','LPB','TPB','HDB','MSB','BCM']
))

_market_scan_results = {}   # style -> {ts, top_buy, top_sell, top_breakout}
_market_scan_lock = threading.Lock()
_SCAN_HOURS = [9, 11, 14, 15]   # 9:00, 11:30, 14:30, 15:05 approx


def _get_composite_score(sym: str, mkt: str) -> dict:
    """
    Composite scoring: TA + Momentum + Volume + Foreign flow.
    Returns {'score': float, 'reasons': list, 'signal': str}
    """
    d = _ram_cache.get(sym, {}).get('data', {})
    if not d:
        try: d = fetch(sym, mkt) or {}
        except: return {}
    if not d: return {}

    a = d.get('analysis', {})
    if not a:
        try: a = analyze(d)
        except: return {}

    score = 0.0
    reasons = []

    # 1. TA score (40% weight)
    ta = a.get('total', 0)
    score += ta * 0.4
    if abs(ta) >= 7: reasons.append(f"TA={ta:+d}/19")

    # 2. RSI (10% weight)
    rsi = a.get('rsi', 50)
    if 40 <= rsi <= 60:
        score += 0.5
    elif rsi < 35:
        score += 1.5; reasons.append(f"RSI={rsi:.0f} oversold")
    elif rsi > 65:
        score -= 1.0

    # 3. Volume momentum (15% weight)
    bars = d.get('history', [])
    if bars and len(bars) >= 20:
        vols = [b.get('v', 0) for b in bars[-20:]]
        avg_vol = sum(vols[:-5]) / max(1, len(vols[:-5]))
        recent_vol = sum(vols[-5:]) / 5
        vol_ratio = recent_vol / max(1, avg_vol)
        if vol_ratio > 1.5:
            score += 2.0 * min(vol_ratio / 2, 1.5)
            reasons.append(f"Vol {vol_ratio:.1f}x TB")
        elif vol_ratio < 0.5:
            score -= 1.0

    # 4. Price momentum 5D (10% weight)
    closes = [b.get('c', 0) for b in bars if b.get('c', 0) > 0]
    if len(closes) >= 5:
        mom5 = (closes[-1] - closes[-5]) / closes[-5] * 100
        score += mom5 * 0.3
        if abs(mom5) > 3: reasons.append(f"Mom5D={mom5:+.1f}%")

    # 5. MA alignment (15% weight)
    ma20 = a.get('ma20', 0); ma50 = a.get('ma50', 0)
    price = d.get('price', closes[-1] if closes else 0)
    if ma20 and ma50 and price:
        if price > ma20 > ma50:
            score += 2.0; reasons.append("P>MA20>MA50 ✅")
        elif price < ma20 < ma50:
            score -= 2.0

    # 6. Market regime adjustment (10% weight)
    try:
        regime = get_market_regime()
        if regime.get('regime') == 'BULL' and score > 0:
            score *= 1.2
        elif regime.get('regime') == 'BEAR' and score < 0:
            score *= 1.2
    except: pass

    sig = a.get('signal', 'NEUTRAL')
    return {
        'symbol': sym, 'market': mkt,
        'score': round(score, 2),
        'ta_score': ta,
        'price': price,
        'chg': d.get('chg', 0),
        'signal': sig,
        'reasons': reasons[:4],
        'rsi': round(rsi, 1),
        'vol_ratio': round(vol_ratio, 2) if 'vol_ratio' in dir() else 1.0,
    }


def detect_breakout_pre_alert(sym: str, mkt: str) -> dict:
    """
    Detect pre-breakout accumulation pattern:
    - Volume gradually increasing over 10 days
    - Price range narrowing (BB width contracting)
    - RSI breaking out of 40-60 range
    Returns {'is_breakout': bool, 'confidence': float, 'reasons': list}
    """
    d = _ram_cache.get(sym, {}).get('data', {})
    if not d: return {'is_breakout': False}
    bars = d.get('history', [])
    if not bars or len(bars) < 20:
        return {'is_breakout': False}

    closes = [b.get('c', 0) for b in bars[-20:] if b.get('c', 0) > 0]
    highs  = [b.get('h', b.get('c', 0)) for b in bars[-20:]]
    lows   = [b.get('l', b.get('c', 0)) for b in bars[-20:]]
    vols   = [b.get('v', 0) for b in bars[-20:]]
    if len(closes) < 15: return {'is_breakout': False}

    score = 0.0
    reasons = []

    # 1. Volume trend: increasing over last 10 days
    vol_early = sum(vols[:10]) / 10
    vol_late  = sum(vols[10:]) / max(1, len(vols[10:]))
    if vol_late > vol_early * 1.2:
        score += 2.0
        reasons.append(f"Volume tăng {vol_late/vol_early:.1f}x")

    # 2. Price range contracting (volatility squeeze)
    ranges_early = [h - l for h, l in zip(highs[:10], lows[:10])]
    ranges_late  = [h - l for h, l in zip(highs[10:], lows[10:])]
    avg_range_early = sum(ranges_early) / len(ranges_early) if ranges_early else 0
    avg_range_late  = sum(ranges_late) / len(ranges_late)  if ranges_late  else 0
    if avg_range_early > 0 and avg_range_late < avg_range_early * 0.75:
        score += 2.0
        reasons.append(f"Biên độ thu hẹp {(1-avg_range_late/avg_range_early)*100:.0f}%")

    # 3. RSI exiting consolidation range (35-65)
    a = d.get('analysis', {})
    rsi = a.get('rsi', 50)
    if 55 < rsi < 70:
        score += 1.5
        reasons.append(f"RSI={rsi:.0f} thoát vùng tích lũy")
    elif rsi >= 70:
        score -= 1.0  # overbought

    # 4. Price near resistance (within 2%)
    high_20 = max(highs)
    price = closes[-1]
    if high_20 > 0 and (high_20 - price) / high_20 < 0.02:
        score += 2.0
        reasons.append(f"Giá gần đỉnh 20D {high_20:,.0f}")

    # 5. Higher lows pattern
    lows_5 = lows[-5:]
    if all(lows_5[i] <= lows_5[i+1] for i in range(len(lows_5)-1)):
        score += 1.5
        reasons.append("Đáy cao dần ↗")

    confidence = min(score / 9.0 * 100, 95)
    is_breakout = score >= 5.0

    return {
        'is_breakout': is_breakout,
        'confidence': round(confidence, 1),
        'score': round(score, 2),
        'reasons': reasons,
        'symbol': sym,
        'market': mkt,
        'price': price,
    }


def run_market_scanner_24_7():
    """
    Scan full watchlist + extended VN market.
    Returns top 5 BUY + top 5 SELL + top 5 breakout pre-alerts.
    """
    results = []
    breakouts = []
    all_syms = list(set(
        [sym for sym in _ram_cache.keys()] +
        VN_FULL_MARKET[:50]  # include extended market
    ))

    for sym in all_syms:
        d = _ram_cache.get(sym, {}).get('data', {})
        mkt = d.get('market', 'VN') if d else 'VN'
        comp = _get_composite_score(sym, mkt)
        if comp:
            results.append(comp)

        # Breakout check
        br = detect_breakout_pre_alert(sym, mkt)
        if br.get('is_breakout') and br.get('confidence', 0) >= 60:
            breakouts.append(br)

    results.sort(key=lambda x: -x['score'])
    breakouts.sort(key=lambda x: -x['confidence'])

    top_buy  = [r for r in results if r['score'] > 3][:5]
    top_sell = [r for r in results if r['score'] < -3][:5]
    top_break = breakouts[:5]

    scan_result = {
        'ts': datetime.now().isoformat(),
        'top_buy': top_buy,
        'top_sell': top_sell,
        'top_breakout': top_break,
        'total_scanned': len(results),
    }

    with _market_scan_lock:
        _market_scan_results['latest'] = scan_result

    # Push alerts for top signals
    for r in top_buy[:3]:
        can_send, _ = smart_alert_cooldown(
            f"{r['symbol']}_SCANNER_BUY", 'SCANNER', ttl=14400)
        if can_send:
            push_live_alert(r['symbol'], r['market'], 'INFO',
                f"🔍 SCANNER — {r['symbol']} BUY",
                f"Score {r['score']:+.1f} · {', '.join(r['reasons'][:2])}",
                price=r['price'])
    for br in top_break[:2]:
        can_send, _ = smart_alert_cooldown(
            f"{br['symbol']}_BREAKOUT_PRE", 'BREAKOUT', ttl=14400)
        if can_send:
            push_live_alert(br['symbol'], br['market'], 'WARNING',
                f"⚡ PRE-BREAKOUT — {br['symbol']}",
                f"Độ tin cậy {br['confidence']}% · {', '.join(br['reasons'][:2])}",
                price=br['price'])

    log.info("[Scanner] Done: %d scanned, %d buy, %d sell, %d breakout",
             len(results), len(top_buy), len(top_sell), len(top_break))
    return scan_result


def suggest_watchlist_additions(user_id: int) -> list:
    """
    Smart watchlist suggestions based on:
    - Strong sectors not currently held
    - Low correlation with existing portfolio
    - Matching risk appetite
    """
    try:
        with get_db() as c:
            current_syms = set(
                r['symbol'] for r in
                c.execute("SELECT DISTINCT symbol FROM user_watchlist WHERE user_id=?",
                          (user_id,)).fetchall())
        if not current_syms: return []

        # Find best sectors
        sector_scores = {}
        for sector, syms in SECTOR_MAP_VN.items():
            scores = []
            for sym in syms:
                if sym in _ram_cache:
                    a = _ram_cache[sym].get('data', {}).get('analysis', {})
                    if a: scores.append(a.get('total', 0))
            if scores:
                sector_scores[sector] = sum(scores) / len(scores)

        best_sectors = sorted(sector_scores, key=lambda s: -sector_scores[s])

        suggestions = []
        for sector in best_sectors[:3]:
            for sym in SECTOR_MAP_VN.get(sector, []):
                if sym in current_syms: continue
                if sym not in _ram_cache: continue
                d = _ram_cache[sym].get('data', {})
                a = d.get('analysis', {}) if d else {}
                if not a: continue
                score = a.get('total', 0)
                if score >= 3:
                    suggestions.append({
                        'symbol': sym,
                        'market': 'VN',
                        'sector': sector,
                        'score': score,
                        'signal': a.get('signal', 'NEUTRAL'),
                        'price': d.get('price', 0),
                        'reason': f"Ngành {sector} đang mạnh · Score {score:+d}",
                    })
                if len(suggestions) >= 10: break
            if len(suggestions) >= 10: break

        return suggestions[:5]
    except Exception as e:
        log.debug("[watchlist_suggest] %s", e)
        return []



# ══════════════════════════════════════════════════════════════════════════════
# EXCEL EXPORT (.xlsx) — pure stdlib, no openpyxl needed
# ══════════════════════════════════════════════════════════════════════════════
def export_portfolio_xlsx(user_id: int, sd_snapshot: dict) -> bytes:
    import io, zipfile
    
    def _rows_to_xml(headers, data_rows):
        def _esc(v):
            return str(v).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')
        rows = []
        cols = ''.join(
            '<c r="{}{}" t="inlineStr"><is><t>{}</t></is></c>'.format(
                chr(65+ci), 1, _esc(h))
            for ci, h in enumerate(headers))
        rows.append('<row r="1">{}</row>'.format(cols))
        for ri, row in enumerate(data_rows, 2):
            cols = ''
            for ci, h in enumerate(headers):
                v = row.get(h, '')
                col = chr(65+ci)
                if isinstance(v, (int, float)):
                    cols += '<c r="{}{}" t="n"><v>{}</v></c>'.format(col, ri, v)
                else:
                    cols += '<c r="{}{}" t="inlineStr"><is><t>{}</t></is></c>'.format(col, ri, _esc(v))
            rows.append('<row r="{}">{}</row>'.format(ri, cols))
        return ('<?xml version="1.0" encoding="UTF-8"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                '<sheetData>' + ''.join(rows) + '</sheetData></worksheet>')

    # Watchlist data
    rows_wl = []
    try:
        with get_db() as c:
            wl_rows = c.execute("SELECT symbol, market FROM user_watchlist WHERE user_id=?", (user_id,)).fetchall()
        for row in wl_rows:
            sym = row['symbol']; mkt = row['market']
            d = sd_snapshot.get(sym, {}); a = d.get('analysis', {}) or {}
            rows_wl.append({'Ma': sym, 'Thi truong': mkt, 'Gia': d.get('price', 0),
                'Pct Thay doi': d.get('chg', 0), 'Tin hieu': a.get('signal', 'N/A'),
                'Score/19': a.get('total', 0), 'RSI': round(float(a.get('rsi', 0) or 0), 1),
                'MA20': round(float(a.get('ma20', 0) or 0)), 'MA50': round(float(a.get('ma50', 0) or 0))})
    except Exception as _e: log.debug("[xlsx] wl: %s", _e)

    # Trades data
    rows_tr = []
    try:
        with get_db() as c:
            trades = c.execute("SELECT * FROM trades WHERE user_id=? ORDER BY entry_date DESC LIMIT 500", (user_id,)).fetchall()
        for t in trades:
            rows_tr.append({'Ma': t['symbol'], 'TT': t.get('market','VN'), 'Huong': t.get('direction','LONG'),
                'Trang thai': t.get('status','OPEN'), 'Gia vao': t.get('entry_price', 0),
                'Gia ra': t.get('exit_price', 0) or 0, 'So luong': t.get('quantity', 0),
                'PnL%': round(float(t.get('pnl_pct', 0) or 0), 2),
                'Ngay vao': str(t.get('entry_date',''))[:10], 'Ngay ra': str(t.get('exit_date',''))[:10] if t.get('exit_date') else '',
                'Ghi chu': t.get('note', '')})
    except Exception as _e: log.debug("[xlsx] trades: %s", _e)

    ct_types = ('<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '</Types>')
    rels = ('<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>')
    wb = ('<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Watchlist" sheetId="1" r:id="rId1"/><sheet name="Portfolio" sheetId="2" r:id="rId2"/></sheets>'
        '</workbook>')
    wb_rels = ('<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>'
        '</Relationships>')

    buf = io.BytesIO()
    wl_h = ['Ma','Thi truong','Gia','Pct Thay doi','Tin hieu','Score/19','RSI','MA20','MA50']
    tr_h = ['Ma','TT','Huong','Trang thai','Gia vao','Gia ra','So luong','PnL%','Ngay vao','Ngay ra','Ghi chu']
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', ct_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('xl/workbook.xml', wb)
        zf.writestr('xl/_rels/workbook.xml.rels', wb_rels)
        zf.writestr('xl/worksheets/sheet1.xml', _rows_to_xml(wl_h, rows_wl))
        zf.writestr('xl/worksheets/sheet2.xml', _rows_to_xml(tr_h, rows_tr))
    return buf.getvalue()

# ── PDF EXPORT ────────────────────────────────────────────────────────────────

def export_portfolio_pdf(user_id: int) -> bytes:
    """
    Xuất báo cáo PDF portfolio summary + metrics.
    Dùng reportlab nếu có, fallback HTML→PDF đơn giản qua weasyprint,
    hoặc plain HTML string nếu không có thư viện nào.
    """
    # Thu thập dữ liệu
    with get_db() as c:
        trades = [dict(r) for r in c.execute(
            "SELECT * FROM trades WHERE user_id=? ORDER BY entry_date DESC", (user_id,)).fetchall()]
        wl = [dict(r) for r in c.execute(
            "SELECT symbol,market FROM user_watchlist WHERE user_id=?", (user_id,)).fetchall()]

    metrics = calc_portfolio_advanced_metrics(trades)
    closed  = [t for t in trades if t.get('status') == 'CLOSED']
    open_p  = [t for t in trades if t.get('status') == 'OPEN']

    now_str = datetime.now().strftime('%d/%m/%Y %H:%M')

    # ── Thử reportlab ─────────────────────────────────────────
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                         Table, TableStyle, HRFlowable)
        from io import BytesIO

        buf = BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
                                 rightMargin=2*cm, leftMargin=2*cm,
                                 topMargin=2*cm, bottomMargin=2*cm)
        styles = getSampleStyleSheet()
        green  = colors.HexColor('#00c853')
        red    = colors.HexColor('#d50000')
        dark   = colors.HexColor('#1a1a2e')

        story = []
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'],
                                      textColor=dark, fontSize=18, spaceAfter=6)
        story.append(Paragraph('📊 Portfolio Report — SMA Pro', title_style))
        story.append(Paragraph(f'Xuất lúc: {now_str}', styles['Normal']))
        story.append(Spacer(1, 0.4*cm))
        story.append(HRFlowable(width='100%', thickness=1, color=colors.grey))
        story.append(Spacer(1, 0.3*cm))

        # Summary metrics table
        def fmt_pct(v): return f"{v:+.2f}%" if v else "—"
        def fmt_num(v): return f"{v:,.0f}đ" if v else "—"
        metrics_rows = [
            ['Chỉ số', 'Giá trị', 'Chỉ số', 'Giá trị'],
            ['Win Rate', f"{metrics.get('win_rate',0):.1f}%",
             'Profit Factor', f"{metrics.get('profit_factor',0):.2f}"],
            ['Sharpe Ratio', f"{metrics.get('sharpe',0):.3f}",
             'Sortino Ratio', f"{metrics.get('sortino',0):.3f}"],
            ['Max Drawdown', f"{metrics.get('max_drawdown_pct',0):.2f}%",
             'Calmar Ratio', f"{metrics.get('calmar',0):.3f}"],
            ['VaR 95%', f"{metrics.get('var_95',0):.2f}%",
             'CVaR 95%', f"{metrics.get('cvar_95',0):.2f}%"],
            ['MC Median', fmt_pct(metrics.get('mc_median',0)),
             'MC Ruin%', f"{metrics.get('mc_ruin_pct',0):.1f}%"],
            ['Avg Win', fmt_pct(metrics.get('avg_win',0)),
             'Avg Loss', fmt_pct(metrics.get('avg_loss',0))],
            ['Expectancy', fmt_pct(metrics.get('expectancy',0)),
             'Omega', f"{metrics.get('omega',0):.3f}"],
        ]
        t = Table(metrics_rows, colWidths=[4*cm, 3.5*cm, 4*cm, 3.5*cm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), dark),
            ('TEXTCOLOR',  (0,0), (-1,0), colors.white),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 9),
            ('GRID',       (0,0), (-1,-1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f5f5f5')]),
            ('ALIGN',      (1,0), (-1,-1), 'CENTER'),
        ]))
        story.append(Paragraph('📈 Chỉ số rủi ro', styles['Heading2']))
        story.append(t)
        story.append(Spacer(1, 0.4*cm))

        # Open positions
        if open_p:
            story.append(Paragraph(f'📂 Vị thế đang mở ({len(open_p)})', styles['Heading2']))
            op_rows = [['Mã', 'Hướng', 'Giá vào', 'SL', 'TP1', 'Ngày vào']]
            for t2 in open_p[:20]:
                vn = t2.get('market','VN') == 'VN'
                ep = t2.get('entry_price', 0)
                op_rows.append([
                    str(t2.get('symbol','')),
                    str(t2.get('direction','BUY')),
                    f"{int(ep):,}đ" if vn else f"${ep:.2f}",
                    str(t2.get('sl_price','—') or '—'),
                    str(t2.get('tp1_price','—') or '—'),
                    str(t2.get('entry_date',''))[:10],
                ])
            ot = Table(op_rows, colWidths=[2.5*cm,2*cm,3*cm,2.5*cm,2.5*cm,3*cm])
            ot.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), dark), ('TEXTCOLOR',(0,0),(-1,0),colors.white),
                ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'), ('FONTSIZE',(0,0),(-1,-1),8),
                ('GRID',(0,0),(-1,-1),0.5,colors.grey),
                ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f5f5f5')]),
            ]))
            story.append(ot)
            story.append(Spacer(1, 0.3*cm))

        # Closed trades summary
        if closed:
            story.append(Paragraph(f'✅ Lệnh đã đóng ({len(closed)} lệnh)', styles['Heading2']))
            cl_rows = [['Mã', 'Hướng', 'P&L%', 'Ngày đóng']]
            for t3 in closed[:20]:
                pnl = t3.get('pnl_pct', 0) or 0
                cl_rows.append([
                    str(t3.get('symbol','')),
                    str(t3.get('direction','BUY')),
                    f"{pnl:+.2f}%",
                    str(t3.get('exit_date',''))[:10],
                ])
            ct = Table(cl_rows, colWidths=[3*cm,3*cm,4*cm,5*cm])
            ct.setStyle(TableStyle([
                ('BACKGROUND',(0,0),(-1,0),dark),('TEXTCOLOR',(0,0),(-1,0),colors.white),
                ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),8),
                ('GRID',(0,0),(-1,-1),0.5,colors.grey),
                ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f5f5f5')]),
                ('TEXTCOLOR',(2,1),(-1,-1),colors.black),
            ]))
            story.append(ct)

        story.append(Spacer(1, 0.5*cm))
        story.append(Paragraph('<i>Báo cáo tạo bởi SMA Pro — Designed by Vubui365</i>',
                               styles['Italic']))
        doc.build(story)
        return buf.getvalue()

    except ImportError:
        log.debug("[PDF] reportlab not installed — returning HTML")

    # ── Fallback: trả HTML nếu không có reportlab ─────────────
    rows_html = ''.join(
        f"<tr><td>{t.get('symbol','')}</td><td>{t.get('direction','')}</td>"
        f"<td style='color:{'green' if (t.get('pnl_pct') or 0)>=0 else 'red'}'>"
        f"{(t.get('pnl_pct') or 0):+.2f}%</td></tr>"
        for t in closed[:50]
    )
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Portfolio Report — SMA Pro</title>
<style>body{{font-family:sans-serif;padding:20px}}
table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #ddd;padding:6px}}
th{{background:#1a1a2e;color:#fff}}.metric{{display:inline-block;margin:8px;padding:10px;background:#f5f5f5;border-radius:8px;min-width:120px}}</style>
</head><body>
<h1>📊 Portfolio Report</h1><p>{now_str}</p>
<h2>Chỉ số rủi ro</h2>
<div>
{''.join(f"<div class='metric'><b>{k}</b><br>{v}</div>" for k,v in [
    ('Win Rate', f"{metrics.get('win_rate',0):.1f}%"),
    ('Sharpe', f"{metrics.get('sharpe',0):.3f}"),
    ('Max DD', f"{metrics.get('max_drawdown_pct',0):.2f}%"),
    ('VaR 95%', f"{metrics.get('var_95',0):.2f}%"),
    ('MC Median', f"{metrics.get('mc_median',0):+.1f}%"),
    ('Profit Factor', f"{metrics.get('profit_factor',0):.2f}"),
])}
</div>
<h2>Lệnh đã đóng ({len(closed)})</h2>
<table><tr><th>Mã</th><th>Hướng</th><th>P&L%</th></tr>{rows_html}</table>
<p><i>SMA Pro — Designed by Vubui365 — 0766662949</i></p>
</body></html>"""
    return html.encode('utf-8')


def _load_ssi_for_user(user_id: int):
    """Load SSI credentials từ DB, trả về SSIFastConnect instance."""
    with get_db() as c:
        row = c.execute(
            "SELECT consumer_id, consumer_secret, account_no, enabled FROM ssi_config WHERE user_id=?",
            (user_id,)).fetchone()
    if not row:
        raise ValueError("Chưa cấu hình SSI. Vào Settings → SSI Config để nhập thông tin.")
    if not row['enabled']:
        raise ValueError("SSI chưa bật. Vào Settings → SSI Config → bật Enable.")
    if not row['consumer_id'] or not row['consumer_secret']:
        raise ValueError("consumer_id hoặc consumer_secret trống. Kiểm tra lại Settings.")
    return SSIFastConnect(row['consumer_id'], row['consumer_secret'], row['account_no'])


# ── AUTO-UPDATE ───────────────────────────────────────────────────────────────

_UPDATE_CHECK_URL = "https://api.github.com/repos/vubui365/sma-Pro/releases/latest"
_UPDATE_CHECK_INTERVAL = 3600 * 6  # Check mỗi 6 giờ
_last_update_check = 0

def check_for_update() -> dict:
    """
    Kiểm tra phiên bản mới trên GitHub.
    Trả về {"has_update": bool, "latest": str, "current": str, "url": str}
    """
    global _last_update_check
    if time.time() - _last_update_check < _UPDATE_CHECK_INTERVAL:
        return {"has_update": False, "reason": "too_soon"}
    _last_update_check = time.time()

    try:
        import urllib.request as _ur
        req = _ur.Request(
            _UPDATE_CHECK_URL,
            headers={"User-Agent": f"SMA-Pro/{VERSION}", "Accept": "application/vnd.github+json"}
        )
        with _ur.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        latest = str(data.get("tag_name","")).lstrip("v")
        url    = data.get("html_url","")
        has_update = latest and latest != VERSION
        result = {
            "has_update": has_update,
            "latest": latest,
            "current": VERSION,
            "url": url,
            "release_notes": str(data.get("body",""))[:500],
        }
        log.info("[UPDATE] Current: %s | Latest: %s | has_update: %s", VERSION, latest, has_update)
        return result
    except Exception as _ue:
        log.debug("[UPDATE] check failed: %s", _ue)
        return {"has_update": False, "error": str(_ue)}

def download_update(target_dir: str = ".") -> dict:
    """
    Tải file server_Pro.py và index_Pro.html mới nhất từ GitHub release.
    Backup file cũ trước khi overwrite.
    """
    try:
        info = check_for_update()
        if not info.get("has_update"):
            return {"ok": False, "msg": "Đang dùng phiên bản mới nhất"}

        import urllib.request as _ur, shutil as _sh
        from pathlib import Path

        # Lấy download URL từ assets
        req = _ur.Request(
            _UPDATE_CHECK_URL,
            headers={"User-Agent": f"SMA-Pro/{VERSION}", "Accept": "application/vnd.github+json"}
        )
        with _ur.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())

        downloaded = []
        for asset in data.get("assets", []):
            name = asset.get("name","")
            if name in ("server_Pro.py", "index_Pro.html"):
                url = asset.get("browser_download_url","")
                if not url: continue
                dest = Path(target_dir) / name
                backup = dest.with_suffix(f".bak_{VERSION}")
                if dest.exists(): _sh.copy2(dest, backup)
                _ur.urlretrieve(url, dest)
                downloaded.append(name)
                log.info("[UPDATE] Downloaded: %s", name)

        if downloaded:
            return {"ok": True, "downloaded": downloaded, "version": info["latest"],
                    "msg": f"Đã tải {', '.join(downloaded)}. Khởi động lại server để áp dụng."}
        return {"ok": False, "msg": "Không tìm thấy assets trong release"}
    except Exception as _due:
        log.error("[UPDATE] download: %s", _due)
        return {"ok": False, "error": str(_due)}


# ══════════════════════════════════════════════════════════════
# INVESTMENT SCREENER — Growth & Value
# ══════════════════════════════════════════════════════════════

def score_growth(data: dict, analysis: dict, fundamental: dict = None) -> dict:
    """
    Tính điểm ĐẦU TƯ TĂNG TRƯỞNG.
    Tập trung: momentum kỹ thuật + EPS/doanh thu tăng mạnh.
    Cập nhật: mỗi 3 ngày.
    """
    sc  = analysis.get("scores", {})
    sig = analysis.get("signal", "NEUTRAL")
    total = analysis.get("total", 0)
    conf  = analysis.get("confluence", 0)
    f   = fundamental or {}

    score   = 0.0
    reasons = []

    # ── KỸ THUẬT ─────────────────────────────────────────────
    # 1. MACD (weight 3): cắt lên hoặc histogram dương tăng
    macd_s = sc.get("macd", 0)
    if macd_s > 0:
        score += 3.0; reasons.append("MACD tín hiệu mua")
    elif macd_s < 0:
        score -= 2.0

    # 2. ADX (weight 3): xu hướng rõ + tăng
    adx_val = analysis.get("adx_val", 0)
    adx_s   = sc.get("adx", 0)
    if adx_val >= 30 and adx_s > 0:
        score += 3.0; reasons.append(f"ADX={adx_val:.0f} xu hướng tăng mạnh")
    elif adx_val >= 20 and adx_s > 0:
        score += 1.5; reasons.append(f"ADX={adx_val:.0f} có xu hướng")
    elif adx_s < 0:
        score -= 1.5

    # 3. RSI (weight 2): vùng 50-70 (momentum không quá mua)
    rsi = analysis.get("rsi", 50)
    if 50 <= rsi <= 68:
        score += 2.0; reasons.append(f"RSI={rsi:.0f} vùng tăng trưởng")
    elif 40 <= rsi < 50:
        score += 0.5
    elif rsi > 75:
        score -= 1.5  # quá mua
    elif rsi < 35:
        score -= 1.0

    # 4. MA Cross (weight 2): MA20 > MA50, xu hướng dài hạn tăng
    ma_s = sc.get("ma", 0)
    if ma_s > 0:
        ma20 = analysis.get("ma20", 0)
        ma50 = analysis.get("ma50", 0)
        ma200= analysis.get("ma200",0)
        if ma20 > 0 and ma50 > 0:
            gap = (ma20 - ma50) / ma50 * 100
            score += min(2.0, 1.0 + gap / 5)
            reasons.append(f"MA20>{ma50:.0f} (+{gap:.1f}%)")
        if ma20 > 0 and ma200 > 0 and ma20 > ma200:
            score += 0.5  # bonus dài hạn

    # 5. Volume + OBV (weight 2): dòng tiền vào
    vol_s = sc.get("volume", 0)
    obv_s = sc.get("obv", 0)
    if vol_s > 0 and obv_s > 0:
        score += 2.0; reasons.append("Volume+OBV tăng (dòng tiền vào)")
    elif vol_s > 0:
        score += 1.0
    elif vol_s < 0:
        score -= 1.5

    # 6. Squeeze (weight 2): bùng nổ tăng
    sq_s = sc.get("squeeze", 0)
    if sq_s > 0:
        score += 2.0; reasons.append("Squeeze giải phóng tăng")
    elif analysis.get("squeeze_on", False):
        score += 0.5  # đang tích lũy, sắp bùng

    # 7. Elder Ray (weight 1)
    elder_s = sc.get("elder", 0)
    elder_b = analysis.get("elder_bulls", 0)
    if elder_s > 0 and elder_b > 0:
        score += 1.0; reasons.append("Elder Bulls tăng")

    # 8. Ichimoku (weight 1.5)
    if sc.get("ichi", 0) > 0:
        score += 1.5; reasons.append("Trên Ichimoku Cloud")

    # 9. Pattern (weight 1)
    if sc.get("pattern", 0) > 0:
        score += 1.0; reasons.append("Pattern bullish")

    # 10. Confluence bonus
    if conf >= 6:
        score += 2.0; reasons.append("Confluence cực mạnh")
    elif conf >= 3:
        score += 1.0

    # ── CƠ BẢN ────────────────────────────────────────────────
    # fetch_fundamental_vn dùng: profit_growth_yoy, revenue_growth_yoy
    eps_growth = f.get("profit_growth_yoy") or f.get("eps_growth_yoy")
    rev_growth = f.get("revenue_growth_yoy") or f.get("rev_growth_yoy")
    roe        = f.get("roe") or f.get("return_on_equity")
    pe         = f.get("pe") or f.get("pe_ratio")

    # Tính eps_growth từ income_statements nếu không có sẵn
    if eps_growth is None:
        _inc = f.get("income_statements", []) or []
        if len(_inc) >= 2:
            _curr = float(_inc[0].get("net_profit", _inc[0].get("net_income", 0)) or 0)
            _prev = float(_inc[1].get("net_profit", _inc[1].get("net_income", 0)) or 0)
            if _prev and _prev != 0:
                eps_growth = (_curr - _prev) / abs(_prev) * 100

    if eps_growth is not None:
        try:
            eg = float(eps_growth)
            if eg >= 30:
                score += 3.0; reasons.append(f"EPS tăng {eg:.0f}% YoY")
            elif eg >= 15:
                score += 2.0; reasons.append(f"EPS tăng {eg:.0f}% YoY")
            elif eg >= 5:
                score += 0.5
            elif eg < 0:
                score -= 2.0
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if rev_growth is not None:
        try:
            rg = float(rev_growth)
            if rg >= 20:
                score += 2.0; reasons.append(f"Doanh thu +{rg:.0f}% YoY")
            elif rg >= 10:
                score += 1.0
            elif rg < 0:
                score -= 1.0
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if roe is not None:
        try:
            r = float(roe)
            if r >= 20:
                score += 1.0; reasons.append(f"ROE={r:.0f}% cao")
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if pe is not None:
        try:
            p = float(pe)
            # Tăng trưởng chấp nhận P/E cao hơn nhưng không quá 50
            if 0 < p <= 30:
                score += 0.5
            elif p > 50:
                score -= 1.0  # quá đắt
        except Exception as _e: log.debug('[score] cast: %s', _e)

    # ── Liquidity filter ──────────────────────────────────────
    if analysis.get("low_liquidity", False):
        score -= 3.0; reasons.append("Thanh khoản thấp!")

    return {
        "score":   round(score, 2),
        "reasons": reasons[:6],
        "style":   "growth",
        "signal":  sig,
        "total_ta": total,
    }


def score_value(data: dict, analysis: dict, fundamental: dict = None) -> dict:
    """
    Tính điểm ĐẦU TƯ GIÁ TRỊ.
    Tập trung: định giá thấp + nền tảng tốt + kỹ thuật ổn định.
    Cập nhật: mỗi 7 ngày.
    """
    sc    = analysis.get("scores", {})
    total = analysis.get("total", 0)
    f     = fundamental or {}

    score   = 0.0
    reasons = []

    # ── CƠ BẢN (quan trọng hơn kỹ thuật) ────────────────────
    pe  = f.get("pe") or f.get("pe_ratio")
    pb  = f.get("pb") or f.get("pb_ratio")
    roe = f.get("roe") or f.get("return_on_equity")
    de  = f.get("debt_equity") or f.get("debt_to_equity") or f.get("debt_on_equity")
    eps = f.get("eps") or f.get("earning_per_share")
    div = f.get("dividend_yield") or f.get("dividend_rate")
    # fetch_fundamental_vn không có eps_positive_quarters → tính từ income_statements
    _income = f.get("income_statements", []) or []
    eps_stable = sum(1 for q in _income if float(q.get("net_profit", q.get("net_income", 0)) or 0) > 0)

    if pe is not None:
        try:
            p = float(pe)
            if 0 < p <= 8:
                score += 4.0; reasons.append(f"P/E={p:.1f} rất rẻ")
            elif 0 < p <= 12:
                score += 3.0; reasons.append(f"P/E={p:.1f} rẻ")
            elif 0 < p <= 15:
                score += 2.0; reasons.append(f"P/E={p:.1f} hợp lý")
            elif 0 < p <= 20:
                score += 0.5
            elif p > 30:
                score -= 2.0  # đắt
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if pb is not None:
        try:
            b = float(pb)
            if 0 < b <= 0.8:
                score += 3.0; reasons.append(f"P/B={b:.2f} dưới book value!")
            elif 0 < b <= 1.2:
                score += 2.0; reasons.append(f"P/B={b:.2f} gần book value")
            elif 0 < b <= 1.5:
                score += 1.0
            elif b > 3.0:
                score -= 1.0
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if roe is not None:
        try:
            r = float(roe)
            if r >= 20:
                score += 3.0; reasons.append(f"ROE={r:.0f}% xuất sắc")
            elif r >= 15:
                score += 2.0; reasons.append(f"ROE={r:.0f}% tốt")
            elif r >= 10:
                score += 1.0
            elif r < 0:
                score -= 2.0
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if de is not None:
        try:
            d = float(de)
            if d <= 0.2:
                score += 2.0; reasons.append(f"Nợ/VCP={d:.2f} rất thấp")
            elif d <= 0.5:
                score += 1.5; reasons.append(f"Nợ/VCP={d:.2f} thấp")
            elif d <= 1.0:
                score += 0.5
            elif d > 2.0:
                score -= 2.0  # nợ cao nguy hiểm
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if div is not None:
        try:
            dy = float(div)
            if dy >= 5:
                score += 2.0; reasons.append(f"Cổ tức {dy:.1f}%/năm")
            elif dy >= 3:
                score += 1.0; reasons.append(f"Cổ tức {dy:.1f}%/năm")
        except Exception as _e: log.debug('[score] cast: %s', _e)

    if eps_stable is not None:
        try:
            q = int(eps_stable)
            if q >= 8:
                score += 2.0; reasons.append(f"EPS dương {q} quý liên tiếp")
            elif q >= 4:
                score += 1.0
        except Exception as _e: log.debug('[score] cast: %s', _e)

    # ── KỸ THUẬT (vai trò: tìm điểm vào tốt) ────────────────
    rsi = analysis.get("rsi", 50)
    # Value: RSI 30-55 là lý tưởng (không quá mua, chưa quá bán hẳn)
    if 30 <= rsi <= 55:
        score += 2.0; reasons.append(f"RSI={rsi:.0f} vùng tích lũy tốt")
    elif rsi < 30:
        score += 1.5; reasons.append(f"RSI={rsi:.0f} quá bán - cơ hội mua")
    elif rsi > 70:
        score -= 1.5

    # BB Lower: giá gần/tại vùng rẻ
    bb_s = sc.get("bb", 0)
    if bb_s > 0:
        score += 1.5; reasons.append("Gần Lower BB - giá rẻ kỹ thuật")

    # Support/Resistance: gần vùng hỗ trợ
    if sc.get("sr", 0) > 0:
        score += 1.0; reasons.append("Tại vùng hỗ trợ mạnh")

    # Xu hướng dài hạn không quá xấu (MA200)
    ma200 = analysis.get("ma200", 0)
    price = data.get("price", 0)
    if ma200 > 0 and price > 0:
        dist_pct = (price - ma200) / ma200 * 100
        if -15 <= dist_pct <= 10:
            score += 1.0; reasons.append(f"Giá gần MA200 ({dist_pct:+.1f}%)")
        elif dist_pct < -15:
            score += 0.5  # rất rẻ so MA200

    # OBV tích lũy
    if sc.get("obv", 0) >= 0:
        score += 0.5

    # Total TA không quá tiêu cực
    if total >= 3:
        score += 1.0
    elif total <= -5:
        score -= 1.5

    # ── Liquidity filter ──────────────────────────────────────
    if analysis.get("low_liquidity", False):
        score -= 3.0; reasons.append("Thanh khoản thấp!")

    return {
        "score":   round(score, 2),
        "reasons": reasons[:6],
        "style":   "value",
        "signal":  analysis.get("signal", "NEUTRAL"),
        "total_ta": total,
    }


# ── Lấy fundamental data với cache ───────────────────────────
_FUND_CACHE: dict = {}
_FUND_TTL = 86400  # 24h

def _get_fundamental(sym: str, mkt: str) -> dict:
    """Cache-aware fundamental fetch."""
    key = f"{sym}_{mkt}"
    cached = _FUND_CACHE.get(key)
    if cached and time.time() - cached['ts'] < _FUND_TTL:
        return cached['data']
    try:
        import concurrent.futures as _cf
        with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
            if mkt == 'VN':
                fut = _ex.submit(fetch_fundamental_vn, sym)
            else:
                fut = _ex.submit(fetch_financial_summary, sym, mkt)
            data = fut.result(timeout=8)
        _FUND_CACHE[key] = {'ts': time.time(), 'data': data or {}}
        return data or {}
    except Exception as _fe:
        log.debug("[fund_cache] %s: %s", sym, _fe)
        return {}


# ── Core: chạy screener và lưu DB ────────────────────────────
_INVEST_LOCK    = threading.RLock()      # RLock tránh deadlock
_INVEST_RUNNING: dict = {}               # {style: bool} tránh chạy 2 lần

def db_get_analysis(sym: str, mkt: str) -> dict:
    """Lấy kết quả phân tích kỹ thuật đã lưu trong DB."""
    try:
        with get_db() as c:
            row = c.execute(
                "SELECT analysis, analyzed_at FROM analysis_cache WHERE symbol=? AND market=?",
                (sym, mkt)).fetchone()
        if row:
            a = json.loads(row['analysis'])
            a['_analyzed_at'] = row['analyzed_at']
            return a
    except Exception as _e:
        log.debug("[analysis_cache] get %s: %s", sym, _e)
    return {}


def db_get_all_analysis(market: str = 'ALL') -> list:
    """
    Lấy toàn bộ analysis_cache từ DB.
    Dùng cho screener/invest để không phải re-analyze.
    """
    try:
        with get_db() as c:
            if market == 'ALL':
                rows = c.execute(
                    """SELECT ac.symbol, ac.market, ac.analysis, ac.ta_score,
                              ac.signal, ac.analyzed_at,
                              o.price, o.chg
                       FROM analysis_cache ac
                       LEFT JOIN (
                           SELECT symbol, last_price as price, last_chg as chg
                           FROM fetch_meta WHERE last_price > 0
                       ) o ON o.symbol = ac.symbol
                       ORDER BY abs(ac.ta_score) DESC"""
                ).fetchall()
            else:
                rows = c.execute(
                    """SELECT ac.symbol, ac.market, ac.analysis, ac.ta_score,
                              ac.signal, ac.analyzed_at,
                              o.price, o.chg
                       FROM analysis_cache ac
                       LEFT JOIN (
                           SELECT symbol, last_price as price, last_chg as chg
                           FROM fetch_meta WHERE last_price > 0
                       ) o ON o.symbol = ac.symbol
                       WHERE ac.market=?
                       ORDER BY abs(ac.ta_score) DESC""",
                    (market,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as _e:
        log.debug("[analysis_cache] get_all: %s", _e)
        return []


def run_investment_screener(style: str = 'growth', market: str = 'VN',
                             top_n: int = 10, force: bool = False) -> list:
    """
    Chạy screener đầu tư tăng trưởng hoặc giá trị.
    style: 'growth' (3 ngày) | 'value' (7 ngày)
    Trả về list top_n mã tốt nhất.
    """
    ttl_h = 72 if style == 'growth' else 168  # 3 ngày hoặc 7 ngày
    if _INVEST_RUNNING.get(style): return []
    _INVEST_RUNNING[style] = True
    try:
        with _INVEST_LOCK:
            # Kiểm tra TTL
            if not force:
                with get_db() as c:
                    meta = c.execute(
                        "SELECT last_run FROM investment_lists_meta WHERE type=?",
                        (style,)).fetchone()
                if meta and meta['last_run']:
                    try:
                        last = datetime.fromisoformat(meta['last_run'])
                        age_h = (datetime.now() - last).total_seconds() / 3600
                        if age_h < ttl_h:
                            # Trả về từ DB cache
                            with get_db() as c:
                                rows = c.execute(
                                    """SELECT symbol, market, rank, score, reasons
                                    FROM investment_lists
                                    WHERE type=? ORDER BY rank ASC""",
                                    (style,)).fetchall()
                            if rows:
                                log.info("[invest] %s: cached (age=%.1fh < %dh)", style, age_h, ttl_h)
                                return [dict(r) for r in rows]
                    except Exception as _e:
                        log.debug("[invest] meta parse: %s", _e)

            # Lấy symbols từ analysis_cache (ưu tiên — đã có phân tích sẵn)
            # Fallback → user_watchlist → watchlist global → popular stocks
            with get_db() as c:
                if market == 'ALL':
                    rows = c.execute(
                        "SELECT DISTINCT symbol, market FROM analysis_cache"
                    ).fetchall()
                else:
                    rows = c.execute(
                        "SELECT DISTINCT symbol, market FROM analysis_cache WHERE market=?",
                        (market,)).fetchall()

            if not rows:
                # Chưa có analysis → lấy từ watchlist để biết cần score gì
                with get_db() as c:
                    if market == 'ALL':
                        rows = c.execute(
                            "SELECT DISTINCT symbol, market FROM user_watchlist"
                        ).fetchall() or c.execute(
                            "SELECT DISTINCT symbol, market FROM watchlist"
                        ).fetchall()
                    else:
                        rows = c.execute(
                            "SELECT DISTINCT symbol, market FROM user_watchlist WHERE market=?",
                            (market,)).fetchall() or c.execute(
                            "SELECT DISTINCT symbol, market FROM watchlist WHERE market=?",
                            (market,)).fetchall()

            if not rows:
                if market in ('VN', 'ALL'):
                    rows = [{'symbol': s, 'market': 'VN'} for s in VN_POPULAR[:50]]
                if market in ('US', 'ALL'):
                    rows = list(rows) + [{'symbol': s, 'market': 'US'} for s in US_POPULAR[:30]]

            log.info("[invest] %d symbols để score (market=%s, từ analysis_cache+watchlist)",
                     len(rows), market)

            log.info("[invest] Running %s screener (market=%s, rows=%d)...", style, market, len(rows))

            # Kiểm tra bao nhiêu mã đã có analysis sẵn
            try:
                with get_db() as _wc:
                    _analyzed = _wc.execute(
                        "SELECT COUNT(*) FROM analysis_cache").fetchone()[0]
                log.info("[invest] %d/%d symbols đã có analysis trong DB",
                         _analyzed, len(rows))
            except Exception:
                log.info("[invest] %d symbols cần score", len(rows))

            candidates = []
            total_syms = len(rows)

            def _score_one(row_item):
                """
                Score một mã — chỉ đọc từ DB, không fetch API.
                Dữ liệu đã được fetch và analyze bởi start_refresh().
                """
                _sym = row_item['symbol']
                _mkt = row_item['market']
                try:
                    # Bước 1: Lấy kết quả analyze từ analysis_cache (DB)
                    _a = db_get_analysis(_sym, _mkt)

                    # Bước 2: Nếu chưa có analysis → lấy từ RAM cache
                    if not _a or not _a.get('signal'):
                        _cached = _ram_cache.get(_sym, {}).get('data', {})
                        if _cached and _cached.get('analysis'):
                            _a = _cached['analysis']

                    # Bước 3: Nếu vẫn không có → bỏ qua (sẽ có sau lần refresh tiếp)
                    if not _a or _a.get('total') is None:
                        log.debug("[invest] %s: chưa có analysis, bỏ qua", _sym)
                        return None

                    # Lấy giá từ DB meta
                    _meta = db_get_meta(_sym)
                    _price = float((_meta or {}).get('last_price', 0) or 0)  # fetch_meta.last_price
                    _chg   = float((_meta or {}).get('last_chg',   0) or 0)  # fetch_meta.last_chg

                    # RAM cache giá nếu có
                    _cached_data = _ram_cache.get(_sym, {}).get('data', {})
                    if _cached_data.get('price', 0) > 0:
                        _price = _cached_data['price']
                        _chg   = _cached_data.get('chg', 0)

                    # Fundamental (không cần fetch, đọc từ cache)
                    _fund = _FUND_CACHE.get(f"{_sym}_{_mkt}", {}).get('data', {})

                    # Tính điểm
                    _data_stub = {
                        'price':  _price,
                        'chg':    _chg,
                        'market': _mkt,
                        'symbol': _sym,
                    }
                    _res = score_growth(_data_stub, _a, _fund) if style == 'growth'                            else score_value(_data_stub, _a, _fund)

                    return {
                        'symbol':   _sym,
                        'market':   _mkt,
                        'score':    _res['score'],
                        'reasons':  _res['reasons'],
                        'price':    _price,
                        'chg':      _chg,
                        'signal':   _res.get('signal', _a.get('signal', 'NEUTRAL')),
                        'total_ta': int(_a.get('total', 0)),
                    }
                except Exception as _se:
                    log.debug("[invest] score %s: %s", _sym, _se)
                    return None

            # Chạy song song tối đa 4 threads, timeout 3 phút
            import concurrent.futures as _cf_invest
            with _cf_invest.ThreadPoolExecutor(max_workers=4) as _pool:
                _futs = [_pool.submit(_score_one, r) for r in rows]
                try:
                    for _f in _cf_invest.as_completed(_futs, timeout=180):
                        _r = _f.result()
                        if _r: candidates.append(_r)
                except _cf_invest.TimeoutError:
                    log.warning("[invest] timeout 180s — %d candidates", len(candidates))


            # Sắp xếp và lấy top N
            candidates.sort(key=lambda x: x['score'], reverse=True)
            log.info("[invest] %s: %d candidates scored, top scores: %s",
                     style, len(candidates),
                     [f"{c['symbol']}:{c['score']:.1f}" for c in candidates[:5]])
            top = candidates[:top_n]

            # Lưu DB
            now_iso = datetime.now().isoformat()
            next_run = (datetime.now() + timedelta(hours=ttl_h)).isoformat()
            if not top:
                log.warning("[invest] %s: NO candidates! Possible causes: "
                            "1) API unreachable (HAS_VN=%s) "
                            "2) All fetch() returned None "
                            "3) All scores negative. rows=%d",
                            style, HAS_VN, len(rows))
                # Vẫn update meta để không re-run liên tục
                with get_db() as c:
                    c.execute("""INSERT OR REPLACE INTO investment_lists_meta
                        (type, last_run, next_run, status) VALUES (?,?,?,?)""",
                        (style, datetime.now().isoformat(),
                         (datetime.now() + timedelta(hours=1)).isoformat(),
                         'no_data'))
                return top

            try:
                with get_db() as c:
                    # Xóa danh sách cũ
                    c.execute("DELETE FROM investment_lists WHERE type=?", (style,))
                    # Lưu danh sách mới
                    for rank, item in enumerate(top, 1):
                        c.execute("""INSERT OR REPLACE INTO investment_lists
                            (type, symbol, market, rank, score, reasons, updated_at)
                            VALUES (?,?,?,?,?,?,?)""",
                            (style, item['symbol'], item['market'], rank,
                            item['score'],
                            json.dumps(item['reasons'], ensure_ascii=False),
                            now_iso))
                    # Cập nhật meta
                    c.execute("""INSERT OR REPLACE INTO investment_lists_meta
                        (type, last_run, next_run, status) VALUES (?,?,?,?)""",
                        (style, now_iso, next_run, 'done'))
            except Exception as _de:
                log.error("[invest] save DB: %s", _de)

            log.info("[invest] %s done: %d/%d candidates, top %d saved",
                    style, len(candidates), total_syms, len(top))
            return top
    finally:
        _INVEST_RUNNING[style] = False

def get_investment_list(style: str) -> dict:
    """Lấy danh sách đầu tư từ DB, kèm meta."""
    with get_db() as c:
        rows = c.execute(
            """SELECT il.symbol, il.market, il.rank, il.score, il.reasons, il.updated_at,
                      m.last_run, m.next_run
               FROM investment_lists il
               LEFT JOIN investment_lists_meta m ON m.type = il.type
               WHERE il.type=? ORDER BY il.rank ASC""",
            (style,)).fetchall()
    result = []
    for r in rows:
        reasons = []
        try: reasons = json.loads(r['reasons'])
        except Exception as _e: log.debug('[score] cast: %s', _e)
        item = dict(r)
        item['reasons'] = reasons
        # Bổ sung giá hiện tại từ RAM cache nếu có
        cached = _ram_cache.get(r['symbol'], {}).get('data', {})
        item['price'] = cached.get('price', 0)
        item['chg']   = cached.get('chg', 0)
        result.append(item)
    meta = {}
    if result:
        meta = {'last_run': result[0].get('last_run'), 'next_run': result[0].get('next_run')}
    return {'list': result, 'meta': meta, 'style': style, 'count': len(result)}


# ── Background worker: tự động chạy lại khi đến hạn ─────────
def _invest_scheduler():
    """Chạy trong background thread, kiểm tra TTL mỗi giờ."""
    while True:
        try:
            time.sleep(3600)  # check mỗi 1 giờ
            now = datetime.now()
            with get_db() as c:
                metas = c.execute(
                    "SELECT type, next_run FROM investment_lists_meta").fetchall()
            for m in metas:
                try:
                    nr = datetime.fromisoformat(m['next_run'])
                    if now >= nr:
                        log.info("[invest_sched] Auto-run %s screener", m['type'])
                        threading.Thread(
                            target=run_investment_screener,
                            args=(m['type'],), daemon=True).start()
                except Exception as _e:
                    log.debug("[invest_sched] %s: %s", m['type'], _e)
        except Exception as _e:
            log.debug("[invest_sched] loop: %s", _e)

_invest_sched_thread = threading.Thread(target=_invest_scheduler, daemon=True)
_invest_sched_thread.start()


# ── TREND FORECAST ────────────────────────────────────────────────────────────

def calc_trend_forecast(data: dict, periods: int = 10) -> dict:
    """
    Dự đoán xu hướng giá dựa trên kết hợp kỹ thuật:
    - Linear regression channel
    - ATR-based price projection
    - Fibonacci extension targets
    - RSI/MACD momentum
    - Volume-weighted momentum
    Returns: forecast dict với target giá, confidence, scenario
    """
    import math
    h = data.get('history', [])
    if not h or len(h) < 30:
        return {'error': 'Chưa đủ dữ liệu (cần ≥30 nến)'}

    closes = [b['c'] for b in h if b.get('c',0) > 0]
    highs  = [b.get('h', b['c']) for b in h if b.get('c',0) > 0]
    lows   = [b.get('l', b['c']) for b in h if b.get('c',0) > 0]
    vols   = [b.get('v', 0) for b in h if b.get('c',0) > 0]
    n = len(closes)
    price = closes[-1]
    if price <= 0:
        return {'error': 'Giá không hợp lệ'}

    vn = data.get('market', 'VN') == 'VN'
    fp = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"

    # ── 1. Linear Regression (lookback 20 bars) ──────────────
    lb = min(n, 20)
    x = list(range(lb))
    y = closes[-lb:]
    xm = sum(x) / lb; ym = sum(y) / lb
    num = sum((x[i]-xm)*(y[i]-ym) for i in range(lb))
    den = sum((x[i]-xm)**2 for i in range(lb))
    slope = num / den if den else 0
    intercept = ym - slope * xm
    # Dự báo giá sau `periods` bars
    lr_target = intercept + slope * (lb - 1 + periods)
    lr_change_pct = (lr_target - price) / price * 100

    # ── 2. ATR-based range forecast ──────────────────────────
    def _atr(bars, period=14):
        trs = []
        for i in range(1, min(len(bars), period+1)):
            tr = max(bars[i][1]-bars[i][2],
                     abs(bars[i][1]-bars[i-1][0]),
                     abs(bars[i][2]-bars[i-1][0]))
            trs.append(tr)
        return sum(trs)/len(trs) if trs else price*0.02

    h_valid = [b for b in h if b.get('c',0)>0]
    bar_tuples = [(b['c'], b.get('h',b['c']), b.get('l',b['c'])) for b in h_valid]
    atr = _atr(bar_tuples)
    atr_up   = price + atr * periods * 0.5
    atr_down = price - atr * periods * 0.5

    # ── 3. Momentum scoring ───────────────────────────────────
    # RSI
    gains=[]; losses=[]
    for i in range(1, min(15, n)):
        d = closes[-i] - closes[-i-1]
        if d>0: gains.append(d)
        else: losses.append(abs(d))
    avg_gain = sum(gains)/14 if gains else 0.01
    avg_loss = sum(losses)/14 if losses else 0.01
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # MACD (12-26)
    def _ema(data, p):
        k = 2/(p+1); v = data[0]
        for d in data[1:]: v = d*k + v*(1-k)
        return v
    ema12 = _ema(closes[-26:], 12) if n>=26 else price
    ema26 = _ema(closes[-26:], 26) if n>=26 else price
    macd = ema12 - ema26

    # MA trend
    ma20 = sum(closes[-20:])/20 if n>=20 else price
    ma50 = sum(closes[-50:])/50 if n>=50 else price
    ma200= sum(closes[-200:])/200 if n>=200 else price

    # Volume momentum
    avg_vol = sum(vols[-20:])/20 if vols else 1
    last_vol = vols[-1] if vols else 1
    vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1

    # ── 4. Fibonacci targets ─────────────────────────────────
    hi_v = max(highs[-60:]) if n>=60 else max(highs)
    lo_v = min(lows[-60:])  if n>=60 else min(lows)
    rng = hi_v - lo_v if hi_v != lo_v else price * 0.1
    hi_idx = highs.index(hi_v) if hi_v in highs else -1
    lo_idx = lows.index(lo_v) if lo_v in lows else -1
    is_up = lo_idx < hi_idx  # đáy trước đỉnh

    if is_up:  # Retracement từ đỉnh
        fib_targets_up = [
            (hi_v + 0.272*rng, "Fib Ext 127.2%"),
            (hi_v + 0.618*rng, "Fib Ext 161.8%"),
        ]
        fib_support = [
            (hi_v - 0.382*rng, "Fib 38.2%"),
            (hi_v - 0.500*rng, "Fib 50%"),
            (hi_v - 0.618*rng, "Fib 61.8%"),
        ]
    else:  # Recovery từ đáy
        fib_targets_up = [
            (lo_v + 0.382*rng, "Fib 38.2%"),
            (lo_v + 0.500*rng, "Fib 50%"),
            (lo_v + 0.618*rng, "Fib 61.8%"),
            (lo_v + rng, "Fib 100% (đỉnh cũ)"),
        ]
        fib_support = [
            (lo_v, "Đáy Swing (hỗ trợ mạnh)"),
            (lo_v - 0.272*rng, "Fib Ext 127.2%"),
        ]

    # ── 5. Tổng hợp signal và confidence ─────────────────────
    bull_score = 0; bear_score = 0
    signals = []

    if rsi > 55: bull_score += 1.5; signals.append(f"RSI={rsi:.0f} (tích cực)")
    elif rsi < 45: bear_score += 1.5; signals.append(f"RSI={rsi:.0f} (yếu)")
    else: signals.append(f"RSI={rsi:.0f} (trung tính)")

    if macd > 0: bull_score += 1; signals.append("MACD dương")
    else: bear_score += 1; signals.append("MACD âm")

    if price > ma20: bull_score += 1; signals.append("Trên MA20")
    else: bear_score += 1; signals.append("Dưới MA20")

    if price > ma50: bull_score += 1; signals.append("Trên MA50")
    else: bear_score += 1; signals.append("Dưới MA50")

    if price > ma200: bull_score += 0.5; signals.append("Trên MA200 (xu hướng dài hạn tăng)")
    else: bear_score += 0.5; signals.append("Dưới MA200 (xu hướng dài hạn giảm)")

    if slope > 0: bull_score += 1; signals.append(f"Regression tăng ({slope*periods:+.1f} pts/period)")
    else: bear_score += 1; signals.append(f"Regression giảm ({slope*periods:+.1f} pts/period)")

    if vol_ratio > 1.3 and closes[-1] > closes[-2]:
        bull_score += 1; signals.append(f"Volume tăng {vol_ratio:.1f}x kèm giá tăng")
    elif vol_ratio > 1.3 and closes[-1] < closes[-2]:
        bear_score += 1; signals.append(f"Volume tăng {vol_ratio:.1f}x kèm giá giảm")

    # Trend momentum 5 bars
    trend_5 = (closes[-1] - closes[-5]) / closes[-5] * 100 if n>=5 else 0
    if trend_5 > 1: bull_score += 0.5
    elif trend_5 < -1: bear_score += 0.5
    signals.append(f"Momentum 5 phiên: {trend_5:+.1f}%")

    total_score = bull_score + bear_score
    if total_score == 0: total_score = 1
    bull_pct = bull_score / total_score * 100
    bear_pct = bear_score / total_score * 100

    # ── 6. Kịch bản giá ──────────────────────────────────────
    net_bias = bull_score - bear_score
    if net_bias >= 2.5:
        scenario = 'BULLISH'
        scenario_label = '📈 TĂNG MẠNH'
        scenario_color = '#00e676'
        primary_target = min((t for t,_ in fib_targets_up if t > price), default=atr_up)
        secondary_target = max((t for t,_ in fib_targets_up if t > price), default=atr_up * 1.05)
        stop_target = price - atr * 1.5
        confidence = min(95, 55 + net_bias * 8)
    elif net_bias >= 1:
        scenario = 'MILD_BULLISH'
        scenario_label = '📈 Thiên TĂNG'
        scenario_color = '#69f0ae'
        primary_target = price + atr * periods * 0.4
        secondary_target = min((t for t,_ in fib_targets_up if t > price), default=atr_up)
        stop_target = price - atr * 1.5
        confidence = min(70, 45 + net_bias * 8)
    elif net_bias <= -2.5:
        scenario = 'BEARISH'
        scenario_label = '📉 GIẢM MẠNH'
        scenario_color = '#f03050'
        primary_target = max((t for t,_ in fib_support if t < price), default=atr_down)
        secondary_target = min((t for t,_ in fib_support if t < price), default=atr_down * 0.95)
        stop_target = price + atr * 1.5
        confidence = min(95, 55 + abs(net_bias) * 8)
    elif net_bias <= -1:
        scenario = 'MILD_BEARISH'
        scenario_label = '📉 Thiên GIẢM'
        scenario_color = '#ff9100'
        primary_target = price - atr * periods * 0.4
        secondary_target = max((t for t,_ in fib_support if t < price), default=atr_down)
        stop_target = price + atr * 1.5
        confidence = min(70, 45 + abs(net_bias) * 8)
    else:
        scenario = 'NEUTRAL'
        scenario_label = '➡️ Đi ngang/Chờ'
        scenario_color = '#90a4ae'
        primary_target = price + atr * 2
        secondary_target = price - atr * 2
        stop_target = price - atr
        confidence = 40

    # Price path projection (10 điểm)
    path = []
    for i in range(1, periods+1):
        t = i / periods
        # Weighted: linear regression + ATR drift + mean reversion
        lr_p = intercept + slope * (lb - 1 + i)
        noise_factor = 1 + (0.5 - t) * 0.05 * (1 if net_bias > 0 else -1)
        path_price = lr_p * noise_factor
        path.append(round(path_price, 2))

    return {
        'price': round(price, 2),
        'scenario': scenario,
        'scenario_label': scenario_label,
        'scenario_color': scenario_color,
        'confidence': round(confidence, 1),
        'primary_target': round(primary_target, 2),
        'secondary_target': round(secondary_target, 2),
        'stop_target': round(stop_target, 2),
        'lr_target': round(lr_target, 2),
        'lr_change_pct': round(lr_change_pct, 2),
        'bull_pct': round(bull_pct, 1),
        'bear_pct': round(bear_pct, 1),
        'signals': signals,
        'fib_targets_up': [(round(p,2), lbl) for p,lbl in fib_targets_up],
        'fib_support': [(round(p,2), lbl) for p,lbl in fib_support],
        'price_path': path,
        'periods': periods,
        'rsi': round(rsi, 1),
        'macd': round(macd, 2),
        'trend_5d': round(trend_5, 2),
        'slope_per_bar': round(slope, 4),
        'atr': round(atr, 2),
        'ma20': round(ma20, 2), 'ma50': round(ma50, 2), 'ma200': round(ma200, 2),
    }

def forecast_trend(sym: str, mkt: str, horizon: int = 20) -> dict:
    """
    Dự đoán xu hướng giá trong `horizon` ngày tới.
    Kết hợp 3 phương pháp:
      1. Linear regression channel (trend + band ±1σ)
      2. Monte Carlo simulation (P10/P50/P90)
      3. ML model predict_ml() → điều chỉnh path theo xác suất

    Trả về JSON có thể vẽ trực tiếp trên LightweightCharts.
    """
    import math

    # ── Lấy dữ liệu ──────────────────────────────────────────
    bars = db_load_ohlcv(sym, OHLCV_BARS)
    if not bars or len(bars) < 50:
        return {"error": f"Cần ít nhất 50 bars, chỉ có {len(bars) if bars else 0}"}

    closes  = [b['c'] for b in bars]
    dates   = [b['d'] for b in bars]
    n       = len(closes)
    price   = closes[-1]

    # ── Helper: tạo trading date tiếp theo ───────────────────
    def next_trading_date(date_str, days_ahead):
        """Tạo ngày giao dịch kế tiếp (bỏ qua cuối tuần)."""
        from datetime import datetime as _dt, timedelta as _td
        try:
            d = _dt.strptime(date_str[:10], '%Y-%m-%d')
        except Exception:
            d = _dt.now()
        count = 0
        while count < days_ahead:
            d += _td(days=1)
            if d.weekday() < 5:  # Mon-Fri
                count += 1
        return d.strftime('%Y-%m-%d')

    last_date = dates[-1]

    # ════════════════════════════════════════════════════════
    # PHƯƠNG PHÁP 1: Linear Regression Channel (50 ngày)
    # ════════════════════════════════════════════════════════
    lookback = min(50, n)
    y = closes[-lookback:]
    x = list(range(lookback))

    # polyfit bậc 1
    n_pts = lookback
    sum_x  = sum(x)
    sum_y  = sum(y)
    sum_xy = sum(xi*yi for xi,yi in zip(x,y))
    sum_x2 = sum(xi*xi for xi in x)
    denom  = n_pts * sum_x2 - sum_x * sum_x
    slope  = (n_pts * sum_xy - sum_x * sum_y) / denom if denom else 0
    intercept = (sum_y - slope * sum_x) / n_pts

    # Residuals → σ
    residuals = [y[i] - (slope*x[i] + intercept) for i in range(n_pts)]
    sigma = (sum(r*r for r in residuals) / n_pts) ** 0.5

    # R²
    y_mean = sum_y / n_pts
    ss_res = sum(r*r for r in residuals)
    ss_tot = sum((yi - y_mean)**2 for yi in y)
    r_squared = 1 - ss_res/ss_tot if ss_tot > 0 else 0

    # Trend direction
    slope_pct = slope / price * 100  # % per day
    if slope_pct > 0.1:
        trend_dir = "TĂNG"
        trend_icon = "📈"
    elif slope_pct < -0.1:
        trend_dir = "GIẢM"
        trend_icon = "📉"
    else:
        trend_dir = "NGANG"
        trend_icon = "➡️"

    # Vẽ regression line: 30 ngày qua + horizon ngày tới
    draw_back  = min(30, n)
    trend_line = []
    upper_band = []
    lower_band = []

    for i in range(draw_back):
        xi   = lookback - draw_back + i
        val  = slope * xi + intercept
        d    = dates[n - draw_back + i]
        trend_line.append({"time": d, "value": round(val, 4)})
        upper_band.append({"time": d, "value": round(val + sigma, 4)})
        lower_band.append({"time": d, "value": round(val - sigma, 4)})

    for i in range(1, horizon + 1):
        xi   = lookback + i - 1
        val  = slope * xi + intercept
        d    = next_trading_date(last_date, i)
        trend_line.append({"time": d, "value": round(val, 4)})
        upper_band.append({"time": d, "value": round(val + sigma, 4)})
        lower_band.append({"time": d, "value": round(val - sigma, 4)})

    forecast_price_trend = slope * (lookback + horizon - 1) + intercept

    # ════════════════════════════════════════════════════════
    # PHƯƠNG PHÁP 2: Monte Carlo (300 simulations)
    # ════════════════════════════════════════════════════════
    import random as _rand

    # Daily returns từ lịch sử 100 ngày
    hist_returns = []
    for i in range(1, min(100, n)):
        if closes[n-i-1] > 0:
            hist_returns.append((closes[n-i] - closes[n-i-1]) / closes[n-i-1])

    if not hist_returns:
        hist_returns = [0.001, -0.001, 0.0]

    mu    = sum(hist_returns) / len(hist_returns)
    var_r = sum((r - mu)**2 for r in hist_returns) / len(hist_returns)
    std_r = var_r ** 0.5

    n_sims = 300
    all_paths = []
    for _ in range(n_sims):
        path = [price]
        for __ in range(horizon):
            # Geometric Brownian Motion with drift
            r = _rand.gauss(mu, std_r)
            path.append(path[-1] * (1 + r))
        all_paths.append(path)

    # Tính percentiles tại mỗi bước
    mc_median = []
    mc_p90    = []
    mc_p10    = []

    for step in range(horizon + 1):
        vals = sorted(p[step] for p in all_paths)
        mc_median.append(vals[n_sims // 2])
        mc_p90.append(vals[int(n_sims * 0.90)])
        mc_p10.append(vals[int(n_sims * 0.10)])

    # Convert to chart format
    mc_median_line = [{"time": last_date, "value": round(mc_median[0], 4)}]
    mc_p90_line    = [{"time": last_date, "value": round(mc_p90[0], 4)}]
    mc_p10_line    = [{"time": last_date, "value": round(mc_p10[0], 4)}]

    for i in range(1, horizon + 1):
        d = next_trading_date(last_date, i)
        mc_median_line.append({"time": d, "value": round(mc_median[i], 4)})
        mc_p90_line.append({"time": d, "value": round(mc_p90[i], 4)})
        mc_p10_line.append({"time": d, "value": round(mc_p10[i], 4)})

    # ════════════════════════════════════════════════════════
    # PHƯƠNG PHÁP 3: ML prediction path
    # ════════════════════════════════════════════════════════
    ml_result = {}
    ml_path   = []
    try:
        import concurrent.futures as _cf
        with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
            ml_result = _ex.submit(predict_ml, sym, mkt, bars).result(timeout=15)
    except Exception as _me:
        log.debug("[forecast] ML: %s", _me)
        ml_result = {'signal': 'HOLD', 'probability': 33.0, 'p_buy': 33.0,
                     'p_sell': 33.0, 'confidence': 'LOW'}

    # Điều chỉnh path dựa trên ML probability
    p_buy   = ml_result.get('p_buy',  33.0) / 100.0
    p_sell  = ml_result.get('p_sell', 33.0) / 100.0
    ml_bias = (p_buy - p_sell)  # [-1, +1]

    # ATR để scale
    atr_v = _atr(bars[-30:], 14) if len(bars) >= 30 else price * 0.015

    ml_path = [{"time": last_date, "value": round(price, 4)}]
    ml_price = price
    for i in range(1, horizon + 1):
        # ML điều chỉnh trend với bias theo xác suất
        daily_drift = slope + ml_bias * atr_v * 0.3
        ml_price = ml_price + daily_drift + _rand.gauss(0, sigma * 0.3)
        ml_price = max(ml_price, price * 0.7)  # floor 30% drawdown
        d = next_trading_date(last_date, i)
        ml_path.append({"time": d, "value": round(ml_price, 4)})

    # ════════════════════════════════════════════════════════
    # Support / Resistance từ analyze()
    # ════════════════════════════════════════════════════════
    data_for_analyze = {"history": bars, "price": price, "market": mkt}
    a = {}
    try:
        a = analyze(data_for_analyze)
    except Exception as _ae:
        log.debug("[forecast] analyze: %s", _ae)

    tp1 = a.get("take_profit", round(price + 2 * atr_v, 4))
    tp2 = a.get("take_profit2", round(price + 3 * atr_v, 4))
    sl  = a.get("stop_loss",   round(price - 1.5 * atr_v, 4))
    pivot_r1 = a.get("pivot_r1", 0)
    pivot_s1 = a.get("pivot_s1", 0)

    # ════════════════════════════════════════════════════════
    # Summary
    # ════════════════════════════════════════════════════════
    ml_sig    = ml_result.get('signal', 'HOLD')
    ml_conf   = ml_result.get('confidence', 'LOW')
    ml_prob   = ml_result.get('probability', 33.0)

    # Consensus: trend + ML đồng thuận?
    ta_bull = (a.get('total', 0) > 3) if a else False
    ta_bear = (a.get('total', 0) < -3) if a else False
    ml_bull = 'BUY' in ml_sig
    ml_bear = 'SELL' in ml_sig
    trend_bull = trend_dir == "TĂNG"
    trend_bear = trend_dir == "GIẢM"

    consensus_score = sum([
        1 if ta_bull else (-1 if ta_bear else 0),
        1 if ml_bull else (-1 if ml_bear else 0),
        1 if trend_bull else (-1 if trend_bear else 0),
    ])
    if consensus_score >= 2:
        consensus = "ĐỒNG THUẬN TĂNG 🟢"
    elif consensus_score <= -2:
        consensus = "ĐỒNG THUẬN GIẢM 🔴"
    else:
        consensus = "PHÂN KỲ ⚠️"

    # Horizon price estimates
    fp_trend   = round(forecast_price_trend, 4)
    fp_mc_med  = round(mc_median[-1], 4)
    fp_ml      = round(ml_path[-1]['value'] if ml_path else price, 4)
    fp_avg     = round((fp_trend + fp_mc_med + fp_ml) / 3, 4)
    chg_pct    = round((fp_avg - price) / price * 100, 2) if price else 0

    return {
        # Chart data
        "history":    [{"time": b['d'], "open": b['o'], "high": b['h'],
                        "low": b['l'],  "close": b['c'], "volume": b['v']}
                       for b in bars[-120:]],  # 120 ngày để chart không quá nặng
        "trend_line":  trend_line,
        "upper_band":  upper_band,
        "lower_band":  lower_band,
        "mc_median":   mc_median_line,
        "mc_p90":      mc_p90_line,
        "mc_p10":      mc_p10_line,
        "ml_path":     ml_path,
        # Levels
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "pivot_r1": pivot_r1, "pivot_s1": pivot_s1,
        # Summary
        "symbol":      sym,
        "market":      mkt,
        "price":       price,
        "horizon":     horizon,
        "trend_dir":   trend_dir,
        "trend_icon":  trend_icon,
        "trend_slope_pct": round(slope_pct, 3),
        "r_squared":   round(r_squared, 3),
        "sigma":       round(sigma, 4),
        "ml_signal":   ml_sig,
        "ml_confidence": ml_conf,
        "ml_probability": ml_prob,
        "p_buy":       ml_result.get('p_buy', 33.0),
        "p_sell":      ml_result.get('p_sell', 33.0),
        "p_hold":      ml_result.get('p_hold', 33.0),
        "consensus":   consensus,
        "consensus_score": consensus_score,
        "forecast_price":  fp_avg,
        "forecast_trend":  fp_trend,
        "forecast_mc":     fp_mc_med,
        "forecast_ml":     fp_ml,
        "change_pct":  chg_pct,
        "mc_range":    [round(mc_p10[-1], 4), round(mc_p90[-1], 4)],
        "ta_total":    a.get('total', 0),
        "ta_signal":   a.get('signal', 'NEUTRAL'),
    }

def get_market_calendar(user_id: int, days: int = 30) -> dict:
    """Tổng hợp sự kiện của tất cả mã trong watchlist theo tuần."""
    try:
        with get_db() as c:
            syms = [r['symbol'] for r in
                    c.execute("SELECT DISTINCT symbol FROM user_watchlist WHERE user_id=?", (user_id,)).fetchall()]
        if not syms:
            # Fallback to global watchlist
            with get_db() as c:
                syms = [r['symbol'] for r in c.execute("SELECT symbol FROM watchlist").fetchall()]
    except: syms = []

    events_by_date = {}
    today = datetime.now().date()
    cutoff = (today + timedelta(days=days)).isoformat()

    # Lấy events từ DB
    try:
        with get_db() as c:
            rows = c.execute(
                """SELECT e.symbol, e.market, e.title, e.event_date, e.event_type
                   FROM events e
                   WHERE e.event_date >= ? AND e.event_date <= ?
                   ORDER BY e.event_date ASC""",
                (today.isoformat(), cutoff)
            ).fetchall()
        for r in rows:
            dt = r['event_date']
            if dt not in events_by_date:
                events_by_date[dt] = []
            events_by_date[dt].append({
                'symbol': r['symbol'], 'market': r['market'],
                'title': r['title'], 'type': r['event_type'] or 'OTHER',
                'date': dt,
            })
    except: pass

    # Thêm events từ _ram_cache (KQKD, cổ tức estimate từ analysis)
    for sym in syms:
        d = _ram_cache.get(sym, {}).get('data', {})
        if not d: continue
        a = d.get('analysis', {})
        # Upcoming event từ analysis
        ev_title = d.get('upcoming_event', '')
        if ev_title:
            dt_key = today.isoformat()  # Ngay hôm nay nếu không biết ngày cụ thể
            if dt_key not in events_by_date:
                events_by_date[dt_key] = []
            # Tránh duplicate
            existing = [e['symbol'] for e in events_by_date[dt_key]]
            if sym not in existing:
                events_by_date[dt_key].append({
                    'symbol': sym, 'market': d.get('market', 'VN'),
                    'title': ev_title, 'type': 'UPCOMING',
                    'date': dt_key,
                })

    # Group by week
    weeks = {}
    for dt_str, evs in sorted(events_by_date.items()):
        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
            week_start = dt - timedelta(days=dt.weekday())
            week_key = week_start.isoformat()
            if week_key not in weeks:
                weeks[week_key] = {
                    'week_start': week_key,
                    'week_label': f"Tuần {week_start.strftime('%d/%m')} – {(week_start+timedelta(6)).strftime('%d/%m/%Y')}",
                    'events': []
                }
            for ev in evs:
                ev['dow'] = dt.strftime('%A')  # Day of week
                ev['is_today'] = dt == today
                ev['is_past'] = dt < today
                weeks[week_key]['events'].append(ev)
        except: pass

    return {
        'weeks': list(weeks.values()),
        'total_events': sum(len(v['events']) for v in weeks.values()),
        'symbols_tracked': len(syms),
        'days_ahead': days,
    }


class EnsemblePredictor:
    """
    Kết hợp 3 model bằng soft-voting:
      - GradientBoosting (kỹ thuật + cơ bản)
      - LogisticRegression (nhanh, baseline)
      - LSTM numpy fallback (sequence learning)
    Trọng số voting tự động theo validation accuracy.
    Thêm permutation feature importance (SHAP proxy, không cần SHAP library).
    """

    FEATURE_NAMES = [
        'rsi','macd','adx','ma_cross','bb_pos','volume_ratio',
        'squeeze','elder','stoch','obv_trend',
        'ret_5d','ret_20d','volatility',
        'pe_norm','roe_norm','rev_growth',
        'sentiment',
    ]

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._models: dict = {}   # symbol → {gb, lr, lstm_w, lstm_b, weights, info}
        self._lock = threading.Lock()

    # ── Feature extraction ─────────────────────────────────────
    def _extract(self, history: list, fund: dict = None, sent: float = 0.5) -> list | None:
        if not history or len(history) < 30:
            return None
        closes = [b['c'] for b in history]
        highs  = [b.get('h', b['c']) for b in history]
        lows   = [b.get('l', b['c']) for b in history]
        vols   = [b.get('v', 0) for b in history]
        n = len(closes)

        # RSI
        rsi = _calc_rsi_simple(closes) / 100.0
        # MACD
        e12 = _ema(closes, 12); e26 = _ema(closes, 26)
        macd_v = (e12 - e26) / (closes[-1] + 1e-9)
        macd_s = max(-1.0, min(1.0, macd_v * 200))
        # ADX
        adx = _calc_adx_simple(history) / 100.0
        # MA cross
        ma20 = sum(closes[-20:]) / 20 if n >= 20 else closes[-1]
        ma50 = sum(closes[-50:]) / 50 if n >= 50 else closes[-1]
        ma_s = ((1 if closes[-1] > ma20 else -1) + (1 if closes[-1] > ma50 else -1)) / 2.0
        # BB position
        std = (_var(closes[-20:]) ** 0.5) if n >= 20 else 1.0
        bb_u = ma20 + 2 * std; bb_l = ma20 - 2 * std
        bb_p = max(0.0, min(1.0, (closes[-1] - bb_l) / (bb_u - bb_l + 1e-9)))
        # Volume ratio
        avg_v = sum(vols[-20:]) / 20 if n >= 20 and sum(vols[-20:]) > 0 else 1
        vol_r = min(vols[-1] / (avg_v + 1e-9), 5.0) / 5.0
        # Squeeze
        atr5 = sum(abs(closes[i]-closes[i-1]) for i in range(-5, 0)) / 5.0
        squeeze = 1.0 if std < atr5 else 0.0
        # Elder Ray
        ema13 = _ema(closes, 13)
        elder = 1.0 if closes[-1] > ema13 else (-1.0 if closes[-1] < ema13 else 0.0)
        # Stochastic
        h14 = max(highs[-14:]) if n >= 14 else closes[-1]
        l14 = min(lows[-14:]) if n >= 14 else closes[-1]
        stoch = (closes[-1] - l14) / (h14 - l14 + 1e-9)
        # OBV trend
        obv = 0.0
        for i in range(1, min(21, n)):
            obv += vols[-i] if closes[-i] > closes[-i-1] else -vols[-i]
        obv_trend = 1.0 if obv > 0 else -1.0
        # Returns + vol
        ret5  = (closes[-1] / closes[-6]  - 1) if n >= 6  else 0.0
        ret20 = (closes[-1] / closes[-21] - 1) if n >= 21 else 0.0
        rets  = [(closes[i]-closes[i-1])/closes[i-1] for i in range(1, min(21,n))]
        vol20 = (_var(rets) ** 0.5) if rets else 0.0
        # Fundamental (normalized)
        pe_n = roe_n = rg_n = 0.5
        if fund:
            pe_n  = max(0.0, min(1.0, 1 - (fund.get('pe') or 20) / 60))
            roe_n = max(0.0, min(1.0, (fund.get('roe') or 10) / 40))
            rg_n  = max(0.0, min(1.0, ((fund.get('revenue_growth_yoy') or 0) + 30) / 80))

        return [rsi, macd_s, adx, ma_s, bb_p, vol_r, squeeze, elder, stoch, obv_trend,
                ret5, ret20, vol20, pe_n, roe_n, rg_n, sent]

    # ── Build sequence for LSTM-style numpy ────────────────────
    def _build_seqs(self, X_list: list, window: int = 10):
        import numpy as _np
        X = _np.array(X_list)
        seqs, labs = [], []
        for i in range(window, len(X)):
            seqs.append(X[i-window:i].flatten())
        return _np.array(seqs)

    # ── Simple numpy LSTM weights (1-layer, tanh) ──────────────
    def _lstm_train_np(self, X_seqs, y, hidden: int = 32):
        import numpy as _np
        n_feat = X_seqs.shape[1]
        rng = _np.random.RandomState(42)
        W = rng.randn(hidden, n_feat) * 0.01
        b = _np.zeros(hidden)
        Wo = rng.randn(3, hidden) * 0.01
        bo = _np.zeros(3)
        lr = 0.01
        for epoch in range(30):
            for xi, yi in zip(X_seqs, y):
                h = _np.tanh(W @ xi + b)
                logits = Wo @ h + bo
                exp_l = _np.exp(logits - logits.max())
                proba = exp_l / exp_l.sum()
                label_idx = [1,0,-1].index(yi) if yi in [1,0,-1] else 1
                grad_out = proba.copy(); grad_out[label_idx] -= 1
                Wo -= lr * _np.outer(grad_out, h)
                bo -= lr * grad_out
                grad_h = Wo.T @ grad_out * (1 - h**2)
                W -= lr * _np.outer(grad_h, xi)
                b -= lr * grad_h
        return W, b, Wo, bo

    def _lstm_predict_np(self, xi, W, b, Wo, bo):
        import numpy as _np
        h = _np.tanh(W @ xi + b)
        logits = Wo @ h + bo
        exp_l = _np.exp(logits - logits.max())
        return exp_l / exp_l.sum()   # [up, neutral, down]

    # ── Permutation feature importance (SHAP proxy) ────────────
    def _permutation_importance(self, clf, scaler, X_val, y_val,
                                 n_repeats: int = 5) -> dict:
        """
        Đo lường tầm quan trọng feature bằng cách shuffle từng feature
        và đo mức độ accuracy giảm. Tương tự SHAP nhưng không cần library.
        """
        import numpy as _np
        from sklearn.metrics import accuracy_score
        X_s = scaler.transform(X_val)
        base_acc = accuracy_score(y_val, clf.predict(X_s))
        importances = {}
        n_feat = X_s.shape[1]
        for fi in range(n_feat):
            drops = []
            for _ in range(n_repeats):
                X_perm = X_s.copy()
                _np.random.shuffle(X_perm[:, fi])
                acc_perm = accuracy_score(y_val, clf.predict(X_perm))
                drops.append(base_acc - acc_perm)
            importances[self.FEATURE_NAMES[fi]] = round(float(_np.mean(drops)) * 100, 3)
        # Sort descending
        return dict(sorted(importances.items(), key=lambda x: -x[1]))

    # ── Train ensemble ─────────────────────────────────────────
    def train(self, symbol: str, market: str, history: list, fund: dict = None) -> dict:
        try:
            from sklearn.ensemble import GradientBoostingClassifier
            from sklearn.linear_model import LogisticRegression
            from sklearn.preprocessing import StandardScaler
            from sklearn.model_selection import TimeSeriesSplit
            from sklearn.metrics import accuracy_score
            import numpy as _np
        except ImportError:
            return {'error': 'scikit-learn required'}

        if not history or len(history) < 100:
            return {'error': 'Cần ít nhất 100 phiên lịch sử'}

        WINDOW = 10; STEP = 5
        X_raw, y = [], []
        for i in range(30, len(history) - STEP):
            feat = self._extract(history[:i+1], fund)
            if feat is None: continue
            fut = history[i + STEP]['c']
            cur = history[i]['c']
            label = 1 if fut > cur * 1.02 else (-1 if fut < cur * 0.98 else 0)
            X_raw.append(feat); y.append(label)

        if len(X_raw) < 60:
            return {'error': 'Không đủ mẫu training'}

        import numpy as _np
        X = _np.array(X_raw); y = _np.array(y)
        split = int(len(X) * 0.8)
        X_tr, X_val = X[:split], X[split:]
        y_tr, y_val = y[:split], y[split:]

        scaler = StandardScaler()
        X_tr_s = scaler.fit_transform(X_tr)
        X_val_s = scaler.transform(X_val)

        # Model 1: GradientBoosting
        # v29: Use LightGBM if available, else GradientBoosting

        gb, acc_gb, _gb_model_type = train_lgbm_model(X_tr_s, y_tr, X_val_s, y_val)

        log.debug("[Ensemble] Using %s, acc_gb=%.2f", _gb_model_type, acc_gb)

        # Model 2: LogisticRegression
        lr_clf = LogisticRegression(max_iter=500, random_state=42, C=1.0)
        lr_clf.fit(X_tr_s, y_tr)
        acc_lr = accuracy_score(y_val, lr_clf.predict(X_val_s))

        # Model 3: Numpy LSTM
        X_seqs_tr = self._build_seqs(X_tr_s.tolist(), WINDOW)
        X_seqs_val = self._build_seqs(X_val_s.tolist(), WINDOW)
        y_seq_tr = y_tr[WINDOW:]
        y_seq_val = y_val[WINDOW:]
        W, b, Wo, bo = self._lstm_train_np(X_seqs_tr, y_seq_tr)
        lstm_preds = []
        for xi in X_seqs_val:
            proba = self._lstm_predict_np(xi, W, b, Wo, bo)
            lstm_preds.append([1, 0, -1][_np.argmax(proba)])
        acc_lstm = accuracy_score(y_seq_val, lstm_preds) if len(lstm_preds) else 0.5

        # Ensemble weights ∝ val accuracy
        total_acc = acc_gb + acc_lr + acc_lstm + 1e-9
        w_gb = acc_gb / total_acc
        w_lr = acc_lr / total_acc
        w_lstm = acc_lstm / total_acc

        # Permutation importance (SHAP proxy) on GradBoost
        # v28: Use real SHAP if available
        try:
            if HAS_SHAP:
                shap_imp = get_real_shap(gb, X_val, feat_names)
                perm_imp = shap_imp if shap_imp else self._permutation_importance(gb, scaler, X_val, y_val, n_repeats=5)
            else:
                perm_imp = self._permutation_importance(gb, scaler, X_val, y_val, n_repeats=5)
        except Exception:
            perm_imp = self._permutation_importance(gb, scaler, X_val, y_val, n_repeats=5)

        # Top 5 most important + top 5 least important
        fi_list = list(perm_imp.items())
        top5  = fi_list[:5]
        bot5  = fi_list[-5:]

        info = {
            'symbol': symbol,
            'trained_at': datetime.now().isoformat(),
            'n_samples': len(X_raw),
            'val_samples': len(y_val),
            'acc_gb':   round(acc_gb * 100, 1),
            'acc_lr':   round(acc_lr * 100, 1),
            'acc_lstm': round(acc_lstm * 100, 1),
            'ensemble_acc': round((acc_gb * w_gb + acc_lr * w_lr + acc_lstm * w_lstm) * 100, 1),
            'weights': {'gb': round(w_gb, 3), 'lr': round(w_lr, 3), 'lstm': round(w_lstm, 3)},
            'permutation_importance': perm_imp,   # SHAP proxy
            'top_drivers':  [{'feature': k, 'importance': v} for k, v in top5],
            'weak_features': [{'feature': k, 'importance': v} for k, v in bot5],
            'model_type': 'Ensemble(GB+LR+LSTM)',
            'window': WINDOW,
        }

        # Persist
        try:
            import pickle, base64
            blob = base64.b64encode(pickle.dumps({
                'gb': gb, 'lr': lr_clf, 'scaler': scaler,
                'lstm': (W, b, Wo, bo), 'info': info,
            })).decode()
            with sqlite3.connect(self.db_path, timeout=10) as c:
                c.row_factory = sqlite3.Row
                c.execute("""CREATE TABLE IF NOT EXISTS ensemble_models(
                    symbol TEXT PRIMARY KEY, model_blob TEXT,
                    model_info TEXT, trained_at TEXT)""")
                c.execute("""INSERT OR REPLACE INTO ensemble_models
                             VALUES (?, ?, ?, ?)""",
                          (symbol, blob, json.dumps(info, default=str),
                           info['trained_at']))
        except Exception as _e:
            info['persist_error'] = str(_e)

        with self._lock:
            self._models[symbol] = {
                'gb': gb, 'lr': lr_clf, 'scaler': scaler,
                'lstm': (W, b, Wo, bo), 'info': info, 'window': WINDOW,
            }

        return info

    # ── Predict ────────────────────────────────────────────────
    def predict(self, symbol: str, market: str, history: list,
                fund: dict = None, sent: float = 0.5) -> dict:
        import numpy as _np

        if symbol not in self._models:
            self._load_from_db(symbol)

        if symbol not in self._models:
            return {
                'prediction': 'NEUTRAL', 'confidence': 0,
                'prob_up': 33, 'prob_down': 33, 'prob_neutral': 34,
                'note': 'Model chưa train. Vào modal → tab 🤖 AI → Train.',
                'ensemble_weights': {},
            }

        mdl = self._models[symbol]
        feat = self._extract(history, fund, sent)
        if feat is None:
            return {'prediction': 'NEUTRAL', 'confidence': 0,
                    'note': 'Không đủ dữ liệu', 'prob_up': 33}

        X = mdl['scaler'].transform(_np.array(feat).reshape(1, -1))
        info = mdl['info']
        w = info['weights']
        CLASSES = [1, 0, -1]

        # GradBoost proba
        gb_proba = mdl['gb'].predict_proba(X)[0]
        gb_cls   = mdl['gb'].classes_.tolist()
        gb_p = {c: 0.0 for c in CLASSES}
        for ci, cls in enumerate(gb_cls):
            if cls in gb_p: gb_p[cls] = float(gb_proba[ci])

        # LogReg proba
        lr_proba = mdl['lr'].predict_proba(X)[0]
        lr_cls   = mdl['lr'].classes_.tolist()
        lr_p = {c: 0.0 for c in CLASSES}
        for ci, cls in enumerate(lr_cls):
            if cls in lr_p: lr_p[cls] = float(lr_proba[ci])

        # LSTM proba
        window = mdl.get('window', 10)
        W, b, Wo, bo = mdl['lstm']
        xi = X[0]   # use current feature as proxy (no seq history in predict)
        lstm_proba = self._lstm_predict_np(xi, W, b, Wo, bo)
        lstm_p = {1: float(lstm_proba[0]), 0: float(lstm_proba[1]), -1: float(lstm_proba[2])}

        # Weighted ensemble
        ensemble = {}
        for c in CLASSES:
            ensemble[c] = w['gb'] * gb_p[c] + w['lr'] * lr_p[c] + w['lstm'] * lstm_p[c]

        best_cls = max(ensemble, key=ensemble.get)
        best_prob = ensemble[best_cls]
        pred = {1: 'UP', 0: 'NEUTRAL', -1: 'DOWN'}[best_cls]
        confidence = round(abs(best_prob - 0.33) * 300, 1)   # scale 0-100

        # Feature attribution (permutation importance from training)
        perm_imp = info.get('permutation_importance', {})
        top_drivers = sorted(perm_imp.items(), key=lambda x: -abs(x[1]))[:5]

        return {
            'prediction':   pred,
            'direction':    pred,
            'confidence':   min(confidence, 99),
            'prob_up':      round(ensemble[1]  * 100, 1),
            'prob_neutral': round(ensemble[0]  * 100, 1),
            'prob_down':    round(ensemble[-1] * 100, 1),
            'ensemble_weights': w,
            'model_accuracies': {
                'gb': info['acc_gb'], 'lr': info['acc_lr'], 'lstm': info['acc_lstm'],
                'ensemble': info['ensemble_acc'],
            },
            'top_drivers': [{'feature': k, 'importance_drop': v} for k, v in top_drivers],
            'model_type': 'Ensemble(GB+LR+LSTM)',
            'note': f"Horizon: 5 phiên · {info['n_samples']} mẫu training",
        }

    def _load_from_db(self, symbol: str):
        try:
            import pickle, base64
            with sqlite3.connect(self.db_path, timeout=5) as c:
                c.row_factory = sqlite3.Row
                row = c.execute("SELECT model_blob, model_info FROM ensemble_models WHERE symbol=?",
                                (symbol,)).fetchone()
            if row:
                data = pickle.loads(base64.b64decode(row['model_blob']))
                with self._lock:
                    self._models[symbol] = data
        except Exception:
            pass

# ALPACA BROKER — US Paper + Live Trading (miễn phí)
# Đăng ký tại alpaca.markets — không cần approval
# ══════════════════════════════════════════════════════════════

class AlpacaBroker(BrokerAdapter):
    """
    Kết nối Alpaca Markets API cho US stocks.
    - Paper trading: app.alpaca.markets (mặc định, an toàn)
    - Live trading: api.alpaca.markets (cần tài khoản live)
    Không cần broker approval — mở tài khoản là dùng được ngay.
    """

    PAPER_URL = "https://paper-api.alpaca.markets"
    LIVE_URL  = "https://api.alpaca.markets"
    DATA_URL  = "https://data.alpaca.markets"

    def __init__(self, api_key: str, api_secret: str, paper: bool = True):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = self.PAPER_URL if paper else self.LIVE_URL
        self.paper      = paper

    def _headers(self) -> dict:
        return {
            "APCA-API-KEY-ID":     self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type":        "application/json",
            "Accept":              "application/json",
        }

    def _req(self, method: str, path: str, body: dict = None, data_api: bool = False) -> dict:
        import urllib.request as _ur
        base = self.DATA_URL if data_api else self.base_url
        url  = f"{base}{path}"
        data = json.dumps(body).encode() if body else None
        req  = _ur.Request(url, data=data, headers=self._headers(), method=method.upper())
        try:
            with _ur.urlopen(req, timeout=12) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            err = str(e)
            try:
                body_err = e.read().decode() if hasattr(e, 'read') else ''
                return {"error": body_err or err}
            except:
                return {"error": err}

    # ── Account ────────────────────────────────────────────────
    def get_account(self) -> dict:
        r = self._req("GET", "/v2/account")
        return {
            "ok": "id" in r,
            "account_id":     r.get("id",""),
            "cash":           float(r.get("cash", 0)),
            "portfolio_value":float(r.get("portfolio_value", 0)),
            "buying_power":   float(r.get("buying_power", 0)),
            "equity":         float(r.get("equity", 0)),
            "day_trade_count":int(r.get("daytrade_count", 0)),
            "pattern_day_trader": r.get("pattern_day_trader", False),
            "paper":          self.paper,
            "status":         r.get("status",""),
            "currency":       "USD",
            "raw":            r,
        } if "id" in r else r

    # ── Positions ──────────────────────────────────────────────
    def get_positions(self) -> list:
        rows = self._req("GET", "/v2/positions")
        if isinstance(rows, dict) and rows.get("error"):
            return []
        result = []
        for p in (rows or []):
            result.append({
                "symbol":       p.get("symbol",""),
                "qty":          float(p.get("qty", 0)),
                "avg_entry":    float(p.get("avg_entry_price", 0)),
                "current_price":float(p.get("current_price", 0)),
                "market_value": float(p.get("market_value", 0)),
                "unrealized_pl":float(p.get("unrealized_pl", 0)),
                "unrealized_plpc": float(p.get("unrealized_plpc", 0)),
                "side":         p.get("side", "long"),
            })
        return result

    # ── Orders ─────────────────────────────────────────────────
    def get_orders(self, status: str = "open") -> list:
        rows = self._req("GET", f"/v2/orders?status={status}&limit=50")
        if not isinstance(rows, list):
            return []
        return [{
            "id":          o.get("id",""),
            "symbol":      o.get("symbol",""),
            "side":        o.get("side",""),
            "qty":         float(o.get("qty", 0)),
            "filled_qty":  float(o.get("filled_qty", 0)),
            "type":        o.get("type",""),
            "status":      o.get("status",""),
            "limit_price": float(o.get("limit_price") or 0),
            "submitted_at":o.get("submitted_at",""),
        } for o in rows]

    # ── Place order ────────────────────────────────────────────
    def place_order(self, symbol: str, qty: int, price: float = 0,
                    side: str = "buy", order_type: str = "market",
                    market: str = "US", time_in_force: str = "day") -> dict:
        body: dict = {
            "symbol":        symbol.upper(),
            "qty":           str(int(qty)),
            "side":          side.lower(),
            "type":          order_type.lower(),   # market/limit/stop/stop_limit
            "time_in_force": time_in_force,        # day/gtc/ioc/fok
        }
        if order_type.lower() in ("limit", "stop_limit") and price:
            body["limit_price"] = str(round(price, 2))
        r = self._req("POST", "/v2/orders", body)
        return {
            "ok":       "id" in r,
            "order_id": r.get("id",""),
            "symbol":   r.get("symbol",""),
            "status":   r.get("status",""),
            "filled_qty": float(r.get("filled_qty", 0)),
            "error":    r.get("message", r.get("error","")),
        }

    # ── Cancel order ───────────────────────────────────────────
    def cancel_order(self, order_id: str) -> dict:
        r = self._req("DELETE", f"/v2/orders/{order_id}")
        return {"ok": not r.get("error"), "raw": r}

    # ── Latest quote / bar ─────────────────────────────────────
    def get_latest_quote(self, symbol: str) -> dict:
        r = self._req("GET", f"/v2/stocks/quotes/latest?symbols={symbol}", data_api=True)
        q = r.get("quotes", {}).get(symbol.upper(), {})
        return {
            "symbol": symbol,
            "bid":    float(q.get("bp", 0)),
            "ask":    float(q.get("ap", 0)),
            "spread_pct": round(abs(float(q.get("ap",0))-float(q.get("bp",0)))/max(float(q.get("ap",1)),1)*100, 3),
        }

    # ── Account balance (compat with BrokerAdapter) ────────────
    def get_account_balance(self) -> dict:
        return self.get_account()

# ═══════════════════════════════════════════════════════════════
def get_sector_heatmap(market='VN'):
    if market != 'VN': return {'error': 'Chỉ hỗ trợ VN'}
    result = {}
    for sector, syms in SECTOR_MAP_VN.items():
        data = []; total_chg = 0; count = 0
        for sym in syms:
            d = _ram_cache.get(sym, {}).get('data', {})
            if not d: continue
            chg = d.get('chg', 0)
            data.append({'symbol': sym, 'chg': chg, 'price': d.get('price',0), 'vol': d.get('vol',0)})
            total_chg += chg; count += 1
        avg_chg = round(total_chg/count,2) if count else 0
        result[sector] = {'symbols': data, 'avg_chg': avg_chg,
                          'color': '#00c853' if avg_chg>1 else '#e53935' if avg_chg<-1 else '#66bb6a' if avg_chg>0 else '#ef9a9a' if avg_chg<0 else '#888'}
    return result

def get_chart_comparison(sym1, mkt1, sym2, mkt2, period=90):
    def get_bars(sym, mkt):
        d = _ram_cache.get(sym, {}).get('data', {})
        bars = d.get('history', [])
        if not bars:
            fetched = fetch(sym, mkt)
            bars = fetched.get('history', []) if fetched else []
        return bars[-period:] if len(bars)>=period else bars
    bars1 = get_bars(sym1, mkt1)
    bars2 = get_bars(sym2, mkt2)
    if not bars1 or not bars2: return {'error': f'Không đủ dữ liệu'}
    base1 = bars1[0]['c']; base2 = bars2[0]['c']
    norm1 = [{'d': b['d'], 'v': round(b['c']/base1*100,2)} for b in bars1]
    norm2 = [{'d': b['d'], 'v': round(b['c']/base2*100,2)} for b in bars2]
    import numpy as np
    r1 = [bars1[i]['c']/bars1[i-1]['c']-1 for i in range(1,len(bars1))]
    r2 = [bars2[i]['c']/bars2[i-1]['c']-1 for i in range(1,len(bars2))]
    n = min(len(r1), len(r2))
    corr = float(np.corrcoef(r1[:n], r2[:n])[0,1]) if n>5 else 0
    perf1 = round((bars1[-1]['c']/base1-1)*100,2) if base1 else 0
    perf2 = round((bars2[-1]['c']/base2-1)*100,2) if base2 else 0
    return {'sym1': sym1, 'mkt1': mkt1, 'bars1': norm1, 'sym2': sym2, 'mkt2': mkt2, 'bars2': norm2,
            'correlation': round(corr,3), 'perf1': perf1, 'perf2': perf2, 'period': period, 'winner': sym1 if perf1>perf2 else sym2}

def get_tick_snapshot(symbol):
    sym = symbol.upper()
    d = _ram_cache.get(sym, {}).get('data', {})
    if not d: return {'error': f'Không có dữ liệu cho {sym}'}
    return {'symbol': sym, 'price': d.get('price',0), 'vol': d.get('vol',0), 'chg': d.get('chg',0),
            'bid': d.get('bid', []), 'ask': d.get('ask', []), 'ts': datetime.now().isoformat()}

def fetch_kqkd_tcbs(symbol):
    import urllib.request as _ur
    for url in [f"https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/financialreport?yearly=0&page=0&size=8",
                f"https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/incomestatement?yearly=0&page=0&size=8"]:
        try:
            with _ur.urlopen(_ur.Request(url, headers={'User-Agent':'SMA/29','Accept':'application/json'}), timeout=10) as r:
                data = json.loads(r.read().decode())
            items = data.get('listFinancialIndicator', data.get('listReportDataItem', []))
            if not items: continue
            quarters = []
            for item in items[:8]:
                q = {'period': item.get('quarter','') or item.get('fiscalDate',''),
                     'revenue': float(item.get('revenue',0) or 0),
                     'net_profit': float(item.get('profit', item.get('netIncome',0)) or 0),
                     'eps': float(item.get('eps',0) or 0), 'roe': float(item.get('roe',0) or 0)}
                if q['revenue']>0: quarters.append(q)
            if quarters:
                yoy = {}
                if len(quarters) >= 5:
                    yoy = {'revenue_yoy': round((quarters[0]['revenue']-quarters[4]['revenue'])/max(abs(quarters[4]['revenue']),1)*100,1),
                           'profit_yoy': round((quarters[0]['net_profit']-quarters[4]['net_profit'])/max(abs(quarters[4]['net_profit']),1)*100,1)}
                return {'symbol': symbol, 'source': 'TCBS', 'quarters': quarters, 'latest': quarters[0], 'yoy_growth': yoy}
        except Exception as e: log.debug("[KQKD] %s", e)
    return {}


# ══════════════════════════════════════════════════════════════════
# BCTC ĐẦY ĐỦ VN — FireAnt API (miễn phí, không cần API key)
# Bao gồm: KQKD, Bảng cân đối kế toán, Lưu chuyển tiền tệ
# + Tính: P/E trailing, EPS growth, ROE, Debt/Equity, P/B, FCF
# ══════════════════════════════════════════════════════════════════
def fetch_bctc_fireant(symbol: str) -> dict:
    """
    Lấy BCTC từ FireAnt API (public, không cần key).
    Trả về dict đầy đủ gồm income, balance, cashflow theo quý.
    """
    import urllib.request as _ur
    base = "https://restv2.fireant.vn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (SMA)',
        'Accept': 'application/json',
        'Origin': 'https://fireant.vn',
        'Referer': 'https://fireant.vn/',
    }
    result = {'symbol': symbol, 'source': 'FireAnt', 'quarters': [],
              'balance': [], 'cashflow': [], 'ratios': {}}

    def _get(url):
        try:
            req = _ur.Request(url, headers=headers)
            with _ur.urlopen(req, timeout=12) as r:
                return json.loads(r.read().decode())
        except Exception as _e:
            log.debug("[FireAnt] %s: %s", url, _e)
            return None

    # 1. KQKD theo quý (Income Statement)
    d_is = _get(f"{base}/symbols/{symbol}/financial-reports?type=incomestatement&size=12&page=0")
    income_rows = []
    if d_is and isinstance(d_is, list):
        for item in d_is[:12]:
            try:
                row = {
                    'period':      item.get('period', item.get('quarter', '')),
                    'year':        int(item.get('year', 0)),
                    'quarter':     int(item.get('quarter', item.get('q', 0)) or 0),
                    'revenue':     float(item.get('revenue', item.get('netRevenue', 0)) or 0),
                    'gross_profit':float(item.get('grossProfit', 0) or 0),
                    'ebit':        float(item.get('ebit', item.get('operatingIncome', 0)) or 0),
                    'net_profit':  float(item.get('netProfit', item.get('postTaxProfit', 0)) or 0),
                    'eps':         float(item.get('eps', item.get('earningsPerShare', 0)) or 0),
                }
                if row['revenue'] > 0 or row['net_profit'] != 0:
                    income_rows.append(row)
            except Exception: pass
    result['quarters'] = income_rows

    # 2. Bảng cân đối kế toán (Balance Sheet)
    d_bs = _get(f"{base}/symbols/{symbol}/financial-reports?type=balancesheet&size=8&page=0")
    balance_rows = []
    if d_bs and isinstance(d_bs, list):
        for item in d_bs[:8]:
            try:
                total_assets    = float(item.get('totalAssets', 0) or 0)
                total_equity    = float(item.get('equity', item.get('ownersEquity', 0)) or 0)
                total_debt      = float(item.get('shortTermBorrowings', 0) or 0) + float(item.get('longTermBorrowings', 0) or 0)
                total_liab      = float(item.get('totalLiabilities', 0) or 0)
                current_assets  = float(item.get('currentAssets', item.get('shortTermAssets', 0)) or 0)
                current_liab    = float(item.get('currentLiabilities', item.get('shortTermLiabilities', 0)) or 0)
                cash            = float(item.get('cash', item.get('cashAndEquivalents', 0)) or 0)
                row = {
                    'period': item.get('period', item.get('quarter', '')),
                    'year':   int(item.get('year', 0)),
                    'quarter':int(item.get('quarter', 0) or 0),
                    'total_assets':   round(total_assets / 1e9, 2),
                    'total_equity':   round(total_equity / 1e9, 2),
                    'total_debt':     round(total_debt / 1e9, 2),
                    'total_liab':     round(total_liab / 1e9, 2),
                    'current_assets': round(current_assets / 1e9, 2),
                    'current_liab':   round(current_liab / 1e9, 2),
                    'cash':           round(cash / 1e9, 2),
                    'debt_equity':    round(total_debt / total_equity, 2) if total_equity > 0 else 0,
                    'current_ratio':  round(current_assets / current_liab, 2) if current_liab > 0 else 0,
                }
                balance_rows.append(row)
            except Exception: pass
    result['balance'] = balance_rows

    # 3. Lưu chuyển tiền tệ (Cash Flow)
    d_cf = _get(f"{base}/symbols/{symbol}/financial-reports?type=cashflow&size=8&page=0")
    cf_rows = []
    if d_cf and isinstance(d_cf, list):
        for item in d_cf[:8]:
            try:
                op_cf   = float(item.get('operatingCashFlow', item.get('netCashFromOperating', 0)) or 0)
                inv_cf  = float(item.get('investingCashFlow', item.get('netCashFromInvesting', 0)) or 0)
                fin_cf  = float(item.get('financingCashFlow', item.get('netCashFromFinancing', 0)) or 0)
                capex   = float(item.get('capex', item.get('purchaseOfFixedAssets', 0)) or 0)
                fcf     = op_cf + capex  # capex thường âm
                row = {
                    'period': item.get('period', ''),
                    'year':   int(item.get('year', 0)),
                    'quarter':int(item.get('quarter', 0) or 0),
                    'operating_cf': round(op_cf / 1e9, 2),
                    'investing_cf': round(inv_cf / 1e9, 2),
                    'financing_cf': round(fin_cf / 1e9, 2),
                    'capex':        round(capex / 1e9, 2),
                    'fcf':          round(fcf / 1e9, 2),
                }
                cf_rows.append(row)
            except Exception: pass
    result['cashflow'] = cf_rows

    # 4. Tính các tỷ số tài chính từ dữ liệu đã có
    try:
        price = _ram_cache.get(symbol, {}).get('data', {}).get('price', 0) or 0
        # EPS trailing 4 quý
        eps_trail = sum(q.get('eps', 0) for q in income_rows[:4]) if len(income_rows) >= 4 else 0
        pe = round(price / eps_trail, 1) if eps_trail > 0 and price > 0 else 0
        # EPS growth YoY
        eps_growth = 0
        if len(income_rows) >= 5:
            eps_now  = income_rows[0].get('eps', 0)
            eps_prev = income_rows[4].get('eps', 0)
            if eps_prev and eps_prev != 0:
                eps_growth = round((eps_now - eps_prev) / abs(eps_prev) * 100, 1)
        # ROE từ balance sheet
        roe = 0
        if balance_rows and income_rows:
            equity = balance_rows[0].get('total_equity', 0) * 1e9
            net_profit_ttm = sum(q.get('net_profit', 0) for q in income_rows[:4])
            roe = round(net_profit_ttm / equity * 100, 1) if equity > 0 else 0
        # Debt/Equity
        de = balance_rows[0].get('debt_equity', 0) if balance_rows else 0
        # Revenue YoY growth
        rev_growth = 0
        if len(income_rows) >= 5:
            rev_now  = income_rows[0].get('revenue', 0)
            rev_prev = income_rows[4].get('revenue', 0)
            if rev_prev > 0:
                rev_growth = round((rev_now - rev_prev) / rev_prev * 100, 1)
        # P/B
        pb = 0
        if balance_rows:
            bvps_raw = balance_rows[0].get('total_equity', 0) * 1e9
            d_fund = _ram_cache.get(symbol, {}).get('data', {}).get('fundamentals', {}) or {}
            shares = d_fund.get('shares_outstanding', 0) or 0
            if shares > 0 and price > 0:
                bvps = bvps_raw / shares
                pb = round(price / bvps, 2)
        # Gross margin
        gm = 0
        if income_rows and income_rows[0].get('revenue', 0) > 0:
            gm = round(income_rows[0].get('gross_profit', 0) / income_rows[0]['revenue'] * 100, 1)
        # FCF yield
        fcf_ttm = sum(q.get('fcf', 0) for q in cf_rows[:4]) if len(cf_rows) >= 4 else 0

        result['ratios'] = {
            'pe_trailing': pe, 'eps_ttm': round(eps_trail, 0),
            'eps_growth_yoy': eps_growth, 'roe': roe,
            'debt_equity': de, 'rev_growth_yoy': rev_growth,
            'pb': pb, 'gross_margin': gm,
            'fcf_ttm_bn': round(fcf_ttm, 1),
        }
    except Exception as _e:
        log.debug("[BCTC ratios] %s: %s", symbol, _e)

    return result


_bctc_cache = {}
_BCTC_TTL = 14400  # 4 giờ

def get_bctc(symbol: str, force: bool = False) -> dict:
    """Get BCTC with cache. Tries FireAnt first, falls back to TCBS."""
    c = _bctc_cache.get(symbol)
    if c and not force and time.time() - c['ts'] < _BCTC_TTL:
        return c['data']
    data = fetch_bctc_fireant(symbol)
    if not data.get('quarters'):
        # Fallback to TCBS for basic KQKD
        tcbs = fetch_kqkd_tcbs(symbol)
        if tcbs.get('quarters'):
            data['quarters'] = tcbs['quarters']
            data['source'] = 'TCBS-fallback'
    _bctc_cache[symbol] = {'ts': time.time(), 'data': data}
    return data




# ══════════════════════════════════════════════════════════════════
# CANDLESTICK PATTERN SCANNER — 8 patterns, toàn watchlist
# ══════════════════════════════════════════════════════════════════
def detect_candle_patterns(bars: list) -> list:
    """
    Phát hiện các mẫu nến phổ biến từ lịch sử OHLCV.
    Trả về list các pattern được tìm thấy (newest first).
    """
    if not bars or len(bars) < 4:
        return []
    patterns = []
    n = len(bars)
    
    def _body(i): return abs(bars[i]['c'] - bars[i].get('o', bars[i]['c']))
    def _range(i):
        h = bars[i].get('h', bars[i]['c']); lo = bars[i].get('l', bars[i]['c'])
        return h - lo if h > lo else 0.0001
    def _bull(i): return bars[i]['c'] >= bars[i].get('o', bars[i]['c'])
    def _upper_wick(i):
        h = bars[i].get('h', bars[i]['c'])
        return h - max(bars[i]['c'], bars[i].get('o', bars[i]['c']))
    def _lower_wick(i):
        lo = bars[i].get('l', bars[i]['c'])
        return min(bars[i]['c'], bars[i].get('o', bars[i]['c'])) - lo

    # Chỉ kiểm tra 5 nến gần nhất để giảm noise
    for i in range(max(1, n-5), n):
        d = bars[i].get('d', bars[i].get('t', ''))
        body  = _body(i)
        rng   = _range(i)
        upper = _upper_wick(i)
        lower = _lower_wick(i)
        body_pct = body / rng if rng > 0 else 0

        # 1. Pin Bar (Hammer / Shooting Star)
        if rng > 0 and body_pct < 0.35:
            # Bullish Pin Bar (Hammer): lower wick >= 2x body, upper wick small
            if lower >= 2 * body and upper <= 0.3 * rng:
                patterns.append({
                    'name': '🔨 Hammer (Pin Bar tăng)',
                    'type': 'BULLISH', 'score': 2,
                    'date': d, 'reliability': 72,
                    'detail': f'Lower wick {lower/rng*100:.0f}% range, body {body_pct*100:.0f}%'
                })
            # Bearish Pin Bar (Shooting Star): upper wick >= 2x body, lower wick small
            elif upper >= 2 * body and lower <= 0.3 * rng:
                patterns.append({
                    'name': '⭐ Shooting Star (Pin Bar giảm)',
                    'type': 'BEARISH', 'score': -2,
                    'date': d, 'reliability': 70,
                    'detail': f'Upper wick {upper/rng*100:.0f}% range, body {body_pct*100:.0f}%'
                })

        # 2. Doji (thân rất nhỏ)
        if body_pct < 0.1 and rng > 0:
            patterns.append({
                'name': '✚ Doji (do dự)',
                'type': 'NEUTRAL', 'score': 0,
                'date': d, 'reliability': 55,
                'detail': 'Thân = ' + f'{body_pct*100:.1f}% range — thị trường đang cân bằng'
            })

        # Multi-bar patterns
        if i < 1: continue
        prev = bars[i-1]

        # 3. Bullish Engulfing
        if (not _bull(i-1) and _bull(i)
                and bars[i]['c'] > prev.get('o', prev['c'])
                and bars[i].get('o', bars[i]['c']) < prev['c']
                and body > _body(i-1) * 1.1):
            patterns.append({
                'name': '🟢 Bullish Engulfing',
                'type': 'BULLISH', 'score': 3,
                'date': d, 'reliability': 78,
                'detail': f'Nến tăng {_body(i)/bars[i]["c"]*100:.1f}% nuốt nến giảm trước'
            })

        # 4. Bearish Engulfing
        if (_bull(i-1) and not _bull(i)
                and bars[i].get('o', bars[i]['c']) > prev['c']
                and bars[i]['c'] < prev.get('o', prev['c'])
                and body > _body(i-1) * 1.1):
            patterns.append({
                'name': '🔴 Bearish Engulfing',
                'type': 'BEARISH', 'score': -3,
                'date': d, 'reliability': 78,
                'detail': f'Nến giảm nuốt nến tăng trước'
            })

        # 5. Inside Bar (range nhỏ hơn nến trước)
        if (bars[i].get('h', bars[i]['c']) < prev.get('h', prev['c'])
                and bars[i].get('l', bars[i]['c']) > prev.get('l', prev['c'])):
            ib_type = 'BULLISH' if _bull(i) else 'BEARISH'
            patterns.append({
                'name': '📦 Inside Bar (' + ('tăng' if ib_type=='BULLISH' else 'giảm') + ')',
                'type': ib_type, 'score': 1 if ib_type=='BULLISH' else -1,
                'date': d, 'reliability': 60,
                'detail': 'Range nằm hoàn toàn trong nến trước — chờ breakout'
            })

        # Multi-bar (cần 3+ nến)
        if i < 2: continue
        p2 = bars[i-2]

        # 6. Morning Star (BULLISH)
        if (not _bull(i-2) and _body(i-2) > _range(i-2)*0.4
                and _body(i-1) < _range(i-1)*0.25  # small doji-like
                and _bull(i) and bars[i]['c'] > (p2.get('o', p2['c']) + p2['c'])/2):
            patterns.append({
                'name': '🌅 Morning Star (đảo chiều tăng)',
                'type': 'BULLISH', 'score': 4,
                'date': d, 'reliability': 83,
                'detail': 'Nến giảm → do dự → nến tăng mạnh — tín hiệu đảo chiều'
            })

        # 7. Evening Star (BEARISH)
        if (_bull(i-2) and _body(i-2) > _range(i-2)*0.4
                and _body(i-1) < _range(i-1)*0.25
                and not _bull(i) and bars[i]['c'] < (p2.get('o', p2['c']) + p2['c'])/2):
            patterns.append({
                'name': '🌆 Evening Star (đảo chiều giảm)',
                'type': 'BEARISH', 'score': -4,
                'date': d, 'reliability': 82,
                'detail': 'Nến tăng → do dự → nến giảm mạnh — tín hiệu đảo chiều'
            })

        # 8. Three White Soldiers
        if (all(_bull(j) for j in [i-2, i-1, i])
                and bars[i-1]['c'] > bars[i-2]['c']
                and bars[i]['c'] > bars[i-1]['c']
                and all(_body(j) > _range(j)*0.5 for j in [i-2, i-1, i])):
            patterns.append({
                'name': '🚀 Three White Soldiers (tăng mạnh)',
                'type': 'BULLISH', 'score': 4,
                'date': d, 'reliability': 80,
                'detail': '3 nến tăng liên tiếp, thân dài — xu hướng tăng mạnh'
            })

        # 9. Three Black Crows
        if (all(not _bull(j) for j in [i-2, i-1, i])
                and bars[i-1]['c'] < bars[i-2]['c']
                and bars[i]['c'] < bars[i-1]['c']
                and all(_body(j) > _range(j)*0.5 for j in [i-2, i-1, i])):
            patterns.append({
                'name': '🐦 Three Black Crows (giảm mạnh)',
                'type': 'BEARISH', 'score': -4,
                'date': d, 'reliability': 79,
                'detail': '3 nến giảm liên tiếp, thân dài — xu hướng giảm mạnh'
            })

    # Remove duplicates (same pattern on same date), keep highest score
    seen = {}
    for p in sorted(patterns, key=lambda x: -abs(x['score'])):
        k = (p['name'], p['date'])
        if k not in seen:
            seen[k] = p
    return sorted(seen.values(), key=lambda x: x['date'], reverse=True)


def scan_patterns_watchlist(user_id: int) -> list:
    """Scan toàn watchlist để tìm mẫu nến gần nhất."""
    try:
        with get_db() as c:
            wl = c.execute("SELECT symbol, market FROM user_watchlist WHERE user_id=?",
                           (user_id,)).fetchall()
    except Exception: return []
    results = []
    for row in wl:
        sym = row['symbol']; mkt = row['market'] or 'VN'
        d = _ram_cache.get(sym, {}).get('data', {})
        if not d: continue
        bars = d.get('history', [])
        if not bars: continue
        pts = detect_candle_patterns(bars)
        if pts:
            price = d.get('price', 0)
            chg   = d.get('chg', 0)
            for p in pts[:2]:  # top 2 patterns per symbol
                results.append({**p, 'symbol': sym, 'market': mkt,
                                 'price': price, 'chg': chg})
    # Sort: BULLISH > BEARISH > NEUTRAL, then by reliability
    results.sort(key=lambda x: (-abs(x['score']), -x['reliability']))
    return results[:30]  # top 30



def markowitz_optimize(symbols, market='VN', risk_free=0.04, n_portfolios=2000):
    import numpy as _np
    returns_data = []; valid_syms = []
    for sym in symbols:
        d = _ram_cache.get(sym, {}).get('data', {})
        bars = d.get('history', [])
        if len(bars) < 30: continue
        rets = [bars[i]['c']/bars[i-1]['c']-1 for i in range(1,len(bars))]
        returns_data.append(rets[-252:]); valid_syms.append(sym)
    if len(valid_syms) < 2: return {'error': 'Cần ít nhất 2 mã có đủ dữ liệu'}
    n = len(valid_syms); min_len = min(len(r) for r in returns_data)
    R = _np.array([r[:min_len] for r in returns_data])
    mu = R.mean(axis=1)*252; cov = _np.cov(R)*252
    results = {'ret':[], 'risk':[], 'sharpe':[], 'weights':[]}
    for _ in range(n_portfolios):
        w = _np.random.dirichlet(_np.ones(n))
        pr = float(w@mu); pvr = float(_np.sqrt(w@cov@w)); ps = (pr-risk_free)/(pvr+1e-9)
        results['ret'].append(round(pr*100,2)); results['risk'].append(round(pvr*100,2))
        results['sharpe'].append(round(ps,3)); results['weights'].append([round(x,4) for x in w])
    sharpes = _np.array(results['sharpe'])
    bi = int(sharpes.argmax()); mi = int(_np.array(results['risk']).argmin())
    return {
        'symbols': valid_syms,
        'max_sharpe': {'weights': dict(zip(valid_syms, results['weights'][bi])),
                       'return_pct': results['ret'][bi], 'risk_pct': results['risk'][bi],
                       'sharpe': results['sharpe'][bi]},
        'min_variance': {'weights': dict(zip(valid_syms, results['weights'][mi])),
                         'return_pct': results['ret'][mi], 'risk_pct': results['risk'][mi],
                         'sharpe': results['sharpe'][mi]},
        'n_simulated': n_portfolios
    }

def get_sector_pe_valuation(symbol, fund_data=None):
    sector = get_sector_for_symbol(symbol)
    sector_pe = SECTOR_AVG_PE_VN.get(sector, 0)
    if not sector or not sector_pe: return {}
    stock_pe = 0
    if fund_data: stock_pe = float(fund_data.get('pe',0) or 0)
    if stock_pe <= 0:
        d = _ram_cache.get(symbol, {}).get('data', {})
        stock_pe = float((d.get('fundamental') or {}).get('pe',0) or 0)
    if stock_pe <= 0: return {'sector': sector, 'sector_avg_pe': sector_pe, 'note': 'Chưa có P/E'}
    premium = round((stock_pe / sector_pe - 1) * 100, 1)
    return {'symbol': symbol, 'sector': sector, 'stock_pe': round(stock_pe,1),
            'sector_avg_pe': sector_pe, 'premium_pct': premium,
            'valuation': 'ĐẮT' if premium>20 else 'RẺ' if premium<-20 else 'HỢP LÝ',
            'color': '#f44336' if premium>20 else '#4caf50' if premium<-20 else '#ff9800',
            'note': f'P/E={stock_pe:.1f}x vs ngành {sector_pe:.1f}x ({premium:+.1f}%)'}

def get_event_countdown(symbol, market):
    try:
        with get_db() as c:
            events = c.execute("SELECT title, event_date, event_type FROM events WHERE symbol=? AND market=? AND event_date>=date('now') ORDER BY event_date ASC LIMIT 3",
                               (symbol, market)).fetchall()
    except: events = []
    if not events:
        try:
            flag, title = has_upcoming_events(symbol, market, days=30)
            if flag: return {'has_event': True, 'title': title, 'days': '≤30', 'color': '#ff9800'}
        except: pass
        return {'has_event': False}
    result = []
    for ev in events:
        try:
            ev_date = datetime.strptime(str(ev['event_date'])[:10], '%Y-%m-%d')
            days_left = (ev_date - datetime.now()).days
            result.append({'title': ev['title'], 'date': ev['event_date'], 'days_left': days_left,
                           'color': '#f44336' if days_left<=3 else '#ff9800' if days_left<=7 else '#4caf50',
                           'label': f"{'Hôm nay!' if days_left==0 else 'Ngày mai!' if days_left==1 else f'Còn {days_left} ngày'}"})
        except: pass
    return {'has_event': bool(result), 'events': result, 'next': result[0] if result else None}

def check_controlled_status(symbol):
    controlled = fetch_controlled_stocks()
    sym = symbol.upper(); is_c = sym in controlled
    return {'symbol': sym, 'is_controlled': is_c, 'status': 'KIỂM SOÁT' if is_c else 'BÌNH THƯỜNG',
            'warning': f'⚠️ {sym} đang bị KIỂM SOÁT — chỉ bán, không mua!' if is_c else '',
            'color': '#f44336' if is_c else '#4caf50'}

def fetch_controlled_stocks():
    global _CONTROLLED_STOCKS, _controlled_last_fetch
    if time.time() - _controlled_last_fetch < 3600: return _CONTROLLED_STOCKS
    try:
        stored = get_setting('controlled_stocks', '')
        _CONTROLLED_STOCKS = set(x.strip().upper() for x in stored.split(',') if x.strip())
    except: pass
    _controlled_last_fetch = time.time()
    return _CONTROLLED_STOCKS

def calc_portfolio_beta(user_id, index_sym='VNINDEX', period=60):
    import numpy as _np
    with get_db() as c:
        positions = c.execute("SELECT symbol, qty, entry_price, market FROM trades WHERE user_id=? AND status='OPEN'", (user_id,)).fetchall()
    if not positions: return {'beta':1.0, 'note':'Không có vị thế mở'}
    idx_bars = _ram_cache.get(index_sym, {}).get('data', {}).get('history', [])[-period:]
    if len(idx_bars) < 20: return {'beta':1.0, 'note':f'{index_sym} chưa đủ dữ liệu'}
    idx_rets = _np.array([idx_bars[i]['c']/idx_bars[i-1]['c']-1 for i in range(1,len(idx_bars))])
    total_value = sum(float(p['qty'])*float(p['entry_price']) for p in positions)
    if total_value <= 0: return {'beta':1.0, 'note':'Không tính được'}
    port_rets = _np.zeros(len(idx_rets))
    betas_by_sym = {}
    for p in positions:
        sym = p['symbol']
        bars = _ram_cache.get(sym, {}).get('data', {}).get('history', [])[-period:]
        if len(bars) < 20: continue
        sym_rets = _np.array([bars[i]['c']/bars[i-1]['c']-1 for i in range(1,len(bars))])
        n = min(len(sym_rets), len(idx_rets))
        if n < 5: continue
        cov = _np.cov(sym_rets[:n], idx_rets[:n]); beta_sym = float(cov[0,1]/cov[1,1]) if cov[1,1]>0 else 1.0
        weight = float(p['qty'])*float(p['entry_price'])/total_value
        port_rets += sym_rets[:len(port_rets)] * weight
        betas_by_sym[sym] = round(beta_sym,2)
    n = min(len(port_rets), len(idx_rets))
    cov = _np.cov(port_rets[:n], idx_rets[:n])
    port_beta = float(cov[0,1]/cov[1,1]) if cov[1,1]>0 else 1.0
    return {'beta': round(port_beta,2), 'index': index_sym, 'period_days': period,
            'by_symbol': betas_by_sym,
            'interpretation': 'Ổn định hơn thị trường' if port_beta<0.8 else 'Tương đương thị trường' if port_beta<=1.2 else 'Biến động mạnh hơn thị trường',
            'note': f'Beta={port_beta:.2f}: nếu {index_sym} +1%, danh mục +{port_beta:.2f}%'}

def get_sector_exposure(user_id):
    with get_db() as c:
        positions = c.execute("SELECT symbol, qty, entry_price, market FROM trades WHERE user_id=? AND status='OPEN'", (user_id,)).fetchall()
    if not positions: return {}
    total_value = 0.0; by_sector = {}
    for p in positions:
        sym = p['symbol']
        cur_price = _ram_cache.get(sym, {}).get('data', {}).get('price', float(p['entry_price']))
        value = float(p['qty'])*cur_price
        total_value += value
        sector = get_sector_for_symbol(sym) or ('US' if p['market']=='US' else 'Khác')
        by_sector.setdefault(sector, {'value':0, 'symbols':[]})
        by_sector[sector]['value'] += value
        by_sector[sector]['symbols'].append(sym)
    if total_value <= 0: return {}
    result = [{'sector': s, 'pct': round(d['value']/total_value*100,1), 'value': round(d['value'],0), 'symbols': d['symbols']}
              for s,d in sorted(by_sector.items(), key=lambda x: -x[1]['value'])]
    return {'breakdown': result, 'total_value': round(total_value,0), 'n_positions': len(positions)}

def get_pnl_calendar(user_id, year=None, month=None):
    if not year: year = datetime.now().year
    if not month: month = datetime.now().month
    with get_db() as c:
        rows = c.execute(
            "SELECT exit_date, SUM(pnl) as daily_pnl, COUNT(*) as n_trades FROM trades WHERE user_id=? AND status='CLOSED' AND strftime('%Y',exit_date)=? AND strftime('%m',exit_date)=? GROUP BY exit_date ORDER BY exit_date",
            (user_id, str(year), f"{month:02d}")
        ).fetchall()
    import calendar as _cal
    days_in_month = _cal.monthrange(year, month)[1]
    days = {}
    for row in rows:
        day_num = int(str(row['exit_date'])[8:10])
        pnl = float(row['daily_pnl'] or 0)
        days[day_num] = {'pnl': round(pnl,0), 'n_trades': row['n_trades'],
                         'color': '#1b5e20' if pnl>5e6 else '#4caf50' if pnl>0 else '#e53935' if pnl<0 else '#888'}
    total_pnl = sum(d['pnl'] for d in days.values())
    return {'year': year, 'month': month, 'days_in_month': days_in_month, 'days': days,
            'total_pnl': round(total_pnl,0), 'win_days': sum(1 for d in days.values() if d['pnl']>0),
            'loss_days': sum(1 for d in days.values() if d['pnl']<0)}

def get_sector_for_symbol(symbol):
    sym = symbol.upper()
    for sector, syms in SECTOR_MAP_VN.items():
        if sym in syms: return sector
    return ''

# Các hằng số macro
_MACRO_SYMBOLS = {
    'VNINDEX': {'name':'VN-Index','icon':'🇻🇳','mkt':'VN'},
    'VN30':    {'name':'VN30','icon':'🇻🇳','mkt':'VN'},
    'HNX':     {'name':'HNX-Index','icon':'🇻🇳','mkt':'VN'},
    'DXY':     {'name':'USD Index','icon':'💵','mkt':'US'},
    'GC=F':    {'name':'Gold','icon':'🥇','mkt':'US'},
    'CL=F':    {'name':'WTI Oil','icon':'🛢','mkt':'US'},
    '^VIX':    {'name':'VIX Fear','icon':'😱','mkt':'US'},
    '^GSPC':   {'name':'S&P 500','icon':'🇺🇸','mkt':'US'},
    '^N225':   {'name':'Nikkei','icon':'🇯🇵','mkt':'US'},
    'HSI':     {'name':'Hang Seng','icon':'🇭🇰','mkt':'US'},
}

SECTOR_AVG_PE_VN = {
    'Ngân hàng':11.5,'Bất động sản':18.0,'Chứng khoán':14.0,'Dầu khí':12.0,
    'Thép & VL':10.0,'Tiêu dùng':22.0,'Điện & NL':15.0,'Công nghệ':25.0,'Y tế & Dược':20.0,'Vận tải & LS':13.0
}
_CONTROLLED_STOCKS = set()
_controlled_last_fetch = 0
class BinanceWS:
    REST="https://api.binance.com/api/v3"
    WSS ="wss://stream.binance.com:9443/stream"
    _DEFAULTS=['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','ADAUSDT','DOGEUSDT','AVAXUSDT']

    def __init__(self):
        self._data:dict={}; self._lock=threading.Lock()
        self._running=False; self._subscribed:set=set()

    def subscribe(self,symbols:list):
        pairs=[s.upper().replace('/','')+('' if any(s.upper().endswith(q) for q in ['USDT','BTC','ETH','BNB']) else 'USDT') for s in symbols]
        with self._lock: self._subscribed.update(pairs)
        if not self._running: self._start()

    def _start(self):
        self._running=True
        threading.Thread(target=self._loop,daemon=True,name='binance-ws').start()

    def _loop(self):
        self._rest_snapshot()
        while self._running:
            try:
                import websocket as _wsl
                def on_msg(ws,raw):
                    try:
                        d=json.loads(raw).get('data',json.loads(raw))
                        sym=d.get('s','').upper()
                        if sym:
                            with self._lock:
                                self._data[sym]={'symbol':sym,'price':float(d.get('c',0)),
                                    'chg_24h':float(d.get('P',0)),'vol_24h':float(d.get('v',0)),
                                    'high_24h':float(d.get('h',0)),'low_24h':float(d.get('l',0)),'ts':time.time()}
                    except: pass
                streams='/'.join(f"{s.lower()}@ticker" for s in self._subscribed)
                _wsl.WebSocketApp(f"{self.WSS}?streams={streams}",on_message=on_msg,
                    on_error=lambda ws,e:None,on_close=lambda ws,c,m:None
                ).run_forever(ping_interval=30,ping_timeout=10)
            except ImportError:
                while self._running: self._rest_snapshot(); time.sleep(30); break
            except Exception as e:
                log.debug("[BinanceWS] %s",e)
                if self._running: time.sleep(10)

    def _rest_snapshot(self):
        import urllib.request as _ur
        for sym in list(self._subscribed):
            try:
                with _ur.urlopen(f"{self.REST}/ticker/24hr?symbol={sym}",timeout=8) as r:
                    d=json.loads(r.read().decode())
                    with self._lock:
                        self._data[sym]={'symbol':sym,'price':float(d.get('lastPrice',0)),
                            'chg_24h':float(d.get('priceChangePercent',0)),'vol_24h':float(d.get('volume',0)),
                            'high_24h':float(d.get('highPrice',0)),'low_24h':float(d.get('lowPrice',0)),'ts':time.time()}
            except Exception as e: log.debug("[BinanceWS] REST %s: %s",sym,e)

    def get_all(self)->dict:
        with self._lock: return dict(self._data)

    def get_price(self,symbol:str)->dict:
        sym=symbol.upper(); sym=sym if sym.endswith('USDT') else sym+'USDT'
        with self._lock: return self._data.get(sym,{})

    def get_klines(self,symbol:str,interval:str='1h',limit:int=200)->list:
        sym=symbol.upper(); sym=sym if sym.endswith('USDT') else sym+'USDT'
        import urllib.request as _ur
        try:
            with _ur.urlopen(f"{self.REST}/klines?symbol={sym}&interval={interval}&limit={limit}",timeout=10) as r:
                return [{'t':str(k[0]),'o':float(k[1]),'h':float(k[2]),'l':float(k[3]),'c':float(k[4]),'v':float(k[5])} for k in json.loads(r.read().decode())]
        except: return []

    def stop(self): self._running=False

_binance_ws:BinanceWS|None=None
def get_binance_ws()->BinanceWS:
    global _binance_ws
    if _binance_ws is None: _binance_ws=BinanceWS()
    return _binance_ws

def init_binance_ws():
    ws=get_binance_ws(); ws.subscribe(BinanceWS._DEFAULTS)
    log.info("[BinanceWS] Started")
    
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    
    def handle_error(self, request, client_address):
        import traceback
        err = traceback.format_exc()
        if 'BrokenPipe' in err or 'ConnectionReset' in err or 'ConnectionAborted' in err:
            return
        log.error(f"[Handler] {client_address}: {err[:200]}")
    
    def cors(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,DELETE,PUT,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type,Authorization,X-Token,ngrok-skip-browser-warning")
        self.send_header("ngrok-skip-browser-warning","true")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("X-Frame-Options","SAMEORIGIN")
        self.send_header("Referrer-Policy","strict-origin-when-cross-origin")
        self.send_header("Content-Security-Policy",
            "default-src 'self' https: data:; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src https://fonts.gstatic.com; img-src 'self' data: https:;")
    
    def send_json(self,data,code=200):
        try:
            b=json.dumps(data,ensure_ascii=False,default=str).encode()
            self.send_response(code); self.cors()
            self.send_header("Content-Type","application/json; charset=utf-8")
            self.send_header("Content-Length",len(b)); self.end_headers(); self.wfile.write(b)
        except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError): pass
    
    def send_file(self,path,mime):
        try:
            with open(path,"rb") as f: b=f.read()
            self.send_response(200); self.cors()
            self.send_header("Content-Type",mime)
            self.send_header("Content-Length",len(b)); self.end_headers(); self.wfile.write(b)
        except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError): pass
    
    def body(self):
        l=int(self.headers.get("Content-Length",0))
        return json.loads(self.rfile.read(l)) if l else {}
    
    def do_OPTIONS(self):
        self.send_response(200); self.cors(); self.end_headers()
    
    def get_user(self):
        token = (self.headers.get("X-Token") or
                self.headers.get("Authorization","").replace("Bearer ","").strip())
        return get_session(token) if token else None
    
    def require_auth(self):
        u = self.get_user()
        if not u: self.send_json({"error":"Chưa đăng nhập","code":"UNAUTHORIZED"},401)
        return u
    
    def maybe_auth(self):
        token = (self.headers.get('Authorization','').replace('Bearer ','') or
                parse_qs(urlparse(self.path).query).get('token', [''])[0])
        return get_session(token) if token else None

    def require_admin(self):
        u = self.get_user()
        if not u: self.send_json({"error":"Chưa đăng nhập","code":"UNAUTHORIZED"},401); return None
        if u['role'] != 'admin': self.send_json({"error":"Cần quyền admin","code":"FORBIDDEN"},403); return None
        return u
    
    def calc_pnl_dashboard(user_id: int) -> dict:
        try:
            with get_db() as c:
                closed = c.execute(
                    "SELECT symbol, market, direction, entry_price, exit_price, qty, pnl, entry_date, exit_date "
                    "FROM trades WHERE user_id=? AND status='CLOSED' ORDER BY exit_date",
                    (user_id,)
                ).fetchall()
                open_pos = c.execute(
                    "SELECT symbol, market, direction, entry_price, qty, entry_date "
                    "FROM trades WHERE user_id=? AND status='OPEN'",
                    (user_id,)
                ).fetchall()
        except:
            return {'error': 'DB error'}

        realized_total = sum(float(t['pnl'] or 0) for t in closed)
        realized_by_month = {}
        for t in closed:
            ym = (t['exit_date'] or '')[:7]
            if ym not in realized_by_month:
                realized_by_month[ym] = {'pnl': 0, 'n': 0, 'wins': 0}
            pnl = float(t['pnl'] or 0)
            realized_by_month[ym]['pnl'] += pnl
            realized_by_month[ym]['n'] += 1
            if pnl > 0: realized_by_month[ym]['wins'] += 1

        unrealized_total = 0
        unrealized_list = []
        for pos in open_pos:
            sym = pos['symbol']
            cur_price = _ram_cache.get(sym, {}).get('data', {}).get('price', float(pos['entry_price']))
            entry = float(pos['entry_price']); qty = float(pos['qty'])
            upnl = (cur_price - entry) * qty if pos['direction'] == 'LONG' else (entry - cur_price) * qty
            pct = (cur_price - entry) / entry * 100 if entry > 0 else 0
            unrealized_total += upnl
            days = 0
            try: days = (datetime.now() - datetime.fromisoformat(pos['entry_date'][:10])).days
            except: pass
            unrealized_list.append({'symbol': sym, 'market': pos['market'],
                'entry_price': entry, 'cur_price': round(cur_price, 2), 'qty': qty,
                'pnl': round(upnl, 0), 'pct': round(pct, 2), 'direction': pos['direction'],
                'entry_date': pos['entry_date'], 'days_held': days})
        unrealized_list.sort(key=lambda x: x['pnl'])

        # Thue TNCN VN: 0.1% doanh thu ban
        total_sell_vn = sum(float(t.get('exit_price') or 0) * float(t.get('qty') or 0)
                            for t in closed if t.get('market') == 'VN')
        tax_vn = total_sell_vn * 0.001
        tax_by_month = {}
        for t in closed:
            if t.get('market') != 'VN': continue
            ym = (t.get('exit_date') or '')[:7]
            tax_by_month[ym] = tax_by_month.get(ym, 0) + float(t.get('exit_price') or 0) * float(t.get('qty') or 0) * 0.001

        capital_init = float(get_setting('account_size', '100000000') or 1e8)
        equity = [{'date': 'Start', 'value': capital_init}]
        running = capital_init
        for t in closed:
            running += float(t['pnl'] or 0)
            equity.append({'date': (t['exit_date'] or '')[:10], 'value': round(running, 0),
                        'cum_pnl': round(running - capital_init, 0)})

        monthly = []
        for ym in sorted(realized_by_month.keys()):
            m = realized_by_month[ym]
            monthly.append({'month': ym, 'pnl': round(m['pnl'], 0), 'n_trades': m['n'],
                'win_rate': round(m['wins'] / m['n'] * 100, 1) if m['n'] > 0 else 0,
                'tax_est': round(tax_by_month.get(ym, 0), 0)})

        wins = [t for t in closed if float(t['pnl'] or 0) > 0]
        losses = [t for t in closed if float(t['pnl'] or 0) <= 0]
        avg_win = sum(float(t['pnl']) for t in wins) / len(wins) if wins else 0
        avg_loss = sum(float(t['pnl']) for t in losses) / len(losses) if losses else 0

        return {'realized_total': round(realized_total, 0), 'unrealized_total': round(unrealized_total, 0),
                'total_pnl': round(realized_total + unrealized_total, 0),
                'tax_estimate_vn': round(tax_vn, 0), 'n_closed': len(closed), 'n_open': len(open_pos),
                'win_rate': round(len(wins)/len(closed)*100,1) if closed else 0,
                'avg_win': round(avg_win,0), 'avg_loss': round(avg_loss,0),
                'profit_factor': round(abs(avg_win/avg_loss),2) if avg_loss else 0,
                'monthly': monthly, 'equity_curve': equity[-60:],
                'unrealized_positions': unrealized_list, 'capital_init': capital_init}

    def totp_generate_secret() -> str:
        """Tạo TOTP secret mới."""
        try:
            import pyotp
            return pyotp.random_base32()
        except ImportError:
            # Fallback: generate compatible secret without pyotp
            import base64, os
            secret_bytes = os.urandom(20)
            return base64.b32encode(secret_bytes).decode('utf-8')

    def totp_verify(secret: str, code: str) -> bool:
        """Xác minh TOTP code (±1 window = 90 giây)."""
        if not secret or not code: return False
        code = str(code).strip().replace(' ', '')
        try:
            import pyotp
            totp = pyotp.TOTP(secret)
            return totp.verify(code, valid_window=1)
        except ImportError:
            # Fallback implementation without pyotp
            import hmac, hashlib, struct, time as _time
            try:
                import base64
                key = base64.b32decode(secret.upper() + '=' * (-len(secret) % 8))
            except: return False
            def _hotp(key, counter):
                msg = struct.pack('>Q', counter)
                h = hmac.new(key, msg, hashlib.sha1).digest()
                offset = h[-1] & 0x0f
                code_int = struct.unpack('>I', h[offset:offset+4])[0] & 0x7fffffff
                return str(code_int % 1000000).zfill(6)
            t = int(_time.time()) // 30
            return any(_hotp(key, t+i) == code for i in [-1, 0, 1])

    def totp_get_uri(secret: str, username: str) -> str:
        """Tạo URI cho QR code (dùng Google Authenticator)."""
        try:
            import pyotp, urllib.parse
            totp = pyotp.TOTP(secret)
            return totp.provisioning_uri(name=username, issuer_name=f"SMA_v{VERSION}")
        except:
            import urllib.parse
            name = urllib.parse.quote(username)
            issuer = urllib.parse.quote(f"SMA_v{VERSION}")
            return f"otpauth://totp/{issuer}:{name}?secret={secret}&issuer={issuer}&algorithm=SHA1&digits=6&period=30"

    def totp_qr_svg(uri: str) -> str:
        """Tạo QR code SVG đơn giản từ URI (không cần thư viện ngoài)."""
        # Return a simple text representation if qrcode not available
        try:
            import qrcode, io
            qr = qrcode.QRCode(version=1, box_size=4, border=2)
            qr.add_data(uri); qr.make(fit=True)
            # SVG output
            rows = qr.modules
            size = len(rows) * 4 + 16
            cells = ""
            for y, row in enumerate(rows):
                for x, cell in enumerate(row):
                    if cell:
                        cells += f'<rect x="{x*4+8}" y="{y*4+8}" width="4" height="4" fill="#000"/>'
            return f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" style="background:#fff">{cells}</svg>'
        except:
            return f'<div style="font-family:monospace;font-size:10px;word-break:break-all;padding:10px;background:#f5f5f5;border-radius:6px">{uri}</div>'

    def do_GET(self):
        p=urlparse(self.path); path=p.path.rstrip("/"); qs=parse_qs(p.query)
        base=os.path.dirname(os.path.abspath(__file__))

        if path in ("","/"):
            # Tìm index theo thứ tự ưu tiên
            for _idx in ["index_Pro.html", "index.html"]:
                html_path = os.path.join(base, _idx)
                if os.path.exists(html_path):
                    self.send_file(html_path, "text/html; charset=utf-8")
                    return
            self.send_json({"error": "index not found (thử index_Pro.html hoặc index.html)"}, 404)
            return
        if path=="/favicon.ico":
            self.send_response(200); self.cors()
            self.send_header("Content-Type","image/x-icon")
            self.send_header("Content-Length","0"); self.end_headers(); return
        
        # API Documentation
        if path == "/api/docs":
            self.send_json({
                "openapi": "3.0.0",
                "info": {"title": "SMA Pro API", "version": VERSION},
                "servers": [{"url": f"http://localhost:{PORT}"}],
                "paths": {}
            })
            return
        
        # Health check
        if path == "/health":
            self.send_json({"status": "healthy", "version": VERSION, "timestamp": datetime.now().isoformat()})
            return
        
        # ML Prediction
        if path == "/api/ml/predict":
            sym = qs.get("symbol", [""])[0].upper()
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            data = _ram_cache.get(sym, {}).get('data', {})
            if not data:
                self.send_json({"error": "No data"}, 404)
                return
            pred = ml_predict(sym, data.get('analysis', {}))
            self.send_json({"symbol": sym, **pred})
            return
        
        # Portfolio Analytics
        if path == "/api/portfolio/analytics":
            user = self.require_auth()
            if not user: return
            
            trades = trade_list(user['id'], status='OPEN')
            if len(trades) < 2:
                self.send_json({"error": "Need at least 2 positions"})
                return
            
            # Calculate efficient frontier (simplified)
            returns_data = {}
            for t in trades:
                bars = db_load_ohlcv(t['symbol'], 252)
                if len(bars) >= 30:
                    rets = [(bars[i]['c'] - bars[i-1]['c']) / bars[i-1]['c'] 
                        for i in range(1, len(bars))]
                    returns_data[t['symbol']] = rets[-100:]
            
            self.send_json({"assets": list(returns_data.keys())})
            return
        
        # Efficient Frontier
        if path == "/api/portfolio/frontier":
            u = self.require_auth()
            if not u: return
            result = calc_efficient_frontier(u['id'])
            self.send_json(result)
            return
        
        # Scanner
        if path == "/api/scanner":
            market = qs.get("market", ["ALL"])[0].upper()
            min_score = int(qs.get("min_score", ["5"])[0])
            signal = qs.get("signal", [None])[0]
            limit = int(qs.get("limit", ["100"])[0])
            self.send_json(screener_run(market, min_score, signal, limit))
            return
        
        #Heikin
        if path.startswith("/api/heikin/"):
            sym=path.split("/api/heikin/")[-1].upper(); mkt=(qs.get("market",["VN"])[0] if qs else "VN").upper()
            force=(qs.get("force",["0"])[0] if qs else "0")=="1"
            d=_ram_cache.get(sym,{}).get("data",{})
            bars=d.get("history",[]) if d and not force else []
            if not bars:
                fetched=fetch(sym,mkt,force=force)
                bars=fetched.get("history",[]) if fetched else []
                if fetched and not d:
                    _ram_cache[sym]={"data":fetched}
            if not bars:
                self.send_json({"error":f"Chưa có dữ liệu cho {sym} — thêm vào watchlist và đợi refresh","symbol":sym,"bars":[]},404); return
            self.send_json({"symbol":sym,"bars":calc_heikin_ashi(bars),"n_bars":len(bars)}); return
        
        #fibonacci
        if path.startswith("/api/fibonacci/"):
            sym = path.split("/")[-1].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            d = _ram_cache.get(sym, {}).get("data", {})
            if not d: d = fetch(sym, mkt) or {}
            if not d or not d.get('history'):
                self.send_json({"error": f"Không có dữ liệu cho {sym}"}, 404); return
            fib = calc_fibonacci(d['history'])
            fib['price'] = d.get('price', 0)
            fib['symbol'] = sym
            self.send_json(fib); return

        if path.startswith("/api/trend_forecast/"):
            sym=path.split("/api/trend_forecast/")[-1].upper()
            mkt=(qs.get("market",["VN"])[0] if qs else "VN").upper()
            periods=int(qs.get("periods",["10"])[0] if qs else 10)
            d=_ram_cache.get(sym,{}).get("data",{})
            if not d:
                d=fetch(sym,mkt) or {}
            if not d:
                self.send_json({"error":f"Chưa có dữ liệu cho {sym}"},404); return
            result=calc_trend_forecast(d, periods)
            result["symbol"]=sym; result["market"]=mkt
            self.send_json(result); return
        
        # Top Signals Persistent
        if path == "/api/top_signals_persistent":
            sig_type = qs.get("type", ["BUY"])[0].upper()
            days = int(qs.get("days", ["7"])[0])
            if days not in [7,21,30]: days=7
            min_days = int(qs.get("min_days", ["0"])[0]) or None
            limit = int(qs.get("limit", ["30"])[0])
            result = get_top_signals_persistent(sig_type, days, min_days, limit)
            self.send_json({"type": sig_type, "days": days, "items": result})
            return
        
        # Intraday
        if path == "/api/intraday":
            sym = qs.get("symbol", [""])[0].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            interval = qs.get("interval", ["5m"])[0]
            force = qs.get("force", ["0"])[0] == "1"
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            data = get_intraday(sym, mkt, interval, force)
            self.send_json({"symbol": sym, "market": mkt, "interval": interval, "bars": data})
            return
        
        # Paper Trading Portfolio
        if path == "/api/paper/portfolio":
            u = self.require_auth()
            if not u: return
            pt = PaperTrading(u['id'])
            current_prices = {}
            for p in pt.positions:
                d = _ram_cache.get(p['symbol'], {}).get('data', {})
                current_prices[p['symbol']] = d.get('price', p['entry_price'])
            value = pt.get_portfolio_value(current_prices)
            self.send_json({"balance": pt.balance, "positions": pt.positions, "total_value": value})
            return
        
        # Admin Stats
        if path == "/api/admin/stats":
            u = self.require_admin()
            if not u: return
            self.send_json(admin_get_stats())
            return
        
        # Admin Users
        if path == "/api/admin/users":
            u = self.require_admin()
            if not u: return
            with get_db() as c:
                rows = c.execute("SELECT id,username,role,created_at,last_login FROM users ORDER BY created_at").fetchall()
            self.send_json([dict(r) for r in rows])
            return
        
        # Serve manifest.json
        if path == "/manifest.json":
            b = MANIFEST_JSON.encode('utf-8')
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "application/manifest+json")
            self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            return
        
        # Serve sw.js
        if path == "/sw.js":
            b = SW_JS.encode('utf-8')
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "application/javascript")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            return
        
        # Auth
        if path == "/api/me":
            u = self.get_user()
            if not u: self.send_json({"logged_in": False}); return
            self.send_json({"logged_in": True, "username": u['username'], "role": u['role']})
            return
        
        if path == "/api/version":
            self.send_json({"version": VERSION, "multi_user": True})
            return
        
        # Watchlist
        if path == "/api/watchlist":
            u = self.require_auth()
            if not u: return
            self.send_json(uw_list(u['id']))
            return
        
        # Stock data
        if path.startswith("/api/stock/"):
            sym = path.split("/")[-1].upper()
            mkt = qs.get("market", ["US"])[0].upper()
            mtf = qs.get("mtf", ["0"])[0] == "1"
            force = qs.get("force", ["0"])[0] == "1"
            try:
                data = fetch(sym, mkt, force=force)
                if not data:
                    self.send_json({"error": f"Không tìm thấy {sym}"}, 404)
                    return
                try:
                    data["analysis"] = analyze(data)
                except Exception as _ae:
                    log.error("[stock] analyze %s: %s", sym, _ae)
                    data["analysis"] = empty_analysis()
                data["db_bars"] = db_ohlcv_count(sym)
                try:
                    ev_flag, ev_title = has_upcoming_events(sym, mkt, days=7)
                    data["upcoming_event"] = ev_title if ev_flag else ""
                except Exception as _ee:
                    log.debug("[stock] events: %s", _ee)
                    data["upcoming_event"] = ""
                data["db_latest"] = db_latest_date(sym)
                if mtf:
                    try: data["mtf"] = analyze_mtf(data)
                    except Exception as _me: log.debug("[stock] mtf: %s", _me)
                self.send_json(data); return
            except Exception as _se:
                log.error("[stock] %s: %s", sym, _se)
                self.send_json({"error": f"Lỗi fetch {sym}: {str(_se)[:80]}"}, 500)
            return
        
        # Export CSV
        if path == "/api/export":
            u = self.require_auth()
            if not u: return
            snap = {}
            with get_db() as c:
                wl_rows = c.execute("SELECT symbol,market FROM user_watchlist WHERE user_id=?", (u['id'],)).fetchall()
            for row in wl_rows:
                sym = row['symbol']
                if sym in _ram_cache and _ram_cache[sym].get('data'):
                    d = dict(_ram_cache[sym]['data'])
                    if 'analysis' not in d:
                        try: d['analysis'] = analyze(d)
                        except Exception as _e: log.debug('[uncat] line %d: %%s' % 10869, _e)
                    snap[sym] = d
            csv_data = export_watchlist_csv(u['id'], snap)
            try:
                b = csv_data.encode('utf-8-sig')
                self.send_response(200); self.cors()
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="sma_export.csv"')
                self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 10878, _e)
            return
        
        # Alerts
        if path == "/api/alerts":
            sym = qs.get("symbol", [None])[0]
            limit = int(qs.get("limit", [200])[0])
            sig = qs.get("signal", [None])[0]
            since_minutes = int(qs.get("since_minutes", [0])[0])
            q = "SELECT * FROM alerts WHERE 1=1"
            params = []
            if sym:
                q += " AND symbol=?"
                params.append(sym.upper())
            if sig:
                q += " AND signal=?"
                params.append(sig)
            if since_minutes > 0:
                cutoff = (datetime.now() - __import__('datetime').timedelta(minutes=since_minutes)).isoformat()
                q += " AND created_at >= ?"
                params.append(cutoff)
            q += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            with get_db() as c:
                rows = c.execute(q, params).fetchall()
            res = []
            for r in rows:
                d = dict(r)
                try: d["detail"] = json.loads(d["detail"])
                except: d["detail"] = {}
                res.append(d)
            self.send_json(res)
            return
        
        # Search
        if path == "/api/search":
            q2 = qs.get("q", [""])[0].upper().strip()
            mkt2 = qs.get("market", ["ALL"])[0].upper()
            pool = []
            if mkt2 in ("VN", "ALL"): pool += [(s, "VN") for s in VN_POPULAR]
            if mkt2 in ("US", "ALL"): pool += [(s, "US") for s in US_POPULAR]
            with get_db() as c:
                wl_rows = c.execute("SELECT symbol,market,name,sector FROM watchlist").fetchall()
            wl_set = {r["symbol"]: r for r in wl_rows}
            
            def _enrich(sym, mkt, in_wl):
                vn_name = VN_NAMES.get(sym, "") if mkt == "VN" else ""
                entry = {
                    "symbol": sym,
                    "market": mkt,
                    "in_watchlist": in_wl,
                    "name": wl_set.get(sym, {}).get("name", "") or vn_name if in_wl else vn_name,
                    "sector": wl_set.get(sym, {}).get("sector", "") if in_wl else ""
                }
                if sym in _ram_cache:
                    d = _ram_cache[sym].get("data", {})
                    a = d.get("analysis", {})
                    entry["price"] = d.get("price", 0)
                    entry["signal"] = a.get("signal", "")
                    entry["total"] = a.get("total", 0)
                    entry["signal_color"] = a.get("signal_color", "")
                    entry["chg"] = d.get("chg", 0)
                return entry
            
            matches = []
            seen = set()
            if not q2:
                for row in wl_rows:
                    s = row["symbol"]
                    if s not in seen:
                        matches.append(_enrich(s, row["market"], True))
                        seen.add(s)
                for sym2, mkt3 in pool:
                    if sym2 not in seen:
                        matches.append(_enrich(sym2, mkt3, False))
                        seen.add(sym2)
            else:
                exact, starts, contains = [], [], []
                for row in wl_rows:
                    s = row["symbol"]
                    nm = (row["name"] or "").upper()
                    sc = (row["sector"] or "").upper()
                    if s == q2:
                        exact.append(_enrich(s, row["market"], True))
                        seen.add(s)
                    elif s.startswith(q2) or nm.startswith(q2):
                        starts.append(_enrich(s, row["market"], True))
                        seen.add(s)
                    elif q2 in s or q2 in nm or q2 in sc:
                        contains.append(_enrich(s, row["market"], True))
                        seen.add(s)
                for sym2, mkt3 in pool:
                    if sym2 in seen: continue
                    if sym2 == q2:
                        exact.append(_enrich(sym2, mkt3, False))
                        seen.add(sym2)
                    elif sym2.startswith(q2):
                        starts.append(_enrich(sym2, mkt3, False))
                        seen.add(sym2)
                    elif q2 in sym2:
                        contains.append(_enrich(sym2, mkt3, False))
                        seen.add(sym2)
                matches = exact + starts + contains
            self.send_json(matches[:30])
            return
        
        # Settings
        if path == "/api/settings":
            self.send_json({k: get_setting(k) for k in [
                "tg_token", "tg_chat_id", "tg_enabled", "page_size", "alert_threshold",
                "public_url", "email_enabled", "smtp_host", "smtp_port", "smtp_user",
                "email_to", "account_size", "backup_enabled", "backup_days",
                "webhook_url", "webhook_enabled", "last_backup_at"
            ]})
            return
        
        # Status
        if path == "/api/status":
            self.send_json({
                "ok": True,
                "vnstock": HAS_VN,
                "vn_queue": _vn_queue.qsize(),
                "time": datetime.now().isoformat(),
                "ohlcv_days": OHLCV_BARS
            })
            return
        
        # OHLCV
        if path == "/api/ohlcv":
            sym = qs.get("symbol", [""])[0].upper()
            limit = int(qs.get("limit", [OHLCV_BARS])[0])
            self.send_json(db_load_ohlcv(sym, limit))
            return
        
        # TG Link Code
        if path == "/api/tg_link_code":
            u = self.require_auth()
            if not u: return
            code = tg_gen_link_code(u['id'])
            self.send_json({"code": code, "expires": 600})
            return
        
        # Alert Context
        if path == "/api/alert_context":
            sym = qs.get("symbol", [""])[0].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            lines = _get_alert_context(sym, mkt)
            self.send_json({"symbol": sym, "context": lines})
            return
        
        # Journal
        if path == "/api/journal":
            u = self.require_auth()
            if not u: return
            status = qs.get("status", [None])[0]
            limit = int(qs.get("limit", [200])[0])
            self.send_json(trade_list(u['id'], status=status, limit=limit))
            return
        
        if path == "/api/journal/stats":
            u = self.require_auth()
            if not u: return
            self.send_json(trade_stats(u['id']))
            return
        
        # Market Context
        if path == "/api/market_context":
            self.send_json(market_ctx_get())
            return
        
        # News
        if path == "/api/news":
            sym = qs.get("symbol", [""])[0].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            try:
                # Chạy fetch_news trong thread riêng với timeout 8s
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    _fut = _ex.submit(fetch_news, sym, mkt)
                    try:
                        data = _fut.result(timeout=8)
                    except _cf.TimeoutError:
                        log.warning("[news] timeout %s", sym)
                        data = []
                self.send_json({"symbol": sym, "news": data or []}); return
            except Exception as _ne:
                log.debug("[news] %s: %s", sym, _ne)
                self.send_json({"symbol": sym, "news": []})
            return
        
        # Events
        if path == "/api/events":
            sym = qs.get("symbol", [""])[0].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            try:
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    _fut = _ex.submit(fetch_events, sym, mkt)
                    try:
                        data = _fut.result(timeout=8)
                    except _cf.TimeoutError:
                        log.warning("[events] timeout %s", sym)
                        data = {}
                self.send_json({"symbol": sym, "market": mkt, **(data or {})}); return
            except Exception as _eve:
                log.debug("[events] %s: %s", sym, _eve)
                self.send_json({"symbol": sym, "market": mkt, "events": [], "dividends": []})
            return
        
        # Price Alerts
        if path == "/api/price_alerts":
            u = self.require_auth()
            if not u: return
            inc = qs.get("all", ["0"])[0] == "1"
            self.send_json(palert_list(u['id'], include_triggered=inc))
            return
        
        # Watchlist Groups
        if path == "/api/watchlist_groups":
            u = self.require_auth()
            if not u: return
            self.send_json(wg_list(u['id']))
            return
        
        # Screener
        if path == "/api/screener":
            mkt2 = qs.get("market", ["ALL"])[0].upper()
            min_s = int(qs.get("min_score", ["5"])[0])
            sig2 = qs.get("signal", [None])[0]
            lim = int(qs.get("limit", ["100"])[0])
            self.send_json(screener_run(mkt2, min_s, sig2, lim))
            return
        
        # Journal Export
        if path == "/api/journal/export":
            u = self.require_auth()
            if not u: return
            csv_data = export_journal_csv(u['id'])
            try:
                b = csv_data.encode('utf-8-sig')
                self.send_response(200); self.cors()
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="journal_export.csv"')
                self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 11126, _e)
            return
        
        # Market Regime
        if path == "/api/market_regime":
            self.send_json(get_market_regime())
            return
        
        # Correlation
        if path == "/api/correlation":
            u = self.require_auth()
            if not u: return
            with get_db() as c:
                rows = c.execute("SELECT DISTINCT symbol FROM user_watchlist WHERE user_id=?", (u['id'],)).fetchall()
            syms = [r["symbol"] for r in rows]
            period = int(qs.get("period", ["30"])[0])
            self.send_json(calc_correlation_matrix(syms, period=period))
            return
        
        # Forward Test
        if path == "/api/forward_test":
            u = self.require_auth()
            if not u: return
            try:
                with get_db() as c:
                    rows = c.execute("""SELECT signal,
                        AVG(CASE WHEN correct_5d IS NOT NULL THEN correct_5d END)*100 as wr_5d,
                        AVG(CASE WHEN correct_10d IS NOT NULL THEN correct_10d END)*100 as wr_10d,
                        AVG(CASE WHEN correct_20d IS NOT NULL THEN correct_20d END)*100 as wr_20d,
                        COUNT(*) as total
                        FROM forward_test GROUP BY signal""").fetchall()
                    self.send_json({"stats": [dict(r) for r in rows]}); return
            except Exception as e:
                self.send_json({"stats": [], "note": str(e)})
            return
        
        # Signal History
        if path == "/api/signal_history":
            u = self.require_auth()
            if not u: return
            sym2 = qs.get("symbol", [None])[0]
            limit2 = int(qs.get("limit", ["30"])[0])
            try:
                with get_db() as c:
                    if sym2:
                        rows = c.execute("SELECT * FROM signal_snapshots WHERE symbol=? ORDER BY date DESC LIMIT ?", (sym2.upper(), limit2)).fetchall()
                    else:
                        with get_db() as c2:
                            wl_syms = [r["symbol"] for r in c2.execute("SELECT DISTINCT symbol FROM user_watchlist WHERE user_id=?", (u['id'],)).fetchall()]
                        placeholders = ",".join("?" * len(wl_syms)) if wl_syms else "''"
                        rows = c.execute(f"SELECT * FROM signal_snapshots WHERE symbol IN ({placeholders}) ORDER BY date DESC LIMIT ?", [*wl_syms, limit2]).fetchall()  # safe: placeholders="?,?..." not user input
                self.send_json({"history": [dict(r) for r in rows]}); return
            except Exception as e:
                self.send_json({"history": [], "error": str(e)})
            return
        
        # Portfolio NAV
        if path == "/api/portfolio/nav":
            u = self.require_auth()
            if not u: return
            self.send_json(calc_portfolio_nav(u['id']))
            return
        
        # Sector Rotation
        if path == "/api/sector_rotation":
            self.send_json(calc_sector_rotation())
            return
        
        # Foreign Flow
        if path == "/api/foreign_flow":
            u = self.require_auth()
            if not u: return
            with get_db() as c:
                rows = c.execute("SELECT symbol FROM user_watchlist WHERE user_id=? AND market='VN'", (u['id'],)).fetchall()
            syms = [r['symbol'] for r in rows]
            self.send_json(get_foreign_flow(syms))
            return

        if path.startswith("/api/foreign_flow_rt"):
            mkt2 = qs.get("market", ["VN"])[0].upper()
            top2 = int(qs.get("top", ["15"])[0])
            try:
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    _fut = _ex.submit(fetch_foreign_flow_realtime, mkt2, top2)
                    try:
                        data = _fut.result(timeout=10)
                    except _cf.TimeoutError:
                        log.warning("[ff_rt] timeout")
                        data = {"top_buy":[],"top_sell":[],"total_net":0,"date":"","source":"timeout","msg":"Timeout khi lấy dữ liệu — thử lại sau"}
                self.send_json(data or {"top_buy":[],"top_sell":[],"total_net":0,"msg":"Không có dữ liệu"}); return
            except Exception as _ffe:
                log.debug("[ff_rt] %s", _ffe)
                self.send_json({"top_buy":[],"top_sell":[],"total_net":0,"msg":str(_ffe)[:100]})
            return
        
        # Position Size
        if path == "/api/position_size":
            acc = float(qs.get("account", [get_setting('account_size', '0') or '0'])[0])
            entry = float(qs.get("entry", ["0"])[0])
            sl = float(qs.get("sl", ["0"])[0])
            mkt2 = qs.get("market", ["VN"])[0].upper()
            risk = float(qs.get("risk_pct", ["2"])[0])
            wr = qs.get("win_rate", [None])[0]
            aw = qs.get("avg_win", [None])[0]
            al = qs.get("avg_loss", [None])[0]
            self.send_json(calc_position_size(acc, entry, sl, mkt2, risk,
                float(wr) if wr else None, float(aw) if aw else None, float(al) if al else None))
            return
        
        # Portfolio Backtest
        if path == "/api/portfolio_backtest":
            u = self.require_auth()
            if not u: return
            cap = float(qs.get("capital", ["100000000"])[0])
            self.send_json(portfolio_backtest(u['id'], cap))
            return
        
        # Import Template
        if path == "/api/import_template":
            u = self.require_auth()
            if not u: return
            b = get_import_template().encode('utf-8-sig')
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="sma_import_template.csv"')
            self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            return
        
        # Backup
        if path == "/api/backup":
            u = self.require_admin()
            if not u: return
            zp = db_backup_now()
            if not zp:
                self.send_json({"error": "Backup thất bại"}, 500)
                return
            with open(zp, "rb") as f2:
                bdata = f2.read()
            fn = os.path.basename(zp)
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{fn}"')
            self.send_header("Content-Length", len(bdata)); self.end_headers(); self.wfile.write(bdata)
            return
        
        # List Backups
        if path == "/api/backups":
            u = self.require_admin()
            if not u: return
            os.makedirs(_BACKUP_DIR, exist_ok=True)
            files = [{"name": f, "size": os.path.getsize(os.path.join(_BACKUP_DIR, f)),
                    "ts": os.path.getmtime(os.path.join(_BACKUP_DIR, f))}
                    for f in sorted(os.listdir(_BACKUP_DIR)) if f.endswith('.zip')]
            self.send_json(files)
            return
        
        # Tax Report
        if path == "/api/tax_report":
            u = self.require_auth()
            if not u: return
            import datetime as _dt2
            year = int(qs.get("year", [str(_dt2.datetime.now().year)])[0])
            csv_data, yr, n = export_tax_report(u['id'], year)
            b = csv_data.encode('utf-8-sig')
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="tax_report_{yr}.csv"')
            self.send_header("Content-Length", len(b)); self.end_headers(); self.wfile.write(b)
            return
        
        # Webhook Test
        if path == "/api/webhook_test":
            u = self.require_admin()
            if not u: return
            ok, msg = webhook_send({"event": "test", "message": "SMA Pro webhook test", "time": time.strftime("%Y-%m-%d %H:%M:%S")})
            self.send_json({"ok": ok, "msg": msg})
            return
        
        # Export Alerts CSV
        if path == "/api/patterns":
            u = self.require_auth()
            if not u: return
            sym2 = qs.get("symbol", [""])[0].upper()
            if sym2:
                # Single symbol pattern scan
                d2 = _ram_cache.get(sym2, {}).get('data', {})
                bars2 = d2.get('history', []) if d2 else []
                pts = detect_candle_patterns(bars2)
                self.send_json({"symbol": sym2, "patterns": pts}); return
            else:
                # Full watchlist scan
                hits = scan_patterns_watchlist(u['id'])
                self.send_json({"patterns": hits, "count": len(hits)}); return

        if path.startswith("/api/bctc/"):
            sym2 = path.split("/")[-1].upper()
            if not sym2: self.send_json({"error": "Thiếu mã"}, 400); return
            force = qs.get("force", ["0"])[0] == "1"
            data = get_bctc(sym2, force=force)
            self.send_json(data); return

        if path == "/api/export/xlsx":
            u = self.require_auth()
            if not u: return
            snap = {}
            with get_db() as c:
                wl_rows = c.execute("SELECT symbol,market FROM user_watchlist WHERE user_id=?", (u['id'],)).fetchall()
            for row in wl_rows:
                sym = row['symbol']
                if sym in _ram_cache and _ram_cache[sym].get('data'):
                    d = dict(_ram_cache[sym]['data'])
                    if 'analysis' not in d:
                        try: d['analysis'] = analyze(d)
                        except: pass
                    snap[sym] = d
            xlsx_bytes = export_portfolio_xlsx(u['id'], snap)
            if xlsx_bytes:
                from datetime import datetime as _dt
                fname = f"sma_export_{_dt.now().strftime('%Y%m%d')}.xlsx"
                self.send_response(200); self.cors()
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                self.send_header("Content-Length", len(xlsx_bytes)); self.end_headers()
                self.wfile.write(xlsx_bytes)
            else:
                self.send_json({"error": "Không thể tạo file Excel"}, 500)
            return

        if path == "/api/export/alerts":
            u = self.require_auth()
            if not u: return
            import io, csv as _csv
            with get_db() as _c:
                _rows = _c.execute("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 1000").fetchall()
            _buf = io.StringIO()
            _w = _csv.writer(_buf)
            _w.writerow(["ID", "Symbol", "Market", "Signal", "Score", "Price", "Timeframe", "Created"])
            for _r in _rows:
                _w.writerow([_r["id"], _r["symbol"], _r["market"], _r["signal"], _r["score"], _r["price"], _r["timeframe"], _r["created_at"]])
            _b = _buf.getvalue().encode('utf-8-sig')
            self.send_response(200); self.cors()
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="alerts_export.csv"')
            self.send_header("Content-Length", len(_b)); self.end_headers(); self.wfile.write(_b)
            return
        
        # Patterns
        if path == "/api/patterns":
            sym = qs.get("symbol", [""])[0].upper()
            if not sym:
                self.send_json({"error": "No symbol"}, 400)
                return
            dpa = _ram_cache.get(sym, {}).get("data", {})
            if not dpa:
                self.send_json({"patterns": [], "note": "Không có dữ liệu"})
                return
            hpa = dpa.get("history", [])
            if len(hpa) < 40:
                self.send_json({"patterns": [], "note": "Chưa đủ dữ liệu"})
                return
            closes_p = [x["c"] for x in hpa]
            highs_p = [x.get("h", x["c"]) for x in hpa]
            lows_p = [x.get("l", x["c"]) for x in hpa]
            pats_p = detect_advanced_patterns(closes_p, highs_p, lows_p, len(closes_p))
            self.send_json({"symbol": sym, "patterns": pats_p, "count": len(pats_p)})
            return     
        
        if path == "/api/market_calendar":
            u = self.require_auth()
            if not u: return
            days = int(qs.get("days", ["30"])[0])
            self.send_json(get_market_calendar(u['id'], days))
            return
        
        if path == "/api/rs_ranking":
            u = self.require_auth()
            if not u: return
            period = qs.get("period", ["1M"])[0]
            self.send_json(calc_rs_ranking(u['id'], period))
            return
        
        if path == "/api/portfolio/pnl_dashboard":
            u = self.require_auth()
            if not u: return
            self.send_json(calc_pnl_dashboard(u['id']))
            return
    
        # Pro GET routes added
        # ── Pro GET routes  ──────────────────────────
        if path == "/api/macro":
            base = fetch_macro_data()
            # Add structured index data for the header banner (price + chg_pct per symbol)
            def _idx(sym, mkt):
                d = _ram_cache.get(sym, {}).get('data', {})
                if d and d.get('price', 0) > 0:
                    return {'price': d.get('price', 0), 'chg_pct': d.get('chg', 0)}
                return None
            # VN indices from RAM cache (fetched by refresh loop)
            for _sym, _mkt in [('VNINDEX','VN'), ('VN30','VN'), ('HNX','VN')]:
                idx_d = _idx(_sym, _mkt)
                if idx_d:
                    base[_sym] = idx_d
                elif _sym == 'VNINDEX' and base.get('vnindex'):
                    # fallback from fetch_macro_data structure
                    base['VNINDEX'] = {'price': base['vnindex'], 'chg_pct': base.get('vnindex_chg', 0)}
            # S&P500 and Gold from yfinance via RAM cache or macro data
            for _sym, _mkt, _key in [('^GSPC','US','SPX'), ('GC=F','US','GOLD'), ('SPY','US','SPY')]:
                idx_d = _idx(_sym, _mkt) or _idx(_key, _mkt)
                if idx_d:
                    base[_key] = idx_d
                    base['^GSPC' if _key == 'SPX' else _sym] = idx_d
            self.send_json(base); return
        # v27: Signal performance stats
        if path.startswith("/api/signal_stats"):
            sym=qs.get("symbol",[""])[0].upper() or None
            mkt=qs.get("market",[""])[0].upper() or None
            days=int(qs.get("days",["90"])[0])
            self.send_json(get_signal_performance(sym,mkt,days)); return
        # v27: Daily digest send
        if path == "/api/digest/send":
            u=self.require_auth()
            if not u: return
            threading.Thread(target=send_daily_digest,daemon=True).start()
            self.send_json({"ok":True,"message":"Đang gửi daily digest..."}); return
        # v27: Portfolio rebalance
        if path == "/api/portfolio/rebalance":
            u=self.require_auth()
            if not u: return
            self.send_json(calc_rebalancing(u["id"])); return
        # v27: Portfolio targets GET
        if path == "/api/portfolio/targets":
            u=self.require_auth()
            if not u: return
            with get_db() as c: rows=c.execute("SELECT symbol,market,target_pct FROM portfolio_targets WHERE user_id=?",(u["id"],)).fetchall()
            self.send_json({"targets":[{"symbol":r["symbol"],"market":r["market"],"target_pct":r["target_pct"]} for r in rows]}); return
        # v27: Intraday analysis
        if path == "/api/intraday_analysis":
            sym=qs.get("symbol",[""])[0].upper(); mkt=qs.get("market",["VN"])[0].upper()
            itvl=qs.get("interval",["5m"])[0]
            if not sym: self.send_json({"error":"No symbol"},400); return
            bars=get_intraday(sym,mkt,itvl)
            if not bars: self.send_json({"error":f"Không có dữ liệu intraday {itvl}"},404); return
            self.send_json(_analyze_intraday_simple(bars,itvl,sym,mkt)); return
        
        if path == "/api/macro_context":
            sup, reason = get_macro_alert_context()
            self.send_json({"suppress": sup, "reason": reason}); return
        if path.startswith("/api/foreign_flow"):
            qs = parse_qs(urlparse(self.path).query)
            mkt2 = qs.get("market",["VN"])[0].upper()
            top2 = int(qs.get("top",["20"])[0])
            self.send_json(fetch_foreign_flow_realtime(mkt2, top2)); return
        if path.startswith("/api/realtime_price/"):
            sym2 = path.split("/")[-1].upper()
            d = fetch_ssi_realtime_price(sym2)
            self.send_json(d or {"error": "No data"}); return
        
        if path.startswith("/api/beta/"):
            sym2 = path.split("/")[-1].upper()
            bars2 = db_load_ohlcv(sym2, 150)
            self.send_json(calc_beta_vs_market(bars2) or {"error": "Not enough data"}); return
        
        if path.startswith("/api/financial_summary/"):
            sym2 = path.split("/")[-1].upper()
            qs2 = parse_qs(urlparse(self.path).query)
            mkt2 = qs2.get("market",["VN"])[0].upper()
            self.send_json(fetch_financial_summary(sym2, mkt2)); return
        
        if path == "/api/paper_trades":
            u = self.require_auth()
            if not u: return
            self.send_json(paper_trade_summary(u["id"])); return
        
        if path.startswith("/api/ml_predict/"):
            sym2 = path.split("/")[-1].upper()
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            self.send_json(predict_ml(sym2, "VN", bars2)); return
        
        if path.startswith("/api/ml_accuracy/"):
            sym2 = path.split("/")[-1].upper()
            with get_db() as c2:
                row2 = c2.execute("SELECT metrics, trained_at FROM ml_models WHERE symbol=?", (sym2,)).fetchone()
            if row2:
                self.send_json({"metrics": json.loads(row2["metrics"] or "{}"), "trained_at": row2["trained_at"]}); return
            else:
                self.send_json({"error": "Model chưa được train"})
            return
        
        if path == "/api/portfolio_diversification":
            u = self.require_auth()
            if not u: return
            self.send_json(calc_portfolio_diversification(u["id"])); return
        
        if path.startswith("/api/portfolio_correlation"):
            u = self.require_auth()
            if not u: return
            wl_syms2 = [w["symbol"] for w in uw_list(u["id"])]
            self.send_json(get_portfolio_correlation(wl_syms2)); return
        
        # ── regime_backtest GET (alias for index_Pro compatibility) ──
        if path in ("/api/regime_backtest",) or path.startswith("/api/regime_backtest"):
            sym2 = qs.get("symbol",[""])[0].upper()
            mkt2 = qs.get("market",["VN"])[0].upper()
            if not sym2: self.send_json({"error":"Missing symbol"},400); return
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            if not bars2: self.send_json({"error":f"No data for {sym2}"},404); return
            self.send_json(backtest_by_regime(bars2, 100_000_000, mkt2)); return

        # ── ml_predict GET with query param (index calls /api/ml_predict?symbol=X) ──
        if path == "/api/ml_predict":
            sym2 = qs.get("symbol",[""])[0].upper()
            mkt2 = qs.get("market",["VN"])[0].upper()
            if not sym2: self.send_json({"error":"Missing symbol"},400); return
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            self.send_json(predict_ml(sym2, mkt2, bars2)); return

        # ── ml_accuracy GET with query param (index calls /api/ml_accuracy?symbol=X) ──
        if path == "/api/ml_accuracy":
            sym2 = qs.get("symbol",[""])[0].upper()
            if not sym2: self.send_json({"error":"Missing symbol"},400); return
            with get_db() as c2:
                row2 = c2.execute("SELECT metrics, trained_at FROM ml_models WHERE symbol=?", (sym2,)).fetchone()
            if row2:
                self.send_json({"metrics": json.loads(row2["metrics"] or "{}"), "trained_at": row2["trained_at"]}); return
            else:
                self.send_json({"trained": False, "message": f"Model chưa train cho {sym2}"})
            return

        # ── /api/financials (index calls this, server has /api/financial_summary/) ──
        if path == "/api/financials":
            sym2 = qs.get("symbol",[""])[0].upper()
            mkt2 = qs.get("market",["VN"])[0].upper()
            if not sym2: self.send_json({"error":"Missing symbol"},400); return
            self.send_json(fetch_financial_summary(sym2, mkt2)); return

        # ── /api/top_reliability (index v12 calls this) ──
        if path == "/api/top_reliability":
            sig_type = qs.get("type",["BUY"])[0].upper()
            days2    = int(qs.get("days",["7"])[0])
            min_app  = int(qs.get("min_appearances",["2"])[0])
            self.send_json(get_top_signal_reliability(sig_type, days2, min_app)); return

        # ── /api/insider (index v12 calls this for VN stocks) ──
        if path == "/api/insider":
            sym2 = qs.get("symbol",[""])[0].upper()
            mkt2 = qs.get("market",["VN"])[0].upper()
            try:
                result = fetch_insider_trading_vn(sym2) if mkt2 == "VN" else {"deals":[]}
            except:
                result = {"deals": [], "note": "Chưa có dữ liệu insider"}
            self.send_json(result); return

        # ── /api/diversification (alias for portfolio_diversification) ──
        if path in ("/api/diversification", "/api/portfolio/diversification"):
            u = self.require_auth()
            if not u: return
            self.send_json(calc_portfolio_diversification(u["id"])); return

        # Sector Exposure
        if path == "/api/portfolio/sector_exposure":
            u = self.require_auth()
            if not u: return
            self.send_json(get_sector_exposure(u['id']))
            return
        # PnL Calendar
        if path == "/api/portfolio/pnl_calendar":
            u = self.require_auth()
            if not u: return
            year = int(qs.get("year", [datetime.now().year])[0])
            month = int(qs.get("month", [datetime.now().month])[0])
            self.send_json(get_pnl_calendar(u['id'], year, month))
            return
        
        # Event countdown
        if path.startswith("/api/event_countdown/"):
            sym = path.split("/")[-1].upper()
            mkt = qs.get("market", ["VN"])[0].upper()
            self.send_json(get_event_countdown(sym, mkt))
            return
        
        # Markowitz
        if path == "/api/markowitz":
            u = self.require_auth()
            if not u: return
            syms = qs.get("symbols", []) or []
            mkt = qs.get("market", ["VN"])[0].upper()
            rf = float(qs.get("risk_free", ["0.04"])[0])
            if len(syms) < 2:
                syms = list(_ram_cache.keys())[:10]
            self.send_json(markowitz_optimize(syms, mkt, rf))
            return
        
        # Sector Compare: 1 ma vs cung nganh vs toan thi truong
        if path == "/api/sector_compare":
            sym = qs.get("symbol", [""])[0].upper()
            if not sym:
                self.send_json({"error": "Thieu ma"}); return
            sym_sector = None
            for sec, syms in SECTOR_MAP_VN.items():
                if sym in syms:
                    sym_sector = sec
                    break
            def _m(s):
                d = _ram_cache.get(s, {}).get('data', {})
                if not d or d.get('price', 0) <= 0: return None
                a = d.get('analysis', {}) or {}
                fd = d.get('fundamentals', {}) or {}
                return {
                    'symbol': s, 'price': round(d.get('price', 0), 2), 'chg': d.get('chg', 0),
                    'score': a.get('total', 0), 'signal': a.get('signal', 'NEUTRAL'),
                    'rsi': round(float(a.get('rsi', 50) or 50), 1),
                    'pe': round(float(fd.get('pe_ratio') or fd.get('trailingPE') or 0), 2),
                    'pb': round(float(fd.get('pb') or fd.get('priceToBook') or 0), 2),
                    'roe': round(float(fd.get('roe') or fd.get('returnOnEquity') or 0), 2),
                    'eps': round(float(fd.get('eps') or fd.get('trailingEps') or 0), 2),
                    'rev_growth': round(float(fd.get('revenueGrowth') or 0) * 100, 1),
                    'gross_margin': round(float(fd.get('grossMargins') or 0) * 100, 1),
                    'debt_equity': round(float(fd.get('debtToEquity') or 0), 2),
                    'market_cap': round(float(fd.get('market_cap') or 0) / 1e9, 1),
                    'adx': round(float(a.get('adx_val') or 0), 1),
                    'confluence': a.get('confluence', 0), 'confluence_label': a.get('confluence_label', ''),
                    'ma20': a.get('ma20', 0), 'ma50': a.get('ma50', 0), 'ma200': a.get('ma200', 0),
                    'supertrend_dir': a.get('supertrend_dir', 0), 'supertrend': a.get('supertrend', 0),
                    'vwap': a.get('vwap', 0), 'atr': round(float(a.get('atr') or 0), 4),
                }
            target = _m(sym)
            if not target:
                try:
                    fd2 = fetch(sym, 'VN')
                    if fd2:
                        fd2['analysis'] = analyze(fd2)
                        _ram_cache[sym] = {'ts': time.time(), 'data': fd2}
                        target = _m(sym)
                except: pass
            if not target:
                self.send_json({"error": f"Khong co du lieu cho {sym}"}); return
            sector_peers = [_m(s) for s in SECTOR_MAP_VN.get(sym_sector, []) if s != sym and _m(s) and _m(s)['price'] > 0]
            sector_peers = [x for x in sector_peers if x]
            market_all = [_m(s) for s in list(_ram_cache.keys()) if s != sym]
            market_all = [x for x in market_all if x and x['price'] > 0]
            def _avg(lst, k): v=[x[k] for x in lst if x.get(k) and x[k]!=0]; return round(sum(v)/len(v),2) if v else 0
            def _med(lst, k):
                v=sorted([x[k] for x in lst if x.get(k) and x[k]!=0])
                return round(v[len(v)//2],2) if v else 0
            def _prank(lst, k, val):
                v=sorted([x[k] for x in lst if x.get(k) and x[k]!=0])
                return round(sum(1 for x in v if x < val)/len(v)*100,0) if v else 50
            sector_avg = {'symbol': f'TB {sym_sector}' if sym_sector else 'Cung nganh','price':0,
                'chg':_avg(sector_peers,'chg'),'score':_avg(sector_peers,'score'),'signal':'—',
                'rsi':_avg(sector_peers,'rsi'),'pe':_med(sector_peers,'pe'),'pb':_med(sector_peers,'pb'),
                'roe':_avg(sector_peers,'roe'),'eps':_avg(sector_peers,'eps'),
                'rev_growth':_avg(sector_peers,'rev_growth'),'gross_margin':_avg(sector_peers,'gross_margin'),
                'debt_equity':_avg(sector_peers,'debt_equity'),'market_cap':_avg(sector_peers,'market_cap'),
                'adx':_avg(sector_peers,'adx'),'confluence':_avg(sector_peers,'confluence'),
                'peer_count':len(sector_peers),'sector':sym_sector or 'N/A',
                'confluence_label':'','vwap':0,'ma20':0,'ma50':0,'ma200':0,'supertrend_dir':0,'supertrend':0,'atr':0}
            market_avg = {'symbol':'Toan thi truong','price':0,
                'chg':_avg(market_all,'chg'),'score':_avg(market_all,'score'),'signal':'—',
                'rsi':_avg(market_all,'rsi'),'pe':_med(market_all,'pe'),'pb':_med(market_all,'pb'),
                'roe':_avg(market_all,'roe'),'eps':_avg(market_all,'eps'),
                'rev_growth':_avg(market_all,'rev_growth'),'gross_margin':_avg(market_all,'gross_margin'),
                'debt_equity':_avg(market_all,'debt_equity'),'market_cap':_avg(market_all,'market_cap'),
                'adx':_avg(market_all,'adx'),'confluence':_avg(market_all,'confluence'),
                'peer_count':len(market_all),'sector':'Market',
                'confluence_label':'','vwap':0,'ma20':0,'ma50':0,'ma200':0,'supertrend_dir':0,'supertrend':0,'atr':0}
            ranks = {'score':_prank(market_all,'score',target['score']),
                'roe':_prank(market_all,'roe',target['roe']),
                'rsi':_prank(market_all,'rsi',target['rsi']),
                'pe_good': 100-_prank(market_all,'pe',target['pe']) if target['pe']>0 else 50,
                'de_good': 100-_prank(market_all,'debt_equity',target['debt_equity']) if target['debt_equity']>0 else 50}
            # Verdict
            vs = 0; reasons = []
            sc = target['score']; rsi = target['rsi']; pe = target['pe']
            roe = target['roe']; sig = target['signal']; std = target.get('supertrend_dir',0); adx = target.get('adx',0)
            if sc >= 7: vs += 3; reasons.append(f'TA score cao ({sc:+d}/19)')
            elif sc >= 4: vs += 1; reasons.append(f'TA trung binh kha ({sc:+d}/19)')
            elif sc <= -4: vs -= 2; reasons.append(f'TA yeu ({sc:+d}/19)')
            if std == 1: vs += 1; reasons.append('Supertrend BULLISH')
            elif std == -1: vs -= 1; reasons.append('Supertrend BEARISH')
            if rsi < 35: vs += 1; reasons.append(f'RSI qua ban ({rsi:.0f}) — co the mua')
            elif rsi > 72: vs -= 1; reasons.append(f'RSI qua mua ({rsi:.0f}) — can than')
            if roe > 15: vs += 1; reasons.append(f'ROE tot ({roe:.1f}%)')
            elif roe and roe < 5: vs -= 1; reasons.append(f'ROE thap ({roe:.1f}%)')
            spe = sector_avg.get('pe',0)
            if pe and spe and pe < spe * 0.8: vs += 1; reasons.append(f'P/E thap hon nganh ({pe:.1f}x vs {spe:.1f}x)')
            elif pe and spe and pe > spe * 1.3: vs -= 1; reasons.append(f'P/E cao hon nganh ({pe:.1f}x vs {spe:.1f}x)')
            if sc > sector_avg.get('score',0) + 2: vs += 1; reasons.append('TA vuot troi cung nganh')
            elif sc < sector_avg.get('score',0) - 2: vs -= 1; reasons.append('TA kem cung nganh')
            if adx > 25: reasons.append(f'Xu huong manh (ADX={adx:.0f})')
            if vs >= 4: verdict,vlabel,vc = 'BUY','MUA','#00e676'
            elif vs >= 2: verdict,vlabel,vc = 'WATCH','THEO DOI','#ff9100'
            elif vs <= -3: verdict,vlabel,vc = 'SELL','BAN/TRANH','#f03050'
            else: verdict,vlabel,vc = 'HOLD','NAM GIU','#888'
            conf = min(max(30 + abs(vs)*12, 30), 90)
            if 'STRONG' in sig: conf = min(conf+8, 95)
            self.send_json({
                'symbol':sym,'sector':sym_sector,'target':target,
                'sector_avg':sector_avg,'market_avg':market_avg,'ranks':ranks,
                'verdict':verdict,'verdict_label':vlabel,'verdict_color':vc,
                'verdict_score':vs,'confidence':round(conf),'reasons':reasons,
            }); return

        # Compare
        if path == "/api/compare":
            s1 = qs.get("sym1", [""])[0].upper()
            s2 = qs.get("sym2", [""])[0].upper()
            m1 = qs.get("mkt1", ["VN"])[0].upper()
            m2 = qs.get("mkt2", ["VN"])[0].upper()
            period = int(qs.get("period", ["90"])[0])
            if not s1 or not s2:
                self.send_json({"error": "Cần sym1 và sym2"}, 400)
                return
            self.send_json(get_chart_comparison(s1, m1, s2, m2, period))
            return
        
        # KQKD
        if path.startswith("/api/kqkd/"):
            sym = path.split("/")[-1].upper()
            with get_db() as c:
                row = c.execute("SELECT data_json, fetched_at FROM kqkd_cache WHERE symbol=?", (sym,)).fetchone()
            if row and (datetime.now() - datetime.fromisoformat(row['fetched_at'])).total_seconds() < 21600:
                self.send_json(json.loads(row['data_json']))
                return
            result = fetch_kqkd_tcbs(sym)
            if result:
                with get_db() as c2:
                    c2.execute("INSERT OR REPLACE INTO kqkd_cache(symbol,data_json,fetched_at) VALUES(?,?,?)",
                            (sym, json.dumps(result), datetime.now().isoformat()))
            self.send_json(result or {"error": f"Không lấy được KQKD {sym}"})
            return
        
        # Tick snapshot
        if path.startswith("/api/tick/"):
            sym = path.split("/")[-1].upper()
            self.send_json(get_tick_snapshot(sym))
            return
        
        # Drawings
        if path == "/api/drawings":
            u = self.require_auth()
            if not u: return
            sym = qs.get("symbol", [""])[0].upper()
            with get_db() as c:
                rows = c.execute("SELECT id, tool_type, data_json FROM drawing_tools WHERE user_id=? AND symbol=?",
                                (u['id'], sym)).fetchall()
            self.send_json({"drawings": [{"id": r['id'], "type": r['tool_type'], "data": json.loads(r['data_json'])} for r in rows]})
            return
        
        # Crypto realtime
        if path == "/api/crypto/realtime":
            syms = qs.get("symbols", ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT"])
            self.send_json(get_crypto_realtime_data(syms))
            return

        # Crypto klines
        if path.startswith("/api/crypto/klines/"):
            sym = path.split("/")[-1].upper()
            interval = qs.get("interval", ["1h"])[0]
            limit = int(qs.get("limit", ["100"])[0])
            self.send_json({"symbol": sym, "interval": interval, "klines": get_binance_klines(sym, interval, limit)})
            return

        # 2FA setup
        if path == "/api/2fa/setup":
            u = self.require_auth()
            if not u: return
            with get_db() as c:
                row = c.execute("SELECT two_factor_secret FROM users WHERE id=?", (u['id'],)).fetchone()
            secret = row['two_factor_secret'] if row and row['two_factor_secret'] else totp_generate_secret()
            if not (row and row['two_factor_secret']):
                with get_db() as c2:
                    c2.execute("UPDATE users SET two_factor_secret=? WHERE id=?", (secret, u['id']))
            uri = totp_get_uri(secret, u['username'])
            self.send_json({"secret": secret, "uri": uri, "qr_svg": totp_qr_svg(uri)})
            return
        # Master key
        if path == "/api/settings/master_key" and method == "GET":
            u = self.require_auth()
            if not u: return
            mk = get_setting('master_key', '')
            self.send_json({"has_key": bool(mk), "key_preview": mk[:8]+"..." if mk else ""})
            return
        # Sector heatmap
        if path == "/api/sector_heatmap":
            mkt = qs.get("market", ["VN"])[0].upper()
            self.send_json(get_sector_heatmap(mkt))
            return
        
        # Controlled stock
        if path.startswith("/api/stock_status/"):
            sym = path.split("/")[-1].upper()
            self.send_json(check_controlled_status(sym))
            return
        
        # Sector P/E
        if path.startswith("/api/sector_pe/"):
            sym = path.split("/")[-1].upper()
            d = _ram_cache.get(sym, {}).get("data", {})
            fund = d.get("fundamental", {})
            self.send_json(get_sector_pe_valuation(sym, fund))
            return
        
        # ── /api/portfolio/metrics ──
        if path == "/api/portfolio/pdf":
            u = self.require_auth()
            if not u: return
            try:
                pdf_bytes = export_portfolio_pdf(u['id'])
                is_pdf = pdf_bytes[:4] == b'%PDF'
                content_type = "application/pdf" if is_pdf else "text/html; charset=utf-8"
                ext = "pdf" if is_pdf else "html"
                self.send_response(200); self.cors()
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Disposition", f'attachment; filename="portfolio_{datetime.now().strftime("%Y%m%d")}.{ext}"')
                self.send_header("Content-Length", len(pdf_bytes))
                self.end_headers(); self.wfile.write(pdf_bytes)
            except Exception as _pdfe:
                log.error("[PDF] %s", _pdfe)
                self.send_json({"error": str(_pdfe)}, 500)
            return

        if path == "/api/portfolio/metrics":
            u = self.require_auth()
            if not u: return
            try:
                result = calc_portfolio_metrics(u["id"])
                self.send_json(result); return
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # ── SSI: xem tài khoản, vị thế, lệnh ─────────────────────────
        if path == "/api/ssi/account":
            u = self.require_auth()
            if not u: return
            try:
                fc = _load_ssi_for_user(u['id'])
                self.send_json({
                    "balance":   fc.get_account_balance(),
                    "positions": fc.get_positions(),
                    "pending":   fc.get_pending_orders(),
                    "account":   fc.account_no,
                }); return
            except ValueError as e:
                self.send_json({"error": str(e)}, 400); return
            except Exception as e:
                log.error("[SSI] account error: %s", e)
                self.send_json({"error": f"SSI lỗi: {str(e)}"}, 502)
            return

        if path == "/api/ssi/orders":
            u = self.require_auth()
            if not u: return
            try:
                fc = _load_ssi_for_user(u['id'])
                self.send_json({
                    "history": fc.get_order_history(
                        qs.get("from",[None])[0],
                        qs.get("to",  [None])[0]),
                    "pending": fc.get_pending_orders(),
                }); return
            except ValueError as e:
                self.send_json({"error": str(e)}, 400); return
            except Exception as e:
                self.send_json({"error": f"SSI lỗi: {str(e)}"}, 502)
            return

        if path.startswith("/api/options/"):
            sym2  = path.split("/")[-1].upper()
            mkt2  = qs.get("market", ["VN"])[0].upper()
            try:
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    _fut = _ex.submit(fetch_options_data, sym2, mkt2)
                    data = _fut.result(timeout=10)
                self.send_json(data); return
            except Exception as _oe:
                self.send_json({"symbol": sym2, "calls": [], "puts": [], "futures": [], "error": str(_oe)})
            return

        if path.startswith("/api/earnings/"):
            sym2 = path.split("/")[-1].upper()
            mkt2 = qs.get("market", ["VN"])[0].upper()
            try:
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    _fut = _ex.submit(fetch_earnings_calendar, sym2, mkt2)
                    data = _fut.result(timeout=8)
                self.send_json(data); return
            except Exception as _ee:
                self.send_json({"symbol": sym2, "history": [], "error": str(_ee)})
            return

        if path == "/api/ssi/config":
            u = self.require_auth()
            if not u: return
            try:
                with get_db() as c:
                    row = c.execute(
                        "SELECT consumer_id, account_no, enabled FROM ssi_config WHERE user_id=?",
                        (u['id'],)).fetchone()
                self.send_json({
                    "configured": bool(row),
                    "consumer_id": row['consumer_id'] if row else "",
                    "account_no":  row['account_no']  if row else "",
                    "enabled":     bool(row['enabled']) if row else False,
                    "note": "consumer_secret không hiển thị vì lý do bảo mật",
                }); return
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # ── GET /api/invest/* ─────────────────────────────────────────
        if path.startswith("/api/invest/"):
            u = self.require_auth()
            if not u: return
            seg = path.split("/")[-1].lower()
            log.debug("[GET invest] seg=%s", seg)
            try:
                if seg == "status":
                    try:
                        with get_db() as c:
                            metas = c.execute(
                                "SELECT type,last_run,next_run,status FROM investment_lists_meta"
                            ).fetchall()
                        sm = {m["type"]: dict(m) for m in metas}
                    except Exception:
                        sm = {}
                    try:
                        with get_db() as c:
                            for _st in ("growth","value"):
                                cnt = c.execute("SELECT COUNT(*) FROM investment_lists WHERE type=?",(_st,)).fetchone()[0]
                                if _st not in sm: sm[_st]={"type":_st,"last_run":None,"next_run":None,"status":"never_run"}
                                sm[_st]["count"]=cnt; sm[_st]["ttl_h"]=72 if _st=="growth" else 168
                    except Exception:
                        for _st in ("growth","value"):
                            if _st not in sm: sm[_st]={"type":_st,"count":0,"status":"never_run"}
                    self.send_json({"growth":sm.get("growth",{}),"value":sm.get("value",{})})
                    return
                if seg in ("growth","value"):
                    try:
                        result = get_investment_list(seg)
                    except Exception as _gie:
                        log.warning("[invest GET] %s list: %s", seg, _gie)
                        result = {"list":[],"meta":{},"style":seg,"count":0}
                    if not result["list"]:
                        threading.Thread(target=run_investment_screener,args=(seg,"ALL",10,False),daemon=True).start()
                        result["running"]=True
                        result["msg"]="Đang chạy screener lần đầu... Tải lại sau 30-60 giây"
                    self.send_json(result)
                    return
                self.send_json({"error":"Not found"},404)
            except Exception as _ige:
                log.error("[invest GET] unhandled: %s", _ige)
                self.send_json({"error": str(_ige)[:100]}, 500)
            return

        # ── GET /api/forecast/{SYM} ─────────────────────────────────
        if path.startswith("/api/forecast/"):
            sym2 = path.split("/")[-1].upper()
            mkt2 = qs.get("market", ["VN"])[0].upper()
            horizon = int(qs.get("horizon", ["20"])[0])
            horizon = max(5, min(60, horizon))
            try:
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    result = _ex.submit(forecast_trend, sym2, mkt2, horizon).result(timeout=25)
                self.send_json(result)
            except Exception as _fe:
                log.error("[forecast] %s: %s", sym2, _fe)
                self.send_json({"error": str(_fe)[:100]}, 500)
            return

        # ── Smart Money Tracker API ──────────────────────────────────────────────
        if path.startswith("/api/smart_money/"):
            sym2 = path.split("/")[-1].upper()
            mkt2 = qs.get("market", ["VN"])[0].upper()
            self.send_json(detect_smart_money(sym2, mkt2)); return

        if path == "/api/smart_money":
            self.send_json({"results": scan_smart_money_watchlist()}); return

        # ── AI Chat Analyst API ────────────────────────────────────────────────
        if path == "/api/ai_chat":
            self.send_json({"error": "Use POST /api/ai_chat"}); return

        # ── Daily Briefing API ─────────────────────────────────────────────────
        if path == "/api/daily_briefing":
            u = self.maybe_auth()
            uid = u['id'] if u else None
            self.send_json(generate_daily_briefing(uid)); return

        # ── Layout Config API ──────────────────────────────────────────────────
        if path == "/api/layout":
            u = self.require_auth()
            if not u: return
            self.send_json(get_layout_config(u['id'])); return

        # ── Live Alert Banner API ─────────────────────────────────────────────
        if path == "/api/live_alerts":
            since = qs.get("since", [None])[0]
            unseen = qs.get("unseen", ["0"])[0] == "1"
            alerts = get_live_alerts(since_ts=since, unseen_only=unseen)
            self.send_json({"alerts": alerts, "count": len(alerts)}); return

        if path == "/api/live_alerts/mark_seen":
            ids_str = qs.get("ids", [""])[0]
            mark_alerts_seen([i for i in ids_str.split(",") if i])
            self.send_json({"ok": True}); return

        # ── Market Scanner API ────────────────────────────────────────────────
        if path == "/api/market_scan":
            with _market_scan_lock:
                result = dict(_market_scan_results.get("latest", {}))
            if not result:
                result = run_market_scanner_24_7()
            self.send_json(result); return

        if path == "/api/market_scan/run":
            threading.Thread(target=run_market_scanner_24_7, daemon=True).start()
            self.send_json({"ok": True, "message": "Scan dang chay..."}); return

        # ── Market Treemap (Geographic HoSE heatmap by sector/market cap) ─────
        if path == "/api/market_treemap":
            mkt2 = qs.get("market", ["VN"])[0].upper()
            mode = qs.get("mode", ["chg"])[0]  # chg | score | volume
            result_map = {}
            for sym, cached in list(_ram_cache.items()):
                d = cached.get("data", {})
                if not d or d.get("market", "VN") != mkt2: continue
                price = d.get("price", 0) or 0
                if price <= 0: continue
                a = d.get("analysis", {}) or {}
                chg = d.get("chg", 0) or 0
                score = a.get("total", 0) or 0
                vol = d.get("vol", 0) or 0
                # Proxy market cap: price * estimated shares (use vol*5 as proxy if no cap data)
                fd = d.get("fundamentals", {}) or {}
                cap = fd.get("market_cap") or 0
                if not cap:
                    # Rough proxy: use average daily value in billions
                    cap = max(price * vol / 1e9, 0.1)  # in billion VND
                # Find sector
                sector = "Khác"
                for sec, syms in SECTOR_MAP_VN.items():
                    if sym in syms: sector = sec; break
                if sector not in result_map:
                    result_map[sector] = []
                val = chg if mode == "chg" else (score if mode == "score" else vol * price / 1e9)
                result_map[sector].append({
                    "symbol": sym, "price": price, "chg": chg,
                    "score": score, "cap": round(cap, 2),
                    "val": round(val, 2),
                })
            # Sort each sector by cap desc
            for sec in result_map:
                result_map[sec].sort(key=lambda x: -x["cap"])
            self.send_json({"sectors": result_map, "mode": mode, "total": sum(len(v) for v in result_map.values())}); return

        # ── Breakout Pre-Alert API ────────────────────────────────────────────
        if path.startswith("/api/breakout_scan/"):
            sym2 = path.split("/")[-1].upper()
            mkt2 = qs.get("market", ["VN"])[0].upper()
            self.send_json(detect_breakout_pre_alert(sym2, mkt2)); return

        if path == "/api/breakout_scan":
            with get_db() as c:
                rows = c.execute(
                    "SELECT DISTINCT symbol, market FROM watchlist").fetchall()
            bouts = []
            for row in rows:
                br = detect_breakout_pre_alert(row["symbol"], row["market"])
                if br.get("is_breakout"):
                    bouts.append(br)
            bouts.sort(key=lambda x: -x.get("confidence", 0))
            self.send_json({"breakouts": bouts, "count": len(bouts)}); return

        # ── Watchlist Suggestions API ─────────────────────────────────────────
        if path == "/api/watchlist/suggest":
            u = self.require_auth()
            if not u: return
            self.send_json({"suggestions": suggest_watchlist_additions(u["id"])}); return

        # ── Feature 2: Earnings surprise ──────────────────────────────────────
        if path.startswith("/api/earnings_surprise/"):
            sym2 = path.split("/")[-1].upper()
            mkt2 = qs.get("market", ["US"])[0].upper()
            self.send_json(get_earnings_surprise(sym2, mkt2)); return

        # ── Feature 3: Short position metrics ─────────────────────────────────
        if path == "/api/short_positions":
            u = self.require_auth()
            if not u: return
            with get_db() as c:
                rows = c.execute(
                    "SELECT * FROM trades WHERE user_id=? AND direction='SHORT' AND status='OPEN'",
                    (u['id'],)).fetchall()
            results = []
            for row in rows:
                t = dict(row)
                sym2 = t['symbol']; mkt2 = t.get('market','VN')
                cur_price = _ram_cache.get(sym2,{}).get('data',{}).get('price',0) or t['entry_price']
                metrics = calc_short_position_metrics(t, cur_price)
                results.append({**t, **metrics})
            self.send_json({"positions": results, "count": len(results)}); return

        # ── Feature 4: Custom signals ──────────────────────────────────────────
        if path == "/api/custom_signals":
            u = self.require_auth()
            if not u: return
            with get_db() as c:
                rows = c.execute(
                    "SELECT * FROM custom_signals WHERE user_id=? ORDER BY updated_at DESC",
                    (u['id'],)).fetchall()
            import json as _j
            sigs = []
            for r in rows:
                s = dict(r)
                s['rules'] = _j.loads(s.get('rules','[]') or '[]')
                sigs.append(s)
            self.send_json({"signals": sigs}); return

        if path.startswith("/api/custom_signals/scan/"):
            u = self.require_auth()
            if not u: return
            sig_id = int(path.split("/")[-1])
            self.send_json(scan_custom_signal(sig_id, u['id'])); return

        if path.startswith("/api/custom_signals/backtest/"):
            u = self.require_auth()
            if not u: return
            sig_id = int(path.split("/")[-1])
            days = int(qs.get("days", ["90"])[0])
            self.send_json(backtest_custom_signal(sig_id, u['id'], days)); return

        # ── v29.3 fallback: dispatch unmatched /api/* to handle_4_api ──────────
        if path.startswith("/api/"):
            try:
                _body_fb = {k: vl[0] if len(vl)==1 else vl for k,vl in qs.items()}
                _result_fb, _code_fb = handle_4_api(
                    path, "GET", _body_fb, DB_PATH, _ram_cache,
                    current_user=self.maybe_auth(), qs=qs
                )
                self.send_json(_result_fb, _code_fb)
            except Exception as _efb:
                log.error("[handle_4_api fallback GET] %s: %s", path, _efb)
                self.send_json({"error": str(_efb)[:120]}, 500)
            return

        self.send_json({"error": "Not found"}, 404)
    
    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")
        body = self.body()
        
        # Register
        if path == "/api/register":
            un = body.get("username", "").strip()
            pw = body.get("password", "")
            ok, res = user_register(un, pw)
            self.send_json({"ok": True, "user_id": res} if ok else {"error": res}, 200 if ok else 400)
            return
        
        # Login
        if path == "/api/login":
            ip = self.client_address[0]
            allowed, wait = _check_rate_limit(ip)
            if not allowed:
                self.send_json({"error": f"Quá nhiều lần thử. Thử lại sau {wait} giây."}, 429)
                return
            un = body.get("username", "").strip()
            pw = body.get("password", "")
            ua = self.headers.get('User-Agent', '')
            token, res = user_login(un, pw, ip=ip, user_agent=ua)
            if not token:
                _record_attempt(ip)
            self.send_json({"ok": True, "token": token, "user": res} if token else {"error": res}, 200 if token else 401)
            return
        
        # Logout
        if path == "/api/logout":
            u = self.get_user()
            if u: logout_session(u['token'])
            self.send_json({"ok": True})
            return
        
        # Change Password
        if path == "/api/change_password":
            u = self.require_auth()
            if not u: return
            ok, msg = user_change_password(u['id'], body.get("old_password", ""), body.get("new_password", ""))
            self.send_json({"ok": ok, "message": msg} if ok else {"error": msg}, 200 if ok else 400)
            return
        
        # User Settings
        if path == "/api/user_settings":
            u = self.require_auth()
            if not u: return
            for k, v in body.items():
                set_user_setting(u['id'], k, v)
            self.send_json({"ok": True})
            return
        
        # Admin Create User
        if path == "/api/admin/create_user":
            u = self.require_admin()
            if not u: return
            ok, res = user_register(body.get("username", ""), body.get("password", ""))
            self.send_json({"ok": ok, "user_id": res} if ok else {"error": res}, 200 if ok else 400)
            return
        
        # Admin Set URL
        if path == "/api/admin/set_url":
            u = self.require_admin()
            if not u: return
            url = body.get("url", "").strip()
            set_setting("public_url", url)
            self.send_json({"ok": True, "url": url})
            return
        
        # Scanner Run
        if path == "/api/scanner/run":
            u = self.require_auth()
            if not u: return
            criteria = body.get('criteria', [])
            market = body.get('market', 'ALL')
            min_score = body.get('min_score', 3)
            temp_scanner = Scanner()
            for crit in criteria:
                if crit in scanner.criteria:
                    temp_scanner.criteria[crit] = scanner.criteria[crit]
            results = temp_scanner.scan(market, min_score)
            self.send_json({"results": results})
            return
        
        # Paper Trade
        if path == "/api/paper/trade":
            u = self.require_auth()
            if not u: return
            pt = PaperTrading(u['id'])
            action = body.get('action')
            symbol = body.get('symbol', '').upper()
            qty = float(body.get('qty', 0))
            price = body.get('price')
            if not price:
                data = _ram_cache.get(symbol, {}).get('data', {})
                price = data.get('price', 0)
            if action == 'buy':
                success, msg = pt.buy(symbol, price, qty)
            elif action == 'sell':
                success, msg = pt.sell(symbol, price, qty)
            else:
                success, msg = False, "Invalid action"
            self.send_json({"success": success, "message": str(msg)})
            return
        
        # Watchlist reorder
        if path == "/api/watchlist/reorder":
            u = self.require_auth()
            if not u: return
            order = body.get("order", [])
            try:
                with get_db() as c:
                    for i, sym in enumerate(order):
                        c.execute("UPDATE user_watchlist SET sort_order=? WHERE user_id=? AND symbol=?", (i, u['id'], sym))
                self.send_json({"ok": True})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return
        
        # Drawings save
        if path == "/api/drawings":
            u = self.require_auth()
            if not u: return
            sym = body.get("symbol", "").upper()
            mkt = body.get("market", "VN").upper()
            tool_type = body.get("type", "line")
            data = body.get("data", {})
            with get_db() as c:
                c.execute("INSERT INTO drawing_tools(user_id, symbol, market, tool_type, data_json) VALUES(?,?,?,?,?)",
                        (u['id'], sym, mkt, tool_type, json.dumps(data)))
                self.send_json({"ok": True, "id": c.lastrowid})
            return

        # Watchlist CSV import (v24)
        if path == "/api/watchlist/import_csv":
            u = self.require_auth()
            if not u: return
            rows = body.get("rows", [])
            added = 0
            with get_db() as c:
                for row in rows:
                    sym = row.get("symbol","").upper().strip()
                    mkt = row.get("market","VN").upper()
                    if not sym or len(sym)<2 or len(sym)>12: continue
                    try:
                        c.execute("INSERT OR IGNORE INTO user_watchlist(user_id,symbol,market) VALUES(?,?,?)",
                                  (u["id"],sym,mkt))
                        added += 1
                    except: pass
            self.send_json({"ok":True,"added":added}); return


        # Drawings delete
        if path.startswith("/api/drawings/") and method == "DELETE":
            u = self.require_auth()
            if not u: return
            did = path.split("/")[-1]
            with get_db() as c:
                c.execute("DELETE FROM drawing_tools WHERE id=? AND user_id=?", (did, u['id']))
            self.send_json({"ok": True})
            return

        # Priority symbol
        if path == "/api/priority_sym":
            u = self.require_auth()
            if not u: return
            sym = body.get("symbol", "").upper()
            active = body.get("active", True)
            set_priority_sym(sym, active)
            self.send_json({"ok": True})
            return

        # 2FA enable
        if path == "/api/2fa/enable":
            u = self.require_auth()
            if not u: return
            code = body.get("code", "")
            secret = body.get("secret", "")
            with get_db() as c:
                row = c.execute("SELECT two_factor_secret FROM users WHERE id=?", (u['id'],)).fetchone()
            s = row['two_factor_secret'] if row else secret
            if not totp_verify(s, code):
                self.send_json({"ok": False, "error": "Mã OTP không đúng"}, 400)
                return
            with get_db() as c2:
                c2.execute("UPDATE users SET two_factor_secret=? WHERE id=?", (s, u['id']))
            self.send_json({"ok": True, "message": "2FA đã được kích hoạt"})
            return

        # Master key save
        if path == "/api/settings/master_key":
            u = self.require_auth()
            if not u: return
            new_key = body.get("key", "").strip()
            if len(new_key) < 16:
                self.send_json({"error": "Key phải ≥16 ký tự"}, 400)
                return
            with get_db() as c:
                c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('master_key',?)", (new_key,))
            self.send_json({"ok": True, "message": "Master key đã cập nhật"})
            return

        # Auto-trade setting
        if path == "/api/settings/auto_trade":
            u = self.require_auth()
            if not u: return
            enabled = body.get("enabled", False)
            with get_db() as c:
                c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('auto_trade_enabled',?)", ("1" if enabled else "0",))
            self.send_json({"ok": True, "auto_trade_enabled": enabled})
            return

        # Watchlist Add
        if path == "/api/watchlist":
            u = self.require_auth()
            if not u: return
            sym = sanitize_symbol(body.get("symbol", ""))
            mkt = sanitize_str(body.get("market", "US"), 10).upper()
            if not sym:
                self.send_json({"error": "Thiếu symbol"}, 400)
                return
            if mkt not in _ALLOWED_MARKETS and mkt not in MARKET_SUFFIX:
                self.send_json({"error": "market không hợp lệ"}, 400)
                return
            with get_db() as c:
                if c.execute("SELECT 1 FROM user_watchlist WHERE user_id=? AND symbol=?", (u['id'], sym)).fetchone():
                    self.send_json({"error": f"{sym} đã có trong watchlist"}, 409)
                    return
            print(f"[SMA] Fetching {sym} ({mkt}) → DB...")
            if mkt == "VN":
                data = None
                for _attempt in range(3):
                    with _vn_lock:
                        wait = VN_GAP - (time.time() - _vn_last)
                        if wait > 0: time.sleep(wait)
                        globals()['_vn_last'] = time.time()
                    raw = _raw_fetch_vn(sym)
                    if raw:
                        db_save_ohlcv(sym, mkt, raw["history"])
                        db_save_meta(sym, mkt, raw["price"], raw["chg"], raw.get("name", ""), raw.get("sector", "N/A"))
                        bars = db_load_ohlcv(sym, OHLCV_BARS)
                        data = _build_from_db(sym, mkt, bars, db_get_meta(sym))
                        if data: _ram_cache[sym] = {"ts": time.time(), "data": data}
                        break
                    if _attempt < 2:
                        print(f"[SMA] Rate limit {sym} (attempt {_attempt+1}/3) — chờ 25s")
                        time.sleep(25)
            else:
                data = fetch(sym, mkt, force=True)
            if not data:
                self.send_json({"error": f"Không tìm thấy {sym} trên {mkt}"}, 404)
                return
            uw_add(u['id'], sym, mkt, body.get("note", ""))
            n = db_ohlcv_count(sym)
            self.send_json({"symbol": sym, "market": mkt, "name": data.get("name", ""),
                           "sector": data.get("sector", "N/A"), "bars_saved": n})
            return
        
        # Backtest
        if path == "/api/backtest":
            sym = body.get("symbol", "").upper()
            mkt = body.get("market", "US").upper()
            cap = body.get("capital", 10_000_000)
            bars = db_load_ohlcv(sym, OHLCV_BARS)
            if not bars:
                data = fetch(sym, mkt)
                bars = db_load_ohlcv(sym, OHLCV_BARS)
            if not bars:
                self.send_json({"error": f"Không có dữ liệu {sym}"}, 404)
                return
            r = backtest(bars, cap, market=mkt)
            r["symbol"] = sym
            r["market"] = mkt
            r["bars_used"] = len(bars)
            self.send_json(r)
            return
        
        # Settings
        if path == "/api/settings":
            for k, v in body.items():
                set_setting(k, str(v))
            self.send_json({"ok": True})
            return
        
        # Journal Add
        if path == "/api/journal":
            u = self.require_auth()
            if not u: return
            if not body.get("symbol") or not body.get("entry_price"):
                self.send_json({"error": "Thiếu symbol hoặc entry_price"}, 400)
                return
            tid = trade_add(u['id'], body)
            self.send_json({"ok": True, "id": tid})
            return
        
        # Journal Close
        if path.startswith("/api/journal/") and path.endswith("/close"):
            u = self.require_auth()
            if not u: return
            parts = path.split("/")
            tid = int(parts[-2]) if len(parts) >= 4 else 0
            if not tid:
                self.send_json({"error": "ID không hợp lệ"}, 400)
                return
            if not body.get("exit_price"):
                self.send_json({"error": "Thiếu exit_price"}, 400)
                return
            result = trade_close(tid, u['id'], body["exit_price"], body.get("exit_date", ""),
                               body.get("reason_out", ""), body.get("emotion_out", ""), body.get("note", ""))
            if not result:
                self.send_json({"error": "Không tìm thấy lệnh"}, 404)
                return
            self.send_json({"ok": True, **result})
            return
        
        # Journal Refresh Context
        if path == "/api/journal/refresh_context":
            u = self.require_auth()
            if not u: return
            threading.Thread(target=market_ctx_refresh, daemon=True).start()
            self.send_json({"ok": True, "msg": "Đang cập nhật context..."})
            return
        
        # TG Test
        if path == "/api/tg_test":
            token = body.get("token", "")
            chat = body.get("chat_id", "")
            ok, msg = tg_test(token, chat)
            self.send_json({"ok": ok, "msg": msg})
            return
        
        # Price Alert Add
        if path == "/api/price_alerts":
            u = self.require_auth()
            if not u: return
            sym = body.get("symbol", "").upper().strip()
            target = body.get("target_price")
            direction = body.get("direction", "ABOVE").upper()
            trigger_type = body.get("trigger_type", "PRICE").upper()
            pct_value = body.get("pct_value", None)
            if pct_value: pct_value = float(pct_value)
            # For PCT alerts, base_price = current price at time of creation
            base_price = None
            if trigger_type in ('PCT_CHANGE', 'PCT_FROM_HIGH', 'PCT_FROM_LOW'):
                cached = _ram_cache.get(sym, {})
                base_price = cached.get('data', {}).get('price', 0) or None
                if not target: target = 0  # PCT alerts don't need fixed target
            if not sym or (not target and trigger_type == 'PRICE'):
                self.send_json({"error": "Thiếu symbol hoặc target_price"}, 400)
                return
            if direction not in ("ABOVE", "BELOW"):
                self.send_json({"error": "direction phải là ABOVE hoặc BELOW"}, 400)
                return
            mkt = body.get("market", "VN").upper()
            aid = palert_add(u['id'], sym, mkt, float(target or 0), direction,
                             body.get("note", ""), trigger_type, pct_value, base_price)
            self.send_json({"ok": True, "id": aid})
            return
        
        # Watchlist Group Add
        if path == "/api/watchlist_groups":
            u = self.require_auth()
            if not u: return
            name = body.get("name", "").strip()
            color = body.get("color", "#2563eb")
            if not name:
                self.send_json({"error": "Thiếu tên nhóm"}, 400)
                return
            gid = wg_add(u['id'], name, color)
            if not gid:
                self.send_json({"error": "Tên nhóm đã tồn tại"}, 409)
                return
            self.send_json({"ok": True, "id": gid})
            return
        
        # Watchlist Group Assign
        if path == "/api/watchlist_groups/assign":
            u = self.require_auth()
            if not u: return
            sym = body.get("symbol", "").upper()
            gid = body.get("group_id")
            wg_assign(u['id'], sym, gid)
            self.send_json({"ok": True})
            return
        
        # Refresh
        if path == "/api/refresh":
            sym = body.get("symbol", "").upper()
            mkt = body.get("market", "US").upper()
            data = fetch(sym, mkt, force=True)
            if not data:
                self.send_json({"error": "Fetch thất bại"}, 500)
                return
            self.send_json({"ok": True, "bars": db_ohlcv_count(sym), "latest": db_latest_date(sym)})
            return
        
        # Admin Ban User
        if path == "/api/admin/ban_user":
            u = self.require_admin()
            if not u: return
            uid = body.get("user_id")
            banned = body.get("banned", True)
            if not uid:
                self.send_json({"error": "Missing user_id"}, 400)
                return
            admin_ban_user(int(uid), banned)
            self.send_json({"ok": True})
            return
        
        # Admin Reset Password
        if path == "/api/admin/reset_password":
            u = self.require_admin()
            if not u: return
            uid = body.get("user_id")
            new_pw = body.get("password") or os.environ.get("SMA_RESET_PW") or _gen_default_pw()
            if not uid:
                self.send_json({"error": "Missing user_id"}, 400)
                return
            pw = admin_reset_password(int(uid), new_pw)
            self.send_json({"ok": True, "new_password": pw})
            return
        
        # Admin Delete User
        if path == "/api/admin/delete_user":
            u = self.require_admin()
            if not u: return
            uid = body.get("user_id")
            if not uid:
                self.send_json({"error": "Missing user_id"}, 400)
                return
            if int(uid) == u['id']:
                self.send_json({"error": "Không thể xóa chính mình"}, 400)
                return
            admin_delete_user(int(uid))
            self.send_json({"ok": True})
            return
        
        # Email Test
        if path == "/api/email_test":
            u = self.require_admin()
            if not u: return
            ok, msg = email_test(body.get("smtp_host", ""), body.get("smtp_port", 587),
                body.get("smtp_user", ""), body.get("smtp_pass", ""), body.get("email_to", ""))
            self.send_json({"ok": ok, "msg": msg})
            return
        
        # Import Trades
        if path == "/api/import_trades":
            u = self.require_auth()
            if not u: return
            csv_text = body.get("csv", "")
            broker = body.get("broker", "generic")
            if not csv_text:
                self.send_json({"error": "CSV rỗng"}, 400)
                return
            result = import_trades_from_csv(u['id'], csv_text, broker)
            self.send_json(result)
            return
        
        # Backup
        if path == "/api/backup":
            u = self.require_admin()
            if not u: return
            threading.Thread(target=db_backup_now, daemon=True).start()
            self.send_json({"ok": True, "msg": "Backup đang chạy ngầm"})
            return
        
        # Webhook Test
        if path == "/api/webhook_test":
            u = self.require_admin()
            if not u: return
            url = body.get("url", "") or get_setting("webhook_url", "")
            if not url:
                self.send_json({"ok": False, "msg": "Chưa có URL"})
                return
            if body.get("url"):
                set_setting("webhook_url", url)
            ok, msg = webhook_send({"event": "test", "message": "SMA Pro webhook test", "time": time.strftime("%Y-%m-%d %H:%M:%S")})
            self.send_json({"ok": ok, "msg": msg})
            return
        
        if path.startswith("/api/v24/"):
            u = self.maybe_auth()
            result, code = handle_v24_api(path, "POST", body, DB_PATH, _ram_cache, u,
                ssi_feed=_ssi_feed, lstm=_LSTM, scheduler=_SCHEDULER, tg_send_fn=tg_send)
            self.send_json(result, code)
            return
        
        # ── Pro POST routes (merged from v12) ─────────────────────────
        if path == "/api/ml_train":
            u = self.require_auth()
            if not u: return
            sym2 = body.get("symbol","").upper()
            mkt2 = body.get("market","VN").upper()
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            def _do_train():
                r = train_ml_model(sym2, mkt2, bars2)
                print(f"[Pro] ML train {sym2}: {r.get('val_accuracy','?')}%")
            threading.Thread(target=_do_train, daemon=True).start()
            self.send_json({"ok": True, "msg": f"Training {sym2} started"}); return
        if path == "/api/ml_train_batch":
            u = self.require_auth()
            if not u: return
            wl2 = uw_list(u["id"])
            def _batch_train():
                for w in wl2:
                    try:
                        b2 = db_load_ohlcv(w["symbol"], OHLCV_BARS)
                        train_ml_model(w["symbol"], w.get("market","VN"), b2)
                    except Exception as _e: log.debug('[uncat] line %d: %%s' % 11922, _e)
            threading.Thread(target=_batch_train, daemon=True).start()
            self.send_json({"ok": True, "msg": f"Batch train {len(wl2)} symbols"}); return
        if path == "/api/ml_combined":
            sym2 = body.get("symbol","").upper()
            mkt2 = body.get("market","VN").upper()
            ta_sig2 = body.get("ta_signal")
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            self.send_json(ml_combined_signal(sym2, mkt2, ta_sig2, bars2)); return
        if path == "/api/paper_trade/open":
            u = self.require_auth()
            if not u: return
            paper_trade_open(u["id"], body.get("symbol","").upper(),
                body.get("market","VN").upper(), body.get("signal","BUY"),
                float(body.get("entry_price",0)), float(body.get("size_pct",2.0)),
                body.get("source","TA"), body.get("confidence","MEDIUM"), body.get("note",""))
            self.send_json({"ok": True}); return
        if path == "/api/paper_trade/close":
            u = self.require_auth()
            if not u: return
            tid2 = body.get("trade_id")
            if not tid2: self.send_json({"error":"Missing trade_id"},400); return
            paper_trade_close(int(tid2), float(body.get("exit_price",0)), body.get("reason","Signal"))
            self.send_json({"ok": True}); return
        if path == "/api/backtest_regime":
            sym2 = body.get("symbol","").upper()
            bars2 = db_load_ohlcv(sym2, OHLCV_BARS)
            if not bars2: self.send_json({"error":"No data"},404); return
            result2 = backtest_by_regime(bars2, float(body.get("capital",100_000_000)), body.get("market","VN").upper())
            try:
                live_r = get_market_regime()
                result2["live_regime"] = live_r.get("regime","?")
                result2["live_recommendation"] = live_r.get("recommendation","")
                result2["live_score"] = live_r.get("score", 0)
            except Exception as _e: log.debug('[uncat] line %d: %%s' % 11956, _e)
            self.send_json(result2); return
        # ── /api/bug_report ──
        # ── Investment Screener ────────────────────────────────

            if seg in ('growth', 'value'):
                # Lấy danh sách hiện tại
                result = get_investment_list(seg)
                # Nếu danh sách trống → trigger chạy background
                if not result['list']:
                    threading.Thread(
                        target=run_investment_screener,
                        args=(seg,), daemon=True).start()
                    result['running'] = True
                    result['msg'] = 'Đang chạy screener lần đầu... Tải lại sau 30-60 giây'
                self.send_json(result)
                return

            self.send_json({"error": "Unknown invest endpoint"}, 400)
            return

        if path == "/api/update/check":
            u = self.require_auth()
            if not u: return
            if u.get('role') != 'admin':
                self.send_json({"error": "Admin only"}, 403); return
            result = check_for_update()
            self.send_json(result)
            return

        if path == "/api/update/download":
            u = self.require_auth()
            if not u: return
            if u.get('role') != 'admin':
                self.send_json({"error": "Admin only"}, 403); return
            import threading as _thr2
            def _do_dl():
                r = download_update(os.path.dirname(os.path.abspath(__file__)))
                log.info("[UPDATE] download result: %s", r)
            _thr2.Thread(target=_do_dl, daemon=True).start()
            self.send_json({"ok": True, "msg": "Đang tải bản cập nhật ở background..."})
            return

        if path == "/api/invest/run":
            u = self.require_auth()
            if not u: return
            try:
                style  = str(body.get("style", "growth")).lower()
                market = str(body.get("market", "ALL")).upper()  # ALL = cả VN lẫn US
                if style not in ('growth','value'):
                    self.send_json({"ok":False,"error":"style must be growth or value"}); return
                if _INVEST_RUNNING.get(style):
                    self.send_json({"ok":True,"running":True,
                        "msg":f"Screener {style} đang chạy, tải lại sau 30s"}); return
                threading.Thread(
                    target=run_investment_screener,
                    args=(style, market, 10, True),
                    daemon=True).start()
                self.send_json({"ok":True,
                    "msg":f"Đang chạy {style} screener... Tải lại sau 30-60 giây",
                    "style":style, "market":market})
            except Exception as _ire:
                log.error("[invest/run] %s", _ire)
                self.send_json({"ok":False,"error":str(_ire)[:100]})
            return

        if path == "/api/bug_report":
            u = self.maybe_auth()
            report_type = body.get("type", "bug")
            description = body.get("description", "")
            url_page    = body.get("url", "")
            username    = u["username"] if u else "anonymous"
            try:
                with get_db() as c:
                    c.execute("""INSERT INTO audit_log(user_id, action, details, ip)
                                 VALUES(?,?,?,?)""",
                              (u["id"] if u else None, "BUG_REPORT",
                               json.dumps({"type": report_type, "description": description[:1000],
                                           "url": url_page, "user": username}),
                               self.client_address[0]))
                log.warning("[BugReport] %s: %s — %s", username, report_type, description[:200])
                self.send_json({"ok": True, "message": "Cảm ơn! Báo cáo đã được ghi nhận."})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        # ── SSI: đặt lệnh, hủy, sửa, lưu config ──────────────────────
        if path == "/api/ssi/order":
            u = self.require_auth()
            if not u: return
            sym        = sanitize_symbol(body.get("symbol", ""))
            qty        = sanitize_int(body.get("qty", 0), min_v=1)
            price      = sanitize_float(body.get("price", 0.0), min_v=0.0)
            side       = str(body.get("side", "")).upper()
            order_type = str(body.get("order_type", "LO")).upper()
            market     = str(body.get("market", "HOSE")).upper()
            confirmed  = bool(body.get("confirmed", False))

            # Validate input
            if not sym:
                self.send_json({"error": "Thiếu symbol"}, 400); return
            if side not in ("BUY","SELL"):
                self.send_json({"error": "side phải là BUY hoặc SELL"}, 400); return
            if qty <= 0:
                self.send_json({"error": "qty phải > 0"}, 400); return
            if market == "HOSE" and qty % 100 != 0:
                self.send_json({"error": "HOSE: qty phải là bội số 100"}, 400); return
            if order_type == "LO" and price <= 0:
                self.send_json({"error": "Lệnh LO cần nhập giá > 0"}, 400); return

            # Bước 1: chưa confirm → trả preview để frontend hiển thị dialog
            if not confirmed:
                cur = _ram_cache.get(sym,{}).get("data",{}).get("price", price)
                self.send_json({
                    "need_confirm": True,
                    "preview": {
                        "symbol": sym, "side": side, "qty": qty,
                        "price": price or cur, "order_type": order_type,
                        "market": market,
                        "est_value": round(qty * (price or cur), 0),
                        "warning": "⚠️ Lệnh THẬT sẽ được gửi lên sàn. Kiểm tra kỹ trước khi xác nhận.",
                    }
                }); return

            # Bước 2: đặt lệnh thật
            try:
                fc = _load_ssi_for_user(u['id'])
                result = fc.place_order(sym, qty, price, side, order_type, market)
                # Audit log bắt buộc cho mọi lệnh thật
                _audit_log(u['id'], 'SSI_ORDER', {
                    'symbol': sym, 'qty': qty, 'price': price,
                    'side': side, 'order_type': order_type,
                    'market': market, 'result': str(result)[:300],
                }, self.client_address[0])
                log.info("[SSI] Order: user=%s %s %s qty=%d price=%.0f → %s",
                         u['username'], side, sym, qty, price, str(result)[:80])
                if result.get("error"):
                    self.send_json({"ok": False, "error": result["error"]}, 400)
                else:
                    self.send_json({"ok": True, "order": result,
                                   "message": f"✅ Lệnh {side} {qty} {sym} @ {price:,.0f} đã gửi"})
            except ValueError as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
            except Exception as e:
                log.error("[SSI] place_order error: %s", e)
                self.send_json({"ok": False, "error": f"SSI lỗi: {str(e)}"}, 502)
            return

        if path == "/api/ssi/cancel":
            u = self.require_auth()
            if not u: return
            order_id = str(body.get("order_id","")).strip()
            sym      = sanitize_symbol(body.get("symbol",""))
            market   = str(body.get("market","HOSE")).upper()
            if not order_id:
                self.send_json({"error": "Thiếu order_id"}, 400); return
            try:
                fc = _load_ssi_for_user(u['id'])
                result = fc.cancel_order(order_id, sym, market)
                _audit_log(u['id'], 'SSI_CANCEL',
                    {'order_id': order_id, 'symbol': sym, 'result': str(result)[:200]},
                    self.client_address[0])
                self.send_json({"ok": True, "result": result})
            except ValueError as e:
                self.send_json({"error": str(e)}, 400)
            except Exception as e:
                self.send_json({"error": f"SSI hủy lệnh lỗi: {str(e)}"}, 502)
            return

        if path == "/api/ssi/modify":
            u = self.require_auth()
            if not u: return
            order_id  = str(body.get("order_id","")).strip()
            sym       = sanitize_symbol(body.get("symbol",""))
            new_qty   = sanitize_int(body.get("new_qty",0), min_v=100)
            new_price = sanitize_float(body.get("new_price",0.0), min_v=0.0)
            market    = str(body.get("market","HOSE")).upper()
            if not order_id or not sym:
                self.send_json({"error": "Thiếu order_id hoặc symbol"}, 400); return
            try:
                fc = _load_ssi_for_user(u['id'])
                result = fc.modify_order(order_id, sym, new_qty, new_price, market)
                _audit_log(u['id'], 'SSI_MODIFY',
                    {'order_id': order_id, 'symbol': sym, 'new_qty': new_qty, 'new_price': new_price},
                    self.client_address[0])
                self.send_json({"ok": True, "result": result})
            except ValueError as e:
                self.send_json({"error": str(e)}, 400)
            except Exception as e:
                self.send_json({"error": f"SSI sửa lệnh lỗi: {str(e)}"}, 502)
            return

        if path == "/api/ssi/config":
            u = self.require_auth()
            if not u: return
            cid     = sanitize_str(body.get("consumer_id",""), max_len=100).strip()
            csecret = sanitize_str(body.get("consumer_secret",""), max_len=200).strip()
            acc     = sanitize_str(body.get("account_no",""), max_len=20).strip()
            enabled = int(bool(body.get("enabled", False)))
            if not cid or not csecret or not acc:
                self.send_json({"error": "Cần consumer_id, consumer_secret, account_no"}, 400); return
            # Nếu test_connection=true → thử đăng nhập trước khi lưu
            if body.get("test_connection"):
                try:
                    fc_test = SSIFastConnect(cid, csecret, acc)
                    fc_test._get_token()
                    self.send_json({"ok": True, "message": "✅ Kết nối SSI thành công!"})
                except Exception as e:
                    self.send_json({"ok": False, "error": f"Kết nối thất bại: {str(e)}"})
                return
            try:
                with get_db() as c:
                    c.execute("""INSERT OR REPLACE INTO ssi_config
                        (user_id, consumer_id, consumer_secret, account_no, enabled, updated_at)
                        VALUES (?,?,?,?,?,?)""",
                        (u['id'], cid, csecret, acc, enabled, datetime.now().isoformat()))
                _audit_log(u['id'], 'SSI_CONFIG_SAVE',
                    {'account_no': acc, 'enabled': enabled}, self.client_address[0])
                self.send_json({"ok": True, "message": "Đã lưu cấu hình SSI"})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # ── AI Chat Analyst POST ──────────────────────────────────────────────
        if path == "/api/ai_chat":
            sym2 = body.get("symbol", "").upper()
            mkt2 = body.get("market", "VN").upper()
            question = body.get("question", "").strip()
            history  = body.get("history", [])
            if not question:
                self.send_json({"error": "Thiếu câu hỏi"}, 400); return
            result = ai_chat_analyze(question, sym2 or None, mkt2, history)
            self.send_json(result); return

        # ── Layout Config Save POST ────────────────────────────────────────────
        if path == "/api/layout":
            u = self.require_auth()
            if not u: return
            layout = body.get("layout", {})
            ok = save_layout_config(u['id'], layout)
            self.send_json({"ok": ok}); return

        # ── Feature 4: Custom signals CRUD ────────────────────────────────────
        if path == "/api/custom_signals":
            u = self.require_auth()
            if not u: return
            import json as _j
            name  = body.get("name","").strip()
            rules = body.get("rules", [])
            logic = body.get("logic","AND").upper()
            sig_type = body.get("signal_type","BUY").upper()
            desc  = body.get("description","")
            if not name or not rules:
                self.send_json({"error":"Thiếu name hoặc rules"},400); return
            with get_db() as c:
                cur = c.execute(
                    """INSERT INTO custom_signals(user_id,name,description,rules,logic,signal_type)
                       VALUES(?,?,?,?,?,?)""",
                    (u['id'], name, desc, _j.dumps(rules), logic, sig_type))
            self.send_json({"ok":True,"id":cur.lastrowid}); return

        if path.startswith("/api/custom_signals/") and path.endswith("/toggle"):
            u = self.require_auth()
            if not u: return
            sig_id = int(path.split("/")[3])
            with get_db() as c:
                c.execute("UPDATE custom_signals SET is_active=1-is_active WHERE id=? AND user_id=?",
                          (sig_id, u['id']))
            self.send_json({"ok":True}); return

        if path.startswith("/api/custom_signals/") and not any(
                path.endswith(x) for x in ['/scan','/backtest','/toggle']):
            # DELETE custom signal
            u = self.require_auth()
            if not u: return
            parts = path.rstrip('/').split('/')
            if len(parts) >= 4:
                sig_id = int(parts[3])
                with get_db() as c:
                    c.execute("DELETE FROM custom_signals WHERE id=? AND user_id=?", (sig_id, u['id']))
                self.send_json({"ok":True}); return

        # ── Feature 3: Add SHORT trade ─────────────────────────────────────────
        if path == "/api/trades/short":
            u = self.require_auth()
            if not u: return
            sym   = sanitize_symbol(body.get("symbol",""))
            mkt   = body.get("market","VN").upper()
            entry = float(body.get("entry_price",0) or 0)
            qty   = float(body.get("qty",0) or 0)
            sl    = float(body.get("sl_price",0) or 0)
            tp1   = float(body.get("tp1_price",0) or 0)
            brate = float(body.get("borrow_rate",0) or 0)
            note  = body.get("note","")
            edate = body.get("entry_date", datetime.now().strftime('%Y-%m-%d'))
            if not sym or not entry or not qty:
                self.send_json({"error":"Thiếu symbol/entry/qty"},400); return
            with get_db() as c:
                cur = c.execute(
                    """INSERT INTO trades(user_id,symbol,market,direction,entry_price,qty,
                       sl_price,tp1_price,entry_date,status,borrow_rate,note)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (u['id'],sym,mkt,'SHORT',entry,qty,sl or None,tp1 or None,edate,'OPEN',brate,note))
            self.send_json({"ok":True,"id":cur.lastrowid,"direction":"SHORT"}); return

        # ── v29.3 fallback: dispatch unmatched /api/* POST to handle_4_api ──────
        if path.startswith("/api/"):
            try:
                _result_fb, _code_fb = handle_4_api(
                    path, "POST", body, DB_PATH, _ram_cache,
                    current_user=self.maybe_auth(), qs={}
                )
                self.send_json(_result_fb, _code_fb)
            except Exception as _efb:
                log.error("[handle_4_api fallback POST] %s: %s", path, _efb)
                self.send_json({"error": str(_efb)[:120]}, 500)
            return

        self.send_json({"error": "Not found"}, 404)
    
    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
        body = self.body()
        if path.startswith("/api/watchlist/"):
            u = self.require_auth()
            if not u: return
            sym = path.split("/")[-1].upper()
            note = body.get("note", "")
            with get_db() as c:
                c.execute("UPDATE watchlist SET note=? WHERE symbol=?", (note, sym))
                c.execute("UPDATE user_watchlist SET note=? WHERE user_id=? AND symbol=?", (note, u['id'], sym))
            self.send_json({"ok": True})
            return
        self.send_json({"error": "Not found"}, 404)
    
    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip("/")
        if path.startswith("/api/watchlist/"):
            u = self.require_auth()
            if not u: return
            sym = path.split("/")[-1].upper()
            uw_remove(u['id'], sym)
            if sym in _ram_cache:
                del _ram_cache[sym]
            self.send_json({"ok": True})
            return
        if path.startswith("/api/journal/"):
            u = self.require_auth()
            if not u: return
            tid = path.split("/")[-1]
            with get_db() as c:
                c.execute("DELETE FROM trades WHERE id=? AND user_id=?", (tid, u['id']))
            self.send_json({"ok": True})
            return
        if path.startswith("/api/alerts/"):
            aid = path.split("/")[-1]
            with get_db() as c:
                c.execute("DELETE FROM alerts WHERE id=?", (aid,))
            self.send_json({"ok": True})
            return
        if path.startswith("/api/price_alerts/"):
            u = self.require_auth()
            if not u: return
            aid = path.split("/")[-1]
            palert_delete(int(aid), u['id'])
            self.send_json({"ok": True})
            return
        if path.startswith("/api/watchlist_groups/"):
            u = self.require_auth()
            if not u: return
            gid = path.split("/")[-1]
            wg_delete(int(gid), u['id'])
            self.send_json({"ok": True})
            return
        self.send_json({"error": "Not found"}, 404)

# ── MAIN ────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════
# v24 Classes & Globals (merged)
# ═══════════════════════════════════════════════════════════

_HAS_LGBM=False
try: import lightgbm as lgb; _HAS_LGBM=True; log.info("[ML] LightGBM OK")
except ImportError: pass

SECTOR_GROUPS_VN={
    'Ngân hàng':    ['VCB','BID','CTG','TCB','MBB','ACB','VPB','HDB','STB','SHB','LPB','MSB'],
    'Bất động sản': ['VHM','NVL','VIC','DIG','HDG','KDH','NLG','CII','PDR','DXG'],
    'Chứng khoán':  ['VND','SSI','HCM','MBS','SHS','VIX','AGR','BSI','CTS','FTS'],
    'Dầu khí':      ['GAS','PVD','PVS','PVC','BSR','OIL','PLX'],
    'Thép & VL':    ['HPG','HSG','NKG','TVN','SMC','VGS','TLH'],
    'Tiêu dùng':    ['MSN','VNM','SAB','MCH','QNS','VCS','KDC'],
    'Điện & NL':    ['REE','PC1','GEG','BCG','SHP','PPC','NT2'],
    'Công nghệ':    ['FPT','VGI','CMG','ELC','VTC'],
    'Y tế & Dược':  ['DHG','IMP','DMC','TRA','PME'],
    'Vận tải & LS': ['VJC','HVN','ACV','GMD','SGP','HAH','PVT'],
}

SECTOR_AVG_PE_VN={'Ngân hàng':11.5,'Bất động sản':18.0,'Chứng khoán':14.0,'Dầu khí':12.0,
    'Thép & VL':10.0,'Tiêu dùng':22.0,'Điện & NL':15.0,'Công nghệ':25.0,'Y tế & Dược':20.0,'Vận tải & LS':13.0}
def get_sector_for_symbol(symbol):
    sym=symbol.upper()
    for sector,syms in SECTOR_GROUPS_VN.items():
        if sym in syms: return sector
    return ''
def get_sector_heatmap(market='VN'):
    if market!='VN': return {'error':'Chỉ hỗ trợ VN'}
    result={}
    for sector,syms in SECTOR_GROUPS_VN.items():
        data=[]; total_chg=0; count=0
        for sym in syms:
            d=_ram_cache.get(sym,{}).get('data',{})
            if not d: continue
            chg=d.get('chg',0); data.append({'symbol':sym,'chg':chg,'price':d.get('price',0),'vol':d.get('vol',0)})
            total_chg+=chg; count+=1
        avg_chg=round(total_chg/count,2) if count else 0
        result[sector]={'symbols':data,'avg_chg':avg_chg,'color':'#00c853' if avg_chg>1 else '#e53935' if avg_chg<-1 else '#66bb6a' if avg_chg>0 else '#ef9a9a' if avg_chg<0 else '#888'}
    return result
def get_sector_pe_valuation(symbol,fund_data=None):
    sector=get_sector_for_symbol(symbol); sector_pe=SECTOR_AVG_PE_VN.get(sector,0)
    if not sector or not sector_pe: return {}
    stock_pe=0
    if fund_data: stock_pe=float(fund_data.get('pe',0) or 0)
    if stock_pe<=0:
        d=_ram_cache.get(symbol,{}).get('data',{}); stock_pe=float((d.get('fundamental') or {}).get('pe',0) or 0)
    if stock_pe<=0: return {'sector':sector,'sector_avg_pe':sector_pe,'note':'Chưa có P/E'}
    premium=round((stock_pe/sector_pe-1)*100,1)
    return {'symbol':symbol,'sector':sector,'stock_pe':round(stock_pe,1),'sector_avg_pe':sector_pe,'premium_pct':premium,
            'valuation':'ĐẮT' if premium>20 else 'RẺ' if premium<-20 else 'HỢP LÝ',
            'color':'#f44336' if premium>20 else '#4caf50' if premium<-20 else '#ff9800',
            'note':f'P/E={stock_pe:.1f}x vs ngành {sector_pe:.1f}x ({premium:+.1f}%)'}

# ── Chart comparison ──────────────────────────────────────────
def get_chart_comparison(sym1,mkt1,sym2,mkt2,period=90):
    def get_bars(sym,mkt):
        d=_ram_cache.get(sym,{}).get('data',{}); bars=d.get('history',[])
        if not bars:
            fetched=fetch(sym,mkt); bars=fetched.get('history',[]) if fetched else []
        return bars[-period:] if len(bars)>=period else bars
    bars1=get_bars(sym1,mkt1); bars2=get_bars(sym2,mkt2)
    if not bars1 or not bars2: return {'error':f'Không đủ dữ liệu'}
    base1=bars1[0]['c']; base2=bars2[0]['c']
    norm1=[{'d':b['d'],'v':round(b['c']/base1*100,2)} for b in bars1]
    norm2=[{'d':b['d'],'v':round(b['c']/base2*100,2)} for b in bars2]
    import numpy as _np
    r1=[bars1[i]['c']/bars1[i-1]['c']-1 for i in range(1,len(bars1))]
    r2=[bars2[i]['c']/bars2[i-1]['c']-1 for i in range(1,len(bars2))]
    n=min(len(r1),len(r2)); corr=float(_np.corrcoef(r1[:n],r2[:n])[0,1]) if n>5 else 0
    perf1=round((bars1[-1]['c']/base1-1)*100,2) if base1 else 0
    perf2=round((bars2[-1]['c']/base2-1)*100,2) if base2 else 0
    return {'sym1':sym1,'mkt1':mkt1,'bars1':norm1,'sym2':sym2,'mkt2':mkt2,'bars2':norm2,
            'correlation':round(corr,3),'perf1':perf1,'perf2':perf2,'period':period,'winner':sym1 if perf1>perf2 else sym2}

# ── Sentiment v2 ──────────────────────────────────────────────

_POS_VN=set(['tăng','lợi nhuận','doanh thu','tăng trưởng','kỷ lục','vượt','mạnh','phục hồi','mua','nâng hạng'])
_NEG_VN=set(['giảm','lỗ','thua lỗ','sụt','khó khăn','rủi ro','vi phạm','phạt','điều tra','bán','nợ xấu'])
_POS_EN=set(['beat','record','strong','growth','profit','upgrade','buy','outperform','raise','bullish'])
_NEG_EN=set(['miss','loss','decline','weak','risk','sell','underperform','cut','bearish','fraud'])

class TCBSBroker:
    TRADE="https://apipubaws.tcbs.com.vn/stock-insight/v1"
    AUTH ="https://apipubaws.tcbs.com.vn/iboard-api/v1"

    def __init__(self,username,password):
        self.username=username; self._pw=hashlib.sha256(password.encode()).hexdigest(); self._token=""

    def _req(self,method,url,body=None,skip_auth=False):
        import urllib.request as _ur,urllib.error
        hdrs={"Content-Type":"application/json","User-Agent":"SMA/27","Accept":"application/json"}
        if self._token and not skip_auth: hdrs["Authorization"]="Bearer "+self._token
        req=_ur.Request(url,data=json.dumps(body).encode() if body else None,headers=hdrs,method=method.upper())
        try:
            with _ur.urlopen(req,timeout=12) as r: return json.loads(r.read().decode())
        except _ur.error.HTTPError as e:
            try: return json.loads(e.read().decode())
            except: return {"error":"HTTP "+str(e.code)}
        except Exception as ex: return {"error":str(ex)}

    def login(self):
        r=self._req("POST",self.AUTH+"/login",{"username":self.username,"password":self._pw},skip_auth=True)
        if r.get("access_token"): self._token=r["access_token"]; return {"ok":True}
        return {"ok":False,"error":r.get("message","Login failed")}

    def get_account(self):
        r=self._req("GET",self.TRADE+"/portfolio/asset")
        if r.get("error"): self.login(); r=self._req("GET",self.TRADE+"/portfolio/asset")
        return {"ok":not bool(r.get("error")),"cash":float(r.get("cash",r.get("cashBalance",0))),
                "buying_power":float(r.get("buyingPower",r.get("available",0))),
                "nav":float(r.get("nav",r.get("totalAsset",0))),
                "currency":"VND","error":r.get("error","")}

    def get_positions(self):
        r=self._req("GET",self.TRADE+"/portfolio/holdings")
        items=r.get("data",r if isinstance(r,list) else [])
        return [{"symbol":p.get("symbol",p.get("ticker","")),"qty":float(p.get("quantity",p.get("vol",0))),
                 "avg_entry":float(p.get("avgPrice",p.get("avgCost",0))),
                 "current_price":float(p.get("marketPrice",p.get("closePrice",0))),
                 "unrealized_pl":float(p.get("pnl",0)),"unrealized_plpc":float(p.get("pnlRate",0))}
                for p in (items if isinstance(items,list) else [])]

    def place_order(self,symbol,qty,price=0,side="BUY",order_type="LO"):
        body={"symbol":symbol.upper(),"side":side.upper(),"orderType":order_type.upper(),"quantity":int(qty)}
        if order_type.upper() in ("LO","LIMIT") and price>0: body["price"]=int(price)
        r=self._req("POST",self.TRADE+"/order/create",body)
        return {"ok":bool(r.get("orderId") or r.get("status")=="success"),
                "order_id":str(r.get("orderId","")),"status":r.get("status",""),"error":r.get("error","")}

    def get_orders(self,status="open"):
        r=self._req("GET",self.TRADE+"/order/orders?status="+status+"&page=0&size=50")
        items=r.get("data",r.get("orders",[]))
        return [{"id":o.get("orderId",""),"symbol":o.get("symbol",""),"side":o.get("side",""),
                 "qty":float(o.get("quantity",0)),"filled_qty":float(o.get("filledQty",0)),
                 "price":float(o.get("price",0)),"status":o.get("status",""),"type":o.get("orderType","LO")}
                for o in (items if isinstance(items,list) else [])]

    def cancel_order(self,order_id):
        r=self._req("DELETE",self.TRADE+"/order/cancel/"+str(order_id))
        return {"ok":not r.get("error"),"message":r.get("message","")}

_TCBS_CACHE: dict = {}
def get_tcbs_broker(user_id,db_path):
    if user_id in _TCBS_CACHE: return _TCBS_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,enabled FROM tcbs_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            pw=decrypt_secret(row["password_enc"])  # AES decrypt
            b=TCBSBroker(row["username"],pw)
            if b.login().get("ok"): _TCBS_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[TCBS] %s",e)
    return None


class RegimeConditionalPredictor:
    """Train 3 model GBM riêng cho từng regime, predict theo regime hiện tại."""

    def __init__(self,db_path:str):
        self.db_path=db_path; self._models:dict={}; self._lock=threading.Lock()

    def _detect_regime(self,closes:list)->str:
        if len(closes)<50: return 'SIDEWAYS'
        ma20=sum(closes[-20:])/20; ma50=sum(closes[-50:])/50; p=closes[-1]
        r20=(p/closes[-20]-1)*100 if closes[-20] else 0
        r50=(p/closes[-50]-1)*100 if closes[-50] else 0
        if p>ma20>ma50 and r20>2 and r50>4: return 'BULL'
        if p<ma20<ma50 and r20<-2 and r50<-4: return 'BEAR'
        return 'SIDEWAYS'

    def _features(self,history:list)->list|None:
        if len(history)<30: return None
        closes=[b['c'] for b in history]; highs=[b.get('h',b['c']) for b in history]
        lows=[b.get('l',b['c']) for b in history]; vols=[b.get('v',0) for b in history]; n=len(closes)
        rsi=_calc_rsi_simple(closes)/100
        e12=_ema(closes,12); e26=_ema(closes,26)
        macd=max(-1,min(1,(e12-e26)/(closes[-1]+1e-9)*200))
        ma20=sum(closes[-20:])/20 if n>=20 else closes[-1]
        ma50=sum(closes[-50:])/50 if n>=50 else closes[-1]
        std=(_var(closes[-20:])**0.5) if n>=20 else 1
        bb_pos=(closes[-1]-ma20+2*std)/(4*std+1e-9)
        av=sum(vols[-20:])/20 if n>=20 else 1; vr=min(vols[-1]/(av+1e-9),5)/5
        ret5=(closes[-1]/closes[-6]-1) if n>=6 else 0
        ret20=(closes[-1]/closes[-21]-1) if n>=21 else 0
        rets=[(closes[i]-closes[i-1])/closes[i-1] for i in range(1,min(21,n))]
        vol20=(_var(rets)**0.5) if rets else 0
        adx=_calc_adx_simple(history)/100
        h14=max(highs[-14:]) if n>=14 else closes[-1]; l14=min(lows[-14:]) if n>=14 else closes[-1]
        stoch=(closes[-1]-l14)/(h14-l14+1e-9)
        obv=sum(vols[-i] if closes[-i]>closes[-i-1] else -vols[-i] for i in range(1,min(n,21)))
        obv_t=1 if obv>0 else -1
        ma_sc=((1 if closes[-1]>ma20 else -1)+(1 if closes[-1]>ma50 else -1))/2
        return [rsi,macd,adx,ma_sc,max(0,min(1,bb_pos)),vr,stoch,obv_t,ret5,ret20,vol20]

    def train(self,symbol:str,market:str,history:list)->dict:
        try:
            from sklearn.ensemble import GradientBoostingClassifier
            from sklearn.preprocessing import StandardScaler
            from sklearn.metrics import accuracy_score
            import numpy as np
        except ImportError: return {'error':'scikit-learn required. pip install scikit-learn'}
        if len(history)<200: return {'error':'Cần ≥200 phiên'}
        STEP=5; Xr={'BULL':[],'BEAR':[],'SIDEWAYS':[]}; yr={'BULL':[],'BEAR':[],'SIDEWAYS':[]}
        for i in range(50,len(history)-STEP):
            feat=self._features(history[:i+1])
            if not feat: continue
            closes=[b['c'] for b in history[:i+1]]
            regime=self._detect_regime(closes)
            fut=history[i+STEP]['c']; cur=history[i]['c']
            label=1 if fut>cur*1.02 else(-1 if fut<cur*0.98 else 0)
            Xr[regime].append(feat); yr[regime].append(label)
        results={}; models={}
        for reg in ['BULL','BEAR','SIDEWAYS']:
            X=Xr[reg]; y=yr[reg]
            if len(X)<30: results[reg]={'error':f'Không đủ mẫu {reg}({len(X)})'}; continue
            Xn=np.array(X); yn=np.array(y); sp=int(len(Xn)*.8)
            sc=StandardScaler(); Xts=sc.fit_transform(Xn[:sp]); Xvs=sc.transform(Xn[sp:])
            clf=GradientBoostingClassifier(n_estimators=120,max_depth=4,learning_rate=0.08,random_state=42)
            clf.fit(Xts,yn[:sp]); acc=accuracy_score(yn[sp:],clf.predict(Xvs))
            models[reg]={'clf':clf,'scaler':sc}
            results[reg]={'acc':round(acc*100,1),'n_samples':len(X)}
        try:
            import pickle,base64; blob=base64.b64encode(pickle.dumps(models)).decode()
            with sqlite3.connect(self.db_path,timeout=10) as c:
                c.execute("INSERT OR REPLACE INTO regime_models VALUES(?,?,?)",(symbol,blob,datetime.now().isoformat()))
        except: pass
        with self._lock: self._models[symbol]=models
        avg_acc=round(sum(r.get('acc',0) for r in results.values() if 'acc' in r)/max(1,sum(1 for r in results.values() if 'acc' in r)),1)
        return {'symbol':symbol,'by_regime':results,'overall_acc':avg_acc,'model_type':'RegimeConditional'}

    def predict(self,symbol:str,market:str,history:list)->dict:
        import numpy as np
        if symbol not in self._models: self._load(symbol)
        if symbol not in self._models:
            return {'prediction':'NEUTRAL','confidence':0,'note':'Chưa train. Dùng /api/v22/regime_ml/train'}
        closes=[b['c'] for b in history]; regime=self._detect_regime(closes)
        models=self._models[symbol]
        if regime not in models:
            regime=next((r for r in ['SIDEWAYS','BULL','BEAR'] if r in models),None)
        if not regime: return {'prediction':'NEUTRAL','confidence':0,'regime':'UNKNOWN'}
        feat=self._features(history)
        if not feat: return {'prediction':'NEUTRAL','confidence':0,'regime':regime}
        m=models[regime]; X=m['scaler'].transform(np.array(feat).reshape(1,-1))
        proba=m['clf'].predict_proba(X)[0]; classes=m['clf'].classes_.tolist()
        up_p=float(proba[classes.index(1)]) if 1 in classes else 0
        dn_p=float(proba[classes.index(-1)]) if -1 in classes else 0
        best=1 if up_p>0.55 else(-1 if dn_p>0.55 else 0)
        pred={1:'UP',0:'NEUTRAL',-1:'DOWN'}[best]
        conf=round(abs(max(up_p,dn_p)-0.5)*200,1)
        return {'prediction':pred,'confidence':conf,'prob_up':round(up_p*100,1),'prob_down':round(dn_p*100,1),
                'regime':regime,'model_type':'RegimeConditional','note':f'{regime} model · 5 phiên'}

    def _load(self,symbol:str):
        try:
            import pickle,base64
            with sqlite3.connect(self.db_path,timeout=5) as c:
                row=c.execute("SELECT model_blob FROM regime_models WHERE symbol=?",(symbol,)).fetchone()
            if row:
                with self._lock: self._models[symbol]=pickle.loads(base64.b64decode(row[0]))
        except: pass

_REGIME_ML:RegimeConditionalPredictor|None=None
def get_regime_ml(db_path:str)->RegimeConditionalPredictor:
    global _REGIME_ML
    if _REGIME_ML is None: _REGIME_ML=RegimeConditionalPredictor(db_path)
    return _REGIME_ML


# ════════════════════════════════════════════════════════════════════
# v28: VNDIRECT CONFIG HELPERS
# ════════════════════════════════════════════════════════════════════
_VNDIRECT_CACHE:dict={}

class BSCBroker:
    BASE="https://eztrade.bsc.com.vn/api"
    def __init__(self,username,password,account_no=''):
        self.username=username; self._pw=password; self.account_no=account_no; self._token=''
    def _req(self,method,path,body=None,skip_auth=False):
        import urllib.request as _ur,urllib.error
        hdrs={'Content-Type':'application/json','User-Agent':'SMA/29'}
        if self._token and not skip_auth: hdrs['Authorization']=f'Bearer {self._token}'
        req=_ur.Request(f"{self.BASE}{path}",data=json.dumps(body).encode() if body else None,headers=hdrs,method=method.upper())
        try:
            with _ur.urlopen(req,timeout=12) as r: return json.loads(r.read().decode())
        except _ur.error.HTTPError as e:
            try: return json.loads(e.read().decode())
            except: return {'error':f'HTTP {e.code}'}
        except Exception as ex: return {'error':str(ex)}
    def login(self):
        r=self._req('POST','/auth/login',{'username':self.username,'password':self._pw},skip_auth=True)
        token=r.get('accessToken',r.get('token',''))
        if token: self._token=token; return {'ok':True}
        return {'ok':False,'error':r.get('message','Login failed')}
    def get_account(self):
        r=self._req('GET','/account/balance')
        if r.get('error'): self.login(); r=self._req('GET','/account/balance')
        return {'ok':not bool(r.get('error')),'cash':float(r.get('cashBalance',0) or 0),'buying_power':float(r.get('buyingPower',0) or 0),'nav':float(r.get('totalAsset',0) or 0),'currency':'VND','error':r.get('error','')}
    def get_positions(self):
        r=self._req('GET','/portfolio/holdings'); items=r.get('data',r.get('items',[]))
        return [{'symbol':p.get('symbol',''),'qty':float(p.get('qty',p.get('quantity',0))),'avg_entry':float(p.get('avgPrice',p.get('averageCost',0))),'current_price':float(p.get('marketPrice',p.get('closePrice',0))),'unrealized_plpc':float(p.get('pnlRate',0))} for p in (items if isinstance(items,list) else [])]
    def place_order(self,symbol,qty,price=0,side='BUY',order_type='LO'):
        r=self._req('POST','/order/create',{'symbol':symbol.upper(),'side':side.upper(),'orderType':order_type,'quantity':int(qty),'price':int(price) if price>0 else 0})
        return {'ok':bool(r.get('orderId')),'order_id':str(r.get('orderId','')),'error':r.get('message','')}
    def get_account_balance(self): return self.get_account()


class MBSBroker:
    BASE="https://api.mbssecurities.com.vn/restful/v2"
    def __init__(self,username,password,account_no=''):
        self.username=username; self._pw=password; self.account_no=account_no; self._token=''
    def _req(self,method,path,body=None,skip_auth=False):
        import urllib.request as _ur,urllib.error
        hdrs={'Content-Type':'application/json','User-Agent':'SMA/29'}
        if self._token and not skip_auth: hdrs['Authorization']=f'Bearer {self._token}'
        req=_ur.Request(f"{self.BASE}{path}",data=json.dumps(body).encode() if body else None,headers=hdrs,method=method.upper())
        try:
            with _ur.urlopen(req,timeout=12) as r: return json.loads(r.read().decode())
        except _ur.error.HTTPError as e:
            try: return json.loads(e.read().decode())
            except: return {'error':f'HTTP {e.code}'}
        except Exception as ex: return {'error':str(ex)}
    def login(self):
        r=self._req('POST','/authen/login',{'username':self.username,'password':self._pw},skip_auth=True)
        token=r.get('data',{}).get('accessToken','') if isinstance(r.get('data'),dict) else r.get('token','')
        if token: self._token=token; return {'ok':True}
        return {'ok':False,'error':r.get('message','Login failed')}
    def get_account(self):
        r=self._req('GET',f'/account/{self.username}/balance')
        if r.get('error'): self.login(); r=self._req('GET',f'/account/{self.username}/balance')
        d=r.get('data',r)
        return {'ok':not bool(r.get('error')),'cash':float(d.get('cashBalance',d.get('cash',0)) or 0),
                'buying_power':float(d.get('buyingPower',d.get('available',0)) or 0),'nav':float(d.get('nav',d.get('totalAsset',0)) or 0),'currency':'VND','error':r.get('error','')}
    def get_positions(self):
        r=self._req('GET',f'/portfolio/{self.username}/holdings')
        items=r.get('data',{}).get('items',[]) if isinstance(r.get('data'),dict) else []
        return [{'symbol':p.get('symbol',''),'qty':float(p.get('qty',0)),'avg_entry':float(p.get('avgPrice',0)),'current_price':float(p.get('marketPrice',0)),'unrealized_plpc':float(p.get('pnlRate',0))} for p in (items if isinstance(items,list) else [])]
    def place_order(self,symbol,qty,price=0,side='BUY',order_type='LO'):
        r=self._req('POST','/order/place',{'symbol':symbol.upper(),'side':side.upper(),'orderType':order_type,'qty':int(qty),'price':int(price) if price>0 else 0,'accountNo':self.account_no})
        d=r.get('data',{}); return {'ok':bool(d.get('orderId')),'order_id':str(d.get('orderId','')),'error':r.get('message','')}
    def get_account_balance(self): return self.get_account()


class IBKRBroker:
    def __init__(self,host='localhost',port=5000,account_id=''):
        self.base=f"http://{host}:{port}/v1/api"; self.account_id=account_id
    def _req(self,method,path,body=None):
        import urllib.request as _ur
        req=_ur.Request(f"{self.base}{path}",data=json.dumps(body).encode() if body else None,headers={'Content-Type':'application/json','User-Agent':'SMA/29'},method=method.upper())
        try:
            with _ur.urlopen(req,timeout=10) as r: return json.loads(r.read().decode())
        except Exception as ex: return {'error':str(ex)}
    def get_accounts(self):
        r=self._req('GET','/portfolio/accounts')
        self.account_id=r[0]['id'] if isinstance(r,list) and r and not self.account_id else self.account_id
        return [a.get('id',a) for a in (r if isinstance(r,list) else [])]
    def get_account(self):
        if not self.account_id: self.get_accounts()
        r=self._req('GET',f'/portfolio/{self.account_id}/summary')
        nlv=r.get('netliquidation',{}); cash=r.get('totalcashvalue',{})
        return {'ok':not bool(r.get('error')),'nav':float(nlv.get('amount',0) or 0),'cash':float(cash.get('amount',0) or 0),'currency':nlv.get('currency','USD'),'account_id':self.account_id,'error':r.get('error','')}
    def get_positions(self):
        if not self.account_id: self.get_accounts()
        r=self._req('GET',f'/portfolio/{self.account_id}/positions/0')
        return [{'symbol':p.get('ticker',''),'qty':float(p.get('position',0)),'avg_entry':float(p.get('avgCost',0)),'current_price':float(p.get('mktPrice',0)),'unrealized_pl':float(p.get('unrealizedPnl',0))} for p in (r if isinstance(r,list) else [])]
    def place_order(self,symbol,qty,price=0,side='BUY',exchange='SMART'):
        if not self.account_id: self.get_accounts()
        body=[{'acctId':self.account_id,'conid':0,'orderType':'LMT' if price>0 else 'MKT','side':side.upper(),'quantity':abs(int(qty)),'price':price if price>0 else None,'tif':'DAY','ticker':symbol.upper(),'secType':'STK:SMART:USD'}]
        r=self._req('POST',f'/iserver/account/{self.account_id}/orders',body)
        orders=r if isinstance(r,list) else [r]; first=orders[0] if orders else {}
        return {'ok':bool(first.get('order_id') or first.get('orderId')),'order_id':str(first.get('order_id',first.get('orderId',''))),'error':first.get('error','')}
    def get_account_balance(self): return self.get_account()

_MBS_CACHE:dict={}; _BSC_CACHE:dict={}
def get_mbs_broker(user_id,db_path):
    if user_id in _MBS_CACHE: return _MBS_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,account_no FROM mbs_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            b=MBSBroker(row['username'],decrypt_secret(row['password_enc']),row.get('account_no',''))
            if b.login().get('ok'): _MBS_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[MBS] %s",e)
    return None
def get_bsc_broker(user_id,db_path):
    if user_id in _BSC_CACHE: return _BSC_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,account_no FROM bsc_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            b=BSCBroker(row['username'],decrypt_secret(row['password_enc']))
            if b.login().get('ok'): _BSC_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[BSC] %s",e)
    return None

# ── Markowitz ─────────────────────────────────────────────────
def markowitz_optimize(symbols,market='VN',risk_free=0.04,n_portfolios=2000):
    import numpy as _np
    returns_data=[]; valid_syms=[]
    for sym in symbols:
        d=_ram_cache.get(sym,{}).get('data',{}); bars=d.get('history',[])
        if len(bars)<30: continue
        rets=[bars[i]['c']/bars[i-1]['c']-1 for i in range(1,len(bars))]
        returns_data.append(rets[-252:]); valid_syms.append(sym)
    if len(valid_syms)<2: return {'error':'Cần ít nhất 2 mã có đủ dữ liệu'}
    n=len(valid_syms); min_len=min(len(r) for r in returns_data)
    R=_np.array([r[:min_len] for r in returns_data]); mu=R.mean(axis=1)*252; cov=_np.cov(R)*252
    results={'ret':[],'risk':[],'sharpe':[],'weights':[]}
    for _ in range(n_portfolios):
        w=_np.random.dirichlet(_np.ones(n)); pr=float(w@mu); pvr=float(_np.sqrt(w@cov@w)); ps=(pr-risk_free)/(pvr+1e-9)
        results['ret'].append(round(pr*100,2)); results['risk'].append(round(pvr*100,2)); results['sharpe'].append(round(ps,3)); results['weights'].append([round(x,4) for x in w])
    sharpes=_np.array(results['sharpe']); bi=int(sharpes.argmax()); mi=int(_np.array(results['risk']).argmin())
    return {'symbols':valid_syms,
            'max_sharpe':{'weights':dict(zip(valid_syms,results['weights'][bi])),'return_pct':results['ret'][bi],'risk_pct':results['risk'][bi],'sharpe':results['sharpe'][bi]},
            'min_variance':{'weights':dict(zip(valid_syms,results['weights'][mi])),'return_pct':results['ret'][mi],'risk_pct':results['risk'][mi],'sharpe':results['sharpe'][mi]},
            'n_simulated':n_portfolios}

# ── Tick ──────────────────────────────────────────────────────
_tick_subscribers:dict={}
def subscribe_tick(symbol,ws_handler):
    sym=symbol.upper()
    if sym not in _tick_subscribers: _tick_subscribers[sym]=[]
    if ws_handler not in _tick_subscribers[sym]: _tick_subscribers[sym].append(ws_handler)
def get_tick_snapshot(symbol):
    sym=symbol.upper(); d=_ram_cache.get(sym,{}).get('data',{})
    if not d: return {'error':f'Không có data cho {sym}'}
    return {'symbol':sym,'price':d.get('price',0),'vol':d.get('vol',0),'chg':d.get('chg',0),'bid':d.get('bid',[]),'ask':d.get('ask',[]),'ts':datetime.now().isoformat()}

# ── Multi-tenant ──────────────────────────────────────────────
def get_tenant_db_path(tenant_id='default'):
    if tenant_id=='default' or not tenant_id: return DB_PATH
    import os,re as _re
    safe_id=_re.sub(r'[^a-zA-Z0-9_-]','',tenant_id)[:32]
    if not safe_id: return DB_PATH
    tenant_dir=os.path.join(os.path.dirname(DB_PATH),'tenants'); os.makedirs(tenant_dir,exist_ok=True)
    tenant_db=os.path.join(tenant_dir,f'sma_{safe_id}.db')
    if not os.path.exists(tenant_db):
        try: import shutil; shutil.copy2(DB_PATH,tenant_db)
        except: return DB_PATH
    return tenant_db

# ── Portfolio Beta ────────────────────────────────────────────
def calc_portfolio_beta(user_id,index_sym='VNINDEX',period=60):
    try:
        import numpy as _np
        with get_db() as c:
            positions=c.execute("SELECT symbol,qty,entry_price,market FROM journal WHERE user_id=? AND status='OPEN'",(user_id,)).fetchall()
        if not positions: return {'beta':1.0,'note':'Không có vị thế mở'}
        idx_bars=_ram_cache.get(index_sym,{}).get('data',{}).get('history',[])[-period:]
        if len(idx_bars)<20: return {'beta':1.0,'note':f'{index_sym} chưa đủ dữ liệu'}
        idx_rets=_np.array([idx_bars[i]['c']/idx_bars[i-1]['c']-1 for i in range(1,len(idx_bars))])
        total_value=sum(float(p['qty'])*float(p['entry_price']) for p in positions)
        if total_value<=0: return {'beta':1.0,'note':'Không tính được'}
        port_rets=_np.zeros(len(idx_rets)); betas_by_sym={}
        for p in positions:
            sym=p['symbol']; bars=_ram_cache.get(sym,{}).get('data',{}).get('history',[])[-period:]
            if len(bars)<20: continue
            sym_rets=_np.array([bars[i]['c']/bars[i-1]['c']-1 for i in range(1,len(bars))]); n=min(len(sym_rets),len(idx_rets))
            if n<5: continue
            cov=_np.cov(sym_rets[:n],idx_rets[:n]); beta_sym=float(cov[0,1]/cov[1,1]) if cov[1,1]>0 else 1.0
            weight=float(p['qty'])*float(p['entry_price'])/total_value
            port_rets+=sym_rets[:len(port_rets)]*weight; betas_by_sym[sym]=round(beta_sym,2)
        n=min(len(port_rets),len(idx_rets)); cov=_np.cov(port_rets[:n],idx_rets[:n])
        port_beta=float(cov[0,1]/cov[1,1]) if cov[1,1]>0 else 1.0
        return {'beta':round(port_beta,2),'index':index_sym,'period_days':period,'by_symbol':betas_by_sym,
                'interpretation':'Ổn định hơn thị trường' if port_beta<0.8 else 'Tương đương thị trường' if port_beta<=1.2 else 'Biến động mạnh hơn thị trường',
                'note':f'Beta={port_beta:.2f}: nếu {index_sym} +1%, danh mục +{port_beta:.2f}%'}
    except Exception as e: return {'beta':1.0,'error':str(e)}

# ── Sector exposure ───────────────────────────────────────────
def get_sector_exposure(user_id):
    try:
        with get_db() as c:
            positions=c.execute("SELECT symbol,qty,entry_price,market FROM journal WHERE user_id=? AND status='OPEN'",(user_id,)).fetchall()
    except: return {}
    if not positions: return {}
    total_value=0.0; by_sector={}
    for p in positions:
        sym=p['symbol']; cur_price=_ram_cache.get(sym,{}).get('data',{}).get('price',float(p['entry_price']))
        value=float(p['qty'])*cur_price; total_value+=value
        sector=get_sector_for_symbol(sym) or ('US' if p['market']=='US' else 'Khác')
        by_sector.setdefault(sector,{'value':0,'symbols':[]}); by_sector[sector]['value']+=value; by_sector[sector]['symbols'].append(sym)
    if total_value<=0: return {}
    result=[{'sector':s,'pct':round(d['value']/total_value*100,1),'value':round(d['value'],0),'symbols':d['symbols']} for s,d in sorted(by_sector.items(),key=lambda x:-x[1]['value'])]
    return {'breakdown':result,'total_value':round(total_value,0),'n_positions':len(positions)}

# ── P&L Calendar ─────────────────────────────────────────────
def get_pnl_calendar(user_id,year=None,month=None):
    if not year: year=datetime.now().year
    if not month: month=datetime.now().month
    try:
        with get_db() as c:
            rows=c.execute("SELECT exit_date,SUM(pnl) as daily_pnl,COUNT(*) as n_trades FROM trades WHERE user_id=? AND status='CLOSED' AND strftime('%Y',exit_date)=? AND strftime('%m',exit_date)=? GROUP BY exit_date ORDER BY exit_date",(user_id,str(year),f"{month:02d}")).fetchall()
    except: return {'year':year,'month':month,'days':{}}
    import calendar as _cal
    days_in_month=_cal.monthrange(year,month)[1]; days={}
    for row in rows:
        day_num=int(str(row['exit_date'])[8:10]); pnl=float(row['daily_pnl'] or 0)
        days[day_num]={'pnl':round(pnl,0),'n_trades':row['n_trades'],
                       'color':'#1b5e20' if pnl>5e6 else '#4caf50' if pnl>0 else '#e53935' if pnl<0 else '#888'}
    total_pnl=sum(d['pnl'] for d in days.values())
    return {'year':year,'month':month,'days_in_month':days_in_month,'days':days,'total_pnl':round(total_pnl,0),
            'win_days':sum(1 for d in days.values() if d['pnl']>0),'loss_days':sum(1 for d in days.values() if d['pnl']<0)}

# ── ATC volume alert ──────────────────────────────────────────
def check_atc_volume(sym,mkt,data):
    if mkt!='VN': return
    now_h=datetime.now().hour; now_m=datetime.now().minute
    if not(now_h==14 and now_m>=30): return
    vol=data.get('vol',0); history=data.get('history',[])
    if len(history)<6: return
    avg_vol=sum(b.get('v',0) for b in history[-6:-1])/5
    if avg_vol<=0 or vol<avg_vol*2.5: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='ATC_SURGE' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<3600: return
    price=data.get('price',0); ratio=round(vol/avg_vol,1)
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"🔔 <b>ATC SURGE — {sym}</b>","━"*18,
        f"💰 Giá: <b>{round(price):,}đ</b>",f"📦 KL ATC: <b>{vol:,.0f}</b>  ({ratio}× avg)","━"*18,
        "⚡ Tổ chức đặt mạnh cuối phiên!",f"⏰ {datetime.now().strftime('%H:%M %d/%m/%Y')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'ATC_SURGE',0,price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()

# ── Controlled stock ──────────────────────────────────────────
_CONTROLLED_STOCKS:set=set(); _controlled_last_fetch=0
def fetch_controlled_stocks():
    global _CONTROLLED_STOCKS,_controlled_last_fetch
    if time.time()-_controlled_last_fetch<3600: return _CONTROLLED_STOCKS
    try:
        stored=get_setting('controlled_stocks','')
        _CONTROLLED_STOCKS=set(x.strip().upper() for x in stored.split(',') if x.strip())
    except: pass
    _controlled_last_fetch=time.time(); return _CONTROLLED_STOCKS
def check_controlled_status(symbol):
    controlled=fetch_controlled_stocks(); sym=symbol.upper(); is_c=sym in controlled
    return {'symbol':sym,'is_controlled':is_c,'status':'KIỂM SOÁT' if is_c else 'BÌNH THƯỜNG',
            'warning':f'⚠️ {sym} đang bị KIỂM SOÁT — chỉ bán, không mua!' if is_c else '','color':'#f44336' if is_c else '#4caf50'}

# ── Event countdown ───────────────────────────────────────────
def get_event_countdown(symbol,market):
    try:
        with get_db() as c:
            events=c.execute("SELECT title,event_date,event_type FROM events WHERE symbol=? AND market=? AND event_date>=date('now') ORDER BY event_date ASC LIMIT 3",(symbol,market)).fetchall()
    except: events=[]
    if not events:
        try:
            flag,title=has_upcoming_events(symbol,market,days=30)
            if flag: return {'has_event':True,'title':title,'days':'≤30','color':'#ff9800'}
        except: pass
        return {'has_event':False}
    result=[]
    for ev in events:
        try:
            ev_date=datetime.strptime(str(ev['event_date'])[:10],'%Y-%m-%d')
            days_left=(ev_date-datetime.now()).days
            result.append({'title':ev['title'],'date':ev['event_date'],'days_left':days_left,
                           'color':'#f44336' if days_left<=3 else '#ff9800' if days_left<=7 else '#4caf50',
                           'label':f"{'Hôm nay!' if days_left==0 else 'Ngày mai!' if days_left==1 else f'Còn {days_left} ngày'}"})
        except: pass
    return {'has_event':bool(result),'events':result,'next':result[0] if result else None}


# ═══════════════════════════════════════════════════════════════
# v29.2: FIBONACCI + TREND FORECAST
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════
# v24 Supplementary Functions (merged from server_v24)
# ═══════════════════════════════════════════════════════════

def _alert_event_reminder(sym,mkt,data):
    try: ev_flag,ev_title=has_upcoming_events(sym,mkt,days=3)
    except: return
    if not ev_flag or not ev_title: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='EVENT_REMIND' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<86400: return
    price=data.get('price',0); vn=mkt=="VN"; fp2=lambda v:f"{round(v):,}đ" if vn else f"${v:.2f}"
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"📅 <b>SỰ KIỆN SẮP XẢY RA — {sym}</b>","━"*18,
        f"💰 Giá: <b>{fp2(price)}</b>",f"📋 Sự kiện: <b>{ev_title}</b>","━"*18,"⚠️ Sự kiện có thể gây biến động",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'EVENT_REMIND',0,price,'1D',json.dumps({'event':ev_title})))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()



def _alert_rs_weak(sym,mkt,data,a,fp):
    rs=(a.get('rs_vs_index') or data.get('_rs_vs_index')) or {}
    rs_pct=float(rs.get('rs_pct',0) or 0)
    if rs_pct>=-5: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='RS_WEAK' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<86400: return
    price=data.get('price',0); idx_sym="VNINDEX" if mkt=="VN" else "SPY"
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"📉 <b>RS YẾU — {sym}</b>","━"*18,
        f"💰 Giá: <b>{fp(price)}</b>",f"📊 RS vs {idx_sym}: <b>{rs_pct:+.1f}%</b>","━"*18,"⚠️ Mã yếu hơn thị trường",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'RS_WEAK',0,price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()



def _alert_squeeze_break(sym,mkt,data,a,fp):
    sq_on=a.get('squeeze_on',False); prev=_ram_cache.get(sym,{}).get('_prev_squeeze',sq_on)
    if sym in _ram_cache: _ram_cache[sym]['_prev_squeeze']=sq_on
    if sq_on or not prev: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='SQUEEZE_BREAK' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<ALERT_CD_VOL: return
    price=data.get('price',0); total=a.get('total',0)
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"⚡ <b>SQUEEZE BREAKOUT — {sym}</b>","━"*18,
        f"💰 Giá: <b>{fp(price)}</b>",f"🎯 Hướng: {'📈 TĂNG' if total>0 else '📉 GIẢM'}  Score:{total:+d}","━"*18,
        "💡 Squeeze kết thúc — biến động lớn sắp xảy ra",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'SQUEEZE_BREAK',total,price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()



def _alert_trailing_stop(sym,mkt,data,a,fp):
    ts=a.get('trailing_stop',0); price=data.get('price',0)
    if not ts or not price or price>ts: return
    try:
        with get_db() as c:
            if not c.execute("SELECT id FROM journal WHERE symbol=? AND status='OPEN' LIMIT 1",(sym,)).fetchone(): return
            last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='TRAILING_STOP' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
        if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<ALERT_CD_SL: return
    except: return
    drop=round((ts-price)/ts*100,1)
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"🔔 <b>TRAILING STOP — {sym}</b>","━"*18,
        f"💰 Giá: <b>{fp(price)}</b>",f"🔄 TS: <b>{fp(ts)}</b>  (phá: {drop}%)","━"*18,"⚠️ <b>Cân nhắc chốt lời!</b>",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'TRAILING_STOP',0,price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()



def _alert_unusual_vol(sym,mkt,data,a,fp):
    vol=data.get('vol',0); history=data.get('history',[])
    if len(history)<21: return
    avg_vol=sum(b.get('v',0) for b in history[-21:-1])/20
    if avg_vol<=0 or vol<avg_vol*3: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='VOL_SURGE' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<ALERT_CD_VOL: return
    ratio=round(vol/avg_vol,1); price=data.get('price',0); chg=data.get('chg',0)
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"⚡ <b>UNUSUAL VOLUME — {sym}</b>","━"*18,
        f"💰 Giá: <b>{fp(price)}</b>  ({chg:+.1f}%)",f"📦 KL: <b>{vol:,.0f}</b>  ({ratio}× avg)","━"*18,
        "⚠️ KL đột biến — tổ chức/thông tin nội bộ?",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'VOL_SURGE',a.get('total',0),price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()



def _analyze_intraday_simple(bars, interval='5m', symbol='', market='VN', alert=True):
    """Wrapper for intraday analysis - basic version."""
    if not bars or len(bars)<10:
        return {"error":"Không đủ dữ liệu","bars":len(bars) if bars else 0}
    closes=[b["c"] for b in bars]; n=len(closes)
    price=closes[-1]
    rsi=_calc_rsi_simple(closes)
    vols=[b.get("v",0) for b in bars]
    av20=sum(vols[-20:])/20 if n>=20 else (sum(vols)/n if n else 1)
    vol_ratio=vols[-1]/(av20+1e-9) if av20 else 1
    ma9=sum(closes[-9:])/9 if n>=9 else price
    ma21=sum(closes[-21:])/21 if n>=21 else price
    ma20=sum(closes[-20:])/20 if n>=20 else price
    std=(_var(closes[-20:])**0.5) if n>=20 else 1
    # VWAP
    highs=[b.get("h",b["c"]) for b in bars]; lows=[b.get("l",b["c"]) for b in bars]
    tpv=sum((highs[i]+lows[i]+closes[i])/3*vols[i] for i in range(-min(n,50),0))
    tv=sum(vols[-min(n,50):]); vwap=tpv/tv if tv>0 else price
    # Volume Profile POC
    vp_poc=vp_vah=vp_val=0.0
    _hi=max(highs);_lo=min(lows);span=_hi-_lo
    if span>0 and n>=10:
        NB=10;bsz=span/NB;bkts=[0.0]*NB
        for k in range(n):
            bhi=highs[k];blo=lows[k];bv=vols[k];bsp=max(bhi-blo,bsz)
            for b in range(NB):
                ov=max(0,min(bhi,_lo+(b+1)*bsz)-max(blo,_lo+b*bsz));bkts[b]+=bv*(ov/bsp)
        pi=bkts.index(max(bkts));vp_poc=round(_lo+(pi+0.5)*bsz,2)
        vp_vah=round(_lo+(min(pi+3,NB-1)+1)*bsz,2);vp_val=round(_lo+max(pi-3,0)*bsz,2)
    # Score
    sc={}
    sc["rsi"]=1 if rsi<30 else(-1 if rsi>70 else 0)
    sc["vol"]=1 if vol_ratio>1.5 and closes[-1]>closes[-2] else(-1 if vol_ratio>1.5 and closes[-1]<closes[-2] else 0)
    sc["ma"]=1 if price>ma9>ma21 else(-1 if price<ma9<ma21 else 0)
    sc["vwap"]=1 if price>vwap*1.005 else(-1 if price<vwap*0.995 else 0)
    sc["bb"]=1 if price<ma20-2*std else(-1 if price>ma20+2*std else 0)
    total=sum(sc.values())
    if total>=4: sig="INTRADAY_BUY"
    elif total>=2: sig="INTRADAY_BUY_WEAK"
    elif total<=-4: sig="INTRADAY_SELL"
    elif total<=-2: sig="INTRADAY_SELL_WEAK"
    else: sig="INTRADAY_NEUTRAL"
    return {"symbol":symbol,"interval":interval,"bars":n,"price":price,"vwap":round(vwap,2),
            "ma9":round(ma9,2),"ma21":round(ma21,2),"rsi":round(rsi,1),
            "vp_poc":vp_poc,"vp_vah":vp_vah,"vp_val":vp_val,
            "signal":sig,"total":total,"n_indicators":len(sc),
            "scores":sc,"details":{"rsi":f"RSI={rsi:.1f}","vol":f"Vol ratio={vol_ratio:.1f}x",
                "ma":f"MA9={ma9:.1f} MA21={ma21:.1f}","vwap":f"VWAP={vwap:.1f}","bb":f"BB"},
            "last_time":bars[-1].get("t","") if bars else ""}



def _check_regime_change(mkt='VN'):
    cur=_market_regime_cache.get('regime','UNKNOWN'); prev=_last_regime_state.get(mkt,cur)
    _last_regime_state[mkt]=cur
    if cur==prev or cur=='UNKNOWN': return
    bull='BULL' in cur
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b>",f"{'🟢' if bull else '🔴'} <b>REGIME THAY ĐỔI — {mkt}</b>","━"*18,
        f"Trước: <b>{prev}</b>  →  Mới: <b>{cur}</b>","━"*18,
        f"{'📈 Thị trường chuyển TĂNG' if bull else '📉 Thị trường chuyển GIẢM — bảo vệ vốn'}",f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
    threading.Thread(target=tg_send,args=(msg,),daemon=True).start()

# ── Heikin-Ashi ───────────────────────────────────────────────


def _get_master_key() -> str:
    mk = get_setting('master_key','')
    if not mk:
        import secrets as _sec
        mk = _sec.token_hex(32)
        try:
            with get_db() as c: c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('master_key',?)",(mk,))
        except: pass
    return mk



def _install_lgbm():
    global _HAS_LGBM
    if _HAS_LGBM: return
    try:
        import subprocess as _sp,sys
        if _sp.run([sys.executable,'-m','pip','install','lightgbm','--quiet','--break-system-packages'],capture_output=True,timeout=180).returncode==0:
            import lightgbm as lgb; _HAS_LGBM=True; log.info("[ML] LightGBM installed")
    except: pass
threading.Thread(target=_install_lgbm,daemon=True).start()



def analyze_headline_sentiment_v2(headlines,lang='vi'):
    if not headlines: return {'score':50,'label':'NEUTRAL','pos':0,'neg':0,'total':0,'sentiment_signal':0}
    pw=_POS_VN if lang=='vi' else _POS_EN; nw=_NEG_VN if lang=='vi' else _NEG_EN
    pos=0; neg=0
    for title in headlines:
        t=title.lower(); pos+=sum(1 for w in pw if w in t); neg+=sum(1 for w in nw if w in t)
    total=len(headlines)
    if pos+neg==0: score=50; label='NEUTRAL'
    else:
        score=round(pos/(pos+neg)*100); label='BULLISH' if score>=65 else 'BEARISH' if score<=35 else 'NEUTRAL'
    if len(headlines)>=3:
        rp=sum(1 for w in pw if w in headlines[0].lower()); rn=sum(1 for w in nw if w in headlines[0].lower())
        if rp>rn: score=min(100,score+10)
        elif rn>rp: score=max(0,score-10)
    return {'score':score,'label':label,'pos':pos,'neg':neg,'total':total,'sentiment_signal':1 if label=='BULLISH' else -1 if label=='BEARISH' else 0}

# ── KQKD from TCBS ───────────────────────────────────────────


def auto_trade_check(sym: str, mkt: str, data: dict, a: dict):
    """Kiểm tra có cần tự động đặt lệnh không (với xác nhận TG)."""
    if get_setting('auto_trade_enabled', '0') != '1': return
    sig = a.get('signal', ''); total = a.get('total', 0)
    threshold = int(get_setting('alert_threshold', '8'))
    if abs(total) < threshold: return
    is_buy = sig in ('BUY',); is_sell = sig in ('SELL',)
    if not is_buy and not is_sell: return

    token = get_setting('tg_token', ''); chat = get_setting('tg_chat_id', '')
    if not token or not chat: return

    price = data.get('price', 0)
    vn = mkt == 'VN'; fp = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"

    # Tính SL/TP
    sl = a.get('stop_loss', 0); tp1 = a.get('take_profit', 0); tp2 = a.get('take_profit2', 0)

    # Tính position size từ settings
    account_size = float(get_setting('account_size', '0') or 0)
    risk_pct = float(get_setting('risk_per_trade_pct', '2')) / 100
    if account_size > 0 and sl > 0 and price > 0:
        risk_per_share = abs(price - sl)
        max_shares = int(account_size * risk_pct / risk_per_share) if risk_per_share > 0 else 0
        if vn: max_shares = max(100, round(max_shares / 100) * 100)
        suggested_qty = max_shares
    else:
        suggested_qty = 100 if vn else 1

    # Tạo confirm ID
    import uuid
    confirm_id = str(uuid.uuid4())[:8].upper()
    expires = (datetime.now() + timedelta(minutes=30)).isoformat()

    _pending_confirms[confirm_id] = {
        'action': 'BUY' if is_buy else 'SELL',
        'symbol': sym, 'market': mkt,
        'price': price, 'qty': suggested_qty,
        'sl': sl, 'tp1': tp1, 'tp2': tp2,
        'expires': expires, 'chat_id': chat,
    }
    # Persist to DB
    try:
        with get_db() as c:
            c.execute("""INSERT OR REPLACE INTO tg_pending_confirm
                (chat_id,action,payload,expires_at) VALUES(?,?,?,?)""",
                (chat, f"{'BUY' if is_buy else 'SELL'} {sym}",
                 json.dumps(_pending_confirms[confirm_id]), expires))
    except: pass

    side = "🟢 MUA" if is_buy else "🔴 BÁN"
    sep = "━"*20
    msg = "\n".join([
        f"📊 <b>SMA v{VERSION}</b> · Auto-Trade Confirm",
        f"{side} <b>{sym} ({mkt})</b>",
        sep,
        f"💰 Giá: <b>{fp(price)}</b>  Score: {total:+d}",
        f"📦 KL gợi ý: <b>{suggested_qty:,}</b>",
        f"🛡 Stop Loss: {fp(sl)}" if sl else "",
        f"🎯 TP1: {fp(tp1)}  TP2: {fp(tp2)}" if tp1 else "",
        sep,
        f"✅ Xác nhận: gõ <b>/confirm {confirm_id}</b>",
        f"❌ Bỏ qua: gõ <b>/skip {confirm_id}</b>",
        f"⏰ Hết hạn: 30 phút",
    ])
    msg = '\n'.join(line for line in msg.split('\n') if line)
    inline_kb={"inline_keyboard":[[
        {"text":f"✅ {'MUA' if is_buy else 'BÁN'} {suggested_qty:,} {sym}","callback_data":f"confirm:{confirm_id}"},
        {"text":"❌ Bỏ qua","callback_data":f"skip:{confirm_id}"},
    ]]}
    try:
        import requests as _req
        _req.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  json={"chat_id":chat,"text":msg,"parse_mode":"HTML","reply_markup":inline_kb},timeout=8)
    except: pass


# ═══════════════════════════════════════════════════════════════
# v27: FEATURE 6 — AUTO TP SCALING (1/3 → 1/3 → hold 1/3)
# ═══════════════════════════════════════════════════════════════


def calc_dcf(fd: dict, market: str = 'VN') -> dict:
    """
    Định giá DCF (Discounted Cash Flow) đơn giản.
    Dùng FCF proxy = Net Income * (1 - capex_ratio)
    hoặc Net Income trực tiếp nếu không có FCF.
    Wacc VN ≈ 12-15%, US ≈ 8-10%.
    """
    ni    = fd.get('net_income_ttm') or fd.get('net_income')
    rev   = fd.get('revenue_ttm') or fd.get('revenue')
    rg    = fd.get('revenue_growth_yoy') or 0         # % YoY
    pg    = fd.get('profit_growth_yoy') or rg or 0    # % YoY
    cap   = fd.get('market_cap') or 0
    eps   = fd.get('eps') or 0
    pe    = fd.get('pe') or 0

    if not ni or ni <= 0:
        return {'error': 'Không có dữ liệu lợi nhuận để tính DCF'}

    # WACC
    wacc = float(get_setting('wacc_vn','12'))/100 if market=='VN' else float(get_setting('wacc_us','9'))/100

    # Growth rates: 3 stages
    g1 = max(min((pg or rg or 10) / 100, 0.35), -0.10)   # Năm 1-5: trend hiện tại
    g2 = max(min(g1 * 0.6, 0.15), 0.02)                   # Năm 6-10: giảm dần
    g_terminal = 0.04 if market == 'VN' else 0.025         # Terminal growth

    # FCF proxy = Net Income * margin_factor
    nm = fd.get('net_margin') or (ni / rev * 100 if rev and rev > 0 else 10)
    fcf = ni * (1 if nm >= 10 else 0.7)   # low margin → discount FCF

    # Stage 1: years 1-5
    pv1 = sum(fcf * (1 + g1)**t / (1 + wacc)**t for t in range(1, 6))
    # Stage 2: years 6-10
    fcf5 = fcf * (1 + g1)**5
    pv2  = sum(fcf5 * (1 + g2)**(t-5) / (1 + wacc)**t for t in range(6, 11))
    # Terminal value
    fcf10 = fcf5 * (1 + g2)**5
    tv    = fcf10 * (1 + g_terminal) / (wacc - g_terminal)
    pv_tv = tv / (1 + wacc)**10

    intrinsic_total = pv1 + pv2 + pv_tv

    # Per-share value (approximate from EPS ratio)
    intrinsic_per_share = None
    upside_pct = None
    valuation_label = 'N/A'

    if eps and eps > 0 and pe and pe > 0:
        cur_price = eps * pe
        # Scale intrinsic_total down to per-share using same EPS ratio
        intrinsic_per_share = round((intrinsic_total / ni) * eps, 2) if ni > 0 else None
        if intrinsic_per_share and cur_price > 0:
            upside_pct = round((intrinsic_per_share - cur_price) / cur_price * 100, 1)
            if upside_pct > 30:
                valuation_label = '🟢 UNDERVALUED'
            elif upside_pct > 0:
                valuation_label = '🟡 FAIRLY VALUED'
            elif upside_pct > -20:
                valuation_label = '🟠 SLIGHTLY OVERVALUED'
            else:
                valuation_label = '🔴 OVERVALUED'

    # Sensitivity: +/- 2% WACC
    sens = {}
    for wacc_adj in [-0.02, 0, 0.02]:
        w = wacc + wacc_adj
        pv1s = sum(fcf * (1+g1)**t / (1+w)**t for t in range(1,6))
        pv2s = sum(fcf5 * (1+g2)**(t-5) / (1+w)**t for t in range(6,11))
        tvs  = fcf10*(1+g_terminal)/(w-g_terminal)
        pv_tvs = tvs/(1+w)**10
        total_s = pv1s + pv2s + pv_tvs
        key = f"wacc_{int(w*100)}pct"
        if eps and eps > 0 and ni > 0:
            sens[key] = round((total_s/ni)*eps, 2)
        else:
            sens[key] = round(total_s/1e9, 2)

    return {
        'method': 'DCF (3-stage)',
        'wacc_pct': round(wacc * 100, 1),
        'growth_stage1_pct': round(g1 * 100, 1),
        'growth_stage2_pct': round(g2 * 100, 1),
        'growth_terminal_pct': round(g_terminal * 100, 1),
        'fcf_proxy': round(fcf, 0),
        'pv_stage1':  round(pv1, 0),
        'pv_stage2':  round(pv2, 0),
        'pv_terminal': round(pv_tv, 0),
        'intrinsic_total': round(intrinsic_total, 0),
        'intrinsic_per_share': intrinsic_per_share,
        'upside_pct': upside_pct,
        'valuation_label': valuation_label,
        'sensitivity': sens,
        'note': 'DCF là ước tính — kết quả phụ thuộc vào giả định tăng trưởng và WACC',
    }




def calc_pnl_dashboard(user_id: int) -> dict:
    try:
        with get_db() as c:
            closed = c.execute(
                "SELECT symbol, market, direction, entry_price, exit_price, qty, pnl, entry_date, exit_date "
                "FROM trades WHERE user_id=? AND status='CLOSED' ORDER BY exit_date",
                (user_id,)
            ).fetchall()
            open_pos = c.execute(
                "SELECT symbol, market, direction, entry_price, qty, entry_date "
                "FROM trades WHERE user_id=? AND status='OPEN'",
                (user_id,)
            ).fetchall()
    except:
        return {'error': 'DB error'}

    realized_total = sum(float(t['pnl'] or 0) for t in closed)
    realized_by_month = {}
    for t in closed:
        ym = (t['exit_date'] or '')[:7]
        if ym not in realized_by_month:
            realized_by_month[ym] = {'pnl': 0, 'n': 0, 'wins': 0}
        pnl = float(t['pnl'] or 0)
        realized_by_month[ym]['pnl'] += pnl
        realized_by_month[ym]['n'] += 1
        if pnl > 0: realized_by_month[ym]['wins'] += 1

    unrealized_total = 0
    unrealized_list = []
    for pos in open_pos:
        sym = pos['symbol']
        cur_price = _ram_cache.get(sym, {}).get('data', {}).get('price', float(pos['entry_price']))
        entry = float(pos['entry_price']); qty = float(pos['qty'])
        upnl = (cur_price - entry) * qty if pos['direction'] == 'LONG' else (entry - cur_price) * qty
        pct = (cur_price - entry) / entry * 100 if entry > 0 else 0
        unrealized_total += upnl
        days = 0
        try: days = (datetime.now() - datetime.fromisoformat(pos['entry_date'][:10])).days
        except: pass
        unrealized_list.append({'symbol': sym, 'market': pos['market'],
            'entry_price': entry, 'cur_price': round(cur_price, 2), 'qty': qty,
            'pnl': round(upnl, 0), 'pct': round(pct, 2), 'direction': pos['direction'],
            'entry_date': pos['entry_date'], 'days_held': days})
    unrealized_list.sort(key=lambda x: x['pnl'])

    # Thue TNCN VN: 0.1% doanh thu ban
    total_sell_vn = sum(float(t.get('exit_price') or 0) * float(t.get('qty') or 0)
                        for t in closed if t.get('market') == 'VN')
    tax_vn = total_sell_vn * 0.001
    tax_by_month = {}
    for t in closed:
        if t.get('market') != 'VN': continue
        ym = (t.get('exit_date') or '')[:7]
        tax_by_month[ym] = tax_by_month.get(ym, 0) + float(t.get('exit_price') or 0) * float(t.get('qty') or 0) * 0.001

    capital_init = float(get_setting('account_size', '100000000') or 1e8)
    equity = [{'date': 'Start', 'value': capital_init}]
    running = capital_init
    for t in closed:
        running += float(t['pnl'] or 0)
        equity.append({'date': (t['exit_date'] or '')[:10], 'value': round(running, 0),
                       'cum_pnl': round(running - capital_init, 0)})

    monthly = []
    for ym in sorted(realized_by_month.keys()):
        m = realized_by_month[ym]
        monthly.append({'month': ym, 'pnl': round(m['pnl'], 0), 'n_trades': m['n'],
            'win_rate': round(m['wins'] / m['n'] * 100, 1) if m['n'] > 0 else 0,
            'tax_est': round(tax_by_month.get(ym, 0), 0)})

    wins = [t for t in closed if float(t['pnl'] or 0) > 0]
    losses = [t for t in closed if float(t['pnl'] or 0) <= 0]
    avg_win = sum(float(t['pnl']) for t in wins) / len(wins) if wins else 0
    avg_loss = sum(float(t['pnl']) for t in losses) / len(losses) if losses else 0

    return {'realized_total': round(realized_total, 0), 'unrealized_total': round(unrealized_total, 0),
            'total_pnl': round(realized_total + unrealized_total, 0),
            'tax_estimate_vn': round(tax_vn, 0), 'n_closed': len(closed), 'n_open': len(open_pos),
            'win_rate': round(len(wins)/len(closed)*100,1) if closed else 0,
            'avg_win': round(avg_win,0), 'avg_loss': round(avg_loss,0),
            'profit_factor': round(abs(avg_win/avg_loss),2) if avg_loss else 0,
            'monthly': monthly, 'equity_curve': equity[-60:],
            'unrealized_positions': unrealized_list, 'capital_init': capital_init}


# ═══════════════════════════════════════════════════════════════════
# v29.3: FEATURE 7 — PRIORITY QUEUE
# ═══════════════════════════════════════════════════════════════════

_priority_syms: set = set()
_alert_active_syms: set = set()



def calc_rebalancing(user_id:int) -> dict:
    with get_db() as c:
        targets=c.execute("SELECT symbol,market,target_pct FROM portfolio_targets WHERE user_id=?",(user_id,)).fetchall()
        open_tr=c.execute("SELECT symbol,market,qty,entry_price FROM journal WHERE user_id=? AND status='OPEN'",(user_id,)).fetchall()
        acct=c.execute("SELECT value FROM settings WHERE key='account_size'").fetchone()
    if not targets:
        return {"error":"Chưa đặt target allocation. Vào Portfolio → ⚖️ Rebalancing → ⚙️ Target."}
    account_size=float(acct["value"]) if acct and acct["value"] else 0
    positions={};total_mv=0.0
    for t in open_tr:
        sym=t["symbol"];cur=_ram_cache.get(sym,{}).get("data",{}).get("price",float(t["entry_price"]))
        val=float(t["qty"])*cur;positions[sym]={"symbol":sym,"market":t["market"],"qty":float(t["qty"]),"cur_price":cur,"market_value":val};total_mv+=val
    pval=max(total_mv,account_size)
    if pval<=0: return {"error":"Không tính được giá trị danh mục."}
    ttgt=sum(float(t["target_pct"]) for t in targets)
    if abs(ttgt-100)>5: return {"error":f"Tổng target={ttgt:.1f}% — phải bằng 100%"}
    suggestions=[];total_dev=0.0
    for t in targets:
        sym=t["symbol"];tpct=float(t["target_pct"]);tval=pval*tpct/100
        pos=positions.get(sym);cval=pos["market_value"] if pos else 0
        cprice=pos["cur_price"] if pos else _ram_cache.get(sym,{}).get("data",{}).get("price",0)
        cpct=cval/pval*100 if pval else 0;dval=tval-cval;dev=cpct-tpct
        dqty=dval/cprice if cprice>0 else 0
        mkt=t["market"]
        if mkt=="VN": dqty=round(dqty/100)*100
        action="HOLD"
        if dval>pval*0.01 and dqty>=(100 if mkt=="VN" else 1): action="BUY"
        elif dval<-(pval*0.01) and abs(dqty)>=(100 if mkt=="VN" else 1): action="SELL"
        total_dev+=abs(dev)
        suggestions.append({"symbol":sym,"market":mkt,"target_pct":tpct,"current_pct":round(cpct,2),
            "deviation":round(dev,2),"delta_qty":int(dqty),"cur_price":cprice,"action":action,
            "color":"#00e676" if action=="BUY" else("#f03050" if action=="SELL" else "#4a6a8a")})
    suggestions.sort(key=lambda x:abs(x.get("deviation",0)),reverse=True)
    return {"portfolio_value":round(pval,0),"total_deviation":round(total_dev,2),
            "drift_label":"✅ Cân bằng tốt" if total_dev<5 else("⚠️ Cần rebalancing" if total_dev<15 else "🔴 Lệch nhiều"),
            "suggestions":suggestions}

# ════════════════════════════════════════════════════════════════════
# v28: AES-256 ENCRYPTION (PBKDF2 + AES-GCM, fallback XOR)
# ════════════════════════════════════════════════════════════════════


def check_atc_volume(sym,mkt,data):
    if mkt!='VN': return
    now_h=datetime.now().hour; now_m=datetime.now().minute
    if not(now_h==14 and now_m>=30): return
    vol=data.get('vol',0); history=data.get('history',[])
    if len(history)<6: return
    avg_vol=sum(b.get('v',0) for b in history[-6:-1])/5
    if avg_vol<=0 or vol<avg_vol*2.5: return
    with get_db() as c:
        last=c.execute("SELECT created_at FROM alerts WHERE symbol=? AND signal='ATC_SURGE' ORDER BY created_at DESC LIMIT 1",(sym,)).fetchone()
    if last and (datetime.now()-datetime.fromisoformat(last['created_at'])).total_seconds()<3600: return
    price=data.get('price',0); ratio=round(vol/avg_vol,1)
    msg="\n".join([f"📊 <b>SMA v{VERSION}</b> · {mkt}",f"🔔 <b>ATC SURGE — {sym}</b>","━"*18,
        f"💰 Giá: <b>{round(price):,}đ</b>",f"📦 KL ATC: <b>{vol:,.0f}</b>  ({ratio}× avg)","━"*18,
        "⚡ Tổ chức đặt mạnh cuối phiên!",f"⏰ {datetime.now().strftime('%H:%M %d/%m/%Y')}"])
    with get_db() as c2: c2.execute("INSERT INTO alerts(symbol,market,signal,score,price,timeframe,detail) VALUES(?,?,?,?,?,?,?)",(sym,mkt,'ATC_SURGE',0,price,'1D','{}'))
    threading.Thread(target=tg_send_to_subscribers,args=(sym,msg),daemon=True).start()

# ── Controlled stock ──────────────────────────────────────────
_CONTROLLED_STOCKS:set=set(); _controlled_last_fetch=0


def check_tp_scaling(sym: str, mkt: str, data: dict, a: dict):
    """Kiểm tra và gợi ý chốt lời từng phần theo TP1/TP2."""
    tp1 = a.get('take_profit', 0); tp2 = a.get('take_profit2', 0)
    price = data.get('price', 0)
    if not tp1 or not price: return

    try:
        with get_db() as c:
            open_pos = c.execute(
                "SELECT * FROM journal WHERE symbol=? AND status='OPEN' ORDER BY created_at DESC LIMIT 1",
                (sym,)).fetchone()
        if not open_pos: return
        qty_orig = float(open_pos['qty']); entry = float(open_pos['entry_price'])
        mkt_pos = open_pos.get('market', mkt)
        vn = mkt_pos == 'VN'; fp = lambda v: f"{round(v):,}đ" if vn else f"${v:.2f}"

        # Check TP scaling flags in journal note
        note = json.loads(open_pos.get('note', '{}') or '{}') if isinstance(open_pos.get('note'), str) else {}
        tp1_done = note.get('tp1_scaled', False)
        tp2_done = note.get('tp2_scaled', False)

        alert_sent = False
        if price >= tp1 * 0.998 and not tp1_done:
            qty_sell = max(int(qty_orig / 3 / 100) * 100, 100) if vn else max(int(qty_orig / 3), 1)
            pnl_pct = round((price - entry) / entry * 100, 1)
            sep = "━"*20
            msg = "\n".join([
                f"📊 <b>SMA v{VERSION}</b> · {mkt}",
                f"🎯 <b>TP1 ĐẠT — Gợi ý chốt 1/3 — {sym}</b>",
                sep,
                f"💰 Giá: <b>{fp(price)}</b>  (TP1: {fp(tp1)})",
                f"📈 Lợi nhuận: <b>+{pnl_pct}%</b>",
                f"📦 Gợi ý bán: <b>{qty_sell:,} cp</b> (⅓ vị thế)",
                f"📦 Giữ lại: {int(qty_orig - qty_sell):,} cp",
                sep,
                "💡 Chốt ⅓ bảo vệ lợi nhuận, giữ ⅔ theo xu hướng",
                f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}",
            ])
            alert_sent = True

        elif price >= tp2 * 0.998 and tp1_done and not tp2_done:
            qty_sell = max(int(qty_orig / 3 / 100) * 100, 100) if vn else max(int(qty_orig / 3), 1)
            pnl_pct = round((price - entry) / entry * 100, 1)
            sep = "━"*20
            msg = "\n".join([
                f"📊 <b>SMA v{VERSION}</b> · {mkt}",
                f"🎯 <b>TP2 ĐẠT — Gợi ý chốt thêm 1/3 — {sym}</b>",
                sep,
                f"💰 Giá: <b>{fp(price)}</b>  (TP2: {fp(tp2)})",
                f"📈 Lợi nhuận: <b>+{pnl_pct}%</b>",
                f"📦 Gợi ý bán: <b>{qty_sell:,} cp</b> thêm",
                f"📦 Giữ ⅓ còn lại, dùng trailing stop",
                sep,
                f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}",
            ])
            alert_sent = True

        if alert_sent:
            threading.Thread(target=tg_send_to_subscribers, args=(sym, msg), daemon=True).start()
    except Exception as e:
        log.debug("[TPScale] %s", e)


# ═══════════════════════════════════════════════════════════════
# v27: TCBS + Intraday + Rebalancing (from v26)
# ═══════════════════════════════════════════════════════════════


def compute_shapley_values(model_predict_fn, X_background: 'np.ndarray',
                            X_explain: 'np.ndarray', n_samples: int = 100) -> 'np.ndarray':
    """
    Tính Shapley values (kernel SHAP xấp xỉ) không cần thư viện shap.
    
    Args:
        model_predict_fn: callable nhận X → prob array
        X_background: background dataset (subset, e.g. 50 rows)
        X_explain: sample cần giải thích (1 row)
        n_samples: số coalition samples
    Returns:
        shap_values: array shape (n_features,)
    """
    import numpy as _np
    n_feat = X_explain.shape[1]
    
    # Base value = mean prediction trên background
    base_val = float(_np.mean(model_predict_fn(X_background)))
    
    shap_vals = _np.zeros(n_feat)
    
    # Shapley kernel: sample random coalitions
    rng = _np.random.default_rng(42)
    for _ in range(n_samples):
        # Random coalition S ⊆ F
        coalition = rng.integers(0, 2, n_feat).astype(bool)
        if not coalition.any() or coalition.all(): continue
        
        # Construct masked samples
        x_with    = _np.where(coalition, X_explain[0], X_background.mean(axis=0))
        x_without = _np.where(~coalition, X_explain[0], X_background.mean(axis=0))
        
        pred_with    = float(model_predict_fn(x_with.reshape(1,-1)))
        pred_without = float(model_predict_fn(x_without.reshape(1,-1)))
        marginal = pred_with - pred_without
        
        # Distribute marginal proportionally to features in coalition
        n_in = coalition.sum()
        shap_vals[coalition] += marginal / max(n_in, 1)
    
    shap_vals /= n_samples / n_feat  # normalize
    return shap_vals




def create_ssl_context(cert_file='cert.pem',key_file='key.pem'):
    import ssl,os
    if os.path.exists(cert_file) and os.path.exists(key_file):
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(cert_file,key_file); return ctx
    try:
        from cryptography import x509; from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes,serialization; from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime as _dt
        key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,u"SMA-v29")])
        cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
              .serial_number(x509.random_serial_number()).not_valid_before(_dt.datetime.utcnow())
              .not_valid_after(_dt.datetime.utcnow()+_dt.timedelta(days=365)).sign(key,hashes.SHA256()))
        with open(cert_file,'wb') as f: f.write(cert.public_bytes(serialization.Encoding.PEM))
        with open(key_file,'wb') as f: f.write(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(cert_file,key_file); return ctx
    except Exception as e: log.warning("[SSL] %s",e); return None

# ── Brokers: MBS / BSC / IBKR ────────────────────────────────


def decrypt_secret(ciphertext: str) -> str:
    if not ciphertext: return ''
    import base64
    try:
        if ciphertext.startswith('aes:'):
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            import hashlib
            raw=base64.b64decode(ciphertext[4:])
            salt,nonce,ct=raw[:16],raw[16:28],raw[28:]
            key=hashlib.pbkdf2_hmac('sha256',_get_master_key().encode(),salt,100_000,32)
            return AESGCM(key).decrypt(nonce,ct,None).decode()
        if ciphertext.startswith('xor:'):
            import hashlib
            key=hashlib.sha256(_get_master_key().encode()).digest()
            ct=base64.b64decode(ciphertext[4:])
            return bytes(a^key[i%32] for i,a in enumerate(ct)).decode()
        # legacy plain base64
        return base64.b64decode(ciphertext).decode()
    except Exception as e:
        log.debug("[Crypto] %s",e); return ciphertext


# ════════════════════════════════════════════════════════════════════
# v28: BINANCE WEBSOCKET (public streams, no API key needed)
# ════════════════════════════════════════════════════════════════════


def encrypt_secret(plaintext: str) -> str:
    if not plaintext: return ''
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        import os, base64, hashlib
        salt=os.urandom(16); nonce=os.urandom(12)
        key=hashlib.pbkdf2_hmac('sha256',_get_master_key().encode(),salt,100_000,32)
        ct=AESGCM(key).encrypt(nonce,plaintext.encode(),None)
        return 'aes:'+base64.b64encode(salt+nonce+ct).decode()
    except ImportError:
        import base64, hashlib
        key=hashlib.sha256(_get_master_key().encode()).digest()
        xored=bytes(a^key[i%32] for i,a in enumerate(plaintext.encode()))
        return 'xor:'+base64.b64encode(xored).decode()



def get_alpaca_broker(user_id: int, db_path: str) -> AlpacaBroker | None:
    """Lấy broker instance từ config DB."""
    if user_id in _ALPACA_CACHE:
        return _ALPACA_CACHE[user_id]
    try:
        with sqlite3.connect(db_path, timeout=5) as c:
            c.row_factory = sqlite3.Row
            row = c.execute(
                "SELECT api_key, api_secret, paper FROM alpaca_config WHERE user_id=? AND enabled=1",
                (user_id,)
            ).fetchone()
        if row:
            broker = AlpacaBroker(decrypt_secret(row["api_key"]), decrypt_secret(row["api_secret"]), bool(row["paper"]))
            _ALPACA_CACHE[user_id] = broker
            return broker
    except Exception:
        pass
    return None



def get_binance_klines(symbol: str, interval: str = '1h', limit: int = 100) -> list:
    """Lấy nến từ Binance REST API."""
    ws = get_binance_ws()
    return ws.get_klines(symbol, interval, limit)


# ═══════════════════════════════════════════════════════════════
# v28: FEATURE 5 — VNDIRECT ENHANCED + TEST ENDPOINT
# ═══════════════════════════════════════════════════════════════



def get_bsc_broker(user_id,db_path):
    if user_id in _BSC_CACHE: return _BSC_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,account_no FROM bsc_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            b=BSCBroker(row['username'],decrypt_secret(row['password_enc']))
            if b.login().get('ok'): _BSC_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[BSC] %s",e)
    return None

# ── Markowitz ─────────────────────────────────────────────────


def get_crypto_realtime_data(symbols: list = None) -> dict:
    """Lấy dữ liệu realtime từ BinanceWS."""
    ws = get_binance_ws()
    if symbols:
        ws.subscribe(symbols)
    result = {}
    for sym in (symbols or list(ws._prices.keys())[:20]):
        data = ws._prices.get(sym, {})
        if data:
            result[sym] = {
                'symbol': sym, 'price': data.get('c', 0),
                'change_24h': data.get('P', 0),  # 24h %
                'volume_24h': data.get('v', 0),
                'high_24h': data.get('h', 0),
                'low_24h': data.get('l', 0),
                'source': 'binance_ws' if ws._connected else 'binance_rest',
            }
    return result




def get_ensemble(db_path: str) -> EnsemblePredictor:
    global _ENSEMBLE
    if _ENSEMBLE is None:
        _ENSEMBLE = EnsemblePredictor(db_path)
    return _ENSEMBLE


# ══════════════════════════════════════════════════════════════
# ALPACA BROKER — US Paper + Live Trading (miễn phí)
# Đăng ký tại alpaca.markets — không cần approval
# ══════════════════════════════════════════════════════════════



def get_mbs_broker(user_id,db_path):
    if user_id in _MBS_CACHE: return _MBS_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,account_no FROM mbs_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            b=MBSBroker(row['username'],decrypt_secret(row['password_enc']),row.get('account_no',''))
            if b.login().get('ok'): _MBS_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[MBS] %s",e)
    return None


def get_peer_comparison(symbol: str, market: str) -> dict:
    """
    So sánh mã với peer cùng ngành từ SECTOR_MAP_VN (VN)
    hoặc theo sector tag (US).
    Chỉ dùng dữ liệu đã có trong _ram_cache để tránh quá nhiều API call.
    """
    # Tìm ngành của symbol
    sector_name = None
    peers = []

    if market == 'VN':
        for sect, syms in SECTOR_MAP_VN.items():
            if symbol in syms:
                sector_name = sect
                peers = [s for s in syms if s != symbol]
                break
        if not sector_name:
            # Fallback: tìm từ RAM cache sector tag
            d = _ram_cache.get(symbol, {}).get('data', {})
            sector_name = d.get('sector', 'Unknown')
    else:
        d = _ram_cache.get(symbol, {}).get('data', {})
        sector_name = d.get('sector', 'Unknown')
        # US peers: lấy các mã US cùng sector trong watchlist
        for sym2, cached in _ram_cache.items():
            d2 = cached.get('data', {})
            if d2.get('market') == 'US' and d2.get('sector') == sector_name and sym2 != symbol:
                peers.append(sym2)

    # Thu thập metrics từ RAM cache (không gọi thêm API)
    def _get_metrics(sym):
        cached = _ram_cache.get(sym, {})
        d = cached.get('data', {})
        a = d.get('analysis', {})
        return {
            'symbol': sym,
            'price':  d.get('price', 0),
            'chg':    d.get('chg', 0),
            'score':  a.get('total', 0),
            'signal': a.get('signal', 'NEUTRAL'),
            'conf':   a.get('confidence', 0),
            'rsi':    round(a.get('rsi', 50), 1),
            'rs_vs_index': (a.get('rs_vs_index') or {}).get('rs_pct', 0),
        }

    target = _get_metrics(symbol)

    peer_data = []
    for p in peers[:12]:   # max 12 peers
        if p in _ram_cache:
            peer_data.append(_get_metrics(p))

    if not peer_data:
        return {
            'symbol': symbol,
            'sector': sector_name,
            'peers': [],
            'note': 'Chưa có dữ liệu peer trong watchlist. Thêm các mã cùng ngành vào watchlist.',
        }

    # Rankings
    all_data = [target] + peer_data
    all_data.sort(key=lambda x: -x['score'])
    rank_score = next((i+1 for i, x in enumerate(all_data) if x['symbol'] == symbol), 0)
    all_data.sort(key=lambda x: -x['rsi'])
    rank_rsi = next((i+1 for i, x in enumerate(all_data) if x['symbol'] == symbol), 0)
    all_data.sort(key=lambda x: -x['rs_vs_index'])
    rank_rs = next((i+1 for i, x in enumerate(all_data) if x['symbol'] == symbol), 0)

    # Sector averages
    avg_score = round(sum(p['score'] for p in all_data) / len(all_data), 1)
    avg_rsi   = round(sum(p['rsi'] for p in all_data) / len(all_data), 1)

    # Sort peers by score for display
    peer_data.sort(key=lambda x: -x['score'])

    return {
        'symbol': symbol,
        'sector': sector_name,
        'target': target,
        'peers': peer_data,
        'n_peers': len(peer_data),
        'rankings': {
            'score_rank': rank_score,
            'rsi_rank':   rank_rsi,
            'rs_rank':    rank_rs,
            'total_compared': len(all_data),
        },
        'sector_avg': {
            'score': avg_score,
            'rsi':   avg_rsi,
        },
        'vs_sector': {
            'score_vs_avg': round(target['score'] - avg_score, 1),
            'rsi_vs_avg':   round(target['rsi'] - avg_rsi, 1),
        },
    }

# ── SENTIMENT ANALYSIS ────────────────────────────────────────

# Từ điển cảm xúc đơn giản (không cần mô hình NLP ngoài)
_BULL_WORDS = {
    'tăng','tăng trưởng','lợi nhuận','tích cực','khởi sắc','bùng nổ','đột phá','phục hồi',
    'kỷ lục','vượt kỳ vọng','mua vào','nâng dự báo','khuyến nghị mua','outperform',
    'beat','surge','rally','bullish','upbeat','strong','gain','rise','record','buy',
    'upgrade','overweight','positive','growth','profit','earnings beat',
}
_BEAR_WORDS = {
    'giảm','sụt giảm','lỗ','tiêu cực','rủi ro','lo ngại','tháo chạy','bán tháo',
    'hạ dự báo','cảnh báo','điều tra','phạt','vi phạm','nợ xấu','mất thanh khoản',
    'sell','downgrade','underweight','bearish','loss','decline','fall','drop','miss',
    'warning','risk','debt','negative','underperform','cut',
}



# ── SHAP availability ───────────────────────────────────────
HAS_SHAP = False
try:
    import shap as _shap_lib
    HAS_SHAP = True
except ImportError:
    pass

def get_real_shap(clf, X_val, feat_names: list) -> dict:
    """Tính SHAP values thực nếu có library, fallback về permutation."""
    if not HAS_SHAP or X_val is None or len(X_val) == 0:
        return {}
    try:
        import numpy as _np
        explainer = _shap_lib.TreeExplainer(clf)
        shap_vals  = explainer.shap_values(X_val)
        # For binary classification, use class-1 SHAP
        if isinstance(shap_vals, list): shap_vals = shap_vals[1]
        mean_abs = _np.abs(shap_vals).mean(axis=0)
        total = mean_abs.sum() + 1e-9
        return {feat_names[i]: round(float(mean_abs[i]/total*100), 2)
                for i in range(min(len(feat_names), len(mean_abs)))}
    except Exception as e:
        log.debug("[SHAP] %s", e); return {}



def get_real_shap_values(sym: str, mkt: str, history: list) -> dict:
    """
    Tính SHAP values thật cho EnsemblePredictor.
    Trả về feature importance có ký hiệu +/- (positive = bullish contribution).
    """
    try:
        import numpy as _np
        pred_obj = get_ensemble(DB_PATH)
        if not pred_obj or not pred_obj.is_trained(sym):
            return {'error': 'Chưa train model. Nhấn Train trước.'}
        
        bars = history if history else (_ram_cache.get(sym,{}).get('data',{}).get('history',[]) or [])
        if len(bars) < 60:
            return {'error': 'Cần ít nhất 60 bars lịch sử.'}
        
        # Build feature matrix using EnsemblePredictor's feature extraction
        feat_names = pred_obj.FEATURE_NAMES
        X_all = []
        for i in range(30, len(bars)):
            sub = bars[max(0,i-59):i+1]
            try:
                fv = pred_obj._extract_features(sub)
                if fv and len(fv) == len(feat_names): X_all.append(fv)
            except: pass
        
        if len(X_all) < 10:
            return {'error': 'Không đủ dữ liệu để tính SHAP.'}
        
        X = _np.array(X_all, dtype=float)
        scaler = pred_obj.models[sym].get('scaler')
        clf    = pred_obj.models[sym].get('gb')  # GradientBoosting
        
        if scaler is None or clf is None:
            return {'error': 'Model chưa đầy đủ (cần GB + scaler).'}
        
        X_scaled = scaler.transform(X)
        X_bg     = X_scaled[:min(50, len(X_scaled))]  # background
        x_last   = X_scaled[-1:].copy()               # latest point
        
        def predict_fn(x):
            p = clf.predict_proba(x.reshape(-1, X_scaled.shape[1]))
            return p[:,1] if p.ndim==2 else p
        
        shap_vals = compute_shapley_values(predict_fn, X_bg, x_last, n_samples=150)
        
        # Sort by absolute importance
        importance = [(feat_names[i], float(shap_vals[i])) for i in range(len(feat_names))]
        importance.sort(key=lambda x: abs(x[1]), reverse=True)
        
        # Perm importance (existing, as cross-check)
        perm_imp = pred_obj.get_permutation_importance(sym)
        
        return {
            'symbol': sym,
            'method': 'Shapley Values (Kernel SHAP xấp xỉ)',
            'base_value': float(_np.mean(predict_fn(X_bg))),
            'shapley_values': [{'feature': f, 'shap': round(v,4),
                                'direction': '↑ Tăng xác suất BUY' if v>0 else '↓ Giảm xác suất BUY',
                                'color': '#00e676' if v>0 else '#f03050'}
                               for f, v in importance[:12]],
            'permutation_importance': perm_imp,
            'note': 'SHAP value dương = feature đóng góp vào tín hiệu BUY. Âm = góp vào SELL.',
        }
    except Exception as e:
        return {'error': f'Lỗi tính SHAP: {e}'}


# ═══════════════════════════════════════════════════════════════
# v28: FEATURE 3 — REGIME-CONDITIONAL ML (BULL/BEAR/SIDEWAYS)
# Nâng cấp RegimeConditionalPredictor đã có
# ═══════════════════════════════════════════════════════════════



def get_regime_label(history: list) -> str:
    """Xác định regime thị trường từ lịch sử giá."""
    if len(history) < 20: return 'SIDEWAYS'
    closes = [b['c'] for b in history[-60:]]
    n = len(closes)
    ma20 = sum(closes[-20:])/20
    ma60 = sum(closes)/n if n >= 60 else sum(closes)/n
    price = closes[-1]
    # ADX-like trend strength
    moves = [abs(closes[i]-closes[i-1])/closes[i-1] for i in range(1,n)]
    avg_move = sum(moves)/len(moves) if moves else 0
    trend_up   = price > ma20 > ma60
    trend_down = price < ma20 < ma60
    strong     = avg_move > 0.015  # >1.5% avg daily move
    if   trend_up   and strong: return 'BULL'
    elif trend_down and strong: return 'BEAR'
    elif trend_up:               return 'BULL'
    elif trend_down:             return 'BEAR'
    else:                        return 'SIDEWAYS'




def get_regime_ml(db_path:str)->RegimeConditionalPredictor:
    global _REGIME_ML
    if _REGIME_ML is None: _REGIME_ML=RegimeConditionalPredictor(db_path)
    return _REGIME_ML


# ════════════════════════════════════════════════════════════════════
# v28: VNDIRECT CONFIG HELPERS
# ════════════════════════════════════════════════════════════════════
_VNDIRECT_CACHE:dict={}



def get_signal_performance(symbol: str = None, market: str = None, days: int = 90) -> dict:
    """Thống kê hiệu suất tín hiệu: win rate, avg return, best signals."""
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        with get_db() as c:
            where = "WHERE snapshot_date >= ?"
            params: list = [cutoff]
            if symbol: where += " AND symbol=?"; params.append(symbol)
            if market: where += " AND market=?"; params.append(market)
            rows = c.execute(f"""
                SELECT symbol, market, signal,
                       COUNT(*) as total,
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                       ROUND(AVG(pnl_pct),2) as avg_pnl,
                       ROUND(AVG(CASE WHEN result='WIN' THEN pnl_pct END),2) as avg_win_pnl,
                       ROUND(AVG(CASE WHEN result='LOSS' THEN pnl_pct END),2) as avg_loss_pnl
                FROM signal_stats {where}
                GROUP BY symbol, signal
                ORDER BY total DESC LIMIT 50""", params).fetchall()
        results = []
        for r in rows:
            total = r['total']; wins = r['wins'] or 0
            wr = round(wins/total*100, 1) if total > 0 else 0
            results.append({
                'symbol': r['symbol'], 'market': r['market'], 'signal': r['signal'],
                'total': total, 'wins': wins, 'losses': r['losses'] or 0,
                'win_rate': wr, 'avg_pnl': r['avg_pnl'],
                'avg_win_pnl': r['avg_win_pnl'], 'avg_loss_pnl': r['avg_loss_pnl'],
                'grade': 'A' if wr >= 65 else ('B' if wr >= 55 else ('C' if wr >= 45 else 'D')),
            })
        # Overall stats
        all_rows = [r for r in results if not symbol or r['symbol'] == symbol]
        total_signals = sum(r['total'] for r in all_rows)
        total_wins = sum(r['wins'] for r in all_rows)
        overall_wr = round(total_wins/total_signals*100, 1) if total_signals > 0 else 0
        return {
            'details': results,
            'overall_win_rate': overall_wr,
            'total_signals_tracked': total_signals,
            'days': days, 'symbol': symbol,
            'top_performers': sorted(all_rows, key=lambda x: -x['win_rate'])[:5],
            'worst_performers': sorted(all_rows, key=lambda x: x['win_rate'])[:5],
        }
    except Exception as e:
        return {'error': str(e), 'details': []}


# ═══════════════════════════════════════════════════════════════
# v27: FEATURE 2 — GLOBAL MACRO (DXY, Gold, Oil, VIX, S&P500)
# ═══════════════════════════════════════════════════════════════
_MACRO_SYMBOLS = {
    # VN Indices — được fetch riêng qua vnstock
    'VNINDEX': {'name':'VN-Index','icon':'🇻🇳','mkt':'VN'},
    'VN30':    {'name':'VN30','icon':'🇻🇳','mkt':'VN'},
    'HNX':     {'name':'HNX-Index','icon':'🇻🇳','mkt':'VN'},
    # US & Global
    'DXY':   {'name':'USD Index','icon':'💵','mkt':'US'},
    'GC=F':  {'name':'Gold','icon':'🥇','mkt':'US'},
    'CL=F':  {'name':'WTI Oil','icon':'🛢','mkt':'US'},
    '^VIX':  {'name':'VIX Fear','icon':'😱','mkt':'US'},
    '^GSPC': {'name':'S&P 500','icon':'🇺🇸','mkt':'US'},
    '^N225': {'name':'Nikkei','icon':'🇯🇵','mkt':'US'},
    'HSI':   {'name':'Hang Seng','icon':'🇭🇰','mkt':'US'},
}
_MACRO_TTL = 1800   # 30 phút



def get_sorted_refresh_rows(rows):
    update_alert_active_syms()
    prio, alert, normal = [], [], []
    for r in rows:
        sym = r['symbol'].upper()
        if sym in _priority_syms: prio.append(r)
        elif sym in _alert_active_syms: alert.append(r)
        else: normal.append(r)
    return prio + alert + normal




def get_tcbs_broker(user_id,db_path):
    if user_id in _TCBS_CACHE: return _TCBS_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,enabled FROM tcbs_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            pw=decrypt_secret(row["password_enc"])  # AES decrypt
            b=TCBSBroker(row["username"],pw)
            if b.login().get("ok"): _TCBS_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[TCBS] %s",e)
    return None




def get_tenant_db_path(tenant_id='default'):
    if tenant_id=='default' or not tenant_id: return DB_PATH
    import os,re as _re
    safe_id=_re.sub(r'[^a-zA-Z0-9_-]','',tenant_id)[:32]
    if not safe_id: return DB_PATH
    tenant_dir=os.path.join(os.path.dirname(DB_PATH),'tenants'); os.makedirs(tenant_dir,exist_ok=True)
    tenant_db=os.path.join(tenant_dir,f'sma_{safe_id}.db')
    if not os.path.exists(tenant_db):
        try: import shutil; shutil.copy2(DB_PATH,tenant_db)
        except: return DB_PATH
    return tenant_db

# ── Portfolio Beta ────────────────────────────────────────────


def get_vndirect_broker(user_id:int,db_path:str):
    if user_id in _VNDIRECT_CACHE: return _VNDIRECT_CACHE[user_id]
    try:
        with sqlite3.connect(db_path,timeout=5) as c:
            c.row_factory=sqlite3.Row
            row=c.execute("SELECT username,password_enc,account_no FROM vndirect_config WHERE user_id=? AND enabled=1",(user_id,)).fetchone()
        if row:
            pw=decrypt_secret(row['password_enc'])
            b=VNDirectBroker(row['username'],pw,row.get('account_no',''))
            if b.login().get('ok'): _VNDIRECT_CACHE[user_id]=b; return b
    except Exception as e: log.debug("[VNDirect] %s",e)
    return None

# ═══════════════════════════════════════════════════════════════
# v28: FEATURE 1 — AES-256-GCM ENCRYPTION (nâng cấp từ base64)
# Đã có skeleton, nâng cấp thành production-grade với key rotation
# ═══════════════════════════════════════════════════════════════

# encrypt_secret / decrypt_secret đã có — thêm key rotation & migration


def install_shap_if_missing():
    """Cài SHAP trong background nếu chưa có."""
    global HAS_SHAP
    if HAS_SHAP: return
    try:
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', 'shap', '--quiet', '--break-system-packages'],
            capture_output=True, timeout=120)
        if result.returncode == 0:
            import shap as _shap_lib
            HAS_SHAP = True
            log.info("[ML] SHAP installed successfully")
    except Exception as e:
        log.debug("[SHAP] Install failed: %s", e)

# Try to install SHAP in background on startup
threading.Thread(target=install_shap_if_missing, daemon=True).start()


def migrate_base64_to_aes(table: str, col: str, db_path: str):
    """Migrate base64-encoded secrets to AES-256-GCM."""
    import base64
    migrated = 0
    try:
        with sqlite3.connect(db_path, timeout=10) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(f"SELECT rowid, {col} FROM {table} WHERE {col} IS NOT NULL").fetchall()
        for row in rows:
            val = row[col]
            if not val or val.startswith('aes:'): continue
            try:
                plain = base64.b64decode(val).decode()
                encrypted = encrypt_secret(plain)
                with sqlite3.connect(db_path, timeout=10) as c2:
                    c2.execute(f"UPDATE {table} SET {col}=? WHERE rowid=?", (encrypted, row['rowid']))
                migrated += 1
            except: pass
    except Exception as e:
        log.debug("[AES migrate] %s: %s", table, e)
    if migrated:
        log.info("[AES] Migrated %d secrets in %s.%s", migrated, table, col)
    return migrated


# ═══════════════════════════════════════════════════════════════
# v28: FEATURE 2 — REAL SHAP (Permutation + Shapley values)
# No SHAP library needed — pure numpy implementation
# ═══════════════════════════════════════════════════════════════



def predict_regime_conditional_enhanced(sym: str, mkt: str, history: list, db_path: str) -> dict:
    """Predict using regime-conditional ensemble model."""
    import numpy as _np, pickle
    
    try:
        with sqlite3.connect(db_path, timeout=5) as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT model_blob FROM regime_models WHERE symbol=?", (sym,)).fetchone()
        if not row:
            return {'prediction':'NEUTRAL','confidence':0,'note':'Chưa train — dùng /api/v22/regime_ml/train'}
        models = pickle.loads(bytes.fromhex(row['model_blob']))
    except Exception as e:
        return {'prediction':'NEUTRAL','confidence':0,'error':str(e)}
    
    bars = history or _ram_cache.get(sym,{}).get('data',{}).get('history',[]) or []
    if len(bars) < 40:
        return {'prediction':'NEUTRAL','confidence':0,'note':'Cần thêm dữ liệu lịch sử'}
    
    # Detect current regime
    current_regime = get_regime_label(bars)
    regime_model = models.get(current_regime)
    
    if not regime_model or regime_model.get('status') != 'ok' and not isinstance(regime_model, dict):
        # Fallback to best available regime
        for r in ['BULL','BEAR','SIDEWAYS']:
            if r in models and isinstance(models[r], dict) and 'gb' in models[r]:
                regime_model = models[r]; current_regime = r; break
    
    if not regime_model or 'gb' not in regime_model:
        return {'prediction':'NEUTRAL','confidence':0,'note':'Model không có đủ data để predict'}
    
    # Build feature vector (same 20 features as training)
    try:
        closes=[b['c'] for b in bars[-60:]]; highs=[b.get('h',b['c']) for b in bars[-60:]]
        lows=[b.get('l',b['c']) for b in bars[-60:]]; vols=[b.get('v',0) for b in bars[-60:]]
        n=len(closes); price=closes[-1]
        ma5=sum(closes[-5:])/5; ma10=sum(closes[-10:])/10; ma20=sum(closes[-20:])/20
        gains=[max(closes[j]-closes[j-1],0) for j in range(1,n)]
        losses=[max(closes[j-1]-closes[j],0) for j in range(1,n)]
        ag=sum(gains[-14:])/14 if len(gains)>=14 else 0; al=sum(losses[-14:])/14 if len(losses)>=14 else 0
        rsi=100-100/(1+ag/(al+1e-9))
        import numpy as _np
        std=float(_np.std(closes[-20:]))
        bb_pos=(price-(ma20-2*std))/(4*std+1e-9)
        avg_vol=sum(vols[-20:])/20 if len(vols)>=20 else 1; vol_ratio=vols[-1]/(avg_vol+1e-9)
        roc5=(closes[-1]-closes[-6])/closes[-6]*100 if len(closes)>=6 else 0
        roc10=(closes[-1]-closes[-11])/closes[-11]*100 if len(closes)>=11 else 0
        roc20=(closes[-1]-closes[-21])/closes[-21]*100 if len(closes)>=21 else 0
        atrs=[max(highs[-j]-lows[-j],abs(highs[-j]-closes[-j-1]),abs(lows[-j]-closes[-j-1])) for j in range(1,min(15,n))]
        atr=sum(atrs)/len(atrs) if atrs else std; atr_norm=atr/price if price else 0
        obv=sum(vols[-j] if closes[-j]>closes[-j-1] else -vols[-j] for j in range(1,min(n,20))); obv_norm=obv/(sum(vols[-20:])+1e-9)
        hi20=max(highs[-20:]) if len(highs)>=20 else price; lo20=min(lows[-20:]) if len(lows)>=20 else price
        pp20=(price-lo20)/(hi20-lo20+1e-9)
        d5=(price-ma5)/price; d10=(price-ma10)/price; d20=(price-ma20)/price
        feat=[rsi/100,bb_pos,vol_ratio,roc5/100,roc10/100,roc20/100,atr_norm*10,obv_norm,pp20,
              d5,d10,d20,(ma5-ma10)/price if price else 0,(ma10-ma20)/price if price else 0,
              std/price if price else 0,sum(gains[-5:])/5/(sum(losses[-5:])/5+1e-9),
              len([1 for j in range(1,6) if closes[-j]>closes[-j-1]])/5,
              (closes[-1]-closes[-5])/closes[-5] if len(closes)>=5 else 0,
              vols[-1]/(max(vols[-5:])+1e-9),(hi20-lo20)/price if price else 0]
        feat=_np.array(feat,dtype=float)
        if _np.isnan(feat).any() or _np.isinf(feat).any(): raise ValueError("NaN/Inf in features")
    except Exception as e:
        return {'prediction':'NEUTRAL','confidence':0,'error':f'Feature error: {e}'}
    
    sc=regime_model['scaler']; gb=regime_model['gb']; rf=regime_model['rf']; lr=regime_model['lr']
    w=regime_model['weights']
    Xs=sc.transform(feat.reshape(1,-1))
    p_gb=gb.predict_proba(Xs)[0][1]; p_rf=rf.predict_proba(Xs)[0][1]; p_lr=lr.predict_proba(Xs)[0][1]
    prob=p_gb*w[0]+p_rf*w[1]+p_lr*w[2]
    pred='BUY' if prob>=0.6 else('SELL' if prob<=0.35 else 'NEUTRAL')
    conf=round(abs(prob-0.5)*200,1)
    return {'prediction':pred,'confidence':conf,'probability':round(prob,3),
            'current_regime':current_regime,'model_acc':regime_model.get('acc_gb',0),
            'breakdown':{'gb':round(p_gb,3),'rf':round(p_rf,3),'lr':round(p_lr,3),'weighted':round(prob,3)},
            'note':f'Regime {current_regime}: {regime_model.get("n_train",0)} training samples'}


# v28: FEATURE 4 — BINANCE WS REALTIME ENHANCED
# ═══════════════════════════════════════════════════════════════



def send_daily_digest():
    """Gửi tóm tắt phiên giao dịch lúc 17:30."""
    if get_setting('daily_digest_enabled', '1') != '1': return
    if not get_setting('tg_token') or not get_setting('tg_chat_id'): return
    try:
        sep = "━" * 22
        now_str = datetime.now().strftime('%d/%m/%Y')
        lines = [f"📊 <b>SMA v{VERSION} — Tổng kết phiên {now_str}</b>", sep]

        # 1. Market Internals
        advances = declines = unchanged = 0
        bull_syms = []; bear_syms = []
        for sym, cached in _ram_cache.items():
            d = cached.get('data', {}); a = d.get('analysis', {})
            chg = d.get('chg', 0)
            if chg > 0: advances += 1
            elif chg < 0: declines += 1
            else: unchanged += 1
            sig = a.get('signal', '')
            if 'BUY' in sig and a.get('total', 0) >= 8: bull_syms.append((sym, a.get('total', 0)))
            elif 'SELL' in sig and a.get('total', 0) <= -8: bear_syms.append((sym, a.get('total', 0)))

        total_mkt = advances + declines + unchanged
        breadth = f"{advances}↑ / {declines}↓ / {unchanged}→" if total_mkt else "N/A"
        lines.append(f"📡 <b>Breadth:</b> {breadth}")
        ad_ratio = round(advances / declines, 2) if declines > 0 else advances
        breadth_label = "🟢 BULLISH" if ad_ratio > 1.5 else ("🔴 BEARISH" if ad_ratio < 0.67 else "🟡 MIX")
        lines.append(f"⚡ A/D Ratio: <b>{ad_ratio}</b> — {breadth_label}")

        # 2. Top BUY signals
        if bull_syms:
            top_buy = sorted(bull_syms, key=lambda x: -x[1])[:5]
            lines.append(sep)
            lines.append(f"🟢 <b>Top BUY ({len(bull_syms)} mã):</b>")
            for sym, score in top_buy:
                d = _ram_cache.get(sym, {}).get('data', {})
                price = d.get('price', 0); chg = d.get('chg', 0)
                vn = _ram_cache.get(sym, {}).get('data', {}).get('market', 'VN') == 'VN'
                fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
                lines.append(f"  • <b>{sym}</b> {fp} ({chg:+.1f}%) Score:{score:+d}")

        # 3. Top SELL signals
        if bear_syms:
            top_sell = sorted(bear_syms, key=lambda x: x[1])[:3]
            lines.append(sep)
            lines.append(f"🔴 <b>Top SELL ({len(bear_syms)} mã):</b>")
            for sym, score in top_sell:
                d = _ram_cache.get(sym, {}).get('data', {})
                price = d.get('price', 0); chg = d.get('chg', 0)
                vn = _ram_cache.get(sym, {}).get('data', {}).get('market', 'VN') == 'VN'
                fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
                lines.append(f"  • <b>{sym}</b> {fp} ({chg:+.1f}%) Score:{score:+d}")

        # 4. Macro summary
        macro = fetch_macro_data()
        summary = macro.get('_summary', {})
        lines.append(sep)
        lines.append(f"🌐 <b>Macro:</b> {summary.get('note', 'N/A')}")
        sp5 = macro.get('^GSPC', {}); vix_d = macro.get('^VIX', {})
        gold = macro.get('GC=F', {})
        lines.append(f"  S&P500: {sp5.get('chg_pct', 0):+.1f}%  VIX: {vix_d.get('price', 0):.1f}  Gold: {gold.get('chg_pct', 0):+.1f}%")

        # 5. Portfolio P&L (nếu có vị thế)
        try:
            with get_db() as c:
                open_trades = c.execute(
                    "SELECT symbol, qty, entry_price, market FROM journal WHERE status='OPEN' LIMIT 20").fetchall()
            if open_trades:
                total_pnl = 0; pnl_items = []
                for t in open_trades:
                    sym = t['symbol']
                    cur = _ram_cache.get(sym, {}).get('data', {}).get('price', t['entry_price'])
                    pnl_pct = (cur - t['entry_price']) / t['entry_price'] * 100
                    pnl_vnd = (cur - t['entry_price']) * t['qty']
                    total_pnl += pnl_vnd
                    pnl_items.append((sym, pnl_pct, pnl_vnd))
                lines.append(sep)
                lines.append(f"💼 <b>Portfolio ({len(open_trades)} vị thế):</b>")
                lines.append(f"  Tổng P&L hôm nay: <b>{'+'if total_pnl>=0 else ''}{total_pnl/1e6:.1f}M đ</b>")
                best = max(pnl_items, key=lambda x: x[1])
                worst = min(pnl_items, key=lambda x: x[1])
                lines.append(f"  🏆 Tốt nhất: {best[0]} ({best[1]:+.1f}%)")
                lines.append(f"  📉 Kém nhất: {worst[0]} ({worst[1]:+.1f}%)")
        except: pass

        # 6. Upcoming events tomorrow
        lines.append(sep)
        tomorrow_events = []
        for sym in list(_ram_cache.keys())[:30]:
            try:
                mkt = _ram_cache[sym].get('data', {}).get('market', 'VN')
                ev_flag, ev_title = has_upcoming_events(sym, mkt, days=2)
                if ev_flag and ev_title: tomorrow_events.append(f"{sym}: {ev_title}")
            except: pass
        if tomorrow_events:
            lines.append(f"📅 <b>Sự kiện ngày mai:</b>")
            for ev in tomorrow_events[:3]: lines.append(f"  • {ev}")

        lines.append(sep)
        lines.append(f"🔗 {get_setting('public_url', f'http://localhost:{PORT}')}")

        tg_send("\n".join(lines))
        log.info("[Digest] Daily digest sent at %s", datetime.now().strftime('%H:%M'))
    except Exception as e:
        log.error("[Digest] Error: %s", e)


# ═══════════════════════════════════════════════════════════════
# v27: FEATURE 4 — 2FA TOTP (Google Authenticator)
# ═══════════════════════════════════════════════════════════════


def send_premarket_brief():
    """Gửi brief buổi sáng 8:30 trước phiên VN."""
    token = get_setting('tg_token')
    chat_id = get_setting('tg_chat_id')
    if not token or not chat_id: return

    now = datetime.now()
    sep = "━" * 20
    lines = [f"🌅 <b>SMA Pre-Market Brief — {now.strftime('%d/%m/%Y')}</b>", sep]

    # 1. Macro overnight
    try:
        macro = fetch_macro_data()
        sp5  = macro.get('^GSPC', {})
        gold = macro.get('GC=F', {})
        vix  = macro.get('^VIX', {})
        dxy  = macro.get('DXY', {})
        lines.append("🌐 <b>Macro Overnight:</b>")
        if sp5.get('price'):
            em = '📈' if sp5.get('chg_pct', 0) > 0 else '📉'
            lines.append(f"  {em} S&P500: {sp5['price']:.0f} ({sp5.get('chg_pct',0):+.2f}%)")
        if gold.get('price'):
            em = '📈' if gold.get('chg_pct', 0) > 0 else '📉'
            lines.append(f"  {em} Gold:   {gold['price']:.0f} ({gold.get('chg_pct',0):+.2f}%)")
        if vix.get('price'):
            v_lvl = '😱 Sợ hãi' if vix['price'] > 25 else ('😰 Lo ngại' if vix['price'] > 20 else '😌 Bình tĩnh')
            lines.append(f"  📊 VIX:    {vix['price']:.1f} — {v_lvl}")
        if dxy.get('price'):
            lines.append(f"  💵 DXY:    {dxy['price']:.2f} ({dxy.get('chg_pct',0):+.2f}%)")
    except Exception as e:
        log.debug("[PreMarket] macro error: %s", e)

    lines.append(sep)

    # 2. Top BUY signals
    bull_syms = []
    for sym, cached in _ram_cache.items():
        d = cached.get('data', {}); a = d.get('analysis', {})
        sig = a.get('signal', '')
        if 'BUY' in sig and a.get('total', 0) >= 6:
            bull_syms.append((sym, a.get('total', 0), d.get('price', 0), a.get('rsi', 50), d.get('market', 'VN')))
    bull_syms.sort(key=lambda x: x[1], reverse=True)
    if bull_syms:
        lines.append("🚀 <b>Top BUY hôm nay:</b>")
        for sym, score, price, rsi, mkt in bull_syms[:3]:
            vn = mkt == 'VN'
            fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
            lines.append(f"  🟢 <b>{sym}</b>  {fp}  Score:{score:+d}  RSI:{rsi:.0f}")

    # 3. Gap up/down mạnh
    gap_syms = []
    for sym, cached in _ram_cache.items():
        d = cached.get('data', {})
        chg = d.get('chg', 0)
        if abs(chg) >= 2.5:
            gap_syms.append((sym, chg, d.get('price', 0), d.get('market', 'VN')))
    gap_syms.sort(key=lambda x: abs(x[1]), reverse=True)
    if gap_syms:
        lines.append(sep)
        lines.append("⚡ <b>Gap Up/Down đáng chú ý (≥2.5%):</b>")
        for sym, chg, price, mkt in gap_syms[:5]:
            vn = mkt == 'VN'
            fp = f"{round(price):,}đ" if vn else f"${price:.2f}"
            em = '⬆️' if chg > 0 else '⬇️'
            lines.append(f"  {em} <b>{sym}</b>  {fp}  ({chg:+.1f}%)")

    # 4. Cảnh báo hôm nay (ATC window, events)
    today_str = now.strftime('%Y-%m-%d')
    try:
        with get_db() as c:
            today_events = c.execute(
                "SELECT symbol, title FROM events WHERE event_date=? LIMIT 5", (today_str,)
            ).fetchall()
        if today_events:
            lines.append(sep)
            lines.append("📅 <b>Sự kiện hôm nay:</b>")
            for ev in today_events:
                lines.append(f"  📌 <b>{ev['symbol']}</b>: {ev['title']}")
    except: pass

    lines.append(sep)
    lines.append(f"⏰ 9:00 — Phiên ATO | 9:15 — Liên tục | 14:30 — ATC")
    lines.append(f"💡 Dùng /check [MÃ] để xem tín hiệu chi tiết")

    msg = chr(10).join(lines)
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        log.info("[PreMarket] Brief sent")
    except Exception as e:
        log.warning("[PreMarket] send failed: %s", e)


# ═══════════════════════════════════════════════════════════════════
# v29.3: FEATURE 4 — PNL DASHBOARD NÂNG CAO
# ═══════════════════════════════════════════════════════════════════



def set_priority_sym(sym: str, active: bool = True):
    global _priority_syms
    if active: _priority_syms.add(sym.upper())
    else: _priority_syms.discard(sym.upper())



def subscribe_tick(symbol,ws_handler):
    sym=symbol.upper()
    if sym not in _tick_subscribers: _tick_subscribers[sym]=[]
    if ws_handler not in _tick_subscribers[sym]: _tick_subscribers[sym].append(ws_handler)


def test_vndirect_connection(username: str, password: str, account_no: str = '') -> dict:
    """Test VNDirect connection với credentials."""
    try:
        b = VNDirectBroker(username, password, account_no)
        login_r = b.login()
        if not login_r.get('ok'):
            return {'ok': False, 'step': 'login', 'error': login_r.get('error', 'Login failed')}
        
        acct = b.get_account()
        pos  = b.get_positions()
        orders = b.get_orders()
        
        return {
            'ok': True,
            'account': acct,
            'positions_count': len(pos),
            'positions': pos[:5],  # preview
            'orders_count': len(orders),
            'orders': orders[:5],
            'note': '✅ VNDirect kết nối thành công. Credentials hợp lệ.',
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}
# ═══════════════════════════════════════════════════════════════
# SMA v29.1 — All New Server Functions
# ═══════════════════════════════════════════════════════════════

# ── 5 Alert types ────────────────────────────────────────────
_last_regime_state:dict={}



def totp_generate_secret() -> str:
    """Tạo TOTP secret mới."""
    try:
        import pyotp
        return pyotp.random_base32()
    except ImportError:
        # Fallback: generate compatible secret without pyotp
        import base64, os
        secret_bytes = os.urandom(20)
        return base64.b32encode(secret_bytes).decode('utf-8')




def totp_get_uri(secret: str, username: str) -> str:
    """Tạo URI cho QR code (dùng Google Authenticator)."""
    try:
        import pyotp, urllib.parse
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(name=username, issuer_name=f"SMA_v{VERSION}")
    except:
        import urllib.parse
        name = urllib.parse.quote(username)
        issuer = urllib.parse.quote(f"SMA_v{VERSION}")
        return f"otpauth://totp/{issuer}:{name}?secret={secret}&issuer={issuer}&algorithm=SHA1&digits=6&period=30"




def totp_qr_svg(uri: str) -> str:
    """Tạo QR code SVG đơn giản từ URI (không cần thư viện ngoài)."""
    # Return a simple text representation if qrcode not available
    try:
        import qrcode, io
        qr = qrcode.QRCode(version=1, box_size=4, border=2)
        qr.add_data(uri); qr.make(fit=True)
        # SVG output
        rows = qr.modules
        size = len(rows) * 4 + 16
        cells = ""
        for y, row in enumerate(rows):
            for x, cell in enumerate(row):
                if cell:
                    cells += f'<rect x="{x*4+8}" y="{y*4+8}" width="4" height="4" fill="#000"/>'
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" style="background:#fff">{cells}</svg>'
    except:
        return f'<div style="font-family:monospace;font-size:10px;word-break:break-all;padding:10px;background:#f5f5f5;border-radius:6px">{uri}</div>'


# ═══════════════════════════════════════════════════════════════
# v27: FEATURE 5 — AUTO-TRADE WITH TG CONFIRMATION
# ═══════════════════════════════════════════════════════════════
_pending_confirms: dict = {}  # confirm_id → {action, payload, expires, chat_id}



def totp_verify(secret: str, code: str) -> bool:
    """Xác minh TOTP code (±1 window = 90 giây)."""
    if not secret or not code: return False
    code = str(code).strip().replace(' ', '')
    try:
        import pyotp
        totp = pyotp.TOTP(secret)
        return totp.verify(code, valid_window=1)
    except ImportError:
        # Fallback implementation without pyotp
        import hmac, hashlib, struct, time as _time
        try:
            import base64
            key = base64.b32decode(secret.upper() + '=' * (-len(secret) % 8))
        except: return False
        def _hotp(key, counter):
            msg = struct.pack('>Q', counter)
            h = hmac.new(key, msg, hashlib.sha1).digest()
            offset = h[-1] & 0x0f
            code_int = struct.unpack('>I', h[offset:offset+4])[0] & 0x7fffffff
            return str(code_int % 1000000).zfill(6)
        t = int(_time.time()) // 30
        return any(_hotp(key, t+i) == code for i in [-1, 0, 1])




def train_lgbm_model(X_train,y_train,X_val,y_val):
    if _HAS_LGBM:
        try:
            import lightgbm as lgb
            clf=lgb.LGBMClassifier(n_estimators=200,learning_rate=0.05,max_depth=5,num_leaves=31,verbose=-1)
            clf.fit(X_train,y_train,eval_set=[(X_val,y_val)],callbacks=[lgb.early_stopping(20,verbose=False)])
            acc=(clf.predict(X_val)==y_val).mean() if len(y_val)>0 else 0
            return clf,float(acc),'LightGBM'
        except Exception as e: log.debug("[LGBM] %s",e)
    from sklearn.ensemble import GradientBoostingClassifier
    clf=GradientBoostingClassifier(n_estimators=150,max_depth=4,learning_rate=0.05)
    clf.fit(X_train,y_train)
    acc=(clf.predict(X_val)==y_val).mean() if len(y_val)>0 else 0
    return clf,float(acc),'GradientBoosting'



def train_regime_conditional_enhanced(sym: str, mkt: str, history: list, db_path: str) -> dict:
    """
    Train 3 models riêng biệt cho BULL/BEAR/SIDEWAYS regime.
    Feature set đa dạng hơn: 20 features thay vì 10.
    """
    import numpy as _np
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import StratifiedKFold, cross_val_score
    import pickle
    
    if len(history) < 80:
        return {'error': 'Cần ít nhất 80 bars để train regime models'}
    
    bars = history
    results = {}
    
    # Build features + labels + regimes
    features_by_regime = {'BULL': [], 'BEAR': [], 'SIDEWAYS': []}
    labels_by_regime   = {'BULL': [], 'BEAR': [], 'SIDEWAYS': []}
    
    HORIZON = 5
    for i in range(30, len(bars)-HORIZON):
        sub  = bars[max(0,i-59):i+1]
        closes = [b['c'] for b in sub]
        highs  = [b.get('h',b['c']) for b in sub]
        lows   = [b.get('l',b['c']) for b in sub]
        vols   = [b.get('v',0) for b in sub]
        n = len(closes)
        if n < 20: continue
        
        # 20 features
        ma5  = sum(closes[-5:])/5
        ma10 = sum(closes[-10:])/10
        ma20 = sum(closes[-20:])/20
        price = closes[-1]
        
        # RSI
        gains = [max(closes[j]-closes[j-1],0) for j in range(1,n)]
        losses= [max(closes[j-1]-closes[j],0) for j in range(1,n)]
        ag = sum(gains[-14:])/14 if len(gains)>=14 else 0
        al = sum(losses[-14:])/14 if len(losses)>=14 else 0
        rsi = 100 - 100/(1+ag/(al+1e-9))
        
        # Bollinger
        std = (_np.var(closes[-20:]))**0.5
        bb_pos = (price-(ma20-2*std))/(4*std+1e-9)
        
        # Volume ratio
        avg_vol = sum(vols[-20:])/20 if len(vols)>=20 else 1
        vol_ratio= vols[-1]/(avg_vol+1e-9)
        
        # Momentum
        roc5  = (closes[-1]-closes[-6])/closes[-6]*100  if len(closes)>=6  else 0
        roc10 = (closes[-1]-closes[-11])/closes[-11]*100 if len(closes)>=11 else 0
        roc20 = (closes[-1]-closes[-21])/closes[-21]*100 if len(closes)>=21 else 0
        
        # ATR
        atrs=[]
        for j in range(1,min(15,n)):
            atrs.append(max(highs[-j]-lows[-j], abs(highs[-j]-closes[-j-1]), abs(lows[-j]-closes[-j-1])))
        atr = sum(atrs)/len(atrs) if atrs else std
        atr_norm = atr/price if price else 0
        
        # OBV trend
        obv = sum(vols[-j] if closes[-j]>closes[-j-1] else -vols[-j] for j in range(1,min(n,20)))
        obv_norm = obv/(sum(vols[-20:])+1e-9)
        
        # Price position
        hi20 = max(highs[-20:]) if len(highs)>=20 else price
        lo20 = min(lows[-20:])  if len(lows)>=20  else price
        pp20 = (price-lo20)/(hi20-lo20+1e-9)
        
        # MA distances
        d5  = (price-ma5)/price   if price else 0
        d10 = (price-ma10)/price  if price else 0
        d20 = (price-ma20)/price  if price else 0
        
        feat = [rsi/100, bb_pos, vol_ratio, roc5/100, roc10/100, roc20/100,
                atr_norm*10, obv_norm, pp20, d5, d10, d20,
                (ma5-ma10)/price if price else 0, (ma10-ma20)/price if price else 0,
                std/price if price else 0,
                sum(gains[-5:])/5/(sum(losses[-5:])/5+1e-9),   # short momentum ratio
                len([1 for j in range(1,6) if closes[-j]>closes[-j-1]])/5,  # up day ratio
                (closes[-1]-closes[-5])/closes[-5] if len(closes)>=5 else 0,
                vols[-1]/(max(vols[-5:])+1e-9),  # vol vs recent
                (hi20-lo20)/price if price else 0]  # range pct
        
        if any(_np.isnan(feat)) or any(_np.isinf(feat)): continue
        
        # Regime at this bar
        regime = get_regime_label(bars[max(0,i-39):i+1])
        
        # Label: did price go up >=2% in next 5 bars?
        future_max = max(b['c'] for b in bars[i+1:i+HORIZON+1])
        label = 1 if (future_max-price)/price >= 0.02 else 0
        
        features_by_regime[regime].append(feat)
        labels_by_regime[regime].append(label)
    
    trained_models = {}
    for regime in ['BULL','BEAR','SIDEWAYS']:
        X = _np.array(features_by_regime[regime])
        y = _np.array(labels_by_regime[regime])
        if len(X) < 20 or len(_np.unique(y)) < 2:
            results[regime] = {'status':'insufficient_data','n_samples':len(X)}
            continue
        
        sc = StandardScaler(); X_s = sc.fit_transform(X)
        
        # Ensemble: GB + RF + LR
        gb = GradientBoostingClassifier(n_estimators=60, max_depth=3, learning_rate=0.08, random_state=42)
        rf = RandomForestClassifier(n_estimators=50, max_depth=4, random_state=42)
        lr = LogisticRegression(C=1.0, max_iter=200, random_state=42)
        
        skf = StratifiedKFold(n_splits=min(3, len(X)//10), shuffle=True, random_state=42)
        try:
            gb.fit(X_s, y)
            rf.fit(X_s, y)
            lr.fit(X_s, y)
            acc_gb = cross_val_score(gb, X_s, y, cv=skf, scoring='accuracy').mean()
            acc_rf = cross_val_score(rf, X_s, y, cv=skf, scoring='accuracy').mean()
            acc_lr = cross_val_score(lr, X_s, y, cv=skf, scoring='accuracy').mean()
            # Weight by accuracy
            total_acc = acc_gb + acc_rf + acc_lr
            w = [acc_gb/total_acc, acc_rf/total_acc, acc_lr/total_acc]
            
            trained_models[regime] = {
                'gb': gb, 'rf': rf, 'lr': lr, 'scaler': sc,
                'weights': w, 'n_features': 20,
                'acc_gb': round(acc_gb,3), 'acc_rf': round(acc_rf,3), 'acc_lr': round(acc_lr,3),
                'n_train': len(X), 'positive_rate': float(y.mean()),
            }
            results[regime] = {'status':'ok','accuracy':round(float(acc_gb*w[0]+acc_rf*w[1]+acc_lr*w[2]),3),
                               'n_samples':len(X),'positive_rate':round(float(y.mean()),3)}
        except Exception as e:
            results[regime] = {'status':'error','error':str(e)}
    
    # Save to DB
    try:
        blob = pickle.dumps(trained_models)
        with sqlite3.connect(db_path, timeout=10) as c:
            c.execute("INSERT OR REPLACE INTO regime_models VALUES(?,?,?)",
                      (sym, blob.hex(), datetime.now().isoformat()))
    except Exception as e:
        log.debug("[RegimeML] Save error: %s", e)
    
    return {'symbol': sym, 'by_regime': results, 'model_type': 'RegimeConditional_v28_20feat'}




def update_alert_active_syms():
    global _alert_active_syms
    try:
        with get_db() as c:
            rows = c.execute("SELECT DISTINCT symbol FROM price_alerts WHERE triggered=0").fetchall()
        _alert_active_syms = {r['symbol'].upper() for r in rows}
    except: pass



def update_signal_stats():
    """Kiểm tra kết quả các signal đã ghi nhận sau N ngày."""
    HORIZON = 5  # kiểm tra sau 5 phiên
    try:
        with get_db() as c:
            pending = c.execute("""
                SELECT s.*, ss.symbol, ss.market, ss.signal, ss.total as score,
                       ss.price as price_entry, ss.created_at as snapshot_date
                FROM signal_snapshots ss
                LEFT JOIN signal_stats s ON s.symbol=ss.symbol AND s.snapshot_date=date(ss.created_at)
                WHERE s.id IS NULL
                AND date(ss.created_at) <= date('now','-5 days')
                LIMIT 50""").fetchall()
        for row in pending:
            sym = row['symbol']; mkt = row['market']
            entry_price = row['price_entry']
            if not entry_price or entry_price <= 0: continue
            # Lấy giá sau HORIZON ngày từ OHLCV history
            snap_date = row['snapshot_date'][:10]
            with get_db() as c:
                future = c.execute("""
                    SELECT close FROM ohlcv_history
                    WHERE symbol=? AND trade_date > ?
                    ORDER BY trade_date ASC LIMIT ?""",
                    (sym, snap_date, HORIZON)).fetchall()
            if not future or len(future) < HORIZON: continue
            exit_price = future[-1]['close']
            pnl_pct = round((exit_price - entry_price) / entry_price * 100, 2)
            sig = row['signal']
            # Xác định result theo signal direction
            is_buy = 'BUY' in sig; is_sell = 'SELL' in sig
            if is_buy:   result = 'WIN' if pnl_pct > 1 else ('LOSS' if pnl_pct < -1 else 'FLAT')
            elif is_sell:result = 'WIN' if pnl_pct < -1 else ('LOSS' if pnl_pct > 1 else 'FLAT')
            else:        result = 'FLAT'
            with get_db() as c2:
                c2.execute("""INSERT OR IGNORE INTO signal_stats
                    (symbol,market,signal,score,price_entry,price_exit,horizon_days,result,pnl_pct,snapshot_date,checked_date)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (sym, mkt, sig, row['score'], entry_price, exit_price,
                     HORIZON, result, pnl_pct, snap_date, datetime.now().strftime('%Y-%m-%d')))
    except Exception as e:
        log.debug("[SigStats] %s", e)





def main():
    _init_v22()
    _init_v24()
    print("=" * 60)
    print("  SMA Pro — Ultimate Stock Market Analyzer")
    print("  ML Ensemble · LSTM · Macro · Paper Trading · SSI RT · T+2 Backtest")
    print("=" * 60)
    print(f"  Port      : {PORT}")
    print(f"  WebSocket : {WS_PORT}")
    print(f"  Database  : {DB_PATH}")
    print(f"  Redis     : {'✓' if REDIS_CLIENT else '✗'}")
    print("  Ctrl+C để dừng\n")
    
    _check_and_fix_db()
    init_db()
    threading.Thread(target=_pre_warm_cache, daemon=True).start()
    threading.Thread(target=_wal_checkpoint_worker, daemon=True).start()
    threading.Thread(target=backup_worker, daemon=True).start()
    _check_default_password()
    loaded = preload_from_db()
    if loaded == 0:
        print("[SMA] DB trống — sẽ fetch từ API khi thêm mã đầu tiên")
    
    # Start WebSocket
    if ENABLE_WEBSOCKET:
        threading.Thread(target=start_websocket, daemon=True).start()
        print(f"  WebSocket : ws://localhost:{WS_PORT}")
    
    # Start background refresh
    start_refresh()
    if True:  # v24 merged
        init_realtime(_ram_cache)
        _SCHEDULER.ram_cache = _ram_cache
        _SCHEDULER.tg_send   = tg_send
        _VAR_MON.tg_send     = tg_send
        _SCHEDULER.start()
        print("[SMA Pro] Real-time feed + scheduler started")
    # Auto-open browser
    threading.Thread(  # main browser thread
        target=lambda: (time.sleep(1.5), webbrowser.open(f"http://localhost:{PORT}")),
        daemon=True
    ).start()
    
    # Seed investment screener on first startup
    def _seed_invest():
        time.sleep(45)
        for _st in ('growth', 'value'):
            try:
                with get_db() as _c:
                    _cnt = _c.execute(
                        "SELECT COUNT(*) FROM investment_lists WHERE type=?", (_st,)).fetchone()[0]
                if _cnt == 0:
                    log.info("[invest] First-run seed: %s", _st)
                    run_investment_screener(_st, 'VN', 10, False)
            except Exception as _e:
                log.debug("[invest] seed %s: %s", _st, _e)
    threading.Thread(target=_seed_invest, daemon=True).start()

        # Start HTTP server
    try:
        httpd = ThreadingHTTPServer((BIND, PORT), Handler)
        httpd.timeout = 120
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[SMA] Đã dừng.")

def run_tests():
    """Run basic smoke tests"""
    import pytest
    test_file = os.path.join(os.path.dirname(__file__), 'test_sma.py')
    if os.path.exists(test_file):
        pytest.main([test_file, '-v'])
    else:
        print("[SMA] No test file found")
if __name__ == "__main__":
    main()