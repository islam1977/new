import os
import csv
import json
import time
import math
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIG
# ============================================================

BINANCE_BASE = "https://api.binance.com"

MIN_WATCH_VOLUME = 3_000_000
MIN_FINAL_VOLUME = 5_000_000

MAX_24H_GAIN = 18.0

MIN_15M_GAIN = -2.0
MAX_15M_GAIN = 10.0

MIN_1H_GAIN = -5.0
MAX_1H_GAIN = 18.0

WATCH_SCORE = 65
FINAL_SCORE = 80
EXCEPTIONAL_SCORE = 90

CONFIRMATIONS_REQUIRED = 2
CANDIDATE_EXPIRY_MINUTES = 20

FINAL_COOLDOWN_HOURS = 12

TRACKING_MINUTES = [5, 15, 30, 60, 240, 1440]

STATE_FILE = "state.json"
RESULTS_FILE = "early_pump_results.csv"

MAX_WORKERS = 10

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 BinanceEarlyPumpScanner/4.0"
})

# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def now_iso():
    return now_utc().isoformat()


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def load_state():
    if not os.path.exists(STATE_FILE):
        return {
            "pending": {},
            "final_tracks": {},
            "cooldowns": {}
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        data.setdefault("pending", {})
        data.setdefault("final_tracks", {})
        data.setdefault("cooldowns", {})

        return data

    except Exception as e:
        print("WARNING: Could not load state.json:", e)

        return {
            "pending": {},
            "final_tracks": {},
            "cooldowns": {}
        }


def save_state(state):
    tmp = STATE_FILE + ".tmp"

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    os.replace(tmp, STATE_FILE)


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram disabled: secrets are empty.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        r = session.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=20
        )

        if r.ok:
            return True

        print("Telegram error:", r.status_code, r.text[:500])
        return False

    except Exception as e:
        print("Telegram exception:", e)
        return False


# ============================================================
# BINANCE
# ============================================================

def get_exchange_info():
    r = session.get(
        f"{BINANCE_BASE}/api/v3/exchangeInfo",
        timeout=20
    )
    r.raise_for_status()

    return r.json()


def get_usdt_symbols():
    data = get_exchange_info()

    symbols = []

    for s in data.get("symbols", []):

        if s.get("status") != "TRADING":
            continue

        if s.get("quoteAsset") != "USDT":
            continue

        symbol = s.get("symbol", "")

        # Exclude leveraged tokens
        if any(x in symbol for x in [
            "UPUSDT",
            "DOWNUSDT",
            "BULLUSDT",
            "BEARUSDT"
        ]):
            continue

        symbols.append(symbol)

    return symbols


def get_24h_tickers():
    r = session.get(
        f"{BINANCE_BASE}/api/v3/ticker/24hr",
        timeout=30
    )
    r.raise_for_status()

    return r.json()


def get_klines(symbol, interval="5m", limit=60):
    r = session.get(
        f"{BINANCE_BASE}/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        },
        timeout=20
    )

    r.raise_for_status()

    return r.json()


# ============================================================
# METRICS
# ============================================================

def candle_change(candle):
    open_price = safe_float(candle[1])
    close_price = safe_float(candle[4])

    if open_price <= 0:
        return 0.0

    return ((close_price / open_price) - 1) * 100


def close_location(candle):
    high = safe_float(candle[2])
    low = safe_float(candle[3])
    close = safe_float(candle[4])

    if high <= low:
        return 0.5

    return (close - low) / (high - low)


def analyze_symbol(symbol, ticker):
    volume_24h = safe_float(ticker.get("quoteVolume"))
    gain_24h = safe_float(ticker.get("priceChangePercent"))

    if volume_24h < MIN_WATCH_VOLUME:
        return None

    if gain_24h > MAX_24H_GAIN:
        return None

    klines = get_klines(symbol, "5m", 60)

    if len(klines) < 50:
        return None

    # Remove currently forming candle
    completed = klines[:-1]

    if len(completed) < 49:
        return None

    latest = completed[-1]

    # 15 minutes = last 3 completed 5m candles
    c15 = completed[-3:]

    # 1 hour = last 12 completed 5m candles
    c1h = completed[-12:]

    previous_15 = completed[-6:-3]

    price = safe_float(latest[4])

    if price <= 0:
        return None

    # ----------------------------
    # Momentum
    # ----------------------------

    open_15 = safe_float(c15[0][1])

    if open_15 > 0:
        gain_15m = ((price / open_15) - 1) * 100
    else:
        gain_15m = 0

    open_1h = safe_float(c1h[0][1])

    if open_1h > 0:
        gain_1h = ((price / open_1h) - 1) * 100
    else:
        gain_1h = 0

    if gain_15m < MIN_15M_GAIN or gain_15m > MAX_15M_GAIN:
        return None

    if gain_1h < MIN_1H_GAIN or gain_1h > MAX_1H_GAIN:
        return None

    # ----------------------------
    # Volume
    # ----------------------------

    vol_15m = sum(
        safe_float(c[7])
        for c in c15
    )

    vol_1h = sum(
        safe_float(c[7])
        for c in c1h
    )

    previous_vol_15m = sum(
        safe_float(c[7])
        for c in previous_15
    )

    # Historical baselines
    baseline_15 = []

    baseline_1h = []

    # Last 12 x 15m windows
    for i in range(3, min(len(completed), 39), 3):
        window = completed[-3 - i:-i]

        if len(window) == 3:
            baseline_15.append(
                sum(safe_float(c[7]) for c in window)
            )

    # Last 6 x 1h windows
    for i in range(12, min(len(completed), 60), 12):
        window = completed[-12 - i:-i]

        if len(window) == 12:
            baseline_1h.append(
                sum(safe_float(c[7]) for c in window)
            )

    avg_15 = (
        sum(baseline_15) / len(baseline_15)
        if baseline_15 else 0
    )

    avg_1h = (
        sum(baseline_1h) / len(baseline_1h)
        if baseline_1h else 0
    )

    vol_ratio_15m = (
        vol_15m / avg_15
        if avg_15 > 0 else 0
    )

    vol_ratio_1h = (
        vol_1h / avg_1h
        if avg_1h > 0 else 0
    )

    acceleration = (
        vol_15m / previous_vol_15m
        if previous_vol_15m > 0 else 0
    )

    # ----------------------------
    # Breakout
    # ----------------------------

    previous_4h = completed[-49:-1]

    previous_high = max(
        safe_float(c[2])
        for c in previous_4h
    )

    if previous_high > 0:
        breakout = ((price / previous_high) - 1) * 100
    else:
        breakout = 0

    # ----------------------------
    # Close location
    # ----------------------------

    cl = close_location(latest)

    # ========================================================
    # SCORE
    # ========================================================

    score = 0

    # 1h volume ratio
    if vol_ratio_1h >= 4:
        score += 30
    elif vol_ratio_1h >= 2:
        score += 20
    elif vol_ratio_1h >= 1.5:
        score += 10

    # Acceleration
    if acceleration >= 2.5:
        score += 20
    elif acceleration >= 1.5:
        score += 12
    elif acceleration >= 1.2:
        score += 6

    # 15m momentum
    if 2 <= gain_15m <= 10:
        score += 15
    elif 0 <= gain_15m < 2:
        score += 7
    elif 10 < gain_15m <= 12:
        score += 5

    # Breakout
    if breakout >= 0:
        score += 20
    elif breakout >= -1.5:
        score += 10

    # Close location
    if cl >= 0.75:
        score += 10
    elif cl >= 0.60:
        score += 5

    # 24h gain
    if 0 <= gain_24h <= 12:
        score += 5
    elif gain_24h <= 20:
        score += 2

    # Liquidity
    if volume_24h >= 20_000_000:
        liquidity = "HIGH"
        score += 5
    elif volume_24h >= 10_000_000:
        liquidity = "GOOD"
        score += 4
    elif volume_24h >= 5_000_000:
        liquidity = "MEDIUM"
        score += 2
    else:
        liquidity = "LOW"

    # Real maximum = 100
    score = min(score, 100)

    if score < WATCH_SCORE:
        return None

    setup_parts = []

    if breakout >= 0:
        setup_parts.append("BREAKOUT")

    if acceleration >= 2.5:
        setup_parts.append("VOLUME ACCELERATION")

    if not setup_parts and vol_ratio_1h >= 2:
        setup_parts.append("VOLUME EXPANSION")

    if not setup_parts and gain_15m > 0:
        setup_parts.append("EARLY MOMENTUM")

    setup = " + ".join(setup_parts) if setup_parts else "EARLY MOMENTUM"

    return {
        "symbol": symbol,
        "price": price,
        "score": score,
        "gain_5m": candle_change(latest),
        "gain_15m": gain_15m,
        "gain_1h": gain_1h,
        "gain_24h": gain_24h,
        "vol_ratio_1h": vol_ratio_1h,
        "vol_ratio_15m": vol_ratio_15m,
        "acceleration": acceleration,
        "breakout": breakout,
        "liquidity": liquidity,
        "volume_24h": volume_24h,
        "close_location": cl,
        "setup": setup
    }


# ============================================================
# SCAN
# ============================================================

def run_scan():

    print("=" * 70)
    print("Binance Early Pump Scanner V4")
    print("UTC:", now_iso())
    print("=" * 70)

    symbols = get_usdt_symbols()

    print("USDT symbols:", len(symbols))

    tickers = get_24h_tickers()

    ticker_map = {
        x["symbol"]: x
        for x in tickers
        if x.get("symbol")
    }

    candidates = []

    # Filter by 24h volume first
    symbols_to_scan = []

    for symbol in symbols:
        ticker = ticker_map.get(symbol)

        if not ticker:
            continue

        volume = safe_float(ticker.get("quoteVolume"))

        if volume >= MIN_WATCH_VOLUME:
            symbols_to_scan.append((symbol, ticker))

    print("Symbols passing liquidity pre-filter:", len(symbols_to_scan))

    def worker(item):
        symbol, ticker = item

        try:
            return analyze_symbol(symbol, ticker)
        except Exception as e:
            print(f"Error {symbol}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(worker, item)
            for item in symbols_to_scan
        ]

        for future in as_completed(futures):

            result = future.result()

            if result:
                candidates.append(result)

    candidates.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return candidates


# ============================================================
# TRACKING
# ============================================================

def create_final_track(result):

    now = now_iso()

    return {
        "symbol": result["symbol"],
        "setup": result["setup"],
        "score": result["score"],
        "entry_time": now,
        "entry_price": result["price"],
        "entry_5m": result["gain_5m"],
        "entry_15m": result["gain_15m"],
        "entry_1h": result["gain_1h"],
        "entry_24h": result["gain_24h"],
        "entry_vol_1h": result["vol_ratio_1h"],
        "entry_vol_15m": result["vol_ratio_15m"],
        "entry_acceleration": result["acceleration"],
        "entry_breakout": result["breakout"],
        "entry_liquidity": result["liquidity"],
        "entry_volume_24h": result["volume_24h"],
        "confirmations": 2,
        "checkpoints": {},
        "peak_price": result["price"],
        "lowest_price": result["price"],
        "max_gain_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "last_price": result["price"],
        "completed": False
    }


def update_tracking(state, ticker_map):

    completed_results = []

    tracks = state["final_tracks"]

    now = now_utc()

    for symbol in list(tracks.keys()):

        track = tracks[symbol]

        try:
            entry_time = datetime.fromisoformat(
                track["entry_time"]
            )
        except Exception:
            entry_time = now

        elapsed_minutes = (
            now - entry_time
        ).total_seconds() / 60

        ticker = ticker_map.get(symbol)

        if not ticker:
            continue

        current_price = safe_float(
            ticker.get("lastPrice")
        )

        if current_price <= 0:
            continue

        entry_price = safe_float(
            track.get("entry_price")
        )

        if entry_price <= 0:
            continue

        gain_pct = (
            (current_price / entry_price) - 1
        ) * 100

        drawdown_pct = gain_pct

        if current_price > safe_float(track.get("peak_price")):
            track["peak_price"] = current_price

        if current_price < safe_float(track.get("lowest_price")):
            track["lowest_price"] = current_price

        peak_price = safe_float(track["peak_price"])

        max_gain = (
            (peak_price / entry_price) - 1
        ) * 100

        lowest_price = safe_float(
            track["lowest_price"]
        )

        max_drawdown = (
            (lowest_price / entry_price) - 1
        ) * 100

        track["last_price"] = current_price
        track["max_gain_pct"] = max_gain
        track["max_drawdown_pct"] = max_drawdown

        # Capture checkpoints
        for minutes in TRACKING_MINUTES:

            key = str(minutes)

            if elapsed_minutes >= minutes and key not in track["checkpoints"]:

                track["checkpoints"][key] = {
                    "time": now_iso(),
                    "price": current_price,
                    "gain_pct": gain_pct
                }

        # Finish after 24h
        if elapsed_minutes >= 1440:

            completed_results.append({
                "symbol": symbol,
                "setup": track["setup"],
                "score": track["score"],
                "entry_time": track["entry_time"],
                "entry_price": track["entry_price"],
                "entry_5m": track["entry_5m"],
                "entry_15m": track["entry_15m"],
                "entry_1h": track["entry_1h"],
                "entry_24h": track["entry_24h"],
                "entry_liquidity": track["entry_liquidity"],
                "entry_volume_24h": track["entry_volume_24h"],
                "gain_5m": track["checkpoints"].get("5", {}).get("gain_pct"),
                "gain_15m": track["checkpoints"].get("15", {}).get("gain_pct"),
                "gain_30m": track["checkpoints"].get("30", {}).get("gain_pct"),
                "gain_1h": track["checkpoints"].get("60", {}).get("gain_pct"),
                "gain_4h": track["checkpoints"].get("240", {}).get("gain_pct"),
                "gain_24h": track["checkpoints"].get("1440", {}).get("gain_pct"),
                "peak_gain_pct": track["max_gain_pct"],
                "max_drawdown_pct": track["max_drawdown_pct"]
            })

            del tracks[symbol]

    return completed_results


# ============================================================
# CSV
# ============================================================

CSV_FIELDS = [
    "symbol",
    "setup",
    "score",
    "entry_time",
    "entry_price",
    "entry_5m",
    "entry_15m",
    "entry_1h",
    "entry_24h",
    "entry_liquidity",
    "entry_volume_24h",
    "gain_5m",
    "gain_15m",
    "gain_30m",
    "gain_1h",
    "gain_4h",
    "gain_24h",
    "peak_gain_pct",
    "max_drawdown_pct"
]


def append_results(results):

    if not results:
        return

    file_exists = os.path.exists(RESULTS_FILE)

    with open(
        RESULTS_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS
        )

        if not file_exists:
            writer.writeheader()

        for row in results:
            writer.writerow(row)


# ============================================================
# FINAL LOGIC
# ============================================================

def cooldown_active(state, symbol):

    value = state["cooldowns"].get(symbol)

    if not value:
        return False

    try:
        last_final = datetime.fromisoformat(value)
    except Exception:
        return False

    elapsed_hours = (
        now_utc() - last_final
    ).total_seconds() / 3600

    return elapsed_hours < FINAL_COOLDOWN_HOURS


def send_final(result):

    message = f"""
🔥 FINAL EARLY-PUMP CANDIDATE

⚠️ هذه إشارة تحليلية وليست ضمانًا أو أمر شراء.

COIN: {result['symbol']}
SETUP: {result['setup']}
SCORE: {result['score']}/100

Price at final signal: {result['price']}

5m:  {result['gain_5m']:+.2f}%
15m: {result['gain_15m']:+.2f}%
1h:  {result['gain_1h']:+.2f}%
24h: {result['gain_24h']:+.2f}%

Volume 1h: {result['vol_ratio_1h']:.2f}x
Volume 15m: {result['vol_ratio_15m']:.2f}x
Acceleration: {result['acceleration']:.2f}x

Breakout: {result['breakout']:+.2f}%

Liquidity: {result['liquidity']}
24h Volume: ${result['volume_24h']:,.0f}

Confirmation scans: 2

📌 السعر + الحجم + تسارع الحجم + الاختراق اجتمعت في نفس الوقت.

📊 Tracking:
5m / 15m / 30m / 1h / 4h / 24h

⚠️ لا يوجد تداول آلي.
"""

    return telegram_send(message.strip())


def process_candidates(state, candidates):

    final_count = 0

    for result in candidates:

        symbol = result["symbol"]

        # ----------------------------------------------------
        # Final liquidity requirement
        # ----------------------------------------------------

        if result["volume_24h"] < MIN_FINAL_VOLUME:
            continue

        # ----------------------------------------------------
        # Already tracking
        # ----------------------------------------------------

        if symbol in state["final_tracks"]:
            continue

        # ----------------------------------------------------
        # Cooldown
        # ----------------------------------------------------

        if cooldown_active(state, symbol):
            continue

        score = result["score"]

        # ----------------------------------------------------
        # Exceptional score
        # ----------------------------------------------------

        if score >= EXCEPTIONAL_SCORE:

            print(
                f"FINAL exceptional: {symbol} "
                f"score={score}"
            )

            if send_final(result):

                state["final_tracks"][symbol] = \
                    create_final_track(result)

                state["cooldowns"][symbol] = now_iso()

                state["pending"].pop(symbol, None)

                final_count += 1

            continue

        # ----------------------------------------------------
        # Normal confirmation
        # ----------------------------------------------------

        if score < FINAL_SCORE:
            continue

        pending = state["pending"].get(symbol)

        now = now_utc()

        if not pending:

            state["pending"][symbol] = {
                "first_seen": now_iso(),
                "last_seen": now_iso(),
                "confirmations": 1,
                "last_score": score
            }

            print(
                f"Pending confirmation: {symbol} "
                f"1/{CONFIRMATIONS_REQUIRED}"
            )

            continue

        try:
            first_seen = datetime.fromisoformat(
                pending["first_seen"]
            )
        except Exception:
            first_seen = now

        elapsed = (
            now - first_seen
        ).total_seconds() / 60

        if elapsed > CANDIDATE_EXPIRY_MINUTES:

            state["pending"].pop(symbol, None)

            state["pending"][symbol] = {
                "first_seen": now_iso(),
                "last_seen": now_iso(),
                "confirmations": 1,
                "last_score": score
            }

            print(
                f"Confirmation window expired: {symbol}"
            )

            continue

        pending["confirmations"] += 1
        pending["last_seen"] = now_iso()
        pending["last_score"] = score

        confirmations = pending["confirmations"]

        print(
            f"Confirmation: {symbol} "
            f"{confirmations}/{CONFIRMATIONS_REQUIRED}"
        )

        if confirmations >= CONFIRMATIONS_REQUIRED:

            print(
                f"FINAL confirmed: {symbol} "
                f"score={score}"
            )

            if send_final(result):

                track = create_final_track(result)

                track["confirmations"] = confirmations

                state["final_tracks"][symbol] = track
                state["cooldowns"][symbol] = now_iso()

                state["pending"].pop(symbol, None)

                final_count += 1

    return final_count


# ============================================================
# MAIN
# ============================================================

def main():

    state = load_state()

    print(
        "Active final tracks:",
        len(state["final_tracks"])
    )

    print(
        "Pending confirmations:",
        len(state["pending"])
    )

    # --------------------------------------------------------
    # Scan
    # --------------------------------------------------------

    candidates = run_scan()

    print()
    print("Watch candidates:", len(candidates))
    print()

    for r in candidates[:10]:

        print(
            f"{r['symbol']:14s} "
            f"Score={r['score']:3d} "
            f"15m={r['gain_15m']:+6.2f}% "
            f"Vol1h={r['vol_ratio_1h']:5.2f}x "
            f"Vol15m={r['vol_ratio_15m']:5.2f}x "
            f"Accel={r['acceleration']:5.2f}x "
            f"Break={r['breakout']:+6.2f}% "
            f"Liquidity={r['liquidity']}"
        )

    # --------------------------------------------------------
    # Get fresh ticker prices for tracking
    # --------------------------------------------------------

    tickers = get_24h_tickers()

    ticker_map = {
        x["symbol"]: x
        for x in tickers
        if x.get("symbol")
    }

    # --------------------------------------------------------
    # Update existing tracks
    # --------------------------------------------------------

    completed = update_tracking(
        state,
        ticker_map
    )

    if completed:

        append_results(completed)

        print()
        print(
            "Completed 24h tracks:",
            len(completed)
        )

        for result in completed:

            print(
                result["symbol"],
                "24h=",
                result["gain_24h"],
                "Peak=",
                result["peak_gain_pct"]
            )

    # --------------------------------------------------------
    # Process new candidates
    # --------------------------------------------------------

    finals = process_candidates(
        state,
        candidates
    )

    # --------------------------------------------------------
    # Save state
    # --------------------------------------------------------

    save_state(state)

    print()
    print("New FINAL signals:", finals)
    print(
        "Active final tracks:",
        len(state["final_tracks"])
    )
    print(
        "Pending confirmations:",
        len(state["pending"])
    )
    print("=" * 70)


if __name__ == "__main__":
    main()
