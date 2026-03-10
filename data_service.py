"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Zerodha (placeholder)

KEY FIX v6:
  Upstox V3 API URL format is DIFFERENT from V2:

  V2 (wrong): /v2/historical-candle/{key}/day/{to_date}/{from_date}
  V3 (correct): /v3/historical-candle/{key}/{unit}/{interval}/{to_date}/{from_date}

  For daily candles:
    unit     = "days"
    interval = 1

  Example correct URL:
    /v3/historical-candle/NSE_EQ%7CINE002A01018/days/1/2026-03-10/2000-01-01
"""

import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime

from upstox_auth import get_upstox_access_token


# ─────────────────────────────────────────────────────────────
# SECTION A — INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_INSTRUMENT_MAP = None


def _load_instrument_map() -> dict:
    global _INSTRUMENT_MAP

    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP
    if "upstox_instrument_map" in st.session_state:
        _INSTRUMENT_MAP = st.session_state["upstox_instrument_map"]
        return _INSTRUMENT_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        st.sidebar.info("Downloading Upstox instrument master...")
        df   = pd.read_csv(url, compression="gzip", low_memory=False)
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()
        mapping = dict(zip(df["tradingsymbol"].astype(str).str.upper(), df["instrument_key"]))
        _INSTRUMENT_MAP = mapping
        st.session_state["upstox_instrument_map"] = mapping
        st.sidebar.success(f"Instrument master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Instrument master load failed: {e}")
        return {}


def _get_instrument_key(symbol_ns: str, instrument_map: dict):
    clean = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)


# ─────────────────────────────────────────────────────────────
# SECTION B — TOKEN VALIDATION
# ─────────────────────────────────────────────────────────────
def _validate_token(access_token: str) -> bool:
    """
    Validate token with 1 real API call using CORRECT V3 URL format.
    V3 daily format: /v3/historical-candle/{key}/days/1/{to}/{from}
    """
    url = (
        "https://api.upstox.com/v3/historical-candle"
        "/NSE_EQ%7CINE002A01018/days/1/2024-01-05/2024-01-01"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code not in (401, 403)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
# SECTION C — SINGLE SYMBOL FETCHER (V3 — CORRECT URL FORMAT)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(
    instrument_key: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime,
    retries: int = 2,
):
    """
    Fetch daily OHLCV for one symbol via Upstox V3 Historical Candle API.

    CORRECT V3 URL format:
      GET /v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}

      unit     = "days"   (for daily candles)
      interval = 1        (1 day per candle)

    Candle array: [timestamp, open, high, low, close, volume, oi]
    """
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    # ✅ CORRECT V3 URL — unit="days", interval=1
    url = (
        f"https://api.upstox.com/v3/historical-candle"
        f"/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)

            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue

            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")

            resp.raise_for_status()
            payload = resp.json()
            candles = payload.get("data", {}).get("candles", [])

            if not candles:
                return None

            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "oi"
            ])

            # Timezone fix: Upstox returns "+05:30" timestamps
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_convert(None)

            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay); delay *= 2
        except Exception as e:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2

    return None


# ─────────────────────────────────────────────────────────────
# SECTION D — DEBUG: Show first symbol response
# ─────────────────────────────────────────────────────────────
def _debug_first_symbol(instrument_key, access_token, start_date, end_date):
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    # Use correct V3 URL format
    url = (
        f"https://api.upstox.com/v3/historical-candle"
        f"/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        with st.sidebar.expander("Debug: First symbol API response", expanded=True):
            st.write(f"**URL:** `{url}`")
            st.write(f"**HTTP Status:** {resp.status_code}")
            try:
                payload = resp.json()
                candles = payload.get("data", {}).get("candles", [])
                st.write(f"**Candles returned:** {len(candles)}")
                if candles:
                    st.write("**First candle:**", candles[0])
                    st.write("**Last candle:**", candles[-1])
                    st.success(f"Data OK! {len(candles)} candles received.")
                else:
                    st.write("**Full response:**", payload)
            except Exception:
                st.write("**Raw text:**", resp.text[:500])
    except Exception as e:
        st.sidebar.error(f"Debug call failed: {e}")


# ─────────────────────────────────────────────────────────────
# SECTION E — ZERODHA PLACEHOLDER
# ─────────────────────────────────────────────────────────────
def _init_zerodha_client():
    return None

def _fetch_zerodha_history(client, symbol, start_date, end_date):
    if client is None:
        return _generate_mock_ohlcv(symbol, start_date, end_date)

def _generate_mock_ohlcv(symbol, start_date, end_date):
    rng  = pd.date_range(start=start_date, end=end_date, freq='B')
    n    = len(rng)
    seed = abs(hash(symbol)) % (2**31)
    rs   = np.random.RandomState(seed)
    close  = 100 * np.cumprod(1 + rs.normal(0.0003, 0.015, n))
    return pd.DataFrame({
        'open':   close * (1 + rs.normal(0, 0.008, n)),
        'high':   close * (1 + rs.uniform(0, 0.03, n)),
        'low':    close * (1 - rs.uniform(0, 0.03, n)),
        'close':  close,
        'volume': rs.randint(50_000, 5_000_000, n).astype(float),
    }, index=rng)


# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER (unchanged)
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols, start=start_date, progress=False,
                auto_adjust=True, threads=True, multi_level_index=False
            )
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay); delay *= 2
            else:
                raise e


def fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text):
    close_chunks, high_chunks, volume_chunks, failed_symbols = [], [], [], []
    total = len(symbols)
    for k in range(0, total, chunk_size):
        progress = min((k + chunk_size) / total, 1.0)
        chunk    = symbols[k:k + chunk_size]
        for attempt in range(3):
            try:
                raw = _download_yfinance_chunk(chunk, start_date)
                close_chunks.append(raw['Close'])
                high_chunks.append(raw['High'])
                volume_chunks.append(raw['Close'] * raw['Volume'])
                break
            except Exception:
                if attempt == 2:
                    failed_symbols.extend(chunk)
        progress_bar.progress(progress)
        status_text.text(f"YFinance: {int(progress*100)}%")
        time.sleep(1.5)

    progress_bar.progress(1.0)
    status_text.text("Download complete!")
    close  = pd.concat(close_chunks,  axis=1) if close_chunks  else pd.DataFrame()
    high   = pd.concat(high_chunks,   axis=1) if high_chunks   else pd.DataFrame()
    volume = pd.concat(volume_chunks, axis=1) if volume_chunks else pd.DataFrame()
    for df in (close, high, volume):
        df.index = pd.to_datetime(df.index)
    return close, high, volume, failed_symbols


# ─────────────────────────────────────────────────────────────
# SECTION G — WIDE FRAME BUILDER
# ─────────────────────────────────────────────────────────────
def _build_wide_frames(symbol_dfs: dict) -> tuple:
    close_map, high_map, vol_map = {}, {}, {}
    for sym, df in symbol_dfs.items():
        if df is None or df.empty:
            continue
        df.index = pd.to_datetime(df.index)
        close_map[sym] = df['close']
        high_map[sym]  = df['high']
        vol_map[sym]   = df['close'] * df['volume']
    return pd.DataFrame(close_map), pd.DataFrame(high_map), pd.DataFrame(vol_map)


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text):

    # STEP 1: Get token
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        progress_bar.progress(0.0)
        st.error("Please complete Upstox login in the sidebar first, then retry.")
        st.stop()

    # STEP 2: Validate token
    status_text.text("Validating Upstox token...")
    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Please re-login from sidebar and retry.")
        st.stop()
    st.sidebar.success("Token validated OK")

    # STEP 3: Instrument master
    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    # STEP 4: Debug first symbol
    first_sym = symbols[0] if symbols else None
    if first_sym:
        first_key = _get_instrument_key(first_sym, instrument_map)
        if first_key:
            _debug_first_symbol(first_key, access_token, start_date, end_date)

    # STEP 5: Bulk fetch
    symbol_dfs = {}
    failed     = []
    not_found  = 0
    total      = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total

        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
            symbol_dfs[sym] = None
        else:
            try:
                df = _fetch_upstox_history_live(
                    instrument_key, access_token, start_date, end_date
                )
                symbol_dfs[sym] = df
                if df is None or df.empty:
                    failed.append(sym)

            except ValueError:
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                st.stop()

            except Exception:
                symbol_dfs[sym] = None
                failed.append(sym)

        # Update UI every 10 symbols
        if i % 10 == 0 or i == total - 1:
            fetched_so_far = (i + 1) - len(failed)
            progress_bar.progress(progress)
            status_text.text(
                f"Upstox: {int(progress*100)}%  |  "
                f"Fetched: {fetched_so_far}  |  "
                f"Failed: {len(failed) - not_found}  |  "
                f"Not in master: {not_found}"
            )

        time.sleep(0.05)

    progress_bar.progress(1.0)
    fetched = total - len(failed)
    status_text.text(f"Done - {fetched}/{total} fetched | Not in master: {not_found}")

    close, high, volume = _build_wide_frames(symbol_dfs)

    if close.empty:
        st.error(
            "No data fetched from Upstox.\n\n"
            "Check sidebar Debug section for API response details.\n"
            "Try re-logging in and retry."
        )
        st.stop()

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION I — ZERODHA BULK FETCHER (Phase-1 mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    client, symbol_dfs = _init_zerodha_client(), {}
    total = len(symbols)
    for i, sym in enumerate(symbols):
        try:
            symbol_dfs[sym] = _fetch_zerodha_history(client, sym.replace('.NS',''), start_date, end_date)
        except Exception:
            symbol_dfs[sym] = None
        if i % 10 == 0:
            progress_bar.progress((i+1)/total)
            status_text.text(f"Zerodha: {int((i+1)/total*100)}%")
        time.sleep(0.05)
    progress_bar.progress(1.0)
    status_text.text("Zerodha (MOCK) complete!")
    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION J — UNIFIED ENTRY POINT
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source, symbols, start_date, end_date,
               chunk_size, progress_bar, status_text) -> tuple:
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")
