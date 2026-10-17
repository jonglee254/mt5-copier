"""
MT5 trade copier client. Run ONE process per MT5 terminal (Windows only).

    python copier_client.py --config config.master.json
    python copier_client.py --config config.slave1.json
    python copier_client.py --config config.slave2.json

role "master": reads open positions from the primary account and pushes them to the hub.
role "slave" : pulls the master's positions from the hub and mirrors them on its own account,
               under the risk rules in the "risk" block of its config.
"""
import argparse
import datetime as dt
import json
import logging
import os
import sys
import time

import MetaTrader5 as mt5
import requests

log = logging.getLogger("copier")


# ───────────────────────── helpers ─────────────────────────
def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def connect(cfg):
    m = cfg["mt5"]
    mt5.shutdown()
    ok = mt5.initialize(
        path=m["path"],
        login=int(m["login"]),
        password=m["password"],
        server=m["server"],
        timeout=30000,
    )
    if not ok:
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    ai = mt5.account_info()
    log.info("Connected: login=%s server=%s balance=%.2f equity=%.2f",
             ai.login, ai.server, ai.balance, ai.equity)


def ensure_connected(cfg):
    ti = mt5.terminal_info()
    ai = mt5.account_info()
    if ti is None or ai is None or not ti.connected or ai.login != int(cfg["mt5"]["login"]):
        connect(cfg)
        ti = mt5.terminal_info()
    if ti is not None and not ti.trade_allowed:
        log.warning("Algo Trading is disabled in the terminal — enable the 'Algo Trading' button.")


def filling_mode(symbol):
    info = mt5.symbol_info(symbol)
    fm = info.filling_mode if info else 0
    if fm & 2:
        return mt5.ORDER_FILLING_IOC
    if fm & 1:
        return mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_RETURN


def decimals_of(step):
    s = ("%.10f" % step).rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0


# ───────────────────────── master ─────────────────────────
def run_master(cfg):
    hub = cfg["hub_url"].rstrip("/")
    hdr = {"X-Token": cfg["token"]}
    interval = float(cfg.get("poll_interval", 0.5))
    sess = requests.Session()
    log.info("MASTER started → %s (every %.2fs)", hub, interval)

    while True:
        t0 = time.time()
        try:
            ensure_connected(cfg)
            positions = mt5.positions_get()
            if positions is None:
                raise RuntimeError(f"positions_get failed: {mt5.last_error()}")
            ai = mt5.account_info()
            out = []
            for p in positions:
                tick = mt5.symbol_info_tick(p.symbol)
                age = max(0, tick.time - p.time) if tick else 0  # both broker-server time
                out.append({
                    "ticket": p.ticket, "symbol": p.symbol, "type": int(p.type),
                    "volume": p.volume, "price_open": p.price_open,
                    "sl": p.sl, "tp": p.tp, "age_sec": age,
                })
            r = sess.post(hub + "/push", headers=hdr, timeout=5,
                          json={"balance": ai.balance, "equity": ai.equity, "positions": out})
            r.raise_for_status()
        except Exception as e:
            log.error("master loop: %s", e)
            time.sleep(2)
        time.sleep(max(0.0, interval - (time.time() - t0)))


# ───────────────────────── slave: order ops ─────────────────────────
def close_position(p, deviation, magic):
    tick = mt5.symbol_info_tick(p.symbol)
    if tick is None:
        return False
    if p.type == mt5.POSITION_TYPE_BUY:
        otype, price = mt5.ORDER_TYPE_SELL, tick.bid
    else:
        otype, price = mt5.ORDER_TYPE_BUY, tick.ask
    res = mt5.order_send({
        "action": mt5.TRADE_ACTION_DEAL, "symbol": p.symbol, "volume": p.volume,
        "type": otype, "position": p.ticket, "price": price, "deviation": deviation,
        "magic": magic, "comment": "CP close", "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": filling_mode(p.symbol),
    })
    ok = res is not None and res.retcode == mt5.TRADE_RETCODE_DONE
    log.info("CLOSE %s %s #%s → %s", p.symbol, p.volume, p.ticket,
             "ok" if ok else (res.retcode if res else mt5.last_error()))
    return ok


def modify_sltp(p, sl, tp):
    res = mt5.order_send({
        "action": mt5.TRADE_ACTION_SLTP, "symbol": p.symbol,
        "position": p.ticket, "sl": sl, "tp": tp,
    })
    ok = res is not None and res.retcode == mt5.TRADE_RETCODE_DONE
    log.info("MODIFY #%s sl=%s tp=%s → %s", p.ticket, sl, tp,
             "ok" if ok else (res.retcode if res else mt5.last_error()))
    return ok


def open_position(sym, mtype, volume, sl, tp, master_ticket, deviation, magic):
    tick = mt5.symbol_info_tick(sym)
    if tick is None:
        return False
    if mtype == 0:
        otype, price = mt5.ORDER_TYPE_BUY, tick.ask
    else:
        otype, price = mt5.ORDER_TYPE_SELL, tick.bid
    res = mt5.order_send({
        "action": mt5.TRADE_ACTION_DEAL, "symbol": sym, "volume": volume, "type": otype,
        "price": price, "sl": sl, "tp": tp, "deviation": deviation, "magic": magic,
        "comment": f"CP{master_ticket}", "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": filling_mode(sym),
    })
    ok = res is not None and res.retcode == mt5.TRADE_RETCODE_DONE
    log.info("OPEN %s %s %s sl=%s tp=%s (master #%s) → %s", "BUY" if mtype == 0 else "SELL",
             volume, sym, sl, tp, master_ticket, "ok" if ok else (res.retcode if res else mt5.last_error()))
    return ok


# ───────────────────────── slave: risk logic ─────────────────────────
def map_symbol(risk, sym):
    m = risk.get("symbol_map", {})
    return m[sym] if sym in m else sym + risk.get("symbol_suffix", "")


def calc_volume(risk, master_vol, master_balance, slave_balance, info):
    mode = risk.get("lot_mode", "multiplier")
    if mode == "fixed":
        v = float(risk["fixed_lot"])
    elif mode == "balance_ratio":
        v = master_vol * (slave_balance / master_balance) if master_balance > 0 else 0.0
    else:
        v = master_vol * float(risk.get("multiplier", 1.0))

    v = min(v, float(risk.get("max_lot_per_trade", 1e9)), info.volume_max)
    step = info.volume_step
    v = int(v / step + 1e-9) * step
    if v < info.volume_min:
        if risk.get("below_min_action", "skip") == "min":
            v = info.volume_min
        else:
            return 0.0
    return round(v, decimals_of(step))


def run_slave(cfg):
    risk = cfg["risk"]
    magic = int(risk.get("magic", 20260101))
    dev = int(risk.get("deviation_points", 20))
    hub = cfg["hub_url"].rstrip("/")
    hdr = {"X-Token": cfg["token"]}
    interval = float(cfg.get("poll_interval", 0.5))
    max_stale = float(risk.get("max_stale_sec", 5))
    max_entry_age = float(risk.get("max_entry_age_sec", 10))
    sfile = f"state_{cfg['mt5']['login']}.json"
    state = load_json(sfile, {"handled": [], "day": None, "day_start": None, "halt": None})
    handled = set(state.get("handled", []))
    attempts = {}
    sess = requests.Session()
    last_warn = 0.0
    log.info("SLAVE %s started → %s", cfg["mt5"]["login"], hub)

    while True:
        t0 = time.time()
        dirty = False
        try:
            ensure_connected(cfg)
            ai = mt5.account_info()
            mine = [p for p in (mt5.positions_get() or []) if p.magic == magic]

            # ── day rollover (set offset so it matches your firm's daily reset) ──
            now_shift = dt.datetime.utcnow() + dt.timedelta(hours=float(risk.get("day_rollover_utc_offset_hours", 0)))
            day = now_shift.strftime("%Y-%m-%d")
            if state.get("day") != day:
                state["day"] = day
                state["day_start"] = max(ai.balance, ai.equity)
                if state.get("halt") == "day":
                    state["halt"] = None
                    log.info("New day — daily halt cleared. Day-start reference: %.2f", state["day_start"])
                dirty = True

            # ── hard stops ──
            if not state.get("halt"):
                loss_pct = (state["day_start"] - ai.equity) / state["day_start"] * 100 if state["day_start"] else 0
                if loss_pct >= float(risk.get("daily_loss_limit_pct", 100)):
                    state["halt"] = "day"
                    log.error("DAILY LOSS LIMIT hit (%.2f%%) — closing all and halting for the day.", loss_pct)
                    dirty = True
                elif ai.equity <= float(risk.get("equity_floor", 0)):
                    state["halt"] = "permanent"
                    log.error("EQUITY FLOOR hit (%.2f) — closing all and halting until manually reset.", ai.equity)
                    dirty = True

            if state.get("halt"):
                for p in mine:
                    close_position(p, dev, magic)
                if dirty:
                    save_json(sfile, state)
                time.sleep(max(0.5, interval))
                continue

            # ── pull master snapshot ──
            r = sess.get(hub + "/state", headers=hdr, timeout=5)
            r.raise_for_status()
            snap = r.json()
            snap_age = float(snap.get("snapshot_age", 1e9))
            if snap_age > max_stale:
                if time.time() - last_warn > 30:
                    log.warning("Master snapshot stale (%.1fs) — no opens/closes until it recovers.", snap_age)
                    last_warn = time.time()
                time.sleep(interval)
                continue

            mpos = {int(p["ticket"]): p for p in snap["positions"]}
            by_master = {}
            for p in mine:
                if p.comment.startswith("CP"):
                    try:
                        by_master[int(p.comment[2:])] = p
                    except ValueError:
                        pass

            # ── closes ──
            for mt, p in list(by_master.items()):
                if mt not in mpos:
                    close_position(p, dev, magic)
                    del by_master[mt]

            # ── SL/TP sync ──
            if risk.get("copy_sl_tp", True):
                for mt, p in by_master.items():
                    m = mpos[mt]
                    pt = mt5.symbol_info(p.symbol).point
                    if abs(p.sl - m["sl"]) > pt / 2 or abs(p.tp - m["tp"]) > pt / 2:
                        modify_sltp(p, m["sl"], m["tp"])

            # ── opens ──
            allowed = risk.get("allowed_symbols", [])
            for mt, m in mpos.items():
                if mt in by_master or mt in handled:
                    continue

                def skip(reason):
                    log.warning("SKIP master #%s %s: %s", mt, m["symbol"], reason)
                    handled.add(mt)

                if allowed and m["symbol"] not in allowed:
                    skip("symbol not in allowed_symbols"); continue
                if m["age_sec"] + snap_age > max_entry_age:
                    skip(f"entry too old ({m['age_sec'] + snap_age:.0f}s)"); continue
                if risk.get("require_sl", False) and not m["sl"]:
                    skip("master trade has no SL"); continue
                if len(mine) >= int(risk.get("max_open_positions", 999)):
                    skip("max_open_positions reached"); continue

                sym = map_symbol(risk, m["symbol"])
                if not mt5.symbol_select(sym, True):
                    skip(f"symbol {sym} not available"); continue
                info = mt5.symbol_info(sym)
                tick = mt5.symbol_info_tick(sym)
                if info is None or tick is None:
                    continue  # transient, retry next loop

                spread_pts = (tick.ask - tick.bid) / info.point
                if spread_pts > float(risk.get("max_spread_points", 1e9)):
                    continue  # transient, retry until entry age expires

                max_slip = float(risk.get("max_price_deviation_points", 0))
                if max_slip > 0:
                    slip = ((tick.ask - m["price_open"]) if m["type"] == 0 else (m["price_open"] - tick.bid)) / info.point
                    if slip > max_slip:
                        skip(f"price moved {slip:.0f} pts against entry"); continue

                vol = calc_volume(risk, float(m["volume"]), float(snap.get("balance", 0)), ai.balance, info)
                if vol <= 0:
                    skip("calculated volume below broker minimum"); continue
                if sum(p.volume for p in mine) + vol > float(risk.get("max_total_lots", 1e9)):
                    skip("max_total_lots reached"); continue

                if open_position(sym, int(m["type"]), vol, m["sl"], m["tp"], mt, dev, magic):
                    handled.add(mt)
                    mine = [p for p in (mt5.positions_get() or []) if p.magic == magic]
                else:
                    attempts[mt] = attempts.get(mt, 0) + 1
                    if attempts[mt] >= int(risk.get("max_open_retries", 3)):
                        skip("open failed repeatedly")

            # ── housekeeping ──
            new_handled = sorted(t for t in handled if t in mpos)
            if new_handled != sorted(state.get("handled", [])):
                state["handled"] = new_handled
                dirty = True
            handled = set(new_handled)
            if dirty:
                save_json(sfile, state)

        except Exception as e:
            log.error("slave loop: %s", e)
            time.sleep(2)
        time.sleep(max(0.0, interval - (time.time() - t0)))


# ───────────────────────── entry ─────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = load_json(args.config)
    if cfg is None:
        sys.exit(f"Config not found: {args.config}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"copier_{cfg['role']}_{cfg['mt5']['login']}.log", encoding="utf-8"),
        ],
    )
    try:
        if cfg["role"] == "master":
            run_master(cfg)
        elif cfg["role"] == "slave":
            run_slave(cfg)
        else:
            sys.exit("role must be 'master' or 'slave'")
    except KeyboardInterrupt:
        pass
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
