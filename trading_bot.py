"""
==============================================================
  ROBOT DE TRADING - PROPR.XYZ  (version corrigée complète)
  Stratégie : Momentum intra-bougie 5 minutes
==============================================================

LOGIQUE :
  - Prix en temps réel via Hyperliquid WebSocket (sur lequel Propr est bâti)
  - Dès que la bougie 5min EN COURS atteint ±CANDLE_MOVE_PCT → trade immédiat
  - Un seul trade par bougie
  - SL et TP placés comme ordres séparés (stop_market + take_profit_market)

INSTALLATION :
  pip install aiohttp websockets python-ulid

USAGE :
  python3 trading_bot.py
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
import aiohttp
import websockets
from ulid import ULID

# ──────────────────────────────────────────────────────────────
#  PARAMÈTRES — les secrets viennent des variables d'environnement
# ──────────────────────────────────────────────────────────────

CONFIG = {
    # ── Authentification Propr ────────────────────────────────
    "API_KEY":    os.environ["PROPR_API_KEY"],       # ← défini dans Render > Environment
    "ACCOUNT_ID": os.environ["PROPR_ACCOUNT_ID"],    # ← défini dans Render > Environment

    # ── Actif à trader ────────────────────────────────────────
    # Sur Hyperliquid/Propr : asset = "BTC", "ETH", "SOL"...
    "ASSET":      "BTC",      # Ticker de l'actif
    "QUOTE":      "USDC",     # Toujours USDC sur Propr/Hyperliquid

    # ── Stratégie ─────────────────────────────────────────────
    "CANDLE_MOVE_PCT":  0.30, # % de variation intra-bougie pour déclencher
    "TIMEFRAME_MIN":       5, # Durée de la bougie en minutes

    # ── Risk Management ───────────────────────────────────────
    "STOP_LOSS_PCT":    0.30, # Stop-loss en % depuis l'entrée
    "TAKE_PROFIT_PCT":  0.30, # Take-profit en % depuis l'entrée
    "ORDER_QTY":        0.01, # Quantité par ordre (en unité de l'actif, ex: 0.01 BTC)
    "LEVERAGE":            5, # Levier (1 = pas de levier)

    # ── Sécurité ──────────────────────────────────────────────
    "MAX_OPEN_POSITIONS":    1,  # Positions ouvertes max simultanément
    "MAX_DAILY_TRADES":     20,  # Trades max par jour
    "TRADE_COOLDOWN_SEC":   30,  # Pause min entre deux trades (sec)

    # ── URLs ──────────────────────────────────────────────────
    "PROPR_BASE_URL":  "https://api.propr.xyz/v1",
    "HL_WS_URL":       "wss://api.hyperliquid.xyz/ws",  # Prix temps réel
    "RETRY_DELAY":     5,   # Secondes avant reconnexion WS

    # ── Logs ──────────────────────────────────────────────────
    # Sur Render, on log uniquement sur stdout (pas de fichier — disque éphémère)
    "LOG_LEVEL":  os.environ.get("LOG_LEVEL", "INFO"),
}

# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────
def setup_logger(level: str):
    logger = logging.getLogger("TradingBot")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    # Render capture uniquement stdout — pas de fichier log (disque éphémère)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

log = setup_logger(CONFIG["LOG_LEVEL"])

# ──────────────────────────────────────────────────────────────
#  LIVE CANDLE TRACKER
# ──────────────────────────────────────────────────────────────
class LiveCandleTracker:
    """Suit la bougie active et déclenche dès que le seuil est atteint."""

    def __init__(self, timeframe_minutes: int, threshold_pct: float):
        self.tf_sec    = timeframe_minutes * 60
        self.threshold = threshold_pct
        self._period_start: Optional[int]   = None
        self._candle_open:  Optional[float] = None
        self._candle_high:  Optional[float] = None
        self._candle_low:   Optional[float] = None
        self.traded_this_candle: bool = False

    def _period_of(self, ts: int) -> int:
        return (ts // self.tf_sec) * self.tf_sec

    def update(self, price: float, ts: int) -> Optional[str]:
        period = self._period_of(ts)

        if period != self._period_start:
            if self._period_start is not None:
                log.info(
                    f"🕯  Bougie [{datetime.fromtimestamp(self._period_start).strftime('%H:%M')}] "
                    f"fermée — O={self._candle_open:.2f}  "
                    f"H={self._candle_high:.2f}  L={self._candle_low:.2f}"
                )
            self._period_start      = period
            self._candle_open       = price
            self._candle_high       = price
            self._candle_low        = price
            self.traded_this_candle = False
            secs_left = self.tf_sec - (ts - period)
            log.info(
                f"🕯  Nouvelle bougie [{datetime.fromtimestamp(period).strftime('%H:%M')}] "
                f"open={price:.2f}  (clôture dans {secs_left}s)"
            )
            return None

        self._candle_high = max(self._candle_high, price)
        self._candle_low  = min(self._candle_low,  price)

        if self.traded_this_candle or self._candle_open == 0:
            return None

        pct = (price - self._candle_open) / self._candle_open * 100
        log.debug(f"  Prix={price:.2f}  open={self._candle_open:.2f}  var={pct:+.3f}%")

        if pct >= self.threshold:
            log.info(f"📈 SEUIL ATTEINT +{pct:.3f}% → BUY  (open={self._candle_open:.2f} → {price:.2f})")
            self.traded_this_candle = True
            return "buy"

        if pct <= -self.threshold:
            log.info(f"📉 SEUIL ATTEINT {pct:.3f}% → SELL (open={self._candle_open:.2f} → {price:.2f})")
            self.traded_this_candle = True
            return "sell"

        return None

# ──────────────────────────────────────────────────────────────
#  PROPR API CLIENT
# ──────────────────────────────────────────────────────────────
class ProprAPIClient:
    """Client HTTP pour l'API Propr.xyz — format d'ordres officiel."""

    def __init__(self, api_key: str, base_url: str, account_id: str):
        self.base_url   = base_url
        self.account_id = account_id
        self.headers    = {
            "X-API-Key":    api_key,
            "Content-Type": "application/json",
        }
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        try:
            async with session.request(method, url, **kwargs) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except Exception:
                    data = {"raw": text}
                if resp.status not in (200, 201):
                    log.error(f"API {resp.status} [{method} {endpoint}]: {text[:200]}")
                    return {}
                return data
        except Exception as e:
            log.error(f"Requête échouée [{method} {endpoint}]: {e}")
            return {}

    async def health_check(self) -> bool:
        resp = await self._request("GET", "/health")
        ok = bool(resp)
        log.info(f"Health check: {'✅ OK' if ok else '❌ KO'} — {resp}")
        return ok

    async def get_positions(self) -> list:
        resp = await self._request("GET", f"/accounts/{self.account_id}/positions")
        positions = resp.get("data", [])
        # Filtre les positions avec quantité zéro (positions fermées)
        return [p for p in positions if float(p.get("quantity", 0)) != 0]

    async def get_open_position_count(self) -> int:
        return len(await self.get_positions())

    async def _place_order(self, order: dict) -> Optional[dict]:
        """Envoie UN ordre via POST /accounts/{id}/orders."""
        payload = {"orders": [order]}
        log.debug(f"Payload ordre: {json.dumps(payload, indent=2)}")
        resp = await self._request(
            "POST",
            f"/accounts/{self.account_id}/orders",
            json=payload,
        )
        orders = resp.get("data", [])
        return orders[0] if orders else None

    async def place_market_order(
        self,
        side: str,           # "buy" | "sell"
        position_side: str,  # "long" | "short"
        asset: str,
        quote: str,
        quantity: float,
    ) -> Optional[dict]:
        """Ordre market (entrée en position)."""
        order = {
            "accountId":    self.account_id,
            "intentId":     str(ULID()),
            "exchange":     "hyperliquid",
            "type":         "market",
            "side":         side,
            "positionSide": position_side,
            "productType":  "perp",
            "timeInForce":  "IOC",
            "asset":        asset,
            "base":         asset,
            "quote":        quote,
            "quantity":     str(quantity),
            "reduceOnly":   False,
            "closePosition": False,
        }
        return await self._place_order(order)

    async def place_stop_loss(
        self,
        side: str,           # côté inverse de la position
        position_side: str,
        asset: str,
        quote: str,
        quantity: float,
        trigger_price: float,
    ) -> Optional[dict]:
        """Stop-loss (stop_market avec reduceOnly)."""
        order = {
            "accountId":    self.account_id,
            "intentId":     str(ULID()),
            "exchange":     "hyperliquid",
            "type":         "stop_market",
            "side":         side,
            "positionSide": position_side,
            "productType":  "perp",
            "timeInForce":  "GTC",
            "asset":        asset,
            "base":         asset,
            "quote":        quote,
            "quantity":     str(quantity),
            "triggerPrice": str(round(trigger_price, 2)),
            "reduceOnly":   True,
            "closePosition": False,
        }
        return await self._place_order(order)

    async def place_take_profit(
        self,
        side: str,
        position_side: str,
        asset: str,
        quote: str,
        quantity: float,
        trigger_price: float,
    ) -> Optional[dict]:
        """Take-profit (take_profit_market avec reduceOnly)."""
        order = {
            "accountId":    self.account_id,
            "intentId":     str(ULID()),
            "exchange":     "hyperliquid",
            "type":         "take_profit_market",
            "side":         side,
            "positionSide": position_side,
            "productType":  "perp",
            "timeInForce":  "GTC",
            "asset":        asset,
            "base":         asset,
            "quote":        quote,
            "quantity":     str(quantity),
            "triggerPrice": str(round(trigger_price, 2)),
            "reduceOnly":   True,
            "closePosition": False,
        }
        return await self._place_order(order)

# ──────────────────────────────────────────────────────────────
#  TRADING BOT
# ──────────────────────────────────────────────────────────────
class TradingBot:

    def __init__(self, config: dict):
        self.cfg     = config
        self.client  = ProprAPIClient(
            api_key    = config["API_KEY"],
            base_url   = config["PROPR_BASE_URL"],
            account_id = config["ACCOUNT_ID"],
        )
        self.tracker = LiveCandleTracker(
            timeframe_minutes = config["TIMEFRAME_MIN"],
            threshold_pct     = config["CANDLE_MOVE_PCT"],
        )
        self._daily_trades    = 0
        self._last_trade_time = 0.0
        self._today_date      = datetime.now(timezone.utc).date()

        log.info("=" * 60)
        log.info("  🤖  ROBOT DE TRADING PROPR.XYZ")
        log.info(f"  Actif         : {config['ASSET']}/{config['QUOTE']}")
        log.info(f"  Timeframe     : {config['TIMEFRAME_MIN']} min")
        log.info(f"  Seuil         : ±{config['CANDLE_MOVE_PCT']}%  (déclenchement immédiat)")
        log.info(f"  Stop-Loss     : {config['STOP_LOSS_PCT']}%")
        log.info(f"  Take-Profit   : {config['TAKE_PROFIT_PCT']}%")
        log.info(f"  Quantité      : {config['ORDER_QTY']} {config['ASSET']}")
        log.info(f"  Prix via      : Hyperliquid WebSocket")
        log.info("=" * 60)

    # ── Gardes-fous ───────────────────────────────────────────

    def _reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).date()
        if today != self._today_date:
            log.info(f"📅 Nouveau jour — compteur remis à zéro.")
            self._daily_trades = 0
            self._today_date   = today

    def _can_trade(self) -> bool:
        self._reset_daily_if_needed()
        if self._daily_trades >= self.cfg["MAX_DAILY_TRADES"]:
            log.warning(f"🚫 Limite journalière atteinte ({self._daily_trades} trades).")
            return False
        elapsed = time.time() - self._last_trade_time
        if elapsed < self.cfg["TRADE_COOLDOWN_SEC"]:
            log.debug(f"⏳ Cooldown : {self.cfg['TRADE_COOLDOWN_SEC'] - elapsed:.0f}s restantes.")
            return False
        return True

    def _compute_sl_tp(self, side: str, entry: float):
        sl = self.cfg["STOP_LOSS_PCT"]   / 100
        tp = self.cfg["TAKE_PROFIT_PCT"] / 100
        if side == "buy":
            return entry * (1 - sl), entry * (1 + tp)
        else:
            return entry * (1 + sl), entry * (1 - tp)

    # ── Exécution du trade ────────────────────────────────────

    async def _execute_trade(self, side: str, price: float):
        if not self._can_trade():
            self.tracker.traded_this_candle = False
            return

        open_pos = await self.client.get_open_position_count()
        if open_pos >= self.cfg["MAX_OPEN_POSITIONS"]:
            log.warning(f"🚫 {open_pos} position(s) ouverte(s) — trade ignoré.")
            return

        asset        = self.cfg["ASSET"]
        quote        = self.cfg["QUOTE"]
        qty          = self.cfg["ORDER_QTY"]
        position_side = "long" if side == "buy" else "short"
        close_side    = "sell" if side == "buy" else "buy"
        sl_price, tp_price = self._compute_sl_tp(side, price)
        rr = self.cfg["TAKE_PROFIT_PCT"] / self.cfg["STOP_LOSS_PCT"]

        log.info("─" * 52)
        log.info(f"  🎯  {'LONG (BUY)' if side == 'buy' else 'SHORT (SELL)'}")
        log.info(f"  Entrée      : {price:.2f} {quote}")
        log.info(f"  Stop-Loss   : {sl_price:.2f}  (-{self.cfg['STOP_LOSS_PCT']}%)")
        log.info(f"  Take-Profit : {tp_price:.2f}  (+{self.cfg['TAKE_PROFIT_PCT']}%)")
        log.info(f"  RR          : 1:{rr:.1f}")
        log.info(f"  Quantité    : {qty} {asset}")
        log.info("─" * 52)

        # 1. Ordre d'entrée (market)
        entry_result = await self.client.place_market_order(
            side=side, position_side=position_side,
            asset=asset, quote=quote, quantity=qty,
        )
        if not entry_result:
            log.error("❌ Échec de l'ordre d'entrée.")
            return

        entry_id = entry_result.get("orderId", "N/A")
        log.info(f"✅ Entrée placée | orderId: {entry_id}")

        # Petite pause pour laisser l'ordre s'enregistrer
        await asyncio.sleep(0.5)

        # 2. Stop-Loss
        sl_result = await self.client.place_stop_loss(
            side=close_side, position_side=position_side,
            asset=asset, quote=quote, quantity=qty,
            trigger_price=sl_price,
        )
        if sl_result:
            log.info(f"✅ Stop-Loss placé  | orderId: {sl_result.get('orderId','N/A')}  trigger={sl_price:.2f}")
        else:
            log.warning("⚠️  Stop-Loss non placé — placez-le manuellement !")

        # 3. Take-Profit
        tp_result = await self.client.place_take_profit(
            side=close_side, position_side=position_side,
            asset=asset, quote=quote, quantity=qty,
            trigger_price=tp_price,
        )
        if tp_result:
            log.info(f"✅ Take-Profit placé | orderId: {tp_result.get('orderId','N/A')}  trigger={tp_price:.2f}")
        else:
            log.warning("⚠️  Take-Profit non placé — placez-le manuellement !")

        self._daily_trades   += 1
        self._last_trade_time = time.time()
        log.info(f"📊 Trades aujourd'hui : {self._daily_trades}/{self.cfg['MAX_DAILY_TRADES']}")

    # ── WebSocket Hyperliquid (flux de prix) ──────────────────

    async def _ws_connect(self):
        """
        Se connecte au WebSocket Hyperliquid pour recevoir les prix BTC en temps réel.
        Format d'abonnement Hyperliquid : {"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
        """
        uri = self.cfg["HL_WS_URL"]

        # Détection version websockets
        import websockets as _ws
        _ver = tuple(int(x) for x in _ws.__version__.split(".")[:2])
        _hdr_kwarg = "additional_headers" if _ver >= (10, 0) else "extra_headers"

        while True:
            try:
                log.info(f"🔌 Connexion WebSocket Hyperliquid → {uri}")
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    log.info("✅ WebSocket Hyperliquid connecté.")

                    # Abonnement au flux de trades en temps réel
                    sub_msg = json.dumps({
                        "method": "subscribe",
                        "subscription": {
                            "type":  "trades",
                            "coin":  self.cfg["ASSET"],
                        }
                    })
                    await ws.send(sub_msg)
                    log.info(f"📡 Abonné au flux trades : {self.cfg['ASSET']}")

                    async for message in ws:
                        await self._process_hl_message(message)

            except websockets.exceptions.ConnectionClosedOK:
                log.info("WebSocket fermé normalement.")
                break
            except Exception as e:
                log.error(f"Erreur WS: {e}. Reconnexion dans {self.cfg['RETRY_DELAY']}s…")

            await asyncio.sleep(self.cfg["RETRY_DELAY"])

    async def _process_hl_message(self, raw: str):
        """
        Parse les messages Hyperliquid.
        Format trade : {"channel":"trades","data":[{"coin":"BTC","px":"67430.5","ts":1234567890,...}]}
        """
        try:
            msg = json.loads(raw)
        except Exception:
            return

        channel = msg.get("channel", "")

        if channel == "trades":
            trades = msg.get("data", [])
            if not trades:
                return
            # On prend le dernier trade de la liste
            last = trades[-1]
            try:
                price = float(last.get("px", 0))
                ts    = int(last.get("ts", time.time() * 1000)) // 1000
                if price > 0:
                    signal = self.tracker.update(price, ts)
                    if signal:
                        await self._execute_trade(signal, price)
            except (TypeError, ValueError):
                pass

        elif channel == "subscriptionResponse":
            log.info(f"📡 Confirmation abonnement : {msg}")
        else:
            log.debug(f"Message WS ignoré (channel={channel})")

    # ── Démarrage ─────────────────────────────────────────────

    async def run(self):
        try:
            if not await self.client.health_check():
                log.error("❌ API Propr inaccessible. Arrêt.")
                return
            await self._ws_connect()
        except asyncio.CancelledError:
            log.info("Bot interrompu.")
        finally:
            await self.client.close()
            log.info("🛑 Bot arrêté proprement.")


# ──────────────────────────────────────────────────────────────
#  POINT D'ENTRÉE
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = TradingBot(CONFIG)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("⛔ Arrêt manuel (Ctrl+C).")
