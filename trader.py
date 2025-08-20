import os
import time
import signal
import logging
import duckdb
from binance.client import Client
from datetime import datetime
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────
load_dotenv()

# Variables de entorno y defaults
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")
USE_BINANCE = os.getenv("USE_BINANCE", "false").lower() == "true"
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
MOMENTUM_THRESHOLD = float(os.getenv("MOMENTUM_THRESHOLD", "0.01"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.003"))  # 0.3%
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.001"))    # 0.1%
SLEEP_TIME = int(os.getenv("SLEEP_TIME", "5"))

logging.basicConfig(level=logging.INFO)

# ─────────────────────────────────────────────────────────────
# Conexión a MotherDuck
# ─────────────────────────────────────────────────────────────
def connect_to_motherduck():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("Falta el token de MotherDuck")
    return duckdb.connect(f"md:?token={token}")

conn = connect_to_motherduck()
client = Client(api_key, api_secret) if USE_BINANCE else None

# ─────────────────────────────────────────────────────────────
# Variables dinámicas
# ─────────────────────────────────────────────────────────────
has_position = False
position = None
dynamic_entry_price = None
dynamic_take_profit = None
dynamic_stop_loss = None

# ─────────────────────────────────────────────────────────────
# Funciones de datos
# ─────────────────────────────────────────────────────────────
def get_latest_price():
    result = conn.execute("""
        SELECT (bid_price + ask_price) / 2
        FROM htf.depth_updates
        WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
        ORDER BY E DESC LIMIT 1
    """).fetchone()
    return result[0] if result else None

def get_latest_vwap():
    rows = conn.execute("""
        SELECT (bid_price + ask_price) / 2 AS price,
               (bid_quantity + ask_quantity) AS volume
        FROM htf.depth_updates
        WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
        ORDER BY E DESC LIMIT 60
    """).fetchall()
    if not rows: return None
    rows = rows[::-1]
    pv = sum(p * v for p, v in rows if p and v)
    vol = sum(v for p, v in rows if p and v)
    return pv / vol if vol > 0 else None

def get_latest_momentum():
    rows = conn.execute("""
        SELECT (bid_price + ask_price) / 2
        FROM htf.depth_updates
        WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
        ORDER BY E DESC LIMIT 60
    """).fetchall()
    if len(rows) < 11: return 0
    rows = rows[::-1]
    prev, curr = rows[-11][0], rows[-1][0]
    return ((curr - prev) / prev) * 100 if prev != 0 else 0

# ─────────────────────────────────────────────────────────────
# Registro de operaciones
# ─────────────────────────────────────────────────────────────
def log_trade(action, price):
    ts = int(time.time() * 1000)
    conn.execute("INSERT INTO htf.trading_log (timestamp, action, price) VALUES (?, ?, ?)",
                 [ts, action, price])
    logging.info(f"Trade loggeado: {action} @ {price:.2f}")

def update_position_state():
    ts = int(time.time() * 1000)
    conn.execute("""
        INSERT INTO htf.position_state (timestamp, has_position, entry_price, take_profit, stop_loss)
        VALUES (?, ?, ?, ?, ?)
    """, [ts, has_position, dynamic_entry_price, dynamic_take_profit, dynamic_stop_loss])
    conn.commit()

def save_trade_summary(exit_price, tipo_trade):
    fecha = datetime.now().strftime("%d/%m/%Y")
    capital = 10000  # inversión fija para el PnL

    # Cálculo de R esperado
    r_esperado = round(((dynamic_take_profit - dynamic_entry_price) / 
                        (dynamic_entry_price - dynamic_stop_loss)) * 100) / 100

    # Cálculo de R final
    if tipo_trade == "LONG":
        r_final = round(((exit_price - dynamic_entry_price) / 
                         (dynamic_entry_price - dynamic_stop_loss)) * 100) / 100
        pnl = round(capital * (exit_price - dynamic_entry_price) / dynamic_entry_price, 2)
    else:  # SHORT
        r_final = round(((dynamic_entry_price - exit_price) / 
                         (dynamic_stop_loss - dynamic_entry_price)) * 100) / 100
        pnl = round(capital * (dynamic_entry_price - exit_price) / dynamic_entry_price, 2)

    conn.execute("""
        INSERT INTO htf.trade_summary (
            timestamp, fecha, ticker, tipo, stop_loss,
            entry_price, take_profit, r_esperado,
            exit_price, r_final, pnl
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        int(time.time() * 1000),
        fecha,
        "BTC",
        tipo_trade,
        dynamic_stop_loss,
        dynamic_entry_price,
        dynamic_take_profit,
        r_esperado,
        exit_price,
        r_final,
        pnl
    ])
    conn.commit()

# ─────────────────────────────────────────────────────────────
# Ejecución de órdenes
# ─────────────────────────────────────────────────────────────
def execute_trade(action, price):
    if USE_BINANCE:
        try:
            if action == "BUY":
                order = client.order_market_buy(symbol=SYMBOL, quantity=0.001)
            elif action == "SELL":
                order = client.order_market_sell(symbol=SYMBOL, quantity=0.001)
            logging.info(f"Orden ejecutada: {order}")
        except Exception as e:
            logging.error(f"Error al ejecutar orden: {e}")
    else:
        logging.info(f"[SIMULADO] Ejecutando {action} @ {price:.2f}")
    if price: log_trade(action, price)

# ─────────────────────────────────────────────────────────────
# Evaluación de señales
# ─────────────────────────────────────────────────────────────
def evaluate_entry_signals(price, vwap, momentum):
    if price < vwap and momentum > MOMENTUM_THRESHOLD:
        return "BUY"
    elif price > vwap and momentum < -MOMENTUM_THRESHOLD:
        return "SELL"
    return None

def evaluate_exit_conditions(price):
    global has_position, position, dynamic_entry_price, dynamic_take_profit, dynamic_stop_loss

    if not has_position:
        return

    change = ((price - dynamic_entry_price) if position == "LONG"
              else (dynamic_entry_price - price)) / dynamic_entry_price

    if change >= TAKE_PROFIT_PCT:
        logging.info(f"[TP] Cerrando {position} con ganancia")
        execute_trade("TP", price)
        save_trade_summary(price, position)
    elif change <= -STOP_LOSS_PCT:
        logging.info(f"[SL] Cerrando {position} con pérdida")
        execute_trade("SL", price)
        save_trade_summary(price, position)
    else:
        return

    has_position = False
    position = None
    dynamic_entry_price = dynamic_take_profit = dynamic_stop_loss = None
    update_position_state()

# ─────────────────────────────────────────────────────────────
# Limpieza
# ─────────────────────────────────────────────────────────────
def clear_trading_log():
    try:
        logging.info("Eliminando registros de trading de la sesión actual...")
        conn.execute("DELETE FROM htf.trading_log")
        conn.execute("DELETE FROM htf.position_state")
        conn.execute("DELETE FROM htf.trade_summary")
        conn.commit()
        logging.info("Tablas limpiadas correctamente.")
    except Exception as e:
        logging.error(f"Error al eliminar registros: {e}")

def signal_handler(sig, frame):
    logging.info("Deteniendo trader.py... Limpiando tablas.")
    clear_trading_log()
    conn.close()
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ─────────────────────────────────────────────────────────────
# Bucle principal
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.info("Iniciando trader...")
    while True:
        price = get_latest_price()
        vwap = get_latest_vwap()
        momentum = get_latest_momentum()

        if price is None or vwap is None:
            time.sleep(SLEEP_TIME)
            continue

        logging.info(f"Precio={price:.2f}, VWAP={vwap:.2f}, Momentum={momentum:.2f}%")

        if has_position:
            evaluate_exit_conditions(price)
        else:
            signal = evaluate_entry_signals(price, vwap, momentum)
            if signal == "BUY":
                execute_trade("BUY", price)
                has_position = True
                position = "LONG"
                dynamic_entry_price = price
                dynamic_take_profit = price * (1 + TAKE_PROFIT_PCT)
                dynamic_stop_loss = price * (1 - STOP_LOSS_PCT)
                update_position_state()
            elif signal == "SELL":
                execute_trade("SELL", price)
                has_position = True
                position = "SHORT"
                dynamic_entry_price = price
                dynamic_take_profit = price * (1 - TAKE_PROFIT_PCT)
                dynamic_stop_loss = price * (1 + STOP_LOSS_PCT)
                update_position_state()

        time.sleep(SLEEP_TIME)