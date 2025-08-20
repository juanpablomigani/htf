import os
import time
import duckdb
from flask import Flask, jsonify, render_template
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
app = Flask(__name__)

# Variables globales
last_soporte = None
last_resistencia = None
last_break_timestamp = None

# ─────────────────────────────────────────────────────────────
# Conexión a MotherDuck
# ─────────────────────────────────────────────────────────────
def get_motherduck_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("Falta el token de MotherDuck")
    return duckdb.connect(f"md:?token={token}")

# ─────────────────────────────────────────────────────────────
# Página principal
# ─────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

# ─────────────────────────────────────────────────────────────
# Datos (Soporte / Resistencia)
# ─────────────────────────────────────────────────────────────
@app.route('/data')
def get_order_book_data():
    global last_soporte, last_resistencia, last_break_timestamp

    conn = get_motherduck_connection()

    # ✅ Subconsulta para obtener los 200 más recientes, pero ordenados cronológicamente
    query = """
        SELECT E, bid_price, ask_price
        FROM (
            SELECT E, bid_price, ask_price
            FROM htf.depth_updates
            ORDER BY E DESC
            LIMIT 200
        ) sub
        ORDER BY E ASC;
    """

    data = conn.execute(query).fetchall()
    conn.close()

    if not data:
        return jsonify({"timestamps": [], "bids": [], "asks": [], "soporte": None, "resistencia": None})

    timestamps = [row[0] for row in data]
    bids = [row[1] for row in data]
    asks = [row[2] for row in data]

    current_soporte = min(bids)
    current_resistencia = max(asks)
    now = time.time()

    if last_soporte is None or last_resistencia is None:
        last_soporte = current_soporte
        last_resistencia = current_resistencia
        last_break_timestamp = now
    else:
        if min(bids) < last_soporte or max(asks) > last_resistencia:
            if now - last_break_timestamp > 10:
                last_soporte = current_soporte
                last_resistencia = current_resistencia
                last_break_timestamp = now
        else:
            last_break_timestamp = now

    return jsonify({
        "timestamps": timestamps,
        "bids": bids,
        "asks": asks,
        "soporte": last_soporte,
        "resistencia": last_resistencia
    })

# ─────────────────────────────────────────────────────────────
# VWAP
# ─────────────────────────────────────────────────────────────
@app.route('/vwap')
def get_vwap():
    conn = get_motherduck_connection()
    query = """
        SELECT 
            E,
            (bid_price + ask_price) / 2 AS price,
            (bid_quantity + ask_quantity) AS volume
        FROM htf.depth_updates
        WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
        ORDER BY E DESC
        LIMIT 60;
    """
    data = conn.execute(query).fetchall()
    conn.close()

    if not data:
        return jsonify({"timestamps": [], "vwap_values": []})

    data = data[::-1]
    timestamps, vwap_values = [], []
    cumulative_pv = cumulative_volume = 0

    for ts, price, volume in data:
        if volume is None or price is None:
            continue
        cumulative_pv += price * volume
        cumulative_volume += volume
        if cumulative_volume > 0:
            vwap = cumulative_pv / cumulative_volume
            timestamps.append(ts)
            vwap_values.append(vwap)

    return jsonify({"timestamps": timestamps, "vwap_values": vwap_values})

# ─────────────────────────────────────────────────────────────
# Momentum (Rate of Change)
# ─────────────────────────────────────────────────────────────
@app.route('/momentum')
def get_momentum():
    conn = get_motherduck_connection()
    query = """
        SELECT E, (ask_price + bid_price) / 2 AS mid_price
        FROM htf.depth_updates
        WHERE ask_price IS NOT NULL AND bid_price IS NOT NULL
        ORDER BY E DESC
        LIMIT 60;
    """
    data = conn.execute(query).fetchall()
    conn.close()

    if not data or len(data) < 11:
        return jsonify({"timestamps": [], "roc_values": []})

    data = data[::-1]
    timestamps = [row[0] for row in data]
    prices = [row[1] for row in data]

    roc_values = []
    roc_timestamps = []

    for i in range(10, len(prices)):
        prev = prices[i - 10]
        curr = prices[i]
        if prev != 0:
            roc = ((curr - prev) / prev) * 100
            roc_values.append(roc)
            roc_timestamps.append(timestamps[i])

    return jsonify({"timestamps": roc_timestamps, "roc_values": roc_values})

# ─────────────────────────────────────────────────────────────
# Trades recientes (BUY, SELL, TP, SL)
# ─────────────────────────────────────────────────────────────
@app.route('/trades')
def get_trades():
    conn = get_motherduck_connection()
    query = """
        SELECT timestamp, action, price
        FROM htf.trading_log
        ORDER BY timestamp DESC
        LIMIT 100
    """
    data = conn.execute(query).fetchall()
    conn.close()
    return jsonify([
        {"timestamp": row[0], "action": row[1], "price": row[2]}
        for row in data
    ])

# ─────────────────────────────────────────────────────────────
# Estado actual de la posición (para líneas horizontales)
# ─────────────────────────────────────────────────────────────
@app.route('/position')
def get_position():
    conn = get_motherduck_connection()
    query = """
        SELECT has_position, entry_price, take_profit, stop_loss
        FROM htf.position_state
        ORDER BY timestamp DESC
        LIMIT 1
    """
    result = conn.execute(query).fetchone()
    conn.close()

    if not result:
        return jsonify({
            "has_position": False,
            "entry_price": None,
            "take_profit": None,
            "stop_loss": None
        })

    return jsonify({
        "has_position": result[0],
        "entry_price": result[1],
        "take_profit": result[2],
        "stop_loss": result[3]
    })
    
# ─────────────────────────────────────────────────────────────
# Resumen de trades para mostrar en dashboard
# ─────────────────────────────────────────────────────────────
@app.route('/trade_summary')
def get_trade_summary():
    conn = get_motherduck_connection()
    query = """
        SELECT timestamp, fecha, ticker, tipo, stop_loss, entry_price, 
               take_profit, r_esperado, exit_price, r_final, pnl
        FROM htf.trade_summary
        ORDER BY timestamp DESC
        LIMIT 50
    """
    rows = conn.execute(query).fetchall()
    conn.close()

    summary = []
    for row in rows:
        summary.append({
            "timestamp": row[0],
            "date": row[1],
            "ticker": row[2],
            "tipo": row[3],
            "stop_loss": row[4],
            "entry_price": row[5],
            "take_profit": row[6],
            "r_esperado": row[7],
            "exit_price": row[8],
            "r_final": row[9],
            "pnl": round(row[10], 2) if row[10] is not None else None
        })

    return jsonify(summary)

# ─────────────────────────────────────────────────────────────
# Histograma del spread
# ─────────────────────────────────────────────────────────────
@app.route('/spread-histogram')
def get_spread_histogram():
    conn = get_motherduck_connection()
    query = """
    SELECT ABS(ask_price - bid_price) AS spread
    FROM htf.depth_updates
    WHERE ask_price IS NOT NULL AND bid_price IS NOT NULL
    ORDER BY E DESC
    LIMIT 500;
    """
    data = conn.execute(query).fetchall()
    conn.close()
    spreads = [row[0] for row in data if row[0] is not None]
    return jsonify({"spreads": spreads})

# ─────────────────────────────────────────────────────────────
# Histograma de profundidad (bid y ask por separado)
# ─────────────────────────────────────────────────────────────
@app.route('/depth-distribution-histogram')
def get_depth_distribution_histogram():
    conn = get_motherduck_connection()
    query = """
    SELECT bid_price, ask_price
    FROM htf.depth_updates
    WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
    ORDER BY E DESC
    LIMIT 500;
    """
    data = conn.execute(query).fetchall()
    conn.close()
    bid_prices = [row[0] for row in data if row[0] is not None]
    ask_prices = [row[1] for row in data if row[1] is not None]
    return jsonify({"bid_prices": bid_prices, "ask_prices": ask_prices})

# ─────────────────────────────────────────────────────────────
# Ejecutar la app
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    app.run(debug=True)
