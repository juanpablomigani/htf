import os
import time
import signal
import logging
import duckdb
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────
# Configuración inicial
# ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
load_dotenv()

# Claves API (desde variables de entorno)
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

# ──────────────────────────────────────────────────────────────
# Conexión a MotherDuck
# ──────────────────────────────────────────────────────────────
def connect_to_motherduck():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("Falta el token de MotherDuck")
    return duckdb.connect(f"md:?token={token}")

conn = connect_to_motherduck()

# ──────────────────────────────────────────────────────────────
# Manejo de cierre limpio y borrado de datos
# ──────────────────────────────────────────────────────────────
def clear_database():
    try:
        logging.info("Eliminando datos de la sesión actual...")
        conn.execute("DELETE FROM htf.depth_updates")
        conn.commit()
        logging.info("Datos eliminados correctamente.")
    except Exception as e:
        logging.error(f"Error al eliminar datos: {e}")

def signal_handler(sig, frame):
    logging.info("Deteniendo script... Eliminando datos de MotherDuck.")
    clear_database()
    conn.close()
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ──────────────────────────────────────────────────────────────
# Inserción de datos en MotherDuck
# ──────────────────────────────────────────────────────────────
def send_to_motherduck(bids, asks, event_time):
    try:
        if bids and asks:
            bid_price, bid_quantity = float(bids[0][0]), float(bids[0][1])
            ask_price, ask_quantity = float(asks[0][0]), float(asks[0][1])
            query = """
                INSERT INTO htf.depth_updates (E, bid_price, bid_quantity, ask_price, ask_quantity)
                VALUES (?, ?, ?, ?, ?)
            """
            conn.execute(query, [event_time, bid_price, bid_quantity, ask_price, ask_quantity])
            logging.info("Datos enviados a MotherDuck.")
    except Exception as e:
        logging.error(f"Error al enviar datos a MotherDuck: {e}")

# ──────────────────────────────────────────────────────────────
# Manejador del stream de Binance
# ──────────────────────────────────────────────────────────────
def handle_depth_message(msg):
    try:
        event_time = msg['E']
        bids = msg['b']
        asks = msg['a']
        send_to_motherduck(bids, asks, event_time)
    except KeyError as e:
        logging.error(f"Error en el mensaje de profundidad: {e}")

# ──────────────────────────────────────────────────────────────
# ▶Inicio del streaming
# ──────────────────────────────────────────────────────────────
def start_streaming():
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    twm.start_depth_socket(callback=handle_depth_message, symbol='BTCUSDT')

    try:
        logging.info("Streaming iniciado...")
        signal.pause()
    except Exception as e:
        logging.error(f"Error en WebSocket: {e}")
    finally:
        twm.stop()
        logging.info("Streaming detenido.")

# ──────────────────────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_streaming()