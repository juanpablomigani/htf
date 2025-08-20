## HTF - High Frequency Trading (Proyecto de diplomatura)

Estructura limpia y mínima para un dashboard Flask, un streamer de mercado y un bucle de trading que leen/escriben en MotherDuck.

### Estructura
- `app.py`: servidor Flask para el dashboard (`templates/index.html`).
- `stream.py`: consume el order book de Binance por WebSocket y persiste en MotherDuck.
- `trader.py`: lee datos de MotherDuck, evalúa señales y registra operaciones/estado.
- `templates/index.html`: dashboard (Plotly + jQuery en el navegador).
- `requirements.txt`: dependencias.
- `env.example`: variables de entorno (copiar a `.env`).

### Requisitos
- Python 3.10+
- Token de MotherDuck
- (Opcional) Claves de API de Binance si quieres operar en real

### Instalación
```bash
python -m venv .venv
source .venv/bin/activate  # en Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp env.example .env
# Edita .env y completa MOTHERDUCK_TOKEN y, opcionalmente, claves de Binance
```

### Ejecución
- Ingesta (WebSocket Binance -> MotherDuck):
```bash
python stream.py
```
- Estrategia de trading (lee de MotherDuck):
```bash
python trader.py
```
- Dashboard Flask:
```bash
python app.py
```

Por defecto `USE_BINANCE=false` (modo simulado). Para operar real, en `.env` define `USE_BINANCE=true` y completa `BINANCE_API_KEY` y `BINANCE_API_SECRET`.