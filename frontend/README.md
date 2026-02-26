┌─────────────────────────────────────────────────────────────────────────────┐
│  📈 Stock Streaming Platform          Real-time S&P 500...     🕐 14:32:05 │
├──────────────────┬──────────────────────────────────────────────────────────┤
│  🎛️ Steuerung    │                                                          │
│  ─────────────── │   📊 Live Chart                                          │
│                  │  ┌──────────────────────────────────────────────────────┐ │
│  🎯 Ticker       │  │                                                      │ │
│  ┌─────────────┐ │  │    AAPL | $187.42 ▲ +2.15 (+1.16%)                 │ │
│  │All│Top10│Man│ │  │                                                      │ │
│  └─────────────┘ │  │    ██                                                │ │
│  ✅ 10 Ticker    │  │   ███ █   █    ██                                    │ │
│                  │  │   ████ ██ ██  ████  █                                │ │
│  ─────────────── │  │  █████████████████ ███                               │ │
│                  │  │  ██████████████████████                               │ │
│  🔄 Stream       │  │                                                      │ │
│  ┌──────┬──────┐ │  │  Volume: ▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐▐                │ │
│  │ Sek  │ Min  │ │  └──────────────────────────────────────────────────────┘ │
│  └──────┴──────┘ │                                                          │
│  ┌──────┬──────┐ │   📋 Ticker Übersicht                                   │
│  │▶Start│⏹Stop│ │  ┌────────┬────────────┬──────────┬────────┬───────────┐ │
│  └──────┴──────┘ │  │Symbol  │Name        │Industry  │Open($) │Close($)   │ │
│  🟢 Stream aktiv │  ├────────┼────────────┼──────────┼────────┼───────────┤ │
│                  │  │ AAPL   │Apple Inc.  │Cons.Elec.│ 185.27 │ 187.42    │ │
│  ─────────────── │  │ MSFT   │Microsoft   │Software  │ 378.50 │ 380.12    │ │
│                  │  │ GOOGL  │Alphabet    │Internet  │ 141.20 │ 142.85    │ │
│  📊 Chart-Typ    │  │ AMZN   │Amazon      │Retail    │ 178.90 │ 179.55    │ │
│  ┌──────┬──────┐ │  │ NVDA   │NVIDIA      │Semicon.  │ 875.30 │ 882.10    │ │
│  │Candle│Linie │ │  │ ...    │...         │...       │ ...    │ ...       │ │
│  └──────┴──────┘ │  └────────┴────────────┴──────────┴────────┴───────────┘ │
│                  │                                                          │
│  🔽 Plot-Ticker  │                                                          │
│  ┌─────────────┐ │                                                          │
│  │ AAPL      ▼ │ │                                                          │
│  └─────────────┘ │                                                          │
└──────────────────┴──────────────────────────────────────────────────────────┘


# 1. Conda-Umgebung aktivieren
conda activate stock-streaming

# 2. .env Datei ausfüllen (API Key, DB Credentials)
nano .env

# 3. Panel App starten
panel serve frontend/app.py \
    --show \
    --autoreload \
    --port 5006 \
    --address 0.0.0.0 \
    --allow-websocket-origin="*" \
    --num-procs 1

# 4. Öffne im Browser:
#    http://localhost:5006/app

# In frontend/callbacks/chart_callbacks.py ist _use_demo_data=True
# Das generiert Fake-Daten, sodass du die GUI ohne DB testen kannst
