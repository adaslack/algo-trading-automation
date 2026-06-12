"""
Pipeline Orchestrator (The Orchestrator)
=========================================
Central process and thread manager for the US equities quant trading pipeline.
Spawns all microservices, attaches event tracing, and monitors their health.
If a thread or process crashes, the Orchestrator automatically restarts it.
"""
import subprocess
import time
import os
import sys
import threading
from logger import get_logger

# Import standard oriented core modules to run as threads
from websocket_ingest import run_websocket_ingest
from alpha_adapter import run_alpha_adapter
from risk_manager import run_risk_manager
from execution_engine import run_execution_engine

from event_bus import get_bus, Topics

log = get_logger("Orchestrator")

def start_process(script_name):
    log.info(f"Booting Background Service: {script_name}...")
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
    return subprocess.Popen([sys.executable, script_path])

def setup_event_tracer():
    """Logs the exact lifecycle of all major pipeline events."""
    bus = get_bus()
    
    def trace(event):
        try:
            ticker = event.data.get('ticker', 'SYS')
            if event.source == "websocket_ingest":
                details = f"Features Ready (OBI={event.data.get('features', {}).get('obi', 0):.2f})"
            elif event.source == "alpha_adapter":
                details = f"{event.data.get('signal_type')} via {event.data.get('target_strategy')} (Conf: {event.data.get('confidence',0)*100:.0f}%)"
            elif event.source == "risk_manager":
                details = f"APPROVED (Kelly: {event.data.get('kelly_pct',0)*100:.1f}%)"
            elif event.source == "execution_engine":
                details = f"FILLED @ ${event.data.get('fill_price',0):.2f} (Slip: {event.data.get('slippage_bps',0):.1f}bps)"
            else:
                details = "Data Update"
                
            log.info(f"🔍 [TRACER] {ticker.ljust(5)} | {event.source.upper().ljust(8)} | {details}")
        except Exception:
            pass

    # Subscribe tracer to major topics
    for topic in [Topics.MARKET_UPDATE, Topics.SIGNAL_GENERATED, Topics.ORDER_APPROVED, Topics.ORDER_FILLED]:
        bus.subscribe(topic, trace)
    log.info("Visual Event Tracer Middleware attached.")

def run_orchestrator():
    log.info("=" * 60)
    log.info("  INSTITUTIONAL ALPHA PIPELINE ONLINE")
    log.info("  Convex Optimizer | Microstructure | Alt Data | Recovery")
    log.info("=" * 60)
    
    # Ensure database is setup
    log.info("Validating Database Schema...")
    subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db_setup.py')])
    
    # Run screener once on boot
    log.info("Booting Screener to find Top US Stocks...")
    screener_proc = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'screener.py')])
    if screener_proc.returncode != 0:
        log.error("Screener failed. Pipeline will wait for manual watchlist population.")
    
    setup_event_tracer()
    
    # Core Event-Driven Threads Definition
    core_thread_defs = {
        "ExecutionEngine": run_execution_engine,
        "RiskManager": run_risk_manager,
        "AlphaAdapter": run_alpha_adapter,
        "WebsocketIngest": run_websocket_ingest
    }
    
    log.info("Booting Core Event-Driven Threads with Recovery Watchdog...")
    core_threads = {}
    
    for name, target in core_thread_defs.items():
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        core_threads[name] = t
        time.sleep(0.5)

    # Background Workers (Processes)
    processes = {
        'meta_learning.py': start_process('meta_learning.py'),
        'cross_sectional_ranker.py': start_process('cross_sectional_ranker.py'),
        'research_core.py': start_process('research_core.py'),
    }
    
    log.info(f"Pipeline Fully Operational: {len(core_threads)} Threads + {len(processes)} Processes.")
    
    try:
        while True:
            time.sleep(5)
            # 1. Monitor background processes and auto-restart if crashed
            for script_name, proc in processes.items():
                if proc.poll() is not None:
                    log.critical(f"{script_name} CRASHED! (Exit code: {proc.returncode})")
                    log.info(f"Auto-restarting background process {script_name}...")
                    processes[script_name] = start_process(script_name)
                    
            # 2. Monitor core threads and auto-restart if crashed (Orchestrator Optimization)
            for name, t in list(core_threads.items()):
                if not t.is_alive():
                    log.critical(f"FATAL: Core thread {name} crashed! Watchdog restarting...")
                    target_fn = core_thread_defs[name]
                    new_t = threading.Thread(target=target_fn, name=name, daemon=True)
                    new_t.start()
                    core_threads[name] = new_t
                    log.info(f"Successfully restarted thread {name}.")
                    
    except KeyboardInterrupt:
        log.info("Shutting down entire pipeline...")
        for proc in processes.values():
            proc.terminate()
        log.info("Pipeline offline.")

if __name__ == '__main__':
    run_orchestrator()

