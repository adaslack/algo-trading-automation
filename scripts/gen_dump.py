"""
Codebase Dump Generator (V15 Alpha Purity & Production Realism Upgrade)
======================================================================
Dynamically scans all active files in the workspace (README, requirements, docker-compose, src/pipeline, and scripts)
and aggregates them into a clean, formatted codebase_dump_v15.md file.
"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, 'codebase_dump_v15.md')

FILES_TO_DUMP = [
    'README.md',
    'requirements.txt',
    'docker-compose.yml',
    # src/pipeline core
    'src/pipeline/alpha_adapter.py',
    'src/pipeline/alpha_engine.py',
    'src/pipeline/cross_sectional_ranker.py',
    'src/pipeline/db_adapter.py',
    'src/pipeline/db_postgres.py',
    'src/pipeline/db_setup.py',
    'src/pipeline/event_bus.py',
    'src/pipeline/execution_analytics.py',
    'src/pipeline/execution_engine.py',
    'src/pipeline/factor_calibration_engine.py',
    'src/pipeline/feature_lineage_registry.py',
    'src/pipeline/feature_store.py',
    'src/pipeline/logger.py',
    'src/pipeline/lob_replay_engine.py',
    'src/pipeline/ml_ranker.py',
    'src/pipeline/meta_learning.py',
    'src/pipeline/ml_validator.py',
    'src/pipeline/models.py',
    'src/pipeline/orchestrator.py',
    'src/pipeline/portfolio.py',
    'src/pipeline/portfolio_state.py',
    'src/pipeline/research_core.py',
    'src/pipeline/risk_gate.py',
    'src/pipeline/risk_manager.py',
    'src/pipeline/screener.py',
    'src/pipeline/walkforward_engine.py',
    'src/pipeline/websocket_ingest.py',
    # scripts
    'scripts/test_alpha_portfolio.py',
    'scripts/test_research_engine.py',
    'scripts/test_microstructure_pipeline.py',
    'scripts/test_systematic_upgrades.py',
    'scripts/backtest_historical.py',
    'scripts/backtest_5years.py',
]

def main():
    print(f"Generating codebase dump at: {OUTPUT_PATH}")
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as out_f:
        out_f.write("# ALGO-TRADING-AUTOMATION SYSTEMATIC CODEBASE DUMP (V15 Alpha Purity & Production Realism)\n\n")
        out_f.write("## Table of Contents\n")
        for f_path in FILES_TO_DUMP:
            normalized_path = f_path.replace('/', '\\')
            out_f.write(f"- {normalized_path}\n")
            
        out_f.write("\n---\n\n")
        
        for f_path in FILES_TO_DUMP:
            abs_path = os.path.join(BASE_DIR, f_path)
            normalized_path = f_path.replace('/', '\\')
            print(f"  Adding {f_path}...")
            
            if not os.path.exists(abs_path):
                print(f"  ⚠️ Warning: {f_path} does not exist. Skipping.")
                continue
                
            out_f.write(f"## {normalized_path}\n\n")
            
            ext = os.path.splitext(f_path)[1]
            lang = 'python'
            if ext == '.md':
                lang = 'markdown'
            elif ext in ('.yml', '.yaml'):
                lang = 'yaml'
            elif ext == '.txt':
                lang = 'text'
                
            out_f.write(f"```{lang}\n")
            with open(abs_path, 'r', encoding='utf-8', errors='ignore') as in_f:
                out_f.write(in_f.read())
            out_f.write("\n```\n\n---\n\n")
            
    print("Done! Codebase dump successfully generated.")

if __name__ == '__main__':
    main()
