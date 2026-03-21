@echo off
cd /d C:\Users\mcala\Documents\convexity_engine\monos-conviction
echo [%date% %time%] Starting MONOS nightly pipeline >> nightly_log.txt
python run_pipeline.py >> nightly_log.txt 2>&1
python guardian_engine.py >> nightly_log.txt 2>&1
python reload_engine.py >> nightly_log.txt 2>&1
python gex_engine.py >> nightly_log.txt 2>&1
python demark_engine.py >> nightly_log.txt 2>&1
echo [%date% %time%] Pipeline complete >> nightly_log.txt
