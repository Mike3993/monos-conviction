@echo off
cd /d C:\Users\mcala\Documents\convexity_engine\monos-conviction

echo ================================
echo [%date% %time%] MONOS nightly pipeline starting
echo ================================

echo [1/14] ticker_universe_setup.py
echo [%date% %time%] [1/14] ticker_universe_setup.py >> nightly_log.txt
python ticker_universe_setup.py 2>&1 | findstr /v "^$"
if %errorlevel% neq 0 echo   [!] ticker_universe_setup had warnings

echo [2/14] gex_engine.py
echo [%date% %time%] [2/14] gex_engine.py >> nightly_log.txt
python gex_engine.py 2>&1 | findstr /v "^$"

echo [3/14] flow_engine.py
echo [%date% %time%] [3/14] flow_engine.py >> nightly_log.txt
python flow_engine.py 2>&1 | findstr /v "^$"

echo [4/14] demark_engine.py
echo [%date% %time%] [4/14] demark_engine.py >> nightly_log.txt
python demark_engine.py 2>&1 | findstr /v "^$"

echo [5/14] fib_engine.py
echo [%date% %time%] [5/14] fib_engine.py >> nightly_log.txt
python fib_engine.py 2>&1 | findstr /v "^$"

echo [6/14] vix_regime_engine.py
echo [%date% %time%] [6/14] vix_regime_engine.py >> nightly_log.txt
python vix_regime_engine.py 2>&1 | findstr /v "^$"

echo [7/14] symmetry_engine.py
echo [%date% %time%] [7/14] symmetry_engine.py >> nightly_log.txt
python symmetry_engine.py 2>&1 | findstr /v "^$"

echo [8/14] scenario_synthesis_engine.py
echo [%date% %time%] [8/14] scenario_synthesis_engine.py >> nightly_log.txt
python scenario_synthesis_engine.py 2>&1 | findstr /v "^$"

echo [9/14] scanner_engine.py
echo [%date% %time%] [9/14] scanner_engine.py >> nightly_log.txt
python scanner_engine.py 2>&1 | findstr /v "^$"

echo [10/14] guardian_engine.py
echo [%date% %time%] [10/14] guardian_engine.py >> nightly_log.txt
python guardian_engine.py 2>&1 | findstr /v "^$"

echo [11/14] position_graph_engine.py
echo [%date% %time%] [11/14] position_graph_engine.py >> nightly_log.txt
python position_graph_engine.py 2>&1 | findstr /v "^$"

echo [12/14] reload_engine.py
echo [%date% %time%] [12/14] reload_engine.py >> nightly_log.txt
python reload_engine.py 2>&1 | findstr /v "^$"

echo [13/14] wealth_builder_engine.py
echo [%date% %time%] [13/14] wealth_builder_engine.py >> nightly_log.txt
python wealth_builder_engine.py 2>&1 | findstr /v "^$"

echo [14/14] monitor_engine.py
echo [%date% %time%] [14/14] monitor_engine.py >> nightly_log.txt
python monitor_engine.py 2>&1 | findstr /v "^$"

echo ================================
echo [%date% %time%] MONOS nightly pipeline complete
echo ================================
