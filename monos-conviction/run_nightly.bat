@echo off
cd /d C:\Users\mcala\Documents\convexity_engine\monos-conviction

echo ================================
echo [%date% %time%] MONOS nightly pipeline starting (19 steps)
echo ================================

echo [1/19] ticker_universe_setup.py
echo [%date% %time%] [1/19] ticker_universe_setup.py >> nightly_log.txt
python ticker_universe_setup.py 2>&1 | findstr /v "^$"
if %errorlevel% neq 0 echo   [!] ticker_universe_setup had warnings

echo [2/19] gex_engine.py
echo [%date% %time%] [2/19] gex_engine.py >> nightly_log.txt
python gex_engine.py 2>&1 | findstr /v "^$"

echo [3/19] flow_engine.py
echo [%date% %time%] [3/19] flow_engine.py >> nightly_log.txt
python flow_engine.py 2>&1 | findstr /v "^$"

echo [4/19] demark_engine.py
echo [%date% %time%] [4/19] demark_engine.py >> nightly_log.txt
python demark_engine.py 2>&1 | findstr /v "^$"

echo [5/19] fib_engine.py
echo [%date% %time%] [5/19] fib_engine.py >> nightly_log.txt
python fib_engine.py 2>&1 | findstr /v "^$"

echo [6/19] vix_regime_engine.py
echo [%date% %time%] [6/19] vix_regime_engine.py >> nightly_log.txt
python vix_regime_engine.py 2>&1 | findstr /v "^$"

echo [7/19] symmetry_engine.py
echo [%date% %time%] [7/19] symmetry_engine.py >> nightly_log.txt
python symmetry_engine.py 2>&1 | findstr /v "^$"

echo [8/19] scenario_synthesis_engine.py
echo [%date% %time%] [8/19] scenario_synthesis_engine.py >> nightly_log.txt
python scenario_synthesis_engine.py 2>&1 | findstr /v "^$"

echo [9/19] scanner_engine.py
echo [%date% %time%] [9/19] scanner_engine.py >> nightly_log.txt
python scanner_engine.py 2>&1 | findstr /v "^$"

echo [10/19] shift_engine.py
echo [%date% %time%] [10/19] shift_engine.py >> nightly_log.txt
python shift_engine.py 2>&1 | findstr /v "^$"

echo [11/19] pre_msa_engine.py
echo [%date% %time%] [11/19] pre_msa_engine.py >> nightly_log.txt
python pre_msa_engine.py 2>&1 | findstr /v "^$"

echo [12/19] rotation_engine.py
echo [%date% %time%] [12/19] rotation_engine.py >> nightly_log.txt
python rotation_engine.py 2>&1 | findstr /v "^$"

echo [13/19] megabrain_engine.py
echo [%date% %time%] [13/19] megabrain_engine.py >> nightly_log.txt
python megabrain_engine.py 2>&1 | findstr /v "^$"

echo [14/19] guardian_engine.py
echo [%date% %time%] [14/19] guardian_engine.py >> nightly_log.txt
python guardian_engine.py 2>&1 | findstr /v "^$"

echo [15/19] position_graph_engine.py
echo [%date% %time%] [15/19] position_graph_engine.py >> nightly_log.txt
python position_graph_engine.py 2>&1 | findstr /v "^$"

echo [16/19] reload_engine.py
echo [%date% %time%] [16/19] reload_engine.py >> nightly_log.txt
python reload_engine.py 2>&1 | findstr /v "^$"

echo [17/19] wealth_builder_engine.py
echo [%date% %time%] [17/19] wealth_builder_engine.py >> nightly_log.txt
python wealth_builder_engine.py 2>&1 | findstr /v "^$"

echo [18/19] monitor_engine.py
echo [%date% %time%] [18/19] monitor_engine.py >> nightly_log.txt
python monitor_engine.py 2>&1 | findstr /v "^$"

echo [19/19] system health check
echo [%date% %time%] [19/19] health check >> nightly_log.txt
echo   Pipeline: 19 engines completed

echo ================================
echo [%date% %time%] MONOS nightly pipeline complete (19 steps)
echo ================================
