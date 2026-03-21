@echo off
cd /d C:\Users\mcala\Documents\convexity_engine\monos-conviction

echo ================================
echo [%date% %time%] MONOS nightly pipeline starting
echo ================================

echo [1/12] ticker_universe_setup.py
echo [%date% %time%] [1/12] ticker_universe_setup.py >> nightly_log.txt
python ticker_universe_setup.py 2>&1 | findstr /v "^$"
if %errorlevel% neq 0 echo   [!] ticker_universe_setup had warnings

echo [2/12] gex_engine.py
echo [%date% %time%] [2/12] gex_engine.py >> nightly_log.txt
python gex_engine.py 2>&1 | findstr /v "^$"

echo [3/12] flow_engine.py
echo [%date% %time%] [3/12] flow_engine.py >> nightly_log.txt
python flow_engine.py 2>&1 | findstr /v "^$"

echo [4/12] demark_engine.py
echo [%date% %time%] [4/12] demark_engine.py >> nightly_log.txt
python demark_engine.py 2>&1 | findstr /v "^$"

echo [5/12] fib_engine.py
echo [%date% %time%] [5/12] fib_engine.py >> nightly_log.txt
python fib_engine.py 2>&1 | findstr /v "^$"

echo [6/12] vix_regime_engine.py
echo [%date% %time%] [6/12] vix_regime_engine.py >> nightly_log.txt
python vix_regime_engine.py 2>&1 | findstr /v "^$"

echo [7/12] symmetry_engine.py
echo [%date% %time%] [7/12] symmetry_engine.py >> nightly_log.txt
python symmetry_engine.py 2>&1 | findstr /v "^$"

echo [8/12] scenario_synthesis_engine.py
echo [%date% %time%] [8/12] scenario_synthesis_engine.py >> nightly_log.txt
python scenario_synthesis_engine.py 2>&1 | findstr /v "^$"

echo [9/12] scanner_engine.py
echo [%date% %time%] [9/12] scanner_engine.py >> nightly_log.txt
python scanner_engine.py 2>&1 | findstr /v "^$"

echo [10/12] guardian_engine.py
echo [%date% %time%] [10/12] guardian_engine.py >> nightly_log.txt
python guardian_engine.py 2>&1 | findstr /v "^$"

echo [11/12] reload_engine.py
echo [%date% %time%] [11/12] reload_engine.py >> nightly_log.txt
python reload_engine.py 2>&1 | findstr /v "^$"

echo [12/12] monitor_engine.py
echo [%date% %time%] [12/12] monitor_engine.py >> nightly_log.txt
python monitor_engine.py 2>&1 | findstr /v "^$"

echo ================================
echo [%date% %time%] MONOS nightly pipeline complete
echo ================================
