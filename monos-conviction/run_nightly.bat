@echo off
cd /d C:\Users\mcala\Documents\convexity_engine\monos-conviction

echo ================================ >> nightly_log.txt
echo [%date% %time%] MONOS nightly pipeline starting >> nightly_log.txt
echo ================================ >> nightly_log.txt

echo [1/10] ticker_universe_setup.py >> nightly_log.txt
python ticker_universe_setup.py >> nightly_log.txt 2>&1

echo [2/10] gex_engine.py >> nightly_log.txt
python gex_engine.py >> nightly_log.txt 2>&1

echo [3/10] demark_engine.py >> nightly_log.txt
python demark_engine.py >> nightly_log.txt 2>&1

echo [4/10] fib_engine.py >> nightly_log.txt
python fib_engine.py >> nightly_log.txt 2>&1

echo [5/10] vix_regime_engine.py >> nightly_log.txt
python vix_regime_engine.py >> nightly_log.txt 2>&1

echo [6/10] scenario_synthesis_engine.py >> nightly_log.txt
python scenario_synthesis_engine.py >> nightly_log.txt 2>&1

echo [7/10] scanner_engine.py >> nightly_log.txt
python scanner_engine.py >> nightly_log.txt 2>&1

echo [8/10] guardian_engine.py >> nightly_log.txt
python guardian_engine.py >> nightly_log.txt 2>&1

echo [9/10] reload_engine.py >> nightly_log.txt
python reload_engine.py >> nightly_log.txt 2>&1

echo [10/10] monitor_engine.py >> nightly_log.txt
python monitor_engine.py >> nightly_log.txt 2>&1

echo ================================ >> nightly_log.txt
echo [%date% %time%] MONOS nightly pipeline complete >> nightly_log.txt
echo Pipeline run complete >> nightly_log.txt
echo ================================ >> nightly_log.txt
