"""
MONOS Dashboard Launcher
Run this file to start the dashboard with all dependencies checked.
"""
import subprocess
import sys

# Ensure dependencies
deps = ["flask", "requests", "yfinance", "numpy", "supabase", "python-dotenv"]
for pkg in deps:
    try:
        __import__(pkg.replace("-", "_"))
    except ImportError:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

# Now launch
from monos_engine.dashboard.app import app

print("\n  MONOS Backtest Dashboard")
print("  http://127.0.0.1:5050\n")
app.run(debug=True, port=5050, use_reloader=False)
