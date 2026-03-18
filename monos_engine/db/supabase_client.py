import os
from dotenv import load_dotenv
from pathlib import Path
from supabase import create_client

# Explicitly load .env from project root
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

_supabase = None

def get_supabase():
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise ValueError("Missing Supabase environment variables")
        _supabase = create_client(url, key)
    return _supabase
