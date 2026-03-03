import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Try loading from .env
load_dotenv()

db_url = os.getenv('DATABASE_URL')
if not db_url:
    print("DATABASE_URL not set.")
    exit(1)

if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

print(f"Connecting to {db_url.split('@')[-1]}")
engine = create_engine(db_url)

commands = [
    "ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS benchmark_price_at_signal FLOAT;",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS benchmark_entry_price FLOAT;",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS benchmark_exit_price FLOAT;",
    "ALTER TABLE fund_performance ADD COLUMN IF NOT EXISTS alpha_vs_benchmark FLOAT DEFAULT 0.0;",
    "ALTER TABLE fund_performance ADD COLUMN IF NOT EXISTS benchmark_return FLOAT DEFAULT 0.0;"
]

with engine.connect() as conn:
    for cmd in commands:
        try:
            conn.execute(text(cmd))
            conn.commit()
            print(f"Executed: {cmd}")
        except Exception as e:
            print(f"Failed: {cmd} | Error: {e}")
