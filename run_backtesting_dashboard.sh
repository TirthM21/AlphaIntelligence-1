#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-8501}"

# Allow explicit override from env var when app file lives in a custom location.
APP_PATH="${BACKTESTING_DASHBOARD_APP:-}"

if [[ -z "$APP_PATH" ]]; then
  CANDIDATES=(
    "dashboard/backtesting_dashboard.py"
    "backtesting_dashboard.py"
    "src/backtesting/dashboard_app.py"
    "src/backtesting/streamlit_app.py"
    "src/web/backtesting_dashboard.py"
  )

  for candidate in "${CANDIDATES[@]}"; do
    if [[ -f "$ROOT_DIR/$candidate" ]]; then
      APP_PATH="$ROOT_DIR/$candidate"
      break
    fi
  done
fi

if [[ -z "$APP_PATH" ]]; then
  cat <<MSG >&2
Could not find a backtesting Streamlit app file.
Set BACKTESTING_DASHBOARD_APP to the app path and rerun, e.g.:
  BACKTESTING_DASHBOARD_APP=dashboard/backtesting_dashboard.py ./run_backtesting_dashboard.sh
MSG
  exit 1
fi

if [[ ! -f "$APP_PATH" ]]; then
  echo "BACKTESTING_DASHBOARD_APP points to a missing file: $APP_PATH" >&2
  exit 1
fi

echo "Launching backtesting dashboard: $APP_PATH"
echo "Port: $PORT"

cd "$ROOT_DIR"
exec python -m streamlit run "$APP_PATH" --server.address 0.0.0.0 --server.port "$PORT" "$@"
