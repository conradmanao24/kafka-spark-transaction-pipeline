import json
import time
import urllib.error
import urllib.request
from pathlib import Path


BASE = "http://127.0.0.1:23380"
ROOT = Path(__file__).resolve().parent.parent


def baca_env():
    result = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def api(method, path, data=None, session=None):
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    payload = json.dumps(data).encode("utf-8") if data is not None else None
    request = urllib.request.Request(
        BASE + path, data=payload, headers=headers, method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read().decode("utf-8")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"Metabase {method} {path}: {error.code} "
            f"{error.read().decode('utf-8')[:500]}"
        ) from error


def tunggu_metabase():
    for _ in range(60):
        try:
            result = api("GET", "/api/health")
            if result.get("status") == "ok":
                return
        except (OSError, RuntimeError):
            pass
        time.sleep(5)
    raise RuntimeError("Metabase belum siap. Cek docker compose logs metabase.")


def main():
    env = baca_env()
    tunggu_metabase()

    properties = api("GET", "/api/session/properties")
    email = env["METABASE_EMAIL"]
    password = env["METABASE_PASSWORD"]

    if properties.get("setup-token"):
        result = api("POST", "/api/setup", {
            "token": properties["setup-token"],
            "user": {
                "first_name": "Local",
                "last_name": "Admin",
                "email": email,
                "password": password,
            },
            "prefs": {"site_name": "Day 51 Transaction Monitoring"},
        })
        session = result["id"]
    else:
        session = api("POST", "/api/session", {
            "username": email, "password": password,
        })["id"]

    databases = api("GET", "/api/database", session=session)
    existing = next(
        (item for item in databases.get("data", [])
         if item.get("name") == "Day 51 Transactions"), None
    )
    if existing:
        db_id = existing["id"]
    else:
        result = api("POST", "/api/database", {
            "name": "Day 51 Transactions",
            "engine": "postgres",
            "details": {
                "host": "postgres",
                "port": 5432,
                "dbname": env["POSTGRES_DB"],
                "user": env["POSTGRES_USER"],
                "password": env["POSTGRES_PASSWORD"],
                "ssl": False,
            },
        }, session=session)
        db_id = result["id"]

    dashboards = api("GET", "/api/dashboard", session=session)
    items = dashboards if isinstance(dashboards, list) else dashboards.get("data", [])
    found = next(
        (item for item in items if item.get("name") == "Monitoring Transaksi - Day 51"),
        None,
    )
    if found:
        dashboard_id = found["id"]
        print(f"Dashboard sudah tersedia: {BASE}/dashboard/{dashboard_id}")
        return

    cards = [
        ("Transaksi Valid", "scalar",
         "SELECT COUNT(*) AS transaksi_valid FROM valid_transactions",
         0, 0, 6, 3),
        ("Transaksi Invalid", "scalar",
         "SELECT COUNT(*) AS transaksi_invalid FROM invalid_transactions",
         6, 0, 6, 3),
        ("Total Nominal Valid", "scalar",
         "SELECT COALESCE(SUM(amount),0) AS total_nominal FROM valid_transactions",
         12, 0, 6, 3),
        ("Total Event Diproses", "scalar",
         "SELECT (SELECT COUNT(*) FROM valid_transactions) + "
         "(SELECT COUNT(*) FROM invalid_transactions) AS total_event",
         18, 0, 6, 3),
        ("Transaksi Per Menit", "line",
         "SELECT window_start AS menit, transaction_count AS transaksi "
         "FROM transaction_windows ORDER BY menit",
         0, 3, 14, 7),
        ("Alasan Data Invalid", "bar",
         "SELECT error_reason AS alasan, COUNT(*) AS jumlah "
         "FROM invalid_transactions GROUP BY error_reason "
         "ORDER BY jumlah DESC",
         14, 3, 10, 7),
        ("Transaksi Terbaru", "table",
         "SELECT user_id, amount, event_time, source "
         "FROM valid_transactions ORDER BY id DESC LIMIT 20",
         0, 10, 24, 7),
    ]

    dashcards = []
    for number, (name, display, sql, col, row, width, height) in enumerate(cards, 1):
        result = api("POST", "/api/card", {
            "name": name,
            "type": "question",
            "display": display,
            "visualization_settings": {},
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": sql},
            },
        }, session=session)
        dashcards.append({
            "id": -number,
            "card_id": result["id"],
            "row": row, "col": col, "size_x": width, "size_y": height,
        })

    dashboard = api("POST", "/api/dashboard", {
        "name": "Monitoring Transaksi - Day 51",
        "description": "Kafka, Spark Streaming, PostgreSQL",
    }, session=session)
    dashboard_id = dashboard["id"]

    api("PUT", f"/api/dashboard/{dashboard_id}", {
        "dashcards": dashcards,
    }, session=session)

    print(f"Dashboard berhasil dibuat: {BASE}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
