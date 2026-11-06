import json
import time
from pathlib import Path
from urllib.request import Request, urlopen


URL = "http://127.0.0.1:23380"
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def api(method, path, data=None, session=None):
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    body = json.dumps(data).encode() if data is not None else None
    request = Request(URL + path, data=body, headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def main():
    env = dict(
        line.split("=", 1)
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.startswith("#")
    )

    # Pastikan Metabase sudah siap.
    for _ in range(60):
        try:
            if api("GET", "/api/health")["status"] == "ok":
                break
        except OSError:
            time.sleep(5)
    else:
        raise RuntimeError("Metabase belum siap")

    settings = api("GET", "/api/session/properties")
    if settings.get("setup-token"):
        hasil = api("POST", "/api/setup", {
            "token": settings["setup-token"],
            "user": {
                "first_name": "Admin", "last_name": "Local",
                "email": env["METABASE_EMAIL"],
                "password": env["METABASE_PASSWORD"],
            },
            "prefs": {"site_name": "Day 51 Transaction Monitoring"},
        })
    else:
        hasil = api("POST", "/api/session", {
            "username": env["METABASE_EMAIL"],
            "password": env["METABASE_PASSWORD"],
        })
    session = hasil["id"]

    daftar_db = api("GET", "/api/database", session=session)["data"]
    db = next((x for x in daftar_db if x["name"] == "Day 51 Transactions"), None)
    if db:
        db_id = db["id"]
    else:
        db = api("POST", "/api/database", {
            "name": "Day 51 Transactions",
            "engine": "postgres",
            "details": {
                "host": "postgres", "port": 5432,
                "dbname": env["POSTGRES_DB"],
                "user": env["POSTGRES_USER"],
                "password": env["POSTGRES_PASSWORD"],
                "ssl": False,
            },
        }, session)
        db_id = db["id"]

    nama = "Monitoring Transaksi - Day 51"
    daftar_dashboard = api("GET", "/api/dashboard", session=session)
    if isinstance(daftar_dashboard, dict):
        daftar_dashboard = daftar_dashboard.get("data", [])
    dashboard = next((x for x in daftar_dashboard if x["name"] == nama), None)
    if dashboard:
        print(f"Dashboard: {URL}/dashboard/{dashboard['id']}")
        return

    grafik = [
        ("Transaksi Valid", "scalar", "SELECT COUNT(*) FROM valid_transactions", 0, 0, 6, 3),
        ("Transaksi Invalid", "scalar", "SELECT COUNT(*) FROM invalid_transactions", 6, 0, 6, 3),
        ("Nominal Valid", "scalar", "SELECT COALESCE(SUM(amount),0) FROM valid_transactions", 12, 0, 6, 3),
        ("Total Event", "scalar",
         "SELECT (SELECT COUNT(*) FROM valid_transactions) + "
         "(SELECT COUNT(*) FROM invalid_transactions)", 18, 0, 6, 3),
        ("Transaksi Per Menit", "line",
         "SELECT window_start AS menit, transaction_count AS transaksi "
         "FROM transaction_windows ORDER BY menit", 0, 3, 14, 7),
        ("Alasan Invalid", "bar",
         "SELECT error_reason AS alasan, COUNT(*) AS jumlah "
         "FROM invalid_transactions GROUP BY error_reason ORDER BY jumlah DESC",
         14, 3, 10, 7),
        ("Transaksi Terbaru", "table",
         "SELECT user_id, amount, event_time, source "
         "FROM valid_transactions ORDER BY id DESC LIMIT 20", 0, 10, 24, 7),
    ]

    kartu = []
    for no, (judul, tampilan, sql, col, row, lebar, tinggi) in enumerate(grafik, 1):
        hasil = api("POST", "/api/card", {
            "name": judul, "type": "question", "display": tampilan,
            "visualization_settings": {},
            "dataset_query": {
                "database": db_id, "type": "native", "native": {"query": sql},
            },
        }, session)
        kartu.append({
            "id": -no, "card_id": hasil["id"], "row": row, "col": col,
            "size_x": lebar, "size_y": tinggi,
        })

    dashboard = api("POST", "/api/dashboard", {"name": nama}, session)
    api("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": kartu}, session)
    print(f"Dashboard dibuat: {URL}/dashboard/{dashboard['id']}")


if __name__ == "__main__":
    main()
