"""ai_agent.py — Pełnoprawny agent AI dla Anity (kupca Add All).

Trzy kluczowe rzeczy odróżniające go od zwykłego chatbota:

1. **Function calling (tool use)** — agent sam wywołuje narzędzia
   (query_products, get_dostawca_summary, list_dostawcy) aby uzyskać
   konkretne, świeże dane z DataFrame analizy zamiast polegać tylko na
   statycznym kontekście tekstowym.

2. **Pamięć długoterminowa** — preferencje i nawyki Anity zapisywane
   w pliku JSON (data/anita_memory.json). Agent potrafi sam zapisać
   nowy fakt/preferencję przez tool save_preference / add_fact.

3. **Iteracyjna pętla narzędzi** — agent może w jednej odpowiedzi
   wywołać kilka narzędzi w łańcuchu (np. najpierw list_dostawcy,
   potem query_products dla wybranego dostawcy).
"""
from __future__ import annotations

import html as _html
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from engine import lookup_supplier_open_orders, match_minimum

# ── Pamięć Anity ──────────────────────────────────────────────────────────────

def _resolve_memory_file() -> Path:
    """Gdzie trzymać pamięć — zawsze na trwałym dysku, jeśli tylko istnieje.

    Kolejność: jawna zmienna ANITA_MEMORY_PATH → wolumen Railway → dysk lokalny.
    Railway wystawia RAILWAY_VOLUME_MOUNT_PATH tylko wtedy, gdy wolumen jest
    faktycznie podpięty, więc to jest wiarygodny sygnał trwałości.
    """
    explicit = os.environ.get("ANITA_MEMORY_PATH")
    if explicit:
        return Path(explicit)
    volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    if volume:
        return Path(volume) / "anita_memory.json"
    return Path("data/anita_memory.json")


MEMORY_FILE = _resolve_memory_file()
MEMORY_BAK = Path(str(MEMORY_FILE) + ".bak")  # kopia ostatniej dobrej wersji
SNAPSHOT_DIR = MEMORY_FILE.parent / "backups"  # rotowane kopie historyczne
MAX_SNAPSHOTS = 30
MAX_HISTORY = 50


def memory_storage_status() -> dict:
    """Czy pamięć leży na trwałym dysku? Do wyświetlenia ostrzeżenia w UI.

    Bez tego utrata pamięci jest cicha — apka wygląda normalnie, a zapisy
    znikają przy każdym restarcie kontenera.
    """
    volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    on_railway = bool(os.environ.get("RAILWAY_SERVICE_ID") or os.environ.get("RAILWAY_ENVIRONMENT"))
    path = MEMORY_FILE.absolute()

    if not on_railway:
        persistent, reason = True, "dysk lokalny"
    elif volume and str(path).startswith(str(Path(volume).absolute())):
        persistent, reason = True, f"wolumen Railway ({volume})"
    else:
        persistent, reason = False, "BRAK WOLUMENU — dysk kontenera znika przy restarcie"

    try:
        snapshots = len(list(SNAPSHOT_DIR.glob("anita_memory_*.json")))
    except Exception:
        snapshots = 0

    return {
        "persistent": persistent,
        "reason": reason,
        "path": str(path),
        "snapshots": snapshots,
    }


def _default_memory() -> dict:
    return {"preferences": {}, "facts": [], "history": [], "exclusions": {"products": [], "suppliers": []}}


def _snapshot_files() -> list[Path]:
    """Rotowane kopie, od najnowszej. Nazwa zawiera datę, więc sort działa."""
    try:
        return sorted(SNAPSHOT_DIR.glob("anita_memory_*.json"), reverse=True)
    except Exception:
        return []


def _load_memory() -> dict:
    """Czyta pamięć; przy uszkodzeniu głównego pliku próbuje kopii .bak,
    a gdy plik w ogóle zniknął (świeży kontener) — najnowszej rotowanej kopii."""
    for p in (MEMORY_FILE, MEMORY_BAK):
        try:
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
        except Exception:
            continue

    # Główny plik NIE ISTNIEJE — ratujemy z rotowanej kopii.
    # Uwaga: świadomie tylko przy braku pliku. Gdyby plik istniał i był pusty,
    # oznaczałoby to celowe wyczyszczenie pamięci i nie wolno go cofać.
    for snap in _snapshot_files():
        try:
            data = json.loads(snap.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _save_memory(data)  # od razu przywróć na główną ścieżkę
                return data
        except Exception:
            continue

    return _default_memory()


def _save_memory(memory: dict) -> None:
    """Zapis ODPORNY na utratę danych:
    1) kopiuje obecny plik do .bak (poprzednia dobra wersja),
    2) zapis atomowy: temp + os.replace (brak ryzyka połowicznego pliku)."""
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        if MEMORY_FILE.exists():
            MEMORY_BAK.write_text(
                MEMORY_FILE.read_text(encoding="utf-8"), encoding="utf-8"
            )
    except Exception:
        pass
    payload = json.dumps(memory, ensure_ascii=False, indent=2)
    tmp = MEMORY_FILE.with_name(MEMORY_FILE.name + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    os.replace(tmp, MEMORY_FILE)
    _write_snapshot(payload)


def _write_snapshot(payload: str) -> None:
    """Rotowana kopia z datą przy każdym zapisie (trzymamy MAX_SNAPSHOTS ostatnich).

    Chroni przed tym, czego .bak nie łapie: przypadkowym wyczyszczeniem pamięci
    albo błędnym nadpisaniem sprzed wielu zapisów. Awarie zapisu kopii nigdy
    nie mogą wywrócić zapisu głównego."""
    try:
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snap = SNAPSHOT_DIR / f"anita_memory_{stamp}.json"
        # Nadpisujemy bez warunku: kilka zapisów w tej samej sekundzie musi
        # zostawić stan NAJNOWSZY, inaczej migawka jest cofnięta o jeden zapis.
        snap.write_text(payload, encoding="utf-8")
        for old in _snapshot_files()[MAX_SNAPSHOTS:]:
            old.unlink(missing_ok=True)
    except Exception:
        pass


def export_memory_json() -> str:
    """Cała pamięć jako tekst JSON — do pobrania/backupu z UI."""
    return json.dumps(_load_memory(), ensure_ascii=False, indent=2)


def import_memory(raw, merge: bool = True) -> dict:
    """Wczytuje pamięć z JSON (str/bytes/dict). Domyślnie ŁĄCZY z obecną
    (nic nie kasuje) — bezpieczne przywracanie backupu."""
    incoming = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else dict(raw)
    if not isinstance(incoming, dict):
        raise ValueError("Niepoprawny format pamięci (oczekiwano obiektu JSON).")
    if not merge:
        _save_memory(incoming)
        return incoming

    cur = _load_memory()
    prefs = {**cur.get("preferences", {}), **incoming.get("preferences", {})}
    facts = list(cur.get("facts", []))
    for f in incoming.get("facts", []):
        if f not in facts:
            facts.append(f)

    def _merge_exc(a, b):
        out = [e for e in a if isinstance(e, dict)]
        seen = {e.get("value", "").lower() for e in out}
        for e in b:
            if isinstance(e, dict) and e.get("value", "").lower() not in seen:
                out.append(e)
                seen.add(e.get("value", "").lower())
        return out

    cur_exc = cur.get("exclusions", {}) or {}
    inc_exc = incoming.get("exclusions", {}) or {}
    exclusions = {
        "products": _merge_exc(cur_exc.get("products", []), inc_exc.get("products", [])),
        "suppliers": _merge_exc(cur_exc.get("suppliers", []), inc_exc.get("suppliers", [])),
    }
    history = (list(cur.get("history", [])) + list(incoming.get("history", [])))[-MAX_HISTORY:]
    merged = {
        "preferences": prefs,
        "facts": facts,
        "exclusions": exclusions,
        "history": history,
    }
    _save_memory(merged)
    return merged


def get_memory() -> dict:
    return _load_memory()


def save_preference(key: str, value: str) -> None:
    m = _load_memory()
    m.setdefault("preferences", {})[key] = value
    _save_memory(m)


def remove_preference(key: str) -> None:
    m = _load_memory()
    m.setdefault("preferences", {}).pop(key, None)
    _save_memory(m)


def add_fact(fact: str) -> None:
    m = _load_memory()
    facts = m.setdefault("facts", [])
    if fact and fact not in facts:
        facts.append(fact)
    _save_memory(m)


def remove_fact(idx: int) -> None:
    m = _load_memory()
    facts = m.setdefault("facts", [])
    if 0 <= idx < len(facts):
        facts.pop(idx)
    _save_memory(m)


def _exclusions(m: dict | None = None) -> dict:
    m = m if m is not None else _load_memory()
    exc = m.setdefault("exclusions", {})
    exc.setdefault("products", [])
    exc.setdefault("suppliers", [])
    return exc


def get_exclusions() -> dict:
    return _exclusions()


def add_exclusion(kind: str, value: str, reason: str | None = None) -> None:
    """kind ∈ {'products','suppliers'}. Trwale wyklucza produkt/dostawcę
    z rekomendacji. value = fragment nazwy/kod (case-insensitive)."""
    if kind not in ("products", "suppliers"):
        return
    value = (value or "").strip()
    if not value:
        return
    m = _load_memory()
    exc = _exclusions(m)
    entry = {"value": value, "reason": (reason or "").strip()}
    if not any(e.get("value", "").lower() == value.lower() for e in exc[kind]):
        exc[kind].append(entry)
    _save_memory(m)


def remove_exclusion(kind: str, value: str) -> None:
    if kind not in ("products", "suppliers"):
        return
    m = _load_memory()
    exc = _exclusions(m)
    exc[kind] = [e for e in exc[kind] if e.get("value", "").lower() != (value or "").lower()]
    _save_memory(m)


def add_history(question: str, answer: str) -> None:
    m = _load_memory()
    h = m.setdefault("history", [])
    h.append({
        "date": datetime.now().isoformat(timespec="seconds"),
        "question": question[:500],
        "answer": (answer or "")[:1000],
    })
    if len(h) > MAX_HISTORY:
        m["history"] = h[-MAX_HISTORY:]
    _save_memory(m)


def memory_summary_text() -> str:
    """Tekstowy snapshot pamięci do umieszczenia w system prompt."""
    m = _load_memory()
    parts: list[str] = []

    prefs = m.get("preferences") or {}
    if prefs:
        parts.append("PREFERENCJE ANITY:")
        for k, v in prefs.items():
            parts.append(f"  - {k}: {v}")

    facts = m.get("facts") or []
    if facts:
        parts.append("\nWIEDZA O FIRMIE / NAWYKI / REGUŁY:")
        for f in facts:
            parts.append(f"  - {f}")

    exc = m.get("exclusions") or {}
    exc_p = exc.get("products") or []
    exc_s = exc.get("suppliers") or []
    if exc_p or exc_s:
        parts.append("\n🚫 WYKLUCZENIA (NIE rekomenduj tych pozycji):")
        for e in exc_s:
            r = f" ({e['reason']})" if e.get("reason") else ""
            parts.append(f"  - DOSTAWCA: {e.get('value', '')}{r}")
        for e in exc_p:
            r = f" ({e['reason']})" if e.get("reason") else ""
            parts.append(f"  - PRODUKT: {e.get('value', '')}{r}")

    hist = m.get("history") or []
    if hist:
        recent = hist[-5:]
        parts.append(f"\nHISTORIA — ostatnie {len(recent)} pytań Anity:")
        for h in recent:
            parts.append(f"  - [{h.get('date', '')[:10]}] {h.get('question', '')}")

    return "\n".join(parts) if parts else "(pamięć pusta — to pierwsze użycie agenta)"


# ── Narzędzia (operują na DataFrame analiza) ─────────────────────────────────

def _get_col(df: pd.DataFrame, *hints: str) -> str | None:
    for h in hints:
        for c in df.columns:
            if h.lower() in str(c).lower():
                return c
    return None


def _filter_excluded(df: pd.DataFrame) -> pd.DataFrame:
    """Usuwa z DataFrame produkty i dostawców wykluczonych przez Anitę
    (pamięć: exclusions). Wykluczenie = zawiera fragment (case-insensitive)
    w nazwie dostawcy / nazwie produktu / kodzie."""
    exc = get_exclusions()
    if df.empty or not (exc.get("products") or exc.get("suppliers")):
        return df

    dos_col = _get_col(df, "dostawca")
    nazwa_col = _get_col(df, "nazwa towaru")
    kod_col = _get_col(df, "kod towaru / usługi", "kod towaru")

    mask = pd.Series(True, index=df.index)
    for e in exc.get("suppliers", []):
        v = str(e.get("value", "")).strip()
        if v and dos_col:
            mask &= ~df[dos_col].astype(str).str.contains(v, case=False, na=False)
    for e in exc.get("products", []):
        v = str(e.get("value", "")).strip()
        if not v:
            continue
        hit = pd.Series(False, index=df.index)
        if nazwa_col:
            hit |= df[nazwa_col].astype(str).str.contains(v, case=False, na=False)
        if kod_col:
            hit |= df[kod_col].astype(str).str.contains(v, case=False, na=False)
        mask &= ~hit
    return df[mask]


def query_products(
    analiza: pd.DataFrame,
    status: str | None = None,
    dostawca: str | None = None,
    nazwa_zawiera: str | None = None,
    min_marza: float | None = None,
    max_marza: float | None = None,
    max_dni_do_wyczerpania: float | None = None,
    min_ile_zamowic: int | None = None,
    sort_by: str = "wartosc_zamowienia",
    ascending: bool = False,
    top_n: int = 20,
    include_excluded: bool = False,
) -> str:
    """Filtruje, sortuje i zwraca produkty z analizy jako JSON.

    Domyślnie pomija produkty/dostawców wykluczonych przez Anitę
    (include_excluded=True wyłącza to filtrowanie)."""
    df = analiza.copy()

    if not include_excluded:
        df = _filter_excluded(df)

    if status:
        df = df[df["status"].astype(str).str.strip().str.upper() == status.upper()]

    dos_col = _get_col(df, "dostawca")
    if dostawca and dos_col:
        df = df[df[dos_col].astype(str).str.contains(dostawca, case=False, na=False)]

    nazwa_col = _get_col(df, "nazwa towaru")
    if nazwa_zawiera and nazwa_col:
        df = df[df[nazwa_col].astype(str).str.contains(nazwa_zawiera, case=False, na=False)]

    if min_marza is not None and "marza_pct" in df.columns:
        df = df[df["marza_pct"] >= min_marza]
    if max_marza is not None and "marza_pct" in df.columns:
        df = df[df["marza_pct"] <= max_marza]
    if max_dni_do_wyczerpania is not None and "dni_do_wyczerpania" in df.columns:
        df = df[df["dni_do_wyczerpania"] <= max_dni_do_wyczerpania]
    if min_ile_zamowic is not None and "ile_zamowic" in df.columns:
        df = df[df["ile_zamowic"] >= min_ile_zamowic]

    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=ascending)

    df = df.head(int(top_n))

    if df.empty:
        return json.dumps({"count": 0, "products": [], "info": "Brak produktów spełniających kryteria."}, ensure_ascii=False)

    kod_col = _get_col(df, "kod towaru / usługi", "kod towaru")

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "kod": str(r.get(kod_col, "")) if kod_col else "",
            "nazwa": str(r.get(nazwa_col, "")) if nazwa_col else "",
            "dostawca": str(r.get(dos_col, "")) if dos_col else "",
            "stan": float(r.get("Stan", 0) or 0),
            "w_drodze": float(r.get("w_drodze", 0) or 0),
            "efektywny_stan": float(r.get("efektywny_stan", 0) or 0),
            "srednie_dzienne": round(float(r.get("srednie_dzienne", 0) or 0), 2),
            "dni_do_wyczerpania": round(float(r.get("dni_do_wyczerpania", 0) or 0), 1),
            "ile_zamowic": int(r.get("ile_zamowic", 0) or 0),
            "wartosc_zamowienia": round(float(r.get("wartosc_zamowienia", 0) or 0), 0),
            "wartosc_stanu": round(float(r.get("wartosc_stanu", 0) or 0), 0),
            "marza_pct": round(float(r.get("marza_pct", 0) or 0), 1),
            "stan_min": float(r.get("Stan Min.", 0) or 0),
            "status": str(r.get("status", "")),
            # Gotowe uzasadnienie "dlaczego" — cytuj je Anitcie zamiast wymyślać własne
            "powod": str(r.get("powod", "")),
        })

    suma_zamowienia = round(sum(r["wartosc_zamowienia"] for r in rows), 0)
    return json.dumps(
        {"count": len(rows), "suma_wartosci_zamowienia": suma_zamowienia, "products": rows},
        ensure_ascii=False,
    )


def get_dostawca_summary(analiza: pd.DataFrame, summary: dict, dostawca: str) -> str:
    """Szczegółowy snapshot per dostawca (z minimum logistycznym)."""
    dos_col = _get_col(analiza, "dostawca")
    if not dos_col:
        return json.dumps({"error": "Brak kolumny 'dostawca' w analizie."})
    mask = analiza[dos_col].astype(str).str.contains(dostawca, case=False, na=False)
    df = analiza[mask]
    if df.empty:
        return json.dumps({"error": f"Nie znaleziono dostawcy zawierającego '{dostawca}'."}, ensure_ascii=False)

    aktywne = df[df["srednie_dzienne"] > 0] if "srednie_dzienne" in df.columns else df
    dzis = df[df["status"] == "ZAMÓW DZIŚ"]
    tydzien = df[df["status"] == "ZAMÓW TYDZIEŃ"]
    dead = df[df["status"] == "DEAD STOCK"]

    matched_names = sorted({str(x) for x in df[dos_col].dropna().unique()})

    # Minimum logistyczne: weź najwyższe dopasowane spośród nazw dostawcy
    min_log = (summary or {}).get("min_log", {}) or {}
    minimum = max((match_minimum(min_log, n) for n in matched_names), default=0.0)
    wartosc_teraz = round(
        float(dzis.get("wartosc_zamowienia", pd.Series([0])).sum())
        + float(tydzien.get("wartosc_zamowienia", pd.Series([0])).sum()),
        0,
    )
    brakuje = round(max(0.0, minimum - wartosc_teraz), 0) if minimum else 0.0

    return json.dumps({
        "dopasowani_dostawcy": matched_names,
        "minimum_logistyczne_pln": round(minimum, 0),
        "wartosc_do_zamowienia_teraz": wartosc_teraz,
        "brakuje_do_minimum_pln": brakuje,
        "produktow_total": int(len(df)),
        "produktow_aktywnych": int(len(aktywne)),
        "wartosc_magazynu": round(float(df.get("wartosc_stanu", pd.Series([0])).sum()), 0),
        "wartosc_w_drodze": round(float(df.get("wartosc_w_drodze", pd.Series([0])).sum()), 0),
        "do_zamowienia_dzis": {
            "liczba": int(len(dzis)),
            "wartosc_pln": round(float(dzis.get("wartosc_zamowienia", pd.Series([0])).sum()), 0),
        },
        "do_zamowienia_tydzien": {
            "liczba": int(len(tydzien)),
            "wartosc_pln": round(float(tydzien.get("wartosc_zamowienia", pd.Series([0])).sum()), 0),
        },
        "dead_stock": {
            "liczba": int(len(dead)),
            "wartosc_pln": round(float(dead.get("wartosc_stanu", pd.Series([0])).sum()), 0),
        },
        "srednia_marza_pct": round(float(aktywne["marza_pct"].mean()) if not aktywne.empty and "marza_pct" in aktywne.columns else 0.0, 1),
    }, ensure_ascii=False)


def list_dostawcy(
    analiza: pd.DataFrame,
    sort_by: str = "wartosc_stanu",
    top_n: int = 20,
    only_with_orders: bool = False,
) -> str:
    """Lista dostawców z agregatami."""
    dos_col = _get_col(analiza, "dostawca")
    if not dos_col:
        return json.dumps({"error": "Brak kolumny 'dostawca'."})

    g = analiza.groupby(dos_col).agg(
        produktow=("Stan", "count"),
        aktywnych=("srednie_dzienne", lambda s: int((s > 0).sum())),
        wartosc_stanu=("wartosc_stanu", "sum"),
        wartosc_zamowienia=("wartosc_zamowienia", "sum"),
        wartosc_w_drodze=("wartosc_w_drodze", "sum"),
    ).reset_index()
    g.rename(columns={dos_col: "dostawca"}, inplace=True)

    if only_with_orders:
        g = g[g["wartosc_zamowienia"] > 0]

    if sort_by in g.columns:
        g = g.sort_values(sort_by, ascending=False)
    g = g.head(int(top_n))

    for col in ("wartosc_stanu", "wartosc_zamowienia", "wartosc_w_drodze"):
        if col in g.columns:
            g[col] = g[col].round(0).astype(float)

    return json.dumps({"count": len(g), "dostawcy": g.to_dict(orient="records")}, ensure_ascii=False)


def get_overall_metrics(summary: dict) -> str:
    """Zwraca pełny słownik metryk (z minimami logistycznymi, bez ciężkiej
    mapy supplier_open_orders)."""
    skip = {"supplier_open_orders", "pozycje_indywidualne"}
    return json.dumps(
        {k: v for k, v in summary.items() if k not in skip},
        ensure_ascii=False,
        default=str,
    )


def get_open_orders_for_supplier(summary: dict, dostawca: str) -> str:
    """Zwraca listę OTWARTYCH zamówień u danego dostawcy z numerami dokumentów,
    datami realizacji, wartościami i listą pozycji (SKU) w każdym.

    Używaj GDY pokazujesz Anitcie rekomendację 'zamów X u DOSTAWCA' — żeby
    powiedzieć jej "u tego dostawcy masz już otwarte zamówienie ZAZ/123/2026
    z 5 pozycjami za 12 345 PLN; możesz dorzucić nowe produkty zamiast
    tworzyć osobny dokument".
    """
    mapping = summary.get("supplier_open_orders", {}) or {}
    info = lookup_supplier_open_orders(mapping, dostawca)

    if not info:
        return json.dumps({
            "dostawca": dostawca,
            "ma_otwarte_zamowienia": False,
            "info": f"Brak otwartych zamówień u dostawcy pasującego do '{dostawca}'.",
        }, ensure_ascii=False)

    orders_out = []
    for o in info.get("orders", [])[:20]:
        orders_out.append({
            "nr": o.get("nr", ""),
            "data_realiz": o.get("data_realiz", ""),
            "wartosc_pln": o.get("wartosc", 0),
            "liczba_pozycji": len(o.get("skus", [])),
            "produkty": [
                {
                    "kod": s.get("kod", ""),
                    "nazwa": s.get("nazwa", ""),
                    "ilosc": s.get("ilosc", 0),
                }
                for s in o.get("skus", [])[:10]
            ],
        })
    payload = {
        "dostawca": info.get("nazwa", ""),
        "ma_otwarte_zamowienia": True,
        "liczba_dokumentow": info.get("liczba_dokumentow", 0),
        "laczna_wartosc_pln": info.get("laczna_wartosc", 0),
        "najblizsza_data_realizacji": info.get("najblizsza_data"),
        "kody_skus_w_drodze": (info.get("kody_w_drodze", []) or [])[:50],
        "orders": orders_out,
    }
    return json.dumps(payload, ensure_ascii=False)


def list_suppliers_with_open_orders(summary: dict, top_n: int = 30) -> str:
    """Lista wszystkich dostawców u których są OTWARTE zamówienia,
    posortowana wg wartości. Używaj GDY Anita pyta 'gdzie mam otwarte
    zamówienia' lub 'do którego dostawcy najwięcej jedzie'."""
    mapping = summary.get("supplier_open_orders", {}) or {}
    if not mapping:
        return json.dumps({
            "count": 0,
            "info": "Brak otwartych zamówień u jakiegokolwiek dostawcy.",
        }, ensure_ascii=False)

    rows = []
    for _, info in mapping.items():
        rows.append({
            "dostawca": info.get("nazwa", ""),
            "liczba_dokumentow": info.get("liczba_dokumentow", 0),
            "laczna_wartosc_pln": info.get("laczna_wartosc", 0),
            "najblizsza_data_realizacji": info.get("najblizsza_data"),
            "liczba_skus_w_drodze": len(info.get("kody_w_drodze", [])),
        })
    rows.sort(key=lambda r: r["laczna_wartosc_pln"], reverse=True)
    rows = rows[: int(top_n)]
    return json.dumps({
        "count": len(rows),
        "laczna_wartosc_wszystkich": round(
            sum(r["laczna_wartosc_pln"] for r in rows), 0
        ),
        "dostawcy": rows,
    }, ensure_ascii=False)


def suggest_topup_to_minimum(analiza: pd.DataFrame, summary: dict, dostawca: str) -> str:
    """Gdy zamówienie u dostawcy jest PONIŻEJ minimum logistycznego — proponuje
    dodatkowe pozycje tego samego dostawcy (najpilniejsze wg dni do wyczerpania)
    żeby dobić do minimum. Pomija pozycje już w bieżącym zamówieniu i wykluczone."""
    dos_col = _get_col(analiza, "dostawca")
    if not dos_col:
        return json.dumps({"error": "Brak kolumny 'dostawca' w analizie."})
    mask = analiza[dos_col].astype(str).str.contains(dostawca, case=False, na=False)
    df = _filter_excluded(analiza[mask])
    if df.empty:
        return json.dumps(
            {"error": f"Nie znaleziono dostawcy '{dostawca}' (lub jest wykluczony)."},
            ensure_ascii=False,
        )

    matched_names = sorted({str(x) for x in df[dos_col].dropna().unique()})
    min_log = (summary or {}).get("min_log", {}) or {}
    minimum = max((match_minimum(min_log, n) for n in matched_names), default=0.0)
    if not minimum:
        return json.dumps({
            "dostawca": matched_names[0] if matched_names else dostawca,
            "minimum_logistyczne_pln": 0,
            "info": "Brak zdefiniowanego minimum logistycznego dla tego dostawcy.",
        }, ensure_ascii=False)

    juz = df[df["status"].isin(["ZAMÓW DZIŚ", "ZAMÓW TYDZIEŃ"])]
    wartosc_teraz = float(juz["wartosc_zamowienia"].sum())
    brakuje = minimum - wartosc_teraz

    base = {
        "dostawca": matched_names[0] if matched_names else dostawca,
        "minimum_logistyczne_pln": round(minimum, 0),
        "wartosc_do_zamowienia_teraz": round(wartosc_teraz, 0),
    }
    if brakuje <= 0:
        base.update({"minimum_osiagniete": True, "brakuje_do_minimum_pln": 0, "propozycje": []})
        return json.dumps(base, ensure_ascii=False)

    nazwa_col = _get_col(df, "nazwa towaru")
    kod_col = _get_col(df, "kod towaru / usługi", "kod towaru")
    cena_col = _get_col(df, "cena zakupu")

    cand = df[(~df.index.isin(set(juz.index))) & (df["srednie_dzienne"] > 0)].copy()
    if "dni_do_wyczerpania" in cand.columns:
        cand = cand.sort_values("dni_do_wyczerpania")

    propozycje: list[dict] = []
    dodane = 0.0
    for _, r in cand.iterrows():
        sd = float(r.get("srednie_dzienne", 0) or 0)
        ile = int(r.get("ile_zamowic", 0) or 0)
        if ile <= 0:
            ile = int(round(sd * 30)) or 1  # ~miesięczny zapas
        unit = float(r.get(cena_col, 0) or 0) if cena_col else 0.0
        if unit <= 0:
            stan = float(r.get("Stan", 0) or 0)
            ws = float(r.get("wartosc_stanu", 0) or 0)
            unit = ws / stan if stan > 0 and ws > 0 else 0.0
        val = round(ile * unit, 0)
        if val <= 0:
            continue
        propozycje.append({
            "kod": str(r.get(kod_col, "")) if kod_col else "",
            "nazwa": str(r.get(nazwa_col, "")) if nazwa_col else "",
            "ile_dorzucic": ile,
            "wartosc_pln": val,
            "dni_do_wyczerpania": round(float(r.get("dni_do_wyczerpania", 0) or 0), 1),
            "srednie_dzienne": round(sd, 2),
            "powod": str(r.get("powod", "")),
        })
        dodane += val
        if wartosc_teraz + dodane >= minimum:
            break

    base.update({
        "minimum_osiagniete": False,
        "brakuje_do_minimum_pln": round(max(0.0, brakuje), 0),
        "suma_propozycji_pln": round(dodane, 0),
        "po_dorzuceniu_pln": round(wartosc_teraz + dodane, 0),
        "minimum_po_dorzuceniu_ok": (wartosc_teraz + dodane) >= minimum,
        "propozycje": propozycje,
    })
    return json.dumps(base, ensure_ascii=False)


# ── Tool definitions for OpenAI ───────────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_products",
            "description": (
                "Filtruje, sortuje i zwraca konkretne produkty z bieżącej analizy magazynowej. "
                "Używaj zawsze gdy potrzebujesz konkretnych pozycji (np. 'co u BIACHEM do zamówienia', "
                "'produkty z marżą poniżej 20%', 'top 10 dead-stocku'). "
                "Zwraca JSON z polami: count, suma_wartosci_zamowienia, products[]."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["ZAMÓW DZIŚ", "ZAMÓW TYDZIEŃ", "OK", "DEAD STOCK", "JEDNORAZÓWKA", "NIEAKTYWNY"],
                    },
                    "dostawca": {"type": "string", "description": "Fragment nazwy dostawcy (case-insensitive)"},
                    "nazwa_zawiera": {"type": "string", "description": "Fragment nazwy produktu"},
                    "min_marza": {"type": "number"},
                    "max_marza": {"type": "number"},
                    "max_dni_do_wyczerpania": {"type": "number"},
                    "min_ile_zamowic": {"type": "integer"},
                    "sort_by": {
                        "type": "string",
                        "enum": [
                            "wartosc_zamowienia", "ile_zamowic", "dni_do_wyczerpania",
                            "srednie_dzienne", "marza_pct", "wartosc_stanu", "Stan", "w_drodze",
                        ],
                        "default": "wartosc_zamowienia",
                    },
                    "ascending": {"type": "boolean", "default": False},
                    "top_n": {"type": "integer", "default": 20, "maximum": 100},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dostawca_summary",
            "description": (
                "Szczegółowe podsumowanie per dostawca: ilość produktów, wartość magazynu, "
                "ile w drodze, do zamówienia dziś/w tygodniu, dead stock, średnia marża, "
                "ORAZ minimum logistyczne (minimum_logistyczne_pln, wartosc_do_zamowienia_teraz, "
                "brakuje_do_minimum_pln)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dostawca": {"type": "string", "description": "Nazwa lub fragment nazwy dostawcy"},
                },
                "required": ["dostawca"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_dostawcy",
            "description": "Listuje wszystkich dostawców posortowanych wg metryki (np. wartość magazynu, do zamówienia).",
            "parameters": {
                "type": "object",
                "properties": {
                    "sort_by": {
                        "type": "string",
                        "enum": ["wartosc_stanu", "wartosc_zamowienia", "wartosc_w_drodze", "produktow", "aktywnych"],
                        "default": "wartosc_stanu",
                    },
                    "top_n": {"type": "integer", "default": 20},
                    "only_with_orders": {
                        "type": "boolean",
                        "default": False,
                        "description": "True = tylko dostawcy u których jest coś do zamówienia",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_overall_metrics",
            "description": "Zwraca pełne metryki agregatu (wartość magazynu, dead stock, w drodze, daty itp.).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_open_orders_for_supplier",
            "description": (
                "Zwraca listę OTWARTYCH zamówień u danego dostawcy: numery dokumentów "
                "(np. ZAZ/123/2026), daty realizacji, wartości i pozycje (SKU) w każdym. "
                "WYWOŁUJ AUTOMATYCZNIE GDY rekomendujesz zamówienie czegoś u konkretnego "
                "dostawcy — żeby powiedzieć Anitcie 'u tego dostawcy masz już otwarte "
                "zamówienia X, Y — możesz dorzucić nowe pozycje zamiast tworzyć osobny "
                "dokument'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dostawca": {
                        "type": "string",
                        "description": "Nazwa lub fragment nazwy dostawcy (case-insensitive)",
                    },
                },
                "required": ["dostawca"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_suppliers_with_open_orders",
            "description": (
                "Listuje WSZYSTKICH dostawców u których są otwarte zamówienia — "
                "liczba dokumentów, łączna wartość, najbliższa data realizacji. "
                "Posortowane wg wartości."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "default": 30, "maximum": 100},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_preference",
            "description": (
                "Zapisuje preferencję Anity do pamięci długoterminowej. Używaj GDY ANITA POWIE "
                "'zapamiętaj że...', 'preferuję...', 'ustaw minimum...' lub gdy z rozmowy widać "
                "powtarzający się wzorzec wartościowy do zapamiętania."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "Krótki klucz, np. 'minimum_BIACHEM_PLN'"},
                    "value": {"type": "string", "description": "Wartość preferencji"},
                },
                "required": ["key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_fact",
            "description": (
                "Dodaje fakt o firmie / sposobie pracy Anity do pamięci. Używaj gdy uczysz się "
                "czegoś nowego (np. 'Anita zamawia u BIACHEM tylko w poniedziałki' albo "
                "'minimum logistyczne ADEKS = 3000 PLN'). Reguły czasowe/warunkowe "
                "('tylko w poniedziałki', 'pomijaj sezonowe latem') zapisuj właśnie tutaj."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "fact": {"type": "string"},
                },
                "required": ["fact"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "suggest_topup_to_minimum",
            "description": (
                "Gdy zamówienie u dostawcy jest PONIŻEJ minimum logistycznego — "
                "zwraca propozycje DODATKOWYCH pozycji tego dostawcy (najpilniejsze), "
                "żeby dobić do minimum. WYWOŁUJ GDY rekomendujesz zamówienie u dostawcy, "
                "u którego wartość zamówienia nie osiąga minimum logistycznego — żeby "
                "powiedzieć Anitcie 'brakuje X PLN do minimum, dorzuć: …'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dostawca": {"type": "string", "description": "Nazwa lub fragment nazwy dostawcy"},
                },
                "required": ["dostawca"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_exclusion",
            "description": (
                "Trwale WYKLUCZA produkt lub dostawcę z rekomendacji. Używaj GDY ANITA POWIE "
                "'nie zamawiaj X', 'pomijaj dostawcę Y', 'wyklucz produkt Z'. Wykluczenia są "
                "respektowane automatycznie przy kolejnych rekomendacjach."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["products", "suppliers"]},
                    "value": {"type": "string", "description": "Fragment nazwy/kodu produktu lub nazwa dostawcy"},
                    "reason": {"type": "string", "description": "Opcjonalny powód (np. 'wycofany', 'sezonowy')"},
                },
                "required": ["kind", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_exclusion",
            "description": "Cofa wcześniejsze wykluczenie produktu/dostawcy. Używaj gdy Anita mówi 'znów zamawiaj X'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["products", "suppliers"]},
                    "value": {"type": "string"},
                },
                "required": ["kind", "value"],
            },
        },
    },
]


# ── Tool definitions for Anthropic (wygenerowane z TOOLS, format Messages API) ─
# Anthropic używa płaskiego schematu: {name, description, input_schema}
# (bez wrappera "function", parameters→input_schema).
ANTHROPIC_TOOLS = [
    {
        "name": t["function"]["name"],
        "description": t["function"]["description"],
        "input_schema": t["function"].get("parameters", {"type": "object", "properties": {}}),
    }
    for t in TOOLS
]


def _execute_tool(name: str, args: dict, analiza: pd.DataFrame, summary: dict) -> str:
    try:
        if name == "query_products":
            return query_products(analiza, **args)
        if name == "get_dostawca_summary":
            return get_dostawca_summary(analiza, summary, **args)
        if name == "list_dostawcy":
            return list_dostawcy(analiza, **args)
        if name == "get_overall_metrics":
            return get_overall_metrics(summary)
        if name == "get_open_orders_for_supplier":
            return get_open_orders_for_supplier(summary, **args)
        if name == "list_suppliers_with_open_orders":
            return list_suppliers_with_open_orders(summary, **args)
        if name == "suggest_topup_to_minimum":
            return suggest_topup_to_minimum(analiza, summary, **args)
        if name == "save_preference":
            save_preference(args["key"], args["value"])
            return json.dumps({"ok": True, "saved": {args["key"]: args["value"]}}, ensure_ascii=False)
        if name == "add_fact":
            add_fact(args["fact"])
            return json.dumps({"ok": True, "added_fact": args["fact"]}, ensure_ascii=False)
        if name == "add_exclusion":
            add_exclusion(args["kind"], args["value"], args.get("reason"))
            return json.dumps({"ok": True, "excluded": {args["kind"]: args["value"]}}, ensure_ascii=False)
        if name == "remove_exclusion":
            remove_exclusion(args["kind"], args["value"])
            return json.dumps({"ok": True, "removed": {args["kind"]: args["value"]}}, ensure_ascii=False)
        return json.dumps({"error": f"Nieznane narzędzie: {name}"})
    except TypeError as e:
        return json.dumps({"error": f"Złe argumenty {name}: {e}"})
    except Exception as e:
        return json.dumps({"error": f"Błąd wywołania {name}: {e}"})


# ── Główna funkcja agenta ─────────────────────────────────────────────────────

DEFAULT_SYSTEM_PROMPT_TEMPLATE = """Jesteś pełnoprawnym AGENTEM AI dla Anity — kupca firmy Add All
(dystrybutor chemii, opakowań i artykułów higienicznych dla HoReCa).

TWOJA FILOZOFIA PRACY:
- Jesteś agentem, nie czat-botem. Aktywnie korzystasz z NARZĘDZI poniżej żeby
  uzyskać świeże, konkretne dane z bieżącej analizy magazynowej.
- NIGDY nie odpowiadaj "tej informacji nie ma w analizie" zanim nie spróbujesz
  użyć narzędzia query_products / get_dostawca_summary / list_dostawcy. Statyczny
  kontekst pokazuje TYLKO podsumowanie — pełnie dane są dostępne przez narzędzia.
- Pamiętaj historię rozmowy i ucz się preferencji Anity (save_preference, add_fact).
- Możesz wywołać kilka narzędzi w łańcuchu (np. najpierw list_dostawcy, potem
  query_products dla wybranego). Nie żałuj sobie iteracji — to robi cię lepszym.

KIEDY UŻYWAĆ NARZĘDZI:
- Pytanie o KONKRETNEGO dostawcę → get_dostawca_summary("BIACHEM")
- Pytanie o produkty z FILTREM (status, marża, top N) → query_products
- "Który dostawca ma najwięcej produktów krytycznych?" →
  list_dostawcy(sort_by="wartosc_zamowienia", only_with_orders=True)
- Pytanie o ogólne metryki / liczby → get_overall_metrics
- "Gdzie mam otwarte zamówienia?" → list_suppliers_with_open_orders
- Polecasz coś u dostawcy X → ZAWSZE wywołaj get_open_orders_for_supplier("X")
  żeby sprawdzić czy są otwarte dokumenty (możliwość dorzucenia)
- Zamówienie u dostawcy poniżej minimum logistycznego → suggest_topup_to_minimum("X")
- Anita mówi "zapamiętaj że..." / nowy nawyk → save_preference / add_fact
- Anita mówi "nie zamawiaj X" / "pomijaj dostawcę Y" → add_exclusion
- Anita mówi "znów zamawiaj X" → remove_exclusion

ZASADY ODPOWIEDZI — to są ZAŁOŻENIA KRYTYCZNE NIE PRZEKRACZALNE:
- Po polsku, konkretnie, przyjaźnie. Anita nie znosi laniem wody.
- Liczby: '12 345 PLN' (spacja jako separator tysięcy).
- ⛔ FORMAT: odpowiadaj WYŁĄCZNIE czystym tekstem i Markdownem. NIGDY nie używaj
  HTML ani XML — żadnych tagów w nawiasach ostrokątnych (np. <table>, <div>, <b>,
  <br>, <p>, <answer>, <thinking>). Tabele rób w Markdownie (| Kol | Kol |),
  pogrubienie przez **gwiazdki**, listy przez „- ". Interfejs Anity renderuje
  Markdown, więc każdy tag HTML/XML pokaże jej się jako brzydki surowy kod.

⚠️ NAZWA DOSTAWCY przy KAŻDEJ rekomendacji zakupowej:
- ZAWSZE — bezwzględnie — podaj NAZWĘ DOSTAWCY przy każdej pozycji
  do zamówienia. Format: "**[NAZWA DOSTAWCY]** — produkt X (Y szt., Z PLN)".
- Grupuj rekomendacje PER DOSTAWCA (nie liniowo) — Anita zamawia DOKUMENTAMI
  do firm, więc widok "u BIACHEM zamów A,B,C; u JS zamów D,E" jest 10x
  bardziej użyteczny niż lista pomieszana.
- Przy każdym dostawcy SPRAWDŹ czy ma już OTWARTE ZAMÓWIENIA
  (get_open_orders_for_supplier). Jeśli TAK — POWIEDZ to wyraźnie:
  "🚚 U dostawcy X masz JUŻ otwarte: ZAZ/123/2026 (do 12.05) na 5 678 PLN
  z 3 pozycjami; możesz **dorzucić** te nowe SKU zamiast tworzyć nowy dokument."
  TO POZWALA ANITCIE NIE DUBLOWAĆ WYSIŁKU.
- Sortuj rekomendacje wg priorytetu: ZAMÓW DZIŚ → ZAMÓW TYDZIEŃ → reszta.
- Przy każdej pozycji ZAWSZE wspomnij ile jest "w drodze" jeśli > 0.

⚠️ MINIMA LOGISTYCZNE (sztywno ustalone z dostawcami):
- Każdy dostawca ma minimum logistyczne (kwotę, poniżej której nie warto/nie można
  złożyć zamówienia). Przy rekomendacji per dostawca ZAWSZE podaj status minimum:
  "✅ minimum OK (3 200 / 3 000 PLN)" albo "⚠️ brakuje 800 PLN do minimum 3 000 PLN".
- Gdy zamówienie u dostawcy jest PONIŻEJ minimum → wywołaj suggest_topup_to_minimum
  i zaproponuj Anitcie konkretne DOZAMÓWIENIE: "żeby dobić do minimum 3 000 PLN,
  dorzuć: Produkt A (40 szt, 520 PLN), Produkt B (30 szt, 300 PLN)". Te pozycje i tak
  niedługo trzeba zamówić — to tańsze niż osobna dostawa.

⚠️ JESTEŚ PERSONALNYM AGENTEM ANITY — jej słowo jest dla Ciebie WIĄŻĄCE:
- Sekcja "PAMIĘĆ O ANITCIE" niżej (preferencje, fakty/reguły, wykluczenia) to
  TWARDE WYTYCZNE — mają PIERWSZEŃSTWO przed Twoimi domyślnymi rekomendacjami.
  Zanim cokolwiek polecisz, sprawdź pamięć i ZASTOSUJ ją. Jeśli reguła Anity stoi
  w sprzeczności z domyślną logiką — wygrywa reguła Anity.
- WYKLUCZENIA (produkty/dostawcy) — NIGDY ich nie rekomenduj. Kropka.
- REGUŁY/NAWYKI (np. "u BIACHEM tylko w poniedziałki") — stosuj zawsze.
- ⚡ ZAPISYWANIE jest OBOWIĄZKOWE i NATYCHMIASTOWE: gdy Anita poda preferencję,
  regułę, minimum, nawyk, albo powie „zapamiętaj / nie zamawiaj / pomijaj / ustaw" —
  od razu w TEJ SAMEJ turze wywołaj właściwe narzędzie (save_preference / add_fact /
  add_exclusion) ZANIM napiszesz odpowiedź. Nie zakładaj, że „zapamiętasz to w głowie"
  — pamięć jest tylko w tych narzędziach. Po zapisie krótko potwierdź: „✅ Zapisałem:
  …". Jeśli nie masz pewności którego narzędzia użyć — użyj add_fact (lepiej zapisać
  niż zgubić).

⚠️ WYPRZEDANE ≠ „OK":
- Produkt, który się sprzedawał, a ma teraz STAN 0 (i nic nie jedzie), to pozycja
  DO ZAMÓWIENIA — nawet jeśli ma niską rotację albo NIE ma ustawionego minimum
  magazynowego (minimum = 0 to najczęściej brak konfiguracji, a NIE sygnał „ok").
- NIGDY nie tłumacz pominięcia produktu tym, że „ma minimum 0, więc zgadza się ze
  stanem". Silnik oznacza takie pozycje statusem ZAMÓW DZIŚ i polem `powod`
  = „WYPRZEDANE…" — pokaż je Anitcie i zaproponuj uzupełnienie (pole ile_zamowic).

⚠️ PRODUKTY INDYWIDUALNE (grupa "INDYWIDUALNE") — POMIJAJ:
- Anita trzyma w iBiznes grupę produktów indywidualnych/klienckich (grupa
  "INDYWIDUALNE"). Tych pozycji silnik JUŻ nie wpuszcza do analizy — więc nie
  pojawią się w query_products ani w rekomendacjach. To jest zamierzone.
- NIE proponuj ich do zamówienia i NIE dobijaj nimi minimów logistycznych.
- Liczbę pominiętych pozycji znajdziesz w metrykach (`pozycje_indywidualne_liczba`,
  `grupy_wykluczone`). Jeśli Anita pyta „czemu nie widzę produktu X" — może być w
  tej grupie; wyjaśnij, że produkty z grupy INDYWIDUALNE są celowo pomijane.

⚠️ ZAWSZE PODAJ "DLACZEGO" przy każdej rekomendacji:
- Każda pozycja "do zamówienia" MUSI mieć krótkie uzasadnienie. Narzędzie
  query_products zwraca gotowe pole `powod` (np. "schodzi 3,0 szt/dzień; zapasu
  na ~2 dni; stan 5 poniżej minimum 25") — CYTUJ JE, nie wymyślaj własnych liczb.
- Dobra rekomendacja brzmi: "**HIGA** — Ręcznik ZZ (zamów 80 szt, 1 104 PLN):
  schodzi 3 szt/dz, zapasu na 2 dni, poniżej minimum." Anita ma od razu wiedzieć
  ILE, ZA ILE i DLACZEGO.
- Jeśli coś już jedzie (`w_drodze` > 0) — wyjaśnij, że dlatego NIE zamawiasz albo
  zamawiasz mniej (efektywny_stan = stan + w drodze).

POZOSTAŁE ZASADY:
- Listy wypunktowane gdy pomagają. Tabele markdownowe dla 5+ pozycji.
- Cytuj liczby DOKŁADNIE jak narzędzia zwracają — bez zaokrąglania w głowie.
- Jeśli używasz narzędzia, krótko wspomnij co znalazłeś (np. "Sprawdziłem
  query_products z filtrem...") — to buduje zaufanie do liczb.

════════ PAMIĘĆ O ANITCIE — TWARDE WYTYCZNE (stosuj je, mają pierwszeństwo) ════════
{memory_text}
(Powyższe to wiążące ustalenia Anity z poprzednich sesji. Zastosuj je w odpowiedzi.)

=== STATYCZNY SNAPSHOT ANALIZY (dla orientacji — szczegóły bierz z narzędzi) ===
{context}
=== KONIEC SNAPSHOTU ==="""


# Tagi HTML, które czyścimy z odpowiedzi (siatka bezpieczeństwa — gdyby model
# mimo instrukcji wstawił HTML/XML; nie tknie zwykłego "a < b" w tekście).
_HTML_TAG_RE = re.compile(
    r"</?(?:table|thead|tbody|tfoot|tr|td|th|div|p|br|hr|b|strong|i|em|u|s|"
    r"ul|ol|li|span|h[1-6]|pre|code|a|font|center|small|blockquote|figure)\b[^>]*>",
    re.IGNORECASE,
)


def _sanitize_answer(text: str | None) -> str | None:
    """Usuwa ewentualne tagi HTML/XML i odkodowuje encje, żeby Anita nigdy nie
    zobaczyła surowego <table>/<br> itp. Zwykły tekst i Markdown zostają nietknięte."""
    if not text:
        return text
    cleaned = _HTML_TAG_RE.sub("", text)
    if cleaned != text:
        cleaned = _html.unescape(cleaned)
    return cleaned


def _build_system_prompt(context: str, extra_system_instructions: str | None) -> str:
    system_prompt = DEFAULT_SYSTEM_PROMPT_TEMPLATE.format(
        memory_text=memory_summary_text(),
        context=context,
    )
    if extra_system_instructions:
        system_prompt += f"\n\n=== DODATKOWE INSTRUKCJE OD ANITY ===\n{extra_system_instructions}"
    return system_prompt


def _history_messages(chat_history: list[dict] | None) -> list[dict]:
    out: list[dict] = []
    if chat_history:
        for msg in chat_history[-8:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                out.append({"role": msg["role"], "content": msg["content"]})
    return out


def _ask_openai(question, analiza, summary, system_prompt, history_msgs, api_key, model, max_iters):
    try:
        from openai import OpenAI
    except ImportError as e:
        return None, [], f"Brak biblioteki openai: {e}"

    client = OpenAI(api_key=api_key)
    messages: list[dict] = [{"role": "system", "content": system_prompt}]
    messages.extend(history_msgs)
    messages.append({"role": "user", "content": question})

    tool_log: list[dict] = []
    answer: str | None = None
    last_error: str | None = None

    for iteration in range(max_iters):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
                temperature=0.2,
            )
        except Exception as e:
            last_error = f"OpenAI API ({model}): {e}"
            break

        msg = response.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []

        if tool_calls:
            messages.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in tool_calls
                ],
            })

            for tc in tool_calls:
                name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}
                result = _execute_tool(name, args, analiza, summary)
                tool_log.append({
                    "iteration": iteration + 1,
                    "name": name,
                    "args": args,
                    "result_preview": (result[:300] + "…") if len(result) > 300 else result,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
            continue

        answer = msg.content or ""
        if not answer.strip():
            last_error = (
                f"Model {model} zwrócił pustą odpowiedź "
                f"(finish_reason={response.choices[0].finish_reason})."
            )
            answer = None
        break

    return answer, tool_log, last_error


def _ask_anthropic(question, analiza, summary, system_prompt, history_msgs, api_key, model, max_iters):
    try:
        import anthropic
    except ImportError as e:
        return None, [], f"Brak biblioteki anthropic: {e}"

    client = anthropic.Anthropic(api_key=api_key)
    messages: list[dict] = list(history_msgs)
    messages.append({"role": "user", "content": question})

    tool_log: list[dict] = []
    answer: str | None = None
    last_error: str | None = None

    for iteration in range(max_iters):
        base_kwargs = dict(
            model=model,
            # UWAGA: max_tokens to wspólny budżet na myślenie (thinking) I na tekst
            # odpowiedzi. Przy zbyt małym limicie myślenie zjada całość i wraca
            # odpowiedź BEZ bloku tekstowego (stop_reason="max_tokens") — czyli
            # z punktu widzenia Anity "bot nie odpisuje". 16k jest bezpieczne
            # bez streamingu (wyżej grozi timeout HTTP w SDK).
            max_tokens=16000,
            system=system_prompt,
            tools=ANTHROPIC_TOOLS,
            messages=messages,
        )
        try:
            # output_config/effort + adaptive thinking są zalecane dla Sonnet 4.6;
            # gdyby SDK był starszy i ich nie znał — spróbuj bez nich.
            try:
                response = client.messages.create(
                    **base_kwargs,
                    output_config={"effort": "medium"},
                    thinking={"type": "adaptive"},
                )
            except TypeError:
                response = client.messages.create(**base_kwargs)
        except Exception as e:
            last_error = f"Anthropic API ({model}): {e}"
            break

        tool_uses = [b for b in response.content if getattr(b, "type", None) == "tool_use"]

        if response.stop_reason == "tool_use" and tool_uses:
            # Dopisz całą odpowiedź asystenta (z blokami thinking/tool_use) bez zmian.
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tu in tool_uses:
                args = tu.input if isinstance(tu.input, dict) else {}
                result = _execute_tool(tu.name, args, analiza, summary)
                tool_log.append({
                    "iteration": iteration + 1,
                    "name": tu.name,
                    "args": args,
                    "result_preview": (result[:300] + "…") if len(result) > 300 else result,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})
            continue

        answer = "".join(
            b.text for b in response.content if getattr(b, "type", None) == "text"
        )
        if not answer.strip():
            # Model nic nie napisał — najczęściej myślenie wyczerpało max_tokens.
            # Bez tego komunikatu Anita widzi po prostu pustą wiadomość.
            last_error = (
                f"Model {model} zwrócił pustą odpowiedź "
                f"(stop_reason={response.stop_reason}). "
                "Jeśli stop_reason=max_tokens — zwiększ max_tokens albo zadaj węższe pytanie."
            )
            answer = None
        break

    return answer, tool_log, last_error


def ask_agent(
    question: str,
    analiza: pd.DataFrame,
    summary: dict,
    context: str,
    api_key: str,
    model: str = "claude-sonnet-4-6",
    chat_history: list[dict] | None = None,
    max_iters: int = 6,
    extra_system_instructions: str | None = None,
) -> tuple[str | None, list[dict], str | None]:
    """Główna funkcja: zapytaj agenta o cokolwiek, on sam pociągnie dane.

    Dyspozytor: model `claude*` → Anthropic Messages API, reszta → OpenAI.

    Returns:
        (answer, tool_call_log, error)
        answer = treść odpowiedzi (str) lub None gdy błąd
        tool_call_log = lista wywołań narzędzi (do diagnostyki)
        error = komunikat błędu lub None gdy OK
    """
    system_prompt = _build_system_prompt(context, extra_system_instructions)
    history_msgs = _history_messages(chat_history)

    provider = _ask_anthropic if str(model).lower().startswith("claude") else _ask_openai
    answer, tool_log, last_error = provider(
        question, analiza, summary, system_prompt, history_msgs, api_key, model, max_iters
    )

    if answer is None and last_error:
        return None, tool_log, last_error

    if answer is None:
        return (
            f"⚠️ Agent nie zakończył odpowiedzi w {max_iters} iteracjach narzędzi. "
            "Spróbuj zadać bardziej konkretne pytanie.",
            tool_log,
            None,
        )

    answer = _sanitize_answer(answer)
    if not (answer or "").strip():
        # Ostatnia siatka bezpieczeństwa: nigdy nie oddawaj pustego tekstu do UI.
        return None, tool_log, f"Model {model} nie zwrócił żadnej treści do wyświetlenia."

    add_history(question, answer)
    return answer, tool_log, None
