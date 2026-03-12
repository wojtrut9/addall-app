"""
ibiznes_connector.py — Add All iBiznes MySQL Connector

Łączy się bezpośrednio z bazą MySQL iBiznes (tak samo jak CRM).
Zwraca DataFrames z kolumnami identycznymi jak eksport CSV z iBiznes
— engine.py nie wymaga żadnych zmian.

Zmienna środowiskowa: IBIZNES_DB_URL = "mysql://user:pass@host:port/dbname"
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from urllib.parse import urlparse

import pandas as pd
import pymysql
import pymysql.cursors


# ── Połączenie ────────────────────────────────────────────────────────────────

def _parse_url(url: str) -> dict:
    """Parsuje mysql://user:pass@host:port/dbname na słownik parametrów."""
    p = urlparse(url)
    return {
        "host":     p.hostname,
        "port":     p.port or 3306,
        "user":     p.username,
        "password": p.password,
        "database": p.path.lstrip("/"),
        "charset":  "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "connect_timeout": 15,
    }


def get_connection(db_url: str) -> pymysql.Connection:
    """Otwiera połączenie z MySQL iBiznes."""
    params = _parse_url(db_url)
    return pymysql.connect(**params)


def test_connection(db_url: str) -> tuple[bool, str]:
    """Testuje połączenie. Zwraca (sukces, komunikat)."""
    try:
        conn = get_connection(db_url)
        conn.ping()
        conn.close()
        return True, "Połączenie z iBiznes OK"
    except Exception as e:
        return False, f"Błąd połączenia: {e}"


# ── Odkrywanie tabel ──────────────────────────────────────────────────────────

def discover_tables(conn: pymysql.Connection) -> list[str]:
    """Zwraca listę wszystkich tabel w bazie iBiznes."""
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    return [list(r.values())[0] for r in rows]


def get_columns(conn: pymysql.Connection, table: str) -> list[str]:
    """Zwraca listę kolumn danej tabeli."""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        return [row["Field"] for row in cur.fetchall()]


def _find_table(tables: list[str], *patterns: str) -> str | None:
    """Znajdź pierwszą tabelę pasującą do któregokolwiek wzorca (case-insensitive)."""
    for pattern in patterns:
        for t in tables:
            if pattern.lower() in t.lower():
                return t
    return None


def identify_tables(conn: pymysql.Connection) -> dict[str, str | None]:
    """
    Identyfikuje nazwy kluczowych tabel iBiznes.
    Zwraca słownik: {
        'spec_spzoo':    nazwa tabeli specyfikacji sp. z o.o.,
        'spec_firma':    nazwa tabeli specyfikacji JDG,
        'klienci_spzoo': nazwa tabeli klientów sp. z o.o.,
        'klienci_firma': nazwa tabeli klientów JDG,
        'towary_spzoo':  nazwa tabeli towarów sp. z o.o.,
        'towary_firma':  nazwa tabeli towarów JDG,
        'zam_spzoo':     nazwa tabeli zamówień sp. z o.o.,
        'zam_firma':     nazwa tabeli zamówień JDG,
    }
    """
    tables = discover_tables(conn)

    # Rozdziel tabele na prefix "addall*" (sp. z o.o.) i "firma*" (JDG)
    spzoo = [t for t in tables if t.lower().startswith("addall")]
    firma = [t for t in tables if t.lower().startswith("firma")]

    return {
        # Ruchy magazynowe (WZ/PZ) — znane z CRM
        "spec_spzoo": _find_table(spzoo, "spec"),
        "spec_firma":  _find_table(firma, "spec"),
        # Klienci — znane z CRM
        "klienci_spzoo": _find_table(spzoo, "klienci"),
        "klienci_firma":  _find_table(firma, "klienci"),
        # Kartoteka towarów — typowe nazwy iBiznes
        "towary_spzoo": _find_table(spzoo, "towar", "kartot", "indeks", "artykul"),
        "towary_firma":  _find_table(firma, "towar", "kartot", "indeks", "artykul"),
        # Zamówienia do dostawców
        "zam_spzoo": _find_table(spzoo, "zam", "zamow", "order", "pz", "zakup"),
        "zam_firma":  _find_table(firma, "zam", "zamow", "order", "pz", "zakup"),
        # Wszystkie tabele (do debugowania)
        "_all_tables": tables,
    }


# ── Mapowanie kolumn MySQL → nazwy CSV (których oczekuje engine.py) ────────────

# Możliwe nazwy kolumny "Kod towaru" w różnych tabelach iBiznes
_KOD_HINTS    = ["Symbol", "KodT", "Kod", "Indeks", "Towar", "SKU"]
_NAZWA_HINTS  = ["Nazwa", "NazwaT", "Opis", "Towar"]
_STAN_HINTS   = ["Stan", "Ilosc", "IloscMag", "Zapas", "IlDost"]
_CENA_Z_HINTS = ["CenaZ", "CenaZak", "CenaKup", "Cb", "CenaZakupu"]
_CENA_S_HINTS = ["CenaSp", "CenaPodst", "CenaS", "Cs", "CenaSprzedazy"]
_STAN_MIN_HINTS = ["StanMin", "MinStan", "Minimum", "MinIlosc"]
_DOSTAWCA_HINTS = ["Dostawca", "Supplier", "Kontrahent"]
_GRUPA_HINTS  = ["Grupa", "Kategoria", "Klasa", "Typ"]
_JM_HINTS     = ["Jm", "JM", "JedMiary", "Jednostka"]


def _pick_col(available: list[str], *hints: str) -> str | None:
    """Wybiera pierwszą pasującą kolumnę z dostępnych."""
    for hint in hints:
        for col in available:
            if hint.lower() == col.lower():
                return col
    # Luźne dopasowanie (zawiera)
    for hint in hints:
        for col in available:
            if hint.lower() in col.lower():
                return col
    return None


def _q(conn: pymysql.Connection, sql: str, params=()) -> pd.DataFrame:
    """Wykonuje zapytanie SQL i zwraca DataFrame."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Fetch: Obroty magazynowe ──────────────────────────────────────────────────

def fetch_obroty(
    conn: pymysql.Connection,
    tbl_info: dict,
    days: int = 90,
) -> pd.DataFrame:
    """
    Pobiera ruchy magazynowe (WZ + PZ) z ostatnich N dni.
    Zwraca DataFrame z kolumnami jak eksport CSV z iBiznes:
    Typ | Data wydania | Kod towaru | Nazwa towaru | Klient |
    Rozchód | Przychód | Wartość netto | Zysk | Cena netto PLN | Cena zakupu PLN
    """
    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    frames = []

    for tbl_key in ("spec_spzoo", "spec_firma"):
        tbl = tbl_info.get(tbl_key)
        if not tbl:
            continue

        cols = get_columns(conn, tbl)
        kod_col   = _pick_col(cols, *_KOD_HINTS) or "Symbol"
        nazwa_col = _pick_col(cols, *_NAZWA_HINTS) or "Nazwa"
        jm_col    = _pick_col(cols, *_JM_HINTS)
        cs_col    = _pick_col(cols, *_CENA_S_HINTS)  # cena sprzedaży

        # Kolumny obowiązkowe
        required = ["NrR", "Alias", "Data", "Typ", "Il", "Cb"]
        missing  = [c for c in required if c not in cols]
        if missing:
            # Spróbuj inne warianty
            alt_map = {"Il": ["Ilosc", "Qty", "Quantity"], "Cb": ["Cena", "CenaZ", "Price"]}
            for m in missing:
                found = _pick_col(cols, *(alt_map.get(m, [m])))
                if found:
                    required[required.index(m)] = found

        select_parts = [
            f"`Typ`",
            f"`Data` AS `Data wydania`",
            f"`{kod_col}` AS `Kod towaru`",
            f"`{nazwa_col}` AS `Nazwa towaru`",
            f"`Alias` AS `Klient`",
            # Rozchód = ilość przy WZ, 0 dla PZ
            "CASE WHEN `Typ` = 'WZ' THEN `Il` ELSE 0 END AS `Rozchód`",
            # Przychód = ilość przy PZ, 0 dla WZ
            "CASE WHEN `Typ` = 'PZ' THEN `Il` ELSE 0 END AS `Przychód`",
            # Wartość netto = ilość × cena sprzedaży (lub zakupu jeśli brak)
            f"ROUND(`Il` * {f'`{cs_col}`' if cs_col else '`Cb`'}, 2) AS `Wartość netto`",
            # Zysk = (cena sprzedaży - cena zakupu) × ilość
            (
                f"ROUND((`{cs_col}` - `Cb`) * `Il`, 2) AS `Zysk`"
                if cs_col
                else "0 AS `Zysk`"
            ),
            f"{f'`{cs_col}`' if cs_col else '`Cb`'} AS `Cena netto PLN`",
            "`Cb` AS `Cena zakupu PLN`",
        ]

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM `{tbl}` "
            f"WHERE `Typ` IN ('WZ', 'PZ', 'K') "
            f"AND `Data` >= %s "
            f"ORDER BY `Data` DESC"
        )

        try:
            df = _q(conn, sql, (since_date,))
            if not df.empty:
                frames.append(df)
        except Exception as e:
            # Spróbuj uproszczone zapytanie jeśli złożone nie zadziała
            try:
                df = _q(conn,
                    f"SELECT * FROM `{tbl}` WHERE `Typ` IN ('WZ','PZ','K') AND `Data` >= %s",
                    (since_date,)
                )
                if not df.empty:
                    # Ręczne przemapowanie kolumn
                    df = _remap_obroty(df, tbl, conn)
                    frames.append(df)
            except Exception:
                pass

    if not frames:
        raise ValueError(
            "Nie znaleziono tabel z obrotami magazynowymi w iBiznes. "
            "Sprawdź czy IBIZNES_DB_URL jest poprawny i baza zawiera dane."
        )

    result = pd.concat(frames, ignore_index=True)

    # Konwertuj daty z formatu iBiznes YYYYMMDD → "17 lut 2026"
    result["Data wydania"] = result["Data wydania"].apply(_ibiznes_date_to_polish)

    return result


def _ibiznes_date_to_polish(val) -> str:
    """Konwertuje datę iBiznes (YYYYMMDD lub datetime) na format 'DD mmm YYYY'."""
    MONTHS_PL = {
        1: "sty", 2: "lut", 3: "mar", 4: "kwi",
        5: "maj", 6: "cze", 7: "lip", 8: "sie",
        9: "wrz", 10: "paź", 11: "lis", 12: "gru",
    }
    if val is None:
        return ""
    try:
        if isinstance(val, (datetime,)):
            dt = val
        else:
            s = str(val).strip()
            if len(s) == 8 and s.isdigit():
                dt = datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))
            else:
                dt = datetime.fromisoformat(s[:10])
        return f"{dt.day:02d} {MONTHS_PL[dt.month]} {dt.year}"
    except Exception:
        return str(val)


def _remap_obroty(df: pd.DataFrame, tbl: str, conn: pymysql.Connection) -> pd.DataFrame:
    """Fallback: przemapuj kolumny tabeli spec na oczekiwane nazwy CSV."""
    cols = df.columns.tolist()
    rename = {}

    if (c := _pick_col(cols, "Data")):         rename[c] = "Data wydania"
    if (c := _pick_col(cols, *_KOD_HINTS)):    rename[c] = "Kod towaru"
    if (c := _pick_col(cols, *_NAZWA_HINTS)):  rename[c] = "Nazwa towaru"
    if (c := _pick_col(cols, "Alias")):         rename[c] = "Klient"

    df = df.rename(columns=rename)

    il_col = _pick_col(df.columns.tolist(), "Il", "Ilosc", "Qty")
    cb_col = _pick_col(df.columns.tolist(), "Cb", "CenaZ", "Cena")
    typ_col = _pick_col(df.columns.tolist(), "Typ")

    if il_col and typ_col:
        df["Rozchód"] = df.apply(
            lambda r: float(str(r[il_col]).replace(",", ".")) if str(r.get(typ_col, "")) == "WZ" else 0,
            axis=1,
        )
        df["Przychód"] = df.apply(
            lambda r: float(str(r[il_col]).replace(",", ".")) if str(r.get(typ_col, "")) == "PZ" else 0,
            axis=1,
        )

    if il_col and cb_col:
        il = pd.to_numeric(df[il_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        cb = pd.to_numeric(df[cb_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        df["Wartość netto"] = (il * cb).round(2)
        df["Cena zakupu PLN"] = cb
        df["Zysk"] = 0

    if "Data wydania" in df.columns:
        df["Data wydania"] = df["Data wydania"].apply(_ibiznes_date_to_polish)

    return df


# ── Fetch: Kartoteka towarów ──────────────────────────────────────────────────

def fetch_kartoteka(
    conn: pymysql.Connection,
    tbl_info: dict,
) -> pd.DataFrame:
    """
    Pobiera kartotekę towarów.
    Zwraca DataFrame z kolumnami jak eksport KartotekaTowarowiUslug.csv:
    Kod towaru / usługi | Nazwa towaru / usługi | Grupa | Stan |
    Cena zakupu netto | Cena Podstawowa netto | Stan Min. | Dostawca | ...
    """
    frames = []

    for tbl_key in ("towary_spzoo", "towary_firma"):
        tbl = tbl_info.get(tbl_key)
        if not tbl:
            continue

        cols = get_columns(conn, tbl)

        kod_col    = _pick_col(cols, *_KOD_HINTS)
        nazwa_col  = _pick_col(cols, *_NAZWA_HINTS)
        stan_col   = _pick_col(cols, *_STAN_HINTS)
        cenaz_col  = _pick_col(cols, *_CENA_Z_HINTS)
        cenas_col  = _pick_col(cols, *_CENA_S_HINTS)
        stanmin_col = _pick_col(cols, *_STAN_MIN_HINTS)
        dos_col    = _pick_col(cols, *_DOSTAWCA_HINTS)
        grupa_col  = _pick_col(cols, *_GRUPA_HINTS)
        jm_col     = _pick_col(cols, *_JM_HINTS)

        if not kod_col or not nazwa_col:
            continue

        select_parts = [
            f"`{kod_col}` AS `Kod towaru / usługi`",
            f"`{nazwa_col}` AS `Nazwa towaru / usługi`",
        ]
        if grupa_col:  select_parts.append(f"`{grupa_col}` AS `Grupa`")
        else:           select_parts.append("'' AS `Grupa`")
        if stan_col:   select_parts.append(f"`{stan_col}` AS `Stan`")
        else:           select_parts.append("0 AS `Stan`")
        if cenaz_col:  select_parts.append(f"`{cenaz_col}` AS `Cena zakupu netto`")
        else:           select_parts.append("0 AS `Cena zakupu netto`")
        if cenas_col:  select_parts.append(f"`{cenas_col}` AS `Cena Podstawowa netto`")
        else:           select_parts.append("0 AS `Cena Podstawowa netto`")
        if stanmin_col: select_parts.append(f"`{stanmin_col}` AS `Stan Min.`")
        else:            select_parts.append("0 AS `Stan Min.`")
        if dos_col:    select_parts.append(f"`{dos_col}` AS `Dostawca`")
        else:           select_parts.append("'' AS `Dostawca`")
        if jm_col:     select_parts.append(f"`{jm_col}` AS `JM`")

        sql = f"SELECT {', '.join(select_parts)} FROM `{tbl}`"

        try:
            df = _q(conn, sql)
            if not df.empty:
                frames.append(df)
        except Exception:
            # Fallback: cała tabela i przemapuj
            try:
                df = _q(conn, f"SELECT * FROM `{tbl}`")
                if not df.empty:
                    df = _remap_kartoteka(df)
                    frames.append(df)
            except Exception:
                pass

    if not frames:
        raise ValueError(
            "Nie znaleziono tabeli kartoteki towarów w iBiznes. "
            "Sprawdź identyfikację tabel (discover_tables)."
        )

    return pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["Kod towaru / usługi"], keep="first"
    )


def _remap_kartoteka(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback: przemapuj kolumny tabeli towarów na oczekiwane nazwy CSV."""
    cols = df.columns.tolist()
    rename = {}
    if (c := _pick_col(cols, *_KOD_HINTS)):    rename[c] = "Kod towaru / usługi"
    if (c := _pick_col(cols, *_NAZWA_HINTS)):  rename[c] = "Nazwa towaru / usługi"
    if (c := _pick_col(cols, *_STAN_HINTS)):   rename[c] = "Stan"
    if (c := _pick_col(cols, *_CENA_Z_HINTS)): rename[c] = "Cena zakupu netto"
    if (c := _pick_col(cols, *_CENA_S_HINTS)): rename[c] = "Cena Podstawowa netto"
    if (c := _pick_col(cols, *_STAN_MIN_HINTS)): rename[c] = "Stan Min."
    if (c := _pick_col(cols, *_DOSTAWCA_HINTS)): rename[c] = "Dostawca"
    if (c := _pick_col(cols, *_GRUPA_HINTS)):  rename[c] = "Grupa"
    return df.rename(columns=rename)


# ── Fetch: Zamówienia do dostawców ────────────────────────────────────────────

def fetch_zamowienia(
    conn: pymysql.Connection,
    tbl_info: dict,
) -> pd.DataFrame:
    """
    Pobiera otwarte zamówienia do dostawców.
    Zwraca DataFrame z kolumnami jak ZamówieniaDlaDostawcy.csv:
    Nr Zamówienia | Dostawca | Wartość | Data realiz. | etap
    """
    frames = []

    for tbl_key in ("zam_spzoo", "zam_firma"):
        tbl = tbl_info.get(tbl_key)
        if not tbl:
            continue

        cols = get_columns(conn, tbl)

        nr_col    = _pick_col(cols, "NrZ", "Nr", "Numer", "NrZam", "NrDoc")
        dos_col   = _pick_col(cols, "Dostawca", "Alias", "Kontrahent", "Supplier")
        war_col   = _pick_col(cols, "Wartosc", "Wartość", "Kwota", "Suma", "Brutto", "Netto")
        data_col  = _pick_col(cols, "DataReal", "DataRealizacji", "DataDost", "DataZam", "Data")
        etap_col  = _pick_col(cols, "Etap", "Status", "Stan", "Realizacja")

        if not nr_col:
            continue

        select_parts = [f"`{nr_col}` AS `Nr Zamówienia`"]
        if dos_col:  select_parts.append(f"`{dos_col}` AS `Dostawca`")
        else:         select_parts.append("'' AS `Dostawca`")
        if war_col:  select_parts.append(f"`{war_col}` AS `Wartość`")
        else:         select_parts.append("0 AS `Wartość`")
        if data_col: select_parts.append(f"`{data_col}` AS `Data realiz.`")
        else:         select_parts.append("'' AS `Data realiz.`")
        if etap_col: select_parts.append(f"`{etap_col}` AS `etap`")
        else:         select_parts.append("'N' AS `etap`")

        # Tylko niezrealizowane (Etap IN ('N','B') lub analogiczne)
        where_clause = ""
        if etap_col:
            where_clause = f"WHERE `{etap_col}` IN ('N', 'B', 'n', 'b', 0, 1)"

        sql = f"SELECT {', '.join(select_parts)} FROM `{tbl}` {where_clause}"

        try:
            df = _q(conn, sql)
            if not df.empty:
                frames.append(df)
        except Exception:
            try:
                df = _q(conn, f"SELECT * FROM `{tbl}`")
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass

    if not frames:
        return pd.DataFrame(columns=["Nr Zamówienia", "Dostawca", "Wartość", "Data realiz.", "etap"])

    return pd.concat(frames, ignore_index=True)


# ── Główna funkcja: pobierz wszystko ─────────────────────────────────────────

def fetch_all(
    db_url: str,
    days: int = 90,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Główna funkcja — łączy się z iBiznes i pobiera wszystkie dane.

    Returns:
        (kartoteka_df, obroty_df, zamowienia_df, tbl_info)
        gdzie tbl_info zawiera m.in. '_all_tables' do debugowania.
    """
    conn = get_connection(db_url)
    try:
        tbl_info = identify_tables(conn)
        kartoteka  = fetch_kartoteka(conn, tbl_info)
        obroty     = fetch_obroty(conn, tbl_info, days=days)
        zamowienia = fetch_zamowienia(conn, tbl_info)
    finally:
        conn.close()

    return kartoteka, obroty, zamowienia, tbl_info
