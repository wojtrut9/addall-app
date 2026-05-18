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
from urllib.parse import urlparse, unquote

import pandas as pd
import pymysql
import pymysql.cursors


# ── Połączenie ────────────────────────────────────────────────────────────────

_BAD_HOSTS = {
    "github.com", "www.github.com", "gitlab.com", "bitbucket.org",
    "railway.app", "vercel.app", "localhost", "127.0.0.1", "0.0.0.0", "",
}


def _validate_url(url: str) -> None:
    """Wyłapuje typowe pomyłki ZANIM pymysql wykona timeout 15s.

    Najczęstszy błąd: ktoś wkleił link do repo (https://github.com/...)
    zamiast connection stringa do MySQL.
    """
    if not url or not isinstance(url, str):
        raise ValueError(
            "IBIZNES_DB_URL jest pusty. Wpisz connection string w formacie "
            "mysql://user:pass@host:port/dbname (lub ustaw zmienną środowiskową "
            "IBIZNES_DB_URL w Railway → Variables)."
        )

    url_stripped = url.strip()

    if url_stripped.lower().startswith(("http://", "https://")):
        raise ValueError(
            f"Wygląda jak URL strony, nie bazy MySQL: '{url_stripped[:60]}...'.\n"
            "Powinno być w formacie: mysql://user:pass@host:3306/dbname"
        )

    if not url_stripped.lower().startswith("mysql://"):
        raise ValueError(
            f"Connection string musi zaczynać się od 'mysql://'. "
            f"Otrzymano: '{url_stripped[:50]}...'."
        )

    try:
        p = urlparse(url_stripped)
    except Exception as e:
        raise ValueError(f"Nieprawidłowy format URL bazy: {e}")

    host = (p.hostname or "").lower()
    if host in _BAD_HOSTS:
        raise ValueError(
            f"Host '{host}' nie jest serwerem MySQL iBiznes. "
            "Sprawdź `IBIZNES_DB_URL` w Railway → Variables — powinien wskazywać "
            "na bazę iBiznes (np. `db.firmatec.pl:3306`), a nie na github.com "
            "ani inną stronę WWW."
        )

    if not p.username or p.password is None:
        raise ValueError(
            "Brak użytkownika lub hasła w connection stringu. "
            "Format: mysql://user:pass@host:port/dbname"
        )

    if not p.path or p.path == "/":
        raise ValueError(
            "Brak nazwy bazy danych w URL (po porcie powinno być /nazwa_bazy). "
            "Format: mysql://user:pass@host:3306/dbname"
        )


def _parse_url(url: str) -> dict:
    """Parsuje mysql://user:pass@host:port/dbname na słownik parametrów.

    UWAGA na URL-encoding: hasła z znakami specjalnymi (np. '&', '@', '/',
    '#') MUSZĄ być URL-encoded w connection stringu, ale pymysql oczekuje
    dekodowanego hasła. urlparse() w Pythonie NIE dekoduje automatycznie —
    trzeba ręcznie wywołać unquote().

    Przykład: hasło 'P5rQ&p4dF' → URL: 'mysql://user:P5rQ%26p4dF@host/db'
    Bez unquote() pymysql wysyłał literalnie 'P5rQ%26p4dF' i dostawał
    'Access denied'.
    """
    _validate_url(url)
    p = urlparse(url.strip())
    return {
        "host":     p.hostname,
        "port":     p.port or 3306,
        "user":     unquote(p.username) if p.username else None,
        "password": unquote(p.password) if p.password else None,
        "database": unquote(p.path.lstrip("/")),
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
        host = urlparse(db_url.strip()).hostname
        return True, f"Połączenie z iBiznes OK ({host})"
    except ValueError as e:
        # Walidacja URL — pokazujemy "czystą" wskazówkę bez śmieci pymysql
        return False, str(e)
    except Exception as e:
        return False, f"Błąd połączenia z bazą: {e}"


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


_ZAM_KEYWORDS = ("zam", "zamow", "zamów", "order", "zaz", "zd", "dokzak", "zakzam", "zamzak")


def _find_zam_header(tables: list[str]) -> str | None:
    """Header zamówień do dostawców — z wyłączeniem tabel typu *spec/*poz.

    Próbujemy szerokiej listy fragmentów nazwy (iBiznes nazywa to różnie:
    addallzam, addallzaz, addalldokzak, zamowienia, ...). Wykluczamy typowe
    sufiksy line-itemów ('spec', 'poz') oraz pliki realizacji ('real')
    żeby nie złapać tabeli pozycji jako headera.
    """
    candidates = [
        t for t in tables
        if any(k in t.lower() for k in _ZAM_KEYWORDS)
        and not any(s in t.lower() for s in ("spec", "poz", "real"))
    ]
    return candidates[0] if candidates else None


def _find_zam_lines(tables: list[str]) -> str | None:
    """Pozycje (line items) zamówień — typowo *zamspec / *zampoz / *zamowspec."""
    for t in tables:
        low = t.lower()
        if any(k in low for k in _ZAM_KEYWORDS) and ("spec" in low or "poz" in low):
            return t
    return None


def identify_tables(conn: pymysql.Connection) -> dict[str, str | None]:
    """
    Identyfikuje nazwy kluczowych tabel iBiznes.
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
        # Zamówienia (header) — bez *spec/*poz
        "zam_spzoo": _find_zam_header(spzoo),
        "zam_firma": _find_zam_header(firma),
        # Pozycje zamówień (line items) — *zamspec / *zampoz
        "zamspec_spzoo": _find_zam_lines(spzoo),
        "zamspec_firma": _find_zam_lines(firma),
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
# Flaga aktywności kartoteki — w iBiznes typowo 'Akt' z wartościami T/N
_AKT_HINTS    = ["Akt", "Aktywny", "Active", "Aktywna"]


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
    only_active: bool = True,
) -> pd.DataFrame:
    """
    Pobiera kartotekę towarów.

    Domyślnie zwraca tylko AKTYWNE pozycje (flaga `Akt='T'` w iBiznes).
    iBiznes trzyma w kartotece tysiące archiwalnych SKU (Akt='N'),
    których nie chcemy w analizie zakupowej.

    Zwraca DataFrame z kolumnami jak eksport KartotekaTowarowiUslug.csv.
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
        akt_col    = _pick_col(cols, *_AKT_HINTS) if only_active else None

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

        # Filtr aktywności — tylko produkty z Akt='T'/'1'/NULL (NULL traktujemy jako aktywne)
        where_clause = ""
        if akt_col:
            where_clause = (
                f" WHERE (`{akt_col}` IS NULL "
                f"OR UPPER(TRIM(CAST(`{akt_col}` AS CHAR))) IN ('T','TAK','Y','YES','1','A'))"
            )

        sql = f"SELECT {', '.join(select_parts)} FROM `{tbl}`{where_clause}"

        try:
            df = _q(conn, sql)
            if not df.empty:
                frames.append(df)
        except Exception:
            # Fallback: bez filtra aktywności
            try:
                df = _q(conn, f"SELECT * FROM `{tbl}`")
                if not df.empty:
                    df = _remap_kartoteka(df)
                    if only_active and akt_col and akt_col in df.columns:
                        # Filtr po stronie Pythona w razie czego
                        active_mask = (
                            df[akt_col].isna()
                            | df[akt_col].astype(str).str.strip().str.upper().isin(
                                ["T", "TAK", "Y", "YES", "1", "A", ""]
                            )
                        )
                        df = df[active_mask]
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

# Etap zamówień iBiznes — kody które OZNACZAJĄ "zamknięte / nieaktywne":
#   Z = zrealizowane, A = anulowane, X = wycofane, K = zakończone, 9 = stop
# Wszystkie inne (N=nowe, B=bufor/bieżące, C=częściowe, P=potwierdzone,
#   puste/NULL, 0, 1) traktujemy jako OTWARTE.
# Filtrujemy "negatywnie" (NOT IN), żeby nie przegapić wariantów etapu które
# iBiznes wprowadza w aktualizacjach.
_ETAP_ZAMKNIETE = ("Z", "A", "X", "K", "9", "z", "a", "x", "k")


def _build_open_orders_where(etap_col: str) -> str:
    """Zwraca WHERE wybierający otwarte (niezrealizowane) zamówienia."""
    placeholders = ", ".join(f"'{e}'" for e in _ETAP_ZAMKNIETE)
    return (
        f" WHERE (`{etap_col}` IS NULL "
        f"OR TRIM(CAST(`{etap_col}` AS CHAR)) = '' "
        f"OR UPPER(TRIM(CAST(`{etap_col}` AS CHAR))) NOT IN ({placeholders.upper()}))"
    )


def fetch_zamowienia(
    conn: pymysql.Connection,
    tbl_info: dict,
) -> pd.DataFrame:
    """
    Pobiera otwarte zamówienia do dostawców (tablica "Zamówienia do dostawców"
    z iBiznes — ta z 18 pozycjami "w realizacji").

    Filtrowanie: bierzemy WSZYSTKO co NIE JEST zrealizowane/anulowane
    (Etap NOT IN Z/A/X/K). To pokrywa zarówno zamówienia nowe ('N'),
    częściowo zrealizowane ('C'), potwierdzone ('P'), w buforze ('B'),
    jak i te z pustym etapem.

    Zwraca DataFrame z kolumnami jak ZamówieniaDlaDostawcy.csv:
    Nr Zamówienia | Dostawca | Wartość | Data realiz. | Data utworzenia | etap
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
        data_real_col  = _pick_col(cols, "DataReal", "DataRealizacji", "DataDost", "DataZam")
        data_utw_col   = _pick_col(cols, "DataUtw", "DataWyst", "DataDok", "Data")
        etap_col  = _pick_col(cols, "Etap", "Status", "Stan", "Realizacja")

        if not nr_col:
            continue

        select_parts = [f"`{nr_col}` AS `Nr Zamówienia`"]
        if dos_col:        select_parts.append(f"`{dos_col}` AS `Dostawca`")
        else:               select_parts.append("'' AS `Dostawca`")
        if war_col:        select_parts.append(f"`{war_col}` AS `Wartość`")
        else:               select_parts.append("0 AS `Wartość`")
        if data_real_col:  select_parts.append(f"`{data_real_col}` AS `Data realiz.`")
        else:               select_parts.append("'' AS `Data realiz.`")
        if data_utw_col:   select_parts.append(f"`{data_utw_col}` AS `Data utworzenia`")
        else:               select_parts.append("'' AS `Data utworzenia`")
        if etap_col:       select_parts.append(f"`{etap_col}` AS `etap`")
        else:               select_parts.append("'N' AS `etap`")

        where_clause = _build_open_orders_where(etap_col) if etap_col else ""

        sql = f"SELECT {', '.join(select_parts)} FROM `{tbl}`{where_clause}"

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
        return pd.DataFrame(columns=[
            "Nr Zamówienia", "Dostawca", "Wartość",
            "Data realiz.", "Data utworzenia", "etap",
        ])

    result = pd.concat(frames, ignore_index=True)

    # Konwertuj daty iBiznes (YYYYMMDD) na polski format dla czytelności
    for col in ("Data realiz.", "Data utworzenia"):
        if col in result.columns:
            result[col] = result[col].apply(_ibiznes_date_to_polish)

    return result


# ── Fetch: Pozycje otwartych zamówień (in-transit per SKU) ────────────────────

def fetch_in_transit_lines(
    conn: pymysql.Connection,
    tbl_info: dict,
) -> pd.DataFrame:
    """
    Pobiera pozycje (line items) OTWARTYCH zamówień do dostawców i agreguje
    ilość per SKU — daje obraz "co już jedzie" do magazynu.

    Otwarte zamówienia to te, które nie zostały jeszcze zrealizowane
    (Etap='N', 'B' lub puste). Tabela line items zwykle ma nazwę
    *zamspec / *zampoz w iBiznes.

    Zwraca DataFrame z kolumnami:
        Kod towaru | w_drodze (sztuki) | wartosc_w_drodze
    Pusty DataFrame jeśli tabel nie ma lub nie ma otwartych pozycji.
    """
    frames = []

    pairs = [
        (tbl_info.get("zamspec_spzoo"), tbl_info.get("zam_spzoo")),
        (tbl_info.get("zamspec_firma"), tbl_info.get("zam_firma")),
    ]

    for spec_tbl, head_tbl in pairs:
        if not spec_tbl:
            continue

        try:
            spec_cols = get_columns(conn, spec_tbl)
        except Exception:
            continue

        kod_col = _pick_col(spec_cols, *_KOD_HINTS)
        il_col  = _pick_col(spec_cols, "Il", "Ilosc", "Qty", "Quantity")
        cena_col = _pick_col(spec_cols, *_CENA_Z_HINTS) or _pick_col(spec_cols, "Cena", "Cb")
        nrz_col = _pick_col(spec_cols, "NrZ", "Nr", "Numer", "NrZam", "NrDoc", "NrR")

        if not kod_col or not il_col:
            continue

        # Agregat: SUM(Il) per kod towaru
        select_parts = [
            f"`{kod_col}` AS `Kod towaru`",
            f"SUM(CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3))) AS `w_drodze`",
        ]
        if cena_col:
            select_parts.append(
                f"SUM(CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3)) * "
                f"CAST(REPLACE(REPLACE(CAST(s.`{cena_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,4))) AS `wartosc_w_drodze`"
            )
        else:
            select_parts.append("0 AS `wartosc_w_drodze`")

        # JOIN z tabelą header by odfiltrować zrealizowane zamówienia
        join_clause = ""
        where_clause = ""
        if head_tbl and nrz_col:
            try:
                head_cols = get_columns(conn, head_tbl)
            except Exception:
                head_cols = []
            head_nrz = _pick_col(head_cols, "NrZ", "Nr", "Numer", "NrZam", "NrDoc", "NrR")
            etap_col = _pick_col(head_cols, "Etap", "Status", "Stan", "Realizacja")
            if head_nrz and etap_col:
                join_clause = (
                    f" JOIN `{head_tbl}` h ON s.`{nrz_col}` = h.`{head_nrz}`"
                )
                # Otwarte = wszystko poza zrealizowane/anulowane.
                # Filtrujemy negatywnie (NOT IN ('Z','A','X','K')) żeby nie
                # przegapić częściowo zrealizowanych ('C') ani potwierdzonych ('P').
                placeholders = ", ".join(f"'{e.upper()}'" for e in _ETAP_ZAMKNIETE)
                where_clause = (
                    f" WHERE (h.`{etap_col}` IS NULL "
                    f"OR TRIM(CAST(h.`{etap_col}` AS CHAR)) = '' "
                    f"OR UPPER(TRIM(CAST(h.`{etap_col}` AS CHAR))) NOT IN ({placeholders}))"
                )

        sql = (
            f"SELECT {select_parts[0]}, {select_parts[1]}"
            + (f", {select_parts[2]}" if len(select_parts) > 2 else "")
            + f" FROM `{spec_tbl}` s{join_clause}{where_clause}"
            + f" GROUP BY s.`{kod_col}`"
        )

        try:
            df = _q(conn, sql)
            if not df.empty:
                # Konwersja do floatów
                df["w_drodze"] = pd.to_numeric(df["w_drodze"], errors="coerce").fillna(0)
                if "wartosc_w_drodze" in df.columns:
                    df["wartosc_w_drodze"] = pd.to_numeric(df["wartosc_w_drodze"], errors="coerce").fillna(0)
                # Tylko pozycje > 0
                df = df[df["w_drodze"] > 0]
                if not df.empty:
                    frames.append(df)
        except Exception:
            # Pomiń jeśli zapytanie nie przejdzie — analiza działa też bez tego
            pass

    if not frames:
        return pd.DataFrame(columns=["Kod towaru", "w_drodze", "wartosc_w_drodze"])

    result = pd.concat(frames, ignore_index=True)
    # Może być ten sam SKU w obu spółkach — zsumuj
    return (
        result.groupby("Kod towaru", as_index=False)
        .agg({"w_drodze": "sum", "wartosc_w_drodze": "sum"})
    )


# ── Fetch: Line items otwartych zamówień Z DOSTAWCĄ ──────────────────────────

def fetch_open_orders_with_lines(
    conn: pymysql.Connection,
    tbl_info: dict,
) -> pd.DataFrame:
    """
    Pobiera SZCZEGÓŁOWE pozycje otwartych zamówień do dostawców — każdy wiersz
    to JEDEN SKU w JEDNYM dokumencie zamówienia, z nazwą dostawcy i datą realiz.

    To jest klucz do funkcji "u tego dostawcy masz już otwarte zamówienie X
    — możesz dorzucić nowe pozycje zamiast robić osobny dokument".

    Zwraca DataFrame:
        Kod towaru | Nazwa | Dostawca | Nr Zamówienia | Data realiz.
        | ilosc | wartosc

    Pusty DataFrame jeśli tabel nie ma lub brak otwartych zamówień.
    """
    frames = []

    pairs = [
        (tbl_info.get("zamspec_spzoo"), tbl_info.get("zam_spzoo")),
        (tbl_info.get("zamspec_firma"), tbl_info.get("zam_firma")),
    ]

    for spec_tbl, head_tbl in pairs:
        if not spec_tbl or not head_tbl:
            continue

        try:
            spec_cols = get_columns(conn, spec_tbl)
            head_cols = get_columns(conn, head_tbl)
        except Exception:
            continue

        kod_col   = _pick_col(spec_cols, *_KOD_HINTS)
        nazwa_col = _pick_col(spec_cols, *_NAZWA_HINTS)
        il_col    = _pick_col(spec_cols, "Il", "Ilosc", "Qty", "Quantity")
        cena_col  = _pick_col(spec_cols, *_CENA_Z_HINTS) or _pick_col(spec_cols, "Cena", "Cb")
        nrz_spec  = _pick_col(spec_cols, "NrZ", "Nr", "Numer", "NrZam", "NrDoc", "NrR")

        nrz_head  = _pick_col(head_cols, "NrZ", "Nr", "Numer", "NrZam", "NrDoc", "NrR")
        dos_head  = _pick_col(head_cols, "Dostawca", "Alias", "Kontrahent", "Supplier")
        data_head = _pick_col(head_cols, "DataReal", "DataRealizacji", "DataDost", "DataZam")
        etap_head = _pick_col(head_cols, "Etap", "Status", "Stan", "Realizacja")

        if not kod_col or not il_col or not nrz_spec or not nrz_head:
            continue

        select_parts = [
            f"s.`{kod_col}` AS `Kod towaru`",
            (f"s.`{nazwa_col}` AS `Nazwa`" if nazwa_col else "'' AS `Nazwa`"),
            (f"h.`{dos_head}` AS `Dostawca`" if dos_head else "'' AS `Dostawca`"),
            f"h.`{nrz_head}` AS `Nr Zamówienia`",
            (f"h.`{data_head}` AS `Data realiz.`" if data_head else "'' AS `Data realiz.`"),
            f"CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3)) AS `ilosc`",
            (
                f"CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3)) * "
                f"CAST(REPLACE(REPLACE(CAST(s.`{cena_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,4)) AS `wartosc`"
                if cena_col else "0 AS `wartosc`"
            ),
        ]

        where_clause = ""
        if etap_head:
            placeholders = ", ".join(f"'{e.upper()}'" for e in _ETAP_ZAMKNIETE)
            where_clause = (
                f" WHERE (h.`{etap_head}` IS NULL "
                f"OR TRIM(CAST(h.`{etap_head}` AS CHAR)) = '' "
                f"OR UPPER(TRIM(CAST(h.`{etap_head}` AS CHAR))) NOT IN ({placeholders}))"
            )

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM `{spec_tbl}` s "
            f"JOIN `{head_tbl}` h ON s.`{nrz_spec}` = h.`{nrz_head}`"
            f"{where_clause}"
        )

        try:
            df = _q(conn, sql)
            if not df.empty:
                df["ilosc"]   = pd.to_numeric(df["ilosc"], errors="coerce").fillna(0)
                df["wartosc"] = pd.to_numeric(df["wartosc"], errors="coerce").fillna(0)
                df = df[df["ilosc"] > 0]
                if not df.empty:
                    if "Data realiz." in df.columns:
                        df["Data realiz."] = df["Data realiz."].apply(_ibiznes_date_to_polish)
                    frames.append(df)
        except Exception:
            pass

    if not frames:
        return pd.DataFrame(columns=[
            "Kod towaru", "Nazwa", "Dostawca",
            "Nr Zamówienia", "Data realiz.", "ilosc", "wartosc",
        ])

    return pd.concat(frames, ignore_index=True)


# ── Główna funkcja: pobierz wszystko ─────────────────────────────────────────

def fetch_all(
    db_url: str,
    days: int = 90,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Główna funkcja — łączy się z iBiznes i pobiera wszystkie dane.

    Returns:
        (kartoteka_df, obroty_df, zamowienia_df, in_transit_df,
         open_orders_lines_df, tbl_info)

        - zamowienia_df:  header otwartych zamówień (1 wiersz = 1 dokument)
        - in_transit_df:  agregat per SKU (w_drodze sumarycznie)
        - open_orders_lines_df: line items per (Nr zam., SKU, dostawca)
          — pozwala powiedzieć "u dostawcy X masz już otwarte zamówienie Y
          z produktami A, B — możesz dorzucić nowe pozycje".
        - tbl_info zawiera m.in. '_all_tables' do debugowania.
    """
    conn = get_connection(db_url)
    try:
        tbl_info = identify_tables(conn)
        kartoteka  = fetch_kartoteka(conn, tbl_info)
        obroty     = fetch_obroty(conn, tbl_info, days=days)
        zamowienia = fetch_zamowienia(conn, tbl_info)
        in_transit = fetch_in_transit_lines(conn, tbl_info)
        open_lines = fetch_open_orders_with_lines(conn, tbl_info)
    finally:
        conn.close()

    return kartoteka, obroty, zamowienia, in_transit, open_lines, tbl_info
