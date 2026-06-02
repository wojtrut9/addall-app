"""
ibiznes_connector.py — Add All iBiznes MySQL Connector

Łączy się bezpośrednio z bazą MySQL iBiznes (tak samo jak CRM).
Zwraca DataFrames z kolumnami identycznymi jak eksport CSV z iBiznes
— engine.py nie wymaga żadnych zmian.

Zmienna środowiskowa: IBIZNES_DB_URL = "mysql://user:pass@host:port/dbname"
"""
from __future__ import annotations

import os
import re
import ssl
from datetime import datetime, timedelta
from urllib.parse import urlparse, unquote

import pandas as pd
import pymysql
import pymysql.cursors


# ── Jawne mapowanie kolumn iBiznes (firmatec) ─────────────────────────────────
#
# Schemat iBiznes jest stały (tabele addallspkazogr* / firma* mają identyczne
# kolumny), więc zamiast ZGADYWAĆ nazwy heurystyką (co dawało błędne wyniki —
# patrz historia: cena zakupu mapowała się na cenę brutto sprzedaży, dostawca na
# pustą kolumnę, status na zawsze-puste `etap`) trzymamy je tu jawnie.
# Heurystyka (_pick_col) zostaje jako fallback dla innych ERP-ów / nietypowych baz.
IBIZNES_COLS = {
    "towary": {  # kartoteka towarów
        "kod": "Kod", "nazwa": "Nazw", "grupa": "Gr", "stan": "STAN",
        "cena_zak": "Cz", "cena_sp": "CN1", "stan_min": "smin",
        "dostawca": "Alias", "jm": "JM", "akt": "Akt", "anul": "Anul",
    },
    "spec": {  # ruchy magazynowe (WZ/PZ/ZAK…)
        "typ": "Typ", "data": "Data", "kod": "Kod", "nazwa": "Nazw",
        "klient": "Alias", "il": "il", "cena_zak": "CB", "cena_sp": "CN",
    },
    "zamz": {  # nagłówek zamówień ZAKUPU (do dostawców)
        "id": "ID", "nr": "NrR", "dostawca": "Nazw", "alias": "Alias",
        "wartosc": "Wart", "status": "Typ", "data_utw": "Dwy", "anul": "Anul",
    },
    "zamzy": {  # pozycje zamówień zakupu
        "id": "ID", "parent": "IDf", "kod": "Kod", "nazwa": "Nazw",
        "il": "il", "cena": "CN",
    },
}


def _env_flag(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "tak", "yes", "y", "on")


def _env_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    val = os.environ.get(name)
    if not val:
        return tuple(default)
    return tuple(p.strip() for p in str(val).split(",") if p.strip())


def _open_order_types() -> tuple[str, ...]:
    """Wartości `zamz.Typ` oznaczające OTWARTE zamówienie (w realizacji / w drodze).

    iBiznes koduje etap realizacji zamówienia zakupu w kolumnie `Typ`:
      0 = nowe / niezatwierdzone (świeżo utworzone)   ┐ OTWARTE (jeszcze nie dotarło)
      1 = w realizacji / częściowo zrealizowane        ┘
      2 = zrealizowane (towar przyjęty)  ← zamknięte
      3 = anulowane                       ← zamknięte
    Konfigurowalne przez Railway → Variables: IBIZNES_OPEN_ORDER_TYPES="0,1".
    """
    return _env_csv("IBIZNES_OPEN_ORDER_TYPES", ("0", "1"))


def _col_present(cols: list[str], name: str | None) -> str | None:
    """Zwraca rzeczywistą nazwę kolumny (z oryginalną wielkością liter) jeśli
    `name` istnieje w tabeli (porównanie case-insensitive), inaczej None."""
    if not name:
        return None
    low = name.lower()
    for c in cols:
        if c.lower() == low:
            return c
    return None


def _map_col(cols: list[str], ibiznes_name: str | None, *hints: str) -> str | None:
    """Najpierw jawna kolumna iBiznes, potem heurystyka _pick_col (fallback)."""
    return _col_present(cols, ibiznes_name) or _pick_col(cols, *hints)


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


def _relaxed_ssl_context() -> ssl.SSLContext:
    """Kontekst TLS akceptujący STARE serwery MySQL.

    Serwer iBiznes (db.firmatec.pl) oferuje starszy TLS/słabsze szyfry, które
    nowy OpenSSL 3 (np. na Railway) domyślnie odrzuca → "SSLV3_ALERT_HANDSHAKE_
    FAILURE". Rozluźniamy: bez weryfikacji certyfikatu, minimalna wersja TLSv1,
    obniżony poziom bezpieczeństwa szyfrów. To łączność wewnętrzna do ERP —
    priorytetem jest działające połączenie, nie walidacja cert.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1
    except (ValueError, AttributeError):
        pass
    try:
        # SECLEVEL=0 dopuszcza stare szyfry/krótkie klucze (OpenSSL 3 blokuje je
        # przy domyślnym SECLEVEL=2).
        ctx.set_ciphers("DEFAULT@SECLEVEL=0")
    except ssl.SSLError:
        pass
    return ctx


def _is_ssl_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in ("ssl", "handshake", "secure transport", "wrong version"))


def get_connection(db_url: str) -> pymysql.Connection:
    """Otwiera połączenie z MySQL iBiznes, odporne na problemy TLS.

    Tryb sterowany zmienną IBIZNES_DB_SSL:
      - "disable"/"off"  → wymuś brak SSL
      - "relaxed"/"on"   → wymuś rozluźniony TLS (dla starych serwerów)
      - (brak/"auto")    → najpierw zwykłe połączenie; jeśli padnie na błędzie
                           SSL — ponów z rozluźnionym TLS (działa lokalnie
                           i na Railway/OpenSSL 3).
    """
    params = _parse_url(db_url)
    mode = (os.environ.get("IBIZNES_DB_SSL") or "auto").strip().lower()

    if mode in ("disable", "off", "none", "no", "false", "0"):
        params.pop("ssl", None)
        return pymysql.connect(**params)

    if mode in ("relaxed", "require", "on", "ssl", "true", "1"):
        params["ssl"] = _relaxed_ssl_context()
        return pymysql.connect(**params)

    # auto
    try:
        return pymysql.connect(**params)
    except Exception as exc:
        if _is_ssl_error(exc):
            params["ssl"] = _relaxed_ssl_context()
            return pymysql.connect(**params)
        raise


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


def _find_zam_header(tables: list[str]) -> str | None:
    """Header zamówień ZAKUPU (do dostawców) — w iBiznes nazwa kończy się na 'zamz'.

    Konwencja nazewnictwa iBiznes:
      *zams   = zamówienia sprzedaży (od klientów) — header
      *zamsy  = pozycje zamówień sprzedaży
      *zamz   = zamówienia ZAKUPU / do dostawców — header ← TO
      *zamzy  = pozycje zamówień zakupu ← line items dla nas

    Pomijamy '*towaryzam' (tabela powiązań towar-zamówienie, nie nagłówek)
    i tabele line-itemów po sufiksie 'y'.
    """
    # 1) Preferowany: dokładny sufiks 'zamz' (np. addallspkazogrzamz, firmazamz)
    for t in tables:
        if t.lower().endswith("zamz"):
            return t
    # 2) Fallback dla innych nazewnictw (zaz, dokzak, zakzam, …)
    for t in tables:
        low = t.lower()
        if "towaryzam" in low or "spec" in low or "poz" in low or "real" in low:
            continue
        if low.endswith(("zams", "zamsy", "zamzy")):
            continue  # to są sprzedaż lub pozycje
        if any(k in low for k in ("zamz", "zaz", "dokzak", "zakzam", "zamzak", "zamow", "order")):
            return t
    return None


def _find_zam_lines(tables: list[str]) -> str | None:
    """Pozycje zamówień zakupu — w iBiznes sufiks 'zamzy' (np. addallspkazogrzamzy).

    Fallback dla innych ERP-ów: tabele kończące się na 'spec'/'poz' zawierające 'zam'.
    """
    # 1) Preferowany sufiks iBiznes: 'zamzy'
    for t in tables:
        if t.lower().endswith("zamzy"):
            return t
    # 2) Fallback: *zamspec / *zampoz / *zamowspec
    for t in tables:
        low = t.lower()
        if ("zam" in low or "zaz" in low) and ("spec" in low or "poz" in low):
            return t
    return None


def identify_tables(conn: pymysql.Connection) -> dict[str, str | None]:
    """
    Identyfikuje nazwy kluczowych tabel iBiznes.
    """
    tables = discover_tables(conn)

    # Rozdziel tabele na prefix "addall*" (sp. z o.o.) i "firma*" (JDG).
    # Domyślnie analizujemy TYLKO sp. z o.o. — to realny kupujący (1002 zam.
    # zakupu vs 1 w JDG). Scalanie kartotek dwóch firm po `Kod` mieszałoby stany,
    # więc JDG włączamy tylko jawnie przez IBIZNES_INCLUDE_FIRMA=true.
    spzoo = [t for t in tables if t.lower().startswith("addall")]
    firma = (
        [t for t in tables if t.lower().startswith("firma")]
        if _env_flag("IBIZNES_INCLUDE_FIRMA", False)
        else []
    )

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
        S = IBIZNES_COLS["spec"]
        kod_col   = _map_col(cols, S["kod"], *_KOD_HINTS) or "Symbol"
        nazwa_col = _map_col(cols, S["nazwa"], *_NAZWA_HINTS) or "Nazw"
        jm_col    = _pick_col(cols, *_JM_HINTS)
        cs_col    = _map_col(cols, S["cena_sp"], *_CENA_S_HINTS)  # cena sprzedaży (CN)

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
        M = IBIZNES_COLS["towary"]

        kod_col    = _map_col(cols, M["kod"], *_KOD_HINTS)
        nazwa_col  = _map_col(cols, M["nazwa"], *_NAZWA_HINTS)
        stan_col   = _map_col(cols, M["stan"], *_STAN_HINTS)
        cenaz_col  = _map_col(cols, M["cena_zak"], *_CENA_Z_HINTS)
        cenas_col  = _map_col(cols, M["cena_sp"], *_CENA_S_HINTS)
        stanmin_col = _map_col(cols, M["stan_min"], *_STAN_MIN_HINTS)
        dos_col    = _map_col(cols, M["dostawca"], *_DOSTAWCA_HINTS)
        grupa_col  = _map_col(cols, M["grupa"], *_GRUPA_HINTS)
        jm_col     = _map_col(cols, M["jm"], *_JM_HINTS)
        akt_col    = _map_col(cols, M["akt"], *_AKT_HINTS) if only_active else None
        anul_col   = _col_present(cols, M["anul"])

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
        # smin: w iBiznes wartość -1 oznacza "nie ustawiono minimum" → traktuj jako 0
        if stanmin_col: select_parts.append(f"GREATEST(0, COALESCE(`{stanmin_col}`, 0)) AS `Stan Min.`")
        else:            select_parts.append("0 AS `Stan Min.`")
        if dos_col:    select_parts.append(f"`{dos_col}` AS `Dostawca`")
        else:           select_parts.append("'' AS `Dostawca`")
        if jm_col:     select_parts.append(f"`{jm_col}` AS `JM`")

        # Filtr aktywności — tylko produkty z Akt='T'/'1'/NULL (NULL traktujemy jako
        # aktywne) i NIE anulowane (Anul != 'T'/'Y').
        conds = []
        if akt_col:
            conds.append(
                f"(`{akt_col}` IS NULL "
                f"OR UPPER(TRIM(CAST(`{akt_col}` AS CHAR))) IN ('T','TAK','Y','YES','1','A'))"
            )
        if anul_col:
            conds.append(
                f"(`{anul_col}` IS NULL "
                f"OR UPPER(TRIM(CAST(`{anul_col}` AS CHAR))) NOT IN ('T','TAK','Y','YES','1'))"
            )
        where_clause = (" WHERE " + " AND ".join(conds)) if conds else ""

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
    """LEGACY (nieużywane): filtr otwartych po kolumnie `etap`.

    W bazie iBiznes Add All `etap` jest ZAWSZE puste, więc ten filtr przepuszczał
    wszystkie 1002 zamówienia jako "otwarte". Zostaje tylko dla zgodności wstecznej
    / innych baz. Aktualnie używamy `_build_open_orders_where_typ` (kolumna `Typ`).
    """
    placeholders = ", ".join(f"'{e}'" for e in _ETAP_ZAMKNIETE)
    return (
        f" WHERE (`{etap_col}` IS NULL "
        f"OR TRIM(CAST(`{etap_col}` AS CHAR)) = '' "
        f"OR UPPER(TRIM(CAST(`{etap_col}` AS CHAR))) NOT IN ({placeholders.upper()}))"
    )


def _build_open_orders_where_typ(
    status_col: str,
    anul_col: str | None = None,
    alias: str = "",
) -> str:
    """WHERE wybierający OTWARTE zamówienia zakupu po kolumnie `Typ` (etap realizacji).

    Otwarte = `Typ` ∈ IBIZNES_OPEN_ORDER_TYPES (domyślnie 0,1) i niezanulowane.
    `alias` to prefiks tabeli w JOIN-ie (np. 'h.') — pusty dla prostego SELECT-a.
    """
    types = _open_order_types()
    placeholders = ", ".join(f"'{t.upper()}'" for t in types)
    parts = [f"UPPER(TRIM(CAST({alias}`{status_col}` AS CHAR))) IN ({placeholders})"]
    if anul_col:
        parts.append(
            f"({alias}`{anul_col}` IS NULL "
            f"OR UPPER(TRIM(CAST({alias}`{anul_col}` AS CHAR))) NOT IN ('T','TAK','Y','YES','1'))"
        )
    return " WHERE " + " AND ".join(parts)


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
        Z = IBIZNES_COLS["zamz"]

        nr_col    = _map_col(cols, Z["nr"], "NrR", "NrZ", "Numer", "NrZam", "NrDoc")
        # Dostawca = `Alias` (krótka nazwa, SPÓJNA z kartoteką, gdzie dostawca też
        # siedzi w `Alias`) — dzięki temu rekomendacje da się podpiąć do otwartych
        # zamówień. Pełną nazwę `Nazw` zostawiamy tylko jako fallback.
        dos_col   = _map_col(cols, Z["alias"], "Alias", "Dostawca", "Kontrahent", "Supplier") \
                    or _col_present(cols, Z["dostawca"])
        war_col   = _map_col(cols, Z["wartosc"], "Wartosc", "Wartość", "Kwota", "Suma", "Brutto", "Netto")
        data_utw_col   = _map_col(cols, Z["data_utw"], "DataUtw", "DataWyst", "DataDok", "Data")
        data_real_col  = _pick_col(cols, "DataReal", "DataRealizacji", "DataDost", "DataZam")
        status_col = _map_col(cols, Z["status"], "Etap", "Status", "Stan", "Realizacja")
        anul_col   = _col_present(cols, Z["anul"])

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
        if status_col:     select_parts.append(f"`{status_col}` AS `etap`")
        else:               select_parts.append("'0' AS `etap`")

        # Otwarte = Typ ∈ {0,1} i niezanulowane (NIE po pustej kolumnie `etap`!)
        where_clause = (
            _build_open_orders_where_typ(status_col, anul_col)
            if status_col else ""
        )

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

        ZY = IBIZNES_COLS["zamzy"]
        kod_col = _map_col(spec_cols, ZY["kod"], *_KOD_HINTS)
        il_col  = _map_col(spec_cols, ZY["il"], "Il", "Ilosc", "Qty", "Quantity")
        cena_col = _map_col(spec_cols, ZY["cena"], *_CENA_Z_HINTS) or _pick_col(spec_cols, "Cena", "Cb")
        # Klucz do nagłówka: zamzy.IDf = zamz.ID (NIE NrR=Nr — to string vs int!)
        nrz_col = _map_col(spec_cols, ZY["parent"], "IDf", "NrR", "NrZ", "Nr")

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

        # JOIN z nagłówkiem (zamz) po IDf=ID, by zostawić tylko OTWARTE zamówienia
        join_clause = ""
        where_clause = ""
        if head_tbl and nrz_col:
            try:
                head_cols = get_columns(conn, head_tbl)
            except Exception:
                head_cols = []
            head_id = _map_col(head_cols, IBIZNES_COLS["zamz"]["id"], "ID", "NrR", "Nr")
            status_col = _map_col(head_cols, IBIZNES_COLS["zamz"]["status"], "Etap", "Status", "Stan", "Realizacja")
            anul_col = _col_present(head_cols, IBIZNES_COLS["zamz"]["anul"])
            if head_id and status_col:
                join_clause = (
                    f" JOIN `{head_tbl}` h ON s.`{nrz_col}` = h.`{head_id}`"
                )
                # Otwarte = Typ ∈ {0,1} i niezanulowane.
                where_clause = _build_open_orders_where_typ(status_col, anul_col, alias="h.")

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

        ZY = IBIZNES_COLS["zamzy"]
        Z = IBIZNES_COLS["zamz"]
        kod_col   = _map_col(spec_cols, ZY["kod"], *_KOD_HINTS)
        nazwa_col = _map_col(spec_cols, ZY["nazwa"], *_NAZWA_HINTS)
        il_col    = _map_col(spec_cols, ZY["il"], "Il", "Ilosc", "Qty", "Quantity")
        cena_col  = _map_col(spec_cols, ZY["cena"], *_CENA_Z_HINTS) or _pick_col(spec_cols, "Cena", "Cb")
        nrz_spec  = _map_col(spec_cols, ZY["parent"], "IDf", "NrR", "Nr")  # klucz do nagłówka

        head_id   = _map_col(head_cols, Z["id"], "ID", "NrR", "Nr")          # ID nagłówka
        nr_disp   = _map_col(head_cols, Z["nr"], "NrR", "Nr", "Numer")        # ładny numer (ZAZ/…)
        dos_head  = _map_col(head_cols, Z["alias"], "Alias", "Dostawca", "Kontrahent", "Supplier")
        data_head = _map_col(head_cols, Z["data_utw"], "DataUtw", "DataWyst", "Data")
        status_head = _map_col(head_cols, Z["status"], "Etap", "Status", "Stan", "Realizacja")
        anul_head = _col_present(head_cols, Z["anul"])

        if not kod_col or not il_col or not nrz_spec or not head_id:
            continue

        select_parts = [
            f"s.`{kod_col}` AS `Kod towaru`",
            (f"s.`{nazwa_col}` AS `Nazwa`" if nazwa_col else "'' AS `Nazwa`"),
            (f"h.`{dos_head}` AS `Dostawca`" if dos_head else "'' AS `Dostawca`"),
            (f"h.`{nr_disp}` AS `Nr Zamówienia`" if nr_disp else f"h.`{head_id}` AS `Nr Zamówienia`"),
            (f"h.`{data_head}` AS `Data realiz.`" if data_head else "'' AS `Data realiz.`"),
            f"CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3)) AS `ilosc`",
            (
                f"CAST(REPLACE(REPLACE(CAST(s.`{il_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,3)) * "
                f"CAST(REPLACE(REPLACE(CAST(s.`{cena_col}` AS CHAR), ',', '.'), ' ', '') AS DECIMAL(18,4)) AS `wartosc`"
                if cena_col else "0 AS `wartosc`"
            ),
        ]

        # Otwarte = Typ ∈ {0,1} i niezanulowane (nagłówek aliasowany jako h.)
        where_clause = (
            _build_open_orders_where_typ(status_head, anul_head, alias="h.")
            if status_head else ""
        )

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM `{spec_tbl}` s "
            f"JOIN `{head_tbl}` h ON s.`{nrz_spec}` = h.`{head_id}`"
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
