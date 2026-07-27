"""
app.py — Add All Asystent Zakupowy
Interfejs Streamlit z dwoma trybami:
  1. iBiznes (auto) — pobiera dane bezpośrednio z MySQL iBiznes
  2. Pliki (fallback) — wgrywanie CSV/Excel ręcznie
"""
import os
from datetime import datetime

import pandas as pd
import streamlit as st

from engine import analyze, lookup_supplier_open_orders, match_minimum
from excel_export import generate_full_excel, generate_order_excel
from ai_agent import (
    ask_agent,
    get_memory,
    save_preference,
    remove_preference,
    add_fact,
    remove_fact,
    get_exclusions,
    add_exclusion,
    remove_exclusion,
    export_memory_json,
    import_memory,
)

# ── Konfiguracja strony ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Add All — Asystent Zakupowy",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  /* ── Typografia i układ ───────────────────────────────────────── */
  html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  }
  .block-container { padding-top: 1.2rem; padding-bottom: 4rem; max-width: 1320px; }

  /* Ukryj domyślny chrome Streamlit dla czystszego wyglądu */
  #MainMenu, footer { visibility: hidden; height: 0; }
  header[data-testid="stHeader"] { background: transparent; height: 0; }

  /* ── Hero / nagłówek ──────────────────────────────────────────── */
  .hero {
    background: linear-gradient(120deg, #1D4ED8 0%, #2563EB 55%, #3B82F6 100%);
    border-radius: 18px;
    padding: 1.5rem 1.8rem;
    margin-bottom: 1.4rem;
    color: #fff;
    box-shadow: 0 8px 24px rgba(37,99,235,.22);
  }
  .hero h1 { margin: 0; font-size: 1.7rem; font-weight: 800; letter-spacing: -.02em; }
  .hero p  { margin: .35rem 0 0; opacity: .92; font-size: .98rem; }

  /* ── Karty metryk ─────────────────────────────────────────────── */
  [data-testid="stMetric"] {
    background: #fff;
    border: 1px solid #E6E9F0;
    border-radius: 14px;
    padding: 14px 16px;
    box-shadow: 0 1px 2px rgba(16,24,40,.05);
  }
  [data-testid="stMetricValue"] { font-size: 1.35rem; font-weight: 700; }
  [data-testid="stMetricLabel"] { font-weight: 600; color: #475467; }
  [data-testid="stMetricDelta"] { font-size: .82rem; }

  /* ── Przyciski ────────────────────────────────────────────────── */
  .stButton > button, .stDownloadButton > button {
    border-radius: 10px;
    font-weight: 600;
    border: 1px solid #E2E6EF;
    transition: all .12s ease;
  }
  .stButton > button:hover, .stDownloadButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(16,24,40,.10);
  }
  .stButton > button[kind="primary"] { box-shadow: 0 2px 8px rgba(37,99,235,.28); }

  /* ── Zakładki (pigułki) ───────────────────────────────────────── */
  .stTabs [data-baseweb="tab-list"] { gap: 6px; flex-wrap: wrap; }
  .stTabs [data-baseweb="tab"] {
    background: #F1F4FA;
    border-radius: 10px;
    padding: 8px 16px;
    font-weight: 600;
    font-size: .95rem;
  }
  .stTabs [aria-selected="true"] { background: #2563EB !important; color: #fff !important; }

  /* ── Expandery, czat, inputy ──────────────────────────────────── */
  [data-testid="stExpander"] { border: 1px solid #E6E9F0; border-radius: 12px; }
  [data-testid="stChatMessage"] {
    background: #F8FAFF;
    border: 1px solid #EAF0FF;
    border-radius: 14px;
    padding: 2px 12px;
  }
  div[data-baseweb="select"] > div, .stTextInput input, .stTextArea textarea, .stNumberInput input {
    border-radius: 10px;
  }
  hr { margin: 1rem 0; border-color: #ECEFF4; }
</style>
""", unsafe_allow_html=True)

# ── Pomocnicze ────────────────────────────────────────────────────────────────
def fmt_pln(value: float) -> str:
    return f"{value:,.0f} PLN".replace(",", " ")


def find_col(df, *hints):
    for hint in hints:
        m = next((c for c in df.columns if hint.lower() in c.lower()), None)
        if m:
            return m
    return None


def get_secret(key: str) -> str | None:
    """Pobiera sekret z Streamlit secrets lub zmiennych środowiskowych."""
    try:
        val = st.secrets.get(key) or st.secrets.get(key.lower())
        if val:
            return str(val)
    except Exception:
        pass
    return os.environ.get(key) or os.environ.get(key.lower())


# ── Nagłówek (hero) ───────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <h1>📦 Add All — Asystent Zakupowy</h1>
  <p>Analiza stanów magazynowych i rekomendacje zakupowe — wspierane przez AI (Claude)</p>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# PANEL DANYCH — zwijany u góry (po wczytaniu sam się składa)
# ═══════════════════════════════════════════════════════════════════
_loaded = "analiza" in st.session_state
_src_lbl = st.session_state.get("data_source")
_data_title = (
    f"⚙️ Źródło danych — ✓ wczytano ({'⚡ iBiznes' if _src_lbl == 'ibiznes' else '📁 pliki'})"
    if _loaded else "⚙️ Źródło danych i połączenie — zacznij tutaj"
)

with st.expander(_data_title, expanded=not _loaded):
    ibiznes_url = get_secret("IBIZNES_DB_URL")

    mode = st.radio(
        "Tryb pobierania danych:",
        options=["ibiznes", "pliki"],
        format_func=lambda x: (
            "⚡ Pobierz z iBiznes (automatycznie)" if x == "ibiznes"
            else "📁 Wgraj pliki ręcznie (CSV/Excel)"
        ),
        horizontal=True,
        help=(
            "Tryb iBiznes pobiera dane bezpośrednio z bazy danych iBiznes. "
            "Tryb pliki — wgraj eksport CSV lub Excel z iBiznes."
        ),
    )

    st.divider()

    # ───────────────────────── TRYB 1: iBiznes ─────────────────────────
    if mode == "ibiznes":
        st.markdown("**1. Połączenie z iBiznes**")

        if ibiznes_url:
            st.success("✅ IBIZNES_DB_URL skonfigurowany (z Railway secrets)")
            db_url_input = ibiznes_url
        else:
            st.info(
                "Wpisz connection string do bazy MySQL iBiznes. "
                "Możesz też dodać go w Railway → Variables jako `IBIZNES_DB_URL`."
            )
            db_url_input = st.text_input(
                "IBIZNES_DB_URL:",
                placeholder="mysql://user:password@host:3306/dbname",
                type="password",
            )

        col_days, col_run = st.columns([2, 3])
        with col_days:
            days = st.number_input(
                "Okres analizy (dni wstecz)",
                min_value=7,
                max_value=365,
                value=90,
                step=7,
                help="Ile dni wstecz pobrać dane o obrotach magazynowych. Zalecane: 60-90 dni.",
            )
        with col_run:
            st.write("")
            run_ibiznes = st.button(
                "⚡ Pobierz dane z iBiznes i analizuj",
                type="primary",
                disabled=not db_url_input,
                use_container_width=True,
            )

        with st.expander("🔧 Diagnostyka połączenia (zaawansowane)", expanded=False):
            if st.button("🔌 Test połączenia", disabled=not db_url_input):
                with st.spinner("Testuję połączenie…"):
                    try:
                        from ibiznes_connector import test_connection
                        ok, msg = test_connection(db_url_input)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)
                    except ImportError:
                        st.error("Brak biblioteki pymysql. Uruchom: pip install pymysql")
                    except Exception as e:
                        st.error(f"Błąd: {e}")

            if st.button("🔍 Pokaż schemat tabel zamówień", disabled=not db_url_input):
                with st.spinner("Czytam schemat…"):
                    try:
                        from ibiznes_connector import (
                            get_connection, identify_tables, get_columns,
                        )
                        conn = get_connection(db_url_input)
                        try:
                            tbl_info = identify_tables(conn)
                            zam_h = tbl_info.get("zam_spzoo") or tbl_info.get("zam_firma")
                            zam_l = tbl_info.get("zamspec_spzoo") or tbl_info.get("zamspec_firma")
                            out = {}
                            with conn.cursor() as cur:
                                for label, tbl in (("HEADER", zam_h), ("LINE_ITEMS", zam_l)):
                                    if not tbl:
                                        out[label] = ("(brak tabeli)", [], [])
                                        continue
                                    cols = get_columns(conn, tbl)
                                    etap_col = next(
                                        (c for c in cols if c.lower() == "typ"),
                                        None,
                                    ) or next(
                                        (c for c in cols if c.lower() in ("etap","status","stan","realizacja")),
                                        None,
                                    )
                                    etap_dist = []
                                    if label == "HEADER" and etap_col:
                                        try:
                                            cur.execute(
                                                f"SELECT `{etap_col}` AS etap, COUNT(*) AS n "
                                                f"FROM `{tbl}` GROUP BY `{etap_col}` ORDER BY n DESC"
                                            )
                                            etap_dist = list(cur.fetchall())
                                        except Exception:
                                            pass
                                    try:
                                        cur.execute(f"SELECT * FROM `{tbl}` LIMIT 2")
                                        sample = list(cur.fetchall())
                                    except Exception:
                                        sample = []
                                    out[label] = (tbl, cols, etap_dist, sample)
                        finally:
                            conn.close()

                        for label, data in out.items():
                            st.markdown(f"### {label}: `{data[0]}`")
                            if len(data) > 1 and data[1]:
                                st.code("Kolumny:\n  " + "\n  ".join(data[1]), language="text")
                            if len(data) > 2 and data[2]:
                                st.markdown(
                                    "**Rozkład statusu realizacji (`Typ`):** "
                                    "`0`/`1` = otwarte (w realizacji), `2` = zrealizowane, "
                                    "`3` = anulowane. Jeśli liczby nie zgadzają się z iBiznes "
                                    "→ ustaw `IBIZNES_OPEN_ORDER_TYPES` w Railway."
                                )
                                for row in data[2]:
                                    vals = list(row.values()) if isinstance(row, dict) else list(row)
                                    st.write(f"  • `{vals[0]!r}` → {vals[1]} dokumentów")
                            if len(data) > 3 and data[3]:
                                import json
                                st.markdown("**Próbka (2 wiersze):**")
                                st.code(
                                    json.dumps(
                                        [dict(r) for r in data[3]],
                                        indent=2, ensure_ascii=False, default=str,
                                    ),
                                    language="json",
                                )
                    except Exception as e:
                        import traceback as _tb
                        st.error(f"Błąd: {e}")
                        st.code(_tb.format_exc(), language="text")

            if st.button("📋 Pokaż wszystkie tabele bazy", disabled=not db_url_input):
                with st.spinner("Czytam listę tabel…"):
                    try:
                        from ibiznes_connector import (
                            get_connection, discover_tables, identify_tables,
                            get_kartoteka_columns,
                        )
                        conn = get_connection(db_url_input)
                        try:
                            tables = discover_tables(conn)
                            tbl_info = identify_tables(conn)
                            kart_cols_info = get_kartoteka_columns(conn, tbl_info)
                        finally:
                            conn.close()
                        addall = [t for t in tables if t.lower().startswith("addall")]
                        firma  = [t for t in tables if t.lower().startswith("firma")]
                        other  = [
                            t for t in tables
                            if not t.lower().startswith(("addall", "firma"))
                        ]
                        st.success(f"Znaleziono {len(tables)} tabel w bazie iBiznes.")
                        st.markdown(
                            f"**Auto-wykryte:**\n"
                            f"- Obroty (spec): `{tbl_info.get('spec_spzoo') or '❌'}`\n"
                            f"- Towary (kartoteka): `{tbl_info.get('towary_spzoo') or tbl_info.get('towary_firma') or '❌'}`\n"
                            f"- Zamówienia (header): `{tbl_info.get('zam_spzoo') or tbl_info.get('zam_firma') or '❌'}`\n"
                            f"- Pozycje zamówień: `{tbl_info.get('zamspec_spzoo') or tbl_info.get('zamspec_firma') or '❌'}`"
                        )
                        if addall:
                            st.code("addall*:\n  " + "\n  ".join(addall), language="text")
                        if firma:
                            st.code("firma*:\n  " + "\n  ".join(firma), language="text")
                        if other:
                            st.code("inne:\n  " + "\n  ".join(other), language="text")

                        # Diagnostyka nr katalogowego — pokaż co wykryto i jakie
                        # kolumny są w kartotece (do ustawienia ANITA_KATALOG_COL).
                        with st.expander("🔖 Nr katalogowy — diagnostyka kolumn kartoteki"):
                            for _tbl, _info in (kart_cols_info or {}).items():
                                _det = _info.get("nr_katalogowy_wykryty")
                                if _det:
                                    st.success(f"`{_tbl}` → wykryto kolumnę **{_det}** jako nr katalogowy")
                                else:
                                    st.warning(
                                        f"`{_tbl}` → nie rozpoznano nr katalogowego automatycznie. "
                                        "Wskaż kolumnę zmienną `ANITA_KATALOG_COL`, a jeśli nr jest "
                                        "w osobnej tabeli — `ANITA_KATALOG_TABLE` + `ANITA_KATALOG_KEY` "
                                        "(kod produktu) + `ANITA_KATALOG_VAL` (nr katalogowy)."
                                    )
                                st.code(
                                    "kolumny:\n  " + "\n  ".join(_info.get("kolumny", [])),
                                    language="text",
                                )
                    except Exception as e:
                        st.error(f"Błąd: {e}")

        if run_ibiznes and db_url_input:
            with st.spinner(f"Łączę się z iBiznes i pobieram dane za ostatnie {days} dni…"):
                try:
                    from ibiznes_connector import fetch_all, identify_tables, get_connection

                    conn_test = get_connection(db_url_input)
                    tbl_info = identify_tables(conn_test)
                    conn_test.close()

                    all_tables  = tbl_info.get("_all_tables", [])
                    spec_spzoo  = tbl_info.get("spec_spzoo")
                    towary      = tbl_info.get("towary_spzoo") or tbl_info.get("towary_firma")
                    zam         = tbl_info.get("zam_spzoo") or tbl_info.get("zam_firma")
                    zamspec     = tbl_info.get("zamspec_spzoo") or tbl_info.get("zamspec_firma")

                    if not spec_spzoo or not towary:
                        st.warning(
                            f"**Uwaga:** Nie wszystkie tabele zostały zidentyfikowane automatycznie.\n\n"
                            f"Tabele w bazie: `{'`, `'.join(all_tables)}`\n\n"
                            f"Zidentyfikowane:\n"
                            f"- Obroty (spec): `{spec_spzoo or '❌ nie znaleziono'}`\n"
                            f"- Towary: `{towary or '❌ nie znaleziono'}`\n"
                            f"- Zamówienia (header): `{zam or '⚠️ nie znaleziono'}`\n"
                            f"- Pozycje zamówień: `{zamspec or '⚠️ nie znaleziono'}`\n\n"
                            "Zgłoś to — dopasujemy nazwy tabel do Twojej bazy iBiznes."
                        )

                    (
                        kartoteka_df,
                        obroty_df,
                        zamowienia_df,
                        in_transit_df,
                        open_lines_df,
                        _,
                    ) = fetch_all(db_url_input, days=days)

                    in_transit_count = len(in_transit_df) if in_transit_df is not None else 0
                    lines_count = len(open_lines_df) if open_lines_df is not None else 0
                    st.caption(
                        f"Pobrano: {len(kartoteka_df)} aktywnych produktów (kartoteka po filtrze Akt), "
                        f"{len(obroty_df)} ruchów magazynowych, "
                        f"**{len(zamowienia_df)} otwartych zamówień do dostawców**, "
                        f"{in_transit_count} pozycji 'w drodze' (per SKU), "
                        f"{lines_count} line items w otwartych dokumentach"
                    )

                    if len(zamowienia_df) == 0:
                        spzoo_tables = [t for t in all_tables if t.lower().startswith("addall")]
                        firma_tables = [t for t in all_tables if t.lower().startswith("firma")]
                        other_tables = [
                            t for t in all_tables
                            if not t.lower().startswith(("addall", "firma"))
                        ]
                        with st.expander(
                            "⚠️ Nie wykryto otwartych zamówień — kliknij aby zobaczyć tabele bazy",
                            expanded=True,
                        ):
                            st.markdown(
                                f"**Zidentyfikowane tabele zamówień (header):**\n"
                                f"- sp. z o.o.: `{zam or '❌ nie znaleziono'}`\n"
                                f"- firma: `{tbl_info.get('zam_firma') or '❌ nie znaleziono'}`\n"
                                f"\n**Pozycje zamówień (line items):**\n"
                                f"- sp. z o.o.: `{zamspec or '❌ nie znaleziono'}`\n"
                                f"- firma: `{tbl_info.get('zamspec_firma') or '❌ nie znaleziono'}`"
                            )
                            st.markdown(
                                "**Wszystkie tabele bazy iBiznes** — znajdź nazwę "
                                "odpowiadającą *Zamówieniom do dostawców* "
                                "(np. z prefiksem `addall…` lub `firma…`) i daj znać, "
                                "która to — dopasujemy auto-discovery:"
                            )
                            if spzoo_tables:
                                st.code("addall*:\n  " + "\n  ".join(spzoo_tables), language="text")
                            if firma_tables:
                                st.code("firma*:\n  " + "\n  ".join(firma_tables), language="text")
                            if other_tables:
                                st.code("inne:\n  " + "\n  ".join(other_tables), language="text")

                    import io

                    def df_to_upload_file(df, name: str):
                        """Symuluje obiekt wgranego pliku dla engine.analyze()."""
                        buf = io.BytesIO()
                        df.to_csv(buf, sep=";", index=False, encoding="utf-8")
                        buf.seek(0)
                        buf.name = name
                        return buf

                    kart_buf  = df_to_upload_file(kartoteka_df, "KartotekaTowarowiUslug.csv")
                    obr_buf   = df_to_upload_file(obroty_df,    "magazyn obroty wszystko.csv")
                    zam_buf   = df_to_upload_file(zamowienia_df, "ZamówieniaDlaDostawcy.csv") if len(zamowienia_df) > 0 else None

                    analiza, zam_result, summary, context = analyze(
                        kart_buf, obr_buf,
                        zam_buf if zam_buf else None,
                        None,  # min_log_file
                        in_transit_df=in_transit_df,
                        open_orders_lines_df=open_lines_df,
                    )

                    st.session_state.update({
                        "analiza": analiza,
                        "zam_df":  zam_result,
                        "summary": summary,
                        "context": context,
                        "chat_history": [],
                        "data_source": "ibiznes",
                    })
                    st.success("✅ Dane pobrane i przeanalizowane! Zobacz zakładki niżej.")

                except Exception as exc:
                    st.error(f"❌ Błąd: {exc}")
                    st.info(
                        "Wskazówka: upewnij się że IBIZNES_DB_URL jest poprawny "
                        "i że serwer MySQL iBiznes jest dostępny z sieci Railway/internet."
                    )
                    st.stop()

    # ───────────────────────── TRYB 2: Pliki ─────────────────────────
    else:
        st.markdown("**1. Wgraj pliki z iBiznes**")
        st.caption(
            "Wymagane: Kartoteka + Obroty. Opcjonalne: Zamówienia + Minima logistyczne.\n"
            "Eksportuj z iBiznes: Magazyn → Kartoteka / Obroty / Zamówienia → Eksportuj CSV"
        )

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**📋 Kartoteka towarów** *(wymagana)*")
            kart_file = st.file_uploader(
                "kartoteka", type=["csv", "xlsx", "xls"],
                key="kart", label_visibility="collapsed",
            )
            if kart_file:
                st.success(f"✅ {kart_file.name}")
            else:
                st.info("KartotekaTowarowiUslug.csv")

            st.markdown("**🚚 Zamówienia** *(opcjonalne)*")
            zam_file = st.file_uploader(
                "zamowienia", type=["csv", "xlsx", "xls"],
                key="zam", label_visibility="collapsed",
            )
            if zam_file:
                st.success(f"✅ {zam_file.name}")
            else:
                st.caption("ZamówieniaDlaDostawcy.csv")

        with c2:
            st.markdown("**📊 Obroty magazynowe** *(wymagane)*")
            obroty_file = st.file_uploader(
                "obroty", type=["csv", "xlsx", "xls"],
                key="obroty", label_visibility="collapsed",
            )
            if obroty_file:
                st.success(f"✅ {obroty_file.name}")
            else:
                st.info("magazyn obroty wszystko.csv")

            st.markdown("**📏 Min. logistyczne** *(opcjonalne)*")
            min_log_file = st.file_uploader(
                "minima", type=["csv", "xlsx", "xls"],
                key="min_log", label_visibility="collapsed",
            )
            if min_log_file:
                st.success(f"✅ {min_log_file.name} (nadpisuje wbite minima)")
            else:
                st.caption("Domyślnie minima wbite w kod — plik tylko nadpisuje")

        st.divider()

        run_files = st.button(
            "▶ Analizuj pliki",
            type="primary",
            use_container_width=True,
            disabled=(not locals().get("kart_file") or not locals().get("obroty_file")),
        )
        if not locals().get("kart_file") or not locals().get("obroty_file"):
            st.warning("Wgraj co najmniej Kartotekę i Obroty.")

        if locals().get("run_files"):
            with st.spinner("Analizuję pliki…"):
                try:
                    analiza, zam_result, summary, context = analyze(
                        kart_file, obroty_file,
                        zam_file if locals().get("zam_file") else None,
                        min_log_file if locals().get("min_log_file") else None,
                        in_transit_df=None,  # tryb plikowy nie ma danych "w drodze" per SKU
                    )
                    st.session_state.update({
                        "analiza": analiza,
                        "zam_df":  zam_result,
                        "summary": summary,
                        "context": context,
                        "chat_history": [],
                        "data_source": "pliki",
                    })
                    st.success("✅ Analiza gotowa! Zobacz zakładki niżej.")
                except Exception as exc:
                    st.error(f"❌ Błąd analizy: {exc}")
                    st.stop()

# ── Ekran startowy gdy brak danych ────────────────────────────────────────────
if "analiza" not in st.session_state:
    st.info(
        "👋 **Zacznij od wczytania danych** — rozwiń panel **⚙️ Źródło danych** powyżej, "
        "wybierz tryb (iBiznes lub pliki) i kliknij *Analizuj*. "
        "Wyniki i agent AI pojawią się tutaj w zakładkach."
    )
    st.stop()

# ── Dane z sesji (wspólne dla zakładek) ───────────────────────────────────────
analiza = st.session_state["analiza"]
zam_df  = st.session_state["zam_df"]
summary = st.session_state["summary"]
context = st.session_state["context"]
source  = st.session_state.get("data_source", "pliki")

# Kolumny i helpery używane w kilku zakładkach
nazwa_col = find_col(analiza, "nazwa towaru")
kod_col   = find_col(analiza, "kod towaru / usługi", "kod towaru")
dos_col   = find_col(analiza, "dostawca")

# Kolejność kolumn list "Zamów dziś / w tygodniu" (wg ustaleń z Anitą):
# nasz kod → nazwa → nr katalogowy dostawcy (zamiast rubryki Dostawca —
# dostawcę mamy już w nagłówku grupy) → Stan i Zamów obok siebie → w drodze →
# starczy dni → (niewymienione zostają) → Stan Min. na samym końcu.
display_cols = [c for c in [
    kod_col, nazwa_col, "Nr katalogowy",
    "Stan", "ile_zamowic", "w_drodze", "dni_do_wyczerpania",
    "srednie_dzienne", "wartosc_zamowienia", "powod",
    "Stan Min.",
] if c and c in analiza.columns]

col_labels = {
    "srednie_dzienne":    "Zuż/dzień",
    "dni_do_wyczerpania": "Starczy (dni)",
    "ile_zamowic":        "Zamów (szt)",
    "wartosc_zamowienia": "Wartość PLN",
    "wartosc_stanu":      "Wartość stanu PLN",
    "wartosc_w_drodze":   "Wartość w drodze PLN",
    "w_drodze":           "W drodze (szt)",
    "efektywny_stan":     "Stan + w drodze",
    "marza_pct":          "Marża %",
    "powod":              "Dlaczego",
}


def show_table(df, cols, extra_rename=None):
    avail   = [c for c in cols if c in df.columns]
    rename  = {**col_labels, **(extra_rename or {})}
    st.dataframe(df[avail].rename(columns=rename), use_container_width=True, hide_index=True)


supplier_open = summary.get("supplier_open_orders", {}) or {}


def render_open_orders_banner(dostawca_name: str) -> None:
    """Baner z numerami dokumentów — dopasowanie nazwy dostawcy jest rozmyte
    (kartoteka vs iBiznes mogą się minimalnie różnić)."""
    info = lookup_supplier_open_orders(supplier_open, dostawca_name)
    if not info or not info.get("orders"):
        return
    liczba = info["liczba_dokumentow"]
    wartosc = info["laczna_wartosc"]
    data = info.get("najblizsza_data")
    data_part = f" • najbliższa dostawa: **{data}**" if data else ""
    nrs = ", ".join(o["nr"] for o in info["orders"][:5])
    if liczba > 5:
        nrs += f" (+{liczba - 5} więcej)"
    st.info(
        f"🚚 **U tego dostawcy masz już {liczba} otwartych zamówień** "
        f"na łącznie **{fmt_pln(wartosc)}**{data_part}\n\n"
        f"Dokumenty: `{nrs}`\n\n"
        f"💡 Możesz **dorzucić nowe pozycje do istniejącego zamówienia** "
        f"zamiast tworzyć nowy dokument."
    )


# Pamięć (potrzebna w zakładkach Agent i Pamięć)
mem = get_memory()
_exc_mem = mem.get("exclusions", {}) or {}
mem_count = (
    len(mem.get("preferences", {}))
    + len(mem.get("facts", []))
    + len(_exc_mem.get("products", []))
    + len(_exc_mem.get("suppliers", []))
)

anthropic_key = get_secret("ANTHROPIC_API_KEY")
openai_key = get_secret("OPENAI_API_KEY")
has_any_key = bool(anthropic_key or openai_key)

source_label = "⚡ iBiznes (live)" if source == "ibiznes" else "📁 Pliki"
st.caption(
    f"Źródło: {source_label}  •  Dane: {summary['data_od']} — {summary['data_do']} "
    f"({summary['dni_okresu']} dni)  •  Wygenerowano: {summary['data_analizy']}"
)

# ── Diagnostyka minimów (jeśli są nietrafione) ────────────────────────────────
_unmatched = (summary.get("min_log_unmatched") or []) if isinstance(summary, dict) else []
if _unmatched:
    st.warning(
        "📏 Minima logistyczne bez dopasowanego dostawcy w analizie "
        f"(sprawdź pisownię/alias): {', '.join(_unmatched)}"
    )

# ── Diagnostyka produktów indywidualnych (grupa INDYWIDUALNE) ─────────────────
_indyw = (summary.get("pozycje_indywidualne") or []) if isinstance(summary, dict) else []
if _indyw:
    _grupy = ", ".join(summary.get("grupy_wykluczone") or ["INDYWIDUALNE"])
    with st.expander(
        f"🙈 Pominięto {len(_indyw)} produktów z grupy indywidualnej ({_grupy}) — "
        "bot ich nie rekomenduje"
    ):
        st.caption(
            "Produkty z grupy INDYWIDUALNE (klienckie/niestandardowe) są celowo "
            "wyłączone z rekomendacji zamówień. Grupa aktualizuje się na bieżąco "
            "z iBiznes."
        )
        st.dataframe(
            pd.DataFrame(_indyw),
            use_container_width=True,
            hide_index=True,
        )

# ═══════════════════════════════════════════════════════════════════
# GŁÓWNE ZAKŁADKI
# ═══════════════════════════════════════════════════════════════════
tab_over, tab_res, tab_agent, tab_mem, tab_exp = st.tabs([
    "📊 Przegląd", "📋 Wyniki", "🤖 Agent AI", "🧠 Pamięć", "⬇️ Eksport",
])

# ── ZAKŁADKA: Przegląd ────────────────────────────────────────────────────────
with tab_over:
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        st.metric(
            "💰 Magazyn (aktywne)",
            fmt_pln(summary["wartosc_magazynu"]),
            f"cały: {fmt_pln(summary.get('wartosc_calego_magazynu', summary['wartosc_magazynu']))}",
            delta_color="off",
        )
    with m2:
        n_dostawcow = summary.get("dostawcow_z_otwartymi", 0)
        delta_label = (
            fmt_pln(summary.get("wartosc_w_drodze", 0))
            + (f"  ({n_dostawcow} dostawców)" if n_dostawcow else "")
        )
        st.metric(
            "🚚 W drodze",
            f"{summary.get('produktow_w_drodze', 0)} poz.",
            delta_label,
            delta_color="off",
        )
    with m3:
        st.metric("🚨 Zamów DZIŚ",       f"{summary['produktow_dzis']} pozycji",
                  f"≈ {fmt_pln(summary['wartosc_dzis'])}", delta_color="inverse")
    with m4:
        st.metric("🟡 Zamów w tygodniu", f"{summary['produktow_tydzien']} pozycji",
                  f"≈ {fmt_pln(summary['wartosc_tydzien'])}", delta_color="off")
    with m5:
        st.metric("📦 Aktywnych prod.",   summary["produktow_aktywnych"],
                  f"z {summary['produktow_total']} w kartotece")
    with m6:
        st.metric("⚫ Dead stock",        f"{summary['dead_stock_produktow']} prod.",
                  fmt_pln(summary["dead_stock_wartosc"]), delta_color="inverse")

    st.caption(
        "ℹ️ **Zasady analizy:** Kartoteka jest filtrowana do produktów aktywnych (Akt='T' w iBiznes). "
        "Rekomendacje 'Zamów' już uwzględniają to, co jest w drodze od dostawców — nie zamawiamy "
        "podwójnie. Wartość 'magazynu (aktywne)' pomija dead stock."
    )

# ── ZAKŁADKA: Wyniki ──────────────────────────────────────────────────────────
with tab_res:
    sub_dzis, sub_tydz, sub_droga, sub_otwarte, sub_top, sub_dead = st.tabs([
        "🚨 Zamów DZIŚ", "🟡 Zamów w tygodniu",
        "🔵 Nagłówek zamówień", "🏭 Otwarte u dostawców",
        "📈 Top movers", "⚫ Dead stock",
    ])

    with sub_dzis:
        dzis = analiza[analiza["status"] == "ZAMÓW DZIŚ"].sort_values("dni_do_wyczerpania")
        if len(dzis) == 0:
            st.success("🎉 Brak produktów do pilnego zamówienia!")
            if supplier_open:
                st.info(
                    f"Nadal masz **{len(supplier_open)}** dostawców z **otwartymi dokumentami** "
                    f"(jak w iBiznes „W realizacji”) — zobacz zakładkę **🏭 Otwarte u dostawców**."
                )
        else:
            st.error(
                f"**{len(dzis)} produktów wymaga zamówienia DZIŚ** "
                f"— łącznie {fmt_pln(dzis['wartosc_zamowienia'].sum())}"
            )
            if dos_col:
                for dostawca, grupa in dzis.groupby(dos_col):
                    razem  = grupa["wartosc_zamowienia"].sum()
                    min_v  = match_minimum(summary.get("min_log", {}), dostawca)
                    status = (
                        f"⚠️ brakuje {fmt_pln(min_v - razem)} do minimum"
                        if min_v > 0 and razem < min_v
                        else ("✅ minimum OK" if min_v > 0 else "")
                    )
                    supplier_info = lookup_supplier_open_orders(supplier_open, dostawca)
                    open_badge = (
                        f"  |  🚚 {supplier_info['liczba_dokumentow']} otwart."
                        if supplier_info else ""
                    )
                    label = f"🏭 {dostawca} — {fmt_pln(razem)}{open_badge}"
                    if status:
                        label += f"  |  {status}"
                    with st.expander(label, expanded=True):
                        render_open_orders_banner(dostawca)
                        show_table(grupa, display_cols)
            else:
                show_table(dzis, display_cols)

    with sub_tydz:
        tydzien = analiza[analiza["status"] == "ZAMÓW TYDZIEŃ"].sort_values("dni_do_wyczerpania")
        if len(tydzien) == 0:
            st.success("Brak produktów do zamówienia w tym tygodniu.")
        else:
            st.warning(
                f"**{len(tydzien)} produktów** — zamów do końca tygodnia "
                f"— {fmt_pln(tydzien['wartosc_zamowienia'].sum())}"
            )
            if dos_col:
                for dostawca, grupa in tydzien.groupby(dos_col):
                    supplier_info = lookup_supplier_open_orders(supplier_open, dostawca)
                    open_badge = (
                        f"  |  🚚 {supplier_info['liczba_dokumentow']} otwart."
                        if supplier_info else ""
                    )
                    with st.expander(
                        f"🏭 {dostawca} — {fmt_pln(grupa['wartosc_zamowienia'].sum())}{open_badge}",
                        expanded=False,
                    ):
                        render_open_orders_banner(dostawca)
                        show_table(grupa, display_cols)
            else:
                show_table(tydzien, display_cols)

    with sub_droga:
        if zam_df is None or len(zam_df) == 0:
            info = (
                "Dane pobrane z iBiznes — nie znaleziono otwartych zamówień."
                if source == "ibiznes"
                else "Nie wgrano pliku z zamówieniami lub plik jest pusty."
            )
            st.info(info)
        else:
            clean = zam_df.drop(columns=["_data_realiz"], errors="ignore")
            st.dataframe(clean, use_container_width=True, hide_index=True)

    with sub_otwarte:
        st.markdown(
            "**Otwarte zamówienia do dostawców** — zestawienie per firma: numery dokumentów (np. ZAZ/…), "
            "wartości i **SKU już w drodze**. To odpowiada widokowi z iBiznes; tu dodatkowo widać, "
            "do którego dokumentu można **dorzucić** kolejne pozycje."
        )
        if supplier_open:
            rows = []
            for _key, info in sorted(
                supplier_open.items(),
                key=lambda kv: (-(kv[1].get("laczna_wartosc") or 0), kv[1].get("nazwa", "")),
            ):
                naj = info.get("najblizsza_data") or ""
                doc_list = info.get("orders") or []
                n_docs = len(doc_list)
                docs_short = ", ".join(o["nr"] for o in doc_list[:12])
                if n_docs > 12:
                    docs_short += f" (+{n_docs - 12} więcej)"
                kody = info.get("kody_w_drodze") or []
                sku_n = len(kody) if isinstance(kody, list) else len(set(kody))
                rows.append({
                    "Dostawca": info.get("nazwa", _key),
                    "Otwartych dokumentów": info.get("liczba_dokumentow", n_docs),
                    "Łączna wartość PLN": round(info.get("laczna_wartosc") or 0, 2),
                    "Najbliższa dostawa": naj,
                    "Numery dokumentów": docs_short,
                    "Unikalnych SKU w drodze": sku_n,
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption(
                f"Łącznie **{len(supplier_open)}** dostawców z aktywnymi zamówieniami. "
                "Szczegóły pozycji (SKU) są w eksporterze Excel → arkusz „🏭 Otwarte u dostawcy”."
            )
        elif zam_df is not None and len(zam_df) > 0:
            st.warning(
                "Rozszerzone zestawienie (łączenie z kartoteką) jest niedostępne — pokazuję surowy "
                "eksport nagłówka zamówień. Uruchom ponownie analizę z iBiznes lub z pliku zamówień."
            )
            clean = zam_df.drop(columns=["_data_realiz"], errors="ignore")
            st.dataframe(clean, use_container_width=True, hide_index=True)
        else:
            st.info(
                "Brak danych o otwartych zamówieniach. "
                + (
                    "Sprawdź połączenie z bazą i czy w iBiznes są dokumenty „w realizacji”."
                    if source == "ibiznes"
                    else "W trybie plików **wgraj** `ZamówieniaDlaDostawcy.csv` z eksportu iBiznes."
                )
            )

    with sub_top:
        top = analiza[analiza["srednie_dzienne"] > 0].nlargest(20, "srednie_dzienne")
        top_cols = [c for c in [
            kod_col, nazwa_col, dos_col,
            "srednie_dzienne", "Stan", "dni_do_wyczerpania", "marza_pct",
        ] if c and c in analiza.columns]
        show_table(top, top_cols, {"marza_pct": "Marża %"})

    with sub_dead:
        dead = analiza[analiza["status"] == "DEAD STOCK"].sort_values("wartosc_stanu", ascending=False)
        if len(dead) == 0:
            st.success("🎉 Brak dead stocku!")
        else:
            st.warning(
                f"**{len(dead)} produktów** — zamrożony kapitał: "
                f"{fmt_pln(dead['wartosc_stanu'].sum())}"
            )
            dead_cols = [c for c in [
                kod_col, nazwa_col, dos_col,
                "Stan", "wartosc_stanu", "ostatnia_sprzedaz",
            ] if c and c in analiza.columns]
            show_table(dead, dead_cols, {"wartosc_stanu": "Wartość stanu PLN"})

# ── ZAKŁADKA: Agent AI ────────────────────────────────────────────────────────
with tab_agent:
    st.caption(
        "Agent sam wywołuje narzędzia żeby odpowiedzieć na pytanie. Pamięta Twoje preferencje "
        "i wykluczenia między sesjami, egzekwuje minima logistyczne i podpowiada dozamówienia."
    )

    if not has_any_key:
        st.info(
            "Dodaj klucz API żeby włączyć agenta: `ANTHROPIC_API_KEY` (Claude, zalecane) "
            "lub `OPENAI_API_KEY` (zapas) w Railway → Variables."
        )
    else:
        if "chat_history" not in st.session_state:
            st.session_state["chat_history"] = []

        # Wybór modelu — domyślnie Claude Sonnet 4.6 (lepszy od OpenAI), GPT jako zapas.
        col_m1, col_m2 = st.columns([3, 4])
        with col_m1:
            model_choice = st.selectbox(
                "🤖 Model AI:",
                options=[
                    "claude-sonnet-4-6",
                    "gpt-4.1",
                    "gpt-4o",
                    "gpt-5",
                    "(własny — wpisz obok)",
                ],
                index=0,
                help=(
                    "claude-sonnet-4-6 — zalecany, najlepszy (klucz ANTHROPIC_API_KEY).\n"
                    "gpt-4.1 / gpt-5 / gpt-4o — zapas OpenAI (klucz OPENAI_API_KEY).\n"
                    "Przy błędzie Claude następuje automatyczny fallback na gpt-4o."
                ),
            )
        with col_m2:
            custom_model = st.text_input(
                "Własna nazwa modelu:",
                value="" if model_choice != "(własny — wpisz obok)" else "claude-sonnet-4-6",
                disabled=(model_choice != "(własny — wpisz obok)"),
                placeholder="np. claude-opus-4-8, gpt-4.1-mini, o1",
            )

        actual_model = custom_model.strip() if model_choice == "(własny — wpisz obok)" and custom_model.strip() else model_choice

        # Klucz API zależnie od wybranego providera
        needs_claude = str(actual_model).lower().startswith("claude")
        api_key = anthropic_key if needs_claude else openai_key
        if not api_key:
            api_key = st.text_input(
                f"🔑 Klucz API {'Anthropic' if needs_claude else 'OpenAI'}:",
                type="password",
                placeholder="sk-ant-..." if needs_claude else "sk-...",
                help=(
                    f"Zapisz jako {'ANTHROPIC_API_KEY' if needs_claude else 'OPENAI_API_KEY'} "
                    "w Railway → Variables"
                ),
                key="model_api_key",
            )

        # Dodatkowe instrukcje (sticky system addon)
        extra_instr = st.text_area(
            "📌 Dodatkowe instrukcje dla agenta (opcjonalne, pamiętane w sesji):",
            value=st.session_state.get("extra_instr", ""),
            height=70,
            placeholder=(
                "Np. 'Zawsze podaj sumę zamówienia per dostawca.' albo "
                "'Jeśli marża < 15%, dodaj ostrzeżenie.'"
            ),
            key="extra_instr",
        )

        # Historia czatu
        for msg in st.session_state["chat_history"]:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
                if msg.get("tool_log"):
                    with st.expander(f"🔧 Narzędzia użyte ({len(msg['tool_log'])} wywołań)", expanded=False):
                        for t in msg["tool_log"]:
                            st.caption(
                                f"**{t['name']}** (iter {t['iteration']}) "
                                f"args: `{t['args']}`"
                            )
                            st.code(t["result_preview"], language="json")

        # Szybkie pytania
        st.markdown("**🎯 Szybkie pytania:**")
        qcols = st.columns(4)
        quick_qs = [
            "☀️ Odprawa poranna: co zamówić DZIŚ i DLACZEGO? Pogrupuj per dostawca, "
            "przy każdej pozycji podaj ile szt, za ile PLN i powód (zapas/tempo). "
            "Jeśli u dostawcy mam już otwarte zamówienie — powiedz że mogę dorzucić.",
            "Pokaż top 5 dostawców wg wartości zamówień.",
            "Produkty z marżą poniżej 20% — top 10 wg sprzedaży.",
            "Co jest już zamówione i w drodze? Podaj per dostawca i łączną kwotę.",
        ]
        for i, (qcol, q) in enumerate(zip(qcols, quick_qs)):
            with qcol:
                if st.button(q, key=f"quick_{i}", use_container_width=True):
                    st.session_state["_pending_q"] = q
                    st.rerun()

        # Główne okno do promptowania (większe, wieloliniowe)
        st.markdown("**💬 Twoje pytanie do agenta:**")

        with st.form(key="agent_form", clear_on_submit=True):
            pending_q = st.session_state.pop("_pending_q", "")
            user_input = st.text_area(
                "Pytanie:",
                value=pending_q,
                height=130,
                placeholder=(
                    "Napisz cokolwiek — agent sam pociągnie odpowiednie dane:\n"
                    "• 'Sprawdź dostawcę BIACHEM i powiedz co warto zamówić.'\n"
                    "• 'Zapamiętaj że minimum logistyczne ADEKS to 3000 PLN.'\n"
                    "• 'Top 15 produktów z najmniejszym zapasem dni — koniecznie z dostawcą.'"
                ),
                label_visibility="collapsed",
            )
            col_b1, col_b2 = st.columns([1, 5])
            with col_b1:
                submitted = st.form_submit_button("▶ Zapytaj", type="primary", use_container_width=True)
            with col_b2:
                st.caption(f"Model: **{actual_model}** | Pamięć: **{mem_count}** zapisów")

        if submitted and not api_key:
            st.warning(
                f"Brak klucza API dla modelu `{actual_model}` — wpisz go wyżej "
                f"({'ANTHROPIC_API_KEY' if needs_claude else 'OPENAI_API_KEY'})."
            )
        elif submitted and user_input.strip():
            question = user_input.strip()
            st.session_state["chat_history"].append({"role": "user", "content": question})

            with st.chat_message("user"):
                st.write(question)

            with st.chat_message("assistant"):
                with st.spinner(f"🤖 Agent pracuje (model: {actual_model})…"):
                    answer, tool_log, err = ask_agent(
                        question=question,
                        analiza=analiza,
                        summary=summary,
                        context=context,
                        api_key=api_key,
                        model=actual_model,
                        chat_history=st.session_state["chat_history"][:-1],
                        extra_system_instructions=extra_instr.strip() or None,
                    )

                    # Fallback (zapas): gdy wybrany model padł, a jest klucz OpenAI → gpt-4o
                    if err and actual_model != "gpt-4o" and openai_key:
                        st.caption(f"⚠️ Model `{actual_model}` zwrócił błąd — próbuję `gpt-4o` (zapas)…")
                        answer, tool_log, err = ask_agent(
                            question=question,
                            analiza=analiza,
                            summary=summary,
                            context=context,
                            api_key=openai_key,
                            model="gpt-4o",
                            chat_history=st.session_state["chat_history"][:-1],
                            extra_system_instructions=extra_instr.strip() or None,
                        )

                    if err:
                        st.error(
                            f"❌ Błąd agenta: {err}\n\n"
                            "Najczęstsze przyczyny:\n"
                            "- nieprawidłowy klucz API (`ANTHROPIC_API_KEY` / `OPENAI_API_KEY`)\n"
                            "- brak środków / limit (billing, rate limit)\n"
                            "- model niedostępny w Twoim koncie\n"
                            "- brak biblioteki `anthropic` (Claude) — sprawdź requirements"
                        )
                    else:
                        st.write(answer)
                        if tool_log:
                            with st.expander(
                                f"🔧 Narzędzia użyte przez agenta ({len(tool_log)})",
                                expanded=False,
                            ):
                                for t in tool_log:
                                    st.caption(
                                        f"**{t['name']}** (iter {t['iteration']}) "
                                        f"args: `{t['args']}`"
                                    )
                                    st.code(t["result_preview"], language="json")
                        st.session_state["chat_history"].append({
                            "role": "assistant",
                            "content": answer,
                            "tool_log": tool_log,
                        })

        if st.session_state.get("chat_history"):
            if st.button("🗑 Wyczyść chat (pamięć Anity zostaje)", key="clear_chat"):
                st.session_state["chat_history"] = []
                st.rerun()

# ── ZAKŁADKA: Pamięć ──────────────────────────────────────────────────────────
with tab_mem:
    st.caption(
        f"Agent uczy się na Anicie — {mem_count} zapisanych preferencji, faktów i wykluczeń. "
        "Pamięć przeżywa sesje (plik `data/anita_memory.json`)."
    )

    # ── Kopia zapasowa / przywracanie pamięci ─────────────────────────────────
    with st.expander("💾 Kopia zapasowa pamięci (pobierz / przywróć)", expanded=False):
        bk1, bk2 = st.columns(2)
        with bk1:
            st.markdown("**Pobierz kopię** — zapisz plik u siebie, żeby nigdy nie stracić danych.")
            st.download_button(
                "⬇️ Pobierz kopię pamięci (JSON)",
                data=export_memory_json().encode("utf-8"),
                file_name=f"anita_memory_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                mime="application/json",
                use_container_width=True,
            )
        with bk2:
            st.markdown("**Przywróć z pliku** — wpisy z kopii zostaną **dołączone** (nic nie kasuje).")
            restore_file = st.file_uploader(
                "Przywróć pamięć z pliku JSON", type=["json"],
                key="mem_restore", label_visibility="collapsed",
            )
            if restore_file is not None and st.button("⬆️ Przywróć (dołącz do obecnej)", use_container_width=True):
                try:
                    merged = import_memory(restore_file.getvalue(), merge=True)
                    n = (
                        len(merged.get("preferences", {}))
                        + len(merged.get("facts", []))
                        + len(merged.get("exclusions", {}).get("products", []))
                        + len(merged.get("exclusions", {}).get("suppliers", []))
                    )
                    st.success(f"✅ Przywrócono. Łącznie {n} wpisów w pamięci.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Nie udało się wczytać pliku: {e}")

    col_pref, col_facts = st.columns(2)
    with col_pref:
        st.markdown("**Preferencje (klucz=wartość):**")
        prefs = mem.get("preferences", {})
        if not prefs:
            st.caption("_Brak — agent nauczy się gdy będziesz mu mówić_ "
                       "_'zapamiętaj że...' albo 'preferuję...'_")
        for k, v in prefs.items():
            c1, c2 = st.columns([10, 1])
            c1.write(f"• `{k}` = **{v}**")
            if c2.button("🗑", key=f"rmpref_{k}"):
                remove_preference(k)
                st.rerun()
        with st.form(key="add_pref_form", clear_on_submit=True):
            pk = st.text_input("Klucz:", placeholder="np. minimum_BIACHEM_PLN")
            pv = st.text_input("Wartość:", placeholder="np. 5000")
            if st.form_submit_button("➕ Dodaj preferencję") and pk and pv:
                save_preference(pk.strip(), pv.strip())
                st.rerun()

    with col_facts:
        st.markdown("**Fakty / nawyki / reguły:**")
        facts = mem.get("facts", [])
        if not facts:
            st.caption("_Pusto — agent dodaje fakty automatycznie z rozmowy._")
        for i, f in enumerate(facts):
            c1, c2 = st.columns([10, 1])
            c1.write(f"• {f}")
            if c2.button("🗑", key=f"rmfact_{i}"):
                remove_fact(i)
                st.rerun()
        with st.form(key="add_fact_form", clear_on_submit=True):
            new_fact = st.text_input(
                "Dodaj fakt ręcznie:",
                placeholder="np. 'Anita woli zamawiać u BIACHEM w poniedziałki'",
            )
            if st.form_submit_button("➕ Dodaj fakt") and new_fact.strip():
                add_fact(new_fact.strip())
                st.rerun()

    st.divider()
    st.markdown("**🚫 Wykluczenia (agent ich NIE rekomenduje):**")
    exc = get_exclusions()
    col_exs, col_exp = st.columns(2)
    with col_exs:
        st.caption("Dostawcy")
        if not exc.get("suppliers"):
            st.caption("_Brak — powiedz agentowi 'pomijaj dostawcę X'_")
        for i, e in enumerate(exc.get("suppliers", [])):
            c1, c2 = st.columns([10, 1])
            r = f" — _{e['reason']}_" if e.get("reason") else ""
            c1.write(f"• **{e.get('value', '')}**{r}")
            if c2.button("🗑", key=f"rmexs_{i}"):
                remove_exclusion("suppliers", e.get("value", ""))
                st.rerun()
        with st.form(key="add_exs_form", clear_on_submit=True):
            exs_v = st.text_input("Wyklucz dostawcę:", placeholder="np. BIACHEM")
            if st.form_submit_button("➕ Wyklucz dostawcę") and exs_v.strip():
                add_exclusion("suppliers", exs_v.strip())
                st.rerun()
    with col_exp:
        st.caption("Produkty")
        if not exc.get("products"):
            st.caption("_Brak — powiedz agentowi 'nie zamawiaj produktu Y'_")
        for i, e in enumerate(exc.get("products", [])):
            c1, c2 = st.columns([10, 1])
            r = f" — _{e['reason']}_" if e.get("reason") else ""
            c1.write(f"• {e.get('value', '')}{r}")
            if c2.button("🗑", key=f"rmexp_{i}"):
                remove_exclusion("products", e.get("value", ""))
                st.rerun()
        with st.form(key="add_exp_form", clear_on_submit=True):
            exp_v = st.text_input("Wyklucz produkt:", placeholder="kod lub fragment nazwy")
            if st.form_submit_button("➕ Wyklucz produkt") and exp_v.strip():
                add_exclusion("products", exp_v.strip())
                st.rerun()

    if mem.get("history"):
        st.divider()
        st.markdown("**Ostatnie pytania (historia):**")
        for h in reversed(mem["history"][-10:]):
            st.caption(f"📅 {h.get('date', '')[:16]} → {h.get('question', '')[:120]}")

    with st.expander("🔧 Podgląd statycznego kontekstu wysyłanego do AI", expanded=False):
        st.caption(
            f"Długość: **{len(context):,} znaków** | "
            f"Linii: **{context.count(chr(10)) + 1}** | "
            "Agent ma też dostęp do narzędzi (query_products, get_dostawca_summary itp.) "
            "więc kontekst to tylko snapshot — szczegóły bierze przez tool use."
        )
        st.code(context[:5000] + ("\n...[ucięte]" if len(context) > 5000 else ""), language="text")

# ── ZAKŁADKA: Eksport ─────────────────────────────────────────────────────────
with tab_exp:
    st.markdown("**Pobierz wyniki jako pliki Excel.**")
    today = datetime.now().strftime("%Y%m%d")
    dl1, dl2 = st.columns(2)

    with dl1:
        try:
            full_bytes = generate_full_excel(analiza, zam_df, summary)
            st.download_button(
                label="📥 Pełna analiza (Excel)",
                data=full_bytes,
                file_name=f"AddAll_analiza_{today}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary", use_container_width=True,
            )
        except Exception as e:
            import traceback as _tb
            st.error(f"Błąd pliku: {type(e).__name__}: {e}")
            with st.expander("🔧 Pełny traceback (do diagnostyki)", expanded=False):
                st.code(_tb.format_exc(), language="text")

    with dl2:
        try:
            order_bytes = generate_order_excel(analiza, summary)
            st.download_button(
                label="📥 Lista zamówień (prosta)",
                data=order_bytes,
                file_name=f"AddAll_zamowienia_{today}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            import traceback as _tb
            st.error(f"Błąd pliku: {type(e).__name__}: {e}")
            with st.expander("🔧 Pełny traceback", expanded=False):
                st.code(_tb.format_exc(), language="text")

    st.info(
        "**Pełna analiza** — m.in. ZAMÓW DZIŚ, Zamów tydzień, "
        "nagłówek zamówień (w drodze), **Otwarte u dostawcy**, Top movers, Dead stock.\n\n"
        "**Lista zamówień** — uproszczony plik; kolumna „Otwarte u dostawcy” pokazuje dokumenty, "
        "do których można dorzucić pozycje."
    )

# ── Stopka ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"Add All Asystent Zakupowy v2.3 (Claude Sonnet 4.6 + minima logistyczne + uczenie) | "
    f"Dane analizy nie są zapisywane, pamięć agenta = `data/anita_memory.json` "
    f"(dla persystencji między deployami dodaj Railway Volume na `/app/data`) | "
    f"{datetime.now().strftime('%Y')}"
)
