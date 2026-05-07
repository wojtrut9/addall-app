"""
app.py — Add All Asystent Zakupowy
Interfejs Streamlit z dwoma trybami:
  1. iBiznes (auto) — pobiera dane bezpośrednio z MySQL iBiznes
  2. Pliki (fallback) — wgrywanie CSV/Excel ręcznie
"""
import os
from datetime import datetime

import streamlit as st

from engine import analyze
from excel_export import generate_full_excel, generate_order_excel

# ── Konfiguracja strony ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Add All — Asystent Zakupowy",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .main { padding: 1.5rem 2rem; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; }
    div[data-testid="stMetricDelta"] { font-size: 0.85rem; }
    .stTabs [data-baseweb="tab"] { font-size: 0.95rem; font-weight: 600; }
    .mode-badge {
        display: inline-block;
        padding: 0.2rem 0.7rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
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


# ── Nagłówek ──────────────────────────────────────────────────────────────────
st.markdown("# 📦 Add All — Asystent Zakupowy")
st.caption("Analizuje stany magazynowe i generuje rekomendacje zakupowe")
st.divider()

# ── Tryb: iBiznes vs Pliki ────────────────────────────────────────────────────
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

# ═══════════════════════════════════════════════════════════════════
# TRYB 1: iBiznes (automatyczny)
# ═══════════════════════════════════════════════════════════════════
if mode == "ibiznes":

    st.subheader("1. Połączenie z iBiznes")

    # URL ze secrets lub ręczne wpisanie
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

    col_days, col_test, col_run = st.columns([2, 2, 3])

    with col_days:
        days = st.number_input(
            "Okres analizy (dni wstecz)",
            min_value=7,
            max_value=365,
            value=90,
            step=7,
            help="Ile dni wstecz pobrać dane o obrotach magazynowych. Zalecane: 60-90 dni.",
        )

    with col_test:
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

    with col_run:
        run_ibiznes = st.button(
            "⚡ Pobierz dane z iBiznes i analizuj",
            type="primary",
            disabled=not db_url_input,
            use_container_width=True,
        )

    if run_ibiznes and db_url_input:
        with st.spinner(f"Łączę się z iBiznes i pobieram dane za ostatnie {days} dni…"):
            try:
                from ibiznes_connector import fetch_all, identify_tables, get_connection

                # Najpierw pokaż dostępne tabele (pomocne przy pierwszym uruchomieniu)
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

                # Pobierz dane (5 elementów: kartoteka, obroty, zamowienia-header,
                # in_transit-per-SKU, tbl_info)
                kartoteka_df, obroty_df, zamowienia_df, in_transit_df, _ = fetch_all(db_url_input, days=days)

                in_transit_count = len(in_transit_df) if in_transit_df is not None else 0
                st.caption(
                    f"Pobrano: {len(kartoteka_df)} aktywnych produktów (kartoteka po filtrze Akt), "
                    f"{len(obroty_df)} ruchów magazynowych, "
                    f"{len(zamowienia_df)} otwartych zamówień (header), "
                    f"{in_transit_count} pozycji 'w drodze' (per SKU)"
                )

                # Konwertuj na format plikowy i przekaż do engine
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
                )

                st.session_state.update({
                    "analiza": analiza,
                    "zam_df":  zam_result,
                    "summary": summary,
                    "context": context,
                    "chat_history": [],
                    "data_source": "ibiznes",
                })
                st.success("✅ Dane pobrane i przeanalizowane!")

            except Exception as exc:
                st.error(f"❌ Błąd: {exc}")
                st.info(
                    "Wskazówka: upewnij się że IBIZNES_DB_URL jest poprawny "
                    "i że serwer MySQL iBiznes jest dostępny z sieci Railway/internet."
                )
                st.stop()

# ═══════════════════════════════════════════════════════════════════
# TRYB 2: Pliki (ręczny fallback)
# ═══════════════════════════════════════════════════════════════════
else:
    st.subheader("1. Wgraj pliki z iBiznes")
    st.caption(
        "Wymagane: Kartoteka + Obroty. Opcjonalne: Zamówienia + Minima logistyczne.\n"
        "Eksportuj z iBiznes: Magazyn → Kartoteka / Obroty / Zamówienia → Eksportuj CSV"
    )

    c1, c2, c3, c4 = st.columns(4)

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

    with c3:
        st.markdown("**🚚 Zamówienia** *(opcjonalne)*")
        zam_file = st.file_uploader(
            "zamowienia", type=["csv", "xlsx", "xls"],
            key="zam", label_visibility="collapsed",
        )
        if zam_file:
            st.success(f"✅ {zam_file.name}")
        else:
            st.caption("ZamówieniaDlaDostawcy.csv")

    with c4:
        st.markdown("**📏 Min. logistyczne** *(opcjonalne)*")
        min_log_file = st.file_uploader(
            "minima", type=["csv", "xlsx", "xls"],
            key="min_log", label_visibility="collapsed",
        )
        if min_log_file:
            st.success(f"✅ {min_log_file.name}")
        else:
            st.caption("Dostawca | Min. wartość PLN")

    st.divider()

    col_btn, col_msg = st.columns([2, 5])
    with col_btn:
        run_files = st.button(
            "▶ Analizuj pliki",
            type="primary",
            use_container_width=True,
            disabled=(not locals().get("kart_file") or not locals().get("obroty_file")),
        )
    with col_msg:
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
                st.success("✅ Analiza gotowa!")
            except Exception as exc:
                st.error(f"❌ Błąd analizy: {exc}")
                st.stop()

# ── Wyniki (wspólne dla obu trybów) ──────────────────────────────────────────
if "analiza" not in st.session_state:
    st.stop()

analiza = st.session_state["analiza"]
zam_df  = st.session_state["zam_df"]
summary = st.session_state["summary"]
context = st.session_state["context"]
source  = st.session_state.get("data_source", "pliki")

source_label = "⚡ iBiznes (live)" if source == "ibiznes" else "📁 Pliki"
st.caption(f"Źródło danych: {source_label}")

# ── Karty podsumowania ────────────────────────────────────────────────────────
st.subheader("2. Podsumowanie dnia")
st.caption(
    f"Dane: {summary['data_od']} — {summary['data_do']} "
    f"({summary['dni_okresu']} dni) | Wygenerowano: {summary['data_analizy']}"
)

m1, m2, m3, m4, m5, m6 = st.columns(6)
with m1:
    st.metric(
        "💰 Magazyn (aktywne)",
        fmt_pln(summary["wartosc_magazynu"]),
        f"cały: {fmt_pln(summary.get('wartosc_calego_magazynu', summary['wartosc_magazynu']))}",
        delta_color="off",
    )
with m2:
    st.metric(
        "🚚 W drodze",
        f"{summary.get('produktow_w_drodze', 0)} poz.",
        fmt_pln(summary.get("wartosc_w_drodze", 0)),
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

st.divider()

# ── Pobierz pliki Excel ───────────────────────────────────────────────────────
st.subheader("3. Pobierz pliki Excel")

today = datetime.now().strftime("%Y%m%d")
dl1, dl2, dl3 = st.columns([2, 2, 3])

with dl1:
    try:
        full_bytes = generate_full_excel(analiza, zam_df, summary)
        st.download_button(
            label="📥 Pełna analiza (6 arkuszy)",
            data=full_bytes,
            file_name=f"AddAll_analiza_{today}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary", use_container_width=True,
        )
    except Exception as e:
        st.error(f"Błąd pliku: {e}")

with dl2:
    try:
        order_bytes = generate_order_excel(analiza)
        st.download_button(
            label="📥 Lista zamówień (prosta)",
            data=order_bytes,
            file_name=f"AddAll_zamowienia_{today}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Błąd pliku: {e}")

with dl3:
    st.info(
        "**Pełna analiza** — 6 arkuszy: ZAMÓW DZIŚ, Zamów tydzień, "
        "W drodze, Top movers, Dead stock, Pełna analiza.\n\n"
        "**Lista zamówień** — uproszczony plik do iBiznes lub dostawców."
    )

st.divider()

# ── Tabele wyników ────────────────────────────────────────────────────────────
st.subheader("4. Wyniki analizy")

tab_dzis, tab_tydz, tab_droga, tab_top, tab_dead = st.tabs([
    "🚨 Zamów DZIŚ", "🟡 Zamów w tygodniu",
    "🔵 W drodze", "📈 Top movers", "⚫ Dead stock",
])

nazwa_col = find_col(analiza, "nazwa towaru")
kod_col   = find_col(analiza, "kod towaru / usługi", "kod towaru")
dos_col   = find_col(analiza, "dostawca")

display_cols = [c for c in [
    kod_col, nazwa_col, dos_col,
    "Stan", "w_drodze", "Stan Min.", "srednie_dzienne",
    "dni_do_wyczerpania", "ile_zamowic", "wartosc_zamowienia",
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
}


def show_table(df, cols, extra_rename=None):
    avail   = [c for c in cols if c in df.columns]
    rename  = {**col_labels, **(extra_rename or {})}
    st.dataframe(df[avail].rename(columns=rename), use_container_width=True, hide_index=True)


with tab_dzis:
    dzis = analiza[analiza["status"] == "ZAMÓW DZIŚ"].sort_values("dni_do_wyczerpania")
    if len(dzis) == 0:
        st.success("🎉 Brak produktów do pilnego zamówienia!")
    else:
        st.error(
            f"**{len(dzis)} produktów wymaga zamówienia DZIŚ** "
            f"— łącznie {fmt_pln(dzis['wartosc_zamowienia'].sum())}"
        )
        if dos_col:
            for dostawca, grupa in dzis.groupby(dos_col):
                razem  = grupa["wartosc_zamowienia"].sum()
                min_v  = summary["min_log"].get(str(dostawca).upper(), 0)
                status = (
                    f"⚠️ brakuje {fmt_pln(min_v - razem)} do minimum"
                    if min_v > 0 and razem < min_v
                    else ("✅ minimum OK" if min_v > 0 else "")
                )
                label = f"🏭 {dostawca} — {fmt_pln(razem)}"
                if status:
                    label += f"  |  {status}"
                with st.expander(label, expanded=True):
                    show_table(grupa, display_cols)
        else:
            show_table(dzis, display_cols)

with tab_tydz:
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
                with st.expander(
                    f"🏭 {dostawca} — {fmt_pln(grupa['wartosc_zamowienia'].sum())}",
                    expanded=False,
                ):
                    show_table(grupa, display_cols)
        else:
            show_table(tydzien, display_cols)

with tab_droga:
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

with tab_top:
    top = analiza[analiza["srednie_dzienne"] > 0].nlargest(20, "srednie_dzienne")
    top_cols = [c for c in [
        kod_col, nazwa_col, dos_col,
        "srednie_dzienne", "Stan", "dni_do_wyczerpania", "marza_pct",
    ] if c and c in analiza.columns]
    show_table(top, top_cols, {"marza_pct": "Marża %"})

with tab_dead:
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

st.divider()

# ── Chat AI ───────────────────────────────────────────────────────────────────
st.subheader("5. Zapytaj AI o analizę")
st.caption(
    "AI odpowiada na podstawie właśnie przeliczonych danych. "
    "Np. 'Co zamówić u BIACHEM?', 'Dlaczego 60 szt?', 'Top klienci'."
)

api_key = get_secret("OPENAI_API_KEY")
if not api_key:
    api_key = st.text_input(
        "🔑 Klucz API OpenAI:",
        type="password",
        placeholder="sk-...",
        help="Zapisz jako OPENAI_API_KEY w Railway → Variables",
    )

# Diagnostyka kontekstu — pomaga jeśli AI mówi "brak danych".
with st.expander("🔧 Diagnostyka kontekstu wysyłanego do AI", expanded=False):
    st.caption(
        f"Długość kontekstu: **{len(context):,} znaków** | "
        f"Linii: **{context.count(chr(10)) + 1}** | "
        "Model używa pełnego kontekstu — to powinno wystarczyć aby AI poprawnie odpowiedział."
    )
    st.code(context[:5000] + ("\n...[ucięte]" if len(context) > 5000 else ""), language="text")

if not api_key:
    st.info("Wpisz klucz API OpenAI żeby włączyć chat (~1-3 grosze za pytanie).")
else:
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # Wybór modelu — gpt-4o jest wyraźnie inteligentniejszy niż mini przy
    # długich, ustrukturyzowanych kontekstach po polsku.
    model_choice = st.selectbox(
        "Model AI:",
        options=["gpt-4o-mini", "gpt-4o", "gpt-4.1", "gpt-4.1-mini"],
        index=1,
        help=(
            "gpt-4o — najlepsza jakość, ~5 gr/pytanie. "
            "gpt-4o-mini — tańszy (~1 gr/pytanie) ale czasem zbyt ostrożny przy długim kontekście."
        ),
    )

    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    st.markdown("**Szybkie pytania:**")
    qcols = st.columns(4)
    quick_qs = [
        "Co zamówić pilnie dziś?",
        "Pokaż zamówienia per dostawca",
        "Który dostawca ma najwięcej produktów krytycznych?",
        "Produkty z marżą poniżej 20%?",
    ]
    for i, (qcol, q) in enumerate(zip(qcols, quick_qs)):
        with qcol:
            if st.button(q, key=f"quick_{i}", use_container_width=True):
                st.session_state["_pending_q"] = q
                st.rerun()

    pending_q = st.session_state.pop("_pending_q", None)
    user_input = st.chat_input("Zadaj pytanie, np. 'Co zamówić u ADEKS?'")
    question = user_input or pending_q

    if question:
        st.session_state["chat_history"].append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Myślę…"):
                # System prompt — jednoznaczny, zachęca do AKTYWNEGO korzystania
                # z danych poniżej. Wcześniej prompt mówił "nigdy nie zmyślaj"
                # przez co model bywał paniczny i odpowiadał "brak danych" mimo
                # że miał komplet w kontekście.
                system_prompt = (
                    "Jesteś asystentem zakupowym firmy Add All — dystrybutora chemii, "
                    "opakowań i artykułów higienicznych dla HoReCa. Mówisz po polsku. "
                    "Waluta: PLN.\n\n"
                    "TWOJE ZADANIE:\n"
                    "Aktywnie korzystaj z DANYCH ANALIZY poniżej, aby odpowiadać na pytania "
                    "Anity (kupiec). Cytuj liczby DOKŁADNIE jak w sekcjach [SEKCJA: ...]. "
                    "Jeżeli dana sekcja jest niepusta — wymień konkretne pozycje, dostawców, "
                    "kwoty i ilości. Nie odpowiadaj 'brak informacji' jeśli widzisz dane — "
                    "po prostu je przedstaw.\n\n"
                    "ZASADY:\n"
                    "- Nie wymyślaj liczb spoza danych. Jeśli czegoś rzeczywiście nie ma w "
                    "  sekcjach, powiedz wprost 'tej informacji nie ma w analizie'.\n"
                    "- Odpowiedzi krótkie, konkretne, z listą wypunktowaną gdy to pomaga.\n"
                    "- Sumy podawaj jako: '12 345 PLN' (spacja jako separator tysięcy).\n"
                    "- Sortuj rekomendacje wg priorytetu (najpierw 'Zamów dziś', potem tydzień).\n"
                    "- Jeśli pytanie dotyczy zamówień, ZAWSZE wspomnij ile już jedzie 'w drodze' "
                    "  (sekcja [SEKCJA: W DRODZE OD DOSTAWCÓW]).\n\n"
                    "=== DANE ANALIZY ===\n"
                    f"{context}\n"
                    "=== KONIEC DANYCH ==="
                )

                messages = [{"role": "system", "content": system_prompt}]
                for msg in st.session_state["chat_history"][-8:]:
                    messages.append({"role": msg["role"], "content": msg["content"]})

                # Próba: wybrany model → fallback do gpt-4o-mini przy błędzie.
                from openai import OpenAI
                client = OpenAI(api_key=api_key)

                answer = None
                last_err = None
                for model_try in (model_choice, "gpt-4o-mini"):
                    try:
                        response = client.chat.completions.create(
                            model=model_try,
                            messages=messages,
                            temperature=0.2,
                            max_tokens=1500,
                        )
                        answer = response.choices[0].message.content
                        if model_try != model_choice:
                            st.caption(f"⚠️ Fallback na {model_try} (błąd przy {model_choice}: {last_err})")
                        break
                    except Exception as exc:
                        last_err = str(exc)
                        continue

                if answer:
                    st.write(answer)
                    st.session_state["chat_history"].append(
                        {"role": "assistant", "content": answer}
                    )
                else:
                    st.error(
                        f"❌ Błąd API OpenAI: {last_err}\n\n"
                        "Najczęstsze przyczyny:\n"
                        "- nieprawidłowy klucz `OPENAI_API_KEY`\n"
                        "- brak środków na koncie OpenAI (billing)\n"
                        "- limit zapytań (rate limit)\n"
                        "- model niedostępny w Twoim regionie/koncie"
                    )

    if st.session_state.get("chat_history"):
        if st.button("🗑 Wyczyść chat", key="clear_chat"):
            st.session_state["chat_history"] = []
            st.rerun()

# ── Stopka ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"Add All Asystent Zakupowy v1.1 | "
    f"Dane nie są zapisywane | {datetime.now().strftime('%Y')}"
)
