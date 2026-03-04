"""
app.py — Add All Asystent Zakupowy
Interfejs Streamlit: wgrywanie plików, analiza, pobieranie Excela, chat AI.
"""
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
    .supplier-header {
        background: #f0f2f6;
        border-radius: 8px;
        padding: 0.4rem 0.8rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Nagłówek ─────────────────────────────────────────────────────────────────
st.markdown("# 📦 Add All — Asystent Zakupowy")
st.caption("Wgraj pliki z iBiznes → kliknij Analizuj → pobierz rekomendacje jako Excel")
st.divider()


# ── Pomocnicze ───────────────────────────────────────────────────────────────
def fmt_pln(value):
    """Formatuje liczbę jako kwotę PLN z separatorem tysięcy (spacja)."""
    return f"{value:,.0f} PLN".replace(",", " ")


def find_col(df, *hints):
    for hint in hints:
        m = next((c for c in df.columns if hint.lower() in c.lower()), None)
        if m:
            return m
    return None


# ── 1. Wgrywanie plików ──────────────────────────────────────────────────────
st.subheader("1. Wgraj pliki z iBiznes")
st.caption("Wymagane: Kartoteka + Obroty. Opcjonalne: Zamówienia + Minima logistyczne.")

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
        st.info("KartotekaTowarowiUslug.csv / .xlsx")

with c2:
    st.markdown("**📊 Obroty magazynowe** *(wymagane)*")
    obroty_file = st.file_uploader(
        "obroty", type=["csv", "xlsx", "xls"],
        key="obroty", label_visibility="collapsed",
    )
    if obroty_file:
        st.success(f"✅ {obroty_file.name}")
    else:
        st.info("magazyn obroty wszystko.csv / .xlsx")

with c3:
    st.markdown("**🚚 Zamówienia do dostawców** *(opcjonalne)*")
    zam_file = st.file_uploader(
        "zamowienia", type=["csv", "xlsx", "xls"],
        key="zam", label_visibility="collapsed",
    )
    if zam_file:
        st.success(f"✅ {zam_file.name}")
    else:
        st.caption("ZamówieniaDlaDostawcy.csv / .xlsx")

with c4:
    st.markdown("**📏 Minima logistyczne** *(opcjonalne)*")
    min_log_file = st.file_uploader(
        "minima", type=["csv", "xlsx", "xls"],
        key="min_log", label_visibility="collapsed",
    )
    if min_log_file:
        st.success(f"✅ {min_log_file.name}")
    else:
        st.caption("Dostawca | Min. wartość PLN")

st.divider()

# ── 2. Przycisk analizy ───────────────────────────────────────────────────────
col_btn, col_msg = st.columns([2, 5])
with col_btn:
    run = st.button(
        "▶  Analizuj",
        type="primary",
        use_container_width=True,
        disabled=(kart_file is None or obroty_file is None),
    )
with col_msg:
    if kart_file is None or obroty_file is None:
        st.warning("Wgraj co najmniej Kartotekę i Obroty, żeby zacząć.")

if run:
    with st.spinner("Analizuję dane… (może potrwać chwilę przy dużych plikach)"):
        try:
            analiza, zam_df, summary, context = analyze(
                kart_file, obroty_file, zam_file, min_log_file
            )
            st.session_state.update({
                "analiza": analiza,
                "zam_df": zam_df,
                "summary": summary,
                "context": context,
                "chat_history": [],
            })
            st.success("✅ Analiza gotowa! Przewiń w dół po wyniki.")
        except Exception as exc:
            st.error(f"❌ Błąd analizy: {exc}")
            st.stop()

# ── Wyniki (tylko gdy analiza gotowa) ────────────────────────────────────────
if "analiza" not in st.session_state:
    st.stop()

analiza = st.session_state["analiza"]
zam_df  = st.session_state["zam_df"]
summary = st.session_state["summary"]
context = st.session_state["context"]

# ── 3. Karty podsumowania ─────────────────────────────────────────────────────
st.subheader("2. Podsumowanie dnia")
st.caption(
    f"Dane: {summary['data_od']} — {summary['data_do']} "
    f"({summary['dni_okresu']} dni) | Wygenerowano: {summary['data_analizy']}"
)

m1, m2, m3, m4, m5 = st.columns(5)

with m1:
    st.metric("💰 Wartość magazynu", fmt_pln(summary["wartosc_magazynu"]))
with m2:
    st.metric(
        "🚨 Zamów DZIŚ",
        f"{summary['produktow_dzis']} pozycji",
        f"≈ {fmt_pln(summary['wartosc_dzis'])}",
    )
with m3:
    st.metric(
        "🟡 Zamów w tygodniu",
        f"{summary['produktow_tydzien']} pozycji",
        f"≈ {fmt_pln(summary['wartosc_tydzien'])}",
    )
with m4:
    st.metric(
        "📦 Aktywnych produktów",
        summary["produktow_aktywnych"],
        f"z {summary['produktow_total']} w bazie",
    )
with m5:
    st.metric(
        "⚫ Dead stock",
        f"{summary['dead_stock_produktow']} prod.",
        fmt_pln(summary["dead_stock_wartosc"]),
        delta_color="inverse",
    )

st.divider()

# ── 4. Pobieranie plików ──────────────────────────────────────────────────────
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
            type="primary",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Błąd generowania pliku: {e}")

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
        st.error(f"Błąd generowania pliku: {e}")

with dl3:
    st.info(
        "💡 **Pełna analiza** zawiera 6 arkuszy: ZAMÓW DZIŚ, Zamów tydzień, "
        "W drodze, Top movers, Dead stock, Pełna analiza.\n\n"
        "**Lista zamówień** to uproszczony plik do wgrania do iBiznes lub "
        "wysłania dostawcom."
    )

st.divider()

# ── 5. Tabele wyników ─────────────────────────────────────────────────────────
st.subheader("4. Wyniki analizy")

tab_dzis, tab_tydz, tab_droga, tab_top, tab_dead = st.tabs([
    "🚨 Zamów DZIŚ",
    "🟡 Zamów w tygodniu",
    "🔵 W drodze",
    "📈 Top movers",
    "⚫ Dead stock",
])

# Kolumny do wyświetlenia
nazwa_col = find_col(analiza, "nazwa towaru")
kod_col   = find_col(analiza, "kod towaru / usługi", "kod towaru")
dos_col   = find_col(analiza, "dostawca")

display_cols = [c for c in [
    kod_col, nazwa_col, dos_col,
    "Stan", "Stan Min.", "srednie_dzienne",
    "dni_do_wyczerpania", "ile_zamowic", "wartosc_zamowienia",
] if c and c in analiza.columns]

col_labels = {
    "srednie_dzienne":    "Zuż/dzień",
    "dni_do_wyczerpania": "Starczy (dni)",
    "ile_zamowic":        "Zamów (szt)",
    "wartosc_zamowienia": "Wartość PLN",
    "wartosc_stanu":      "Wartość stanu PLN",
    "marza_pct":          "Marża %",
}


def show_table(df, cols, label_map=None):
    available = [c for c in cols if c in df.columns]
    renamed = df[available].rename(columns=label_map or col_labels)
    st.dataframe(renamed, use_container_width=True, hide_index=True)


# ── Tab: Zamów DZIŚ ──────────────────────────────────────────────────────────
with tab_dzis:
    dzis = (
        analiza[analiza["status"] == "ZAMÓW DZIŚ"]
        .sort_values("dni_do_wyczerpania")
    )
    if len(dzis) == 0:
        st.success("🎉 Brak produktów do pilnego zamówienia!")
    else:
        st.error(
            f"**{len(dzis)} produktów wymaga zamówienia DZIŚ** "
            f"— łączna wartość: {fmt_pln(dzis['wartosc_zamowienia'].sum())}"
        )

        if dos_col:
            for dostawca, grupa in dzis.groupby(dos_col):
                razem = grupa["wartosc_zamowienia"].sum()
                min_v = summary["min_log"].get(str(dostawca).upper(), 0)

                if min_v > 0 and razem < min_v:
                    status_min = f"⚠️ Brakuje {fmt_pln(min_v - razem)} do minimum logistycznego ({fmt_pln(min_v)})"
                    expanded = True
                else:
                    status_min = "✅ Minimum logistyczne spełnione" if min_v > 0 else ""
                    expanded = True

                label = f"🏭 {dostawca} — {fmt_pln(razem)}"
                if status_min:
                    label += f"  |  {status_min}"

                with st.expander(label, expanded=expanded):
                    show_table(grupa, display_cols)
        else:
            show_table(dzis, display_cols)

# ── Tab: Zamów w tygodniu ────────────────────────────────────────────────────
with tab_tydz:
    tydzien = (
        analiza[analiza["status"] == "ZAMÓW TYDZIEŃ"]
        .sort_values("dni_do_wyczerpania")
    )
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

# ── Tab: W drodze ────────────────────────────────────────────────────────────
with tab_droga:
    if zam_df is None or len(zam_df) == 0:
        st.info(
            "Nie wgrano pliku z zamówieniami do dostawców "
            "lub plik jest pusty.\n\n"
            "Wgraj **ZamówieniaDlaDostawcy.csv** żeby zobaczyć co jest w drodze."
        )
    else:
        clean = zam_df.drop(columns=["_data_realiz"], errors="ignore")
        st.dataframe(clean, use_container_width=True, hide_index=True)

# ── Tab: Top movers ──────────────────────────────────────────────────────────
with tab_top:
    top = analiza[analiza["srednie_dzienne"] > 0].nlargest(20, "srednie_dzienne")
    top_cols = [c for c in [
        kod_col, nazwa_col, dos_col,
        "srednie_dzienne", "Stan", "dni_do_wyczerpania", "marza_pct",
    ] if c and c in analiza.columns]
    show_table(top, top_cols, col_labels | {"marza_pct": "Marża %"})

# ── Tab: Dead stock ──────────────────────────────────────────────────────────
with tab_dead:
    dead = (
        analiza[analiza["status"] == "DEAD STOCK"]
        .sort_values("wartosc_stanu", ascending=False)
    )
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
        show_table(dead, dead_cols, col_labels | {"wartosc_stanu": "Wartość stanu PLN"})

st.divider()

# ── 6. Chat AI ────────────────────────────────────────────────────────────────
st.subheader("5. Zapytaj AI o analizę")
st.caption(
    "AI odpowiada na podstawie właśnie przeliczonych danych — nie zgaduje. "
    "Np. 'Co zamówić u BIACHEM?', 'Dlaczego rekomendujesz 60 szt?', 'Top klienci'."
)

# Pobierz klucz API (Streamlit secrets lub ręczne wpisanie)
api_key = None
try:
    api_key = st.secrets.get("OPENAI_API_KEY") or st.secrets.get("openai_api_key")
except Exception:
    pass

if not api_key:
    api_key = st.text_input(
        "🔑 Klucz API OpenAI (tylko raz na sesję):",
        type="password",
        placeholder="sk-...",
        help=(
            "Pobierz na platform.openai.com → API keys. "
            "Lub zapisz w pliku .streamlit/secrets.toml: OPENAI_API_KEY = 'sk-...'"
        ),
    )

if not api_key:
    st.info(
        "Wpisz klucz API OpenAI powyżej żeby włączyć chat. "
        "Koszt: ok. 1-3 grosze za pytanie (model gpt-4o-mini)."
    )
else:
    # Historia czatu
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # Wyświetl historię
    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # Szybkie pytania
    st.markdown("**Szybkie pytania:**")
    qcols = st.columns(4)
    quick_qs = [
        "Co zamówić pilnie dziś?",
        "Pokaż zamówienia per dostawca z wartościami",
        "Który dostawca ma najwięcej produktów krytycznych?",
        "Jakie produkty mają marżę poniżej 20%?",
    ]
    for i, (qcol, q) in enumerate(zip(qcols, quick_qs)):
        with qcol:
            if st.button(q, key=f"quick_{i}", use_container_width=True):
                st.session_state["_pending_q"] = q
                st.rerun()

    # Odbierz oczekujące szybkie pytanie
    pending_q = st.session_state.pop("_pending_q", None)

    user_input = st.chat_input(
        "Zadaj pytanie, np. 'Co zamówić u ADEKS?' lub 'Analiza produktu Bacticid AF'"
    )
    question = user_input or pending_q

    if question:
        st.session_state["chat_history"].append(
            {"role": "user", "content": question}
        )
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Myślę…"):
                try:
                    from openai import OpenAI  # lazy import

                    client = OpenAI(api_key=api_key)

                    system_prompt = (
                        "Jesteś asystentem magazynowym i zakupowym firmy Add All — "
                        "dystrybutora chemii, opakowań i artykułów higienicznych dla HoReCa. "
                        "Odpowiadasz WYŁĄCZNIE po polsku. "
                        "Waluta: PLN, liczby z separatorem tysięcy (spacja). "
                        "Odpowiedzi konkretne i zwięzłe — bez zbędnych wstępów. "
                        "Korzystasz TYLKO z danych poniżej — nigdy nie zmyślasz liczb.\n\n"
                        f"DANE Z ANALIZY:\n{context}"
                    )

                    messages = [{"role": "system", "content": system_prompt}]
                    # Ostatnie 8 wiadomości jako historia
                    for msg in st.session_state["chat_history"][-8:]:
                        messages.append({"role": msg["role"], "content": msg["content"]})

                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=messages,
                        temperature=0.2,
                        max_tokens=1200,
                    )
                    answer = response.choices[0].message.content
                    st.write(answer)
                    st.session_state["chat_history"].append(
                        {"role": "assistant", "content": answer}
                    )

                except ImportError:
                    st.error(
                        "Brak biblioteki openai. Uruchom: pip install openai"
                    )
                except Exception as exc:
                    st.error(f"Błąd API OpenAI: {exc}")

    # Przycisk czyszczenia historii
    if st.session_state.get("chat_history"):
        if st.button("🗑 Wyczyść historię chatu", key="clear_chat"):
            st.session_state["chat_history"] = []
            st.rerun()

# ── Stopka ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Add All Asystent Zakupowy | "
    "Dane przetwarzane lokalnie — pliki nie są nigdzie zapisywane | "
    f"Wersja 1.0 — {datetime.now().strftime('%Y')}"
)
