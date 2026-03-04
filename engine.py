"""
engine.py — Add All Inventory Analysis Engine
Logika analizy: czyta pliki, liczy metryki, generuje rekomendacje.
"""
import io
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Polskie nazwy miesięcy → numery
MONTH_MAP = {
    "sty": "01", "lut": "02", "mar": "03", "kwi": "04",
    "maj": "05", "cze": "06", "lip": "07", "sie": "08",
    "wrz": "09", "paź": "10", "paz": "10", "lis": "11", "gru": "12",
}


def parse_polish_date(s):
    """Parsuje datę w formacie '17 lut 2026' na datetime."""
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip().lower()
    for pl, num in MONTH_MAP.items():
        s = s.replace(pl, num)
    for fmt in ["%d %m %Y", "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return pd.NaT


def read_uploaded_file(uploaded_file):
    """Czyta plik CSV lub Excel wgrany przez Streamlit (auto-encoding dla CSV)."""
    name = uploaded_file.name.lower()

    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file, dtype=str)

    # CSV — próbuj kolejne encodingi
    raw = uploaded_file.read()
    for enc in ["cp1250", "utf-8", "utf-8-sig", "iso-8859-2", "latin-1"]:
        try:
            df = pd.read_csv(
                io.BytesIO(raw),
                sep=";",
                encoding=enc,
                on_bad_lines="skip",
                low_memory=False,
                dtype=str,
            )
            if len(df.columns) > 2:
                return df
        except Exception:
            continue

    # Spróbuj z przecinkiem jako separatorem
    for enc in ["cp1250", "utf-8"]:
        try:
            df = pd.read_csv(
                io.BytesIO(raw),
                sep=",",
                encoding=enc,
                on_bad_lines="skip",
                low_memory=False,
                dtype=str,
            )
            if len(df.columns) > 2:
                return df
        except Exception:
            continue

    raise ValueError(f"Nie można wczytać pliku: {uploaded_file.name}")


def fix_numeric(df, cols):
    """Konwertuje polskie liczby dziesiętne (przecinek→kropka) na float."""
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("\xa0", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def find_col(df, *hints):
    """Elastyczne znajdowanie kolumny po fragmencie nazwy."""
    for hint in hints:
        match = next(
            (c for c in df.columns if hint.lower() in c.lower()), None
        )
        if match:
            return match
    return None


def analyze(kart_file, obroty_file, zam_file=None, min_log_file=None):
    """
    Główna funkcja analizy.

    Returns:
        analiza (DataFrame), zam_df (DataFrame|None),
        summary (dict), context (str)
    """

    # ── 1. Wczytaj pliki ─────────────────────────────────────────────────────
    kart = read_uploaded_file(kart_file)
    obroty = read_uploaded_file(obroty_file)

    # ── 2. Oczyść kartotekę ──────────────────────────────────────────────────
    kart_num_cols = [
        "Stan", "Cena zakupu netto", "Cena Podstawowa netto",
        "Obroty za 60 dni", "Zapas (dni)", "Stan Min.",
        "Zamówić (Min.)", "Wart. zakupu netto",
    ]
    kart = fix_numeric(kart, kart_num_cols)

    kod_col = find_col(kart, "kod towaru / usługi", "kod towaru")
    if kod_col is None:
        raise ValueError(
            "Nie znaleziono kolumny z kodem towaru w kartotece. "
            "Sprawdź czy wgrałaś właściwy plik."
        )

    # ── 3. Oczyść obroty ────────────────────────────────────────────────────
    obroty_num_cols = [
        "Rozchód", "Przychód", "Cena netto PLN", "Cena zakupu PLN",
        "Wartość netto", "Zysk PLN", "Zysk",
    ]
    obroty = fix_numeric(obroty, obroty_num_cols)

    # Parsuj daty
    data_col = find_col(obroty, "data wydania", "data")
    if data_col is None:
        raise ValueError("Nie znaleziono kolumny z datą w pliku obrotów.")
    obroty["_Data"] = obroty[data_col].apply(parse_polish_date)

    valid_dates = obroty["_Data"].dropna()
    if len(valid_dates) == 0:
        raise ValueError(
            "Nie można sparsować dat w pliku obrotów. "
            "Sprawdź format — powinien być np. '17 lut 2026'."
        )

    date_min = valid_dates.min()
    date_max = valid_dates.max()
    dni_okresu = max((date_max - date_min).days + 1, 1)

    # ── 4. Analiza WZ (wydań) ────────────────────────────────────────────────
    typ_col = find_col(obroty, "typ")
    kod_obroty_col = find_col(obroty, "kod towaru")
    klient_col = find_col(obroty, "klient")

    if kod_obroty_col is None:
        raise ValueError("Nie znaleziono kolumny z kodem towaru w pliku obrotów.")

    if typ_col:
        wydania = obroty[
            obroty[typ_col].astype(str).str.strip().str.upper() == "WZ"
        ].copy()
    else:
        wydania = obroty.copy()

    # Agregacja per produkt
    agg_dict = {
        "ilosc_sprzedana": ("Rozchód", "sum"),
        "liczba_transakcji": ("Rozchód", "count"),
        "ostatnia_sprzedaz": ("_Data", "max"),
    }
    if "Wartość netto" in wydania.columns:
        agg_dict["wartosc_sprzedana"] = ("Wartość netto", "sum")
    if "Zysk" in wydania.columns:
        agg_dict["zysk"] = ("Zysk", "sum")
    if klient_col:
        agg_dict["rozni_klienci"] = (klient_col, "nunique")

    obrot = wydania.groupby(kod_obroty_col).agg(**agg_dict).reset_index()
    obrot.rename(columns={kod_obroty_col: "_kod_towaru"}, inplace=True)
    obrot["srednie_dzienne"] = (obrot["ilosc_sprzedana"] / dni_okresu).round(3)

    # ── 5. Połącz z kartoteką ────────────────────────────────────────────────
    analiza = kart.merge(
        obrot, left_on=kod_col, right_on="_kod_towaru", how="left"
    )

    fill_cols = ["ilosc_sprzedana", "srednie_dzienne", "liczba_transakcji"]
    for extra in ["wartosc_sprzedana", "zysk", "rozni_klienci"]:
        if extra in analiza.columns:
            fill_cols.append(extra)
    for col in fill_cols:
        analiza[col] = analiza[col].fillna(0)

    # ── 6. Kluczowe metryki ──────────────────────────────────────────────────
    analiza["dni_do_wyczerpania"] = np.where(
        analiza["srednie_dzienne"] > 0,
        (analiza["Stan"] / analiza["srednie_dzienne"]).round(1),
        9999.0,
    )

    analiza["marza_pct"] = np.where(
        analiza["Cena Podstawowa netto"] > 0,
        (
            (analiza["Cena Podstawowa netto"] - analiza["Cena zakupu netto"])
            / analiza["Cena Podstawowa netto"]
            * 100
        ).round(1),
        0.0,
    )

    analiza["wartosc_stanu"] = (
        analiza["Stan"] * analiza["Cena zakupu netto"]
    ).round(2)

    # Rekomendowana ilość zamówienia: prognoza 30d + safety 7d - stan
    analiza["prognoza_30d"] = (analiza["srednie_dzienne"] * 30).round(0)
    analiza["safety_7d"] = (analiza["srednie_dzienne"] * 7).round(0)
    analiza["ile_zamowic"] = np.where(
        analiza["srednie_dzienne"] > 0,
        np.maximum(
            0,
            analiza["prognoza_30d"] + analiza["safety_7d"] - analiza["Stan"],
        ).round(0),
        0,
    )
    # Limit: max 2× miesięczne zużycie
    analiza["ile_zamowic"] = np.minimum(
        analiza["ile_zamowic"],
        (analiza["srednie_dzienne"] * 60).round(0),
    ).astype(int)

    analiza["wartosc_zamowienia"] = (
        analiza["ile_zamowic"] * analiza["Cena zakupu netto"]
    ).round(2)

    # ── 7. Status każdego produktu ───────────────────────────────────────────
    def get_status(row):
        z = row["srednie_dzienne"]
        d = row["dni_do_wyczerpania"]
        t = row.get("liczba_transakcji", 0)
        s = row["Stan"]

        if z == 0 and s > 0:
            return "DEAD STOCK"
        if z == 0 and s == 0:
            return "NIEAKTYWNY"
        if t < 3:
            return "JEDNORAZÓWKA"
        if z >= 0.5 and d < 8:
            return "ZAMÓW DZIŚ"
        if z >= 0.3 and d < 15:
            return "ZAMÓW TYDZIEŃ"
        return "OK"

    analiza["status"] = analiza.apply(get_status, axis=1)

    # ── 8. Minima logistyczne dostawców (opcjonalny plik) ────────────────────
    min_log = {}
    if min_log_file:
        try:
            df_min = read_uploaded_file(min_log_file)
            for _, row in df_min.iterrows():
                vals = list(row.values)
                if len(vals) >= 2:
                    supplier = str(vals[0]).strip()
                    try:
                        val = float(
                            str(vals[1])
                            .replace(",", ".")
                            .replace(" ", "")
                            .replace("\xa0", "")
                        )
                        min_log[supplier.upper()] = val
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

    # ── 9. Otwarte zamówienia do dostawców ──────────────────────────────────
    zam_df = None
    if zam_file:
        try:
            zam_df = read_uploaded_file(zam_file)
            for col in zam_df.columns:
                if "data" in col.lower() or "realiz" in col.lower():
                    zam_df["_data_realiz"] = zam_df[col].apply(parse_polish_date)
                    break
        except Exception:
            zam_df = None

    # ── 10. Summary dict ─────────────────────────────────────────────────────
    dzis = analiza[analiza["status"] == "ZAMÓW DZIŚ"]
    tydzien = analiza[analiza["status"] == "ZAMÓW TYDZIEŃ"]
    dead = analiza[analiza["status"] == "DEAD STOCK"]

    summary = {
        "data_analizy": datetime.now().strftime("%d.%m.%Y"),
        "produktow_total": len(analiza),
        "produktow_aktywnych": int((analiza["srednie_dzienne"] >= 0.3).sum()),
        "wartosc_magazynu": float(analiza["wartosc_stanu"].sum()),
        "produktow_dzis": len(dzis),
        "wartosc_dzis": float(dzis["wartosc_zamowienia"].sum()),
        "produktow_tydzien": len(tydzien),
        "wartosc_tydzien": float(tydzien["wartosc_zamowienia"].sum()),
        "dead_stock_wartosc": float(dead["wartosc_stanu"].sum()),
        "dead_stock_produktow": len(dead),
        "dni_okresu": dni_okresu,
        "data_od": date_min.strftime("%d.%m.%Y"),
        "data_do": date_max.strftime("%d.%m.%Y"),
        "min_log": min_log,
    }

    context = _build_llm_context(analiza, zam_df, summary)
    return analiza, zam_df, summary, context


# ── LLM context builder ───────────────────────────────────────────────────────

def _build_llm_context(analiza, zam_df, summary):
    """
    Buduje tekst kontekstu dla LLM na podstawie wyników analizy.
    LLM dostaje gotowe liczby — nie musi sam liczyć.
    """
    nazwa_col = find_col(analiza, "nazwa towaru")
    kod_col = find_col(analiza, "kod towaru / usługi", "kod towaru")
    dostawca_col = find_col(analiza, "dostawca")

    def prod_name(row):
        return row.get(nazwa_col) or row.get(kod_col) or "N/A"

    lines = [
        f"=== ANALIZA MAGAZYNU Add All — {summary['data_analizy']} ===",
        f"Okres danych: {summary['data_od']} — {summary['data_do']} ({summary['dni_okresu']} dni)",
        f"Produktów w bazie: {summary['produktow_total']} (aktywnych: {summary['produktow_aktywnych']})",
        f"Wartość magazynu: {summary['wartosc_magazynu']:,.0f} PLN",
        f"Dead stock: {summary['dead_stock_wartosc']:,.0f} PLN ({summary['dead_stock_produktow']} prod.)",
        "",
    ]

    # Zamów dziś
    lines.append("=== ZAMÓW DZIŚ ===")
    dzis = analiza[analiza["status"] == "ZAMÓW DZIŚ"].sort_values("dni_do_wyczerpania")
    if len(dzis) == 0:
        lines.append("Brak produktów do pilnego zamówienia.")
    else:
        if dostawca_col:
            for dostawca, g in dzis.groupby(dostawca_col):
                razem = g["wartosc_zamowienia"].sum()
                min_v = summary["min_log"].get(str(dostawca).upper(), 0)
                status_min = (
                    f"✅ minimum OK ({razem:,.0f} >= {min_v:,.0f} PLN)"
                    if min_v == 0 or razem >= min_v
                    else f"⚠️ brakuje {min_v - razem:,.0f} PLN do minimum ({min_v:,.0f} PLN)"
                )
                lines.append(f"\nDOSTAWCA: {dostawca} — {razem:,.0f} PLN | {status_min}")
                for _, r in g.iterrows():
                    lines.append(
                        f"  • {prod_name(r)} | Stan: {r['Stan']:.0f} | "
                        f"Zużycie: {r['srednie_dzienne']:.2f}/dzień | "
                        f"Starczy: {r['dni_do_wyczerpania']:.0f} dni | "
                        f"Zamów: {r['ile_zamowic']} szt | "
                        f"Wartość: {r['wartosc_zamowienia']:,.0f} PLN"
                    )
        else:
            for _, r in dzis.iterrows():
                lines.append(
                    f"  • {prod_name(r)} — {r['dni_do_wyczerpania']:.0f} dni, "
                    f"zamów {r['ile_zamowic']} szt"
                )

    # Zamów tydzień
    lines.append("\n=== ZAMÓW W TYM TYGODNIU ===")
    tydzien = analiza[analiza["status"] == "ZAMÓW TYDZIEŃ"].sort_values("dni_do_wyczerpania")
    if len(tydzien) == 0:
        lines.append("Brak.")
    else:
        for _, r in tydzien.iterrows():
            d = r.get(dostawca_col, "N/A") if dostawca_col else "N/A"
            lines.append(
                f"  • {prod_name(r)} ({d}) — {r['dni_do_wyczerpania']:.0f} dni, "
                f"zamów {r['ile_zamowic']} szt"
            )

    # Top 10 fast movers
    lines.append("\n=== TOP 10 NAJSZYBCIEJ SCHODZĄCYCH ===")
    top = analiza.nlargest(10, "srednie_dzienne")
    for _, r in top.iterrows():
        lines.append(
            f"  • {prod_name(r)} — {r['srednie_dzienne']:.2f} szt/dzień, "
            f"stan: {r['Stan']:.0f}, starczy: "
            + (f"{r['dni_do_wyczerpania']:.0f} dni" if r["dni_do_wyczerpania"] < 9999 else "∞")
        )

    # Dead stock
    lines.append("\n=== DEAD STOCK (top 5 wg zamrożonej kwoty) ===")
    dead = analiza[analiza["status"] == "DEAD STOCK"].nlargest(5, "wartosc_stanu")
    for _, r in dead.iterrows():
        lines.append(f"  • {prod_name(r)} — {r['wartosc_stanu']:,.0f} PLN zamrożone, stan: {r['Stan']:.0f}")

    # Analiza per dostawca
    if dostawca_col:
        lines.append("\n=== ANALIZA PER DOSTAWCA ===")
        per_d = analiza.groupby(dostawca_col).agg(
            produktow=("Stan", "count"),
            wartosc_stanu=("wartosc_stanu", "sum"),
        ).sort_values("wartosc_stanu", ascending=False).head(15)
        for dostawca, r in per_d.iterrows():
            lines.append(
                f"  • {dostawca}: {r['produktow']} prod., "
                f"wartość stanu: {r['wartosc_stanu']:,.0f} PLN"
            )

    # Zamówienia w drodze
    if zam_df is not None and len(zam_df) > 0:
        lines.append("\n=== ZAMÓWIENIA W DRODZE (otwarte PO) ===")
        for _, r in zam_df.iterrows():
            nr = r.get("Nr Zamówienia") or r.get("Nr zamówienia") or "N/A"
            dos = r.get("Dostawca") or "N/A"
            data = r.get("_data_realiz") or r.get("Data realiz.") or "N/A"
            w_col = find_col(zam_df, "wartość", "wartosc", "value")
            war = r.get(w_col, "N/A") if w_col else "N/A"
            lines.append(f"  • {nr} — {dos}, {war} PLN, dostawa: {data}")

    return "\n".join(lines)
