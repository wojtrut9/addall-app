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
    """Czyta plik CSV lub Excel wgrany przez Streamlit (auto-encoding dla CSV).

    Kolejność enkodowań ma znaczenie: UTF-8 jest pierwszy, bo nasz connector
    iBiznes generuje CSV w UTF-8 z polskimi nagłówkami ('Rozchód', 'Przychód').
    cp1250 zdekoduje bajty UTF-8 BEZ błędu (ó → 'Ăł') i podstępnie zwróci złe
    nazwy kolumn — dlatego musi być ostatni jako fallback dla starszych
    eksportów iBiznes.
    """
    name = uploaded_file.name.lower()

    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file, dtype=str)

    # CSV — próbuj kolejne encodingi (UTF-8 PIERWSZY, cp1250 jako fallback)
    raw = uploaded_file.read()
    for enc in ["utf-8-sig", "utf-8", "cp1250", "iso-8859-2", "latin-1"]:
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
                # Sanity check — jeśli nagłówki zawierają charakterystyczne
                # artefakty (np. 'Ăł' = źle zdekodowane 'ó' z UTF-8 jako cp1250)
                # to próbujemy następne kodowanie.
                hdr = " ".join(str(c) for c in df.columns)
                if any(art in hdr for art in ("Ăł", "Ä™", "Ĺ›", "Ĺ‚", "ĹĽ")):
                    continue
                return df
        except Exception:
            continue

    # Spróbuj z przecinkiem jako separatorem
    for enc in ["utf-8-sig", "utf-8", "cp1250"]:
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


def analyze(kart_file, obroty_file, zam_file=None, min_log_file=None, in_transit_df=None):
    """
    Główna funkcja analizy.

    Args:
        kart_file: plik kartoteki towarów (CSV/Excel) lub buffer.
        obroty_file: plik obrotów magazynowych.
        zam_file: opcjonalny plik header otwartych zamówień (do dostawców).
        min_log_file: opcjonalny plik minimów logistycznych (Dostawca | PLN).
        in_transit_df: opcjonalny DataFrame z agregatem "w drodze" per SKU
            (kolumny: Kod towaru, w_drodze, wartosc_w_drodze).
            Jeśli podany, zamówienia w drodze są odejmowane od rekomendacji
            "ile zamówić".

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

    # ── 5a. Wpinamy "w drodze" (otwarte PO per SKU) ─────────────────────────
    # in_transit_df — opcjonalny DataFrame z agregatem otwartych zamówień
    # do dostawców per SKU. Pozwala uniknąć rekomendowania zakupu czegoś,
    # co już jest w trakcie dostawy.
    if in_transit_df is not None and len(in_transit_df) > 0:
        it = in_transit_df.copy()
        # Ujednolicamy nazwę kolumny SKU
        it_kod = find_col(it, "kod towaru / usługi", "kod towaru")
        if it_kod is None and "Kod towaru" in it.columns:
            it_kod = "Kod towaru"
        if it_kod and "w_drodze" in it.columns:
            it = it[[it_kod, "w_drodze"] + (
                ["wartosc_w_drodze"] if "wartosc_w_drodze" in it.columns else []
            )].rename(columns={it_kod: "_kod_w_drodze"})
            it["w_drodze"] = pd.to_numeric(it["w_drodze"], errors="coerce").fillna(0)
            if "wartosc_w_drodze" in it.columns:
                it["wartosc_w_drodze"] = pd.to_numeric(it["wartosc_w_drodze"], errors="coerce").fillna(0)
            analiza = analiza.merge(
                it, left_on=kod_col, right_on="_kod_w_drodze", how="left"
            )
            analiza.drop(columns=["_kod_w_drodze"], inplace=True, errors="ignore")

    if "w_drodze" not in analiza.columns:
        analiza["w_drodze"] = 0.0
    else:
        analiza["w_drodze"] = analiza["w_drodze"].fillna(0)
    if "wartosc_w_drodze" not in analiza.columns:
        analiza["wartosc_w_drodze"] = 0.0
    else:
        analiza["wartosc_w_drodze"] = analiza["wartosc_w_drodze"].fillna(0)

    # ── 6. Kluczowe metryki ──────────────────────────────────────────────────
    # Efektywny stan = to co w magazynie + to co już zamówione u dostawcy
    analiza["efektywny_stan"] = analiza["Stan"] + analiza["w_drodze"]

    analiza["dni_do_wyczerpania"] = np.where(
        analiza["srednie_dzienne"] > 0,
        (analiza["efektywny_stan"] / analiza["srednie_dzienne"]).round(1),
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

    # Rekomendowana ilość zamówienia: prognoza 21d + safety 7d - efektywny stan
    # (efektywny = Stan + w_drodze, więc nie zamawiamy podwójnie tego co jedzie)
    analiza["prognoza_30d"] = (analiza["srednie_dzienne"] * 21).round(0)
    analiza["safety_7d"] = (analiza["srednie_dzienne"] * 7).round(0)
    analiza["ile_zamowic"] = np.where(
        analiza["srednie_dzienne"] > 0,
        np.maximum(
            0,
            analiza["prognoza_30d"] + analiza["safety_7d"] - analiza["efektywny_stan"],
        ).round(0),
        0,
    )
    # Limit: max 45 dni zużycia (sensowny zapas dla dystrybutora)
    analiza["ile_zamowic"] = np.minimum(
        analiza["ile_zamowic"],
        (analiza["srednie_dzienne"] * 45).round(0),
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
        wd = row.get("w_drodze", 0)

        if z == 0 and s > 0 and wd == 0:
            return "DEAD STOCK"
        if z == 0 and s == 0 and wd == 0:
            return "NIEAKTYWNY"
        if z == 0 and wd > 0:
            return "OK"  # nie ma sprzedaży, ale jedzie — zostaw
        if t < 3:
            return "JEDNORAZÓWKA"
        # Status liczymy na bazie EFEKTYWNEGO stanu (włącznie z "w drodze"),
        # więc jeśli coś jedzie, nie pojawi się jako "ZAMÓW DZIŚ".
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

    # "Aktywny" = ma jakąkolwiek sprzedaż w okresie LUB ma otwarte zamówienie
    # u dostawcy (czyli to czym faktycznie zarządzamy zakupowo).
    is_active = (analiza["srednie_dzienne"] > 0) | (analiza["w_drodze"] > 0)
    aktywne = analiza[is_active]

    # Wartość magazynu = stan × cena_zakupu, ale TYLKO dla aktywnych produktów.
    # Dead stock liczymy osobno — to zamrożony kapitał, nie "magazyn roboczy".
    wartosc_aktywnego_magazynu = float(aktywne["wartosc_stanu"].sum())
    wartosc_calego_magazynu    = float(analiza["wartosc_stanu"].sum())

    summary = {
        "data_analizy": datetime.now().strftime("%d.%m.%Y"),
        # produktow_total = liczba aktywnych pozycji (kartoteka po filtrze Akt='T')
        "produktow_total": len(analiza),
        # Aktywnych = z ruchem lub z otwartym zamówieniem
        "produktow_aktywnych": int(is_active.sum()),
        # Magazyn = wartość stanu aktywnych pozycji (nie wlicza dead stocku)
        "wartosc_magazynu": wartosc_aktywnego_magazynu,
        "wartosc_calego_magazynu": wartosc_calego_magazynu,
        # Co już jedzie od dostawców
        "wartosc_w_drodze": float(analiza["wartosc_w_drodze"].sum()),
        "produktow_w_drodze": int((analiza["w_drodze"] > 0).sum()),
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

    Konwencja:
    - Twardo nagłówkowane sekcje [SEKCJA: ...] aby model nie miał wątpliwości
      gdzie szukać konkretnych liczb.
    - Kwoty bez separatorów (np. 118121 PLN), żeby LLM łatwo je cytował.
    """
    nazwa_col = find_col(analiza, "nazwa towaru")
    kod_col = find_col(analiza, "kod towaru / usługi", "kod towaru")
    dostawca_col = find_col(analiza, "dostawca")

    def prod_name(row):
        return row.get(nazwa_col) or row.get(kod_col) or "N/A"

    lines = [
        "=== KONTEKST ANALIZY MAGAZYNU Add All ===",
        f"Data analizy: {summary['data_analizy']}",
        f"Okres danych: {summary['data_od']} — {summary['data_do']} ({summary['dni_okresu']} dni)",
        "",
        "[SEKCJA: PODSUMOWANIE]",
        f"- Produktów aktywnych w kartotece: {summary['produktow_total']}",
        f"- Z aktywnym ruchem (sprzedaż lub zamówienie u dostawcy): {summary['produktow_aktywnych']}",
        f"- Wartość aktywnego magazynu (zakup netto): {summary['wartosc_magazynu']:.0f} PLN",
        f"- Wartość całego magazynu (z dead-stockiem): {summary.get('wartosc_calego_magazynu', summary['wartosc_magazynu']):.0f} PLN",
        f"- W drodze od dostawców: {summary.get('produktow_w_drodze', 0)} pozycji za {summary.get('wartosc_w_drodze', 0):.0f} PLN",
        f"- Do zamówienia DZIŚ: {summary['produktow_dzis']} pozycji za {summary['wartosc_dzis']:.0f} PLN",
        f"- Do zamówienia w TYGODNIU: {summary['produktow_tydzien']} pozycji za {summary['wartosc_tydzien']:.0f} PLN",
        f"- Dead stock: {summary['dead_stock_produktow']} pozycji za {summary['dead_stock_wartosc']:.0f} PLN",
        "",
    ]

    # Zamów dziś
    lines.append("[SEKCJA: ZAMÓW DZIŚ]")
    dzis = analiza[analiza["status"] == "ZAMÓW DZIŚ"].sort_values("dni_do_wyczerpania")
    if len(dzis) == 0:
        lines.append("Brak — żaden aktywny produkt nie zejdzie do zera w ciągu 7 dni.")
    else:
        lines.append(f"Razem: {len(dzis)} pozycji, łącznie {dzis['wartosc_zamowienia'].sum():.0f} PLN.")
        if dostawca_col:
            for dostawca, g in dzis.groupby(dostawca_col):
                razem = g["wartosc_zamowienia"].sum()
                min_v = summary["min_log"].get(str(dostawca).upper(), 0)
                status_min = (
                    f"min logistyczne OK ({razem:.0f} >= {min_v:.0f} PLN)"
                    if min_v == 0 or razem >= min_v
                    else f"BRAKUJE {min_v - razem:.0f} PLN do minimum ({min_v:.0f} PLN)"
                )
                lines.append(f"\nDostawca: {dostawca} — {razem:.0f} PLN | {status_min}")
                for _, r in g.iterrows():
                    lines.append(
                        f"  - {prod_name(r)} | stan {r['Stan']:.0f} + w drodze {r['w_drodze']:.0f} = "
                        f"{r['efektywny_stan']:.0f} | zużycie {r['srednie_dzienne']:.2f}/d | "
                        f"starczy {r['dni_do_wyczerpania']:.0f} dni | "
                        f"zamów {r['ile_zamowic']} szt za {r['wartosc_zamowienia']:.0f} PLN"
                    )
        else:
            for _, r in dzis.iterrows():
                lines.append(
                    f"  - {prod_name(r)} — {r['dni_do_wyczerpania']:.0f} dni, "
                    f"zamów {r['ile_zamowic']} szt za {r['wartosc_zamowienia']:.0f} PLN"
                )

    # Zamów tydzień
    lines.append("\n[SEKCJA: ZAMÓW W TYM TYGODNIU]")
    tydzien = analiza[analiza["status"] == "ZAMÓW TYDZIEŃ"].sort_values("dni_do_wyczerpania")
    if len(tydzien) == 0:
        lines.append("Brak.")
    else:
        lines.append(f"Razem: {len(tydzien)} pozycji, łącznie {tydzien['wartosc_zamowienia'].sum():.0f} PLN.")
        # Limit do 50 pozycji żeby kontekst nie eksplodował
        for _, r in tydzien.head(50).iterrows():
            d = r.get(dostawca_col, "N/A") if dostawca_col else "N/A"
            lines.append(
                f"  - {prod_name(r)} ({d}) — {r['dni_do_wyczerpania']:.0f} dni, "
                f"zamów {r['ile_zamowic']} szt za {r['wartosc_zamowienia']:.0f} PLN"
            )
        if len(tydzien) > 50:
            lines.append(f"  ... i {len(tydzien) - 50} kolejnych (pełna lista w tabeli/Excelu).")

    # W drodze (per SKU)
    in_transit = analiza[analiza["w_drodze"] > 0].sort_values("w_drodze", ascending=False)
    lines.append("\n[SEKCJA: W DRODZE OD DOSTAWCÓW (per SKU)]")
    if len(in_transit) == 0:
        lines.append("Brak otwartych zamówień u dostawców.")
    else:
        lines.append(f"Razem {len(in_transit)} SKU za {in_transit['wartosc_w_drodze'].sum():.0f} PLN.")
        for _, r in in_transit.head(20).iterrows():
            lines.append(
                f"  - {prod_name(r)} — {r['w_drodze']:.0f} szt za {r['wartosc_w_drodze']:.0f} PLN"
            )
        if len(in_transit) > 20:
            lines.append(f"  ... i {len(in_transit) - 20} kolejnych pozycji.")

    # Top 10 fast movers
    lines.append("\n[SEKCJA: TOP 10 NAJSZYBCIEJ SCHODZĄCYCH]")
    top = analiza[analiza["srednie_dzienne"] > 0].nlargest(10, "srednie_dzienne")
    for _, r in top.iterrows():
        lines.append(
            f"  - {prod_name(r)} — {r['srednie_dzienne']:.2f} szt/dzień, "
            f"stan {r['Stan']:.0f} (+{r['w_drodze']:.0f} w drodze), starczy "
            + (f"{r['dni_do_wyczerpania']:.0f} dni" if r["dni_do_wyczerpania"] < 9999 else "∞")
        )

    # Dead stock
    lines.append("\n[SEKCJA: DEAD STOCK — TOP 5 WG ZAMROŻONEJ KWOTY]")
    dead = analiza[analiza["status"] == "DEAD STOCK"].nlargest(5, "wartosc_stanu")
    if len(dead) == 0:
        lines.append("Brak.")
    else:
        for _, r in dead.iterrows():
            lines.append(f"  - {prod_name(r)} — {r['wartosc_stanu']:.0f} PLN zamrożone, stan: {r['Stan']:.0f}")

    # Marża <20%
    lines.append("\n[SEKCJA: PRODUKTY Z MARŻĄ < 20% (top 15)]")
    low_margin = analiza[
        (analiza["srednie_dzienne"] > 0)
        & (analiza["marza_pct"] > 0)
        & (analiza["marza_pct"] < 20)
    ].nlargest(15, "ilosc_sprzedana")
    if len(low_margin) == 0:
        lines.append("Brak — żaden aktywny produkt nie ma marży poniżej 20%.")
    else:
        for _, r in low_margin.iterrows():
            lines.append(
                f"  - {prod_name(r)} — marża {r['marza_pct']:.1f}%, "
                f"sprzedano {r['ilosc_sprzedana']:.0f} szt"
            )

    # Per dostawca
    if dostawca_col:
        lines.append("\n[SEKCJA: TOP 15 DOSTAWCÓW WG WARTOŚCI MAGAZYNU]")
        per_d = analiza.groupby(dostawca_col).agg(
            produktow=("Stan", "count"),
            aktywnych=("srednie_dzienne", lambda s: int((s > 0).sum())),
            wartosc_stanu=("wartosc_stanu", "sum"),
            wartosc_zamowienia=("wartosc_zamowienia", "sum"),
        ).sort_values("wartosc_stanu", ascending=False).head(15)
        for dostawca, r in per_d.iterrows():
            lines.append(
                f"  - {dostawca}: {r['produktow']} prod. ({r['aktywnych']} aktywnych), "
                f"magazyn {r['wartosc_stanu']:.0f} PLN, "
                f"do zamówienia {r['wartosc_zamowienia']:.0f} PLN"
            )

    # Otwarte zamówienia (header level)
    if zam_df is not None and len(zam_df) > 0:
        lines.append("\n[SEKCJA: OTWARTE ZAMÓWIENIA (header)]")
        for _, r in zam_df.head(20).iterrows():
            nr = r.get("Nr Zamówienia") or r.get("Nr zamówienia") or "N/A"
            dos = r.get("Dostawca") or "N/A"
            data = r.get("_data_realiz") or r.get("Data realiz.") or "N/A"
            w_col = find_col(zam_df, "wartość", "wartosc", "value")
            war = r.get(w_col, "N/A") if w_col else "N/A"
            lines.append(f"  - {nr} — {dos}, {war} PLN, dostawa: {data}")

    return "\n".join(lines)
