import io
import re
import math
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# NEW
import requests
from PIL import Image
from io import BytesIO

# ---------- Page setup ----------
st.set_page_config(page_title="Product List Translator & Category Mapper", layout="wide")

# ---------- Expected Product List columns ----------
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # Optional new column for images:
    # "image_url"
]

# ---------- DeepL (auto) ----------
translator = None
deepl_active = False
try:
    import deepl
    DEEPL_API_KEY = st.secrets.get("DEEPL_API_KEY")
    if DEEPL_API_KEY:
        translator = deepl.Translator(DEEPL_API_KEY)
        deepl_active = True
except Exception:
    translator = None
    deepl_active = False


# ---------- Helpers ----------
def read_any_table(uploaded_file):
    """Read xlsx/xls/csv safely (explicit engine for cloud)."""
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file, engine="openpyxl")
    elif name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    else:
        raise ValueError("Please upload .xlsx, .xls, or .csv")


def validate_columns(df, required_cols, label):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"{label}: missing required columns: {missing}")
        return False
    return True


def clean_arabic_text(s: str) -> str:
    """Light e-commerce cleanup for Arabic (extend rules as needed)."""
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    # normalize spaces
    s = re.sub(r"\s+", " ", s)
    # normalize common units/spaces
    s = re.sub(r"\b(\d+)\s*(مل|ml)\b", r"\1 مل", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(جم|g)\b",  r"\1 جم", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(كغ|kg)\b", r"\1 كغ", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(قطعة|pcs?)\b", r"\1 قطعة", s, flags=re.I)
    return s


def translate_deepl_ar_to_en(texts):
    """
    Translate Arabic -> English with DeepL in safe batches.
    - Limits by number of items AND total characters.
    - On error (quota/limit), stops gracefully and reports.
    - Returns full-length list aligned with inputs.
    """
    if not translator:
        return list(texts)

    results = list(texts)
    idx_texts = [(i, (t if isinstance(t, str) else "")) for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    if not idx_texts:
        return results

    MAX_ITEMS = 45
    MAX_CHARS = 28000
    start = 0
    translated_count = 0
    error_message = None

    while start < len(idx_texts):
        batch = []
        chars = 0
        k = start
        while k < len(idx_texts) and len(batch) < MAX_ITEMS:
            i, t = idx_texts[k]
            if batch and (chars + len(t) > MAX_CHARS):
                break
            batch.append((i, t))
            chars += len(t)
            k += 1

        try:
            texts_only = [t for _, t in batch]
            res = translator.translate_text(texts_only, source_lang="AR", target_lang="EN-GB")
            out_texts = [r.text for r in res] if isinstance(res, list) else [res.text]
            for (i, _), out in zip(batch, out_texts):
                results[i] = out
                translated_count += 1
            start = k
        except Exception as e:
            error_message = str(e)
            break

    if translated_count:
        st.success(f"Translation complete: {translated_count} / {len(idx_texts)} rows translated.")
    else:
        st.warning("DeepL call returned no translations; keeping original Arabic.")

    if error_message:
        st.warning(
            f"Stopped translating remaining rows due to an API error: {error_message}. "
            "This often indicates a quota or request limit; try again later."
        )

    return results


def translate_deepl_en_to_ar(texts):
    """Translate English -> Arabic with batch safety (used for desc_ar if DeepL is available)."""
    if not translator:
        return list(texts)

    results = list(texts)
    idx_texts = [(i, (t if isinstance(t, str) else "")) for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    if not idx_texts:
        return results

    MAX_ITEMS = 45
    MAX_CHARS = 28000
    start = 0
    translated_count = 0
    error_message = None

    while start < len(idx_texts):
        batch = []
        chars = 0
        k = start
        while k < len(idx_texts) and len(batch) < MAX_ITEMS:
            i, t = idx_texts[k]
            if batch and (chars + len(t) > MAX_CHARS):
                break
            batch.append((i, t))
            chars += len(t)
            k += 1

        try:
            texts_only = [t for _, t in batch]
            res = translator.translate_text(texts_only, source_lang="EN", target_lang="AR")
            out_texts = [r.text for r in res] if isinstance(res, list) else [res.text]
            for (i, _), out in zip(batch, out_texts):
                results[i] = out
                translated_count += 1
            start = k
        except Exception as e:
            error_message = str(e)
            break

    if translated_count:
        st.info(f"Arabic description translation: {translated_count} entries.")
    if error_message:
        st.warning(f"Arabic desc translation stopped due to API error: {error_message}")
    return results


def to_excel_download(df, sheet_name="Products"):
    """Return an Excel bytes buffer to download."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer


def build_mapping_struct_fixed(map_df: pd.DataFrame):
    """
    Mapping columns EXACTLY:
      category_id                (Main NAME)
      sub_category_id            (Sub NAME)
      sub_category_id NO         (Sub NUMBER/ID)
      sub_sub_category_id        (Sub-Sub NAME)
      sub_sub_category_id NO     (Sub-Sub NUMBER/ID)
    """
    for c in ["category_id", "sub_category_id", "sub_category_id NO",
              "sub_sub_category_id", "sub_sub_category_id NO"]:
        if c in map_df.columns:
            map_df[c] = map_df[c].astype(str).str.strip()

    main_names = sorted(map_df["category_id"].dropna().unique().tolist())

    main_to_subnames = {}
    for mc, g1 in map_df.groupby("category_id", dropna=True):
        subs = sorted(g1["sub_category_id"].dropna().unique().tolist())
        main_to_subnames[str(mc)] = subs

    pair_to_subsubnames = {}
    for (mc, sc), g2 in map_df.groupby(["category_id", "sub_category_id"], dropna=True):
        ssubs = sorted(g2["sub_sub_category_id"].dropna().unique().tolist())
        pair_to_subsubnames[(str(mc), str(sc))] = ssubs

    sub_name_to_no_by_main = {}
    ssub_name_to_no_by_main_sub = {}
    for _, r in map_df.iterrows():
        mc = r["category_id"]
        sc_name = r["sub_category_id"]
        sc_no = r["sub_category_id NO"]
        ssc_name = r["sub_sub_category_id"]
        ssc_no = r["sub_sub_category_id NO"]
        sub_name_to_no_by_main[(mc, sc_name)] = sc_no
        ssub_name_to_no_by_main_sub[(mc, sc_name, ssc_name)] = ssc_no

    return {
        "main_names": main_names,
        "main_to_subnames": main_to_subnames,
        "pair_to_subsubnames": pair_to_subsubnames,
        "sub_name_to_no_by_main": sub_name_to_no_by_main,
        "ssub_name_to_no_by_main_sub": ssub_name_to_no_by_main_sub,
    }


# ---------- Description helpers ----------
SIZE_RE = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)\s*(?P<u>ml|l|g|kg|oz|fl\s?oz|mL|ML|KG|G|L)\b",
    flags=re.I
)
COUNT_RE = re.compile(r"\b(?P<count>\d+)\s*(?:pcs?|قطع(?:ة)?|pack|pkt)\b", flags=re.I)
SCENT_RE = re.compile(r"\b(lemon|rose|lavender|musk|jasmine|apple|pine|fresh|ocean)\b", flags=re.I)

def extract_attrs_en(name_en: str):
    """Parse common attributes from English name."""
    if not isinstance(name_en, str):
        name_en = ""
    brand = ""
    # Heuristic brand: first token capitalized sequence before a common term
    tokens = name_en.split()
    if tokens:
        brand = tokens[0]
    size = None
    m = SIZE_RE.search(name_en)
    if m:
        size = f'{m.group("num")} {m.group("u").upper()}'.replace("ML", "ml").replace("L", "L").replace("KG", "kg").replace("G", "g")

    count = None
    m2 = COUNT_RE.search(name_en)
    if m2:
        count = m2.group("count")

    scent = None
    m3 = SCENT_RE.search(name_en.lower())
    if m3:
        scent = m3.group(1).title()

    return brand, size, count, scent


def make_desc_en(row):
    """Simple ecommerce style English description."""
    title = str(row.get("name_en") or row.get("name") or "").strip()
    brand, size, count, scent = extract_attrs_en(title)

    parts = []
    if brand:
        parts.append(f"{brand} — premium quality.")
    parts.append(f"{title}.")
    if size:
        parts.append(f"Size: {size}.")
    if count:
       
