import io
import re
import pandas as pd
import streamlit as st

# ---------- Page setup ----------
st.set_page_config(page_title="Product List Translator & Category Mapper", layout="wide")

# ---------- Expected Product List columns ----------
# (Main stays as name; Sub & Sub-Sub store numbers after Apply)
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
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
    """Light e-commerce cleanup for Arabic (extend these rules as you like)."""
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
    """Translate Arabic -> English with DeepL in safe ~30k-character batches."""
    if not translator:
        return list(texts)  # keep original (cleaned Arabic) if DeepL not available

    out = []
    batch, batch_chars = [], 0
    LIMIT = 30000

    def flush(items):
        if not items:
            return []
        try:
            res = translator.translate_text(items, source_lang="AR", target_lang="EN-GB")
            if isinstance(res, list):
                return [r.text for r in res]
            return [res.text]
        except Exception:
            return items  # fail-safe: return originals for this batch

    for t in texts:
        t = t or ""
        if batch and (batch_chars + len(t) > LIMIT):
            out.extend(flush(batch))
            batch, batch_chars = [], 0
        batch.append(t)
        batch_chars += len(t)

    if batch:
        out.extend(flush(batch))
    return out


def to_excel_download(df, sheet_name="Products"):
    """Return an Excel bytes buffer to download."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer


# ---------- Build mapping structures for cascade & lookups ----------
def build_mapping_struct_fixed(map_df: pd.DataFrame):
    """
    Assumes mapping columns EXACTLY as:
      category_id                (Main NAME)
      sub_category_id            (Sub NAME)
      sub_category_id NO         (Sub NUMBER/ID)
      sub_sub_category_id        (Sub-Sub NAME)
      sub_sub_category_id NO     (Sub-Sub NUMBER/ID)
    - Dropdowns show NAMES.
    - On Apply, we write numbers for Sub & Sub-Sub using the NO columns.
    """
    # Normalize types/whitespace
    for c in ["category_id", "sub_category_id", "sub_category_id NO",
              "sub_sub_category_id", "sub_sub_category_id NO"]:
        if c in map_df.columns:
            map_df[c] = map_df[c].astype(str).str.strip()

    # Unique mains (names)
    main_names = sorted(map_df["category_id"].dropna().unique().tolist())

    # Main -> list of Sub NAMES
    main_to_subnames = {}
    for mc, g1 in map_df.groupby("category_id", dropna=True):
        subs = sorted(g1["sub_category_id"].dropna().unique().tolist())
        main_to_subnames[str(mc)] = subs

    # (Main, Sub NAME) -> list of Sub-Sub NAMES
    pair_to_subsubnames = {}
    for (mc, sc), g2 in map_df.groupby(["category_id", "sub_category_id"], dropna=True):
        ssubs = sorted(g2["sub_sub_category_id"].dropna().unique().tolist())
        pair_to_subsubnames[(str(mc), str(sc))] = ssubs

    # --- Lookup dictionaries to resolve NAMES -> NUMBERS on Apply ---
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


# ---------- UI ----------
st.title("🛒 Product List Translator & Category Mapper")

st.markdown("""
Upload your **Product List** and **Category Mapping** files.  
Arabic is auto-cleaned; if a DeepL key is configured in Secrets, the app also translates to English.  
Then search, choose **Main (name) → Sub (name) → Sub-Sub (name)**, and **Apply**.  
The app writes **numbers** for Sub & Sub-Sub (from your “NO” columns); Main stays as a **name**.
""")

col1, col2, col3 = st.columns(3)
with col1:
    product_file = st.file_uploader("Product List (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="prod")
with col2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
with col3:
    glossary_file = st.file_uploader("(Optional) Translation Glossary (.csv)", type=["csv"], key="gloss")  # reserved

# Read files
prod_df = read_any_table(product_file) if product_file else None
map_df = read_any_table(mapping_file) if mapping_file else None

# Validate availability
ok = True
if prod_df is None or not validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List"):
    ok = False

MAPPING_REQUIRED = [
    "category_id",
    "sub_category_id", "sub_category_id NO",
    "sub_sub_category_id", "sub_sub_category_id NO",
]
if map_df is None or not validate_columns(map_df, MAPPING_REQUIRED, "Category Mapping"):
    ok = False

if not ok:
    st.info("Upload both files with the required headers to continue.")
    st.stop()

# ---------- Auto-clean + translate (always runs; translates only if key present) ----------
# Ensure helper columns exist
for col in ["name_ar_clean", "name_en", "ProductNameEn"]:
    if col not in prod_df.columns:
        prod_df[col] = ""

if "name_ar" in prod_df.columns:
    prod_df["name_ar_clean"] = prod_df["name_ar"].astype(str).map(clean_arabic_text)
else:
    st.error("Column 'name_ar' not found in your Product List file. Translation skipped.")

if deepl_active and "name_ar_clean" in prod_df.columns:
    st.info("🔤 DeepL key detected — translating Arabic → English…")
    prod_df["name_en"] = translate_deepl_ar_to_en(prod_df["name_ar_clean"].fillna("").tolist())
    st.success("Translation complete.")
else:
    # keep cleaned Arabic in English column as a fallback so the UI never breaks
    if "name_ar_clean" in prod_df.columns:
        prod_df["name_en"] = prod_df["name_ar_clean"]
    st.warning("DeepL not active — showing cleaned Arabic in English column. "
               "Confirm Secrets + requirements.txt, then reboot.")

# Keep ProductNameEn in sync (if other parts of your app use it)
prod_df["ProductNameEn"] = prod_df["name_en"]

with st.expander("Translation preview (first 10)"):
    st.dataframe(prod_df[["name_ar", "name_ar_clean", "name_en"]].head(10), use_container_width=True)

# Build lookups for mapping
lookups = build_mapping_struct_fixed(map_df)

# ---------- Working dataframe persisted across searches ----------
if "work" not in st.session_state:
    st.session_state.work = prod_df.copy()
work = st.session_state.work

# Ensure all expected columns exist
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""

# --- Previews ---
with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

# ---------- Search + Bulk Assign ----------
st.subheader("Find products & bulk-assign category IDs")

c1, c2 = st.columns([3, 1])
with c1:
    q = st.text_input("Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):", key="search_q")
with c2:
    if st.button("Show all"):
        st.session_state.search_q = ""
        st.experimental_rerun()

if st.session_state.get("search_q", "").strip():
    qlower = st.session_state["search_q"].strip().lower()
    mask = work["name"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["name_ar"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["ProductNameEn"].astype(str).str.lower().str.contains(qlower, na=False)
else:
    mask = pd.Series(True, index=work.index)

filtered = work[mask].copy()
st.caption(f"Matched rows: {filtered.shape[0]}")

# Cascading pickers (NAMES only)
main_opts = [""] + lookups["main_names"]
sel_main = st.selectbox("Main (category_id — NAME)", options=main_opts)

sub_opts = [""] + (lookups["main_to_subnames"].get(sel_main, []) if sel_main else [])
sel_sub = st.selectbox("Sub (sub_category_id — NAME, filtered by Main)", options=sub_opts)

subsub_opts = [""] + (lookups["pair_to_subsubnames"].get((sel_main, sel_sub), []) if sel_main and sel_sub else [])
sel_subsub = st.selectbox("Sub-Sub (sub_sub_category_id — NAME, filtered by Sub)", options=subsub_opts)

# ---- Apply: write Main as NAME; Sub & Sub-Sub as NUMBERS from mapping ----
def get_sub_no(main_name, sub_name) -> str:
    if not main_name or not sub_name:
        return ""
    return lookups["sub_name_to_no_by_main"].get((main_name, sub_name), "")

def get_ssub_no(main_name, sub_name, ssub_name) -> str:
    if not main_name or not sub_name or not ssub_name:
        return ""
    return lookups["ssub_name_to_no_by_main_sub"].get((main_name, sub_name, ssub_name), "")

if st.button("Apply to all filtered rows"):
    # Main stays as name
    if sel_main:
        work.loc[mask, "category_id"] = sel_main

    # Resolve numbers via mapping
    sub_no = get_sub_no(sel_main, sel_sub)
    ssub_no = get_ssub_no(sel_main, sel_sub, sel_subsub)

    if sub_no:
        work.loc[mask, "sub_category_id"] = sub_no  # write NUMBER
    if ssub_no:
        work.loc[mask, "sub_sub_category_id"] = ssub_no  # write NUMBER

    # Persist updates for next searches
    st.session_state.work = work

    # Refresh filtered view
    filtered = work[mask].copy()
    st.success("Applied (Main name; Sub & Sub-Sub numbers) to all filtered rows.")

# Show filtered preview (numbers should appear in sub/sub-sub columns)
st.dataframe(
    filtered[["merchant_sku", "name", "name_ar", "name_ar_clean", "name_en",
              "category_id", "sub_category_id", "sub_sub_category_id"]],
    use_container_width=True, height=360
)

# Optional reset (handy for testing)
with st.expander("Reset working data"):
    if st.button("🔄 Reset working data (start over)"):
        st.session_state.pop("work", None)
        st.experimental_rerun()

# ---------- Download ----------
st.subheader("Download")
excel_bytes = to_excel_download(work, sheet_name="Products")
st.download_button(
    label="⬇️ Download Updated Excel",
    data=excel_bytes,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.caption(
    "Main category stays as a NAME (no numeric main ID provided). "
    "Sub & Sub-Sub are saved as NUMBERS from your mapping. "
    "Arabic is always cleaned; if a DeepL key is present, English is auto-translated."
)
