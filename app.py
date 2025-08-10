import io
import pandas as pd
import streamlit as st

# ---------- Config ----------
st.set_page_config(page_title="Product List Translator & Category Mapper", layout="wide")

# Product List columns (as per your files)
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
]

# Category Mapping columns (IDs only; one row per valid combination)
REQUIRED_MAP_COLS = ["category_id", "sub_category_id", "sub_sub_category_id"]

# Optional glossary CSV columns
GLOSSARY_COLS = ["Arabic", "English"]


# ---------- Helpers ----------
def read_any_table(uploaded_file):
    """Read xlsx/xls/csv safely."""
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        # Explicit engine for Streamlit Cloud
        return pd.read_excel(uploaded_file, engine="openpyxl")
    elif name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    else:
        raise ValueError("Please upload .xlsx, .xls, or .csv")


def validate_columns(df, required_cols, label):
    """Ensure required columns exist."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"{label}: missing required columns: {missing}")
        return False
    return True


@st.cache_data
def build_mapping_struct(map_df: pd.DataFrame):
    """
    Build 3-level cascade using IDs only:
    category_id (Main) -> sub_category_id (Sub) -> sub_sub_category_id (Sub-Sub)
    """
    for c in ["category_id", "sub_category_id", "sub_sub_category_id"]:
        map_df[c] = map_df[c].astype(str).str.strip()

    # Unique mains
    main_ids = sorted(map_df["category_id"].dropna().unique().tolist())

    # Main -> [Subs]
    main_to_subs = {}
    for mc, g1 in map_df.groupby("category_id", dropna=True):
        subs = sorted(g1["sub_category_id"].dropna().astype(str).str.strip().unique().tolist())
        main_to_subs[str(mc)] = subs

    # (Main, Sub) -> [SubSubs]
    pair_to_subsubs = {}
    for (mc, sc), g2 in map_df.groupby(["category_id", "sub_category_id"], dropna=True):
        ssubs = sorted(g2["sub_sub_category_id"].dropna().astype(str).str.strip().unique().tolist())
        pair_to_subsubs[(str(mc), str(sc))] = ssubs

    return {
        "main_ids": [str(x) for x in main_ids],
        "main_to_subs": main_to_subs,
        "pair_to_subsubs": pair_to_subsubs,
    }


def apply_glossary_translate(series_ar, glossary_df):
    """
    Optional helper: if a glossary CSV (Arabic,English) is provided,
    build a quick English helper column. Otherwise return the Arabic.
    """
    if glossary_df is None:
        return series_ar.astype(str)
    glossary_df["Arabic"] = glossary_df["Arabic"].astype(str)
    glossary_df["English"] = glossary_df["English"].astype(str)
    g = dict(zip(glossary_df["Arabic"], glossary_df["English"]))
    return series_ar.astype(str).map(lambda x: g.get(x, x))


def to_excel_download(df, sheet_name="Products"):
    """Return an Excel bytes buffer to download."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer


# ---------- UI ----------
st.title("🛒 Product List Translator & Category Mapper")

st.markdown("""
Upload your **Product List** and **Category Mapping** files.  
Then **search** products (e.g., “Dishwashing”, “liquid detergent”), select **category_id → sub_category_id → sub_sub_category_id**, and **apply to all filtered rows**.  
Finally, download the enriched Excel.
""")

col1, col2, col3 = st.columns(3)
with col1:
    product_file = st.file_uploader("Product List (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="prod")
with col2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
with col3:
    glossary_file = st.file_uploader("(Optional) Translation Glossary (.csv)", type=["csv"], key="gloss")

# Read files
prod_df = read_any_table(product_file) if product_file else None
map_df = read_any_table(mapping_file) if mapping_file else None
glossary_df = read_any_table(glossary_file) if glossary_file else None

# Validate
ok = True
if prod_df is None or not validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List"):
    ok = False
if map_df is None or not validate_columns(map_df, REQUIRED_MAP_COLS, "Category Mapping"):
    ok = False
if glossary_df is not None and not validate_columns(glossary_df, GLOSSARY_COLS, "Translation Glossary"):
    glossary_df = None

if not ok:
    st.info("Upload the required files to continue.")
    st.stop()

# Build lookups
lookups = build_mapping_struct(map_df)

# Prepare working frame; ensure columns exist (robustness)
work = prod_df.copy()
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""

# Optional English helper column for searching
if "ProductNameEn" not in work.columns:
    work["ProductNameEn"] = apply_glossary_translate(work["name_ar"], glossary_df)

# --- File previews ---
with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

# ---------- Search + Bulk Assign ----------
st.subheader("Find products & bulk-assign category IDs")

q = st.text_input("Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):", "")
mask = pd.Series(True, index=work.index)
if q.strip():
    qlower = q.strip().lower()
    mask = work["name"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["name_ar"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["ProductNameEn"].astype(str).str.lower().str.contains(qlower, na=False)

filtered = work[mask].copy()
st.caption(f"Matched rows: {filtered.shape[0]}")

# Cascading pickers (IDs only)
main_opts = [""] + lookups["main_ids"]
sel_main = st.selectbox("Main category_id", options=main_opts)

sub_opts = [""] + (lookups["main_to_subs"].get(sel_main, []) if sel_main else [])
sel_sub = st.selectbox("Sub sub_category_id (filtered by main)", options=sub_opts)

subsub_opts = [""] + (lookups["pair_to_subsubs"].get((sel_main, sel_sub), []) if sel_main and sel_sub else [])
sel_subsub = st.selectbox("Sub-Sub sub_sub_category_id (filtered by sub)", options=subsub_opts)

if st.button("Apply IDs to all filtered rows"):
    if sel_main:
        work.loc[mask, "category_id"] = sel_main
    if sel_sub:
        work.loc[mask, "sub_category_id"] = sel_sub
    if sel_subsub:
        work.loc[mask, "sub_sub_category_id"] = sel_subsub
    filtered = work[mask].copy()
    st.success("Applied your selection to all filtered rows.")

# Show filtered preview
st.dataframe(
    filtered[["merchant_sku", "name", "name_ar",
              "category_id", "sub_category_id", "sub_sub_category_id"]],
    use_container_width=True, height=340
)

# ---------- Download ----------
st.subheader("Download")
excel_bytes = to_excel_download(work, sheet_name="Products")
st.download_button(
    label="⬇️ Download Updated Excel",
    data=excel_bytes,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.caption("Tip: Use the search + bulk-apply to speed through categories. Only valid Sub/Sub-Sub IDs for the selected Main are shown.")
