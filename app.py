import io
import pandas as pd
import streamlit as st

# ---------- Config ----------
st.set_page_config(page_title="Product List Translator & Category Mapper", layout="wide")

# Product List columns (your format)
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
]

# Category Mapping (IDs only; one row per valid combination)
REQUIRED_MAP_COLS = ["category_id", "sub_category_id", "sub_sub_category_id"]

# Optional glossary CSV columns
GLOSSARY_COLS = ["Arabic", "English"]


# ---------- Helpers ----------
def read_any_table(uploaded_file):
    """Read xlsx/xls/csv safely (explicit engine for Streamlit Cloud)."""
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


@st.cache_data
def build_mapping_struct(map_df: pd.DataFrame):
    """Build 3-level cascade (IDs) + optional ID->label maps if name columns exist."""
    for c in ["category_id", "sub_category_id", "sub_sub_category_id"]:
        map_df[c] = map_df[c].astype(str).str.strip()

    def pick_col(cands):
        for c in cands:
            if c in map_df.columns:
                return c
        return None

    cat_label_col  = pick_col(["category_name","category_en","category_ar","category"])
    sub_label_col  = pick_col(["sub_category_name","sub_category_en","sub_category_ar","sub_category"])
    ssub_label_col = pick_col(["sub_sub_category_name","sub_sub_category_en","sub_sub_category_ar","sub_sub_category"])

    main_ids = sorted(map_df["category_id"].dropna().astype(str).unique().tolist())

    main_to_subs = {}
    for mc, g1 in map_df.groupby("category_id", dropna=True):
        subs = sorted(g1["sub_category_id"].dropna().astype(str).unique().tolist())
        main_to_subs[str(mc)] = subs

    pair_to_subsubs = {}
    for (mc, sc), g2 in map_df.groupby(["category_id", "sub_category_id"], dropna=True):
        ssubs = sorted(g2["sub_sub_category_id"].dropna().astype(str).unique().tolist())
        pair_to_subsubs[(str(mc), str(sc))] = ssubs

    if cat_label_col:
        main_id_to_label = dict(map_df[["category_id", cat_label_col]].drop_duplicates().itertuples(index=False, name=None))
    else:
        main_id_to_label = {cid: cid for cid in main_ids}

    if sub_label_col:
        sub_id_to_label = dict(map_df[["sub_category_id", sub_label_col]].drop_duplicates().itertuples(index=False, name=None))
    else:
        sub_id_to_label = {sid: sid for sid in map_df["sub_category_id"].astype(str).unique()}

    if ssub_label_col:
        ssub_id_to_label = dict(map_df[["sub_sub_category_id", ssub_label_col]].drop_duplicates().itertuples(index=False, name=None))
    else:
        ssub_id_to_label = {ssid: ssid for ssid in map_df["sub_sub_category_id"].astype(str).unique()}

    return {
        "main_ids": main_ids,
        "main_to_subs": main_to_subs,
        "pair_to_subsubs": pair_to_subsubs,
        "labels": {"main": main_id_to_label, "sub": sub_id_to_label, "ssub": ssub_id_to_label},
    }


def apply_glossary_translate(series_ar, glossary_df):
    """Optional helper: glossary exact match; else return Arabic."""
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
Search products, choose **category_id → sub_category_id → sub_sub_category_id**,  
click **Apply IDs to all filtered rows**, then download the updated Excel.
""")

col1, col2, col3 = st.columns(3)
with col1:
    product_file = st.file_uploader("Product List (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="prod")
with col2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
with col3:
    glossary_file = st.file_uploader("(Optional) Translation Glossary (.csv)", type=["csv"], key="gloss")

prod_df = read_any_table(product_file) if product_file else None
map_df = read_any_table(mapping_file) if mapping_file else None
glossary_df = read_any_table(glossary_file) if glossary_file else None

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

lookups = build_mapping_struct(map_df)
labels = lookups["labels"]

work = prod_df.copy()
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""

if "ProductNameEn" not in work.columns:
    work["ProductNameEn"] = apply_glossary_translate(work["name_ar"], glossary_df)

with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

st.subheader("Find products & bulk-assign category IDs")

q = st.text_input("Search by 'name' or 'name_ar':", "")
if q.strip():
    qlower = q.strip().lower()
    mask = work["name"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["name_ar"].astype(str).str.lower().str.contains(qlower, na=False) | \
           work["ProductNameEn"].astype(str).str.lower().str.contains(qlower, na=False)
else:
    mask = pd.Series(True, index=work.index)

filtered = work[mask].copy()
st.caption(f"Matched rows: {filtered.shape[0]}")

main_opts = [""] + lookups["main_ids"]
sel_main = st.selectbox(
    "Main category_id",
    options=main_opts,
    format_func=lambda v: labels["main"].get(v, v)
)

sub_opts = [""] + (lookups["main_to_subs"].get(sel_main, []) if sel_main else [])
sel_sub = st.selectbox(
    "Sub sub_category_id (filtered by main)",
    options=sub_opts,
    format_func=lambda v: labels["sub"].get(v, v)
)

subsub_opts = [""] + (lookups["pair_to_subsubs"].get((sel_main, sel_sub), []) if sel_main and sel_sub else [])
sel_subsub = st.selectbox(
    "Sub-Sub sub_sub_category_id (filtered by sub)",
    options=subsub_opts,
    format_func=lambda v: labels["ssub"].get(v, v)
)

# ---- FORCE WRITING IDS (not labels) ----
rev_main = {v: k for k, v in labels["main"].items()}
rev_sub  = {v: k for k, v in labels["sub"].items()}
rev_ssub = {v: k for k, v in labels["ssub"].items()}

def as_id(value, level):
    """Ensure we always write the ID; convert labels -> IDs if needed."""
    if not value:
        return ""
    if level == "main":
        return value if value in labels["main"] else rev_main.get(value, value)
    if level == "sub":
        return value if value in labels["sub"] else rev_sub.get(value, value)
    if level == "ssub":
        return value if value in labels["ssub"] else rev_ssub.get(value, value)
    return value

if st.button("Apply IDs to all filtered rows"):
    main_id = as_id(sel_main, "main")
    sub_id  = as_id(sel_sub, "sub")
    ssub_id = as_id(sel_subsub, "ssub")

    if main_id:
        work.loc[mask, "category_id"] = main_id
    if sub_id:
        work.loc[mask, "sub_category_id"] = sub_id
    if ssub_id:
        work.loc[mask, "sub_sub_category_id"] = ssub_id

    filtered = work[mask].copy()
    st.success("Applied your selection to all filtered rows (IDs only).")

st.dataframe(
    filtered[["merchant_sku", "name", "name_ar",
              "category_id", "sub_category_id", "sub_sub_category_id"]],
    use_container_width=True, height=340
)

st.subheader("Download")
excel_bytes = to_excel_download(work, sheet_name="Products")
st.download_button(
    label="⬇️ Download Updated Excel",
    data=excel_bytes,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.caption("Dropdowns show labels when available, but the table stores **IDs** so your export is ready for your system.")
