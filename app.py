import io
import pandas as pd
import streamlit as st

# ---------- Page setup ----------
st.set_page_config(page_title="Product List Translator & Category Mapper", layout="wide")

# ---------- Expected Product List columns ----------
# (Main stays as name; Sub & Sub-Sub columns store numbers after Apply)
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
]

# Optional glossary CSV columns for name_ar -> English helper (exact match)
GLOSSARY_COLS = ["Arabic", "English"]


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


def apply_glossary_translate(series_ar, glossary_df):
    """Optional helper: build an English helper column via exact-match glossary."""
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
    # (Main NAME, Sub NAME) -> Sub NUMBER
    sub_name_to_no_by_main = {}
    # (Main NAME, Sub NAME, Sub-Sub NAME) -> Sub-Sub NUMBER
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
Search products, choose **Main (name) → Sub (name) → Sub-Sub (name)**, then **Apply to filtered rows**.  
The app writes **numbers** for Sub & Sub-Sub from your mapping’s **“… NO”** columns; Main stays as a **name**.
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

# Validate availability
ok = True
if prod_df is None or not validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List"):
    ok = False
# Ensure mapping has the exact required columns
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

# Build lookups
lookups = build_mapping_struct_fixed(map_df)

# Prepare working frame
work = prod_df.copy()
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""

# Optional English helper column for searching
if "ProductNameEn" not in work.columns:
    work["ProductNameEn"] = apply_glossary_translate(work["name_ar"], glossary_df)

# --- Previews ---
with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

# ---------- Search + Bulk Assign ----------
st.subheader("Find products & bulk-assign category IDs")

q = st.text_input("Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):", "")
if q.strip():
    qlower = q.strip().lower()
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

    filtered = work[mask].copy()
    st.success("Applied (Main name; Sub & Sub-Sub numbers) to all filtered rows.")

# Show filtered preview (you should see numbers in sub/sub-sub columns)
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

st.caption("Main category stays as a NAME (no numeric main ID provided). Sub & Sub-Sub are saved as NUMBERS from your mapping.")
