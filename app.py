import io
import re
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

# Category Mapping: we’ll detect via UI (no fixed headers required)
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


def guess_col(cols, patterns):
    """Pick the first column whose name matches any of the regex patterns (case-insensitive)."""
    for p in patterns:
        rx = re.compile(p, re.I)
        for c in cols:
            if rx.fullmatch(c) or rx.search(c):
                return c
    return None


@st.cache_data
def build_mapping_struct(map_df: pd.DataFrame, cfg: dict):
    """
    Build 3-level cascade using selected columns.
    cfg keys:
      main_id, main_name (opt), sub_id, sub_name (opt), ssub_id, ssub_name (opt)
    """
    # Normalize to string to avoid 1 vs "1" issues
    for k in ["main_id", "sub_id", "ssub_id"]:
        map_df[cfg[k]] = map_df[cfg[k]].astype(str).str.strip()

    # Unique mains (IDs)
    main_ids = sorted(map_df[cfg["main_id"]].dropna().astype(str).unique().tolist())

    # Main -> [Sub IDs]
    main_to_subs = {}
    for mc, g1 in map_df.groupby(cfg["main_id"], dropna=True):
        subs = sorted(g1[cfg["sub_id"]].dropna().astype(str).str.strip().unique().tolist())
        main_to_subs[str(mc)] = subs

    # (Main, Sub) -> [Sub-Sub IDs]
    pair_to_subsubs = {}
    for (mc, sc), g2 in map_df.groupby([cfg["main_id"], cfg["sub_id"]], dropna=True):
        ssubs = sorted(g2[cfg["ssub_id"]].dropna().astype(str).str.strip().unique().tolist())
        pair_to_subsubs[(str(mc), str(sc))] = ssubs

    # Labels (fallback to IDs if name cols not provided)
    def build_labels(id_col, name_col):
        if name_col:
            tmp = map_df[[id_col, name_col]].drop_duplicates()
            return dict(tmp.itertuples(index=False, name=None))
        else:
            return {v: v for v in map_df[id_col].astype(str).unique()}

    main_labels = build_labels(cfg["main_id"], cfg.get("main_name"))
    sub_labels  = build_labels(cfg["sub_id"],  cfg.get("sub_name"))
    ssub_labels = build_labels(cfg["ssub_id"], cfg.get("ssub_name"))

    return {
        "main_ids": main_ids,
        "main_to_subs": main_to_subs,
        "pair_to_subsubs": pair_to_subsubs,
        "labels": {"main": main_labels, "sub": sub_labels, "ssub": ssub_labels},
        "cfg": cfg,
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
1) Upload your **Product List** and **Category Mapping** files  
2) Map the **ID** and optional **Name** columns from the Category Mapping (Main → Sub → Sub-Sub)  
3) Search products, pick **IDs** (names shown), **Apply to all filtered rows**, and download
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
if map_df is None:
    ok = False

if not ok:
    st.info("Upload both Product List and Category Mapping to continue.")
    st.stop()

# --------- Category Mapping: let user select columns ---------
st.subheader("Map Category Mapping columns")

cols = list(map_df.columns)

# Smart defaults / guesses
guess_main_id  = guess_col(cols, [r"^category_id$", r"^main_id$", r"category.*id"])
guess_sub_id   = guess_col(cols, [r"^sub_category_id$", r"sub.*cat.*id"])
guess_ssub_id  = guess_col(cols, [r"^sub_sub_category_id$", r"sub.*sub.*cat.*id"])

guess_main_name = guess_col(cols, [r"^category_name$", r"category.*(name|en|ar)$", r"^category$"])
guess_sub_name  = guess_col(cols, [r"^sub_category_name$", r"sub.*cat.*(name|en|ar)$", r"^sub_category$"])
guess_ssub_name = guess_col(cols, [r"^sub_sub_category_name$", r"sub.*sub.*cat.*(name|en|ar)$", r"^sub_sub_category$"])

c1, c2 = st.columns(2)
with c1:
    main_id_col = st.selectbox("Main ID column (required)", options=cols, index=cols.index(guess_main_id) if guess_main_id in cols else 0)
    sub_id_col  = st.selectbox("Sub ID column (required)",  options=cols, index=cols.index(guess_sub_id)  if guess_sub_id  in cols else 0)
    ssub_id_col = st.selectbox("Sub-Sub ID column (required)", options=cols, index=cols.index(guess_ssub_id) if guess_ssub_id in cols else 0)
with c2:
    main_name_col = st.selectbox("Main NAME column (optional)", options=["(none)"] + cols,
                                 index=(["(none)"] + cols).index(guess_main_name) if guess_main_name in cols else 0)
    sub_name_col  = st.selectbox("Sub NAME column (optional)",  options=["(none)"] + cols,
                                 index=(["(none)"] + cols).index(guess_sub_name)  if guess_sub_name  in cols else 0)
    ssub_name_col = st.selectbox("Sub-Sub NAME column (optional)", options=["(none)"] + cols,
                                 index=(["(none)"] + cols).index(guess_ssub_name) if guess_ssub_name in cols else 0)

cfg = {
    "main_id": main_id_col,
    "sub_id": sub_id_col,
    "ssub_id": ssub_id_col,
    "main_name": None if main_name_col == "(none)" else main_name_col,
    "sub_name":  None if sub_name_col  == "(none)" else sub_name_col,
    "ssub_name": None if ssub_name_col == "(none)" else ssub_name_col,
}

# Build lookups
lookups = build_mapping_struct(map_df, cfg)
labels = lookups["labels"]

# ---------- Product working frame ----------
work = prod_df.copy()
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""

# Optional English helper column for searching
if "ProductNameEn" not in work.columns:
    work["ProductNameEn"] = apply_glossary_translate(work["name_ar"], glossary_df)

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

# Cascading pickers (show names if provided, but options are IDs)
main_opts = [""] + lookups["main_ids"]
sel_main = st.selectbox(
    "Main (ID shown as label if no name)",
    options=main_opts,
    format_func=lambda v: labels["main"].get(v, v)
)

sub_opts = [""] + (lookups["main_to_subs"].get(sel_main, []) if sel_main else [])
sel_sub = st.selectbox(
    "Sub (filtered by Main)",
    options=sub_opts,
    format_func=lambda v: labels["sub"].get(v, v)
)

subsub_opts = [""] + (lookups["pair_to_subsubs"].get((sel_main, sel_sub), []) if sel_main and sel_sub else [])
sel_subsub = st.selectbox(
    "Sub-Sub (filtered by Sub)",
    options=subsub_opts,
    format_func=lambda v: labels["ssub"].get(v, v)
)

# ---- FORCE WRITING IDS (never names) ----
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

st.caption("Dropdowns display names (when provided), but the table always stores **IDs**.")
