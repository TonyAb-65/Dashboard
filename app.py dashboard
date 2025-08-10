import io
import pandas as pd
import streamlit as st

# ---------- Config ----------
st.set_page_config(page_title="Product Mapper & Translator", layout="wide")

REQUIRED_PRODUCT_COLS = ["SKU", "ProductNameAr"]
REQUIRED_MAP_COLS = [
    "SubCategoryAr", "SubCategoryEn", "SubCategoryID",
    "SubSubCategoryAr", "SubSubCategoryEn", "SubSubCategoryID"
]
GLOSSARY_COLS = ["Arabic", "English"]

# ---------- Helpers ----------
def read_any_table(uploaded_file):
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
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
def build_mapping_struct(map_df):
    for c in ["SubCategoryAr","SubCategoryEn","SubCategoryID","SubSubCategoryAr","SubSubCategoryEn","SubSubCategoryID"]:
        map_df[c] = map_df[c].astype(str)

    subcats = (
        map_df[["SubCategoryEn", "SubCategoryID"]]
        .drop_duplicates()
        .sort_values("SubCategoryEn")
        .reset_index(drop=True)
    )

    subcat_en_to_id = dict(zip(subcats["SubCategoryEn"], subcats["SubCategoryID"]))

    subcat_to_subsubs = {}
    for sc_en, grp in map_df.groupby("SubCategoryEn"):
        items = (
            grp[["SubSubCategoryEn", "SubSubCategoryID"]]
            .drop_duplicates()
            .sort_values("SubSubCategoryEn")
            .values.tolist()
        )
        subcat_to_subsubs[sc_en] = items

    return subcats["SubCategoryEn"].tolist(), subcat_en_to_id, subcat_to_subsubs

def apply_glossary_translate(series_ar, glossary_df):
    if glossary_df is None:
        return series_ar.astype(str)
    glossary_df["Arabic"] = glossary_df["Arabic"].astype(str)
    glossary_df["English"] = glossary_df["English"].astype(str)
    mapping = dict(zip(glossary_df["Arabic"], glossary_df["English"]))
    return series_ar.astype(str).map(lambda x: mapping.get(x, x))

def enrich_ids(df, subcat_en_to_id, subcat_to_subsubs):
    if "SubCategoryID" not in df.columns:
        df["SubCategoryID"] = ""
    if "SubSubCategoryID" not in df.columns:
        df["SubSubCategoryID"] = ""

    def get_subcat_id(sc_en):
        if pd.isna(sc_en) or not str(sc_en).strip():
            return ""
        return subcat_en_to_id.get(str(sc_en), "")

    def get_subsub_id(sc_en, ssc_en):
        if pd.isna(sc_en) or pd.isna(ssc_en):
            return ""
        sc_en = str(sc_en)
        ssc_en = str(ssc_en)
        pairs = subcat_to_subsubs.get(sc_en, [])
        for name, sid in pairs:
            if name == ssc_en:
                return sid
        return ""

    df["SubCategoryID"] = df["SubCategoryEn"].map(get_subcat_id)
    df["SubSubCategoryID"] = df.apply(
        lambda r: get_subsub_id(r.get("SubCategoryEn",""), r.get("SubSubCategoryEn","")), axis=1
    )
    return df

def to_excel_download(df, sheet_name="Products"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer

# ---------- UI ----------
st.title("🛒 Product List Translator & Category Mapper")

st.markdown("""
Upload your **Product List** and **Category Mapping** files, optionally a **Translation Glossary**.
Then assign Sub-Category and Sub-Sub-Category per row using cascading dropdowns, auto-fill IDs, and download the enriched Excel.
""")

col1, col2, col3 = st.columns(3)
with col1:
    product_file = st.file_uploader("Product List (.xlsx/.csv)", type=["xlsx","xls","csv"], key="prod")
with col2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"], key="map")
with col3:
    glossary_file = st.file_uploader("(Optional) Translation Glossary (.csv)", type=["csv"], key="gloss")

prod_df = read_any_table(product_file) if product_file else None
map_df = read_any_table(mapping_file) if mapping_file else None
glossary_df = read_any_table(glossary_file) if glossary_file else None

if prod_df is not None:
    ok = validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List")
else:
    ok = False

if map_df is not None:
    ok = ok and validate_columns(map_df, REQUIRED_MAP_COLS, "Category Mapping")
else:
    ok = False

if glossary_df is not None:
    if not validate_columns(glossary_df, GLOSSARY_COLS, "Translation Glossary"):
        glossary_df = None

if not ok:
    st.info("Upload the required files to continue.")
    st.stop()

subcat_list, subcat_en_to_id, subcat_to_subsubs = build_mapping_struct(map_df)

st.subheader("Preview")
with st.expander("Product List (first rows)"):
    st.dataframe(prod_df.head(20), use_container_width=True)
with st.expander("Category Mapping (first rows)"):
    st.dataframe(map_df.head(20), use_container_width=True)
if glossary_df is not None:
    with st.expander("Glossary (first rows)"):
        st.dataframe(glossary_df.head(20), use_container_width=True)

work = prod_df.copy()

if "ProductNameEn" not in work.columns:
    work["ProductNameEn"] = apply_glossary_translate(work["ProductNameAr"], glossary_df)

if "SubCategoryEn" not in work.columns:
    work["SubCategoryEn"] = ""
if "SubSubCategoryEn" not in work.columns:
    work["SubSubCategoryEn"] = ""

st.subheader("Assign Categories")
st.markdown("Pick **SubCategoryEn** first; the **SubSubCategoryEn** options will filter automatically.")

work_stepA = st.data_editor(
    work[["SKU","ProductNameAr","ProductNameEn","SubCategoryEn"]],
    column_config={
        "SubCategoryEn": st.column_config.SelectboxColumn(
            "SubCategoryEn",
            options=[""] + subcat_list,
            help="Choose a Sub-Category",
            required=False
        )
    },
    hide_index=True,
    use_container_width=True,
    num_rows="dynamic",
    key="stepA"
)
work["SubCategoryEn"] = work_stepA["SubCategoryEn"]

st.write("Now choose **SubSubCategoryEn** per row:")
new_subsubs = []
for i in range(len(work)):
    r = work.iloc[i]
    sc = r.get("SubCategoryEn", "")
    pairs = subcat_to_subsubs.get(sc, []) if sc else []
    opts = [""] + [p[0] for p in pairs]
    default_val = r.get("SubSubCategoryEn", "")
    if default_val not in opts:
        default_val = ""
    val = st.selectbox(
        f"Row {i+1} — SKU {r['SKU']}",
        options=opts,
        index=opts.index(default_val) if default_val in opts else 0,
        key=f"subsub_{i}"
    )
    new_subsubs.append(val)

work["SubSubCategoryEn"] = new_subsubs
work = enrich_ids(work, subcat_en_to_id, subcat_to_subsubs)

st.subheader("Result Preview")
st.dataframe(
    work[
        ["SKU", "ProductNameAr", "ProductNameEn", "SubCategoryEn", "SubCategoryID",
         "SubSubCategoryEn", "SubSubCategoryID"]
    ],
    use_container_width=True
)

excel_bytes = to_excel_download(work)
st.download_button(
    label="⬇️ Download Updated Excel",
    data=excel_bytes,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.caption("Tip: Maintain your glossary CSV to steadily improve English product titles for e-commerce style naming.")
