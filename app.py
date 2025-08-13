import io
import re
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# For image previews & fetching
import requests
from PIL import Image
from io import BytesIO

# ---------- Page setup ----------
st.set_page_config(
    page_title="Product List: Mapping + AI Descriptions (OpenAI→DeepL)",
    layout="wide",
)

# ---------- Expected Product List columns ----------
# Your original product list headers:
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # optional image URL column:
    # 'thumbnail' (W) or 'image_url'
]

# ---------- DeepL (official SDK) ----------
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

# ---------- OpenAI (official SDK) for image→EN description ----------
openai_client = None
openai_active = False
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")
    if OPENAI_API_KEY:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        openai_active = True
except Exception:
    openai_client = None
    openai_active = False


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
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(\d+)\s*(مل|ml)\b", r"\1 مل", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(جم|g)\b",  r"\1 جم", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(كغ|kg)\b", r"\1 كغ", s, flags=re.I)
    s = re.sub(r"\b(\d+)\s*(قطعة|pcs?)\b", r"\1 قطعة", s, flags=re.I)
    return s


# -------- DeepL batch translation helpers --------
def translate_deepl_ar_to_en(texts):
    """Arabic -> English with batching & progress for name_en preview/search."""
    if not translator:
        return list(texts)

    results = list(texts)
    idx_texts = [(i, (t if isinstance(t, str) else "")) for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    if not idx_texts:
        return results

    MAX_ITEMS = 45
    MAX_CHARS = 28000
    start, translated_count = 0, 0
    error_message = None

    while start < len(idx_texts):
        batch, chars, k = [], 0, start
        while k < len(idx_texts) and len(batch) < MAX_ITEMS:
            i, t = idx_texts[k]
            if batch and (chars + len(t) > MAX_CHARS): break
            batch.append((i, t)); chars += len(t); k += 1
        try:
            texts_only = [t for _, t in batch]
            res = translator.translate_text(texts_only, source_lang="AR", target_lang="EN-GB")
            out_texts = [r.text for r in res] if isinstance(res, list) else [res.text]
            for (i, _), out in zip(batch, out_texts):
                results[i] = out; translated_count += 1
            start = k
        except Exception as e:
            error_message = str(e); break

    if translated_count:
        st.success(f"AR→EN: {translated_count} / {len(idx_texts)} translated.")
    else:
        st.warning("DeepL AR→EN returned no translations; keeping Arabic.")
    if error_message:
        st.warning(f"DeepL AR→EN stopped due to API error: {error_message}")
    return results


def translate_deepl_en_to_ar(texts):
    """English -> Arabic with batching (for AI descriptions)."""
    if not translator:
        return list(texts)

    results = list(texts)
    idx_texts = [(i, (t if isinstance(t, str) else "")) for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    if not idx_texts:
        return results

    MAX_ITEMS = 45
    MAX_CHARS = 28000
    start, translated_count = 0, 0
    error_message = None

    while start < len(idx_texts):
        batch, chars, k = [], 0, start
        while k < len(idx_texts) and len(batch) < MAX_ITEMS:
            i, t = idx_texts[k]
            if batch and (chars + len(t) > MAX_CHARS): break
            batch.append((i, t)); chars += len(t); k += 1
        try:
            texts_only = [t for _, t in batch]
            res = translator.translate_text(texts_only, source_lang="EN", target_lang="AR")
            out_texts = [r.text for r in res] if isinstance(res, list) else [res.text]
            for (i, _), out in zip(batch, out_texts):
                results[i] = out; translated_count += 1
            start = k
        except Exception as e:
            error_message = str(e); break

    if translated_count:
        st.info(f"EN→AR descriptions: {translated_count} translated.")
    if error_message:
        st.warning(f"DeepL EN→AR stopped due to API error: {error_message}")
    return results


def to_excel_download(df, sheet_name="Products"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer


# ---------- Mapping structures ----------
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


# ---------- Description helpers (fallback templates) ----------
SIZE_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*(?P<u>ml|l|g|kg|oz|fl\s?oz|mL|ML|KG|G|L)\b", flags=re.I)
COUNT_RE = re.compile(r"\b(?P<count>\d+)\s*(?:pcs?|قطع(?:ة)?|pack|pkt|Pk|CT)\b", flags=re.I)
SCENT_RE = re.compile(r"\b(lemon|rose|lavender|musk|jasmine|apple|pine|fresh|ocean|vanilla|berry)\b", flags=re.I)

def extract_attrs_en(name_en: str):
    if not isinstance(name_en, str):
        name_en = ""
    brand = name_en.split()[0] if name_en.strip() else ""
    size = None
    m = SIZE_RE.search(name_en); 
    if m:
        size = f'{m.group("num")} {m.group("u").upper()}'.replace("ML","ml").replace("KG","kg").replace("G","g")
    count = None
    m2 = COUNT_RE.search(name_en); 
    if m2:
        count = m2.group("count")
    scent = None
    m3 = SCENT_RE.search(name_en.lower()); 
    if m3:
        scent = m3.group(1).title()
    return brand, size, count, scent

def make_desc_en(title: str) -> str:
    title = (title or "").strip()
    brand, size, count, scent = extract_attrs_en(title)
    parts = []
    if brand:
        parts.append(f"{brand} — premium quality.")
    if title:
        parts.append(f"{title}.")
    if size:
        parts.append(f"Size: {size}.")
    if count:
        parts.append(f"Pack: {count} pcs.")
    if scent:
        parts.append(f"Scent: {scent}.")
    parts.append("Ideal for everyday household use.")
    return " ".join(parts)

def make_desc_ar_from_title(title_ar: str) -> str:
    t = clean_arabic_text(title_ar or "")
    if not t:
        return ""
    return f"{t}. جودة عالية مناسبة للاستخدام اليومي في المنزل."


# ---------- OpenAI vision: describe an image URL ----------
def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def openai_describe_image(url: str) -> str:
    """
    Returns a concise ecommerce-style ENGLISH product description from an image URL.
    Falls back to empty string on error. Requires OPENAI_API_KEY.
    """
    if not openai_active or not is_valid_url(url):
        return ""

    prompt = (
        "You are a product copy expert. Look at the product image and write a concise, "
        "ecommerce-ready English product description (1-2 sentences). "
        "Include brand, form, size/count if visible, and typical use. "
        "Avoid marketing fluff; be accurate to the image."
    )
    try:
        # Using Chat Completions API (v1 python SDK)
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": url}},
                    ],
                }
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""


# ---------- Description generation (image -> EN; EN -> AR) ----------
def generate_descriptions_via_openai(df: pd.DataFrame, mask: pd.Series = None) -> pd.DataFrame:
    """
    For rows in mask (or all rows), use image URL (thumbnail or image_url)
    -> OpenAI EN description -> DeepL EN->AR -> overwrite:
       df['name'] (EN), df['name_ar'] (AR)
    If image or OpenAI fails, fall back to template built from existing name_en/name.
    """
    work_df = df if mask is None else df.loc[mask].copy()

    # Find image column
    img_col = None
    for candidate in ["thumbnail", "image_url"]:
        if candidate in df.columns:
            img_col = candidate
            break

    # Build EN descriptions
    desc_en_list = []
    for idx, row in work_df.iterrows():
        url = str(row.get(img_col, "")) if img_col else ""
        en = ""
        if url:
            en = openai_describe_image(url)
        if not en:
            # fallback to template from existing English name
            seed = row.get("name_en") or row.get("name") or ""
            en = make_desc_en(str(seed))
        desc_en_list.append(en)

    desc_en = pd.Series(desc_en_list, index=work_df.index)

    # Build AR descriptions (DeepL preferred)
    if deepl_active:
        desc_ar = translate_deepl_en_to_ar(desc_en.tolist())
        desc_ar = pd.Series(desc_ar, index=work_df.index)
    else:
        # fallback Arabic from cleaned Arabic title
        ar_seed = work_df.get("name_ar_clean", work_df.get("name_ar", pd.Series([""]*len(work_df)))).fillna("").astype(str)
        desc_ar = ar_seed.map(make_desc_ar_from_title)

    # Overwrite columns A/B
    df.loc[work_df.index, "name"] = desc_en
    df.loc[work_df.index, "name_ar"] = desc_ar
    return df


# ---------- UI ----------
st.title("🛒 Product List: Mapping + AI Descriptions (OpenAI→DeepL)")

st.markdown("""
**Flow**  
1) Upload **Product List** & **Category Mapping**.  
2) (Optional) Auto AR→EN for search preview (name_en).  
3) Search, pick Main/Sub/Sub-Sub, **Apply** (Sub/Sub-Sub saved as numbers).  
4) (NEW) **Generate Descriptions from Images** → overwrite Column A (EN) & Column B (AR).  
5) Download full or filtered Excel.
""")

col1, col2, col3 = st.columns(3)
with col1:
    product_file = st.file_uploader("Product List (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="prod")
with col2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
with col3:
    glossary_file = st.file_uploader("(Optional) Glossary (.csv, reserved)", type=["csv"], key="gloss")

# Read files
prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

# --- Detect a NEW upload and clear previous working data ---
new_upload = False
if product_file is not None:
    upload_sig = (product_file.name, product_file.size, getattr(product_file, "type", None))
    if st.session_state.get("upload_sig") != upload_sig:
        st.session_state.upload_sig = upload_sig
        st.session_state.pop("work", None)   # discard old edits ONLY on new upload
        new_upload = True

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

# ---------- Auto-clean + (optional) AR->EN translate for name_en ----------
for col in ["name_ar_clean", "name_en", "ProductNameEn"]:
    if col not in prod_df.columns:
        prod_df[col] = ""

if "name_ar" in prod_df.columns:
    prod_df["name_ar_clean"] = prod_df["name_ar"].astype(str).map(clean_arabic_text)
else:
    st.error("Column 'name_ar' not found in your Product List file. Translation skipped.")

if deepl_active and "name_ar_clean" in prod_df.columns:
    st.info("🔤 DeepL key detected — translating Arabic → English for search (name_en)…")
    prod_df["name_en"] = translate_deepl_ar_to_en(prod_df["name_ar_clean"].fillna("").tolist())
else:
    if "name_ar_clean" in prod_df.columns:
        prod_df["name_en"] = prod_df["name_ar_clean"]
    st.warning("DeepL not active — 'name_en' mirrors cleaned Arabic for search convenience.")

# Keep ProductNameEn in sync (if other parts use it)
prod_df["ProductNameEn"] = prod_df["name_en"]

with st.expander("Translation preview (first 10)"):
    st.dataframe(prod_df[["name_ar", "name_ar_clean", "name_en"]].head(10), use_container_width=True)

# ---------- Create/keep the working dataframe ----------
# Only replace the working df on first load OR new upload; keep it otherwise (so edits persist).
if ("work" not in st.session_state) or new_upload:
    st.session_state.work = prod_df.copy()

# Build lookups for mapping
lookups = build_mapping_struct_fixed(map_df)

# ---------- Working dataframe (persisted) ----------
work = st.session_state.work

# Ensure columns exist and are string-typed
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""
    else:
        work[col] = work[col].fillna("").astype(str)

# ---------- Search + Bulk Assign ----------
st.subheader("Find products & bulk-assign category IDs")

# Ensure search key exists BEFORE rendering widget
if "search_q" not in st.session_state:
    st.session_state["search_q"] = ""

c1, c2 = st.columns([3, 1])
with c1:
    st.text_input(
        "Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):",
        key="search_q",
        placeholder="Type to filter…",
    )
with c2:
    if st.button("Show all"):
        st.session_state["search_q"] = ""
        st.rerun()

qval = st.session_state["search_q"].strip().lower()
if qval:
    mask = (
        work["name"].astype(str).str.lower().str.contains(qval, na=False)
        | work["name_ar"].astype(str).str.lower().str.contains(qval, na=False)
        | work["ProductNameEn"].astype(str).str.lower().str.contains(qval, na=False)
    )
else:
    mask = pd.Series(True, index=work.index)

filtered = work[mask].copy()
st.caption(f"Matched rows in view: {filtered.shape[0]}")

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
    if sel_main:
        work.loc[mask, "category_id"] = sel_main

    sub_no = get_sub_no(sel_main, sel_sub)
    ssub_no = get_ssub_no(sel_main, sel_sub, sel_subsub)

    if sub_no:
        work.loc[mask, "sub_category_id"] = str(sub_no)
    if ssub_no:
        work.loc[mask, "sub_sub_category_id"] = str(ssub_no)

    # Persist updates
    st.session_state.work = work
    filtered = work[mask].copy()
    st.success("Applied (Main name; Sub & Sub-Sub numbers) to all filtered rows.")

# ---------- NEW: Generate Descriptions from Images (OVERWRITES A/B) ----------
st.subheader("AI Descriptions from Images (OpenAI → DeepL)")
if not openai_active:
    st.warning("OpenAI key not detected — set OPENAI_API_KEY in Streamlit Secrets to enable image-based descriptions.")
scope = st.radio("Scope", ["Filtered rows", "All rows"], horizontal=True)
if st.button("🖼️ Generate from Images → English, then DeepL → Arabic (overwrite A/B)"):
    if scope == "Filtered rows":
        work = generate_descriptions_via_openai(work, mask=mask)
    else:
        work = generate_descriptions_via_openai(work, mask=None)
    st.session_state.work = work
    filtered = work[mask].copy()
    st.success("Descriptions generated from images. Column A (EN) & B (AR) overwritten.")

# ---------- Image preview (from column W) ----------
st.subheader("Image thumbnails (from column W)")
img_col = None
for candidate in ["thumbnail", "image_url"]:
    if candidate in work.columns:
        img_col = candidate
        break

def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def fetch_image_thumb(url: str, timeout=5):
    try:
        if not is_valid_url(url):
            return None
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        return None

if img_col:
    st.caption(f"Using image URLs from column: **{img_col}** (showing thumbnails for current view)")
    urls = filtered[img_col].fillna("").astype(str).tolist()
    max_show = min(24, len(urls))
    grid_cols = st.columns(6)
    shown = 0
    for idx, u in enumerate(urls[:max_show]):
        img = fetch_image_thumb(u)
        with grid_cols[idx % 6]:
            if img:
                st.image(img, caption=f"Row {filtered.index[idx]}", use_container_width=True)
            else:
                st.write("No image")
            shown += 1
    if shown == 0:
        st.info("No valid image URLs in current selection.")
else:
    st.info("No `thumbnail` or `image_url` column found — skip image previews.")

# ---------- Tables (show ALL rows; tall viewport) ----------
st.markdown("### Current selection (all rows in view)")
st.dataframe(
    filtered[[
        "merchant_sku", "name", "name_ar", "name_ar_clean", "name_en",
        "category_id", "sub_category_id", "sub_sub_category_id"
    ]],
    use_container_width=True,
    height=900,
)

# --- Quick previews for full DF and mapping (first rows only) ---
with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

# Optional reset (handy for testing)
with st.expander("Reset working data"):
    if st.button("🔄 Reset working data (start over)"):
        st.session_state.pop("work", None)
        st.rerun()

# ---------- Download ----------
st.subheader("Download")
excel_full = to_excel_download(work, sheet_name="Products")
st.download_button(
    label="⬇️ Download FULL Excel (all rows)",
    data=excel_full,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

excel_filtered = to_excel_download(filtered, sheet_name="Filtered")
st.download_button(
    label="⬇️ Download FILTERED Excel (current view)",
    data=excel_filtered,
    file_name="products_filtered.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.caption(
    "Main category stays as a NAME (no numeric main ID provided). "
    "Sub & Sub-Sub are saved as NUMBERS from your mapping. "
    "Arabic is cleaned. If keys are set: OpenAI describes images → English, then DeepL translates → Arabic."
)
