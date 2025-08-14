import io
import re
import time
import math
from typing import List, Tuple
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ---------- Page ----------
st.set_page_config(
    page_title="Product Mapping + Batched AI Titles (OpenAI → DeepL/OpenAI)",
    layout="wide",
)

# ---------- Expected Product List columns (original layout) ----------
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # image URL column W — we will ALWAYS read 'thumbnail'
]

# ---------- API clients ----------
# DeepL
translator = None
deepl_active = False
deepl_status_note = ""
try:
    import deepl
    DEEPL_API_KEY = st.secrets.get("DEEPL_API_KEY")
    if DEEPL_API_KEY:
        translator = deepl.Translator(DEEPL_API_KEY)
        deepl_active = True
except Exception as e:
    translator = None
    deepl_active = False
    deepl_status_note = f"(DeepL unavailable: {e})"

# OpenAI
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


# ---------- File IO ----------
def read_any_table(uploaded_file):
    """Load xlsx/xls/csv with explicit engine in cloud."""
    if uploaded_file is None:
        return None
    fn = uploaded_file.name.lower()
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file, engine="openpyxl")
    if fn.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    raise ValueError("Please upload .xlsx, .xls, or .csv")


def validate_columns(df, required_cols, label):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"{label}: missing required columns: {missing}")
        return False
    return True


# ---------- Text helpers ----------
def strip_markdown(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[*_`]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tidy_title(s: str, max_chars: int = 70) -> str:
    s = strip_markdown(s)
    if len(s) <= max_chars:
        return s
    cut = s[:max_chars].rstrip()
    if " " in cut:
        cut = cut[: cut.rfind(" ")]
    return cut


SIZE_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*(?P<u>ml|l|g|kg|oz|fl\s?oz|mL|ML|KG|G|L)\b", flags=re.I)
COUNT_RE = re.compile(r"\b(?P<count>\d+)\s*(?:pcs?|قطع(?:ة)?|pack|pkt|Pk|CT)\b", flags=re.I)


def template_title_from_name(name_en: str) -> str:
    """Compact fallback title when no image or OpenAI returns nothing."""
    if not isinstance(name_en, str):
        name_en = ""
    name_en = strip_markdown(name_en)
    brand = name_en.split()[0] if name_en.strip() else ""

    size = None
    m = SIZE_RE.search(name_en)
    if m:
        size = f'{m.group("num")} {m.group("u").upper()}'.replace("ML", "ml").replace("KG", "kg").replace("G", "g")
    cnt = None
    m2 = COUNT_RE.search(name_en)
    if m2:
        cnt = m2.group("count")

    parts = []
    if brand:
        parts.append(brand)
    main = " ".join(name_en.split()[:7]).strip()
    if main and main != brand:
        parts.append(main)
    if size:
        parts.append(size)
    if cnt:
        parts.append(f"{cnt} pcs")
    return tidy_title(" ".join(parts), 70)


# ---------- Image helpers ----------
def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def fetch_thumb(url: str, timeout=7):
    """Fetch a small thumbnail for preview; return PIL.Image or None."""
    try:
        if not is_valid_url(url):
            return None
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/125 Safari/537.36")
        }
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        if "image" not in ctype and not url.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
            return None
        content = r.content
        img = Image.open(BytesIO(content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        return None


# ---------- OpenAI: image → EN short title ----------
def openai_title_from_image(url: str, max_chars: int) -> str:
    if not openai_active or not is_valid_url(url):
        return ""
    prompt = (
        "You are an e-commerce title generator. Return ONE short product TITLE only, "
        "6–8 words, max ~70 chars. Include brand if visible and size/count if obvious. "
        "No markdown, no emojis, no extra text. Examples:\n"
        "Fairy Dishwashing Liquid Lemon 650 ml\n"
        "Scotch-Brite Cleaning Sponges Pack of 6"
    )
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system",
                 "content": "You are a precise e-commerce title writer. Output one short title only."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": url}},
                    ],
                },
            ],
            temperature=0.2,
        )
        title = resp.choices[0].message.content or ""
        return tidy_title(title, max_chars)
    except Exception:
        return ""


# ---------- Translation engines ----------
def _deepl_batch(texts: List[str]) -> List[str]:
    """DeepL EN→AR batched."""
    if not translator:
        return list(texts)
    if not texts:
        return []
    MAX_ITEMS = 45
    MAX_CHARS = 28000
    out = [""] * len(texts)
    idx_texts = [(i, t if isinstance(t, str) else "") for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    start = 0
    while start < len(idx_texts):
        chars = 0
        batch: List[Tuple[int, str]] = []
        k = start
        while k < len(idx_texts) and len(batch) < MAX_ITEMS:
            i, t = idx_texts[k]
            if batch and chars + len(t) > MAX_CHARS:
                break
            batch.append((i, t)); chars += len(t); k += 1
        for attempt in range(3):
            try:
                texts_only = [t for _, t in batch]
                res = translator.translate_text(texts_only, source_lang="EN", target_lang="AR")
                outs = [r.text for r in (res if isinstance(res, list) else [res])]
                for (i, _), txt in zip(batch, outs):
                    out[i] = txt
                break
            except Exception:
                time.sleep(1.2 * (attempt + 1))
        start = k
        time.sleep(0.35)
    for i, t in enumerate(out):
        if not t and texts[i]:
            out[i] = texts[i]
    return out


def openai_translate_en_to_ar_batch(texts: List[str]) -> List[str]:
    """OpenAI EN→AR translation, batched."""
    if not openai_active or not texts:
        return list(texts)
    out = [""] * len(texts)
    BATCH = 40

    def translate_chunk(chunk_items: List[Tuple[int, str]]):
        lines = [t for _, t in chunk_items]
        joined = "\n".join(lines)
        sys = "You are a translator. Translate short e-commerce product TITLES to natural, concise Arabic."
        usr = (
            "Translate each line into Arabic. Return EXACTLY the same number of lines, "
            "in the same order, one Arabic title per line, no numbering or extra text.\n\n"
            + joined
        )
        for attempt in range(3):
            try:
                resp = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "system", "content": sys},
                              {"role": "user", "content": usr}],
                    temperature=0.0,
                )
                txt = (resp.choices[0].message.content or "").strip()
                lines_ar = [l.strip() for l in txt.splitlines() if l.strip()][: len(lines)]
                while len(lines_ar) < len(lines):
                    lines_ar.append(lines[len(lines_ar)])
                for (i, _), t_ar in zip(chunk_items, lines_ar):
                    out[i] = t_ar
                return
            except Exception:
                time.sleep(1.2 * (attempt + 1))
        for (i, t_en) in chunk_items:
            out[i] = t_en

    items = [(i, t if isinstance(t, str) else "") for i, t in enumerate(texts)]
    items = [(i, t) for i, t in items if t.strip()]
    for start in range(0, len(items), BATCH):
        chunk = items[start : start + BATCH]
        translate_chunk(chunk)
        time.sleep(0.35)
    for i, t in enumerate(out):
        if not t and texts[i]:
            out[i] = texts[i]
    return out


def translate_en_titles(titles_en: pd.Series, engine: str, batch_size: int) -> pd.Series:
    texts = titles_en.fillna("").astype(str).tolist()
    if engine == "DeepL" and deepl_active:
        out = _deepl_batch(texts)
        return pd.Series(out, index=titles_en.index)
    if engine == "OpenAI":
        out = []
        for start in range(0, len(texts), batch_size):
            part = texts[start : start + batch_size]
            out.extend(openai_translate_en_to_ar_batch(part))
            time.sleep(0.35)
        return pd.Series(out, index=titles_en.index)
    # None: mirror English
    return titles_en.copy()


# ---------- Mapping structures (your original rules) ----------
def build_mapping_struct_fixed(map_df: pd.DataFrame):
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
        scn = r["sub_category_id"]
        scno = r["sub_category_id NO"]
        sscn = r["sub_sub_category_id"]
        sscno = r["sub_sub_category_id NO"]
        sub_name_to_no_by_main[(mc, scn)] = scno
        ssub_name_to_no_by_main_sub[(mc, scn, sscn)] = sscno

    return {
        "main_names": main_names,
        "main_to_subnames": main_to_subnames,
        "pair_to_subsubnames": pair_to_subsubnames,
        "sub_name_to_no_by_main": sub_name_to_no_by_main,
        "ssub_name_to_no_by_main_sub": ssub_name_to_no_by_main_sub,
    }


# ---------- Excel download ----------
def to_excel_download(df, sheet_name="Products"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf


# ---------- Batched titles from images (ALWAYS reads 'thumbnail') ----------
def titles_from_images_batched(
    df: pd.DataFrame,
    row_index: List[int],
    max_chars: int,
    batch_size: int,
) -> pd.Series:
    """Return EN titles for given row indices (batched, throttled, retries)."""
    if "thumbnail" not in df.columns:
        st.error("Column 'thumbnail' not found (expected in column W).")
        return pd.Series([""] * len(row_index), index=row_index, dtype="object")

    titles_en = pd.Series([""] * len(row_index), index=row_index, dtype="object")
    prog = st.progress(0)
    steps = max(1, math.ceil(len(row_index) / max(1, batch_size)))
    for step, start in enumerate(range(0, len(row_index), batch_size), start=1):
        chunk_idx = row_index[start : start + batch_size]
        for i in chunk_idx:
            url = str(df.loc[i, "thumbnail"])
            title = ""
            if url:
                for attempt in range(3):
                    title = openai_title_from_image(url, max_chars)
                    if title:
                        break
                    time.sleep(0.6 * (attempt + 1))
            if not title:
                seed = df.loc[i, "name"]
                title = template_title_from_name(str(seed))
            titles_en.loc[i] = title
        prog.progress(min(step / steps, 1.0))
        time.sleep(0.35)  # throttle between batches
    return titles_en


# ---------- UI ----------
st.title("🛒 Product Mapping + Batched AI Titles (OpenAI → DeepL/OpenAI)")

if deepl_active:
    st.caption("DeepL available. Translation happens ONLY after English titles are generated from images.")
elif deepl_status_note:
    st.caption(deepl_status_note)

st.markdown("""
**Flow**  
1) Upload **Product List** and **Category Mapping**.  
2) Search, pick Main/Sub/Sub-Sub → **Apply** (Sub/Sub-Sub saved as numbers).  
3) (NEW) **Batched image → EN short titles** from column **W: `thumbnail`**; then EN→AR via **DeepL / OpenAI / None**.  
4) Download full or filtered Excel.

**Note:** We removed the earlier Arabic→English step at upload.
""")

# Uploads
c1, c2, c3 = st.columns(3)
with c1:
    product_file = st.file_uploader("Product List (.xlsx/.csv) — must include 'thumbnail' in column W", type=["xlsx", "xls", "csv"], key="prod")
with c2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
with c3:
    glossary_file = st.file_uploader("(Optional) Glossary (.csv, reserved)", type=["csv"], key="gloss")

prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

# Reset working data if new upload
new_upload = False
if product_file is not None:
    sig = (product_file.name, product_file.size, getattr(product_file, "type", None))
    if st.session_state.get("upload_sig") != sig:
        st.session_state.upload_sig = sig
        st.session_state.pop("work", None)
        new_upload = True

# Validate availability
ok = True
if prod_df is None or not validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List"):
    ok = False
if map_df is None or not validate_columns(map_df, [
    "category_id",
    "sub_category_id", "sub_category_id NO",
    "sub_sub_category_id", "sub_sub_category_id NO",
], "Category Mapping"):
    ok = False
if not ok:
    st.info("Upload both files with the required headers to continue.")
    st.stop()

# Working DF persistence
if ("work" not in st.session_state) or new_upload:
    st.session_state.work = prod_df.copy()
work = st.session_state.work

# Ensure required cols exist & string-typed
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""
    else:
        work[col] = work[col].fillna("").astype(str)

# Build mapping lookups
lookups = build_mapping_struct_fixed(map_df)

# ---------- Search + Bulk Apply ----------
st.subheader("Find products & bulk-assign category IDs")
if "search_q" not in st.session_state:
    st.session_state["search_q"] = ""

s1, s2 = st.columns([3, 1])
with s1:
    st.text_input("Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):",
                  key="search_q", placeholder="Type to filter…")
with s2:
    if st.button("Show all"):
        st.session_state["search_q"] = ""
        st.rerun()

q = st.session_state["search_q"].strip().lower()
if q:
    mask = (
        work["name"].astype(str).str.lower().str.contains(q, na=False)
        | work["name_ar"].astype(str).str.lower().str.contains(q, na=False)
    )
else:
    mask = pd.Series(True, index=work.index)

filtered = work[mask].copy()
st.caption(f"Matched rows in view: {filtered.shape[0]}")

main_opts = [""] + lookups["main_names"]
sel_main = st.selectbox("Main (category_id — NAME)", options=main_opts)

sub_opts = [""] + (lookups["main_to_subnames"].get(sel_main, []) if sel_main else [])
sel_sub = st.selectbox("Sub (sub_category_id — NAME, filtered by Main)", options=sub_opts)

subsub_opts = [""] + (lookups["pair_to_subsubnames"].get((sel_main, sel_sub), []) if sel_main and sel_sub else [])
sel_subsub = st.selectbox("Sub-Sub (sub_sub_category_id — NAME, filtered by Sub)", options=subsub_opts)

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
    st.session_state.work = work
    filtered = work[mask].copy()
    st.success("Applied (Main name; Sub & Sub-Sub numbers) to all filtered rows.")

# ---------- Batched image → EN title; EN → AR ----------
st.subheader("Batched image → EN short title (from column 'thumbnail'); EN → AR via DeepL / OpenAI / None")

# Controls
colA, colB, colC, colD = st.columns(4)
with colA:
    max_len = st.slider("Max title length", 50, 90, 70, 5)
with colB:
    batch_size = st.slider("Batch size", 25, 100, 50, 5)
with colC:
    engine = st.selectbox("Arabic translation engine", ["DeepL", "OpenAI", "None"])
with colD:
    scope = st.selectbox("Scope", ["Filtered rows", "All rows"])

# Fixed image column
st.caption("🖼️ Using image URLs from **column W: `thumbnail`** (exact header required).")
if "thumbnail" not in work.columns:
    st.warning("No 'thumbnail' column found. Add it to column W and re-upload to enable image-based titles.")

if st.button("🖼️ Generate short titles (batched)"):
    # target indices
    if scope == "Filtered rows":
        idx_list = filtered.index.tolist()
    else:
        idx_list = work.index.tolist()

    if not idx_list:
        st.warning("No rows to process.")
    else:
        st.info(f"Processing {len(idx_list)} rows in batches of {batch_size}…")
        # 1) English titles from images
        titles_en = titles_from_images_batched(work, idx_list, max_len, batch_size)
        work.loc[idx_list, "name"] = titles_en.loc[idx_list]

        # 2) Arabic titles per chosen engine
        if engine == "DeepL" and not deepl_active:
            st.warning("DeepL not available (or quota exceeded). Arabic will mirror English.")
            titles_ar = titles_en.loc[idx_list]
        else:
            st.info(f"Translating English → Arabic via {engine}…")
            titles_ar = translate_en_titles(titles_en.loc[idx_list], engine, batch_size)

        work.loc[idx_list, "name_ar"] = titles_ar
        st.session_state.work = work
        filtered = work[mask].copy()
        st.success("Titles updated (Column A EN, Column B AR).")

# Re-translate Arabic later without regenerating English
st.caption("Need to re-translate Arabic later (e.g., after DeepL quota resets)?")
colR1, colR2, colR3 = st.columns([2,2,6])
with colR1:
    re_engine = st.selectbox("Re-translate engine", ["DeepL", "OpenAI", "None"], key="reeng")
with colR2:
    re_scope = st.selectbox("Rows", ["Filtered rows", "All rows"], key="rescope")
if st.button("🔁 Re-translate Arabic from current English"):
    idx = (filtered.index.tolist() if st.session_state["rescope"] == "Filtered rows" else work.index.tolist())
    if not idx:
        st.warning("No rows to process.")
    else:
        en_titles_now = work.loc[idx, "name"].fillna("").astype(str)
        ar_titles_new = translate_en_titles(en_titles_now, st.session_state["reeng"], batch_size)
        work.loc[idx, "name_ar"] = ar_titles_new
        st.session_state.work = work
        filtered = work[mask].copy()
        st.success("Arabic re-translation complete.")

# ---------- Image thumbnails (from column W: thumbnail) ----------
st.subheader("Image thumbnails (from column 'thumbnail')")
if "thumbnail" in work.columns:
    urls = filtered["thumbnail"].fillna("").astype(str).tolist()
    show_n = min(24, len(urls))
    cols = st.columns(6)
    for i, url in enumerate(urls[:show_n]):
        with cols[i % 6]:
            img = fetch_thumb(url)
            if img:
                st.image(img, caption=f"Row {filtered.index[i]}", use_container_width=True)
            else:
                st.write("No image")
else:
    st.info("No `thumbnail` column detected, so no thumbnails to show.")

# ---------- Tables ----------
st.markdown("### Current selection (all rows in view)")
st.dataframe(
    filtered[[
        "merchant_sku", "name", "name_ar",
        "category_id", "sub_category_id", "sub_sub_category_id"
    ]],
    use_container_width=True,
    height=900,
)

with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

with st.expander("Reset working data"):
    if st.button("🔄 Reset working data (start over)"):
        st.session_state.pop("work", None)
        st.rerun()

# ---------- Downloads ----------
st.subheader("Download")
full_xlsx = to_excel_download(work, "Products")
st.download_button(
    "⬇️ Download FULL Excel",
    data=full_xlsx,
    file_name="products_mapped.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
filtered_xlsx = to_excel_download(filtered, "Filtered")
st.download_button(
    "⬇️ Download FILTERED Excel (current view)",
    data=filtered_xlsx,
    file_name="products_filtered.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption(
    "Main category remains a NAME (no numeric main ID provided). "
    "Sub & Sub-Sub are saved as NUMBERS from your mapping. "
    "Batched titles: OpenAI vision → English (short), then Arabic via DeepL/OpenAI/None. "
    "Images are always read from column W: 'thumbnail'."
)
