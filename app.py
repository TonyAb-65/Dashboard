import io
import re
import time
import math
import base64
from typing import List, Tuple, Dict, Any
from urllib.parse import urlparse, quote

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# =========================
# Page
# =========================
st.set_page_config(
    page_title="Product Mapping + (Manual) AI Titles",
    layout="wide",
)

# =========================
# Expected Product List columns
# =========================
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # image URL column W — we ALWAYS read 'thumbnail' when you ask us to
]

# =========================
# API clients
# =========================
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
openai_status_note = ""
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")
    if OPENAI_API_KEY:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        openai_active = True
    else:
        openai_status_note = "(OpenAI API key not set in secrets)"
except Exception as e:
    openai_client = None
    openai_active = False
    openai_status_note = f"(OpenAI unavailable: {e})"

# =========================
# File IO helpers
# =========================
def _read_any_table_bytes(name: str, data: bytes):
    name = name.lower()
    bio = io.BytesIO(data)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(bio, engine="openpyxl")
    if name.endswith(".csv"):
        return pd.read_csv(bio)
    raise ValueError("Please upload .xlsx, .xls, or .csv")

def validate_columns(df, required_cols, label):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"{label}: missing required columns: {missing}")
        return False
    return True

# =========================
# Text helpers
# =========================
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

# =========================
# Image / URL helpers
# =========================
def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def normalize_url(u: str) -> str:
    if not isinstance(u, str):
        return ""
    s = u.strip().strip('"\'')

    if not s:
        return ""

    if s.startswith("//"):
        s = "https:" + s
    if s.startswith("www.") and not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    if "://" not in s and "." in s.split("/")[0]:
        s = "https://" + s

    return s

def _default_headers(url: str) -> Dict[str, str]:
    p = urlparse(url)
    origin = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""
    return {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125 Safari/537.36"),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": origin,
    }

def proxyize(url: str) -> str:
    try:
        s = url.strip()
        if s.startswith("http://"):
            host_path = s[len("http://"):]
        elif s.startswith("https://"):
            host_path = s[len("https://"):]
        else:
            host_path = s
        return f"https://images.weserv.nl/?url={quote(host_path, safe='')}&w=1280"
    except Exception:
        return url

def fetch_image_bytes(url: str, timeout=20) -> Tuple[bytes, str]:
    if not is_valid_url(url):
        raise ValueError("Invalid URL")
    r = requests.get(url, timeout=timeout, headers=_default_headers(url), allow_redirects=True)
    r.raise_for_status()
    content = r.content
    if not content:
        raise ValueError("Empty content")
    mime = r.headers.get("Content-Type", "").split(";")[0].strip().lower()
    if not mime or "text/html" in mime:
        try:
            fmt = Image.open(BytesIO(content)).format or "JPEG"
            mime = f"image/{fmt.lower()}"
        except Exception:
            mime = "image/jpeg"
    return content, mime

def fetch_thumb(url: str, timeout=8):
    # Only used when user clicks "Show thumbnails"
    try:
        content, _ = fetch_image_bytes(url, timeout=timeout)
        img = Image.open(BytesIO(content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        try:
            purl = proxyize(url)
            content, _ = fetch_image_bytes(purl, timeout=timeout)
            img = Image.open(BytesIO(content)).convert("RGB")
            img.thumbnail((256, 256))
            return img
        except Exception:
            return None

def compress_to_jpeg_bytes(content: bytes, max_side: int = 1280, quality: int = 85) -> bytes:
    img = Image.open(BytesIO(content))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    w, h = img.size
    if max(w, h) > max_side:
        scale = max_side / float(max(w, h))
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()

def to_data_url_jpeg(content: bytes) -> str:
    b64 = base64.b64encode(content).decode("utf-8")
    return f"data:image/jpeg;base64,{b64}"

# =========================
# OpenAI vision (Responses → Chat fallback)
# =========================
VISION_PROMPT = (
    "You are an e-commerce title generator. Return ONE short product TITLE only, "
    "6–8 words, max ~70 chars. Include brand if visible and size/count if obvious. "
    "No markdown, no emojis, no extra text."
)

def _responses_image_title(image_url: str) -> str:
    try:
        resp = openai_client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": VISION_PROMPT},
                    {"type": "input_image", "image_url": image_url},
                ]
            }],
        )
        return (resp.output_text or "").strip()
    except Exception:
        return ""

def _chat_image_title(image_url: str) -> str:
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_PROMPT},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ]
            }],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return ""

def openai_title_from_data_url(data_url: str, max_chars: int, stats: Dict[str, int]) -> str:
    txt = _responses_image_title(data_url)
    if txt:
        stats["responses_ok"] += 1
        return tidy_title(txt, max_chars)
    txt = _chat_image_title(data_url)
    if txt:
        stats["chat_fallback_ok"] += 1
        return tidy_title(txt, max_chars)
    return ""

def openai_title_from_url(url: str, max_chars: int, stats: Dict[str, int]) -> str:
    txt = _responses_image_title(url)
    if txt:
        stats["responses_ok"] += 1
        return tidy_title(txt, max_chars)
    txt = _chat_image_title(url)
    if txt:
        stats["chat_fallback_ok"] += 1
        return tidy_title(txt, max_chars)
    return ""

# =========================
# Translation
# =========================
def _deepl_batch(texts: List[str]) -> List[str]:
    if not translator:
        return list(texts)
    if not texts:
        return []
    MAX_ITEMS = 45
    MAX_CHARS = 28000
    out = [""] * len(texts)
    idx_texts = [(i, (t if isinstance(t, str) else "")) for i, t in enumerate(texts)]
    idx_texts = [(i, t) for i, t in idx_texts if t.strip()]
    start = 0
    while start < len(idx_texts):
        chars = 0
        batch = []
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
                resp = openai_client.responses.create(
                    model="gpt-4o-mini",
                    input=[{"role": "system", "content": [{"type":"input_text","text": sys}]},
                           {"role": "user", "content": [{"type":"input_text","text": usr}]}],
                )
                txt = (resp.output_text or "").strip()
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
    return titles_en.copy()

# =========================
# Mapping structures
# =========================
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

# =========================
# Excel download
# =========================
def to_excel_download(df, sheet_name="Products"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf

# =========================
# Title generation (manual only)
# =========================
def titles_from_images_batched(
    df: pd.DataFrame,
    row_index: List[int],
    max_chars: int,
    batch_size: int,
) -> Tuple[pd.Series, Dict[str, int]]:
    if "thumbnail" not in df.columns:
        st.error("Column 'thumbnail' not found (expected in column W).")
        return pd.Series([""] * len(row_index), index=row_index, dtype="object"), {
            "ai":0,"fallback":0,"total":0,"download_ok":0,"proxy_fetch_ok":0,
            "openai_calls":0,"openai_errors":0,"openai_via_url":0,"openai_via_proxy_url":0,
            "responses_ok":0,"chat_fallback_ok":0
        }

    titles_en = pd.Series([""] * len(row_index), index=row_index, dtype="object")
    stats = {"ai": 0, "fallback": 0, "total": len(row_index),
             "download_ok": 0, "proxy_fetch_ok": 0,
             "openai_calls": 0, "openai_errors": 0,
             "openai_via_url": 0, "openai_via_proxy_url": 0,
             "responses_ok": 0, "chat_fallback_ok": 0}
    prog = st.progress(0)
    steps = max(1, math.ceil(len(row_index) / max(1, batch_size)))

    for step, start in enumerate(range(0, len(row_index), batch_size), start=1):
        chunk_idx = row_index[start : start + batch_size]
        for i in chunk_idx:
            raw_url = str(df.loc[i, "thumbnail"])
            url = normalize_url(raw_url)
            title = ""
            data_url = ""
            proxy_url = proxyize(url) if url else ""

            # Server download → JPEG → data URL
            if url:
                try:
                    raw, _ = fetch_image_bytes(url, timeout=20)
                    if raw:
                        jpg = compress_to_jpeg_bytes(raw, max_side=1280, quality=85)
                        data_url = to_data_url_jpeg(jpg)
                        stats["download_ok"] += 1
                except Exception:
                    try:
                        if proxy_url:
                            raw, _ = fetch_image_bytes(proxy_url, timeout=20)
                            if raw:
                                jpg = compress_to_jpeg_bytes(raw, max_side=1280, quality=85)
                                data_url = to_data_url_jpeg(jpg)
                                stats["proxy_fetch_ok"] += 1
                    except Exception:
                        data_url = ""

            # Use data URL with OpenAI
            if data_url and openai_active:
                for attempt in range(2):
                    t = openai_title_from_data_url(data_url, max_chars, stats)
                    stats["openai_calls"] += 1
                    if t:
                        title = t
                        break
                    time.sleep(0.6 * (attempt + 1))

            # Let OpenAI fetch the original URL
            if not title and url and openai_active and is_valid_url(url):
                for attempt in range(2):
                    t = openai_title_from_url(url, max_chars, stats)
                    stats["openai_calls"] += 1
                    if t:
                        title = t
                        stats["openai_via_url"] += 1
                        break
                    time.sleep(0.6 * (attempt + 1))

            # Let OpenAI fetch the PROXY URL
            if not title and proxy_url and openai_active and is_valid_url(proxy_url):
                for attempt in range(2):
                    t = openai_title_from_url(proxy_url, max_chars, stats)
                    stats["openai_calls"] += 1
                    if t:
                        title = t
                        stats["openai_via_proxy_url"] += 1
                        break
                    time.sleep(0.6 * (attempt + 1))

            # Final fallback
            if title:
                stats["ai"] += 1
            else:
                seed = df.loc[i, "name"]
                title = template_title_from_name(str(seed))
                stats["fallback"] += 1
                stats["openai_errors"] += 1

            titles_en.loc[i] = title

        prog.progress(min(step / steps, 1.0))
        time.sleep(0.2)

    return titles_en, stats

# =========================
# Session-state persistence for uploads & working data
# =========================
def cache_upload(file, key_bytes: str, key_name: str, key_df: str):
    """
    Read and cache uploaded file bytes + name + parsed df into session_state,
    so reruns do not lose them.
    """
    if file is not None:
        data = file.read()
        st.session_state[key_bytes] = data
        st.session_state[key_name] = file.name
        st.session_state[key_df] = _read_any_table_bytes(file.name, data)

def get_cached_df(key_bytes: str, key_name: str, key_df: str):
    """
    Return cached DataFrame if present, else None.
    """
    if key_df in st.session_state and st.session_state[key_df] is not None:
        return st.session_state[key_df]
    return None

def clear_cached_upload(key_bytes: str, key_name: str, key_df: str):
    for k in [key_bytes, key_name, key_df]:
        if k in st.session_state:
            st.session_state.pop(k, None)

# =========================
# UI – Header / Status
# =========================
st.title("🛒 Product Mapping + Manual AI Title Generation")

pre1, pre2, pre3 = st.columns(3)
with pre1:
    st.metric("OpenAI Vision", "Active" if openai_active else "Inactive")
    if openai_status_note:
        st.caption(openai_status_note)
with pre2:
    st.metric("DeepL", "Active" if deepl_active else "Inactive")
    if deepl_status_note:
        st.caption(deepl_status_note)
with pre3:
    st.caption("Nothing auto-runs. You choose when to fetch images or generate titles.")

st.markdown("""
**Flow**  
1) Upload **Product List** and **Category Mapping** (they stay cached).  
2) (Optional) **Show thumbnails (preview)** — only if you click it.  
3) (Optional) **Generate AI titles** — only if you enable and click it.  
4) Assign **Sub / Sub‑Sub** categories and **Download**.
""")

# =========================
# Uploads (cached)
# =========================
c1, c2, c3 = st.columns(3)
with c1:
    product_file = st.file_uploader("Product List (.xlsx/.csv) — contains 'thumbnail' in column W", type=["xlsx", "xls", "csv"], key="prod")
    if product_file:
        cache_upload(product_file, "prod_bytes", "prod_name", "prod_df")

with c2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx", "xls", "csv"], key="map")
    if mapping_file:
        cache_upload(mapping_file, "map_bytes", "map_name", "map_df")

with c3:
    st.caption("Use the buttons below to replace or clear uploads if needed.")
    if st.button("🔁 Clear Product upload"):
        clear_cached_upload("prod_bytes", "prod_name", "prod_df")
    if st.button("🔁 Clear Mapping upload"):
        clear_cached_upload("map_bytes", "map_name", "map_df")

# Get cached DFs
prod_df = get_cached_df("prod_bytes", "prod_name", "prod_df")
map_df  = get_cached_df("map_bytes", "map_name", "map_df")

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
    st.info("Upload and cache both files with the required headers to continue.")
    st.stop()

# Working DF persistence (never reset unless user clears uploads)
if "work" not in st.session_state:
    st.session_state.work = prod_df.copy()
work = st.session_state.work
work.columns = [str(c).strip() for c in work.columns]

# Ensure required cols exist & string-typed
for col in REQUIRED_PRODUCT_COLS:
    if col not in work.columns:
        work[col] = ""
    else:
        work[col] = work[col].fillna("").astype(str)

# Build mapping lookups
lookups = build_mapping_struct_fixed(map_df)

# =========================
# Search + Bulk Apply
# =========================
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

# =========================
# Manual thumbnails (only if you click)
# =========================
with st.expander("🖼️ Show thumbnails (preview) — manual"):
    if "thumbnail" not in work.columns:
        st.info("No `thumbnail` column found.")
    else:
        non_empty = (work["thumbnail"].astype(str).str.strip() != "").sum()
        st.caption(f"Non-empty thumbnail URLs: {int(non_empty)}")
        if st.button("Render thumbnails (first 24)"):
            urls = [str(u).strip().strip('"\'') for u in filtered["thumbnail"].fillna("").astype(str).tolist()]
            show_n = min(24, len(urls))
            cols = st.columns(6)
            for i, url in enumerate(urls[:show_n]):
                with cols[i % 6]:
                    img = fetch_thumb(url)
                    if img:
                        st.image(img, caption=f"Row {filtered.index[i]}", use_container_width=True)
                    else:
                        purl = proxyize(url)
                        st.image(purl, caption=f"(proxy) Row {filtered.index[i]}", use_container_width=True)

# =========================
# Manual AI title generation (only if you enable + click)
# =========================
st.subheader("Manual: Image → EN short title; EN → AR via DeepL / OpenAI / None")

enable_ai = st.checkbox("Enable AI title generation (manual)", value=False)

colA, colB, colC, colD = st.columns(4)
with colA:
    max_len = st.slider("Max title length", 50, 90, 70, 5, disabled=not enable_ai)
with colB:
    batch_size = st.slider("Batch size", 20, 100, 50, 5, disabled=not enable_ai)
with colC:
    engine = st.selectbox("Arabic translation engine", ["DeepL", "OpenAI", "None"], disabled=not enable_ai)
with colD:
    scope = st.selectbox("Scope", ["Filtered rows", "All rows"], disabled=not enable_ai)

if enable_ai and st.button("🧪 Test first 5 rows (manual)"):
    idx_list = (filtered.index.tolist() if scope == "Filtered rows" else work.index.tolist())[:5]
    if not idx_list:
        st.warning("No rows to process.")
    else:
        titles_en, stats = titles_from_images_batched(work, idx_list, max_len, batch_size=5)
        st.write("Stats:", stats)
        st.dataframe(pd.DataFrame({
            "merchant_sku": work.loc[idx_list, "merchant_sku"],
            "thumbnail": work.loc[idx_list, "thumbnail"],
            "generated_en": titles_en.loc[idx_list],
        }), use_container_width=True)

if enable_ai and st.button("🖼️ Generate short titles (manual)"):
    idx_list = filtered.index.tolist() if scope == "Filtered rows" else work.index.tolist()
    if not idx_list:
        st.warning("No rows to process.")
    else:
        st.info(f"Processing {len(idx_list)} rows in batches of {batch_size}…")
        titles_en, stats = titles_from_images_batched(work, idx_list, max_len, batch_size)
        work.loc[idx_list, "name"] = titles_en.loc[idx_list]

        # Arabic
        if engine == "DeepL" and not deepl_active:
            st.warning("DeepL not available (or quota exceeded). Arabic will mirror English.")
            titles_ar = titles_en.loc[idx_list]
        elif engine == "OpenAI" and not openai_active:
            st.warning("OpenAI not available. Arabic will mirror English.")
            titles_ar = titles_en.loc[idx_list]
        else:
            st.info(f"Translating English → Arabic via {engine}…")
            titles_ar = translate_en_titles(titles_en.loc[idx_list], engine, batch_size)

        work.loc[idx_list, "name_ar"] = titles_ar
        st.session_state.work = work
        filtered = work[mask].copy()

        st.success(
            f"Done. AI titles: {stats['ai']} | fallbacks: {stats['fallback']} "
            f"| downloads OK: {stats['download_ok']} | proxy fetch OK: {stats['proxy_fetch_ok']} "
            f"| OpenAI calls: {stats['openai_calls']} | via URL: {stats['openai_via_url']} | via PROXY URL: {stats['openai_via_proxy_url']} "
            f"| responses_ok: {stats['responses_ok']} | chat_ok: {stats['chat_fallback_ok']} | errors: {stats['openai_errors']}"
        )
        st.dataframe(work.loc[idx_list, ["merchant_sku", "thumbnail", "name"]].head(12), use_container_width=True)

# =========================
# Tables
# =========================
st.markdown("### Current selection (all rows in view)")
st.dataframe(
    filtered[["merchant_sku", "name", "name_ar", "category_id", "sub_category_id", "sub_sub_category_id"]],
    use_container_width=True,
    height=900,
)

with st.expander("🔎 Product List (first rows)"):
    st.dataframe(work.head(30), use_container_width=True)
with st.expander("🗂️ Category Mapping (first rows)"):
    st.dataframe(map_df.head(30), use_container_width=True)

with st.expander("Reset working data (keeps uploads cached)"):
    if st.button("🔄 Reset working data to original upload"):
        st.session_state.work = st.session_state["prod_df"].copy() if "prod_df" in st.session_state else prod_df.copy()
        st.success("Working data reset.")

# =========================
# Downloads
# =========================
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
