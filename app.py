# Product Mapping Dashboard – pro UI + fixed unmapped filter + full pipeline + memory + audit
# - Emerald-green sidebar, clean header
# - New OVERVIEW tab for KPIs and charts
# - Unmapped filter fixed (proper NaN/blank handling) in both Filter and Sheet tabs
# - Batch: image→EN titles (via data URL) → AR translation
# - Per-file cache by file hash; resets on new upload
# - Audit log for failures; downloadable
# - Manual, scope-aware controls

import io
import re
import time
import math
import hashlib
import base64
from typing import List, Iterable, Dict, Tuple
from urllib.parse import urlsplit, urlunsplit, quote
from collections import Counter

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ============================== PAGE & THEME ===============================
st.set_page_config(page_title="Product Mapping Dashboard", page_icon="🧭", layout="wide")

EMERALD = "#10b981"  # emerald green
BG_DARK = "#0b1721"
TEXT_LIGHT = "#f3f4f6"

st.markdown(f"""
<style>
/* App header */
.app-header {{padding:10px 0 10px 0;border-bottom:1px solid #e6e6e6;}}
.app-title {{font-size:28px;font-weight:700;letter-spacing:.2px;}}
.app-subtitle {{color:#666;font-size:14px;margin-top:2px;}}

/* Sidebar styling */
[data-testid="stSidebar"] > div:first-child {{
  background: linear-gradient(180deg, {EMERALD} 0%, #059669 100%);
  color: {TEXT_LIGHT};
}}
[data-testid="stSidebar"] .css-1d391kg, [data-testid="stSidebar"] p, [data-testid="stSidebar"] label {{
  color: {TEXT_LIGHT} !important;
}}
[data-testid="stSidebar"] .stMetric label, [data-testid="stSidebar"] .stMetric span {{
  color: {TEXT_LIGHT} !important;
}}
/* Buttons */
.stButton>button {{
  border-radius:8px;
  border:1px solid #e5e7eb;
}}
/* Cards */
.card {{
  border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#fff;
}}
.small-note {{color:#777;font-size:12px;margin-top:-6px}}

/* Tabs spacing */
.block-container {{padding-top: 12px;}}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="app-header">
  <div class="app-title">🧭 Product Mapping Dashboard</div>
  <div class="app-subtitle">Images → English Title → Arabic → Categorization → Export</div>
</div>
""", unsafe_allow_html=True)

# ============================== REQUIRED COLUMNS ==========================
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    "thumbnail",
]

# ============================== API CLIENTS ===============================
# DeepL
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

with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.write("DeepL:", "✅ Active" if deepl_active else "❌ Missing/Invalid")
    st.write("OpenAI:", "✅ Active" if openai_active else "❌ Missing/Invalid")

# ============================== FILE IO ===================================
def read_any_table(uploaded_file):
    if uploaded_file is None: return None
    fn = uploaded_file.name.lower()
    if fn.endswith((".xlsx",".xls")): return pd.read_excel(uploaded_file, engine="openpyxl")
    if fn.endswith(".csv"): return pd.read_csv(uploaded_file)
    raise ValueError("Please upload .xlsx, .xls, or .csv")

def validate_columns(df, required_cols: Iterable[str], label: str) -> bool:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"{label}: missing required columns: {missing}")
        return False
    return True

def hash_uploaded_file(uploaded_file) -> str:
    try:
        uploaded_file.seek(0)
        data = uploaded_file.read()
        uploaded_file.seek(0)
        return hashlib.sha256(data).hexdigest()
    except Exception:
        return hashlib.sha256(str(uploaded_file.name).encode()).hexdigest()

# ============================== HELPERS ===================================
STOP = {"the","and","for","with","of","to","in","on","by","a","an","&","-",
        "ml","g","kg","l","oz","pcs","pc","pack","pkt","ct","size","new","extra","x"}

def tokenize(text: str) -> List[str]:
    return [t for t in re.split(r"[^A-Za-z0-9]+", str(text).lower())
            if t and len(t) > 2 and not t.isdigit() and t not in STOP]

def strip_markdown(s: str) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"[*_`]+","",s)
    s = re.sub(r"\s+"," ",s).strip()
    return s

def tidy_title(s: str, max_chars: int = 70) -> str:
    s = strip_markdown(s)
    if len(s) <= max_chars: return s
    cut = s[:max_chars].rstrip()
    if " " in cut: cut = cut[: cut.rfind(" ")]
    return cut

def is_valid_url(u: str) -> bool:
    if not isinstance(u,str): return False
    u = u.strip().strip('"').strip("'")
    try:
        p = urlsplit(u)
        return p.scheme in ("http","https") and bool(p.netloc)
    except Exception:
        return False

def _normalize_url(u: str) -> str:
    u = (u or "").strip().strip('"').strip("'")
    p = urlsplit(u)
    path = quote(p.path, safe="/:%@&?=#,+!$;'()*[]")
    if p.query:
        parts=[]
        for kv in p.query.split("&"):
            if not kv: continue
            if "=" in kv:
                k,v = kv.split("=",1); parts.append(f"{quote(k,safe=':/@')}={quote(v,safe=':/@')}")
            else:
                parts.append(quote(kv,safe=":/@"))
        query="&".join(parts)
    else:
        query=""
    return urlunsplit((p.scheme,p.netloc,path,query,p.fragment))

def fetch_thumb(url: str, timeout=10, max_bytes=8_000_000):
    try:
        if not is_valid_url(url):
            return None
        url = _normalize_url(url)
        origin = urlsplit(url).netloc
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": f"https://{origin}"
        }
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()
        data = r.content if r.content else r.raw.read(max_bytes + 1)
        if len(data) > max_bytes:
            return None
        img = Image.open(BytesIO(data)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            r.raise_for_status()
            data = r.content[:max_bytes + 1]
            if len(data) > max_bytes:
                return None
            img = Image.open(BytesIO(data)).convert("RGB")
            img.thumbnail((256, 256))
            return img
        except Exception:
            return None

def fetch_image_as_data_url(url: str, timeout=10, max_bytes=8_000_000) -> str:
    """Download image and return data URL to avoid remote hotlink issues."""
    try:
        if not is_valid_url(url):
            return ""
        url = _normalize_url(url)
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": f"https://{urlsplit(url).netloc}"
        }
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()
        data = r.content if r.content else r.raw.read(max_bytes + 1)
        if len(data) > max_bytes:
            return ""
        try:
            fmt = Image.open(BytesIO(data)).format or "JPEG"
        except Exception:
            fmt = "JPEG"
        mime = "image/jpeg" if fmt.upper() in ("JPG","JPEG") else f"image/{fmt.lower()}"
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""

# ============================== OPENAI & TRANSLATION ======================
def openai_title_from_image(url: str, max_chars: int) -> str:
    if not openai_active:
        return ""
    data_url = fetch_image_as_data_url(url)
    if not data_url:
        return ""
    prompt = (
        "Look at the product photo and return ONE short English title only. "
        "Keep it 6–8 words, ≤70 characters. Include brand if visible and "
        "size/count if obvious. Output ONLY the title."
    )
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise e-commerce title writer."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ]},
            ],
            temperature=0,
            max_tokens=96,
        )
        choice = resp.choices[0]
        content = getattr(choice.message, "content", "") if hasattr(choice, "message") else ""
        title = (content or "").strip()
        return tidy_title(title, max_chars) if title else ""
    except Exception:
        return ""

def deepl_batch_en2ar(texts: List[str]) -> List[str]:
    if not translator: return list(texts)
    try:
        res = translator.translate_text(texts, source_lang="EN", target_lang="AR")
        return [r.text for r in (res if isinstance(res, list) else [res])]
    except Exception:
        return texts

def openai_translate_batch_en2ar(texts: List[str]) -> List[str]:
    if not openai_active or not texts: return list(texts)
    sys = "Translate e-commerce product titles into natural, concise Arabic."
    usr = "Translate each of these lines to Arabic, one per line:\n\n" + "\n".join(texts)
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":sys},{"role":"user","content":usr}],
            temperature=0,
        )
        lines = (resp.choices[0].message.content or "").splitlines()
        return [l.strip() for l in lines if l.strip()] or texts
    except Exception:
        return texts

def translate_en_titles(titles_en: pd.Series, engine: str, batch_size: int) -> pd.Series:
    texts = titles_en.fillna("").astype(str).tolist()
    if engine == "DeepL" and deepl_active:
        return pd.Series(deepl_batch_en2ar(texts), index=titles_en.index)
    if engine == "OpenAI":
        out_all = []
        for s in range(0, len(texts), max(1, batch_size)):
            chunk = texts[s:s+batch_size]
            out_all.extend(openai_translate_batch_en2ar(chunk))
            time.sleep(0.15)
        return pd.Series(out_all, index=titles_en.index)
    return titles_en.copy()

# ============================== MAPPING LOOKUPS ===========================
def build_mapping_struct_fixed(map_df: pd.DataFrame):
    for c in ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"]:
        if c in map_df.columns: map_df[c] = map_df[c].astype(str).str.strip()
    main_to_sub = {str(mc): sorted(g["sub_category_id"].dropna().unique().tolist())
                   for mc,g in map_df.groupby("category_id", dropna=True)}
    pair_to_subsub = {(str(mc),str(sc)): sorted(g["sub_sub_category_id"].dropna().unique().tolist())
                      for (mc,sc),g in map_df.groupby(["category_id","sub_category_id"], dropna=True)}
    sub_no = {(r["category_id"], r["sub_category_id"]): r["sub_category_id NO"] for _,r in map_df.iterrows()}
    ssub_no = {(r["category_id"], r["sub_category_id"], r["sub_sub_category_id"]): r["sub_sub_category_id NO"] for _,r in map_df.iterrows()}
    return {"main_names": sorted(map_df["category_id"].dropna().unique().tolist()),
            "main_to_subnames": main_to_sub,
            "pair_to_subsubnames": pair_to_subsub,
            "sub_name_to_no_by_main": sub_no,
            "ssub_name_to_no_by_main_sub": ssub_no}

def get_sub_no(lookups, main, sub): return lookups["sub_name_to_no_by_main"].get((main, sub), "")
def get_ssub_no(lookups, main, sub, ssub): return lookups["ssub_name_to_no_by_main_sub"].get((main, sub, ssub), "")

# ============================== DOWNLOAD ==================================
def to_excel_download(df, sheet_name="Products"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf

# ============================== UPLOAD ====================================
c1, c2 = st.columns(2)
with c1: product_file = st.file_uploader("Product List (.xlsx/.csv, includes 'thumbnail')", type=["xlsx","xls","csv"])
with c2: mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])

prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

if not (prod_df is not None and validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List")
        and map_df is not None and validate_columns(map_df, ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"], "Category Mapping")):
    st.stop()

# ------------- Per-file memory & audit -----------
st.session_state.setdefault("file_hash", None)
st.session_state.setdefault("proc_cache", {})   # {sku: {"name": EN, "name_ar": AR}}
st.session_state.setdefault("audit_rows", [])   # list of dicts for failures or skips
current_hash = hash_uploaded_file(product_file)

if st.session_state.file_hash != current_hash:
    st.session_state.work = prod_df.copy()
    st.session_state.proc_cache = {}
    st.session_state.audit_rows = []
    st.session_state.file_hash = current_hash

work = st.session_state.work
lookups = build_mapping_struct_fixed(map_df)

# ============================== UTIL: mapped/unmapped masks ===============
def is_nonempty_series(s: pd.Series) -> pd.Series:
    return s.notna() & s.astype(str).str.strip().ne("")

def mapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    return is_nonempty_series(df["sub_category_id"].fillna("")) & is_nonempty_series(df["sub_sub_category_id"].fillna(""))

def unmapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    # unmapped if either sub or sub-sub is empty or NaN
    sub_ok  = is_nonempty_series(df["sub_category_id"].fillna(""))
    ssub_ok = is_nonempty_series(df["sub_sub_category_id"].fillna(""))
    return ~(sub_ok & ssub_ok)

# ============================== OVERVIEW TAB FIRST ========================
tab_overview, tab_filter, tab_titles, tab_group, tab_sheet, tab_dl, tab_settings = st.tabs(
    ["📊 Overview", "🔎 Filter", "🖼️ Titles & Translate", "🧩 Grouping", "📑 Sheet", "⬇️ Downloads", "⚙️ Settings"]
)

def mapping_stats(df: pd.DataFrame):
    mapped_mask = mapped_mask_fn(df)
    total=len(df); mapped=int(mapped_mask.sum()); unmapped=total-mapped
    pct=0 if total==0 else round(mapped*100/total,1)
    named=int(is_nonempty_series(df["name"].fillna("")).sum())
    titled_pct=0 if total==0 else round(named*100/total,1)
    en_missing = int((~is_nonempty_series(df["name"].fillna(""))).sum())
    ar_missing = int((~is_nonempty_series(df["name_ar"].fillna(""))).sum())
    return total,mapped,unmapped,pct,named,titled_pct,en_missing,ar_missing,mapped_mask

with tab_overview:
    st.subheader("Overview")
    total,mapped,unmapped,pct,named,titled_pct,en_missing,ar_missing,global_mmask = mapping_stats(work)
    k1,k2,k3,k4 = st.columns(4)
    with k1: st.metric("Total rows", total)
    with k2: st.metric("Mapped rows", mapped, f"{pct}%")
    with k3: st.metric("EN titled", named, f"{titled_pct}%")
    with k4: st.metric("Unmapped rows", unmapped)

    cA,cB = st.columns(2)
    with cA:
        st.markdown("**Mapped vs Unmapped**")
        st.bar_chart(pd.DataFrame({"count":[mapped, unmapped]}, index=["Mapped","Unmapped"]))
    with cB:
        st.markdown("**Missing coverage**")
        st.bar_chart(pd.DataFrame({"count":[en_missing, ar_missing]}, index=["Missing EN","Missing AR"]))

    # Top tokens among unmapped
    st.markdown("**Top tokens in Unmapped**")
    unm_df = work[~global_mmask].copy()
    counts = Counter()
    for _, r in unm_df.iterrows():
        counts.update(tokenize(r.get("name","")))
        counts.update(tokenize(r.get("name_ar","")))
    top = pd.DataFrame(counts.most_common(12), columns=["token","count"])
    if len(top)>0:
        st.dataframe(top, use_container_width=True, height=280)
    else:
        st.caption("No tokens found.")

# ------------------------------ FILTER TAB --------------------------------
with tab_filter:
    st.subheader("Filter view")

    q = st.text_input("Search", value=st.session_state.get("search_q",""), placeholder="e.g., dishwashing / صابون / SKU123")
    st.session_state["search_q"] = q

    with st.expander("Advanced search", expanded=False):
        cB,cC,cD = st.columns([2,2,2])
        with cB:
            fields = st.multiselect("Fields", ["name","name_ar","merchant_sku","thumbnail"], default=["name","name_ar"])
        with cC:
            mode = st.selectbox("Match mode", ["OR","AND"])
        with cD:
            whole_word = st.checkbox("Whole word", value=False)
        st.caption('<div class="small-note">Tip: multiple terms split by space</div>', unsafe_allow_html=True)

    cE, cF, cG = st.columns([2,2,2])
    with cE:
        unmapped_only = st.checkbox("Show Unmapped Only", value=st.session_state.get("show_unmapped", False))
        st.session_state["show_unmapped"] = unmapped_only
    with cF:
        if st.button("Clear filters"):
            st.session_state["search_q"] = ""
            st.session_state["show_unmapped"] = False
            q = ""
            unmapped_only = False
    with cG:
        st.write("")

    def build_filter_mask(df: pd.DataFrame, query: str, fields: List[str], mode: str, whole_word: bool):
        if not query.strip():
            base = pd.Series(True, index=df.index)
        else:
            terms = [t for t in re.split(r"\s+", query.strip()) if t]
            parts = []
            for t in terms:
                if whole_word:
                    pat = rf"(?:^|\b){re.escape(t)}(?:\b|$)"
                    term_mask = pd.Series(False, index=df.index)
                    for f in fields:
                        if f in df.columns:
                            term_mask |= df[f].astype(str).str.contains(pat, case=False, regex=True, na=False)
                else:
                    term_mask = pd.Series(False, index=df.index)
                    for f in fields:
                        if f in df.columns:
                            term_mask |= df[f].astype(str).str.contains(t, case=False, na=False)
                parts.append(term_mask)
            base = parts[0] if parts else pd.Series(True, index=df.index)
            for p in parts[1:]:
                base = (base & p) if mode == "AND" else (base | p)
        if st.session_state["show_unmapped"]:
            base = base & unmapped_mask_fn(df)
        return base

    fields = locals().get("fields", ["name","name_ar"])
    mode = locals().get("mode", "OR")
    whole_word = locals().get("whole_word", False)

    mask = build_filter_mask(work, q, fields, mode, whole_word)
    filtered = work[mask].copy()

    st.caption(f"Rows in current filtered view: {filtered.shape[0]}")
    st.dataframe(filtered[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
                 use_container_width=True, height=360)

# --------------------------- TITLES & TRANSLATE ---------------------------
with tab_titles:
    st.subheader("Titles from images, then Arabic")

    c1,c2,c3,c4 = st.columns([2,2,2,2])
    with c1: max_len = st.slider("Max English title length", 50, 90, 70, 5)
    with c2: engine  = st.selectbox("Arabic translation engine", ["DeepL","OpenAI","None"])
    with c3: only_empty = st.checkbox("Run only on empty EN titles", value=True)
    with c4: force_refresh = st.checkbox("Force overwrite", value=False)

    scope = st.radio("Scope", ["All rows", "Current filtered view"], horizontal=True)
    base_scope = work if scope=="All rows" else filtered

    c5,c6 = st.columns(2)
    with c5: fetch_batch = st.number_input("Batch size (image→EN title)", min_value=10, max_value=300, value=100, step=10)
    with c6: trans_batch = st.number_input("Batch size (EN→AR)", min_value=10, max_value=300, value=150, step=10)

    # Coverage
    if st.button("Scan coverage in scope"):
        sc = base_scope.copy()
        en_missing = int((~is_nonempty_series(sc["name"].fillna(""))).sum())
        ar_missing = int((~is_nonempty_series(sc["name_ar"].fillna(""))).sum())
        st.info(f"Scope rows: {len(sc)} | Missing EN: {en_missing} | Missing AR: {ar_missing}")

    # Preview
    if st.button("Preview first 24 images in scope"):
        view=base_scope.head(24).copy()
        if "thumbnail" in view.columns and len(view)>0:
            cols=st.columns(6)
            for j,(i,row) in enumerate(view.iterrows()):
                with cols[j%6]:
                    url=_normalize_url(str(row.get("thumbnail","")))
                    if is_valid_url(url): st.image(url, caption=f"Row {i}", use_container_width=True)
                    else: st.write("No image / bad URL")
        else:
            st.info("No thumbnails in scope.")

    def indices_for_scope(df: pd.DataFrame) -> List[int]:
        return df.index.tolist()

    # Workers
    def run_titles_on_indices(idx_list: List[int]):
        updated_en = 0; skipped_en = 0; failed_en = 0
        prog = st.progress(0.0, text="Generating English titles from images…")
        for s in range(0, len(idx_list), fetch_batch):
            chunk = idx_list[s:s+fetch_batch]
            for i in chunk:
                sku = str(work.at[i,"merchant_sku"])
                cached = st.session_state.proc_cache.get(sku, {})
                current_en = str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else ""
                current_en = current_en.strip()
                url = str(work.at[i,"thumbnail"]) if "thumbnail" in work.columns else ""

                if not force_refresh and cached.get("name"):
                    work.at[i,"name"] = cached["name"]
                    skipped_en += 1
                    continue
                if only_empty and current_en and not force_refresh:
                    skipped_en += 1
                    st.session_state.proc_cache.setdefault(sku, {})["name"] = current_en
                    continue

                title = openai_title_from_image(url, max_len) if url else ""
                if title:
                    work.at[i,"name"] = title
                    st.session_state.proc_cache.setdefault(sku, {})["name"] = title
                    updated_en += 1
                else:
                    failed_en += 1
                    st.session_state.audit_rows.append({"sku": sku, "phase": "EN title", "reason": "no title or fetch failed", "url": url})
            prog.progress(min((s+len(chunk))/len(idx_list), 1.0))
            time.sleep(0.02)
        st.success(f"EN titles → updated: {updated_en}, skipped: {skipped_en}, failed: {failed_en}")

    def run_translations_on_indices(idx_list: List[int]):
        if engine not in ("DeepL","OpenAI"):
            st.info("Translation engine set to None. Skipped EN→AR.")
            return
        to_translate_idx, to_translate_texts = [], []
        for i in idx_list:
            sku = str(work.at[i,"merchant_sku"])
            cached = st.session_state.proc_cache.get(sku, {})
            current_ar = str(work.at[i,"name_ar"]) if pd.notna(work.at[i,"name_ar"]) else ""
            current_ar = current_ar.strip()
            en = str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else ""
            en = en.strip()

            if not en:
                st.session_state.audit_rows.append({"sku": sku, "phase": "AR translate", "reason": "missing EN", "url": str(work.at[i,"thumbnail"])})
                continue
            if not force_refresh and cached.get("name_ar"):
                work.at[i,"name_ar"] = cached["name_ar"]
                continue
            if force_refresh or not current_ar:
                to_translate_idx.append(i)
                to_translate_texts.append(en)

        updated_ar = 0; failed_ar = 0
        if to_translate_texts:
            prog2 = st.progress(0.0, text="Translating EN → AR…")
            out_all = []
            for s in range(0, len(to_translate_texts), trans_batch):
                chunk = to_translate_texts[s:s+trans_batch]
                trans = translate_en_titles(pd.Series(chunk), engine, batch_size=trans_batch)
                out_all.extend(trans.tolist())
                prog2.progress(min((s+len(chunk))/len(to_translate_texts), 1.0))
                time.sleep(0.02)
            for j, i in enumerate(to_translate_idx):
                ar = out_all[j] if j < len(out_all) else ""
                if ar:
                    work.at[i,"name_ar"] = ar
                    sku = str(work.at[i,"merchant_sku"])
                    st.session_state.proc_cache.setdefault(sku, {})["name_ar"] = ar
                    updated_ar += 1
                else:
                    failed_ar += 1
                    st.session_state.audit_rows.append({"sku": str(work.at[i,"merchant_sku"]), "phase": "AR translate", "reason": "model returned empty", "url": str(work.at[i,"thumbnail"])})
        st.success(f"AR translations → updated: {updated_ar}, failed: {failed_ar}")

    # Buttons
    if st.button("Run FULL pipeline on scope (Image→EN, then EN→AR)"):
        ids = indices_for_scope(base_scope)
        if not ids: st.info("No rows in scope.")
        else:
            run_titles_on_indices(ids)
            run_translations_on_indices(ids)

    colm1, colm2 = st.columns(2)
    with colm1:
        if st.button("Run ONLY for MISSING EN in scope"):
            ids = indices_for_scope(base_scope[~is_nonempty_series(base_scope["name"].fillna(""))])
            if not ids: st.info("Nothing missing for EN in scope.")
            else: run_titles_on_indices(ids)
    with colm2:
        if st.button("Run ONLY for MISSING AR in scope"):
            ids = indices_for_scope(base_scope[~is_nonempty_series(base_scope["name_ar"].fillna(""))])
            if not ids: st.info("Nothing missing for AR in scope.")
            else: run_translations_on_indices(ids)

    if st.session_state.audit_rows:
        audit_df = pd.DataFrame(st.session_state.audit_rows)
        st.download_button("⬇️ Download audit log CSV", data=audit_df.to_csv(index=False).encode("utf-8"),
                           file_name="audit_log.csv", mime="text/csv")

# ------------------------------- GROUPING ---------------------------------
with tab_group:
    st.subheader("Grouping via keywords")
    st.session_state.setdefault("keyword_library", [])
    left,right = st.columns([1,2])

    with left:
        st.markdown("**Keyword Library**")
        new_kws_text = st.text_area("Add keywords (one per line)", placeholder="soap\nshampoo\ndishwashing\nlemon")
        if st.button("➕ Add to library"):
            fresh=[k.strip() for k in new_kws_text.splitlines() if k.strip()]
            if fresh:
                existing=set(st.session_state.keyword_library)
                st.session_state.keyword_library.extend([k for k in fresh if k not in existing])
                st.session_state.keyword_library = list(dict.fromkeys(st.session_state.keyword_library))
                st.success(f"Added {len(fresh)} keyword(s).")
            else:
                st.info("Nothing to add.")
        to_remove = st.multiselect("Remove from library", options=st.session_state.keyword_library, key="lib_remove")
        if st.button("🗑️ Remove selected"):
            if to_remove:
                st.session_state.keyword_library=[k for k in st.session_state.keyword_library if k not in set(to_remove)]
                st.success(f"Removed {len(to_remove)} keyword(s).")
            else:
                st.info("No selection.")

    with right:
        st.markdown("**Select keywords/tokens to group and map**")
        scope_filtered_only = st.checkbox("Scope = CURRENT filtered view", value=True)
        base_df = (work[unmapped_mask_fn(work)] if scope_filtered_only else work)
        # Auto tokens
        tok_counts = Counter()
        for _, r in base_df.iterrows():
            tok_counts.update(tokenize(r.get("name","")))
            tok_counts.update(tokenize(r.get("name_ar","")))
        auto_candidates = [t for t,c in tok_counts.most_common() if c>=3][:60]

        def hit_count(df: pd.DataFrame, term: str) -> int:
            term_l = term.lower()
            m = df["name"].astype(str).str.lower().str.contains(term_l, na=False) | \
                df["name_ar"].astype(str).str.lower().str.contains(term_l, na=False)
            return int(m.sum())

        options_display = []
        display_to_key: Dict[str, Tuple[str,str]] = {}
        for kw in st.session_state.keyword_library:
            cnt = hit_count(base_df, kw)
            disp = f"{kw} ({cnt}) [Saved]"
            options_display.append(disp); display_to_key[disp] = ("lib", kw)
        for tok in auto_candidates:
            cnt = hit_count(base_df, tok)
            disp = f"{tok} ({cnt}) [Auto]"
            options_display.append(disp); display_to_key[disp] = ("auto", tok)

        picked = st.multiselect("Pick one or more keywords/tokens", options=options_display, default=[])

        if picked:
            union_mask = pd.Series(False, index=base_df.index)
            for disp in picked:
                term = display_to_key[disp][1].lower()
                union_mask |= base_df["name"].astype(str).str.lower().str.contains(term, na=False)
                union_mask |= base_df["name_ar"].astype(str).str.lower().str.contains(term, na=False)
            hits_df = base_df[union_mask].copy()
            st.write(f"Total matches across selected: {hits_df.shape[0]}")
            if hits_df.shape[0] > 0:
                st.dataframe(hits_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]],
                             use_container_width=True, height=260)
                default_skus = hits_df["merchant_sku"].astype(str).tolist()
                chosen_skus = st.multiselect("Select SKUs to MAP", options=default_skus, default=default_skus, key="grp_apply_skus")

                gm1,gm2,gm3 = st.columns(3)
                g_main = gm1.selectbox("Main", [""]+lookups["main_names"], key="grp_main")
                g_sub  = gm2.selectbox("Sub", [""]+lookups["main_to_subnames"].get(g_main,[]), key="grp_sub")
                g_ssub = gm3.selectbox("Sub-Sub", [""]+lookups["pair_to_subsubnames"].get((g_main,g_sub),[]), key="grp_ssub")

                if st.button("Apply mapping to selected SKUs"):
                    if not chosen_skus: st.info("No SKUs selected.")
                    elif not (g_main and g_sub and g_ssub): st.warning("Pick Main, Sub, and Sub-Sub.")
                    else:
                        apply_mask = work["merchant_sku"].astype(str).isin(chosen_skus)
                        work.loc[apply_mask,"category_id"]=g_main
                        work.loc[apply_mask,"sub_category_id"]=get_sub_no(lookups,g_main,g_sub)
                        work.loc[apply_mask,"sub_sub_category_id"]=get_ssub_no(lookups,g_main,g_sub,g_ssub)
                        st.success(f"Applied mapping to {apply_mask.sum()} rows.")
        else:
            st.info("Pick at least one keyword/token to see matches.")

# ------------------------------- SHEET ------------------------------------
with tab_sheet:
    st.subheader("Full sheet preview")

    view_mode = st.radio("Quick filter", ["All","Mapped only","Unmapped only"], horizontal=True)
    base_df = work.copy()
    mapped_mask_v = mapped_mask_fn(base_df)
    if view_mode == "Mapped only":
        base_df = base_df[mapped_mask_v]
    elif view_mode == "Unmapped only":
        base_df = base_df[~mapped_mask_v]

    # Pagination
    st.session_state.setdefault("page_size", 200)
    st.session_state.setdefault("page_num", 1)
    st.session_state.page_size = st.number_input("Rows per page", min_value=50, max_value=5000, value=st.session_state.page_size, step=50)
    total_rows = base_df.shape[0]
    total_pages = max(1, math.ceil(total_rows / st.session_state.page_size))
    st.session_state.page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=min(st.session_state.page_num, total_pages), step=1)
    start = (st.session_state.page_num - 1) * st.session_state.page_size
    end = start + st.session_state.page_size
    page_df = base_df.iloc[start:end].copy()
    st.caption(f"Showing rows {start+1}–{min(end,total_rows)} of {total_rows}")

    def style_map(row):
        sub_ok  = str(row.get("sub_category_id","") or "").strip() != ""
        ssub_ok = str(row.get("sub_sub_category_id","") or "").strip() != ""
        is_mapped = sub_ok and ssub_ok
        color = "background-color: rgba(16,185,129,0.10)" if is_mapped else "background-color: rgba(234,179,8,0.18)"
        return [color for _ in row]

    term = st.session_state.get("search_q","").strip().lower()
    def cell_highlight(v):
        if not term: return ""
        try:
            if term in str(v).lower(): return "background-color: rgba(66,133,244,0.15)"
        except Exception:
            pass
        return ""

    if len(page_df) > 0:
        styler = page_df.style.apply(style_map, axis=1).applymap(cell_highlight, subset=["name","name_ar"])
        st.dataframe(styler, use_container_width=True, height=440)
    else:
        st.info("No rows to display.")

# ------------------------------- DOWNLOADS --------------------------------
with tab_dl:
    st.subheader("Download")
    st.download_button("⬇️ Download FULL Excel", to_excel_download(work), file_name="products_mapped.xlsx")
    st.download_button("⬇️ Download CURRENT VIEW Excel", to_excel_download(page_df), file_name="products_view.xlsx")
    if st.session_state.audit_rows:
        audit_df = pd.DataFrame(st.session_state.audit_rows)
        st.download_button("⬇️ Download audit log CSV", data=audit_df.to_csv(index=False).encode("utf-8"),
                           file_name="audit_log.csv", mime="text/csv")

# ------------------------------- SETTINGS ---------------------------------
with tab_settings:
    st.subheader("Diagnostics")
    c1,c2 = st.columns(2)
    with c1:
        if st.button("Show 10 sample normalized thumbnail URLs"):
            sample = work["thumbnail"].astype(str).head(10).tolist() if "thumbnail" in work.columns else []
            for u in sample:
                norm=_normalize_url(u); st.write({"raw":u,"normalized":norm,"valid":is_valid_url(norm)})
    with c2:
        if st.button("Clear cache for THIS file"):
            st.session_state.proc_cache = {}
            st.session_state.audit_rows = []
            st.success("Cleared per-file cache.")
