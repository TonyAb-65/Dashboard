# Product Mapping Dashboard — Master (UI intact, manual trigger, batched pipeline, persistent cache, clearer job panel)

import io, re, time, math, hashlib, base64, json
from typing import List, Iterable, Dict, Tuple
from urllib.parse import urlsplit, urlunsplit, quote
from collections import Counter

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ================= PAGE =================
st.set_page_config(page_title="Product Mapping Dashboard", page_icon="🧭", layout="wide")

# ===== UI THEME & HEADER =====
EMERALD = "#10b981"
EMERALD_DARK = "#059669"
TEXT_LIGHT = "#f8fafc"

st.markdown(f"""
<style>
/* Sticky header */
.app-header {{
  padding: 8px 0 8px 0;
  border-bottom: 1px solid #e5e7eb;
  background: #ffffff;
  position: sticky; top: 0; z-index: 5;
}}
.app-title {{ font-size: 22px; font-weight: 800; color:#111827; }}
.app-sub {{ color:#6b7280; font-size:12px; }}

/* Sidebar emerald theme */
[data-testid="stSidebar"] > div:first-child {{
  background: linear-gradient(180deg, {EMERALD} 0%, {EMERALD_DARK} 100%);
  color: {TEXT_LIGHT};
}}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {{ color:{TEXT_LIGHT} !important; }}
[data-testid="stSidebar"] .stRadio > div > label {{
  margin-bottom: 6px; padding: 6px 10px; border-radius: 8px;
  background: rgba(255,255,255,0.08);
}}

/* Buttons and cards */
.stButton>button {{ border-radius:8px; border:1px solid #e5e7eb; padding:.45rem .9rem; }}
.card {{ border:1px solid #e5e7eb; border-radius:12px; padding:14px; background:#fff; }}
.small-note {{ color:#6b7280; font-size:12px; margin-top:-6px; }}
.block-container {{ padding-top: 6px; }}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="app-header">
  <div class="app-title">🧭 Product Mapping Dashboard</div>
  <div class="app-sub">Images → English Title → Arabic → Categorization → Export</div>
</div>
""", unsafe_allow_html=True)

# ============== REQUIRED COLUMNS ==============
REQUIRED_PRODUCT_COLS = [
    "name","name_ar","merchant_sku","category_id","category_id_ar",
    "sub_category_id","sub_sub_category_id","thumbnail",
]

# ============== API CLIENTS ==============
translator=None; deepl_active=False
try:
    import deepl
    DEEPL_API_KEY = st.secrets.get("DEEPL_API_KEY")
    if DEEPL_API_KEY:
        translator = deepl.Translator(DEEPL_API_KEY); deepl_active=True
except Exception:
    translator=None; deepl_active=False

openai_client=None; openai_active=False
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")
    if OPENAI_API_KEY:
        openai_client = OpenAI(api_key=OPENAI_API_KEY); openai_active=True
except Exception:
    openai_client=None; openai_active=False

# -------- Persistent cache across reruns (per server process) --------
@st.cache_resource
def global_cache() -> dict:
    # {file_hash: {sku: {"en": "...", "ar": "..."}}}
    return {}

# ============== FILE IO ==============
def read_any_table(uploaded_file):
    if uploaded_file is None: return None
    fn = uploaded_file.name.lower()
    if fn.endswith((".xlsx",".xls")): return pd.read_excel(uploaded_file, engine="openpyxl")
    if fn.endswith(".csv"): return pd.read_csv(uploaded_file)
    raise ValueError("Please upload .xlsx, .xls, or .csv")

def validate_columns(df, required_cols: Iterable[str], label: str) -> bool:
    missing=[c for c in required_cols if c not in df.columns]
    if missing: st.error(f"{label}: missing columns: {missing}"); return False
    return True

def hash_uploaded_file(uploaded_file) -> str:
    try:
        uploaded_file.seek(0); data=uploaded_file.read(); uploaded_file.seek(0)
        return hashlib.sha256(data).hexdigest()
    except Exception:
        return hashlib.sha256(str(uploaded_file.name).encode()).hexdigest()

# ============== HELPERS ==============
STOP={"the","and","for","with","of","to","in","on","by","a","an","&","-",
      "ml","g","kg","l","oz","pcs","pc","pack","pkt","ct","size","new","extra","x"}

def tokenize(t:str)->List[str]:
    return [x for x in re.split(r"[^A-Za-z0-9]+", str(t).lower())
            if x and len(x)>2 and not x.isdigit() and x not in STOP]

def strip_markdown(s:str)->str:
    if not isinstance(s,str): return ""
    s=re.sub(r"[*_`]+","",s); s=re.sub(r"\s+"," ",s).strip(); return s

def tidy_title(s:str,max_chars:int=70)->str:
    s=strip_markdown(s)
    if len(s)<=max_chars: return s
    cut=s[:max_chars].rstrip()
    if " " in cut: cut=cut[:cut.rfind(" ")]
    return cut

def is_valid_url(u:str)->bool:
    if not isinstance(u,str): return False
    u=u.strip().strip('"').strip("'")
    try:
        p=urlsplit(u); return p.scheme in ("http","https") and bool(p.netloc)
    except Exception: return False

def _normalize_url(u:str)->str:
    u=(u or "").strip().strip('"').strip("'")
    p=urlsplit(u); path=quote(p.path, safe="/:%@&?=#,+!$;'()*[]")
    if p.query:
        parts=[]
        for kv in p.query.split("&"):
            if not kv: continue
            if "=" in kv:
                k,v=kv.split("=",1); parts.append(f"{quote(k,safe=':/@')}={quote(v,safe=':/@')}")
            else: parts.append(quote(kv,safe=':/@'))
        q="&".join(parts)
    else: q=""
    return urlunsplit((p.scheme,p.netloc,path,q,p.fragment))

# ---------- stronger fetch with streaming cap ----------
def fetch_image_as_data_url(url:str, timeout=10, max_bytes=8_000_000)->str:
    """Manual fetch → data URL. Resilient stream with size cap."""
    try:
        if not is_valid_url(url): return ""
        url=_normalize_url(url)
        headers={"User-Agent":"Mozilla/5.0",
                 "Accept":"image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                 "Referer":f"https://{urlsplit(url).netloc}"}
        with requests.get(url,timeout=timeout,headers=headers,allow_redirects=True,stream=True) as r:
            r.raise_for_status()
            data=bytearray()
            for chunk in r.iter_content(chunk_size=65536):
                if not chunk: break
                data.extend(chunk)
                if len(data)>max_bytes: return ""
        try:
            fmt=Image.open(BytesIO(bytes(data))).format or "JPEG"
        except Exception:
            fmt="JPEG"
        mime="image/jpeg" if fmt.upper() in ("JPG","JPEG") else f"image/{fmt.lower()}"
        b64=base64.b64encode(bytes(data)).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""

# ---------- tiny retry wrapper for OpenAI ----------
def _retry(fn, attempts=4, base=0.5):
    for i in range(attempts):
        try:
            return fn()
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(base * (2 ** i))

def _openai_chat(messages, **kwargs):
    return _retry(lambda: openai_client.chat.completions.create(
        model="gpt-4o-mini", messages=messages, **kwargs))

# ===== Structured extraction for titles =====
STRUCT_PROMPT_JSON = (
    "You read ONLY the product label in the image and extract fields for a title.\n"
    "Return EXACTLY ONE LINE of STRICT JSON with keys:"
    '{"brand":string|null,"product":string,"variant":string|null,'
    '"flavor_scent":string|null,"material":string|null,"size_value":string|null,'
    '"size_unit":string|null,"count":string|null,"feature":string|null}\n'
    "Rules:\n"
    "- If brand not visible, set brand=null.\n"
    "- product must be a generic noun if unclear (e.g., 'glass teapot').\n"
    "- size_value numeric only; size_unit in ['ml','L','g','kg','pcs','tabs','caps'].\n"
    "- count is pack count if shown.\n"
    "- feature is a short key attribute on the label if present (e.g., 'heat-resistant').\n"
    "- Output JSON only."
)

def assemble_title_from_fields(d: dict) -> str:
    brand = (d.get("brand") or "").strip()
    product = (d.get("product") or "").strip()
    variant = (d.get("variant") or "").strip()
    flavor  = (d.get("flavor_scent") or "").strip()
    material= (d.get("material") or "").strip()
    feature = (d.get("feature") or "").strip()
    size_v  = (d.get("size_value") or "").strip()
    size_u  = (d.get("size_unit") or "").strip().lower()
    count   = (d.get("count") or "").strip()

    parts=[]
    if brand:  parts.append(brand)
    if product: parts.append(product)

    qual = variant or flavor or material or feature
    if qual: parts.append(qual)

    unit=size_u
    if unit in ["milliliter","mls","ml."]: unit="ml"
    if unit in ["liter","litre","ltrs","ltr"]: unit="L"
    if unit in ["grams","gram","gr"]: unit="g"
    if unit in ["kilogram","kilo","kgs"]: unit="kg"

    size_str=""
    if size_v and unit: size_str=f"{size_v}{unit}"
    if count and not size_str: size_str=f"{count}pcs"
    elif count and size_str: size_str=f"{size_str} {count}pcs"
    if size_str: parts.append(size_str)

    return tidy_title(" ".join(p for p in parts if p), 70)

# ---------- stricter fallback title writer ----------
def _fallback_simple_title(data_url: str, max_chars: int) -> str:
    if not openai_active or not data_url: return ""
    prompt = (
        "Write ONE clean English e-commerce title ≤70 chars. "
        "Order: Brand, Product, Variant/Flavor/Scent, Material, Size/Count. "
        "Omit unknowns. No marketing words. One line."
    )
    try:
        resp=_openai_chat(
            [{"role":"system","content":"You are a precise e-commerce title writer."},
             {"role":"user","content":[
                 {"type":"text","text":prompt},
                 {"type":"image_url","image_url":{"url":data_url}}
             ]}],
            temperature=0, max_tokens=64
        )
        txt=(resp.choices[0].message.content or "").strip()
        return tidy_title(txt,max_chars) if txt else ""
    except Exception:
        return ""

def openai_title_from_image(url:str,max_chars:int)->str:
    """Vision JSON extraction → assembled title; robust fallback to simple title."""
    if not openai_active: return ""
    data_url=fetch_image_as_data_url(url)
    if not data_url: return ""
    try:
        resp=_openai_chat(
            [{"role":"system","content":"Extract concise, accurate product fields from the image."},
             {"role":"user","content":[
                 {"type":"text","text":STRUCT_PROMPT_JSON},
                 {"type":"image_url","image_url":{"url":data_url}}
             ]}],
            temperature=0.1, max_tokens=220
        )
        raw=(resp.choices[0].message.content or "").strip()
        m=re.search(r"\{.*\}", raw, re.S)
        if not m: return _fallback_simple_title(data_url, max_chars)
        try:
            data=json.loads(m.group(0))
        except Exception:
            return _fallback_simple_title(data_url, max_chars)

        # ---- HARD REQUIRE product noun (guard against '1 liter tea bag' types) ----
        product = (data.get("product") or "").strip().lower()
        if not product or product in {"ml","l","g","kg","pcs","tabs","caps"}:
            return _fallback_simple_title(data_url, max_chars)

        title=assemble_title_from_fields(data)
        if title and len(title)>=3:
            return tidy_title(title,max_chars)
        return _fallback_simple_title(data_url, max_chars)
    except Exception:
        return _fallback_simple_title(data_url, max_chars)

# ============== Translation ==============
def deepl_batch_en2ar(texts:List[str])->List[str]:
    if not translator: return list(texts)
    try:
        res=translator.translate_text(texts, source_lang="EN", target_lang="AR")
        return [r.text for r in (res if isinstance(res,list) else [res])]
    except Exception:
        return texts

def openai_translate_batch_en2ar(texts:List[str])->List[str]:
    if not openai_active or not texts: return list(texts)
    sys="Translate e-commerce product titles into natural, concise Arabic."
    usr="Translate each of these lines to Arabic, one per line:\n\n" + "\n".join(texts)
    try:
        resp=_openai_chat(
            [{"role":"system","content":sys},{"role":"user","content":usr}],
            temperature=0
        )
        lines=(resp.choices[0].message.content or "").splitlines()
        return [l.strip() for l in lines if l.strip()] or texts
    except Exception:
        return texts

def translate_en_titles(titles_en: pd.Series, engine:str, batch_size:int)->pd.Series:
    texts=titles_en.fillna("").astype(str).tolist()
    if engine=="DeepL" and deepl_active: return pd.Series(deepl_batch_en2ar(texts), index=titles_en.index)
    if engine=="OpenAI":
        out=[]
        for s in range(0,len(texts),max(1,batch_size)):
            out.extend(openai_translate_batch_en2ar(texts[s:s+batch_size])); time.sleep(0.1)
        return pd.Series(out, index=titles_en.index)
    return titles_en.copy()

# ============== Mapping lookups ==============
def build_mapping_struct_fixed(map_df: pd.DataFrame):
    for c in ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"]:
        if c in map_df.columns: map_df[c]=map_df[c].astype(str).str.strip()
    main_to_sub={str(mc): sorted(g["sub_category_id"].dropna().unique().tolist()) for mc,g in map_df.groupby("category_id",dropna=True)}
    pair_to_subsub={(str(mc),str(sc)): sorted(g["sub_sub_category_id"].dropna().unique().tolist()) for (mc,sc),g in map_df.groupby(["category_id","sub_category_id"],dropna=True)}
    sub_no={(r["category_id"],r["sub_category_id"]): r["sub_category_id NO"] for _,r in map_df.iterrows()}
    ssub_no={(r["category_id"],r["sub_category_id"],r["sub_sub_category_id"]): r["sub_sub_category_id NO"] for _,r in map_df.iterrows()}
    return {"main_names": sorted(map_df["category_id"].dropna().unique().tolist()),
            "main_to_subnames": main_to_sub,
            "pair_to_subsubnames": pair_to_subsub,
            "sub_name_to_no_by_main": sub_no,
            "ssub_name_to_no_by_main_sub": ssub_no}
def get_sub_no(lookups, main, sub): return lookups["sub_name_to_no_by_main"].get((main,sub),"")
def get_ssub_no(lookups, main, sub, ssub): return lookups["ssub_name_to_no_by_main_sub"].get((main,sub,ssub),"")

# ============== Downloads ==============
def to_excel_download(df, sheet_name="Products"):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w: df.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0); return buf

# ============== Uploads ==============
c1,c2=st.columns(2)
with c1: product_file = st.file_uploader("Product List (.xlsx/.csv, includes 'thumbnail')", type=["xlsx","xls","csv"])
with c2: mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])
prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None
if not (prod_df is not None and validate_columns(prod_df,REQUIRED_PRODUCT_COLS,"Product List")
        and map_df is not None and validate_columns(map_df,["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"],"Category Mapping")):
    st.stop()

# ============== Memory & State ==============
st.session_state.setdefault("file_hash", None)
st.session_state.setdefault("proc_cache", {})
st.session_state.setdefault("audit_rows", [])
st.session_state.setdefault("keyword_library", [])
st.session_state.setdefault("page_size", 200)
st.session_state.setdefault("page_num", 1)
st.session_state.setdefault("search_q","")

current_hash = hash_uploaded_file(product_file)
if st.session_state.file_hash != current_hash:
    st.session_state.work = prod_df.copy()
    st.session_state.proc_cache = {}
    st.session_state.audit_rows = []
    st.session_state.file_hash = current_hash

work = st.session_state.work
lookups = build_mapping_struct_fixed(map_df)

# Prefill from persistent cache if this file was seen before
_g = global_cache()
file_store = _g.get(current_hash, {})
if file_store:
    for i, row in work.iterrows():
        sku = str(row["merchant_sku"])
        entry = file_store.get(sku)
        if entry:
            if entry.get("en"): work.at[i, "name"] = entry["en"]
            if entry.get("ar"): work.at[i, "name_ar"] = entry["ar"]

# ============== Sidebar NAV ==============
with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.write("DeepL:", "✅ Active" if deepl_active else "❌ Missing/Invalid")
    st.write("OpenAI:", "✅ Active" if openai_active else "❌ Missing/Invalid")
    st.markdown("---")
    section = st.radio(
        "Navigate",
        ["📊 Overview","🔎 Filter","🖼️ Titles & Translate","🧩 Grouping","📑 Sheet","⬇️ Downloads","⚙️ Settings"],
        index=0
    )

# ============== Shared utils ==============
def is_nonempty_series(s: pd.Series) -> pd.Series:
    return s.notna() & s.astype(str).str.strip().ne("")

def mapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    return is_nonempty_series(df["sub_category_id"].fillna("")) & is_nonempty_series(df["sub_sub_category_id"].fillna(""))

def unmapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    sub_ok  = is_nonempty_series(df["sub_category_id"].fillna(""))
    ssub_ok = is_nonempty_series(df["sub_sub_category_id"].fillna(""))
    return ~(sub_ok & ssub_ok)

def mapping_stats(df: pd.DataFrame):
    mm=mapped_mask_fn(df); total=len(df); mapped=int(mm.sum()); unmapped=total-mapped
    en_ok=int(is_nonempty_series(df["name"].fillna("")).sum()); ar_ok=int(is_nonempty_series(df["name_ar"].fillna("")).sum())
    return total,mapped,unmapped,en_ok,ar_ok,mm

# ============== Sections ==============
def sec_overview():
    st.subheader("Overview")
    total,mapped,unmapped,en_ok,ar_ok,mm = mapping_stats(work)
    k1,k2,k3,k4=st.columns(4)
    with k1: st.metric("Total", total)
    with k2: st.metric("Mapped", mapped, f"{round(mapped*100/total,1) if total else 0}%")
    with k3: st.metric("EN titled", en_ok, f"-{total-en_ok} missing")
    with k4: st.metric("AR titled", ar_ok, f"-{total-ar_ok} missing")
    cA,cB=st.columns(2)
    with cA:
        st.markdown("**Mapped vs Unmapped**")
        st.bar_chart(pd.DataFrame({"count":[mapped,unmapped]}, index=["Mapped","Unmapped"]))
    with cB:
        st.markdown("**Missing coverage**")
        st.bar_chart(pd.DataFrame({"count":[total-en_ok, total-ar_ok]}, index=["Missing EN","Missing AR"]))
    st.markdown("**Top tokens in Unmapped**")
    unm=work[~mm].copy(); counts=Counter()
    for _,r in unm.iterrows():
        counts.update(tokenize(r.get("name",""))); counts.update(tokenize(r.get("name_ar","")))
    top=pd.DataFrame(counts.most_common(12), columns=["token","count"])
    st.dataframe(top, use_container_width=True, height=260) if len(top)>0 else st.caption("No tokens.")

def sec_filter():
    st.subheader("Filter")
    q=st.text_input("Search", value=st.session_state["search_q"], placeholder="e.g., dishwashing / صابون / SKU123")
    st.session_state["search_q"]=q
    with st.expander("Advanced", expanded=False):
        f1,f2,f3=st.columns(3)
        with f1: fields=st.multiselect("Fields", ["name","name_ar","merchant_sku","thumbnail"], default=["name","name_ar"])
        with f2: mode=st.selectbox("Mode", ["OR","AND"])
        with f3: whole=st.checkbox("Whole word", value=False)
    c1,c2=st.columns(2)
    with c1: show_unmapped=st.checkbox("Unmapped only", value=False)
    with c2:
        if st.button("Clear"): st.session_state["search_q"]=""; show_unmapped=False
    def mask(df):
        if not q.strip(): base=pd.Series(True,index=df.index)
        else:
            terms=[t for t in re.split(r"\s+", q.strip()) if t]; parts=[]
            for t in terms:
                if whole:
                    pat=rf"(?:^|\\b){re.escape(t)}(?:\\b|$)"; m=pd.Series(False,index=df.index)
                    for f in fields:
                        if f in df.columns: m|=df[f].astype(str).str.contains(pat, case=False, regex=True, na=False)
                else:
                    m=pd.Series(False,index=df.index)
                    for f in fields:
                        if f in df.columns:
                            m|=df[f].astype(str).str.contains(t, case=False, na=False)
                parts.append(m)
            base=parts[0] if parts else pd.Series(True,index=df.index)
            for p in parts[1:]: base=(base&p) if mode=="AND" else (base|p)
        if show_unmapped: base=base & unmapped_mask_fn(df)
        return base
    filtered=work[mask(work)].copy()
    st.caption(f"{filtered.shape[0]} rows")
    st.dataframe(filtered[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
                 use_container_width=True, height=380)

def sec_titles():
    st.subheader("Titles & Translate")

    # Controls
    c1,c2,c3,c4=st.columns(4)
    with c1: max_len=st.slider("Max EN length",50,90,70,5)
    with c2: engine=st.selectbox("Arabic engine", ["DeepL","OpenAI","None"])
    with c3: only_empty=st.checkbox("Only empty EN", value=True)
    with c4: force_over=st.checkbox("Force overwrite", value=False)

    scope=st.radio("Scope", ["All","Unmapped only","Missing EN","Missing AR"], horizontal=True)
    if scope=="All": base=work
    elif scope=="Unmapped only": base=work[unmapped_mask_fn(work)]
    elif scope=="Missing EN": base=work[~is_nonempty_series(work["name"].fillna(""))]
    else: base=work[~is_nonempty_series(work["name_ar"].fillna(""))]

    b1,b2=st.columns(2)
    with b1: fetch_batch=st.number_input("Batch (image→EN)",10,300,100,10)
    with b2: trans_batch=st.number_input("Batch (EN→AR)",10,300,150,10)

    # Preview 24 images (no processing)
    if st.button("Preview 24 images (no processing)", key="btn_preview_imgs"):
        gallery = st.container()
        view = base.head(24)
        if "thumbnail" in view.columns and len(view) > 0:
            cols = gallery.columns(6)
            for j, (i, row) in enumerate(view.iterrows()):
                url = _normalize_url(str(row.get("thumbnail", "")))
                with cols[j % 6]:
                    if is_valid_url(url):
                        st.image(url, caption=f"Row {i}", use_container_width=True)
                    else:
                        st.caption("Bad URL")
        else:
            st.info("No thumbnails found in current scope.")

    # ---------- Batched workers with persistent cache ----------
    MAX_CACHE_PER_FILE = 20000
    def _trim_store(store: dict):
        if len(store) <= MAX_CACHE_PER_FILE: return
        for k in list(store.keys())[: len(store)//2]:
            store.pop(k, None)

    def run_titles(idx, fetch_batch, max_len, only_empty, force_over) -> Tuple[int,int,int]:
        updated=skipped=failed=0
        store = global_cache().setdefault(st.session_state.file_hash, {})
        for s in range(0, len(idx), fetch_batch):
            chunk = idx[s:s+fetch_batch]
            for i in chunk:
                sku = str(work.at[i,"merchant_sku"])
                cache_local = st.session_state.proc_cache.get(sku, {})
                cur_en = (str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else "").strip()
                url = str(work.at[i,"thumbnail"]) if "thumbnail" in work.columns else ""

                if not force_over and store.get(sku, {}).get("en"):
                    work.at[i,"name"] = store[sku]["en"]
                    st.session_state.proc_cache.setdefault(sku,{})["name"] = store[sku]["en"]
                    skipped += 1
                    continue

                if not force_over and cache_local.get("name"):
                    work.at[i,"name"] = cache_local["name"]; skipped += 1; continue
                if only_empty and cur_en and not force_over:
                    st.session_state.proc_cache.setdefault(sku,{})["name"]=cur_en; skipped += 1; continue

                title = openai_title_from_image(url, max_len) if url else ""
                if title:
                    work.at[i,"name"] = title
                    st.session_state.proc_cache.setdefault(sku,{})["name"] = title
                    store.setdefault(sku, {})["en"] = title
                    _trim_store(store)
                    updated += 1
                else:
                    failed += 1
                    st.session_state.audit_rows.append({"sku":sku,"phase":"EN title","reason":"no title or fetch failed","url":url})
        return updated, skipped, failed

    def run_trans(idx, trans_batch, engine, force_over) -> Tuple[int,int]:
        if engine not in ("DeepL","OpenAI"): return 0,0
        store = global_cache().setdefault(st.session_state.file_hash, {})
        ids=[]; texts=[]
        for i in idx:
            sku=str(work.at[i,"merchant_sku"])
            cache_local = st.session_state.proc_cache.get(sku,{})
            cur_ar=(str(work.at[i,"name_ar"]) if pd.notna(work.at[i,"name_ar"]) else "").strip()
            en=(str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else "").strip()

            if not en:
                st.session_state.audit_rows.append({"sku":sku,"phase":"AR translate","reason":"missing EN","url":str(work.at[i,"thumbnail"])})
                continue

            if not force_over and store.get(sku, {}).get("ar"):
                work.at[i,"name_ar"] = store[sku]["ar"]
                st.session_state.proc_cache.setdefault(sku,{})["name_ar"] = store[sku]["ar"]
                continue

            if not force_over and cache_local.get("name_ar"):
                work.at[i,"name_ar"]=cache_local["name_ar"]; continue
            if force_over or not cur_ar:
                ids.append(i); texts.append(en)

        updated=failed=0
        for s in range(0, len(texts), trans_batch):
            chunk = texts[s:s+trans_batch]
            outs = translate_en_titles(pd.Series(chunk), engine, trans_batch).tolist()
            for j, _ in enumerate(chunk):
                i = ids[s+j]
                ar = outs[j] if j < len(outs) else ""
                if ar:
                    work.at[i,"name_ar"] = ar
                    sku = str(work.at[i,"merchant_sku"])
                    st.session_state.proc_cache.setdefault(sku,{})["name_ar"] = ar
                    store.setdefault(sku, {})["ar"] = ar
                    _trim_store(store)
                    updated += 1
                else:
                    failed += 1
                    st.session_state.audit_rows.append({"sku":str(work.at[i,"merchant_sku"]),"phase":"AR translate","reason":"empty output","url":str(work.at[i,"thumbnail"])})
        return updated, failed

    # === FULL AUTOMATIC BATCHED PIPELINE (manual trigger only) ===
    if st.button("Run FULL pipeline on ENTIRE scope (auto-batched)", key="btn_full_pipeline"):
        idx_all = base.index.tolist()
        if not idx_all:
            st.info("No rows in scope.")
        else:
            st.info(
                f"Scope: {scope} • Batch(image→EN)={fetch_batch} • Batch(EN→AR)={trans_batch} • "
                f"Only empty EN={'Yes' if only_empty else 'No'} • Force overwrite={'Yes' if force_over else 'No'}"
            )

            total = len(idx_all)
            bar = st.progress(0.0, text="Starting…")
            en_up=en_skip=en_fail=ar_up=ar_fail=0

            st.caption("Job panel")
            jp_cols = st.columns(5)
            c_total, c_done, c_en, c_ar, c_batch = jp_cols
            c_total.metric("Rows in scope", total)
            done_placeholder = c_done.empty()
            en_placeholder   = c_en.empty()
            ar_placeholder   = c_ar.empty()
            batch_placeholder= c_batch.empty()

            def update_panel(done, en_up, en_skip, en_fail, ar_up, ar_fail, batch_no, total_batches):
                done_placeholder.metric("Rows processed", done)
                en_placeholder.metric("EN titles", f"✔ {en_up}", f"skip {en_skip} / fail {en_fail}")
                ar_placeholder.metric("AR translated", f"✔ {ar_up}", f"fail {ar_fail}")
                batch_placeholder.metric("Batch", f"{batch_no}/{total_batches}")

            total_batches = math.ceil(total / fetch_batch)
            batch_no = 0
            for s in range(0, total, fetch_batch):
                batch_no += 1
                batch_idx = idx_all[s:s+fetch_batch]

                u,k,f = run_titles(batch_idx, fetch_batch, max_len, only_empty, force_over)
                en_up += u; en_skip += k; en_fail += f

                u2,f2 = run_trans(batch_idx, trans_batch, engine, force_over)
                ar_up += u2; ar_fail += f2

                done_count = s + len(batch_idx)
                bar.progress(min(done_count/total,1.0),
                             text=f"Processed {done_count}/{total} rows")
                update_panel(done_count, en_up, en_skip, en_fail, ar_up, ar_fail, batch_no, total_batches)
                time.sleep(0.15)

            st.success(f"Done. EN updated {en_up}, skipped {en_skip}, failed {en_fail} | "
                       f"AR updated {ar_up}, failed {ar_fail}")

    # Optional targeted runners
    cA,cB=st.columns(2)
    with cA:
        if st.button("Run ONLY missing EN"):
            ids=base[~is_nonempty_series(base["name"].fillna(""))].index.tolist()
            if ids:
                u,k,f = run_titles(ids, fetch_batch, max_len, only_empty, force_over)
                st.success(f"EN → updated {u}, skipped {k}, failed {f}")
            else:
                st.info("No missing EN.")
    with cB:
        if st.button("Run ONLY missing AR"):
            ids=base[~is_nonempty_series(base["name_ar"].fillna(""))].index.tolist()
            if ids:
                u2,f2 = run_trans(ids, trans_batch, engine, force_over)
                st.success(f"AR → updated {u2}, failed {f2}")
            else:
                st.info("No missing AR.")

    if st.session_state.audit_rows:
        audit_df=pd.DataFrame(st.session_state.audit_rows)
        st.download_button("⬇️ Audit log (CSV)", data=audit_df.to_csv(index=False).encode("utf-8"),
                           file_name="audit_log.csv", mime="text/csv")

def sec_grouping():
    st.subheader("Grouping")
    left,right=st.columns([1,2])
    with left:
        st.markdown("**Keyword Library**")
        new_kws=st.text_area("Add keywords (one per line)", placeholder="soap\nshampoo\ndishwashing\nlemon")
        if st.button("➕ Add"):
            fresh=[k.strip() for k in new_kws.splitlines() if k.strip()]
            if fresh:
                exist=set(st.session_state.keyword_library)
                st.session_state.keyword_library.extend([k for k in fresh if k not in exist])
                st.session_state.keyword_library=list(dict.fromkeys(st.session_state.keyword_library))
                st.success(f"Added {len(fresh)}")
            else: st.info("Nothing to add.")
        rm=st.multiselect("Remove", options=st.session_state.keyword_library)
        if st.button("🗑️ Remove selected"):
            s=set(rm); st.session_state.keyword_library=[k for k in st.session_state.keyword_library if k not in s]
            st.success(f"Removed {len(s)}")
    with right:
        st.markdown("**Select keywords/tokens to map**")
        base=work[unmapped_mask_fn(work)]
        tok=Counter()
        for _,r in base.iterrows():
            tok.update(tokenize(r.get("name",""))); tok.update(tokenize(r.get("name_ar","")))
        auto=[t for t,c in tok.most_common() if c>=3][:60]
        def hits(df,term):
            tl=term.lower()
            m=df["name"].astype(str).str.lower().str.contains(tl,na=False) | df["name_ar"].astype(str).str.lower().str.contains(tl,na=False)
            return int(m.sum())
        opts=[]; keymap={}
        for kw in st.session_state.keyword_library:
            disp=f"{kw} ({hits(base,kw)}) [Saved]"; opts.append(disp); keymap[disp]=kw
        for t in auto:
            disp=f"{t} ({hits(base,t)}) [Auto]"; opts.append(disp); keymap[disp]=t
        picked=st.multiselect("Pick", options=opts)
        if picked:
            mask=pd.Series(False,index=base.index)
            for d in picked:
                term=keymap[d].lower()
                mask|=base["name"].astype(str).str.lower().str.contains(term,na=False)
                mask|=base["name_ar"].astype(str).str.lower().str.contains(term,na=False)
            hits_df=base[mask].copy()
            st.write(f"Matches: {hits_df.shape[0]}")
            st.dataframe(hits_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]],
                         use_container_width=True, height=260)
            skus=hits_df["merchant_sku"].astype(str).tolist()
            chosen=st.multiselect("Select SKUs", options=skus, default=skus)
            c1,c2,c3=st.columns(3)
            g_main=c1.selectbox("Main", [""]+lookups["main_names"])
            g_sub =c2.selectbox("Sub", [""]+lookups["main_to_subnames"].get(g_main,[]))
            g_ssub=c3.selectbox("Sub-Sub", [""]+lookups["pair_to_subsubnames"].get((g_main,g_sub),[]))
            if st.button("Apply mapping"):
                if not chosen: st.info("No SKUs.")
                elif not (g_main and g_sub and g_ssub): st.warning("Pick all levels.")
                else:
                    m=work["merchant_sku"].astype(str).isin(chosen)
                    work.loc[m,"category_id"]=g_main
                    work.loc[m,"sub_category_id"]=get_sub_no(lookups,g_main,g_sub)
                    work.loc[m,"sub_sub_category_id"]=get_ssub_no(lookups,g_main,g_sub,g_ssub)
                    st.success(f"Applied to {int(m.sum())} rows.")
        else:
            st.info("Pick at least one keyword/token.")

def sec_sheet():
    st.subheader("Sheet")
    view=st.radio("Quick filter", ["All","Mapped only","Unmapped only"], horizontal=True)
    base=work.copy(); mm=mapped_mask_fn(base)
    if view=="Mapped only": base=base[mm]
    elif view=="Unmapped only": base=base[~mm]
    st.session_state.page_size=st.number_input("Rows/page",50,5000,st.session_state.page_size,50)
    total=base.shape[0]; pages=max(1, math.ceil(total/st.session_state.page_size))
    st.session_state.page_num=st.number_input("Page",1,pages,min(st.session_state.page_num,pages),1)
    st.caption(f"{total} rows total")
    start=(st.session_state.page_num-1)*st.session_state.page_size; end=start+st.session_state.page_size
    page=base.iloc[start:end].copy()
    def style_map(row):
        sub_ok=str(row.get("sub_category_id","") or "").strip()!=""
        ssub_ok=str(row.get("sub_sub_category_id","") or "").strip()!=""
        return [("background-color: rgba(16,185,129,0.10)" if (sub_ok and ssub_ok) else "background-color: rgba(234,179,8,0.18)") for _ in row]
    term=st.session_state.get("search_q","").strip().lower()
    def hi(v):
        if not term: return ""
        try:
            if term in str(v).lower(): return "background-color: rgba(59,130,246,0.15)"
        except Exception: pass
        return ""
    if len(page)>0:
        styler=page.style.apply(style_map, axis=1).applymap(hi, subset=["name","name_ar"])
        st.dataframe(styler, use_container_width=True, height=440)
    else: st.info("No rows.")
    return page

def sec_downloads(page_df):
    st.subheader("Downloads")
    st.download_button("⬇️ Full Excel", to_excel_download(work), file_name="products_mapped.xlsx")
    st.download_button("⬇️ Current view Excel", to_excel_download(page_df), file_name="products_view.xlsx")
    if st.session_state.audit_rows:
        audit_df=pd.DataFrame(st.session_state.audit_rows)
        st.download_button("⬇️ Audit log (CSV)", data=audit_df.to_csv(index=False).encode("utf-8"),
                           file_name="audit_log.csv", mime="text/csv")

def sec_settings():
    st.subheader("Settings & Diagnostics")
    c1,c2=st.columns(2)
    with c1:
        if st.button("Show 10 normalized thumbnail URLs"):
            sample=work["thumbnail"].astype(str).head(10).tolist() if "thumbnail" in work.columns else []
            for u in sample:
                norm=_normalize_url(u); st.write({"raw":u,"normalized":norm,"valid":is_valid_url(norm)})
    with c2:
        if st.button("Clear per-file cache & audit"):
            st.session_state.proc_cache={}; st.session_state.audit_rows=[]
            store = global_cache()
            if st.session_state.file_hash in store:
                del store[st.session_state.file_hash]
            st.success("Cleared.")

# ============== Router ==============
if section=="📊 Overview":
    sec_overview()
elif section=="🔎 Filter":
    sec_filter()
elif section=="🖼️ Titles & Translate":
    sec_titles()
elif section=="🧩 Grouping":
    sec_grouping()
elif section=="📑 Sheet":
    page_df=sec_sheet()
elif section=="⬇️ Downloads":
    try: page_df
    except NameError: page_df=work.copy()
    sec_downloads(page_df)
else:
    sec_settings()
