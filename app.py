# Product Mapping Dashboard — master (image-dataURL ready)
# Uses `thumbnail_dataurl` when present so OpenAI Vision always sees images.

import io, re, time, math, hashlib, json, sys, traceback, base64, random
from typing import List, Iterable, Tuple, Optional, Dict
from urllib.parse import urlsplit, urlunsplit, quote
from collections import Counter

import pandas as pd
import streamlit as st
import requests

# ================= PAGE =================
st.set_page_config(page_title="Product Mapping Dashboard", page_icon="🧭", layout="wide")
st.set_option("client.showErrorDetails", True)

# ===== UI THEME & HEADER =====
EMERALD = "#10b981"; EMERALD_DARK = "#059669"; TEXT_LIGHT = "#f8fafc"
st.markdown(f"""
<style>
.app-header {{{ padding: 8px 0; border-bottom: 1px solid #e5e7eb; background:#fff; position:sticky; top:0; z-index:5; }}
.app-title {{ font-size:22px; font-weight:800; color:#111827; }}
.app-sub {{ color:#6b7280; font-size:12px; }}
[data-testid="stSidebar"] > div:first-child {{ background:linear-gradient(180deg, {EMERALD} 0%, {EMERALD_DARK} 100%); color:{TEXT_LIGHT}; }}
[data-testid="stSidebar"] .stMarkdown p,[data-testid="stSidebar"] label,[data-testid="stSidebar"] span {{ color:{TEXT_LIGHT} !important; }}
[data-testid="stSidebar"] .stRadio > div > label {{ margin-bottom:6px; padding:6px 10px; border-radius:8px; background:rgba(255,255,255,0.08); }}
.stButton>button {{ border-radius:8px; border:1px solid #e5e7eb; padding:.45rem .9rem; }}
.block-container {{ padding-top:6px; }}
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
    "name","name_ar","merchant_sku","category_id",
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

# -------- Persistent cache across reruns --------
@st.cache_resource
def global_cache() -> dict:
    return {}

@st.cache_resource
def http_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en",
        "Cache-Control": "no-cache"
    })
    return s

def ui_sleep(s):
    try: st.sleep(s)
    except Exception: time.sleep(s)

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
    u=u.strip()
    try:
        p=urlsplit(u); return p.scheme in ("http","https") and bool(p.netloc)
    except Exception: return False

def _normalize_url(u:str)->str:
    u=(u or "").strip().strip('"').strip("'")
    if not u: return ""
    if u.startswith("//"): u="https:"+u
    if not re.match(r"^https?://", u, flags=re.I): u="https://"+u
    p=urlsplit(u)
    path=quote(p.path, safe="/:%@&?=#,+!$;'()*[]")
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

def clean_url_for_vision(raw: str) -> str:
    u = str(raw or "").strip().strip('"').strip("'")
    u = re.sub(r"\s+", "", u)
    u = _normalize_url(u)
    return u if is_valid_url(u) else ""

def _to_data_url_from_http(url: str, timeout: int = 20, max_bytes: int = 12_000_000) -> str:
    try:
        u = clean_url_for_vision(url)
        if not u: return ""
        sess = http_session()
        for _ in range(3):
            r = sess.get(u, timeout=timeout, stream=True, allow_redirects=True)
            try:
                r.raise_for_status()
                data = r.content if r.content else r.raw.read(max_bytes + 1)
                if data and len(data) <= max_bytes:
                    mime = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
                    if not mime or "/" not in mime: mime = "image/jpeg"
                    b64 = base64.b64encode(data).decode("ascii")
                    return f"data:{mime};base64,{b64}"
            finally:
                try: r.close()
                except Exception: pass
            ui_sleep(0.3 + random.random()*0.3)
        return ""
    except Exception:
        return ""

def _retry(fn, attempts=4, base=0.5):
    for i in range(attempts):
        try: return fn()
        except Exception:
            if i == attempts - 1: raise
            ui_sleep(base * (2 ** i) + random.random()*0.2)

DEBUG = False
def debug_log(title: str, obj):
    if DEBUG:
        try: msg = json.dumps(obj, ensure_ascii=False, indent=2)
        except Exception: msg = str(obj)
        print(f"\n===== {title} =====\n{msg}\n", file=sys.stderr)

def safe_section(label, fn):
    try:
        st.markdown(f"<span style='color:#94a3b8;font-size:12px'>entering {label}</span>", unsafe_allow_html=True)
        return fn()
    except Exception as e:
        st.error(f"{label} crashed: {type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        return None

STRUCT_PROMPT_JSON = """
Look at the PHOTO and extract fields for an e-commerce title.
Return EXACTLY ONE LINE of STRICT JSON with keys:
{"object_type":string,"brand":string|null,"product":string|null,"variant":string|null,
"flavor_scent":string|null,"material":string|null,"size_value":string|null,
"size_unit":string|null,"count":string|null,"feature":string|null,"color":string|null,"descriptor":string|null}
Rules:
- object_type = visible item category in plain nouns.
- PRIORITIZE what you SEE over printed text when they disagree.
- brand MUST be null if no brand is clearly visible.
- size_value numeric only; size_unit in ['ml','L','g','kg','pcs','tabs','caps'].
- Output JSON only.
"""

# ============== Sidebar NAV ==============
with st.sidebar:
    st.markdown("### 🔑 API Keys")
    st.write("DeepL:", "✅ Active" if deepl_active else "❌ Missing/Invalid")
    st.write("OpenAI:", "✅ Active" if openai_active else "❌ Missing/Invalid")

    st.markdown("### 🧩 Translation options")
    USE_GLOSSARY = st.checkbox("Use glossary for EN→AR", value=True)
    GLOSSARY_CSV = st.text_area("Glossary CSV (source,target) one per line", height=120,
                                placeholder="Head & Shoulders,هيد اند شولدرز\nFairy,فيري")
    CONTEXT_HINT = st.text_input("Optional translation context", value="E-commerce product titles for a marketplace.")

    st.markdown("---")
    DEBUG = st.checkbox("🪲 Debug mode (log payloads)", value=False)
    section = st.radio(
        "Navigate",
        ["📊 Overview","🔎 Filter","🖼️ Titles & Translate","🧩 Grouping","📑 Sheet","⬇️ Downloads","⚙️ Settings"],
        index=0
    )

# ============== Uploads ==============
c1,c2=st.columns(2)
with c1: product_file = st.file_uploader("Product List (.xlsx/.csv, includes 'thumbnail')", type=["xlsx","xls","csv"], key="u1")
with c2: mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"], key="u2")
prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

if prod_df is not None:
    st.session_state["prod_df_cached"] = prod_df.copy()
if map_df is not None:
    st.session_state["map_df_cached"] = map_df.copy()
prod_df = prod_df if prod_df is not None else st.session_state.get("prod_df_cached")
map_df  = map_df  if map_df  is not None else st.session_state.get("map_df_cached")

loaded_ok = (
    isinstance(prod_df, pd.DataFrame) and
    validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List") and
    isinstance(map_df, pd.DataFrame) and
    validate_columns(map_df, ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"], "Category Mapping")
)
if not loaded_ok:
    st.info("Upload both files to proceed.")
    st.stop()

# ============== Memory & State ==============
st.session_state.setdefault("file_hash", None)
st.session_state.setdefault("proc_cache", {})
st.session_state.setdefault("audit_rows", [])
st.session_state.setdefault("keyword_library", [])
st.session_state.setdefault("page_size", 200)
st.session_state.setdefault("page_num", 1)
st.session_state.setdefault("search_q","")
st.session_state.setdefault("page_df", pd.DataFrame())

def mapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "sub_category_id" not in df or "sub_sub_category_id" not in df:
        return pd.Series([False]* (0 if df is None else len(df)), index=([] if df is None else df.index))
    return df["sub_category_id"].fillna("").astype(str).str.strip().ne("") & df["sub_sub_category_id"].fillna("").astype(str).str.strip().ne("")

def unmapped_mask_fn(df: pd.DataFrame) -> pd.Series:
    return ~mapped_mask_fn(df)

def build_lookups(map_df: pd.DataFrame):
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

current_hash = hash_uploaded_file(product_file) if product_file else st.session_state.get("file_hash")
if st.session_state.get("file_hash") != current_hash and isinstance(prod_df, pd.DataFrame):
    st.session_state.work = prod_df.copy()
    st.session_state.proc_cache = {}
    st.session_state.audit_rows = []
    st.session_state.file_hash = current_hash

work = st.session_state.get("work", pd.DataFrame())
# enforce string dtype for title columns
for _c in ["name","name_ar"]:
    if _c not in work.columns:
        work[_c] = pd.Series("", index=work.index, dtype="string")
    else:
        try:
            work[_c] = work[_c].astype("string")
        except Exception:
            work[_c] = work[_c].astype(str)

lookups = build_lookups(map_df) if map_df is not None else {"main_names":[], "main_to_subnames":{}, "pair_to_subsubnames":{}, "sub_name_to_no_by_main":{}, "ssub_name_to_no_by_main_sub":{}}

# ===== Overview =====
def sec_overview():
    if work is None or work.empty:
        st.info("No data loaded.")
        return
    mm = mapped_mask_fn(work)
    total=len(work); mapped=int(mm.sum()); unmapped=total-mapped
    en_ok=int(work["name"].fillna("").astype(str).str.strip().ne("").sum())
    ar_ok=int(work["name_ar"].fillna("").astype(str).str.strip().ne("").sum())
    st.subheader("Overview")
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

# ===== Titles & Translate =====
def _title_case_keep_units(s: str) -> str:
    if not s: return s
    words = s.split()
    keep_lower = {"ml","l","g","kg","pcs","tabs","caps","&","of"}
    out=[]
    for w in words:
        wl = w.lower()
        out.append(w if wl in keep_lower else w[:1].upper()+w[1:])
    return " ".join(out)

def assemble_title_from_fields(d: dict) -> str:
    def _s(v): return str(v).strip() if v is not None else ""
    def _num(v):
        s = _s(v)
        m = re.search(r"\d+(?:\.\d+)?", s)
        return m.group(0) if m else ""
    brand=_s(d.get("brand")); object_type=_s(d.get("object_type")); product=_s(d.get("product"))
    variant=_s(d.get("variant")); flavor=_s(d.get("flavor_scent")); material=_s(d.get("material"))
    feature=_s(d.get("feature")); color=_s(d.get("color")); descr=_s(d.get("descriptor"))
    size_v=_num(d.get("size_value")); size_u=_s(d.get("size_unit")).lower(); count=_num(d.get("count"))
    unit=size_u
    if unit in ["milliliter","mls","ml."]: unit="ml"
    if unit in ["liter","litre","ltrs","ltr"]: unit="L"
    if unit in ["grams","gram","gr"]: unit="g"
    if unit in ["kilogram","kilo","kgs"]: unit="kg"
    noun = object_type or product or descr
    qual = variant or flavor or material or feature
    parts=[]
    if brand: parts.append(brand)
    if noun:
        if not brand and (color or material) and noun not in {"deodorant","shampoo","chocolate","chocolate bar","soap","detergent"}:
            cm = " ".join([x for x in [color, material] if x])
            parts.append(f"{cm} {noun}".strip())
        else:
            parts.append(noun)
    if qual and (not brand or qual.lower() not in brand.lower()):
        parts.append(qual)
    size_str=""
    if size_v and unit: size_str=f"{size_v}{unit}"
    if count and not size_str: size_str=f"{count}pcs"
    elif count and size_str: size_str=f"{size_str} {count}pcs"
    if size_str: parts.append(size_str)
    title = " ".join(p for p in parts if p)
    if not title.strip():
        title = " ".join([x for x in [color, material, descr or noun] if x])
    return tidy_title(_title_case_keep_units(title), 70)

def _fallback_simple_title_from_dataurl(data_url: str, max_chars: int) -> str:
    if not openai_active or not data_url: return ""
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role":"system","content":"You are a precise e-commerce title writer."},
            {"role":"user","content":[
                {"type":"text","text":"Write ONE clean English product title ≤70 chars. Order: Brand, Product type, Variant, Material, Size/Count. No marketing."},
                {"type":"image_url","image_url":{"url":data_url}}
            ]}
        ],
        "temperature": 0.1,
        "max_tokens": 64
    }
    try:
        resp=_retry(lambda: openai_client.chat.completions.create(**payload))
        txt=(resp.choices[0].message.content or "").strip()
        return tidy_title(txt,max_chars) if txt else ""
    except Exception:
        return ""

def openai_title_from_url(img_url: str, max_chars: int, sku: Optional[str] = None) -> str:
    if not openai_active or not img_url: return ""
    # Prefer data URL (already provided by Image Extractor). If not data URL, try to convert.
    if img_url.startswith("data:"):
        data_url = img_url
    else:
        # Try raw URL first, then fallback to data-URL
        url = clean_url_for_vision(img_url)
        def _call(image_url: str) -> str:
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "Extract concise, accurate product fields from the image."},
                    {"role": "user", "content": [
                        {"type": "text", "text": STRUCT_PROMPT_JSON},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]}
                ],
                "temperature": 0.1,
                "max_tokens": 220,
            }
            resp=_retry(lambda: openai_client.chat.completions.create(**payload))
            raw=(resp.choices[0].message.content or "").strip()
            m = re.search(r"\{.*\}", raw, re.S)
            if not m: return ""
            data = json.loads(m.group(0))
            title = assemble_title_from_fields(data)
            return tidy_title(title, max_chars) if title else ""
        if url:
            try:
                t=_call(url)
                if t: return t
            except Exception: pass
        data_url = _to_data_url_from_http(url or img_url)
        if not data_url:
            st.session_state.audit_rows.append({"sku":sku or "", "phase":"EN title","reason":"no_fetchable_image","url": str(img_url)})
            return ""
    # Call with data URL
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Extract concise, accurate product fields from the image."},
            {"role": "user", "content": [
                {"type": "text", "text": STRUCT_PROMPT_JSON},
                {"type": "image_url", "image_url": {"url": data_url}}
            ]}
        ],
        "temperature": 0.1,
        "max_tokens": 220,
    }
    try:
        resp=_retry(lambda: openai_client.chat.completions.create(**payload))
        raw=(resp.choices[0].message.content or "").strip()
    except Exception:
        return _fallback_simple_title_from_dataurl(data_url, max_chars)
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return _fallback_simple_title_from_dataurl(data_url, max_chars)
    try:
        data = json.loads(m.group(0))
    except Exception:
        return _fallback_simple_title_from_dataurl(data_url, max_chars)
    obj = (data.get("object_type") or "").strip().lower()
    prod = (data.get("product") or "").strip().lower()
    if not (obj or prod):
        return _fallback_simple_title_from_dataurl(data_url, max_chars)
    title = assemble_title_from_fields(data)
    return tidy_title(title, max_chars) if title else _fallback_simple_title_from_dataurl(data_url, max_chars)

def deepl_batch_en2ar(texts: List[str], context_hint: str = "") -> List[str]:
    if not translator: return list(texts)
    try:
        if context_hint:
            return [translator.translate_text(t, source_lang="EN", target_lang="AR", context=context_hint).text for t in texts]
        return [translator.translate_text(t, source_lang="EN", target_lang="AR").text for t in texts]
    except Exception:
        return list(texts)

def openai_translate_batch_en2ar(texts:List[str])->List[str]:
    if not openai_active or not texts: return list(texts)
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role":"system","content":"Translate e-commerce product titles into natural, concise Arabic."},
            {"role":"user","content":"Translate each of these lines to Arabic, one per line:\n\n" + "\n".join(texts)}
        ],
        "temperature": 0
    }
    try:
        resp=_retry(lambda: openai_client.chat.completions.create(**payload))
        lines=(resp.choices[0].message.content or "").splitlines()
        return [l.strip() for l in lines if l.strip()] or texts
    except Exception:
        return texts

def translate_en_titles(titles_en: pd.Series, engine:str, batch_size:int, use_glossary=False, glossary_map:Optional[Dict[str,str]]=None, context_hint:str="")->pd.Series:
    texts=titles_en.fillna("").astype(str).tolist()
    if use_glossary and glossary_map:
        mapped=[]
        for t in texts:
            t2=t
            for src,tgt in glossary_map.items():
                if src and tgt:
                    t2=re.sub(rf"(?i)\b{re.escape(src)}\b", tgt, t2)
            mapped.append(t2)
        texts=mapped
    if engine=="DeepL" and deepl_active:
        return pd.Series(deepl_batch_en2ar(texts, context_hint), index=titles_en.index)
    if engine=="OpenAI":
        out=[]
        for s in range(0,len(texts),max(1,batch_size)):
            out.extend(openai_translate_batch_en2ar(texts[s:s+batch_size]))
            ui_sleep(0.1)
        return pd.Series(out, index=titles_en.index)
    return pd.Series(texts, index=titles_en.index)

# ===== Sections =====
def sec_titles():
    st.subheader("Titles & Translate")
    if work is None or work.empty:
        st.info("No data loaded."); return

    c1,c2,c3,c4=st.columns(4)
    with c1: max_len=st.slider("Max EN length",50,90,70,5, key="mxlen")
    with c2: engine=st.selectbox("Arabic engine", ["DeepL","OpenAI","None"], key="areng")
    with c3: only_empty=st.checkbox("Only empty EN", value=True, key="only_empty")
    with c4: force_over=st.checkbox("Force overwrite", value=False, key="force_over")

    scope=st.radio("Scope", ["All","Unmapped only","Missing EN","Missing AR"], horizontal=True, key="scope")
    if scope=="All": base_df=work
    elif scope=="Unmapped only": base_df=work[unmapped_mask_fn(work)]
    elif scope=="Missing EN": base_df=work[work["name"].fillna("").astype(str).str.strip().eq("")]
    else: base_df=work[work["name_ar"].fillna("").astype(str).str.strip().eq("")]

    b1,b2=st.columns(2)
    with b1: fetch_batch=st.number_input("Batch (image→EN)",10,300,100,10, key="fetch_batch")
    with b2: trans_batch=st.number_input("Batch (EN→AR)",10,300,150,10, key="trans_batch")

    ignore_cache = st.checkbox("Ignore cache this run", value=False, key="ign_cache")

    if st.button("Preview 24 images (no processing)", key="btn_preview_imgs"):
        gallery = st.container()
        view = base_df.head(24)
        if "thumbnail_dataurl" in view.columns:
            cols = gallery.columns(6)
            for j, (i, row) in enumerate(view.iterrows()):
                src = str(row.get("thumbnail_dataurl", "")) or clean_url_for_vision(str(row.get("thumbnail","")))
                with cols[j % 6]:
                    if src.startswith("data:") or is_valid_url(src): st.image(src, caption=f"Row {i}", width="stretch")
                    else: st.caption("Bad image")
        else:
            st.info("No thumbnail_dataurl present in current scope.")

    MAX_CACHE_PER_FILE = 20000
    def _trim_store(store: dict):
        if len(store) <= MAX_CACHE_PER_FILE: return
        for k in list(store.keys())[: len(store)//2]:
            store.pop(k, None)

    def _image_cache_key(i: int) -> str:
        raw_thumb = str(work.at[i,"thumbnail"]) if "thumbnail" in work.columns else ""
        data_col  = str(work.at[i,"thumbnail_dataurl"]) if "thumbnail_dataurl" in work.columns else ""
        if data_col.startswith("data:"):
            basis = data_col[:256]
        else:
            basis = clean_url_for_vision(raw_thumb)
        import hashlib as _hl
        return _hl.sha1(basis.encode()).hexdigest()[:12] if basis else f"row-{i}"

    def run_titles(idx, fetch_batch, max_len, only_empty, force_over) -> Tuple[int,int,int,int]:
        updated=skipped=failed=hard_fail_batches=0
        store = global_cache().setdefault(st.session_state.file_hash, {})
        en_out = {}
        for s in range(0, len(idx), fetch_batch):
            chunk = idx[s:s+fetch_batch]
            for i in chunk:
                sku = str(work.at[i,"merchant_sku"])
                cur_en = (str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else "").strip()

                # prefer data URL, else sanitized URL
                raw_thumb = str(work.at[i,"thumbnail"]) if "thumbnail" in work.columns else ""
                data_col  = str(work.at[i,"thumbnail_dataurl"]) if "thumbnail_dataurl" in work.columns else ""
                norm_url  = data_col if data_col.startswith("data:") else clean_url_for_vision(raw_thumb)

                cache_key = _image_cache_key(i)
                if not ignore_cache and not force_over and store.get(cache_key, {}).get("en"):
                    en_out[i] = store[cache_key]["en"]; skipped+=1; continue
                if only_empty and cur_en and not force_over:
                    if not ignore_cache and store.get(cache_key, {}).get("en"):
                        en_out[i] = store[cache_key]["en"]; skipped+=1; continue
                if not (data_col.startswith("data:") or is_valid_url(norm_url)):
                    st.session_state.audit_rows.append({"sku":sku,"phase":"EN title","reason":"no_image_source","url":raw_thumb}); failed+=1; continue

                title = openai_title_from_url(norm_url, max_len, sku)
                if title:
                    en_out[i] = title
                    store.setdefault(cache_key, {})["en"] = title
                    _trim_store(store)
                    updated += 1
                else:
                    failed += 1
            if en_out:
                idxs=list(en_out.keys()); vals=[en_out[j] for j in idxs]
                work.loc[idxs, "name"] = pd.Series(vals, index=idxs, dtype="string")
            ui_sleep(0.1)
        return updated, skipped, failed, hard_fail_batches

    def run_trans(idx, trans_batch, engine, force_over) -> Tuple[int,int]:
        if engine not in ("DeepL","OpenAI"): return 0,0
        store = global_cache().setdefault(st.session_state.file_hash, {})
        ids=[]; texts=[]
        for i in idx:
            sku=str(work.at[i,"merchant_sku"])
            cur_ar=(str(work.at[i,"name_ar"]) if pd.notna(work.at[i,"name_ar"]) else "").strip()
            en=(str(work.at[i,"name"]) if pd.notna(work.at[i,"name"]) else "").strip()
            if not en:
                st.session_state.audit_rows.append({"sku":sku,"phase":"AR translate","reason":"missing EN","url":str(work.at[i,"thumbnail"])})
                continue
            cache_key = _image_cache_key(i)
            if not ignore_cache and not force_over and store.get(cache_key, {}).get("ar"):
                work.at[i,"name_ar"]=store[cache_key]["ar"]; continue
            if force_over or not cur_ar:
                ids.append(i); texts.append(en)

        glossary_map = {}
        for line in (GLOSSARY_CSV or "").splitlines():
            if "," in line:
                src, tgt = line.split(",", 1)
                src = src.strip(); tgt = tgt.strip()
                if src and tgt: glossary_map[src]=tgt

        updated=failed=0
        for s in range(0, len(texts), trans_batch):
            chunk = texts[s:s+trans_batch]
            outs = translate_en_titles(pd.Series(chunk), engine, trans_batch, use_glossary=USE_GLOSSARY, glossary_map=glossary_map, context_hint=CONTEXT_HINT).tolist()
            for j, _ in enumerate(chunk):
                i = ids[s+j]; ar = outs[j] if j < len(outs) else ""
                if ar:
                    work.at[i,"name_ar"] = str(ar)
                    cache_key = _image_cache_key(i)
                    store.setdefault(cache_key, {})["ar"] = str(ar)
                    updated += 1
                else:
                    failed += 1
            ui_sleep(0.05)
        return updated, failed

    if st.button("Run FULL pipeline on ENTIRE scope (auto-batched)", key="btn_full_pipeline"):
        idx_all = base_df.index.tolist()
        if not idx_all:
            st.info("No rows in scope.")
        else:
            total=len(idx_all); bar=st.progress(0.0, text="Starting…")
            en_up=en_skip=en_fail=en_halt=ar_up=ar_fail=0
            total_batches=math.ceil(total/fetch_batch); bno=0
            for s in range(0,total,fetch_batch):
                bno+=1; batch_idx=idx_all[s:s+fetch_batch]
                u,k,f,h = run_titles(batch_idx, fetch_batch, max_len, only_empty, force_over); en_up+=u; en_skip+=k; en_fail+=f; en_halt+=h
                u2,f2 = run_trans(batch_idx, trans_batch, engine, force_over); ar_up+=u2; ar_fail+=f2
                done=s+len(batch_idx)
                bar.progress(min(done/total,1.0), text=f"Processed {done}/{total} rows • Batch {bno}/{total_batches}")
                ui_sleep(0.15)
            st.success(f"Done. EN updated {en_up}, skipped {en_skip}, failed {en_fail} | AR updated {ar_up}, failed {ar_fail}")

    cA,cB=st.columns(2)
    with cA:
        if st.button("Run ONLY missing EN", key="btn_only_en"):
            ids=base_df[base_df["name"].fillna("").astype(str).str.strip().eq("")].index.tolist()
            if ids:
                u,k,f,h = run_titles(ids, fetch_batch, max_len, only_empty, force_over)
                st.success(f"EN → updated {u}, skipped {k}, failed {f}")
            else:
                st.info("No missing EN.")
    with cB:
        if st.button("Run ONLY missing AR", key="btn_only_ar"):
            ids=base_df[base_df["name_ar"].fillna("").astype(str).str.strip().eq("")].index.tolist()
            if ids:
                u2,f2 = run_trans(ids, trans_batch, engine, force_over)
                st.success(f"AR → updated {u2}, failed {f2}")
            else:
                st.info("No missing AR.")

    # Data URL sanity check
    if st.button("🔎 Check first 3 data URLs"):
        rows = work.head(3)
        for idx, r in rows.iterrows():
            du = str(r.get("thumbnail_dataurl",""))
            st.write({"row": int(idx), "has_dataurl": du.startswith("data:"), "len": len(du)})
            if du.startswith("data:"):
                st.image(du, caption=f"Row {idx}", width="stretch")

def sec_grouping():
    st.subheader("Grouping")
    if work is None or work.empty:
        st.info("No data loaded."); return
    left,right=st.columns([1,2])
    with left:
        st.markdown("**Keyword Library**")
        new_kws=st.text_area("Add keywords (one per line)", placeholder="soap\nshampoo\ndishwashing\nlemon", key="kw_add")
        if st.button("➕ Add", key="kw_add_btn"):
            fresh=[k.strip() for k in new_kws.splitlines() if k.strip()]
            if fresh:
                exist=set(st.session_state.keyword_library)
                st.session_state.keyword_library.extend([k for k in fresh if k not in exist])
                st.session_state.keyword_library=list(dict.fromkeys(st.session_state.keyword_library))
                st.success(f"Added {len(fresh)}")
            else: st.info("Nothing to add.")
        rm=st.multiselect("Remove", options=st.session_state.keyword_library, key="kw_rm_sel")
        if st.button("🗑️ Remove selected", key="kw_rm_btn"):
            s=set(rm); st.session_state.keyword_library=[k for k in st.session_state.keyword_library if k not in s]
            st.success(f"Removed {len(s)}")
    with right:
        st.markdown("**Select keywords/tokens to map**")
        base_df=work[unmapped_mask_fn(work)]
        if base_df is None or base_df.empty:
            st.info("No unmapped rows to group."); return
        tok=Counter()
        for _,r in base_df.iterrows():
            tok.update(tokenize(r.get("name",""))); tok.update(tokenize(r.get("name_ar","")))
        auto=[t for t,c in tok.most_common() if c>=3][:60]
        def hits(df,term):
            tl=term.lower()
            m=df["name"].astype(str).str.lower().str.contains(tl,na=False) | df["name_ar"].astype(str).str.lower().str.contains(tl,na=False)
            return int(m.sum())
        opts=[]; keymap={}
        for kw in st.session_state.keyword_library:
            disp=f"{kw} ({hits(base_df,kw)}) [Saved]"; opts.append(disp); keymap[disp]=kw
        for t in auto:
            disp=f"{t} ({hits(base_df,t)}) [Auto]"; opts.append(disp); keymap[disp]=t
        picked=st.multiselect("Pick", options=opts, key="kw_pick")
        if picked:
            mask=pd.Series(False,index=base_df.index)
            for d in picked:
                term=keymap[d].lower()
                mask|=base_df["name"].astype(str).str.lower().str.contains(term,na=False)
                mask|=base_df["name_ar"].astype(str).str.lower().str.contains(term,na=False)
            hits_df=base_df[mask].copy()
            st.write(f"Matches: {hits_df.shape[0]}")
            st.dataframe(
                hits_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]],
                width="stretch", height=260
            )
            skus=hits_df["merchant_sku"].astype(str).tolist()
            chosen=st.multiselect("Select SKUs", options=skus, default=skus, key="kw_skus")
            c1,c2,c3=st.columns(3)
            g_main=c1.selectbox("Main", [""]+lookups["main_names"], key="g_main")
            g_sub =c2.selectbox("Sub", [""]+lookups["main_to_subnames"].get(g_main,[]), key="g_sub")
            g_ssub=c3.selectbox("Sub-Sub", [""]+lookups["pair_to_subsubnames"].get((g_main,g_sub),[]), key="g_ssub")
            if st.button("Apply mapping", key="apply_map_btn"):
                if not chosen: st.info("No SKUs.")
                elif not (g_main and g_sub and g_ssub): st.warning("Pick all levels.")
                else:
                    m=work["merchant_sku"].astype(str).isin(chosen)
                    work.loc[m,"category_id"]=g_main
                    work.loc[m,"sub_category_id"]=lookups["sub_name_to_no_by_main"].get((g_main,g_sub),"")
                    work.loc[m,"sub_sub_category_id"]=lookups["ssub_name_to_no_by_main_sub"].get((g_main,g_sub,g_ssub),"")
                    st.success(f"Applied to {int(m.sum())} rows.")
        else:
            st.info("Pick at least one keyword/token.")

def sec_sheet():
    st.subheader("Sheet")
    if work is None or work.empty:
        st.info("No data loaded."); return pd.DataFrame()
    view=st.radio("Quick filter", ["All","Mapped only","Unmapped only"], horizontal=True, key="sheet_filter")
    base=work.copy(); mm=mapped_mask_fn(base)
    if view=="Mapped only": base=base[mm]
    elif view=="Unmapped only": base=base[~mm]
    st.session_state.page_size=st.number_input("Rows/page",50,5000,st.session_state.page_size,50, key="rows_page")
    total=base.shape[0]; pages=max(1, math.ceil(total/st.session_state.page_size))
    st.session_state.page_num=st.number_input("Page",1,pages,min(st.session_state.page_num,pages),1, key="page_no")
    st.caption(f"{total} rows total")
    start=(st.session_state.page_num-1)*st.session_state.page_size; end=start+st.session_state.page_size
    page=base.iloc[start:end].copy()
    st.dataframe(page, width="stretch", height=440)
    st.session_state["page_df"] = page
    return page

def to_excel_download(df, sheet_name="Products"):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w: df.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0); return buf

def sec_downloads():
    st.subheader("Downloads")
    df_full = st.session_state.get("work", pd.DataFrame())
    df_view = st.session_state.get("page_df", df_full)
    try:
        st.download_button("⬇️ Full Excel", to_excel_download(df_full), file_name="products_mapped.xlsx", key="dl_full")
    except Exception as e:
        st.error(f"Full export failed: {e}")
    try:
        st.download_button("⬇️ Current view Excel", to_excel_download(df_view), file_name="products_view.xlsx", key="dl_view")
    except Exception as e:
        st.error(f"View export failed: {e}")
    if st.session_state.get("audit_rows"):
        audit_df=pd.DataFrame(st.session_state.audit_rows)
        st.download_button("⬇️ Audit log (CSV)", data=audit_df.to_csv(index=False).encode("utf-8"),
                           file_name="audit_log.csv", mime="text/csv", key="dl_audit")
    else:
        st.caption("No audit rows yet.")

# ============== Router ==============
if section=="📊 Overview":
    safe_section("Overview", sec_overview)
elif section=="🔎 Filter":
    safe_section("Grouping (quick view)", sec_grouping)
elif section=="🖼️ Titles & Translate":
    safe_section("Titles & Translate", sec_titles)
elif section=="🧩 Grouping":
    safe_section("Grouping", sec_grouping)
elif section=="📑 Sheet":
    _tmp = safe_section("Sheet", sec_sheet)
    if isinstance(_tmp, pd.DataFrame):
        st.session_state["page_df"] = _tmp
elif section=="⬇️ Downloads":
    safe_section("Downloads", sec_downloads)
else:
    st.subheader("Settings & Diagnostics")
    c1,c2=st.columns(2)
    with c1:
        if st.button("Show 10 sanitized thumbnail URLs", key="diag_urls"):
            sample=work["thumbnail"].astype(str).head(10).tolist() if "thumbnail" in work.columns else []
            for u in sample:
                norm=clean_url_for_vision(u); st.write({"raw":u,"sanitized":norm,"valid":is_valid_url(norm)})
    with c2:
        if st.button("Clear per-file cache & audit", key="diag_clear"):
            st.session_state.proc_cache={}; st.session_state.audit_rows=[]
            store = global_cache()
            if st.session_state.file_hash in store: del store[st.session_state.file_hash]
            st.success("Cleared.")
