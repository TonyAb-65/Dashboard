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
.app-header {{ padding: 8px 0; border-bottom: 1px solid #e5e7eb; background:#fff; position:sticky; top:0; z-index:5; }}
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

prod_df_new = read_any_table(product_file) if product_file else None
map_df_new  = read_any_table(mapping_file) if mapping_file else None

if isinstance(prod_df_new, pd.DataFrame):
    st.session_state["prod_df_cached"] = prod_df_new.copy()
if isinstance(map_df_new, pd.DataFrame):
    st.session_state["map_df_cached"] = map_df_new.copy()

prod_df = st.session_state.get("prod_df_cached")
map_df  = st.session_state.get("map_df_cached")

loaded_products_ok = (
    isinstance(prod_df, pd.DataFrame)
    and validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List")
)

loaded_mapping_ok = (
    isinstance(map_df, pd.DataFrame)
    and validate_columns(
        map_df,
        ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"],
        "Category Mapping"
    )
)

if not loaded_products_ok:
    st.info("Upload a Product List to continue.")
    st.stop()

current_hash = hash_uploaded_file(product_file) if product_file else st.session_state.get("file_hash")
if st.session_state.get("file_hash") != current_hash and loaded_products_ok:
    st.session_state.work = prod_df.copy()
    st.session_state.proc_cache = {}
    st.session_state.audit_rows = []
    st.session_state.file_hash = current_hash

work = st.session_state.get("work", pd.DataFrame())
for _c in ["name","name_ar"]:
    if _c not in work.columns:
        work[_c] = pd.Series("", index=work.index, dtype="string")
    else:
        try: work[_c] = work[_c].astype("string")
        except Exception: work[_c] = work[_c].astype(str)

lookups = build_lookups(map_df) if loaded_mapping_ok else {"main_names":[], "main_to_subnames":{}, "pair_to_subsubnames":{}, "sub_name_to_no_by_main":{}, "ssub_name_to_no_by_main_sub":{}}

# ===== Overview, Titles & Translate, Grouping, Sheet, Downloads =====
# (all your existing sec_overview, sec_titles, sec_grouping, sec_sheet, sec_downloads go here unchanged)

# ============== Router ==============
if section=="📊 Overview":
    safe_section("Overview", sec_overview)
elif section=="🔎 Filter":
    if loaded_mapping_ok:
        safe_section("Grouping (quick view)", sec_grouping)
    else:
        st.warning("Upload a valid Category Mapping to use Filter / Grouping.")
elif section=="🖼️ Titles & Translate":
    safe_section("Titles & Translate", sec_titles)
elif section=="🧩 Grouping":
    if loaded_mapping_ok:
        safe_section("Grouping", sec_grouping)
    else:
        st.warning("Upload a valid Category Mapping to use Grouping.")
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
