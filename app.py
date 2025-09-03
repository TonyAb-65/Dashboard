# Product Mapping Dashboard — Master (fixed v3)
# Includes: gpt-4o, two-pass Vision extractor, normalize_title_en, router fix

import io, re, time, math, hashlib, json, sys, traceback, base64
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
    pass

openai_client=None; openai_active=False
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")
    if OPENAI_API_KEY:
        openai_client = OpenAI(api_key=OPENAI_API_KEY); openai_active=True
except Exception:
    pass

@st.cache_resource
def global_cache() -> dict:
    return {}

# ============== HELPERS ==============
def tidy_title(s: str, max_chars: int=70) -> str:
    s=re.sub(r"[*_`]+","",s or ""); s=re.sub(r"\s+"," ",s).strip()
    if len(s)<=max_chars: return s
    cut=s[:max_chars].rstrip()
    if " " in cut: cut=cut[:cut.rfind(" ")]
    return cut

def is_valid_url(u:str)->bool:
    if not isinstance(u,str): return False
    try:
        p=urlsplit(u.strip()); return p.scheme in ("http","https") and bool(p.netloc)
    except: return False

def clean_url_for_vision(raw: str) -> str:
    u=str(raw or "").strip().strip('"').strip("'")
    if not re.match(r"^https?://",u): u="https://"+u
    return u if is_valid_url(u) else ""

def _retry(fn, attempts=3, base=0.5):
    for i in range(attempts):
        try: return fn()
        except:
            if i==attempts-1: raise
            time.sleep(base*(2**i))

DEBUG=False
def debug_log(title,obj):
    if DEBUG:
        try: print(f"\n===== {title} =====\n{json.dumps(obj,ensure_ascii=False,indent=2)}\n",file=sys.stderr)
        except: print(f"\n===== {title} =====\n{obj}\n",file=sys.stderr)

# ===== Title generation =====
STRUCT_PROMPT_JSON = """
Look at the PHOTO and extract fields for an e-commerce title.
Return JSON with keys:
{"object_type":string,"brand":string|null,"flavor_scent":string|null,
"material":string|null,"size_value":string|null,"size_unit":string|null,
"count":string|null,"color":string|null,"descriptor":string|null}
Rules:
- Use plain nouns (deodorant, chocolate bar, soap holder).
- brand=null if no visible brand.
- size_unit in [ml,L,g,kg,pcs,tabs,caps].
- color basic if clear.
- descriptor for generic name when brand/size missing.
"""

def normalize_title_en(d: dict) -> str:
    b=(d.get("brand") or "").strip()
    n=(d.get("object_type") or d.get("descriptor") or "").strip()
    flav=(d.get("flavor_scent") or "").strip()
    mat=(d.get("material") or "").strip()
    col=(d.get("color") or "").strip()
    sz=(d.get("size_value") or "").strip()
    su=(d.get("size_unit") or "").strip()
    ct=(d.get("count") or "").strip()

    parts=[]
    if b: parts.append(b)
    if n:
        if not b and (col or mat): parts.append(" ".join([col,mat,n]).strip())
        else: parts.append(n)
    if flav: parts.append(flav)
    if sz and su: parts.append(f"{sz}{su}")
    if ct and not (sz and su): parts.append(f"{ct}pcs")
    title=" ".join(p for p in parts if p)
    return tidy_title(title,70)

def vision_extract_fields(img_url: str) -> Optional[dict]:
    payload={
      "model":"gpt-4o",
      "messages":[
        {"role":"system","content":"Extract structured product fields."},
        {"role":"user","content":[
          {"type":"text","text":STRUCT_PROMPT_JSON},
          {"type":"image_url","image_url":{"url":img_url}}
        ]}
      ],
      "temperature":0,
      "max_tokens":200
    }
    try:
        resp=_retry(lambda: openai_client.chat.completions.create(**payload))
        raw=resp.choices[0].message.content
        m=re.search(r"\{.*\}",raw,re.S)
        return json.loads(m.group(0)) if m else None
    except: return None

def openai_title_from_url(img_url: str, max_chars:int, sku=None) -> str:
    if not openai_active or not img_url: return ""
    fields=vision_extract_fields(img_url)
    if fields: return normalize_title_en(fields)
    # fallback quick title
    payload={
      "model":"gpt-4o",
      "messages":[
        {"role":"system","content":"Write concise e-commerce title"},
        {"role":"user","content":[
          {"type":"text","text":"One clean English title. Examples: Axe Deodorant Active 150 ml; Cadbury Milk Chocolate Caramel 90 g; Green Ceramic Soap Holder; Olive Oil Dispenser."},
          {"type":"image_url","image_url":{"url":img_url}}
        ]}
      ],
      "temperature":0,
      "max_tokens":64
    }
    try:
        resp=_retry(lambda: openai_client.chat.completions.create(**payload))
        return tidy_title(resp.choices[0].message.content or "",max_chars)
    except: return ""

# ============== Router fix ==============
def sec_sheet(): 
    st.subheader("Sheet"); return pd.DataFrame()  # stub

section="📑 Sheet"  # simulate
if section=="📑 Sheet":
    _tmp=sec_sheet()
    page_df=_tmp if _tmp is not None else pd.DataFrame()
