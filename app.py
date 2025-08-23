# Product Mapping Dashboard — Master (direct-URL vision, object_type-aware titles, strict fallback, clean audit, OpenAI error logging)

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

def _ensure_http_url(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if u.startswith("//"): return "https:" + u
    if not re.match(r"^https?://", u, flags=re.I):
        return "https://" + u
    return u

# ---------- retry wrapper for OpenAI ----------
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
    "Look at the PHOTO and extract fields for an e-commerce title.\n"
    "Return EXACTLY ONE LINE of STRICT JSON with keys:"
    '{"object_type":string,"brand":string|null,"product":string|null,"variant":string|null,'
    '"flavor_scent":string|null,"material":string|null,"size_value":string|null,'
    '"size_unit":string|null,"count":string|null,"feature":string|null}\n'
    "Rules:\n"
    "- object_type = visible item category (e.g., 'glass teapot', 'shampoo bottle').\n"
    "- PRIORITIZE object_type over printed text when they disagree.\n"
    "- NEVER output 'tea bag' unless an actual bag/sachet is visible.\n"
    "- If brand not visible set brand=null. If product is unclear set product=null.\n"
    "- size_value numeric only; size_unit in ['ml','L','g','kg','pcs','tabs','caps'].\n"
    "- feature is a short visible attribute (e.g., 'heat-resistant').\n"
    "- Output JSON only."
)

def assemble_title_from_fields(d: dict) -> str:
    brand = (d.get("brand") or "").strip()
    object_type = (d.get("object_type") or "").strip()
    product = (d.get("product") or "").strip()
    variant = (d.get("variant") or "").strip()
    flavor  = (d.get("flavor_scent") or "").strip()
    material= (d.get("material") or "").strip()
    feature = (d.get("feature") or "").strip()
    size_v  = (d.get("size_value") or "").strip()
    size_u  = (d.get("size_unit") or "").strip().lower()
    count   = (d.get("count") or "").strip()

    parts=[]
    if brand: parts.append(brand)
    noun = object_type or product
    if noun: parts.append(noun)

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
def _fallback_simple_title_url(img_url: str, max_chars: int) -> str:
    if not openai_active or not img_url: return ""
    prompt = (
        "Write ONE clean English e-commerce title ≤70 chars.\n"
        "Describe the VISIBLE object first. Ignore printed claims if they contradict the object.\n"
        "Order: Brand, Object/Product, Variant/Flavor/Scent, Material, Size/Count.\n"
        "No marketing words. One line."
    )
    try:
        resp=_openai_chat(
            [{"role":"system","content":"You are a precise e-commerce title writer."},
             {"role":"user","content":[
                 {"type":"text","text":prompt},
                 {"type":"image_url","image_url":{"url":img_url}}
             ]}],
            temperature=0, max_tokens=64
        )
        txt=(resp.choices[0].message.content or "").strip()
        return tidy_title(txt,max_chars) if txt else ""
    except Exception:
        return ""

# ---------- Vision on direct public URL with error logging ----------
def openai_title_from_url(img_url: str, max_chars: int, sku: str | None = None) -> str:
    """Call OpenAI Vision on a PUBLIC image URL. On errors, log precise reason."""
    if not openai_active or not img_url:
        return ""
    try:
        resp = _openai_chat(
            [
                {"role": "system", "content": "Extract concise, accurate product fields from the image."},
                {"role": "user", "content": [
                    {"type": "text", "text": STRUCT_PROMPT_JSON},
                    {"type": "image_url", "image_url": {"url": img_url}}
                ]}
            ],
            temperature=0.1, max_tokens=220
        )
        raw = (resp.choices[0].message.content or "").strip()
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return _fallback_simple_title_url(img_url, max_chars)

        try:
            data = json.loads(m.group(0))
        except Exception:
            return _fallback_simple_title_url(img_url, max_chars)

        obj = (data.get("object_type") or "").strip().lower()
        prod = (data.get("product") or "").strip().lower()
        invalid = {"ml","l","g","kg","pcs","tabs","caps"}
        if (not obj and not prod) or (prod in invalid) or (
            ("bag" in obj and "tea" in obj) or ("tea bag" in prod)
        ):
            return _fallback_simple_title_url(img_url, max_chars)

        title = assemble_title_from_fields(data)
        return tidy_title(title, max_chars) if title else _fallback_simple_title_url(img_url, max_chars)

    except Exception as e:
        try:
            st.session_state.audit_rows.append({
                "sku": str(sku or ""),
                "phase": "EN title",
                "reason": f"openai_error:{type(e).__name__}",
                "url": img_url
            })
        except Exception:
            pass
        return ""

# ============== Translation ==============
# ... (UNCHANGED, keep DeepL + OpenAI translation functions here)

# ============== run_titles function (patched) ==============
def run_titles(idx, fetch_batch, max_len, only_empty, force_over) -> Tuple[int,int,int]:
    updated = skipped = failed = 0
    store = global_cache().setdefault(st.session_state.file_hash, {})

    for s in range(0, len(idx), fetch_batch):
        chunk = idx[s:s+fetch_batch]
        for i in chunk:
            sku = str(work.at[i, "merchant_sku"])
            cache_local = st.session_state.proc_cache.get(sku, {})
            cur_en = (str(work.at[i, "name"]) if pd.notna(work.at[i, "name"]) else "").strip()
            raw_url = str(work.at[i, "thumbnail"]) if "thumbnail" in work.columns else ""
            norm_url = _normalize_url(_ensure_http_url(raw_url))

            if not force_over and store.get(sku, {}).get("en"):
                work.at[i, "name"] = store[sku]["en"]
                st.session_state.proc_cache.setdefault(sku, {})["name"] = store[sku]["en"]
                skipped += 1
                continue
            if not force_over and cache_local.get("name"):
                work.at[i, "name"] = cache_local["name"]
                skipped += 1
                continue
            if only_empty and cur_en and not force_over:
                st.session_state.proc_cache.setdefault(sku, {})["name"] = cur_en
                skipped += 1
                continue

            if not is_valid_url(norm_url):
                st.session_state.audit_rows.append({
                    "sku": sku, "phase": "EN title", "reason": "url_invalid", "url": norm_url
                })
                failed += 1
                continue

            title = openai_title_from_url(norm_url, max_len, sku)

            if title:
                work.at[i, "name"] = title
                st.session_state.proc_cache.setdefault(sku, {})["name"] = title
                store.setdefault(sku, {})["en"] = title
                updated += 1
            else:
                st.session_state.audit_rows.append({
                    "sku": sku, "phase": "EN title", "reason": "vision_empty_or_invalid", "url": norm_url
                })
                failed += 1

    return updated, skipped, failed
