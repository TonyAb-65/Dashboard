# Product Mapping Dashboard — Master (patched: stronger object_type handling, no nonsense titles)

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

# … [UNCHANGED UI STYLE AND HEADER BLOCKS] …

# ============== API CLIENTS ==============
# … [UNCHANGED] …

# ============== HELPERS ==============
# … [UNCHANGED tokenize, tidy_title, etc.] …

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
def _fallback_simple_title(data_url: str, max_chars: int) -> str:
    if not openai_active or not data_url: return ""
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
                 {"type":"image_url","image_url":{"url":data_url}}
             ]}],
            temperature=0, max_tokens=64
        )
        txt=(resp.choices[0].message.content or "").strip()
        return tidy_title(txt,max_chars) if txt else ""
    except Exception:
        return ""

def openai_title_from_image(url:str,max_chars:int)->str:
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

        obj=(data.get("object_type") or "").strip().lower()
        prod=(data.get("product") or "").strip().lower()
        invalid_tokens={"ml","l","g","kg","pcs","tabs","caps"}
        if (not obj and not prod) or (prod in invalid_tokens):
            return _fallback_simple_title(data_url, max_chars)
        if (obj and "bag" in obj and "tea" in obj) or (prod and "tea bag" in prod):
            return _fallback_simple_title(data_url, max_chars)

        title=assemble_title_from_fields(data)
        if title and len(title)>=3:
            return tidy_title(title,max_chars)
        return _fallback_simple_title(data_url, max_chars)
    except Exception:
        return _fallback_simple_title(data_url, max_chars)

# ============== run_titles with surgical fix ==============
def run_titles(idx, fetch_batch, max_len, only_empty, force_over) -> Tuple[int,int,int]:
    updated = skipped = failed = 0
    store = global_cache().setdefault(st.session_state.file_hash, {})

    for s in range(0, len(idx), fetch_batch):
        chunk = idx[s:s+fetch_batch]
        for i in chunk:
            sku = str(work.at[i, "merchant_sku"])
            cache_local = st.session_state.proc_cache.get(sku, {})
            cur_en = (str(work.at[i, "name"]) if pd.notna(work.at[i, "name"]) else "").strip()
            url = str(work.at[i, "thumbnail"]) if "thumbnail" in work.columns else ""

            # prefer persistent store if not forcing
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

            title = ""
            if url:
                title = openai_title_from_image(url, max_len)

            if title:
                work.at[i, "name"] = title
                st.session_state.proc_cache.setdefault(sku, {})["name"] = title
                store.setdefault(sku, {})["en"] = title
                updated += 1
            else:
                # precise audit reason
                data_url = fetch_image_as_data_url(url) if url else ""
                reason = "fetch_failed" if not data_url else "vision_empty_or_invalid"
                st.session_state.audit_rows.append({
                    "sku": sku, "phase": "EN title", "reason": reason, "url": url
                })
                failed += 1

    return updated, skipped, failed

# ============== Translation, Mapping, UI Sections, Router ==============
# … [UNCHANGED from previous master file version] …
