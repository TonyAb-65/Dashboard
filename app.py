import io
import re
import time
import math
import json
from typing import List
from urllib.parse import urlparse
from collections import Counter

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ========================= Page & THEME =========================
st.set_page_config(page_title="Product Master: Overview • Grouping • Sheet", layout="wide")

# Emerald-green sidebar styling (non-invasive)
st.markdown("""
<style>
/* Sidebar emerald theme */
section[data-testid="stSidebar"] {
  background: #064e3b; /* emerald-900 */
  color: #ecfdf5;      /* emerald-50 */
}
section[data-testid="stSidebar"] * {
  color: #ecfdf5 !important;
}
.sidebar-btn {
  width: 100%;
  background: #10b981; /* emerald-500 */
  color: #062c22;      /* dark text for contrast */
  border: 0;
  border-radius: 8px;
  padding: 10px 12px;
  margin: 4px 0 8px 0;
  font-weight: 600;
  cursor: pointer;
}
.sidebar-btn:hover {
  background: #059669; /* emerald-600 */
}
.small-caption {
  font-size: 12px;
  opacity: 0.85;
}
</style>
""", unsafe_allow_html=True)

st.title("🛒 Product Master")

# ========================= Required Columns =========================
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    "thumbnail",  # Column W (we also accept aliases and map them)
]

REQUIRED_MAP_COLS = [
    "category_id",
    "sub_category_id",
    "sub_category_id NO",
    "sub_sub_category_id",
    "sub_sub_category_id NO",
]

# ========================= Optional APIs =========================
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

# ========================= File IO =========================
def read_any_table(uploaded_file):
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

def to_excel_download(df, sheet_name="Products"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf

# ========================= Helpers =========================
def strip_markdown(s: str) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"[*_`]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tidy_title(s: str, max_chars: int = 65) -> str:
    s = strip_markdown(s or "")
    s = re.sub(r"\s{2,}", " ", s).strip()
    if len(s) <= max_chars:
        return s
    cut = s[:max_chars].rstrip()
    if " " in cut: cut = cut[: cut.rfind(" ")]
    return cut

def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(str(u))
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def fetch_thumb(url: str, timeout=7):
    try:
        if not is_valid_url(url): return None
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        return None

# ========================= Mapping Lookups =========================
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

# ========================= Vision Title Extraction =========================
STRUCT_PROMPT_JSON = (
    "You are extracting product info from an image for an e-commerce TITLE.\n"
    "Return a SINGLE LINE of STRICT JSON with keys:\n"
    '{'
    '"brand": string|null, '
    '"product": string, '
    '"variant": string|null, '
    '"flavor_scent": string|null, '
    '"material": string|null, '
    '"size_value": string|null, '
    '"size_unit": string|null, '
    '"count": string|null'
    '}\n'
    "Rules:\n"
    "- Read visible label text. If unknown, use null.\n"
    "- size_value examples: '500', '1', '2.5'  (numeric only)\n"
    "- size_unit examples: 'ml','L','g','kg','pcs','tabs','caps'\n"
    "- count is package count if shown (e.g., '4', '12').\n"
    "- Do not add extra keys or commentary. JSON only.\n"
)

def assemble_title_from_fields(d: dict) -> str:
    brand = (d.get("brand") or "").strip()
    product = (d.get("product") or "").strip()
    variant = (d.get("variant") or "").strip()
    flavor = (d.get("flavor_scent") or "").strip()
    material = (d.get("material") or "").strip()
    size_v = (d.get("size_value") or "").strip()
    size_u = (d.get("size_unit") or "").strip().lower()
    count  = (d.get("count") or "").strip()

    parts = []
    if brand: parts.append(brand)
    if product: parts.append(product)
    if variant: parts.append(variant)
    elif flavor: parts.append(flavor)
    elif material: parts.append(material)

    unit = size_u
    if unit in ["milliliter","mls","ml."]: unit = "ml"
    if unit in ["liter","litre","ltrs","ltr"]: unit = "L"
    if unit in ["grams","gram","gr"]: unit = "g"
    if unit in ["kilogram","kilo","kgs"]: unit = "kg"

    size_str = ""
    if size_v and unit:
        size_str = f"{size_v}{unit}"
    if count and not size_str:
        size_str = f"{count}pcs"
    elif count and size_str:
        size_str = f"{size_str} {count}pcs"

    if size_str: parts.append(size_str)
    return tidy_title(" ".join(p for p in parts if p), 65)

def openai_title_from_image(url: str, fallback_hint: str, max_chars: int) -> str:
    if not openai_active or not is_valid_url(url):
        return tidy_title(fallback_hint or "", max_chars)
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":"Extract concise, accurate product fields from the image."},
                {"role":"user","content":[
                    {"type":"text","text":STRUCT_PROMPT_JSON},
                    {"type":"image_url","image_url":{"url":url}}
                ]}
            ],
            temperature=0.1,
        )
        raw = (resp.choices[0].message.content or "").strip()
        m = re.search(r"\{.*\}", raw, re.S)
        json_str = m.group(0) if m else raw
        data = json.loads(json_str)
        title = assemble_title_from_fields(data)
        if not title or len(title) < 3:
            return tidy_title(fallback_hint or "", max_chars)
        return tidy_title(title, max_chars)
    except Exception:
        return tidy_title(fallback_hint or "", max_chars)

def translate_en_to_ar(texts: List[str], engine: str) -> List[str]:
    if engine == "DeepL" and deepl_active and translator:
        try:
            res = translator.translate_text(texts, source_lang="EN", target_lang="AR")
            return [r.text for r in (res if isinstance(res, list) else [res])]
        except Exception:
            return texts
    if engine == "OpenAI" and openai_active:
        sys = "Translate e-commerce product titles into natural, concise Arabic suitable for product cards."
        usr = "Translate each of these lines to Arabic, one per line:\n\n" + "\n".join(texts)
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content":sys},{"role":"user","content":usr}],
                temperature=0,
            )
            lines = (resp.choices[0].message.content or "").splitlines()
            out = [l.strip() for l in lines if l.strip()]
            return out if len(out) == len(texts) else texts
        except Exception:
            return texts
    return texts

def titles_from_images_batched(df, row_index, max_chars, batch_size, force_regen=False):
    titles_en = pd.Series([""]*len(row_index), index=row_index, dtype="object")
    prog = st.progress(0)
    steps = max(1, math.ceil(len(row_index)/max(1,batch_size)))
    for step, start in enumerate(range(0,len(row_index),batch_size),1):
        chunk_idx = row_index[start:start+batch_size]
        for i in chunk_idx:
            cur_en = str(df.loc[i,"name"]) if "name" in df.columns else ""
            if cur_en.strip() and not force_regen:
                titles_en.loc[i] = cur_en
                continue
            url = str(df.loc[i,"thumbnail"]) if "thumbnail" in df.columns else ""
            fallback = cur_en.strip() or str(df.loc[i,"merchant_sku"])
            title = openai_title_from_image(url, fallback, max_chars)
            if not title and str(df.loc[i,"merchant_sku"]).strip():
                title = f"Product {df.loc[i,'merchant_sku']}"
            titles_en.loc[i] = title
        prog.progress(min(step/steps,1.0))
        time.sleep(0.05)
    return titles_en

# ========================= Upload (Overview uses these) =========================
if "tab" not in st.session_state:
    st.session_state.tab = "Overview"

col_up1, col_up2 = st.columns(2)
with col_up1:
    product_file = st.file_uploader("Product List (.xlsx/.csv) — include column W named 'thumbnail'", type=["xlsx","xls","csv"])
with col_up2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])

prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

if prod_df is not None:
    # Normalize/alias thumbnail before validation
    if "thumbnail" not in prod_df.columns:
        for alt in ["Thumbnail","image_url","ImageURL","image","img","Image Url","ImageURL"]:
            if alt in prod_df.columns:
                prod_df["thumbnail"] = prod_df[alt]
                break

if not (prod_df is not None and validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List")):
    st.stop()
if not (map_df is not None and validate_columns(map_df, REQUIRED_MAP_COLS, "Category Mapping")):
    st.stop()

# Working DataFrame persisted across tabs
if "work" not in st.session_state:
    st.session_state.work = prod_df.copy()
work = st.session_state.work.copy()
lookups = build_mapping_struct_fixed(map_df)

# ========================= SIDEBAR (Emerald) =========================
with st.sidebar:
    st.markdown("## ")
    # Nav buttons
    if st.button("Overview", key="btn_overview", use_container_width=True, help="Upload & image preview", type="secondary"):
        st.session_state.tab = "Overview"
    if st.button("Grouping", key="btn_grouping", use_container_width=True, help="Rules • Auto-Groups • Manual apply", type="secondary"):
        st.session_state.tab = "Grouping"
    if st.button("Sheet", key="btn_sheet", use_container_width=True, help="Review & download", type="secondary"):
        st.session_state.tab = "Sheet"

    st.markdown("<div class='small-caption'>Navigation</div>", unsafe_allow_html=True)
    st.divider()

    # Title Assistant (non-invasive)
    st.header("🖼️ Title Assistant")
    st.caption("EN from images (W:`thumbnail`) → optional AR translation.")

    max_len = st.slider("Max EN title length", 50, 90, 65, 5)
    engine = st.selectbox("Arabic translation engine", ["DeepL", "OpenAI", "None"], index=0)
    scope = st.selectbox("Scope", ["Only empty EN", "All rows"], index=0)
    force_regen = st.checkbox("Regenerate even if EN not empty", value=False)
    run_titles = st.button("Generate Titles & Translate", use_container_width=True)

    if st.checkbox("Show 12 thumbnails preview", value=False):
        if "thumbnail" in work.columns:
            urls = work["thumbnail"].fillna("").astype(str).tolist()
            show_n = min(12, len(urls))
            cols = st.columns(3)
            for i in range(show_n):
                img = fetch_thumb(urls[i])
                with cols[i % 3]:
                    if img: st.image(img, use_container_width=True)
                    else:   st.write("No image")

if run_titles:
    if scope == "Only empty EN" and not force_regen:
        idx = work[work["name"].astype(str).str.strip().eq("")].index
    else:
        idx = work.index
    if len(idx) == 0:
        st.info("No target rows to process.")
    else:
        titles_en = titles_from_images_batched(work, idx, max_len, batch_size=60, force_regen=force_regen)
        work.loc[idx, "name"] = titles_en.loc[idx]
        if engine != "None":
            out_all = []
            en_all = titles_en.loc[idx].astype(str).tolist()
            for s in range(0, len(en_all), 60):
                chunk = en_all[s:s+60]
                out_all.extend(translate_en_to_ar(chunk, engine))
                time.sleep(0.05)
            work.loc[idx, "name_ar"] = pd.Series(out_all, index=idx)
        st.success(f"Updated titles for {len(idx)} rows.")
        st.session_state.work = work.copy()

# ========================= TABS (Overview / Grouping / Sheet) =========================
tab = st.session_state.tab

# ---------- OVERVIEW ----------
if tab == "Overview":
    st.subheader("Overview")
    cA, cB = st.columns([2, 1])
    with cA:
        st.markdown("**Product sample (top 25)**")
        st.dataframe(
            work.head(25)[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
            use_container_width=True, height=420
        )
    with cB:
        st.markdown("**Images (first 12)**")
        if "thumbnail" in work.columns:
            urls = work["thumbnail"].fillna("").astype(str).tolist()
            show_n = min(12, len(urls))
            for i in range(show_n):
                img = fetch_thumb(urls[i])
                if img: st.image(img, use_container_width=True)
                else: st.write("No image")

# ---------- GROUPING ----------
elif tab == "Grouping":
    st.subheader("Find & Group")
    # Filters
    st.session_state.setdefault("search_q", "")
    st.session_state.setdefault("show_unmapped", False)

    def clear_filters():
        st.session_state["search_q"] = ""
        st.session_state["show_unmapped"] = False

    f1, f2, f3 = st.columns([3,1,1])
    with f1:
        st.text_input("Search by 'name' or 'name_ar' (e.g., Dishwashing / سائل):", key="search_q", placeholder="")
    with f2:
        st.button("Show all", on_click=clear_filters)
    with f3:
        st.checkbox("Show Unmapped Only", key="show_unmapped")

    q = st.session_state["search_q"].strip().lower()
    base_mask = (
        work["name"].astype(str).str.lower().str.contains(q, na=False) |
        work["name_ar"].astype(str).str.lower().str.contains(q, na=False)
    ) if q else pd.Series(True, index=work.index)

    if st.session_state["show_unmapped"]:
        mask = base_mask & (
            (work["sub_category_id"].astype(str).str.strip()=="") |
            (work["sub_sub_category_id"].astype(str).str.strip()=="")
        )
    else:
        mask = base_mask

    filtered = work[mask].copy()
    st.caption(f"Matched rows: {filtered.shape[0]}")
    st.dataframe(
        filtered[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
        use_container_width=True, height=320
    )

    # Manual apply to current filtered view
    st.markdown("**Manual apply to CURRENT filtered view**")
    if 'lookups' not in st.session_state:
        st.session_state.lookups = lookups
    sel_main = st.selectbox("Main Category", [""]+lookups["main_names"], key="manual_main")
    sel_sub  = st.selectbox("Sub Category", [""]+lookups["main_to_subnames"].get(sel_main,[]), key="manual_sub")
    sel_ssub = st.selectbox("Sub-Sub Category", [""]+lookups["pair_to_subsubnames"].get((sel_main,sel_sub),[]), key="manual_ssub")
    if st.button("Apply IDs to all rows in current filtered view"):
        if sel_main: work.loc[mask,"category_id"]=sel_main
        if sel_sub:  work.loc[mask,"sub_category_id"]=get_sub_no(lookups,sel_main,sel_sub)
        if sel_ssub: work.loc[mask,"sub_sub_category_id"]=get_ssub_no(lookups,sel_main,sel_sub,sel_ssub)
        st.session_state.work = work.copy()
        st.success("Applied IDs to current view.")

    # Keyword rules
    st.markdown("---")
    st.subheader("Keyword Groups")
    st.session_state.setdefault("keyword_rules", [])

    with st.expander("➕ Add keyword(s) + mapping"):
        colkw, colm, cols, colss = st.columns([3,2,2,2])
        with colkw:
            kws_text = st.text_area("Keywords (one per line)", placeholder="soap\nshampoo\ndetergent gel\ndishwashing")
        with colm:
            k_main = st.selectbox("Main", [""] + lookups["main_names"], key="kmain")
        subs_for_main = lookups["main_to_subnames"].get(k_main, []) if k_main else []
        with cols:
            k_sub = st.selectbox("Sub", [""] + subs_for_main, key="ksub")
        ssubs_for_pair = lookups["pair_to_subsubnames"].get((k_main, k_sub), []) if (k_main and k_sub) else []
        with colss:
            k_ssub = st.selectbox("Sub-Sub", [""] + ssubs_for_pair, key="kssub")
        if st.button("Add keywords"):
            kw_list = [k.strip() for k in kws_text.splitlines() if k.strip()]
            if not kw_list:
                st.warning("Enter at least one keyword.")
            elif not (k_main and k_sub and k_ssub):
                st.warning("Please pick Main, Sub, and Sub-Sub.")
            else:
                st.session_state.keyword_rules.append({
                    "keywords": kw_list,
                    "main": k_main,
                    "sub": k_sub,
                    "subsub": k_ssub
                })
                st.success(f"Added {len(kw_list)} keyword(s).")

    if st.session_state.keyword_rules:
        for idx_rule, rule in enumerate(st.session_state.keyword_rules):
            st.markdown(f"**Rule #{idx_rule+1}** — Main: `{rule['main']}` / Sub: `{rule['sub']}` / Sub-Sub: `{rule['subsub']}`")
            st.caption(f"Keywords: {', '.join(rule['keywords'])}")

            m = pd.Series(False, index=work.index)
            for kw in rule["keywords"]:
                kw_l = kw.lower()
                m = m | work["name"].astype(str).str.lower().str.contains(kw_l, na=False) | \
                        work["name_ar"].astype(str).str.lower().str.contains(kw_l, na=False)

            hits_df = work[m].copy()
            st.write(f"Matches found: {hits_df.shape[0]}")
            if hits_df.shape[0] > 0:
                st.dataframe(hits_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]],
                             use_container_width=True, height=280)
                default_skus = hits_df["merchant_sku"].astype(str).tolist()
                chosen_skus = st.multiselect(
                    f"Select SKUs to APPLY for Rule #{idx_rule+1}",
                    options=default_skus,
                    default=default_skus,
                    key=f"kw_sel_{idx_rule}"
                )
                if st.button(f"Apply Rule #{idx_rule+1} to selected"):
                    if not chosen_skus:
                        st.info("No SKUs selected.")
                    else:
                        apply_mask = work["merchant_sku"].astype(str).isin(chosen_skus)
                        work.loc[apply_mask,"category_id"] = rule["main"]
                        work.loc[apply_mask,"sub_category_id"] = get_sub_no(lookups, rule["main"], rule["sub"])
                        work.loc[apply_mask,"sub_sub_category_id"] = get_ssub_no(lookups, rule["main"], rule["sub"], rule["subsub"])
                        st.session_state.work = work.copy()
                        st.success(f"Applied mapping to {apply_mask.sum()} rows.")

        cclr, _ = st.columns([1,6])
        with cclr:
            if st.button("🗑️ Clear keyword rules"):
                st.session_state.keyword_rules = []
                st.experimental_rerun()
    else:
        st.info("Add keyword groups above to map big buckets fast.")

    # Auto-Groups by tokens
    st.markdown("---")
    st.subheader("Auto-Groups by feature tokens")
    STOP={"the","and","for","with","of","to","in","on","by","a","an","&","-","ml","g","kg","l","oz","pcs","pc","pack","pkt","ct","size","new","extra","x"}
    def tokenize(name): return [t for t in re.split(r"[^A-Za-z0-9]+", str(name).lower()) if t and t not in STOP and len(t)>2 and not t.isdigit()]
    use_entire=st.checkbox("Build groups from ALL rows (not just filtered)",value=False)
    source_df=work if use_entire else filtered
    tok_counts=Counter(); row_tokens={}
    for idx,row in source_df.iterrows():
        toks=set(tokenize(row.get("name",""))+tokenize(row.get("name_ar","")))
        row_tokens[idx]=toks; tok_counts.update(toks)
    min_count=st.slider("Minimum token frequency",3,30,8,1)
    candidates=[t for t,c in tok_counts.most_common() if c>=min_count][:30]
    if candidates:
        chosen_token=st.selectbox("Choose a token group",[""]+candidates)
        if chosen_token:
            group_idx=[i for i,toks in row_tokens.items() if chosen_token in toks]
            group_df=source_df.loc[group_idx].copy()
            st.write(f"Group **'{chosen_token}'** — {group_df.shape[0]} rows")
            st.dataframe(
                group_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]],
                use_container_width=True, height=280
            )
            preselect=group_df["merchant_sku"].astype(str).tolist()
            picked=st.multiselect("Select SKUs to apply mapping",options=preselect,default=preselect,key=f"tok_pick_{chosen_token}")
            apply_idx=group_df[group_df["merchant_sku"].astype(str).isin(picked)].index

            gm1,gm2,gm3=st.columns(3)
            g_main=gm1.selectbox("Main",[""]+lookups["main_names"], key=f"t_main_{chosen_token}")
            g_sub =gm2.selectbox("Sub",[""]+lookups["main_to_subnames"].get(g_main,[]), key=f"t_sub_{chosen_token}")
            g_ssub=gm3.selectbox("Sub-Sub",[""]+lookups["pair_to_subsubnames"].get((g_main,g_sub),[]), key=f"t_ssub_{chosen_token}")
            if st.button(f"Apply mapping to token group '{chosen_token}'"):
                if g_main and g_sub and g_ssub:
                    work.loc[apply_idx,"category_id"]=g_main
                    work.loc[apply_idx,"sub_category_id"]=get_sub_no(lookups,g_main,g_sub)
                    work.loc[apply_idx,"sub_sub_category_id"]=get_ssub_no(lookups,g_main,g_sub,g_ssub)
                    st.session_state.work = work.copy()
                    st.success(f"Applied mapping to {len(apply_idx)} rows.")
    else:
        st.info("No frequent tokens found. Adjust threshold or dataset.")

# ---------- SHEET ----------
elif tab == "Sheet":
    st.subheader("Sheet Review & Download")

    # Current filtered echo (use last search state)
    q = st.session_state.get("search_q","").strip().lower()
    base_mask = (
        work["name"].astype(str).str.lower().str.contains(q, na=False) |
        work["name_ar"].astype(str).str.lower().str.contains(q, na=False)
    ) if q else pd.Series(True, index=work.index)
    if st.session_state.get("show_unmapped", False):
        mask = base_mask & (
            (work["sub_category_id"].astype(str).str.strip()=="") |
            (work["sub_sub_category_id"].astype(str).str.strip()=="")
        )
    else:
        mask = base_mask
    filtered = work[mask].copy()

    st.subheader("Filtered view")
    st.dataframe(
        filtered[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
        use_container_width=True, height=320
    )
    st.subheader("Full sheet")
    st.dataframe(
        work[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
        use_container_width=True, height=320
    )

    st.download_button("⬇️ Download FULL Excel", to_excel_download(work), file_name="products_mapped.xlsx")
    st.download_button("⬇️ Download FILTERED Excel", to_excel_download(filtered), file_name="products_filtered.xlsx")
