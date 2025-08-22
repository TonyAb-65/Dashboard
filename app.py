import io
import re
import time
import math
from typing import List
from urllib.parse import urlparse
from collections import Counter

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ---------- Page ----------
st.set_page_config(
    page_title="Product Mapping + Rules + Auto-Groups + AI Titles",
    layout="wide",
)

# ---------- Expected Product List columns ----------
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # image URL column W — fixed header 'thumbnail'
]

# ---------- API clients ----------
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


# ---------- File IO ----------
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


# ---------- Helpers ----------
def strip_markdown(s: str) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"[*_`]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tidy_title(s: str, max_chars: int = 70) -> str:
    s = strip_markdown(s)
    if len(s) <= max_chars: return s
    cut = s[:max_chars].rstrip()
    if " " in cut: cut = cut[: cut.rfind(" ")]
    return cut

def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def fetch_thumb(url: str, timeout=7):
    try:
        if not is_valid_url(url): return None
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()
        if "image" not in r.headers.get("Content-Type", "").lower() and not url.lower().endswith((".jpg",".jpeg",".png",".webp",".gif")):
            return None
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        return None

# ---------- OpenAI: image → EN short title ----------
def openai_title_from_image(url: str, max_chars: int) -> str:
    if not openai_active or not is_valid_url(url): return ""
    prompt = (
        "Return ONE short product TITLE only (6–8 words, max ~70 chars). "
        "Include brand if visible and size/count if obvious. "
        "No markdown, no emojis, no extra text."
    )
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system","content":"You are a precise e-commerce title writer. Output one short title only."},
                {"role":"user","content":[{"type":"text","text":prompt},{"type":"image_url","image_url":{"url":url}}]},
            ],
            temperature=0.2,
        )
        title = resp.choices[0].message.content or ""
        return tidy_title(title, max_chars)
    except Exception:
        return ""

# ---------- Translation ----------
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
        for s in range(0, len(texts), batch_size):
            chunk = texts[s:s+batch_size]
            out_all.extend(openai_translate_batch_en2ar(chunk))
            time.sleep(0.3)
        return pd.Series(out_all, index=titles_en.index)
    return titles_en.copy()

# ---------- Mapping ----------
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

# ---------- Excel download ----------
def to_excel_download(df, sheet_name="Products"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf

# ---------- Batched titles ----------
def titles_from_images_batched(df, row_index, max_chars, batch_size):
    if "thumbnail" not in df.columns:
        st.error("Column 'thumbnail' not found.")
        return pd.Series([""]*len(row_index), index=row_index, dtype="object")
    titles_en = pd.Series([""]*len(row_index), index=row_index, dtype="object")
    prog = st.progress(0)
    steps = max(1, math.ceil(len(row_index)/max(1,batch_size)))
    for step, start in enumerate(range(0,len(row_index),batch_size),1):
        chunk_idx = row_index[start:start+batch_size]
        for i in chunk_idx:
            url = str(df.loc[i,"thumbnail"])
            title = openai_title_from_image(url,max_chars) if url else ""
            titles_en.loc[i] = title or df.loc[i,"name"]
        prog.progress(min(step/steps,1.0))
        time.sleep(0.25)
    return titles_en

# ---------- UI ----------
st.title("🛒 Product Mapping + Rules + Auto-Groups + AI Titles")

# File upload
c1,c2 = st.columns(2)
with c1: product_file = st.file_uploader("Product List (.xlsx/.csv, must include 'thumbnail' in column W)", type=["xlsx","xls","csv"])
with c2: mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])
prod_df = read_any_table(product_file) if product_file else None
map_df = read_any_table(mapping_file) if mapping_file else None
if not (prod_df is not None and validate_columns(prod_df,REQUIRED_PRODUCT_COLS,"Product List")
        and map_df is not None and validate_columns(map_df,["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"],"Category Mapping")):
    st.stop()

if "work" not in st.session_state: st.session_state.work = prod_df.copy()
work = st.session_state.work
lookups = build_mapping_struct_fixed(map_df)

# ---------- Quick search & unmapped filter ----------
st.subheader("Quick search & manual apply")
st.session_state.setdefault("search_q",""); st.session_state.setdefault("show_unmapped",False)
def clear_search(): st.session_state["search_q"]=""; st.session_state["show_unmapped"]=False
col_top = st.columns([3,1,1])
with col_top[0]: st.text_input("Search by 'name' or 'name_ar':", key="search_q")
with col_top[1]: st.button("Show all", on_click=clear_search)
with col_top[2]: st.checkbox("Show Unmapped Only", key="show_unmapped")
q = st.session_state["search_q"].strip().lower()
base_mask = work["name"].astype(str).str.lower().str.contains(q,na=False) | work["name_ar"].astype(str).str.lower().str.contains(q,na=False) if q else pd.Series(True,index=work.index)
mask = base_mask & (((work["sub_category_id"].astype(str).str.strip()=="") | (work["sub_sub_category_id"].astype(str).str.strip()==""))) if st.session_state["show_unmapped"] else base_mask
filtered = work[mask].copy()

sel_main = st.selectbox("Main Category", [""]+lookups["main_names"])
sel_sub  = st.selectbox("Sub Category", [""]+lookups["main_to_subnames"].get(sel_main,[]))
sel_ssub = st.selectbox("Sub-Sub Category", [""]+lookups["pair_to_subsubnames"].get((sel_main,sel_sub),[]))
if st.button("Apply to all rows in current view"):
    if sel_main: work.loc[mask,"category_id"]=sel_main
    if sel_sub:  work.loc[mask,"sub_category_id"]=get_sub_no(lookups,sel_main,sel_sub)
    if sel_ssub: work.loc[mask,"sub_sub_category_id"]=get_ssub_no(lookups,sel_main,sel_sub,sel_ssub)
    st.success("Applied IDs to current view.")

# ---------- Auto-Groups by tokens ----------
st.divider(); st.subheader("Auto-Groups by feature tokens")
STOP={"the","and","for","with","of","to","in","on","by","a","an","&","-","ml","g","kg","l","oz","pcs","pc","pack"}
def tokenize(name): return [t for t in re.split(r"[^A-Za-z0-9]+", str(name).lower()) if t and t not in STOP and len(t)>2 and not t.isdigit()]
use_entire=st.checkbox("Group from ALL rows",value=False)
source_df=work if use_entire else filtered
tok_counts=Counter(); row_tokens={}
for idx,row in source_df.iterrows():
    toks=set(tokenize(row.get("name",""))+tokenize(row.get("name_ar","")))
    row_tokens[idx]=toks; tok_counts.update(toks)
min_count=st.slider("Minimum token frequency",3,30,8,1)
candidates=[t for t,c in tok_counts.most_common() if c>=min_count][:20]
if candidates:
    chosen_token=st.selectbox("Choose a token group",[""]+candidates)
    if chosen_token:
        group_idx=[i for i,toks in row_tokens.items() if chosen_token in toks]
        group_df=source_df.loc[group_idx]
        st.write(f"Group '{chosen_token}' — {group_df.shape[0]} rows")
        preselect=group_df["merchant_sku"].astype(str).tolist()
        picked=st.multiselect("Select SKUs to apply mapping",options=preselect,default=preselect)
        apply_idx=group_df[group_df["merchant_sku"].astype(str).isin(picked)].index
        gm1,gm2,gm3=st.columns(3)
        g_main=gm1.selectbox("Main",[""]+lookups["main_names"])
        g_sub =gm2.selectbox("Sub",[""]+lookups["main_to_subnames"].get(g_main,[]))
        g_ssub=gm3.selectbox("Sub-Sub",[""]+lookups["pair_to_subsubnames"].get((g_main,g_sub),[]))
        if st.button(f"Apply mapping to group '{chosen_token}'"):
            if g_main and g_sub and g_ssub:
                work.loc[apply_idx,"category_id"]=g_main
                work.loc[apply_idx,"sub_category_id"]=get_sub_no(lookups,g_main,g_sub)
                work.loc[apply_idx,"sub_sub_category_id"]=get_ssub_no(lookups,g_main,g_sub,g_ssub)
                st.success(f"Applied mapping to {len(apply_idx)} rows.")
else:
    st.info("No frequent tokens found. Adjust threshold or dataset.")

# ---------- AI Titles ----------
st.divider(); st.subheader("AI Titles from Images → EN → AR")
max_len=st.slider("Max title length",50,90,70,5)
batch_size=st.slider("Batch size",25,100,50,5)
engine=st.selectbox("Arabic translation engine",["DeepL","OpenAI","None"])
scope=st.selectbox("Scope",["Filtered rows","All rows"])
if st.button("Generate Titles & Translations"):
    idx_list=filtered.index.tolist() if scope=="Filtered rows" else work.index.tolist()
    titles_en=titles_from_images_batched(work,idx_list,max_len,batch_size)
    work.loc[idx_list,"name"]=titles_en.loc[idx_list]
    work.loc[idx_list,"name_ar"]=translate_en_titles(titles_en.loc[idx_list],engine,batch_size)
    st.success("Updated product titles.")

# ---------- Downloads ----------
st.subheader("Download")
st.download_button("⬇️ Download FULL Excel",to_excel_download(work),file_name="products_mapped.xlsx")
st.download_button("⬇️ Download FILTERED Excel",to_excel_download(filtered),file_name="products_filtered.xlsx")
