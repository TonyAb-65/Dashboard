import io
import re
import time
import math
from typing import List
from urllib.parse import urlparse, urlsplit, urlunsplit, quote
from collections import Counter

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ---------- Page ----------
st.set_page_config(
    page_title="Product Mapper: Images → Titles → Groups",
    layout="wide",
)

# ---------- Expected Product List columns ----------
REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku",
    "category_id", "category_id_ar",
    "sub_category_id", "sub_sub_category_id",
    # Column W must be named exactly:
    "thumbnail",
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
    # ---------- Key status ----------
st.sidebar.markdown("### 🔑 API Key Status")
st.sidebar.write("DeepL:", "✅ Active" if deepl_active else "❌ Missing/Invalid")
st.sidebar.write("OpenAI:", "✅ Active" if openai_active else "❌ Missing/Invalid")


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

from urllib.parse import urlsplit, urlunsplit, quote

def is_valid_url(u: str) -> bool:
    if not isinstance(u, str): return False
    u = u.strip().strip('"').strip("'")
    try:
        p = urlsplit(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def _normalize_url(u: str) -> str:
    u = (u or "").strip().strip('"').strip("'")
    p = urlsplit(u)
    path = quote(p.path, safe="/:%@&?=#,+!$;'()*[]")
    if p.query:
        parts = []
        for kv in p.query.split("&"):
            if kv == "": continue
            if "=" in kv:
                k, v = kv.split("=", 1)
                parts.append(f"{quote(k, safe=':/@')}={quote(v, safe=':/@')}")
            else:
                parts.append(quote(kv, safe=":/@"))
        query = "&".join(parts)
    else:
        query = ""
    return urlunsplit((p.scheme, p.netloc, path, query, p.fragment))

def fetch_thumb(url: str, timeout=10, max_bytes=8_000_000):
    try:
        if not is_valid_url(url): return None
        url = _normalize_url(url)

        origin = urlsplit(url).netloc
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": f"https://{origin}",
        }

        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True, stream=True)
        r.raise_for_status()

        content_length = r.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            return None

        data = r.content if r.content else r.raw.read(max_bytes + 1)
        if len(data) > max_bytes:
            return None

        img = Image.open(BytesIO(data)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
            r.raise_for_status()
            data = r.content[:max_bytes+1]
            if len(data) > max_bytes: return None
            img = Image.open(BytesIO(data)).convert("RGB")
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
                {"role":"user","content":[{"type":"text","text":prompt},{"type":"image_url","image_url":{"url":_normalize_url(url)}}]},
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

# ---------- Batched titles (kept for reuse if needed) ----------
def titles_from_images_batched(df, row_index, max_chars, batch_size):
    titles_en = pd.Series([""]*len(row_index), index=row_index, dtype="object")
    prog = st.progress(0)
    steps = max(1, math.ceil(len(row_index)/max(1,batch_size)))
    for step, start in enumerate(range(0,len(row_index),batch_size),1):
        chunk_idx = row_index[start:start+batch_size]
        for i in chunk_idx:
            url = str(df.loc[i,"thumbnail"]) if "thumbnail" in df.columns else ""
            title = openai_title_from_image(url,max_chars) if url else ""
            titles_en.loc[i] = title or df.loc[i,"name"]
        prog.progress(min(step/steps,1.0))
        time.sleep(0.15)
    return titles_en


# ---------- UI ----------
st.title("🛒 Product Mapper: Images → EN Title → AR → Grouping")

# Upload files
c1,c2 = st.columns(2)
with c1: product_file = st.file_uploader("Product List (.xlsx/.csv, includes column W named 'thumbnail')", type=["xlsx","xls","csv"])
with c2: mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])

prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

if not (prod_df is not None and validate_columns(prod_df,REQUIRED_PRODUCT_COLS,"Product List")
        and map_df is not None and validate_columns(map_df,["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"],"Category Mapping")):
    st.stop()

if "work" not in st.session_state:
    st.session_state.work = prod_df.copy()

work = st.session_state.work
lookups = build_mapping_struct_fixed(map_df)

# ---------- 1) Manual: fetch image → generate English title → optional Arabic ----------
st.header("1) Manual: fetch image → generate English title → optional Arabic")

max_len = st.slider("Max title length", 50, 90, 70, 5)
engine  = st.selectbox("Arabic translation engine", ["DeepL", "OpenAI", "None"])
st.caption("Flow: 1) Preview images. 2) Select rows. 3) Generate titles. 4) Optional translate.")

# ---------- 2) Quick Search & Filtering BEFORE grouping ----------
st.header("2) Filter view")
st.session_state.setdefault("search_q","")
st.session_state.setdefault("show_unmapped",False)

def clear_filters():
    st.session_state["search_q"] = ""
    st.session_state["show_unmapped"] = False

f1,f2,f3 = st.columns([3,1,1])
with f1:
    st.text_input("Search by 'name' or 'name_ar':", key="search_q", placeholder="e.g., dishwashing / صابون")
with f2:
    st.button("Show all", on_click=clear_filters)
with f3:
    st.checkbox("Show Unmapped Only", key="show_unmapped")

q = st.session_state["search_q"].strip().lower()
base_mask = (
    work["name"].astype(str).str.lower().str.contains(q,na=False) |
    work["name_ar"].astype(str).str.lower().str.contains(q,na=False)
) if q else pd.Series(True,index=work.index)

if st.session_state["show_unmapped"]:
    mask = base_mask & (
        (work["sub_category_id"].astype(str).str.strip()=="") |
        (work["sub_sub_category_id"].astype(str).str.strip()=="")
    )
else:
    mask = base_mask

filtered = work[mask].copy()
st.caption(f"Rows in current filtered view: {filtered.shape[0]}")

# A) Preview images (manual trigger, browser fetch)
st.subheader("A) Preview images (manual)")
st.caption("Click to fetch thumbnails in your browser. Nothing loads automatically.")
if st.button("Preview first 24 images in CURRENT filtered view"):
    view = filtered.head(24).copy()
    if "thumbnail" in view.columns and len(view) > 0:
        cols = st.columns(6)
        for j, (i, row) in enumerate(view.iterrows()):
            with cols[j % 6]:
                url = _normalize_url(str(row.get("thumbnail", "")))
                if is_valid_url(url):
                    # Let the browser fetch the URL directly
                    st.image(url, caption=f"Row {i}", use_container_width=True)
                else:
                    st.write("No image / bad URL")
    else:
        st.info("No thumbnails in current filtered view.")

with st.expander("Image diagnostics (manual)"):
    st.caption("Helps confirm URLs are valid and reachable.")
    if st.button("Show first 10 normalized URLs"):
        sample = filtered["thumbnail"].astype(str).head(10).tolist() if "thumbnail" in filtered.columns else []
        for u in sample:
            norm = _normalize_url(u)
            st.write({"raw": u, "normalized": norm, "valid": is_valid_url(norm)})

# B) Pick rows to generate titles from image
st.subheader("B) Pick rows to generate titles from image")
sku_opts = filtered["merchant_sku"].astype(str).tolist()
sel_skus = st.multiselect("Select SKUs to process", options=sku_opts, default=sku_opts)

# C) Generate titles ONLY for selected rows
if st.button("Generate EN titles for selected rows (from image)"):
    if not openai_active:
        st.error("OpenAI client inactive. Add OPENAI_API_KEY in secrets.")
    elif not sel_skus:
        st.info("No SKUs selected.")
    else:
        idx = work[work["merchant_sku"].astype(str).isin(sel_skus)].index
        updated = 0
        prog = st.progress(0.0)
        for k, i in enumerate(idx, 1):
            url = str(work.at[i, "thumbnail"]) if "thumbnail" in work.columns else ""
            title = openai_title_from_image(url, max_len) if url else ""
            if title:
                work.at[i, "name"] = title
                updated += 1
            prog.progress(k / len(idx))
            time.sleep(0.05)
        st.success(f"Generated titles for {updated} row(s).")

# D) Optional translate ONLY for selected rows
if st.button("Translate selected rows' EN titles → AR"):
    if engine == "None":
        st.info("Translation engine set to None.")
    else:
        idx = work[work["merchant_sku"].astype(str).isin(sel_skus)].index
        titles_en = work.loc[idx, "name"].fillna("").astype(str)
        trans = translate_en_titles(titles_en, engine, batch_size=max(20, len(idx)//2 or 20))
        work.loc[idx, "name_ar"] = trans
        st.success(f"Translated {len(idx)} row(s) to Arabic.")

# Show filtered table
st.dataframe(
    filtered[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id","thumbnail"]],
    use_container_width=True, height=320
)

# ---------- 3) Grouping & bulk apply ----------
st.header("3) Grouping & bulk apply")

# Session stores
st.session_state.setdefault("keyword_rules", [])
st.session_state.setdefault("keyword_library", [])

# ----- 3A. Keyword Groups with library -----
st.subheader("3A) Keyword groups (add once, reuse from dropdown)")

with st.expander("➕ Add or reuse keyword(s) and mapping"):
    lib_col, map_col = st.columns([3,4])

    with lib_col:
        st.markdown("**Keyword library**")
        new_kws_text = st.text_area("Add keywords (one per line)", placeholder="soap\nshampoo\ndetergent gel\ndishwashing")
        if st.button("Add to library"):
            fresh = [k.strip() for k in new_kws_text.splitlines() if k.strip()]
            if fresh:
                existing = set(st.session_state.keyword_library)
                st.session_state.keyword_library.extend([k for k in fresh if k not in existing])
                st.success(f"Added {len(fresh)} keyword(s) to library.")
            else:
                st.info("Nothing to add.")

        lib_selected = st.multiselect(
            "Pick keywords from library",
            options=st.session_state.keyword_library,
            default=st.session_state.keyword_library,
            key="lib_pick_for_rule"
        )

        to_remove = st.multiselect("Remove from library", options=st.session_state.keyword_library, key="lib_remove")
        if st.button("🗑️ Remove selected from library"):
            if to_remove:
                st.session_state.keyword_library = [k for k in st.session_state.keyword_library if k not in set(to_remove)]
                st.success(f"Removed {len(to_remove)} keyword(s).")
            else:
                st.info("No keywords selected to remove.")

    with map_col:
        st.markdown("**Mapping for selected keywords**")
        k_main = st.selectbox("Main", [""] + lookups["main_names"], key="kmain")
        subs_for_main = lookups["main_to_subnames"].get(k_main, []) if k_main else []
        k_sub = st.selectbox("Sub", [""] + subs_for_main, key="ksub")
        ssubs_for_pair = lookups["pair_to_subsubnames"].get((k_main, k_sub), []) if (k_main and k_sub) else []
        k_ssub = st.selectbox("Sub-Sub", [""] + ssubs_for_pair, key="kssub")

        if st.button("Add keywords as a new rule"):
            chosen_kws = lib_selected or [k.strip() for k in new_kws_text.splitlines() if k.strip()]
            if not chosen_kws:
                st.warning("Select keywords from library or add some in the textarea.")
            elif not (k_main and k_sub and k_ssub):
                st.warning("Pick Main, Sub, and Sub-Sub.")
            else:
                st.session_state.keyword_rules.append({
                    "keywords": chosen_kws,
                    "main": k_main,
                    "sub": k_sub,
                    "subsub": k_ssub
                })
                st.success(f"Added rule with {len(chosen_kws)} keyword(s).")

# Render rules and apply
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
            st.dataframe(hits_df[["merchant_sku","name","name_ar","category_id","sub_category_id","sub_sub_category_id"]], use_container_width=True, height=280)

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
                    st.success(f"Applied mapping to {apply_mask.sum()} rows.")

    cclr, _ = st.columns([1,6])
    with cclr:
        if st.button("🗑️ Clear keyword rules"):
            st.session_state.keyword_rules = []
            st.experimental_rerun()
else:
    st.info("Add keyword groups above to map big buckets fast.")

# ----- 3B. Auto-Groups by feature tokens (also shows table) -----
st.subheader("3B) Auto-Groups by feature tokens (plastic, sensitive, lemon, baby, etc.)")
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
                st.success(f"Applied mapping to {len(apply_idx)} rows.")
else:
    st.info("No frequent tokens found. Adjust threshold or dataset.")

# ----- 3C. Manual one-shot apply on current filtered view -----
st.subheader("3C) Manual apply to CURRENT filtered view")
sel_main = st.selectbox("Main Category", [""]+lookups["main_names"], key="manual_main")
sel_sub  = st.selectbox("Sub Category", [""]+lookups["main_to_subnames"].get(sel_main,[]), key="manual_sub")
sel_ssub = st.selectbox("Sub-Sub Category", [""]+lookups["pair_to_subsubnames"].get((sel_main,sel_sub),[]), key="manual_ssub")
if st.button("Apply IDs to all rows in current filtered view"):
    if sel_main: work.loc[mask,"category_id"]=sel_main
    if sel_sub:  work.loc[mask,"sub_category_id"]=get_sub_no(lookups,sel_main,sel_sub)
    if sel_ssub: work.loc[mask,"sub_sub_category_id"]=get_ssub_no(lookups,sel_main,sel_sub,sel_ssub)
    st.success("Applied IDs to current view.")

# ---------- 4) Full & Filtered Excel views + Downloads ----------
st.header("4) Review & Download")
st.subheader("Filtered view (again)")
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
