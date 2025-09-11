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

lookups = build_lookups(map_df) if loaded_mapping_ok else {
    "main_names":[],
    "main_to_subnames":{},
    "pair_to_subsubnames":{},
    "sub_name_to_no_by_main":{},
    "ssub_name_to_no_by_main_sub":{}
}

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
