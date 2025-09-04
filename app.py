# ============== Uploads ==============
c1,c2 = st.columns(2)
with c1:
    product_file = st.file_uploader("Product List (.xlsx/.csv, includes 'thumbnail')", type=["xlsx","xls","csv"])
with c2:
    mapping_file = st.file_uploader("Category Mapping (.xlsx/.csv)", type=["xlsx","xls","csv"])

# Try fresh reads
prod_df = read_any_table(product_file) if product_file else None
map_df  = read_any_table(mapping_file) if mapping_file else None

# Persist once loaded
if prod_df is not None:
    st.session_state["prod_df_cached"] = prod_df.copy()
if map_df is not None:
    st.session_state["map_df_cached"] = map_df.copy()

# Fall back to cache when uploaders are empty on rerun
prod_df = prod_df if prod_df is not None else st.session_state.get("prod_df_cached")
map_df  = map_df  if map_df  is not None else st.session_state.get("map_df_cached")

# Validate gently. Do not st.stop() after first success.
loaded_ok = (
    isinstance(prod_df, pd.DataFrame) and
    validate_columns(prod_df, REQUIRED_PRODUCT_COLS, "Product List") and
    isinstance(map_df, pd.DataFrame) and
    validate_columns(map_df, ["category_id","sub_category_id","sub_category_id NO","sub_sub_category_id","sub_sub_category_id NO"], "Category Mapping")
)
if not loaded_ok:
    st.info("Upload both files to proceed.")
    st.stop()
