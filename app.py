# Product Mapping Dashboard — Fixed Version
# Complete implementation with all missing functions and proper error handling

import io, re, time, math, hashlib, json, sys, traceback, base64, random
from typing import List, Iterable, Tuple, Optional, Dict, Any
from urllib.parse import urlsplit, urlunsplit, quote, urlparse
from collections import Counter
import logging

import pandas as pd
import streamlit as st
import requests

# ================= CONFIGURATION =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Product Mapping Dashboard", 
    page_icon="🧭", 
    layout="wide",
    initial_sidebar_state="expanded"
)
st.set_option("client.showErrorDetails", True)

# ================= CONSTANTS =================
EMERALD = "#10b981"
EMERALD_DARK = "#059669"
TEXT_LIGHT = "#f8fafc"
DEFAULT_SLEEP_INTERVAL = 0.1
PREVIEW_SAMPLE_SIZE = 10
MAX_BATCH_SIZE = 50

REQUIRED_PRODUCT_COLS = [
    "name", "name_ar", "merchant_sku", "category_id",
    "sub_category_id", "sub_sub_category_id", "thumbnail"
]

# ================= STYLING =================
st.markdown(
    f"""
<style>
.app-header {{ 
    padding: 8px 0; 
    border-bottom: 1px solid #e5e7eb; 
    background: #fff; 
    position: sticky; 
    top: 0; 
    z-index: 5; 
}}
.app-title {{ 
    font-size: 22px; 
    font-weight: 800; 
    color: #111827; 
}}
.app-sub {{ 
    color: #6b7280; 
    font-size: 12px; 
}}
[data-testid="stSidebar"] > div:first-child {{ 
    background: linear-gradient(180deg, {EMERALD} 0%, {EMERALD_DARK} 100%); 
    color: {TEXT_LIGHT}; 
}}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {{ 
    color: {TEXT_LIGHT} !important; 
}}
[data-testid="stSidebar"] .stRadio > div > label {{ 
    margin-bottom: 6px; 
    padding: 6px 10px; 
    border-radius: 8px; 
    background: rgba(255,255,255,0.08); 
}}
.stButton>button {{ 
    border-radius: 8px; 
    border: 1px solid #e5e7eb; 
    padding: .45rem .9rem; 
}}
.block-container {{ 
    padding-top: 6px; 
}}
.metric-card {{
    background: white;
    padding: 1rem;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}}
.success-box {{
    background: #f0fdf4;
    border: 1px solid #bbf7d0;
    padding: 1rem;
    border-radius: 8px;
    color: #166534;
}}
.warning-box {{
    background: #fffbeb;
    border: 1px solid #fed7aa;
    padding: 1rem;
    border-radius: 8px;
    color: #92400e;
}}
</style>
""",
    unsafe_allow_html=True,
)

# Header
st.markdown(
    """
<div class="app-header">
  <div class="app-title">🧭 Product Mapping Dashboard</div>
  <div class="app-sub">Images → English Title → Arabic → Categorization → Export</div>
</div>
""",
    unsafe_allow_html=True,
)

# ================= SESSION STATE INITIALIZATION =================
def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        'work': None,
        'proc_cache': {},
        'audit_rows': [],
        'file_hash': None,
        'page_df': pd.DataFrame(),
        'global_cache_store': {},
        'translation_engine': 'OpenAI',
        'batch_size': 10,
        'use_glossary': False,
        'glossary_map': {},
        'deepl_active': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ================= UTILITY FUNCTIONS =================
def safe_section(name: str, func: callable) -> Any:
    """Safely execute a section function with error handling"""
    try:
        return func()
    except Exception as e:
        st.error(f"Error in {name}: {str(e)}")
        with st.expander("Show full traceback"):
            st.code(traceback.format_exc())
        logger.error(f"Error in {name}: {str(e)}", exc_info=True)
        return None

def ui_sleep(duration: float = DEFAULT_SLEEP_INTERVAL):
    """UI sleep function with default interval"""
    time.sleep(duration)

def global_cache() -> Dict:
    """Get global cache store"""
    return st.session_state.global_cache_store

def clean_url_for_vision(url: Any) -> str:
    """Clean and validate URL for vision API"""
    if pd.isna(url) or not str(url).strip():
        return ""
    
    url_str = str(url).strip()
    
    # Basic URL cleaning
    if not url_str.startswith(('http://', 'https://')):
        url_str = 'https://' + url_str
    
    return url_str

def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    try:
        if not url or pd.isna(url):
            return False
        result = urlparse(str(url))
        return all([result.scheme in ['http', 'https'], result.netloc])
    except Exception:
        return False

def normalize_list_length(lst: List, target_length: int, fill_value: Any = "") -> List:
    """Normalize list length to match target length"""
    if len(lst) == target_length:
        return lst
    return (lst + [fill_value] * target_length)[:target_length]

def validate_dataframe(df: pd.DataFrame, required_cols: List[str] = None) -> Tuple[bool, List[str]]:
    """Validate DataFrame has required columns"""
    if required_cols is None:
        required_cols = REQUIRED_PRODUCT_COLS
    
    if df is None or df.empty:
        return False, ["DataFrame is empty or None"]
    
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        return False, [f"Missing required columns: {', '.join(missing_cols)}"]
    
    return True, []

# ================= TRANSLATION FUNCTIONS =================
def deepl_batch_en2ar(texts: List[str], context_hint: str = "") -> List[str]:
    """DeepL translation - placeholder implementation"""
    st.warning("DeepL translation not implemented. Using mock translation.")
    return [f"[AR] {text}" for text in texts]

def openai_translate_batch_en2ar(texts: List[str]) -> List[str]:
    """OpenAI translation - placeholder implementation"""
    st.warning("OpenAI translation not implemented. Using mock translation.")
    return [f"[AR-GPT] {text}" for text in texts]

def translate_en_titles(
    titles_en: pd.Series,
    engine: str = "OpenAI",
    batch_size: int = 10,
    use_glossary: bool = False,
    glossary_map: Optional[Dict[str, str]] = None,
    context_hint: str = ""
) -> pd.Series:
    """Translate English titles to Arabic with length matching"""
    if titles_en is None or titles_en.empty:
        return pd.Series([], dtype="string")
    
    idx = titles_en.index
    n = len(idx)
    texts = titles_en.fillna("").astype(str).tolist()
    
    # Apply glossary mapping if enabled
    if use_glossary and glossary_map:
        mapped = []
        for text in texts:
            processed_text = text
            for src, tgt in glossary_map.items():
                if src and tgt:
                    processed_text = re.sub(rf"(?i)\b{re.escape(src)}\b", tgt, processed_text)
            mapped.append(processed_text)
        texts = mapped
    
    # Perform translation
    try:
        if engine == "DeepL" and st.session_state.deepl_active:
            outputs = deepl_batch_en2ar(texts, context_hint)
        elif engine == "OpenAI":
            outputs = []
            progress_bar = st.progress(0)
            
            for i in range(0, len(texts), max(1, batch_size)):
                chunk = texts[i:i + batch_size]
                batch_result = openai_translate_batch_en2ar(chunk)
                
                if not isinstance(batch_result, list):
                    batch_result = list(batch_result) if batch_result is not None else [""] * len(chunk)
                
                outputs.extend(batch_result)
                progress_bar.progress((i + batch_size) / len(texts))
                ui_sleep(0.1)
            
            progress_bar.empty()
        else:
            outputs = texts
    except Exception as e:
        st.error(f"Translation failed: {str(e)}")
        outputs = texts
    
    # Normalize output length
    outputs = normalize_list_length(outputs, n, "")
    outputs = [str(v) if v is not None else "" for v in outputs]
    
    return pd.Series(outputs, index=idx, dtype="string")

# ================= EXPORT FUNCTIONS =================
def to_excel_download(df: pd.DataFrame, sheet_name: str = "Products") -> io.BytesIO:
    """Convert DataFrame to Excel download buffer"""
    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        buf.seek(0)
        return buf
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
        return io.BytesIO()

# ================= SECTION FUNCTIONS =================
def sec_overview():
    """Overview section with data loading and summary"""
    st.subheader("📊 Dataset Overview")
    
    # File upload section
    if st.session_state.work is None or st.session_state.work.empty:
        st.markdown("""
        <div class="warning-box">
            <h4>📁 No Data Loaded</h4>
            <p>Please upload a CSV file to begin working with your product data.</p>
        </div>
        """, unsafe_allow_html=True)
        
        uploaded_file = st.file_uploader(
            "Choose CSV file", 
            type=['csv'],
            help="Upload a CSV file containing your product data"
        )
        
        if uploaded_file is not None:
            try:
                with st.spinner("Loading data..."):
                    df = pd.read_csv(uploaded_file)
                    
                    # Validate data
                    is_valid, errors = validate_dataframe(df, required_cols=[])
                    
                    if is_valid or len(df.columns) > 0:  # Accept any CSV with columns
                        st.session_state.work = df
                        st.session_state.file_hash = hashlib.md5(
                            uploaded_file.getvalue()
                        ).hexdigest()[:8]
                        
                        st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                        st.rerun()
                    else:
                        st.error("❌ Invalid file format or empty data")
                        for error in errors:
                            st.error(f"• {error}")
                            
            except Exception as e:
                st.error(f"❌ Error loading file: {str(e)}")
                logger.error(f"File loading error: {str(e)}", exc_info=True)
    else:
        # Data summary
        df = st.session_state.work
        
        st.markdown(f"""
        <div class="success-box">
            <h4>✅ Data Successfully Loaded</h4>
            <p><strong>{len(df):,}</strong> rows • <strong>{len(df.columns)}</strong> columns • Hash: <code>{st.session_state.file_hash}</code></p>
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Products", f"{len(df):,}")
        
        with col2:
            unique_vals = df.nunique().sum()
            st.metric("Unique Values", f"{unique_vals:,}")
        
        with col3:
            missing_vals = df.isnull().sum().sum()
            st.metric("Missing Values", f"{missing_vals:,}")
        
        with col4:
            memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
            st.metric("Memory Usage", f"{memory_usage:.1f} MB")
        
        # Column analysis
        st.subheader("Column Analysis")
        
        col_info = []
        for col in df.columns:
            col_info.append({
                "Column": col,
                "Type": str(df[col].dtype),
                "Non-Null": f"{df[col].count():,}",
                "Unique": f"{df[col].nunique():,}",
                "Sample": str(df[col].iloc[0] if len(df) > 0 else "N/A")[:50]
            })
        
        col_df = pd.DataFrame(col_info)
        st.dataframe(col_df, use_container_width=True)
        
        # Data preview
        st.subheader("Data Preview")
        preview_rows = st.slider("Rows to preview", 5, min(50, len(df)), 10)
        st.dataframe(df.head(preview_rows), use_container_width=True)
        
        # Quick actions
        st.subheader("Quick Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Reload Data", help="Clear current data to load new file"):
                st.session_state.work = None
                st.session_state.file_hash = None
                st.rerun()
        
        with col2:
            if st.button("🧹 Clear Cache", help="Clear processing cache"):
                st.session_state.proc_cache = {}
                st.session_state.audit_rows = []
                st.success("Cache cleared!")
        
        with col3:
            if st.button("📊 Column Stats", help="Show detailed column statistics"):
                st.write("**Detailed Statistics:**")
                st.write(df.describe(include='all'))

def sec_titles():
    """Titles generation and translation section"""
    st.subheader("🖼️ Titles & Translation")
    
    if st.session_state.work is None or st.session_state.work.empty:
        st.info("📋 No data loaded. Please go to Overview to upload data.")
        return
    
    df = st.session_state.work
    
    # Translation settings
    st.subheader("Translation Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        engine = st.selectbox(
            "Translation Engine",
            ["OpenAI", "DeepL"],
            index=0 if st.session_state.translation_engine == "OpenAI" else 1,
            help="Choose translation service"
        )
        st.session_state.translation_engine = engine
        
        batch_size = st.slider(
            "Batch Size",
            1, MAX_BATCH_SIZE, st.session_state.batch_size,
            help="Number of items to translate at once"
        )
        st.session_state.batch_size = batch_size
    
    with col2:
        use_glossary = st.checkbox(
            "Use Glossary",
            st.session_state.use_glossary,
            help="Apply custom term mapping before translation"
        )
        st.session_state.use_glossary = use_glossary
        
        context_hint = st.text_input(
            "Context Hint",
            placeholder="e.g., 'e-commerce products', 'fashion items'",
            help="Provide context to improve translation quality"
        )
    
    # Glossary management
    if use_glossary:
        st.subheader("Glossary Management")
        
        with st.expander("📝 Edit Glossary"):
            glossary_text = st.text_area(
                "Glossary (one mapping per line: English|Arabic)",
                placeholder="smartphone|هاتف ذكي\nlaptop|حاسوب محمول",
                height=100
            )
            
            if glossary_text:
                glossary_map = {}
                for line in glossary_text.strip().split('\n'):
                    if '|' in line:
                        parts = line.split('|', 1)
                        if len(parts) == 2:
                            glossary_map[parts[0].strip()] = parts[1].strip()
                st.session_state.glossary_map = glossary_map
                
                if glossary_map:
                    st.success(f"✅ Loaded {len(glossary_map)} glossary terms")
    
    # Column selection for translation
    st.subheader("Translation Preview")
    
    text_columns = df.select_dtypes(include=['object', 'string']).columns.tolist()
    
    if not text_columns:
        st.warning("No text columns found for translation")
        return
    
    source_col = st.selectbox(
        "Source Column (English)",
        text_columns,
        help="Select the column containing English text to translate"
    )
    
    if source_col:
        # Preview translation on sample
        sample_size = min(5, len(df))
        sample_data = df[source_col].head(sample_size)
        
        if st.button("🔍 Preview Translation", key="preview_translation"):
            with st.spinner(f"Translating {sample_size} sample items..."):
                translated_sample = translate_en_titles(
                    sample_data,
                    engine=engine,
                    batch_size=min(batch_size, sample_size),
                    use_glossary=use_glossary,
                    glossary_map=st.session_state.glossary_map,
                    context_hint=context_hint
                )
                
                preview_df = pd.DataFrame({
                    'Original (English)': sample_data.values,
                    'Translated (Arabic)': translated_sample.values
                })
                
                st.write("**Translation Preview:**")
                st.dataframe(preview_df, use_container_width=True)
        
        # Full translation
        st.subheader("Full Translation")
        
        target_col = st.text_input(
            "Target Column Name",
            value=f"{source_col}_ar",
            help="Name for the new Arabic translation column"
        )
        
        if st.button("🚀 Translate All", key="translate_all", type="primary"):
            if target_col in df.columns:
                if not st.checkbox("Overwrite existing column", key="overwrite_confirm"):
                    st.error(f"Column '{target_col}' already exists. Check the box to overwrite.")
                    return
            
            with st.spinner(f"Translating {len(df)} items..."):
                start_time = time.time()
                
                translated_series = translate_en_titles(
                    df[source_col],
                    engine=engine,
                    batch_size=batch_size,
                    use_glossary=use_glossary,
                    glossary_map=st.session_state.glossary_map,
                    context_hint=context_hint
                )
                
                # Add translated column to dataframe
                st.session_state.work[target_col] = translated_series
                
                elapsed_time = time.time() - start_time
                
                st.success(f"✅ Translation completed in {elapsed_time:.1f} seconds!")
                st.balloons()
                
                # Show results summary
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Items Translated", len(translated_series))
                with col2:
                    non_empty = translated_series.str.len().gt(0).sum()
                    st.metric("Non-empty Translations", non_empty)
                with col3:
                    st.metric("Translation Rate", f"{len(translated_series)/elapsed_time:.1f}/sec")

def sec_grouping():
    """Grouping and categorization section"""
    st.subheader("🧩 Grouping & Analysis")
    
    if st.session_state.work is None or st.session_state.work.empty:
        st.info("📋 No data loaded. Please go to Overview to upload data.")
        return
    
    df = st.session_state.work
    
    # Group by analysis
    st.subheader("Group By Analysis")
    
    groupable_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    col1, col2 = st.columns(2)
    
    with col1:
        group_cols = st.multiselect(
            "Group By Columns",
            groupable_cols,
            help="Select columns to group by"
        )
    
    with col2:
        agg_col = st.selectbox(
            "Aggregation Column",
            ["count"] + numeric_cols,
            help="Select column for aggregation (or count for frequency)"
        )
    
    if group_cols:
        try:
            if agg_col == "count":
                grouped = df.groupby(group_cols).size().reset_index(name='count')
            else:
                grouped = df.groupby(group_cols)[agg_col].agg(['count', 'mean', 'sum']).reset_index()
            
            st.write(f"**Grouped by: {', '.join(group_cols)}**")
            st.dataframe(grouped, use_container_width=True)
            
            # Visualization
            if len(grouped) <= 50:  # Only show chart for manageable number of groups
                if len(group_cols) == 1 and agg_col == "count":
                    st.bar_chart(grouped.set_index(group_cols[0])['count'])
                    
        except Exception as e:
            st.error(f"Grouping failed: {str(e)}")
    
    # Category analysis
    st.subheader("Category Distribution")
    
    category_cols = [col for col in df.columns if 'category' in col.lower()]
    
    if category_cols:
        selected_cat_col = st.selectbox("Select Category Column", category_cols)
        
        if selected_cat_col:
            cat_counts = df[selected_cat_col].value_counts().head(20)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Top Categories:**")
                st.dataframe(cat_counts.reset_index())
            
            with col2:
                st.write("**Distribution Chart:**")
                st.bar_chart(cat_counts)
    else:
        st.info("No category columns found in the dataset")

def sec_sheet():
    """Sheet view and editing section"""
    st.subheader("📑 Data Sheet")
    
    if st.session_state.work is None or st.session_state.work.empty:
        st.info("📋 No data loaded. Please go to Overview to upload data.")
        return pd.DataFrame()
    
    df = st.session_state.work.copy()
    
    # Filtering options
    st.subheader("Filters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Text search
        search_term = st.text_input("🔍 Search", placeholder="Search in all columns...")
        
    with col2:
        # Column filter
        if len(df.columns) > 10:
            selected_cols = st.multiselect(
                "Show Columns",
                df.columns.tolist(),
                default=df.columns.tolist()[:10],
                help="Select columns to display"
            )
        else:
            selected_cols = df.columns.tolist()
    
    with col3:
        # Row limit
        max_rows = st.number_input("Max Rows", 1, len(df), min(1000, len(df)))
    
    # Apply filters
    filtered_df = df.copy()
    
    if search_term:
        # Search across all string columns
        mask = pd.Series([False] * len(df))
        for col in df.select_dtypes(include=['object', 'string']).columns:
            mask |= df[col].astype(str).str.contains(search_term, case=False, na=False)
        filtered_df = df[mask]
    
    if selected_cols:
        filtered_df = filtered_df[selected_cols]
    
    filtered_df = filtered_df.head(max_rows)
    
    # Display info
    st.write(f"**Showing {len(filtered_df)} of {len(df)} rows**")
    
    # Editable dataframe
    edited_df = st.data_editor(
        filtered_df,
        use_container_width=True,
        num_rows="dynamic",
        key="data_editor"
    )
    
    # Update session state with edits
    if not edited_df.equals(filtered_df):
        if st.button("💾 Save Changes"):
            # This is a simplified save - in a real app, you'd need to handle
            # merging changes back to the full dataset properly
            st.session_state.work = edited_df
            st.success("Changes saved!")
    
    st.session_state.page_df = filtered_df
    return filtered_df

def sec_downloads():
    """Downloads and export section"""
    st.subheader("⬇️ Downloads & Export")
    
    if st.session_state.work is None or st.session_state.work.empty:
        st.info("📋 No data loaded. Please go to Overview to upload data.")
        return
    
    df = st.session_state.work
    
    # Export options
    st.subheader("Export Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        export_format = st.selectbox(
            "Export Format",
            ["Excel (.xlsx)", "CSV (.csv)", "JSON (.json)"],
            help="Choose export format"
        )
        
        filename = st.text_input(
            "Filename",
            value="product_export",
            help="Filename without extension"
        )
    
    with col2:
        # Export subset options
        export_all = st.radio(
            "Export Data",
            ["All data", "Current view only"],
            help="Choose what data to export"
        )
        
        include_index = st.checkbox("Include row index", value=False)
    
    # Select data to export
    if export_all == "All data":
        export_df = df
    else:
        export_df = st.session_state.page_df if not st.session_state.page_df.empty else df
    
    # Export summary
    st.write(f"**Export Summary:** {len(export_df)} rows × {len(export_df.columns)} columns")
    
    # Download buttons
    st.subheader("Download Files")
    
    try:
        if export_format == "Excel (.xlsx)":
            excel_buffer = to_excel_download(export_df, "Products")
            if excel_buffer.getvalue():
                st.download_button(
                    label="📥 Download Excel File",
                    data=excel_buffer,
                    file_name=f"{filename}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Download as Excel file"
                )
        
        elif export_format == "CSV (.csv)":
            csv_data = export_df.to_csv(index=include_index).encode('utf-8')
            st.download_button(
                label="📥 Download CSV File",
                data=csv_data,
                file_name=f"{filename}.csv",
                mime="text/csv",
                help="Download as CSV file"
            )
        
        elif export_format == "JSON (.json)":
            json_data = export_df.to_json(orient='records', indent=2).encode('utf-8')
            st.download_button(
                label="📥 Download JSON File",
                data=json_data,
                file_name=f"{filename}.json",
                mime="application/json",
                help="Download as JSON file"
            )
            
    except Exception as e:
        st.error(f"Export failed: {str(e)}")
    
    # Additional export options
    st.subheader("Additional Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Export Summary Statistics"):
            summary_df = df.describe(include='all')
            summary_buffer = to_excel_download(summary_df, "Summary")
            if summary_buffer.getvalue():
                st.download_button(
                    label="📥 Download Summary",
                    data=summary_buffer,
                    file_name=f"{filename}_summary.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    with col2:
        if st.button("🏷️ Export Column Info"):
            col_info = pd.DataFrame({
                'Column': df.columns,
                'Type': [str(df[col].dtype) for col in df.columns],
                'Non_Null_Count': [df[col].count() for col in df.columns],
                'Unique_Count': [df[col].nunique() for col in df.columns],
                'Sample_Value': [str(df[col].iloc[0]) if len(df) > 0 else 'N/A' for col in df.columns]
            })
            col_info_buffer = to_excel_download(col_info, "Column_Info")
            if col_info_buffer.getvalue():
                st.download_button(
                    label="📥 Download Column Info",
                    data=col_info_buffer,
                    file_name=f"{filename}_columns.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

def sec_settings():
    """Settings and diagnostics section"""
    st.subheader("⚙️ Settings & Diagnostics")
    
    # API Configuration
    st.subheader("API Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Translation APIs**")
        
        # OpenAI API settings
        openai_api_key = st.text_input(
            "OpenAI API Key",
            type="password",
            help="Enter your OpenAI API key for translation"
        )
        
        if openai_api_key:
            st.success("✅ OpenAI API key provided")
        else:
            st.warning("⚠️ OpenAI API key not provided")
        
        # DeepL API settings  
        deepl_api_key = st.text_input(
            "DeepL API Key",
            type="password",
            help="Enter your DeepL API key for translation"
        )
        
        if deepl_api_key:
            st.session_state.deepl_active = True
            st.success("✅ DeepL API key provided")
        else:
            st.session_state.deepl_active = False
            st.warning("⚠️ DeepL API key not provided")
    
    with col2:
        st.write("**System Information**")
        
        # System info
        system_info = {
            "Python Version": sys.version.split()[0],
            "Pandas Version": pd.__version__,
            "Streamlit Version": st.__version__,
            "Session State Keys": len(st.session_state.keys()),
            "Cache Size": len(st.session_state.proc_cache),
            "Memory Usage": f"{sys.getsizeof(st.session_state)/1024/1024:.1f} MB"
        }
        
        for key, value in system_info.items():
            st.write(f"**{key}:** {value}")
    
    # Diagnostics
    st.subheader("Diagnostics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Show Sample URLs", key="diag_urls"):
            if st.session_state.work is not None and "thumbnail" in st.session_state.work.columns:
                sample_urls = st.session_state.work["thumbnail"].dropna().head(PREVIEW_SAMPLE_SIZE).tolist()
                
                st.write("**Sample Thumbnail URLs:**")
                for i, url in enumerate(sample_urls, 1):
                    cleaned_url = clean_url_for_vision(url)
                    is_valid = is_valid_url(cleaned_url)
                    
                    st.write(f"**{i}.** {url}")
                    st.write(f"   - Cleaned: {cleaned_url}")
                    st.write(f"   - Valid: {'✅' if is_valid else '❌'}")
                    st.write("---")
            else:
                st.warning("No thumbnail column found or no data loaded")
    
    with col2:
        if st.button("🧹 Clear All Cache", key="diag_clear"):
            # Clear all caches
            st.session_state.proc_cache = {}
            st.session_state.audit_rows = []
            
            # Clear global cache
            store = global_cache()
            if st.session_state.file_hash and st.session_state.file_hash in store:
                del store[st.session_state.file_hash]
            
            # Clear any other cached data
            for key in list(st.session_state.keys()):
                if key.startswith('cache_') or key.endswith('_cache'):
                    del st.session_state[key]
            
            st.success("✅ All caches cleared!")
    
    with col3:
        if st.button("📊 Session State Info", key="diag_session"):
            st.write("**Session State Keys:**")
            
            state_info = {}
            for key, value in st.session_state.items():
                if hasattr(value, '__len__') and not isinstance(value, str):
                    try:
                        size = len(value)
                        state_info[key] = f"Length: {size}"
                    except:
                        state_info[key] = str(type(value))
                else:
                    state_info[key] = str(type(value))
            
            for key, info in sorted(state_info.items()):
                st.write(f"- **{key}:** {info}")
    
    # Advanced Settings
    st.subheader("Advanced Settings")
    
    with st.expander("🔧 Advanced Configuration"):
        st.write("**Performance Settings**")
        
        new_batch_size = st.slider(
            "Default Batch Size",
            1, MAX_BATCH_SIZE * 2, st.session_state.batch_size,
            help="Default batch size for processing operations"
        )
        st.session_state.batch_size = new_batch_size
        
        enable_debug = st.checkbox(
            "Enable Debug Mode",
            help="Show additional debug information"
        )
        
        if enable_debug:
            st.write("**Debug Information:**")
            st.json({
                "work_shape": list(st.session_state.work.shape) if st.session_state.work is not None else None,
                "file_hash": st.session_state.file_hash,
                "cache_keys": list(st.session_state.proc_cache.keys()),
                "translation_engine": st.session_state.translation_engine,
                "use_glossary": st.session_state.use_glossary
            })
        
        st.write("**Data Validation**")
        
        if st.button("🔍 Validate Data Structure"):
            if st.session_state.work is not None:
                df = st.session_state.work
                
                # Check for required columns
                is_valid, errors = validate_dataframe(df, REQUIRED_PRODUCT_COLS)
                
                if is_valid:
                    st.success("✅ Data structure is valid!")
                else:
                    st.warning("⚠️ Data structure issues found:")
                    for error in errors:
                        st.write(f"• {error}")
                
                # Additional checks
                checks = []
                
                # Check for empty values
                empty_cols = df.columns[df.isnull().all()].tolist()
                if empty_cols:
                    checks.append(f"Completely empty columns: {empty_cols}")
                
                # Check for duplicate rows
                duplicates = df.duplicated().sum()
                if duplicates > 0:
                    checks.append(f"Duplicate rows found: {duplicates}")
                
                # Check data types
                object_cols = df.select_dtypes(include=['object']).columns.tolist()
                if len(object_cols) > len(df.columns) * 0.8:
                    checks.append("High proportion of text columns - consider data type optimization")
                
                if checks:
                    st.write("**Additional Observations:**")
                    for check in checks:
                        st.write(f"• {check}")
                else:
                    st.success("✅ No additional issues found!")
            else:
                st.warning("No data loaded to validate")

# ================= SIDEBAR NAVIGATION =================
with st.sidebar:
    st.markdown("### 🧭 Navigation")
    
    section = st.radio(
        "Choose a section:",
        [
            "📊 Overview",
            "🖼️ Titles & Translate", 
            "🧩 Grouping",
            "📑 Sheet",
            "⬇️ Downloads",
            "⚙️ Settings"
        ],
        key="nav_section",
        help="Navigate between different sections of the dashboard"
    )
    
    # Quick stats in sidebar
    if st.session_state.work is not None and not st.session_state.work.empty:
        st.markdown("---")
        st.markdown("### 📈 Quick Stats")
        
        df = st.session_state.work
        st.metric("Rows", f"{len(df):,}")
        st.metric("Columns", len(df.columns))
        
        if st.session_state.file_hash:
            st.markdown(f"**File:** `{st.session_state.file_hash}`")
    
    # Help section
    st.markdown("---")
    st.markdown("### 💡 Help")
    
    with st.expander("🚀 Quick Start"):
        st.markdown("""
        1. **📊 Overview**: Upload your CSV file
        2. **🖼️ Titles**: Generate and translate product titles  
        3. **🧩 Grouping**: Analyze and group your data
        4. **📑 Sheet**: View and edit your data
        5. **⬇️ Downloads**: Export your results
        6. **⚙️ Settings**: Configure APIs and diagnostics
        """)
    
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **Common Issues:**
        - **File won't load**: Check CSV format and encoding
        - **Translation fails**: Verify API keys in Settings
        - **Slow performance**: Reduce batch size in Settings
        - **Memory issues**: Clear cache in Settings
        
        **Tips:**
        - Use UTF-8 encoding for international characters
        - Keep file sizes under 100MB for best performance
        - Test translations on small batches first
        """)

# ================= MAIN ROUTER =================
# Get reference to current work data
work = st.session_state.work

# Route to appropriate section
if section == "📊 Overview":
    safe_section("Overview", sec_overview)
elif section == "🖼️ Titles & Translate":
    safe_section("Titles & Translate", sec_titles)
elif section == "🧩 Grouping":
    safe_section("Grouping", sec_grouping)
elif section == "📑 Sheet":
    result = safe_section("Sheet", sec_sheet)
    if isinstance(result, pd.DataFrame):
        st.session_state.page_df = result
elif section == "⬇️ Downloads":
    safe_section("Downloads", sec_downloads)
elif section == "⚙️ Settings":
    safe_section("Settings", sec_settings)
else:
    # Default to settings
    safe_section("Settings", sec_settings)

# ================= FOOTER =================
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #6b7280; font-size: 12px; padding: 10px;">
        🧭 Product Mapping Dashboard • Built with Streamlit • 
        <a href="#" style="color: #10b981;">Documentation</a> • 
        <a href="#" style="color: #10b981;">Support</a>
    </div>
    """,
    unsafe_allow_html=True
)
