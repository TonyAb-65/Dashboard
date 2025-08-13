import io
import re
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
import requests
from PIL import Image
from io import BytesIO

# ---- API clients (official SDKs) ----
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

# DeepL (official SDK)
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


# ---------- Streamlit page ----------
st.set_page_config(page_title="Image → EN (OpenAI) → AR (DeepL) Excel Updater", layout="wide")
st.title("🧾 Excel Updater: Image → English (OpenAI) → Arabic (DeepL)")

st.markdown("""
**What it does**
1. Load your Excel file.
2. For each row, the app reads the image URL in column **`thumbnail`** (W).
3. It generates a concise **English** product description using **OpenAI**.
4. It translates that text into **Arabic** using **DeepL**.
5. It overwrites:
   - Column **A** (`name`) with **English**
   - Column **B** (`name_ar`) with **Arabic**
""")

# ---------- Helpers ----------
def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def fetch_image_thumb(url: str, timeout=6):
    """Fetch an image and return a small PIL thumbnail (or None)."""
    try:
        if not is_valid_url(url):
            return None
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.thumbnail((256, 256))
        return img
    except Exception:
        return None

def openai_describe_image(url: str, fallback_title: str = "") -> str:
    """
    Returns a short ecommerce-ready EN description from an image URL using OpenAI.
    If unavailable, returns a simple fallback based on the current name.
    """
    if not openai_active or not is_valid_url(url):
        # Fallback: a minimal cleaned-up version of the current name
        return (fallback_title or "").strip()

    prompt = (
        "You are a product copy expert for an online store. "
        "Look at the image and write a concise English product title/short description (1–2 sentences). "
        "Be specific: brand, form, size/count if visible, typical use. "
        "Avoid hype and keep it accurate to the image."
    )
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": url}},
                    ],
                }
            ],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        # Graceful fallback to existing name
        return (fallback_title or "").strip()

def deepl_en_to_ar(text_en: str) -> str:
    """Translate English → Arabic via DeepL (official SDK)."""
    if not deepl_active:
        return text_en
    try:
        res = translator.translate_text(text_en, source_lang="EN", target_lang="AR")
        return res.text
    except Exception:
        return text_en

def to_excel_download(df: pd.DataFrame, file_name="updated_products.xlsx"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Products")
    buffer.seek(0)
    st.download_button(
        "⬇️ Download updated Excel",
        buffer,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ---------- UI: Upload & Process ----------
uploaded = st.file_uploader("Upload your Excel (.xlsx)", type=["xlsx"])
if not uploaded:
    st.stop()

try:
    df = pd.read_excel(uploaded, engine="openpyxl")
except Exception as e:
    st.error(f"Could not read Excel: {e}")
    st.stop()

# Expect column A/B names:
expected_cols = ["name", "name_ar"]
missing = [c for c in expected_cols if c not in df.columns]
if missing:
    st.error(f"Missing required columns: {missing} (Column A must be 'name', Column B must be 'name_ar').")
    st.stop()

# Image column (W) expected to be 'thumbnail'
img_col = "thumbnail"
if img_col not in df.columns:
    st.error("No 'thumbnail' column found (Column W expected). Please add it and try again.")
    st.stop()

st.success("File loaded successfully. Ready to generate descriptions.")
st.caption(f"Rows detected: {len(df)} | Using image URLs from column: **{img_col}**")

# Preview a few thumbnails
with st.expander("Preview image thumbnails (first 12)"):
    grid = st.columns(6)
    for i, (_, row) in enumerate(df.head(12).iterrows()):
        url = str(row.get(img_col, ""))
        img = fetch_image_thumb(url)
        with grid[i % 6]:
            if img:
                st.image(img, caption=f"Row {i}", use_container_width=True)
            else:
                st.write("No image")

# Process button
if st.button("🖼️ Generate EN (OpenAI) → AR (DeepL) and overwrite A/B"):
    if not openai_active:
        st.warning("OPENAI_API_KEY not found in Secrets – falling back to current English text only.")
    if not deepl_active:
        st.warning("DEEPL_API_KEY not found in Secrets – skipping Arabic translation (will copy English).")

    progress = st.progress(0)
    status = st.empty()

    total = len(df)
    for idx, row in df.iterrows():
        # Current values (for fallback if needed)
        current_en = str(row.get("name", "") or "")
        url = str(row.get(img_col, "") or "")

        # 1) English from image
        english_desc = openai_describe_image(url, fallback_title=current_en)
        english_desc = english_desc.strip() if isinstance(english_desc, str) else current_en

        # 2) Arabic via DeepL
        arabic_desc = deepl_en_to_ar(english_desc)

        # Overwrite columns A & B
        df.at[idx, "name"] = english_desc
        df.at[idx, "name_ar"] = arabic_desc

        # Progress UI
        pct = int(((idx + 1) / total) * 100)
        progress.progress(pct)
        status.write(f"Processed {idx + 1} / {total}")

    status.write("✅ Done")
    st.success("Descriptions generated and written to columns A (English) and B (Arabic).")

    st.subheader("Preview (first 20 rows)")
    st.dataframe(df.head(20), use_container_width=True)

    to_excel_download(df, file_name="updated_products.xlsx")
