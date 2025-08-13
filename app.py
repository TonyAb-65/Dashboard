import os
import io
import pandas as pd
import requests
import streamlit as st
from PIL import Image
from openai import OpenAI
from deep_translator import DeeplTranslator

# Load API keys from Streamlit Secrets
DEEPL_KEY = st.secrets["DEEPL_API_KEY"]
OPENAI_KEY = st.secrets["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_KEY)

# --- FUNCTIONS ---

def fetch_image(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content))
    except Exception as e:
        st.error(f"Error fetching image: {e}")
        return None

def generate_english_description(image_url, current_name):
    """Generate product description in English using OpenAI GPT."""
    try:
        prompt = f"""
        You are an e-commerce product content writer.
        Write a clear, appealing English product name/short description for the product shown in this image:
        {image_url}
        Current name: {current_name}
        Make it suitable for online store listing.
        """
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=60
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        st.error(f"OpenAI error: {e}")
        return current_name

def translate_to_arabic_deepl(text_en):
    """Translate English text to Arabic using DeepL API."""
    try:
        resp = requests.post(
            "https://api-free.deepl.com/v2/translate",
            data={
                "auth_key": DEEPL_KEY,
                "text": text_en,
                "target_lang": "AR"
            }
        )
        resp.raise_for_status()
        return resp.json()["translations"][0]["text"]
    except Exception as e:
        st.error(f"DeepL error: {e}")
        return text_en

def process_excel(file):
    df = pd.read_excel(file)
    if "thumbnail" not in df.columns:
        st.error("No 'thumbnail' column found (Column W expected).")
        return None

    for idx, row in df.iterrows():
        img_url = row["thumbnail"]
        current_name_en = str(row.iloc[0]) if pd.notnull(row.iloc[0]) else ""
        english_desc = generate_english_description(img_url, current_name_en)
        arabic_desc = translate_to_arabic_deepl(english_desc)
        df.iat[idx, 0] = english_desc  # Overwrite Column A (English)
        df.iat[idx, 1] = arabic_desc   # Overwrite Column B (Arabic)
    return df

# --- STREAMLIT UI ---
st.title("Excel Product Updater with Image → English → Arabic Translation")

uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

if uploaded_file:
    df_updated = process_excel(uploaded_file)
    if df_updated is not None:
        st.success("Processing complete.")
        st.dataframe(df_updated.head(30))
        buffer = io.BytesIO()
        df_updated.to_excel(buffer, index=False)
        buffer.seek(0)
        st.download_button(
            "Download updated Excel",
            buffer,
            file_name="updated_products.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
