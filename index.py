import streamlit as st
import streamlit.components.v1 as components
import os

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="FlatWise")

# --- Read and Serve Frontend HTML ---
# Construct the full path to the HTML file
frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")

try:
    with open(frontend_path, 'r', encoding='utf-8') as f:
        html_string = f.read()

    # --- Read and Inject Frontend JS ---
    # Construct the full path to the JS file
    scripts_path = os.path.join(os.path.dirname(__file__), "frontend", "scripts.js")
    with open(scripts_path, 'r', encoding='utf-8') as f:
        js_string = f.read()

    # Inject the JavaScript into the HTML string before the closing </body> tag
    # This ensures our script runs after the HTML body is loaded
    html_with_js = html_string.replace('</body>', f'<script>{js_string}</script></body>')
    
    # The 'height' parameter is crucial for a full-page experience
    components.html(html_with_js, height=1200, scrolling=True)

except FileNotFoundError as e:
    st.error(f"Frontend file not found. Ensure your HTML and JS files are in the 'frontend' directory. Error: {e}")