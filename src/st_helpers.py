import streamlit as st
import requests
import base64


def bytes_to_base64(data: bytes) -> str:
    return base64.b64encode(data).decode()

@st.cache_data(show_spinner=False)
def get_image(url: str) -> bytes:
    req = requests.get(url)
    return bytes_to_base64(req.content)

def display_image_with_hover(image_url, hover_text, rated=False):
    state_class = "poster-rated" if rated else "poster-unrated"

    img_base64 = get_image(image_url)

    html = f"""
    <style>
    .tooltip {{
      position: relative;
      display: inline-block;
      cursor: pointer;
    }}

    .tooltip .tooltiptext {{
      visibility: hidden;
      width: 120px;
      background-color: black;
      color: white;
      text-align: center;
      border-radius: 6px;
      padding: 5px 0;
      position: absolute;
      z-index: 1;
      top: 0%;
      left: 50%;
      margin-left: -60px;
      opacity: 0;
      transition: opacity 0.3s;
    }}

    .tooltip:hover .tooltiptext {{
      visibility: visible;
      opacity: 1;
    }}

    .tooltip img {{
      border-radius: 8px;             
      box-shadow: 0 2px 5px rgba(0,0,0,0.15);
      transition: transform 0.2s;
    }}

    .tooltip img:hover {{
      transform: scale(1.05);       
    }}
    
    /* NEW: rating states */
    .poster-unrated img {{
      opacity: 0.55;
      filter: grayscale(30%);
    }}

    .poster-rated img {{
      opacity: 1.0;
      filter: none;
      box-shadow: 0 0 0 2px rgba(34,197,94,0.9);
    }}
    </style>

    </style>
    <div class='tooltip {state_class}'>
      <img src='data:image/png;base64,{img_base64}' style='margin-bottom:10px;'>
      <span class='tooltiptext'>{hover_text}</span>
    </div>
    """
    return st.markdown(html, unsafe_allow_html=True)