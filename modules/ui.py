import streamlit as st

def render_flat_card(row, insight: str):
    st.markdown(f"""
<div style="border:1px solid #e5e7eb; border-radius: 12px; padding: 1.2em; margin-bottom: 1em; background: #fff;">
  <div style="display: flex; justify-content: space-between;">
    <div>
      <strong>#{row['rank']} {row['town']}, {row['flat_type']} - {row['flat_model']}</strong><br/>
      <span style="color: #6b7280">{row['storey_range']} | Lease: {row['remaining_lease_years']} yrs</span>
    </div>
    <div style="color:#10b981; font-size: 1.6em; font-weight: bold;">
      {row['score']:.2f}/10
    </div>
  </div>
  <div style="margin-top: 1em;">
    <span style="font-size: 1.1em;">💰 <b>S${row['resale_price']:,}</b></span> ·
    <span style="font-size: 1.1em;">📐 {row['floor_area_sqm']} sqm</span>
  </div>
  <div style="margin-top: 1em; background: #ecfeff; border-left: 5px solid #06b6d4; padding: .8em 1em;">
    <b>AI Market Insight:</b> {insight}
  </div>
</div>
""", unsafe_allow_html=True)
