import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import io
import re

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IT Job Market Dashboard 🇻🇳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CUSTOM CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Dark sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29 0%, #1a1a3e 50%, #24243e 100%);
    }
    [data-testid="stSidebar"] * {
        color: #e0e0ff !important;
    }
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] .stSelectbox label {
        color: #a0a0cc !important;
        font-size: 0.85rem;
    }

    /* Main background */
    .main .block-container {
        background: #0d0d1a;
        padding: 2rem 3rem;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1e1e3f 0%, #2d2d5e 100%);
        border: 1px solid rgba(120,120,255,0.2);
        border-radius: 16px;
        padding: 1.4rem 1.6rem;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        transition: transform 0.2s;
    }
    .kpi-card:hover { transform: translateY(-4px); }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #a78bfa, #60a5fa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .kpi-label {
        color: #a0a0cc;
        font-size: 0.82rem;
        margin-top: 4px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    /* Section headers */
    .section-header {
        font-size: 1.5rem;
        font-weight: 700;
        color: #c4b5fd;
        border-left: 4px solid #7c3aed;
        padding-left: 14px;
        margin: 2rem 0 1rem 0;
    }

    /* Tabs override */
    .stTabs [data-baseweb="tab-list"] {
        background: #1e1e3f;
        border-radius: 12px;
        padding: 4px;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: #a0a0cc;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #7c3aed, #2563eb) !important;
        color: white !important;
    }

    /* Metric delta */
    [data-testid="metric-container"] {
        background: #1e1e3f;
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid rgba(120,120,255,0.15);
    }

    /* Plotly chart background */
    .js-plotly-plot .plotly { background: transparent !important; }
</style>
""", unsafe_allow_html=True)

# ─── DATA LOADING ────────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "itjob_crud", "data")

@st.cache_data
def load_top_skills():
    """Parse format: Rank  Ky nang   So job   Ti le (%)"""
    rows = []
    with open(os.path.join(DATA_DIR, "top_skills.txt"), encoding="utf-8") as f:
        for line in f:
            # Match: number  skill_text  number  percentage%
            m = re.match(r"^\s*(\d+)\s{1,6}(.+?)\s{2,}(\d+)\s+[\d.]+%\s*$", line)
            if m:
                rows.append({
                    "Rank": int(m.group(1)),
                    "Skill": m.group(2).strip(),
                    "Jobs": int(m.group(3))
                })
    return pd.DataFrame(rows)

@st.cache_data
def load_pagerank():
    """Parse format: Rank   Skill   Score"""
    rows = []
    with open(os.path.join(DATA_DIR, "skill_pagerank.txt"), encoding="utf-8") as f:
        for line in f:
            m = re.match(r"^\s*(\d+)\s{1,6}(.+?)\s{2,}([\d.]+)\s*$", line)
            if m:
                rows.append({
                    "Rank": int(m.group(1)),
                    "Skill": m.group(2).strip(),
                    "Score": float(m.group(3))
                })
    return pd.DataFrame(rows)

@st.cache_data
def load_tfidf():
    """Parse format: Level   Skill   TF-IDF"""
    rows = []
    skip = {"Level", "TOP", "=====", "-----"}
    with open(os.path.join(DATA_DIR, "tfidf_wordcloud.txt"), encoding="utf-8") as f:
        for line in f:
            line = line.rstrip()
            if not line or any(line.lstrip().startswith(s) for s in skip):
                continue
            m = re.match(r"^\s*(\S+)\s{2,}(.+?)\s{2,}([\d.]+)\s*$", line)
            if m:
                level = m.group(1).strip()
                skill = m.group(2).strip()
                score = float(m.group(3))
                rows.append({"Level": level, "Skill": skill, "TF-IDF": score})
    return pd.DataFrame(rows)

@st.cache_data
def load_top_paying():
    """
    Parse format:
      --- LEVEL ---
      Top N: SALARY Triệu | TITLE | Cty: COMPANY
    """
    rows = []
    current_level = None
    with open(os.path.join(DATA_DIR, "top_paying_jobs.txt"), encoding="utf-8") as f:
        for line in f:
            line = line.rstrip()
            # Detect level header: --- FRESHER ---
            lm = re.match(r"^---\s+(.+?)\s+---\s*$", line)
            if lm:
                current_level = lm.group(1).strip().title()
                continue
            # Detect data line: Top N: SALARY Triệu | TITLE | Cty: COMPANY
            dm = re.match(
                r"^Top\s+(\d+):\s+([\d.]+)\s+Tri\u1ec7u\s*\|\s*(.+?)\s*\|\s*Cty:\s*(.+)$",
                line
            )
            if dm and current_level:
                rows.append({
                    "Level": current_level,
                    "Rank": int(dm.group(1)),
                    "Salary": float(dm.group(2)),
                    "Title": dm.group(3).strip(),
                    "Company": dm.group(4).strip(),
                })
    return pd.DataFrame(rows)

@st.cache_data
def load_companies():
    """Parse fixed-width company table."""
    rows = []
    with open(os.path.join(DATA_DIR, "company_hiring_broadcast.txt"), encoding="utf-8") as f:
        for line in f:
            line_s = line.strip()
            if not line_s or line_s.startswith("=") or line_s.startswith("-") \
               or line_s.startswith("TOP") or line_s.startswith("Company") \
               or line_s.startswith("(") or line_s.startswith("Luong"):
                continue
            m = re.match(
                r"^(.{31,45}?)\s{2,}(\d+)\s+([\d.]+)\s+([\d.]+)\s+([+-]?[\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\S.+?)\s{2,}(.+)$",
                line
            )
            if m:
                rows.append({
                    "Company": m.group(1).strip(),
                    "Jobs": int(m.group(2)),
                    "Share%": float(m.group(3)),
                    "AvgSalary": float(m.group(4)),
                    "vsMkt%": float(m.group(5)),
                    "MaxSalary": float(m.group(6)),
                    "AvgYoE": float(m.group(7)),
                    "Remote%": float(m.group(8)),
                    "TopSkill": m.group(9).strip(),
                    "Levels": m.group(10).strip(),
                })
    return pd.DataFrame(rows)

df_skills    = load_top_skills()
df_pagerank  = load_pagerank()
df_tfidf     = load_tfidf()
df_paying    = load_top_paying()
df_companies = load_companies()

LEVELS = sorted(df_tfidf["Level"].unique().tolist())

# ─── PLOTLY THEME ────────────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#c4b5fd"),
    title_font=dict(size=16, color="#e0e0ff"),
    xaxis=dict(gridcolor="rgba(120,120,255,0.1)", zerolinecolor="rgba(120,120,255,0.2)"),
    yaxis=dict(gridcolor="rgba(120,120,255,0.1)", zerolinecolor="rgba(120,120,255,0.2)"),
    colorway=["#a78bfa","#60a5fa","#34d399","#f472b6","#fbbf24","#fb923c","#38bdf8","#c084fc"],
    margin=dict(l=20, r=20, t=50, b=20),
    hoverlabel=dict(bgcolor="#1e1e3f", bordercolor="#7c3aed", font=dict(color="white")),
)

GRAD_COLORS = px.colors.sequential.Purpor

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 💼 IT Job Market VN")
    st.markdown("---")
    page = st.radio(
        "Chọn Dashboard",
        ["📊 Top Kỹ Năng", "🔍 TF-IDF theo Cấp Bậc", "💰 Lương Cao Nhất", "🏢 Công Ty Tuyển Nhiều"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.markdown("<small style='color:#6060aa'>Dữ liệu: IT Job Market VN</small>", unsafe_allow_html=True)

# ─── HEADER ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center; padding: 1.5rem 0 1rem 0;'>
  <h1 style='font-size:2.6rem; font-weight:800;
    background:linear-gradient(90deg,#a78bfa,#60a5fa,#34d399);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    margin-bottom:0.3rem;'>
    🇻🇳 IT Job Market Dashboard
  </h1>
  <p style='color:#6b6b9a; font-size:1rem;'>Phân tích thị trường việc làm IT Việt Nam · Big Data Project</p>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: TOP KỸ NĂNG
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Top Kỹ Năng":
    st.markdown("<div class='section-header'>🏆 Top 30 Kỹ Năng Công Nghệ Phổ Biến Nhất</div>", unsafe_allow_html=True)

    tabs = st.tabs(["📊 Bar Chart", "🫧 Bubble Chart", "📋 Bảng Dữ Liệu"])

    with tabs[0]:
        top_n = st.slider("Hiển thị Top N kỹ năng:", 5, 30, 20)
        df_plot = df_skills.head(top_n).sort_values("Jobs")
        fig = go.Figure(go.Bar(
            x=df_plot["Jobs"], y=df_plot["Skill"],
            orientation="h",
            marker=dict(
                color=df_plot["Jobs"],
                colorscale="Plasma",
                showscale=True,
                colorbar=dict(title="Jobs", tickfont=dict(color="#c4b5fd")),
                line=dict(width=0)
            ),
            text=df_plot["Jobs"],
            textposition="outside",
            textfont=dict(color="#c4b5fd", size=12),
            hovertemplate="<b>%{y}</b><br>Số Việc Làm: <b>%{x}</b><extra></extra>"
        ))
        fig.update_layout(**PLOTLY_LAYOUT, height=max(400, top_n * 28),
                          title=f"Top {top_n} Kỹ Năng Theo Số Lượng Việc Làm",
                          xaxis_title="Số Lượng Việc Làm")
        st.plotly_chart(fig, use_container_width=True)

    with tabs[1]:
        fig_b = go.Figure(go.Scatter(
            x=df_skills["Rank"],
            y=df_skills["Jobs"],
            mode="markers+text",
            text=df_skills["Skill"],
            textposition="top center",
            textfont=dict(size=10, color="#c4b5fd"),
            marker=dict(
                size=df_skills["Jobs"] / df_skills["Jobs"].max() * 60 + 8,
                color=df_skills["Jobs"],
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title="Jobs", tickfont=dict(color="#c4b5fd")),
                line=dict(color="rgba(255,255,255,0.3)", width=1)
            ),
            hovertemplate="<b>%{text}</b><br>Rank: %{x}<br>Jobs: %{y}<extra></extra>"
        ))
        fig_b.update_layout(**PLOTLY_LAYOUT, height=520,
                            title="Bubble Chart – Kỹ Năng vs Số Việc Làm",
                            xaxis_title="Rank", yaxis_title="Jobs")
        st.plotly_chart(fig_b, use_container_width=True)

    with tabs[2]:
        st.dataframe(
            df_skills,
            use_container_width=True, height=600
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: TF-IDF
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🔍 TF-IDF theo Cấp Bậc":
    st.markdown("<div class='section-header'>🔍 Kỹ Năng Nổi Bật theo Cấp Bậc (TF-IDF)</div>", unsafe_allow_html=True)
    st.info("TF-IDF phát hiện kỹ năng **đặc trưng riêng** cho từng cấp bậc – không phải kỹ năng phổ biến chung chung.")

    # Level selector – không có option "Tất Cả"
    col_sel, col_info = st.columns([2, 1])
    with col_sel:
        selected_levels = st.multiselect(
            "Chọn Cấp Bậc:",
            LEVELS,
            default=LEVELS[:1],
            key="tfidf_levels"
        )

    if not selected_levels:
        selected_levels = LEVELS[:1]

    df_filtered = df_tfidf[df_tfidf["Level"].isin(selected_levels)]

    with col_info:
        st.metric("Cấp bậc chọn", len(selected_levels))
        st.metric("Số kỹ năng", len(df_filtered))

    st.markdown("<br>", unsafe_allow_html=True)

    tabs = st.tabs(["☁️ Word Cloud", "📊 Bar Chart", "📋 Bảng"])

    # ── WORD CLOUD ────────────────────────────────────────────────────────────
    with tabs[0]:
        if len(selected_levels) == 1:
            # Single level – one cloud
            lvl = selected_levels[0]
            df_wc = df_filtered.copy()
            wc_dict = dict(zip(df_wc["Skill"], df_wc["TF-IDF"]))
            cmap_name = {
                "Fresher": "Purples", "Junior": "Blues", "Mid-level": "Greens",
                "Senior": "Oranges", "Lead": "Reds", "Manager": "YlOrRd", "Undefined": "Greys"
            }.get(lvl, "viridis")
            wc = WordCloud(
                width=900, height=450, background_color="#0d0d1a",
                colormap=cmap_name, max_words=50,
                prefer_horizontal=0.85, relative_scaling=0.6,
                min_font_size=10
            ).generate_from_frequencies(wc_dict)
            fig_wc, ax = plt.subplots(figsize=(11, 5))
            fig_wc.patch.set_facecolor("#0d0d1a")
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            ax.set_title(f"Word Cloud – {lvl}", color="#c4b5fd", fontsize=16, pad=12)
            st.pyplot(fig_wc)
        else:
            # Multiple levels – grid
            n_cols = min(3, len(selected_levels))
            n_rows = (len(selected_levels) + n_cols - 1) // n_cols
            fig_grid, axes = plt.subplots(n_rows, n_cols,
                                           figsize=(n_cols * 5, n_rows * 3.5))
            fig_grid.patch.set_facecolor("#0d0d1a")
            if n_rows == 1 and n_cols == 1:
                axes = [[axes]]
            elif n_rows == 1:
                axes = [axes]
            elif n_cols == 1:
                axes = [[ax] for ax in axes]

            cmaps = ["Purples","Blues","Greens","Oranges","Reds","YlOrRd","cool"]
            for idx, lvl in enumerate(selected_levels):
                row, col = divmod(idx, n_cols)
                ax = axes[row][col]
                df_lvl = df_tfidf[df_tfidf["Level"] == lvl]
                wc_dict = dict(zip(df_lvl["Skill"], df_lvl["TF-IDF"]))
                if wc_dict:
                    wc = WordCloud(
                        width=400, height=220, background_color="#0d0d1a",
                        colormap=cmaps[idx % len(cmaps)], max_words=30,
                        prefer_horizontal=0.8, relative_scaling=0.5
                    ).generate_from_frequencies(wc_dict)
                    ax.imshow(wc, interpolation="bilinear")
                ax.axis("off")
                ax.set_title(lvl, color="#c4b5fd", fontsize=13, pad=6)
                ax.set_facecolor("#0d0d1a")

            # Hide extra axes
            for idx in range(len(selected_levels), n_rows * n_cols):
                row, col = divmod(idx, n_cols)
                axes[row][col].set_visible(False)

            plt.tight_layout(pad=1.5)
            st.pyplot(fig_grid)

    # ── BAR CHART ─────────────────────────────────────────────────────────────
    with tabs[1]:
        if len(selected_levels) == 1:
            df_bar = df_filtered.sort_values("TF-IDF", ascending=True)
            fig = go.Figure(go.Bar(
                x=df_bar["TF-IDF"], y=df_bar["Skill"],
                orientation="h",
                marker=dict(color=df_bar["TF-IDF"], colorscale="Plasma",
                            showscale=True,
                            colorbar=dict(title="TF-IDF", tickfont=dict(color="#c4b5fd"))),
                text=[f"{v:.4f}" for v in df_bar["TF-IDF"]],
                textposition="outside",
                textfont=dict(color="#c4b5fd", size=11),
                hovertemplate="<b>%{y}</b><br>TF-IDF: <b>%{x:.4f}</b><extra></extra>"
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=max(400, len(df_bar)*28),
                              title=f"Kỹ Năng Nổi Bật – {selected_levels[0]}",
                              xaxis_title="TF-IDF Score")
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Grouped bar
            fig = go.Figure()
            colors = ["#a78bfa","#60a5fa","#34d399","#f472b6","#fbbf24","#fb923c","#38bdf8"]
            for i, lvl in enumerate(selected_levels):
                df_lvl = df_tfidf[df_tfidf["Level"] == lvl].sort_values("TF-IDF", ascending=False).head(10)
                fig.add_trace(go.Bar(
                    name=lvl,
                    x=df_lvl["Skill"], y=df_lvl["TF-IDF"],
                    marker_color=colors[i % len(colors)],
                    hovertemplate=f"<b>%{{x}}</b><br>Level: {lvl}<br>TF-IDF: %{{y:.4f}}<extra></extra>"
                ))
            fig.update_layout(**PLOTLY_LAYOUT, height=500,
                              title="Top 10 Kỹ Năng theo TF-IDF – Từng Cấp Bậc",
                              yaxis_title="TF-IDF Score", barmode="group",
                              legend=dict(orientation="h", y=-0.15))
            fig.update_xaxes(tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

    # ── TABLE ─────────────────────────────────────────────────────────────────
    with tabs[2]:
        st.dataframe(
            df_filtered.sort_values(["Level","TF-IDF"], ascending=[True, False]),
            use_container_width=True, height=500
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: LƯƠNG CAO NHẤT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💰 Lương Cao Nhất":
    st.markdown("<div class='section-header'>💰 Top 5 Công Việc Lương Cao Nhất theo Cấp Bậc</div>", unsafe_allow_html=True)

    LEVELS_PAY = sorted(df_paying["Level"].unique().tolist())
    tabs_pay = st.tabs(["🏅 All Levels", "🎯 Chọn Cấp Bậc", "📈 So Sánh", "📋 Bảng"])

    palette = ["#a78bfa","#60a5fa","#34d399","#f472b6","#fbbf24","#fb923c","#38bdf8"]

    with tabs_pay[0]:
        fig_all = go.Figure()
        for i, lvl in enumerate(LEVELS_PAY):
            df_lvl = df_paying[df_paying["Level"] == lvl].sort_values("Salary", ascending=True)
            fig_all.add_trace(go.Bar(
                y=[f"[{lvl}] {t[:30]}..." if len(t)>30 else f"[{lvl}] {t}" for t in df_lvl["Title"]],
                x=df_lvl["Salary"],
                orientation="h",
                name=lvl,
                marker_color=palette[i % len(palette)],
                hovertemplate="<b>%{y}</b><br>Salary: %{x}M VND<extra></extra>"
            ))
        fig_all.update_layout(**PLOTLY_LAYOUT, height=700, barmode="stack",
                              title="Top 5 Lương Cao Nhất – Tất Cả Cấp Bậc",
                              xaxis_title="Lương (triệu VND)",
                              legend=dict(orientation="h", y=-0.12))
        st.plotly_chart(fig_all, use_container_width=True)

    with tabs_pay[1]:
        sel_level = st.selectbox("Chọn cấp bậc:", LEVELS_PAY, key="pay_lvl")
        df_sel = df_paying[df_paying["Level"] == sel_level].sort_values("Salary", ascending=True)

        fig_s = go.Figure(go.Bar(
            x=df_sel["Salary"],
            y=[f"#{r} {t}" for r, t in zip(df_sel["Rank"], df_sel["Title"])],
            orientation="h",
            marker=dict(
                color=df_sel["Salary"],
                colorscale="Plasma", showscale=True,
                colorbar=dict(title="Salary (M)", tickfont=dict(color="#c4b5fd"))
            ),
            text=[f"{s}M" for s in df_sel["Salary"]],
            textposition="outside",
            textfont=dict(color="#c4b5fd", size=13, family="Inter"),
            hovertemplate="<b>%{y}</b><br>Công ty: " +
                df_sel["Company"].values.tolist()[0] +
                "<br>Salary: %{x}M VND<extra></extra>"
        ))
        fig_s.update_layout(**PLOTLY_LAYOUT, height=360,
                            title=f"Top 5 Lương – {sel_level}",
                            xaxis_title="Lương (triệu VND)")
        st.plotly_chart(fig_s, use_container_width=True)

        # Company cards
        st.markdown("<br>**Chi tiết:**", unsafe_allow_html=True)
        for _, row in df_sel.sort_values("Rank").iterrows():
            color = palette[LEVELS_PAY.index(sel_level) % len(palette)]
            st.markdown(f"""
            <div style='background:linear-gradient(135deg,#1e1e3f,#2d2d5e);
                border-left:4px solid {color}; border-radius:10px;
                padding:0.8rem 1.2rem; margin-bottom:0.6rem;
                display:flex; justify-content:space-between; align-items:center;'>
                <div>
                  <span style='color:#a0a0cc;font-size:0.8rem;'>#{row["Rank"]} · {row["Company"][:40]}</span><br>
                  <span style='color:white;font-weight:600;font-size:1rem;'>{row["Title"]}</span>
                </div>
                <div style='font-size:1.5rem;font-weight:800;color:{color};'>{row["Salary"]}M</div>
            </div>""", unsafe_allow_html=True)

    with tabs_pay[2]:
        # Max salary per level comparison
        df_max = df_paying.groupby("Level")["Salary"].max().reset_index()
        df_avg = df_paying.groupby("Level")["Salary"].mean().reset_index()
        fig_cmp = go.Figure()
        fig_cmp.add_trace(go.Bar(name="Max Salary", x=df_max["Level"], y=df_max["Salary"],
                                  marker_color="#a78bfa",
                                  hovertemplate="<b>%{x}</b><br>Max: %{y}M<extra></extra>"))
        fig_cmp.add_trace(go.Bar(name="Avg of Top5", x=df_avg["Level"], y=df_avg["Salary"].round(1),
                                  marker_color="#60a5fa",
                                  hovertemplate="<b>%{x}</b><br>Avg Top5: %{y}M<extra></extra>"))
        fig_cmp.update_layout(**PLOTLY_LAYOUT, height=430, barmode="group",
                              title="So Sánh Max / Avg Lương Top5 theo Cấp Bậc",
                              yaxis_title="Lương (triệu VND)")
        st.plotly_chart(fig_cmp, use_container_width=True)

    with tabs_pay[3]:
        st.dataframe(
            df_paying,
            use_container_width=True, height=550
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: CÔNG TY TUYỂN NHIỀU
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🏢 Công Ty Tuyển Nhiều":
    st.markdown("<div class='section-header'>🏢 Top 20 Công Ty Tuyển Dụng Nhiều Nhất</div>", unsafe_allow_html=True)

    if df_companies.empty:
        st.warning("Không parse được dữ liệu công ty:")
        with open(f"{DATA_DIR}/company_hiring_broadcast.txt", encoding="utf-8") as f:
            st.text(f.read())
    else:
        tabs_co = st.tabs(["📊 Jobs Count", "💵 Salary Analysis", "⚖️ vs Market", "📋 Bảng Full"])

        with tabs_co[0]:
            df_co = df_companies.sort_values("Jobs", ascending=True)
            fig = go.Figure(go.Bar(
                x=df_co["Jobs"], y=df_co["Company"],
                orientation="h",
                marker=dict(color=df_co["Jobs"], colorscale="Purpor", showscale=True,
                            colorbar=dict(title="Jobs", tickfont=dict(color="#c4b5fd"))),
                text=df_co["Jobs"],
                textposition="outside",
                textfont=dict(color="#c4b5fd", size=12),
                hovertemplate="<b>%{y}</b><br>Jobs: %{x}<br>Share: " +
                    df_co["Share%"].astype(str) + "%<extra></extra>"
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=600,
                              title="Số Lượng Tin Tuyển Dụng theo Công Ty",
                              xaxis_title="Số Tin Tuyển Dụng")
            st.plotly_chart(fig, use_container_width=True)

        with tabs_co[1]:
            fig_sal = go.Figure()
            df_s = df_companies.sort_values("AvgSalary", ascending=False)
            fig_sal.add_trace(go.Bar(
                name="Avg Salary", x=df_s["Company"], y=df_s["AvgSalary"],
                marker_color="#a78bfa",
                hovertemplate="<b>%{x}</b><br>Avg Salary: %{y}M VND<extra></extra>"
            ))
            fig_sal.add_trace(go.Bar(
                name="Max Salary", x=df_s["Company"], y=df_s["MaxSalary"],
                marker_color="#34d399",
                hovertemplate="<b>%{x}</b><br>Max Salary: %{y}M VND<extra></extra>"
            ))
            fig_sal.add_hline(y=36.6, line_dash="dot", line_color="#f472b6",
                              annotation_text="Avg Market 36.6M", annotation_position="top right",
                              annotation_font_color="#f472b6")
            fig_sal.update_layout(**PLOTLY_LAYOUT, height=480, barmode="group",
                                  title="Lương TB và Max theo Công Ty",
                                  yaxis_title="Lương (triệu VND)")
            fig_sal.update_xaxes(tickangle=-35)
            st.plotly_chart(fig_sal, use_container_width=True)

        with tabs_co[2]:
            colors_vs = ["#34d399" if v >= 0 else "#f87171" for v in df_companies["vsMkt%"]]
            fig_vs = go.Figure(go.Bar(
                x=df_companies["Company"], y=df_companies["vsMkt%"],
                marker_color=colors_vs,
                text=[f"{v:+.1f}%" for v in df_companies["vsMkt%"]],
                textposition="outside",
                textfont=dict(size=10, color="#c4b5fd"),
                hovertemplate="<b>%{x}</b><br>vs Market: %{y:+.1f}%<extra></extra>"
            ))
            fig_vs.add_hline(y=0, line_color="#ffffff", line_width=1, line_dash="dash")
            fig_vs.update_layout(**PLOTLY_LAYOUT, height=480,
                                 title="Lương So Với Thị Trường (%) – Xanh = Cao Hơn, Đỏ = Thấp Hơn",
                                 yaxis_title="% so với thị trường")
            fig_vs.update_xaxes(tickangle=-35)
            st.plotly_chart(fig_vs, use_container_width=True)

        with tabs_co[3]:
            st.dataframe(
                df_companies,
                use_container_width=True, height=500
            )

# ─── FOOTER ──────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style='text-align:center; color:#3a3a6a; font-size:0.8rem; padding:1rem 0;'>
· Big Data Project · HK II 2025-2026
</div>
""", unsafe_allow_html=True)