"""
Streamlit Pages - Dashboard với charts và analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import config


def render_dashboard(get_stats_fn, get_jobs_fn):
    """Render trang Dashboard."""
    
    st.markdown("""
    <div class="page-header">
        <h1>📊 Dashboard Thị Trường IT</h1>
        <p>Tổng quan thị trường việc làm công nghệ thông tin Việt Nam</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Load data ─────────────────────────────────────────────────────────────
    with st.spinner("Đang tải dữ liệu..."):
        try:
            stats = get_stats_fn()
        except Exception as e:
            st.error(f"Lỗi tải dữ liệu: {e}")
            _render_demo_dashboard()
            return

    summary = stats["summary"]
    by_level = stats["by_level"]
    by_location = stats["by_location"]
    by_source = stats["by_source"]

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    
    def kpi_card(col, icon, label, value, delta=None):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon">{icon}</div>
                <div class="kpi-value">{value}</div>
                <div class="kpi-label">{label}</div>
                {f'<div class="kpi-delta">{delta}</div>' if delta else ''}
            </div>
            """, unsafe_allow_html=True)

    total = summary.get("total", 0) or 0
    companies = summary.get("companies", 0) or 0
    locations = summary.get("locations", 0) or 0
    avg_sal = summary.get("avg_salary", 0) or 0
    avg_yoe = summary.get("avg_yoe", 0) or 0

    kpi_card(col1, "💼", "Tổng Việc Làm", f"{total:,}")
    kpi_card(col2, "🏢", "Công Ty", f"{companies:,}")
    kpi_card(col3, "📍", "Địa Điểm", f"{locations:,}")
    kpi_card(col4, "💰", "Lương TB", f"{avg_sal/1e6:.1f}M₫")
    kpi_card(col5, "🎓", "Kinh Nghiệm TB", f"{avg_yoe:.1f} năm")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts Row 1 ──────────────────────────────────────────────────────────
    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        if not by_level.empty:
            fig = px.bar(
                by_level.head(8),
                x="count", y="job_level",
                orientation="h",
                title="🎯 Số Lượng Theo Cấp Độ",
                color="count",
                color_continuous_scale="Viridis",
                text="count"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                title_font_size=14,
                showlegend=False,
                coloraxis_showscale=False,
                height=350
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        if not by_location.empty:
            fig = px.pie(
                by_location.head(8),
                values="count", names="location_clean",
                title="🗺️ Phân Bố Theo Địa Điểm",
                hole=0.45,
                color_discrete_sequence=px.colors.sequential.Plasma_r
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                title_font_size=14,
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Charts Row 2 ──────────────────────────────────────────────────────────
    col_l2, col_r2 = st.columns([3, 2])

    with col_l2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        # Salary distribution - load sample data
        try:
            pdf, _ = get_jobs_fn(page_size=500)
            if not pdf.empty and "salary_final_vnd" in pdf.columns:
                pdf_sal = pdf[pdf["salary_final_vnd"].notna() & (pdf["salary_final_vnd"] > 0)]
                if not pdf_sal.empty:
                    fig = px.histogram(
                        pdf_sal,
                        x="salary_final_vnd",
                        nbins=30,
                        title="💸 Phân Phối Lương (VND)",
                        color_discrete_sequence=["#6366f1"],
                        labels={"salary_final_vnd": "Lương (VND)"}
                    )
                    fig.update_layout(
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#e2e8f0"),
                        title_font_size=14,
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.info("Biểu đồ lương sẽ hiển thị sau khi import dữ liệu")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        if not by_source.empty:
            fig = px.bar(
                by_source,
                x="source", y="count",
                title="📡 Nguồn Tuyển Dụng",
                color="count",
                color_continuous_scale="Blues",
                text="count"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                title_font_size=14,
                showlegend=False,
                coloraxis_showscale=False,
                height=300
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Top Skills ─────────────────────────────────────────────────────────────
    try:
        pdf, _ = get_jobs_fn(page_size=500)
        if not pdf.empty and "skills_clean" in pdf.columns:
            all_skills = []
            for s in pdf["skills_clean"].dropna():
                all_skills.extend([sk.strip() for sk in s.split(",") if sk.strip()])
            
            if all_skills:
                skill_counts = pd.Series(all_skills).value_counts().head(20)
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                fig = px.bar(
                    x=skill_counts.values,
                    y=skill_counts.index,
                    orientation="h",
                    title="🔧 Top 20 Kỹ Năng Được Yêu Cầu",
                    color=skill_counts.values,
                    color_continuous_scale="Turbo",
                    labels={"x": "Số lượng", "y": "Kỹ năng"}
                )
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#e2e8f0"),
                    title_font_size=14,
                    coloraxis_showscale=False,
                    height=500,
                    yaxis={"categoryorder": "total ascending"}
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
    except Exception:
        pass


def _render_demo_dashboard():
    """Demo dashboard khi chưa có data."""
    st.info("ℹ️ Chưa có dữ liệu. Vào **Import Data** để tải dataset lên Hive.")
    st.markdown("""
    ### 🚀 Bắt đầu nhanh:
    1. Vào **⚙️ Cài đặt** → kiểm tra kết nối Spark/Hive
    2. Vào **📥 Import Data** → upload CSV
    3. Quay lại Dashboard để xem thống kê
    """)
