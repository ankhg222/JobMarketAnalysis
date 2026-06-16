"""
Main Streamlit Application - IT Job Market CRUD
Stack: Streamlit + PySpark + Apache Hive + HDFS
"""

import sys
import os
import logging
import warnings
warnings.filterwarnings("ignore")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd

import config

# ─── Page Config (PHẢI gọi đầu tiên) ─────────────────────────────────────────
st.set_page_config(
    page_title=config.APP_TITLE,
    page_icon=config.APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": None,
        "Report a bug": None,
        "About": f"**{config.APP_TITLE}** | Spark + Hive + HDFS"
    }
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─── CSS Styling ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Google Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Root Variables ── */
:root {
    --bg-primary:   #0f172a;
    --bg-secondary: #1e293b;
    --bg-card:      #1e293b;
    --accent:       #6366f1;
    --accent-2:     #8b5cf6;
    --text-primary: #e2e8f0;
    --text-secondary:#94a3b8;
    --success:      #22c55e;
    --warning:      #f59e0b;
    --danger:       #ef4444;
    --border:       rgba(99,102,241,0.2);
    --radius:       12px;
    --shadow:       0 4px 24px rgba(0,0,0,0.4);
}

/* ── Global ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}

.main .block-container {
    padding: 1.5rem 2rem 3rem 2rem;
    max-width: 1400px;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1a1f35 100%) !important;
    border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] * {
    color: var(--text-primary) !important;
}
[data-testid="stSidebarNav"] {
    padding-top: 0.5rem;
}

/* ── Page Header ── */
.page-header {
    background: linear-gradient(135deg, rgba(99,102,241,0.15) 0%, rgba(139,92,246,0.1) 100%);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem 2rem;
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
}
.page-header::before {
    content: '';
    position: absolute;
    top: -50%;
    right: -20%;
    width: 300px;
    height: 300px;
    background: radial-gradient(circle, rgba(99,102,241,0.15) 0%, transparent 70%);
    border-radius: 50%;
}
.page-header h1 {
    font-size: 1.8rem !important;
    font-weight: 700 !important;
    margin: 0 0 0.3rem 0 !important;
    background: linear-gradient(135deg, #6366f1, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.page-header p {
    color: var(--text-secondary) !important;
    margin: 0 !important;
    font-size: 0.95rem;
}

/* ── KPI Cards ── */
.kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.2rem 1rem;
    text-align: center;
    transition: transform 0.2s, border-color 0.2s;
    box-shadow: var(--shadow);
}
.kpi-card:hover {
    transform: translateY(-2px);
    border-color: var(--accent);
}
.kpi-icon { font-size: 1.8rem; margin-bottom: 0.4rem; }
.kpi-value {
    font-size: 1.6rem;
    font-weight: 700;
    color: var(--accent);
    line-height: 1.2;
}
.kpi-label {
    font-size: 0.78rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-top: 0.3rem;
}

/* ── Chart Cards ── */
.chart-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem;
    box-shadow: var(--shadow);
    margin-bottom: 1rem;
}

/* ── Connection Cards ── */
.conn-card {
    background: var(--bg-card);
    border: 2px solid;
    border-radius: var(--radius);
    padding: 1.5rem;
    text-align: center;
    box-shadow: var(--shadow);
}
.conn-icon { font-size: 2rem; margin-bottom: 0.5rem; }
.conn-name { font-size: 1rem; font-weight: 600; margin-bottom: 0.3rem; }
.conn-status { font-size: 0.85rem; font-weight: 500; }

/* ── HDFS Info ── */
.hdfs-info {
    background: rgba(99,102,241,0.08);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem;
    margin-bottom: 1rem;
}
.hdfs-metric {
    display: flex;
    justify-content: space-between;
    padding: 0.4rem 0;
    border-bottom: 1px solid var(--border);
    font-size: 0.9rem;
}
.hdfs-metric:last-child { border-bottom: none; }
.hdfs-metric span { color: var(--text-secondary); }

/* ── Result Info ── */
.result-info {
    background: rgba(99,102,241,0.1);
    border-left: 3px solid var(--accent);
    padding: 0.6rem 1rem;
    border-radius: 0 8px 8px 0;
    margin-bottom: 1rem;
    font-size: 0.9rem;
    color: var(--text-secondary);
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: all 0.2s !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #6366f1, #8b5cf6) !important;
    border: none !important;
    color: white !important;
}
.stButton > button[kind="primary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 15px rgba(99,102,241,0.4) !important;
}

/* ── Input fields ── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stNumberInput > div > div > input,
.stSelectbox > div > div {
    background: var(--bg-secondary) !important;
    border: 1px solid var(--border) !important;
    color: var(--text-primary) !important;
    border-radius: 8px !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--bg-secondary) !important;
    border-radius: var(--radius) !important;
    padding: 4px !important;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    color: var(--text-secondary) !important;
    font-weight: 500;
}
.stTabs [aria-selected="true"] {
    background: var(--accent) !important;
    color: white !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    overflow: hidden;
}

/* ── Alert/Info boxes ── */
.stAlert {
    border-radius: var(--radius) !important;
}

/* ── Divider ── */
hr {
    border-color: var(--border) !important;
}

/* ── Logo / Brand ── */
.sidebar-brand {
    background: linear-gradient(135deg, rgba(99,102,241,0.2), rgba(139,92,246,0.1));
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem;
    margin-bottom: 1.5rem;
    text-align: center;
}
.sidebar-brand h2 {
    font-size: 1.1rem !important;
    margin: 0 !important;
    background: linear-gradient(135deg, #6366f1, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 700 !important;
}
.sidebar-brand p {
    font-size: 0.75rem !important;
    color: var(--text-secondary) !important;
    margin: 0.2rem 0 0 0 !important;
}

/* ── Spark status badge ── */
.spark-status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(34,197,94,0.15);
    border: 1px solid rgba(34,197,94,0.3);
    border-radius: 20px;
    padding: 3px 10px;
    font-size: 0.75rem;
    color: #22c55e;
}

/* ── Code blocks ── */
code {
    background: rgba(99,102,241,0.15) !important;
    color: #a78bfa !important;
    border-radius: 4px !important;
    padding: 1px 5px !important;
    font-family: 'JetBrains Mono', monospace !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--accent); }

/* ── Progress bar ── */
.stProgress > div > div {
    background: linear-gradient(90deg, #6366f1, #8b5cf6) !important;
    border-radius: 4px !important;
}
</style>
""", unsafe_allow_html=True)


# ─── Lazy load Spark operations ──────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _load_spark_ops():
    """Load Spark operations - cached singleton."""
    try:
        from src.spark_client import get_spark, test_connection
        from src.hive_schema import init_database, get_table_info
        from src.hive_operations import (
            read_jobs, insert_one, update_one, soft_delete,
            bulk_soft_delete, get_by_id, restore_deleted,
            get_deleted_records, get_stats, import_csv_to_hive
        )
        from src.backup_restore import (
            create_backup, list_backups, restore_backup,
            delete_backup, preview_backup, export_to_hdfs,
            get_hdfs_storage_info
        )
        return {
            "test_conn": test_connection,
            "init_db": init_database,
            "get_table_info": get_table_info,
            "read_jobs": read_jobs,
            "insert_one": insert_one,
            "update_one": update_one,
            "soft_delete": soft_delete,
            "bulk_delete": bulk_soft_delete,
            "get_by_id": get_by_id,
            "restore_deleted": restore_deleted,
            "get_deleted": get_deleted_records,
            "get_stats": get_stats,
            "import_csv": import_csv_to_hive,
            "create_backup": create_backup,
            "list_backups": list_backups,
            "restore_backup": restore_backup,
            "delete_backup": delete_backup,
            "preview_backup": preview_backup,
            "export": export_to_hdfs,
            "hdfs_info": get_hdfs_storage_info,
        }
    except Exception as e:
        logger.error(f"Failed to load Spark ops: {e}")
        return None


# ─── Sidebar Navigation ───────────────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-brand">
            <h2>💼 IT Job Market</h2>
            <p>Spark · Hive · HDFS</p>
        </div>
        """, unsafe_allow_html=True)

        # Spark status
        spark_ok = st.session_state.get("spark_connected", False)
        if spark_ok:
            st.markdown("""
            <div class="spark-status">⚡ Spark Connected</div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="color:#ef4444; font-size:0.8rem; margin-bottom:8px;">
            ⚠️ Spark chưa kết nối
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        nav = st.radio(
            "Navigation",
            ["📊 Dashboard", "🗃️ Quản Lý CRUD",
             "💾 Backup & Restore", "⚙️ Cài Đặt"],
            label_visibility="collapsed"
        )

        st.markdown("---")

        # Quick stats
        if "quick_stats" in st.session_state:
            qs = st.session_state["quick_stats"]
            st.markdown(f"""
            <div style="font-size:0.8rem; color:#94a3b8;">
                <div>📦 <b>{qs.get('total',0):,}</b> records</div>
                <div>🏢 <b>{qs.get('companies',0):,}</b> công ty</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("""
        <div style="font-size:0.7rem; color:#475569; text-align:center;">
            Hadoop 3.3.6 · Java 8<br>
            Hive 3.x · PySpark 3.5
        </div>
        """, unsafe_allow_html=True)

    return nav


# ─── Main App ─────────────────────────────────────────────────────────────────

def main():
    # Initialize session state
    if "spark_connected" not in st.session_state:
        st.session_state["spark_connected"] = False

    # Sidebar + navigation
    page = render_sidebar()

    # Load Spark ops
    ops = _load_spark_ops()

    # Auto update Spark status
    if ops:
        st.session_state["spark_connected"] = True
    else:
        st.session_state["spark_connected"] = False

    # Import pages
    from src.pages.dashboard import render_dashboard
    from src.pages.crud_page import render_crud
    from src.pages.backup_page import render_backup_restore
    from src.pages.settings_page import render_settings

    # ── Route pages ─────────────────────────────────────────────────────────
    if page == "📊 Dashboard":
        if ops:
            # Cache quick stats
            def get_stats_cached():
                s = ops["get_stats"]()
                summary = s.get("summary", {})
                st.session_state["quick_stats"] = {
                    "total": summary.get("total", 0) or 0,
                    "companies": summary.get("companies", 0) or 0,
                }
                return s
            render_dashboard(get_stats_cached, ops["read_jobs"])
        else:
            _no_spark_warning()

    elif page == "🗃️ Quản Lý CRUD":
        if ops:
            render_crud(
                read_fn=ops["read_jobs"],
                insert_fn=ops["insert_one"],
                update_fn=ops["update_one"],
                delete_fn=ops["soft_delete"],
                get_by_id_fn=ops["get_by_id"],
                restore_fn=ops["restore_deleted"],
                get_deleted_fn=ops["get_deleted"],
                bulk_delete_fn=ops["bulk_delete"],
            )
        else:
            _no_spark_warning()

    elif page == "💾 Backup & Restore":
        if ops:
            render_backup_restore(
                list_backups_fn=ops["list_backups"],
                create_backup_fn=ops["create_backup"],
                restore_backup_fn=ops["restore_backup"],
                delete_backup_fn=ops["delete_backup"],
                preview_backup_fn=ops["preview_backup"],
                export_fn=ops["export"],
                hdfs_info_fn=ops["hdfs_info"],
                import_csv_fn=ops["import_csv"],
            )
        else:
            _no_spark_warning()

    elif page == "⚙️ Cài Đặt":
        if ops:
            def test_and_update():
                result = ops["test_conn"]()
                if result.get("spark") and result.get("hive"):
                    st.session_state["spark_connected"] = True
                else:
                    st.session_state["spark_connected"] = False
                return result

            render_settings(
                test_conn_fn=test_and_update,
                init_db_fn=ops["init_db"],
                get_table_info_fn=ops["get_table_info"],
            )
        else:
            # Settings page vẫn show kể cả khi Spark chưa ready
            from src.pages.settings_page import render_settings, _render_config
            st.markdown("""
            <div class="page-header">
                <h1>⚙️ Cài Đặt & Trạng Thái</h1>
            </div>
            """, unsafe_allow_html=True)
            st.error("⚠️ Không load được Spark. Kiểm tra cấu hình bên dưới.")
            _render_config()


def _no_spark_warning():
    st.warning("""
    ⚠️ **Spark chưa sẵn sàng**

    Vào **⚙️ Cài Đặt** → **Khởi Tạo Schema** để thiết lập.

    Hoặc chạy lệnh setup:
    """)
    st.code("""
# 1. Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# 2. Tạo HDFS directories
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/backups/it_jobs
hdfs dfs -chmod -R 777 /user/hive

# 3. Init Hive schema
$HIVE_HOME/bin/schematool -dbType derby -initSchema

# 4. Start Hive Metastore (optional)
$HIVE_HOME/bin/hive --service metastore &

# 5. Reload app
    """, language="bash")


if __name__ == "__main__":
    main()
