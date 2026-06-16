"""
Streamlit Pages - Settings & Connection Status
"""

import streamlit as st
import os

import config


def render_settings(test_conn_fn, init_db_fn, get_table_info_fn):
    """Render Settings page."""

    st.markdown("""
    <div class="page-header">
        <h1>⚙️ Cài Đặt & Trạng Thái</h1>
        <p>Kiểm tra kết nối và cấu hình hệ thống</p>
    </div>
    """, unsafe_allow_html=True)

    tab_conn, tab_db, tab_config = st.tabs([
        "🔌 Kết Nối", "🗄️ Database", "⚙️ Cấu Hình"
    ])

    with tab_conn:
        _render_connection(test_conn_fn)

    with tab_db:
        _render_database(init_db_fn, get_table_info_fn)

    with tab_config:
        _render_config()


def _render_connection(test_fn):
    st.subheader("🔌 Kiểm Tra Kết Nối")

    col1, col2, col3 = st.columns(3)

    def status_card(col, name, ok, icon):
        with col:
            color = "#22c55e" if ok else "#ef4444"
            status = "✅ Kết nối" if ok else "❌ Lỗi"
            st.markdown(f"""
            <div class="conn-card" style="border-color: {color}">
                <div class="conn-icon">{icon}</div>
                <div class="conn-name">{name}</div>
                <div class="conn-status" style="color: {color}">{status}</div>
            </div>
            """, unsafe_allow_html=True)

    if st.button("🔄 Kiểm Tra Kết Nối", type="primary", use_container_width=True):
        with st.spinner("Đang kiểm tra..."):
            try:
                result = test_fn()
                status_card(col1, "Apache Spark", result.get("spark", False), "⚡")
                status_card(col2, "Apache Hive", result.get("hive", False), "🐝")
                status_card(col3, "HDFS", result.get("hdfs", False), "📁")

                if result.get("error"):
                    st.error(f"Chi tiết lỗi: {result['error']}")
                elif all([result.get("spark"), result.get("hive"), result.get("hdfs")]):
                    st.success("🎉 Tất cả dịch vụ hoạt động bình thường!")
            except Exception as e:
                st.error(f"Lỗi kiểm tra kết nối: {e}")

    st.markdown("---")
    st.subheader("🔧 Thông Tin Cấu Hình Hiện Tại")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"""
        | Thành phần | Giá trị |
        |:-----------|:--------|
        | HDFS Host | `{config.HDFS_HOST}:{config.HDFS_PORT}` |
        | Spark Master | `{config.SPARK_MASTER}` |
        | Hive Database | `{config.HIVE_DATABASE}` |
        | Main Table | `{config.MAIN_TABLE}` |
        """)
    with col2:
        st.markdown(f"""
        | Thành phần | Giá trị |
        |:-----------|:--------|
        | Spark App Name | `{config.SPARK_APP_NAME}` |
        | Executor Memory | `{config.SPARK_EXECUTOR_MEMORY}` |
        | Driver Memory | `{config.SPARK_DRIVER_MEMORY}` |
        | Max Backups | `{config.BACKUP_MAX_VERSIONS}` |
        """)

    st.markdown("---")
    st.subheader("💡 Hướng Dẫn Khắc Phục")
    with st.expander("Spark không kết nối được"):
        st.code("""
# Kiểm tra JAVA_HOME
echo $JAVA_HOME
java -version

# Kiểm tra SPARK_HOME
echo $SPARK_HOME
$SPARK_HOME/bin/spark-shell --version

# Start Spark standalone (nếu dùng cluster)
$SPARK_HOME/sbin/start-all.sh
        """, language="bash")

    with st.expander("HDFS không kết nối được"):
        st.code("""
# Kiểm tra HDFS status
hdfs dfsadmin -report

# Start HDFS nếu chưa chạy
$HADOOP_HOME/sbin/start-dfs.sh

# Kiểm tra port
ss -tlnp | grep 9000

# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/backups/it_jobs
hdfs dfs -chmod 777 /user/hive/warehouse
        """, language="bash")

    with st.expander("Hive Metastore lỗi"):
        st.code("""
# Init schema Hive (lần đầu)
$HIVE_HOME/bin/schematool -dbType derby -initSchema
# Hoặc MySQL:
$HIVE_HOME/bin/schematool -dbType mysql -initSchema

# Start metastore
$HIVE_HOME/bin/hive --service metastore &

# Check metastore log
tail -f /tmp/root/hive.log
        """, language="bash")


def _render_database(init_fn, table_info_fn):
    st.subheader("🗄️ Quản Lý Database")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚀 Khởi Tạo Schema", type="primary", use_container_width=True,
                     help="Tạo database + tables trong Hive (an toàn, không xóa data)"):
            with st.spinner("Đang khởi tạo schema..."):
                try:
                    ok = init_fn()
                    if ok:
                        st.success("✅ Schema đã sẵn sàng!")
                except Exception as e:
                    st.error(f"Lỗi: {e}")

    with col2:
        if st.button("🔄 Làm mới thông tin", use_container_width=True):
            st.rerun()

    st.markdown("---")
    st.subheader("📊 Thông Tin Tables")

    with st.spinner("Đang tải thông tin tables..."):
        try:
            info = table_info_fn()
            if info:
                rows = []
                for tname, count in info.items():
                    rows.append({
                        "Table": tname,
                        "Database": config.HIVE_DATABASE,
                        "Records": count if count >= 0 else "N/A",
                        "Status": "✅ Active" if count >= 0 else "⚠️ Error"
                    })
                st.dataframe(pd.DataFrame(rows) if rows else pd.DataFrame(),
                             hide_index=True, use_container_width=True)
            else:
                st.info("Database chưa được khởi tạo. Nhấn 'Khởi Tạo Schema' ở trên.")
        except Exception as e:
            st.warning(f"Chưa tải được thông tin: {e}")


def _render_config():
    st.subheader("⚙️ Cấu Hình Qua Environment Variables")

    st.info("""
    Chỉnh cấu hình bằng cách set environment variables trước khi chạy app:
    """)

    st.code("""
# ~/.bashrc hoặc /etc/environment

# HDFS
export HDFS_HOST=localhost          # IP của HDFS NameNode
export HDFS_PORT=9000               # Port HDFS

# Spark
export SPARK_MASTER=local[*]        # local[*] hoặc spark://host:7077
export SPARK_HOME=/opt/spark

# Hive
export HIVE_HOST=localhost          # Host Hive Thrift Server
export HIVE_PORT=10000
export HIVE_DATABASE=itjobs_db

# Reload
source ~/.bashrc

# Start app
cd /opt/itjob_crud
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
    """, language="bash")

    st.markdown("---")
    st.subheader("🌐 Cấu Hình Streamlit")
    
    st.code("""
# ~/.streamlit/config.toml
[server]
port = 8501
address = "0.0.0.0"
maxUploadSize = 500       # MB

[browser]
gatherUsageStats = false
serverAddress = "localhost"

[theme]
primaryColor = "#6366f1"
backgroundColor = "#0f172a"
secondaryBackgroundColor = "#1e293b"
textColor = "#e2e8f0"
    """, language="toml")


# Fix missing import in _render_database
try:
    import pandas as pd
except ImportError:
    pass
