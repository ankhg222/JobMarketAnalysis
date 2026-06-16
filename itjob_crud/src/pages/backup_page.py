"""
Streamlit Pages - Backup & Restore Management
"""

import streamlit as st
import pandas as pd
from datetime import datetime

import config


def render_backup_restore(
    list_backups_fn,
    create_backup_fn,
    restore_backup_fn,
    delete_backup_fn,
    preview_backup_fn,
    export_fn,
    hdfs_info_fn,
    import_csv_fn,
):
    """Render Backup/Restore page."""

    st.markdown("""
    <div class="page-header">
        <h1>💾 Sao Lưu & Phục Hồi</h1>
        <p>Quản lý backup Hive tables trên HDFS</p>
    </div>
    """, unsafe_allow_html=True)

    tab_backup, tab_restore, tab_import, tab_export = st.tabs([
        "📸 Tạo Backup", "♻️ Phục Hồi", "📥 Import CSV", "📤 Export"
    ])

    with tab_backup:
        _render_backup_tab(list_backups_fn, create_backup_fn,
                          delete_backup_fn, hdfs_info_fn)

    with tab_restore:
        _render_restore_tab(list_backups_fn, restore_backup_fn, preview_backup_fn)

    with tab_import:
        _render_import_tab(import_csv_fn)

    with tab_export:
        _render_export_tab(export_fn, hdfs_info_fn)


# ══════════════════════════════════════════════════════════════════════
# BACKUP TAB
# ══════════════════════════════════════════════════════════════════════

def _render_backup_tab(list_fn, create_fn, delete_fn, hdfs_fn):
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("📸 Tạo Backup Mới")

        with st.form("backup_form"):
            bname = st.text_input(
                "Tên backup",
                placeholder="vd: BEFORE_CLEANUP_2024",
                help="Để trống để tự động đặt tên theo thời gian"
            )
            bdesc = st.text_area("Mô tả", placeholder="Mô tả lý do tạo backup...",
                                  height=80)
            include_del = st.checkbox("Bao gồm records đã xóa mềm")
            submit = st.form_submit_button("💾 Tạo Backup", type="primary",
                                           use_container_width=True)

        if submit:
            with st.spinner("Đang tạo backup lên HDFS..."):
                try:
                    ok, bid, count = create_fn(bname, bdesc, include_del)
                    if ok:
                        st.success(f"""
                        ✅ **Backup thành công!**  
                        🆔 ID: `{bid}`  
                        📦 {count:,} records  
                        """)
                        st.rerun()
                except Exception as e:
                    st.error(f"Lỗi tạo backup: {e}")

    with col_right:
        st.subheader("💿 HDFS Storage")
        try:
            info = hdfs_fn()
            if info:
                used_pct = info.get("used_pct", 0)
                col = "🟢" if used_pct < 70 else ("🟡" if used_pct < 90 else "🔴")
                st.markdown(f"""
                <div class="hdfs-info">
                    <div class="hdfs-metric">
                        <span>Tổng dung lượng</span>
                        <b>{info.get('capacity_gb', '?')} GB</b>
                    </div>
                    <div class="hdfs-metric">
                        <span>Đã dùng</span>
                        <b>{info.get('used_gb', '?')} GB</b>
                    </div>
                    <div class="hdfs-metric">
                        <span>Còn trống</span>
                        <b>{info.get('remaining_gb', '?')} GB</b>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.progress(used_pct / 100,
                            text=f"{col} Sử dụng: {used_pct}%")
        except Exception as e:
            st.warning(f"Không lấy được HDFS info: {e}")

    # ── Backup List ─────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"📋 Danh Sách Backups (tối đa {config.BACKUP_MAX_VERSIONS})")

    try:
        df = list_fn()
        if df.empty:
            st.info("Chưa có backup nào.")
        else:
            for _, row in df.iterrows():
                with st.container():
                    c1, c2, c3, c4, c5 = st.columns([2, 3, 2, 2, 1])
                    with c1:
                        st.markdown(f"**🆔 `{row.get('backup_id','')}`**")
                    with c2:
                        st.markdown(f"📝 {row.get('backup_name','')}")
                        desc = row.get("description", "")
                        if desc:
                            st.caption(desc[:60] + ("..." if len(str(desc)) > 60 else ""))
                    with c3:
                        count = row.get("record_count", 0)
                        st.markdown(f"📦 {count:,} records")
                    with c4:
                        ts = row.get("created_at", "")
                        if hasattr(ts, "strftime"):
                            ts = ts.strftime("%d/%m/%Y %H:%M")
                        st.markdown(f"🕐 {ts}")
                    with c5:
                        if st.button("🗑️", key=f"del_{row.get('backup_id','')}_btn",
                                     help="Xóa backup này"):
                            try:
                                delete_fn(row["backup_id"])
                                st.success("Đã xóa backup")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                    st.divider()
    except Exception as e:
        st.error(f"Lỗi tải danh sách backup: {e}")


# ══════════════════════════════════════════════════════════════════════
# RESTORE TAB
# ══════════════════════════════════════════════════════════════════════

def _render_restore_tab(list_fn, restore_fn, preview_fn):
    st.subheader("♻️ Phục Hồi Từ Backup")

    try:
        df_backups = list_fn()
    except Exception as e:
        st.error(f"Không tải được danh sách backup: {e}")
        return

    if df_backups.empty:
        st.info("Chưa có backup nào để restore.")
        return

    # Select backup
    options = {
        f"{row['backup_id']} — {row['backup_name']} ({row.get('record_count',0):,} records, "
        f"{row.get('created_at','')})": row["backup_id"]
        for _, row in df_backups.iterrows()
    }

    selected_label = st.selectbox("Chọn backup", list(options.keys()))
    selected_id = options[selected_label]

    # Preview
    if st.button("👁️ Xem trước dữ liệu"):
        with st.spinner("Đang đọc từ HDFS..."):
            try:
                preview_df = preview_fn(selected_id, limit=10)
                st.dataframe(preview_df, hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"Lỗi preview: {e}")

    st.markdown("---")

    restore_mode = st.radio(
        "Chế độ restore",
        ["replace", "merge", "append"],
        captions=[
            "🔴 Xóa data hiện tại, thay bằng backup (auto-backup trước khi replace)",
            "🟡 Chỉ thêm records chưa tồn tại trong Hive",
            "🟢 Thêm tất cả records từ backup vào Hive"
        ]
    )

    st.warning("""
    ⚠️ **Lưu ý:**  
    - Chế độ **replace** sẽ tự động backup data hiện tại trước khi thay thế.  
    - Thao tác này không thể hoàn tác trực tiếp (nhưng auto-backup sẽ giữ lại).
    """)

    confirm = st.checkbox("Tôi hiểu và xác nhận muốn restore")

    if confirm and st.button("▶️ Bắt đầu Restore", type="primary",
                              use_container_width=True):
        with st.spinner("Đang restore..."):
            try:
                ok, count = restore_fn(selected_id, restore_mode)
                if ok:
                    st.success(f"""
                    ✅ **Restore thành công!**  
                    📦 {count:,} records đã được restore  
                    Chế độ: **{restore_mode}**
                    """)
                    st.balloons()
            except Exception as e:
                st.error(f"Restore thất bại: {e}")


# ══════════════════════════════════════════════════════════════════════
# IMPORT CSV TAB
# ══════════════════════════════════════════════════════════════════════

def _render_import_tab(import_fn):
    st.subheader("📥 Import CSV vào Hive")

    st.info("""
    ℹ️ **Hướng dẫn:**  
    - File CSV phải có header row  
    - Các cột cần có: `title_clean`, `company`, `source`  
    - Encoding: UTF-8  
    """)

    # Option 1: Upload file
    st.markdown("**Option 1: Upload file CSV**")
    uploaded = st.file_uploader("Chọn file CSV", type=["csv"])

    # Option 2: HDFS path
    st.markdown("**Option 2: HDFS path**")
    hdfs_path = st.text_input(
        "HDFS path",
        placeholder="hdfs://localhost:9000/user/uploads/jobs.csv"
    )

    # Option 3: Local path
    st.markdown("**Option 3: Local path trên Ubuntu server**")
    local_path = st.text_input(
        "Local path",
        placeholder="/home/ubuntu/data/Data_ITJOB_Cleaned.csv",
        value=config.CSV_DATA_FILE
    )

    overwrite = st.checkbox("Ghi đè data hiện tại (OVERWRITE)")

    if overwrite:
        st.warning("⚠️ OVERWRITE sẽ xóa toàn bộ data cũ trong Hive!")

    if st.button("📥 Bắt đầu Import", type="primary", use_container_width=True):
        import_path = None

        if uploaded:
            # Lưu file tạm
            import os
            tmp_path = os.path.join(config.TEMP_DIR, uploaded.name)
            with open(tmp_path, "wb") as f:
                f.write(uploaded.getvalue())
            import_path = tmp_path
        elif hdfs_path.strip():
            import_path = hdfs_path.strip()
        elif local_path.strip():
            import_path = local_path.strip()

        if not import_path:
            st.error("Chọn nguồn dữ liệu!")
            return

        with st.spinner(f"Đang import từ {import_path}..."):
            try:
                ok, count = import_fn(import_path, overwrite)
                if ok:
                    st.success(f"✅ Import thành công! {count:,} records")
                    st.balloons()
            except Exception as e:
                st.error(f"Import thất bại: {e}")


# ══════════════════════════════════════════════════════════════════════
# EXPORT TAB
# ══════════════════════════════════════════════════════════════════════

def _render_export_tab(export_fn, hdfs_fn):
    st.subheader("📤 Export Data từ Hive")

    col1, col2 = st.columns(2)
    with col1:
        fmt = st.selectbox("Format xuất", ["parquet", "csv", "json"])
    with col2:
        custom_path = st.text_input(
            "HDFS path (tùy chọn)",
            placeholder="Để trống = tự động"
        )

    if st.button("📤 Xuất ra HDFS", type="primary", use_container_width=True):
        with st.spinner("Đang xuất..."):
            try:
                path = export_fn(custom_path.strip() or "", fmt)
                st.success(f"""
                ✅ **Export thành công!**  
                📁 Path: `{path}`  
                Format: **{fmt}**
                """)
            except Exception as e:
                st.error(f"Export thất bại: {e}")
