"""
Streamlit Pages - CRUD Operations
Xem, thêm, sửa, xóa records
"""

import streamlit as st
import pandas as pd
from datetime import datetime

import config


def render_crud(read_fn, insert_fn, update_fn, delete_fn,
                get_by_id_fn, restore_fn, get_deleted_fn, bulk_delete_fn):
    """Render CRUD page."""

    st.markdown("""
    <div class="page-header">
        <h1>🗃️ Quản Lý Dữ Liệu</h1>
        <p>Xem, thêm, chỉnh sửa và xóa records</p>
    </div>
    """, unsafe_allow_html=True)

    tab_list, tab_add, tab_edit, tab_delete, tab_deleted = st.tabs([
        "📋 Danh Sách", "➕ Thêm Mới",
        "✏️ Chỉnh Sửa", "🗑️ Xóa",
        "♻️ Đã Xóa"
    ])

    with tab_list:
        _render_list(read_fn)

    with tab_add:
        _render_add(insert_fn)

    with tab_edit:
        _render_edit(get_by_id_fn, update_fn)

    with tab_delete:
        _render_delete(read_fn, delete_fn, bulk_delete_fn)

    with tab_deleted:
        _render_deleted(get_deleted_fn, restore_fn)


# ═══════════════════════════════════════════════════════════════════════════════
# LIST / SEARCH
# ═══════════════════════════════════════════════════════════════════════════════

def _render_list(read_fn):
    st.subheader("🔍 Tìm Kiếm & Lọc")

    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        search = st.text_input("🔎 Tìm kiếm (chức danh, công ty, kỹ năng)",
                               placeholder="vd: python developer, FPT Software...")
    with col2:
        page_size = st.selectbox("Số dòng/trang", [10, 20, 50, 100], index=1)
    with col3:
        sort_col = st.selectbox("Sắp xếp theo",
                                ["id", "salary_final_vnd", "yoe_extracted",
                                 "title_clean", "company"])

    with st.expander("🔧 Bộ lọc nâng cao", expanded=False):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            locs = st.multiselect("Địa điểm", config.LOCATIONS)
            levels = st.multiselect("Cấp độ", config.JOB_LEVELS)
        with col_b:
            sal_min = st.number_input("Lương tối thiểu (VND)", min_value=0,
                                      value=0, step=5_000_000)
            sal_max = st.number_input("Lương tối đa (VND)", min_value=0,
                                      value=0, step=5_000_000,
                                      help="0 = không giới hạn")
        with col_c:
            is_remote = st.selectbox("Remote", ["Tất cả", "Remote", "Onsite"])
            yoe_max = st.number_input("Kinh nghiệm tối đa (năm)",
                                     min_value=0, value=0, step=1)

    filters = {}
    if locs:
        filters["location"] = locs
    if levels:
        filters["job_level"] = levels
    if sal_min > 0:
        filters["salary_min"] = sal_min
    if sal_max > 0:
        filters["salary_max"] = sal_max
    if is_remote == "Remote":
        filters["is_remote"] = 1
    elif is_remote == "Onsite":
        filters["is_remote"] = 0
    if yoe_max > 0:
        filters["yoe_max"] = yoe_max

    # Pagination state
    if "list_page" not in st.session_state:
        st.session_state.list_page = 1

    with st.spinner("Đang tải..."):
        try:
            df, total = read_fn(
                filters=filters,
                search_text=search,
                page=st.session_state.list_page,
                page_size=page_size,
                sort_col=sort_col,
            )
        except Exception as e:
            st.error(f"Lỗi: {e}")
            return

    total_pages = max(1, (total + page_size - 1) // page_size)

    # Fix page vượt quá số trang thực tế
    if st.session_state.list_page > total_pages:
        st.session_state.list_page = total_pages

    if st.session_state.list_page < 1:
        st.session_state.list_page = 1
    st.markdown(f"""
    <div class="result-info">
        Tìm thấy <b>{total:,}</b> records · 
        Trang {st.session_state.list_page}/{total_pages}
    </div>
    """, unsafe_allow_html=True)

    if not df.empty:
        # Format display
        display_df = _format_display(df)
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400,
            hide_index=True,
            column_config={
                "Lương (VND)": st.column_config.NumberColumn(format="%.0f ₫"),
                "URL": st.column_config.LinkColumn("Link"),
                "Remote": st.column_config.CheckboxColumn(),
            }
        )

        # Download
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            "📥 Tải xuống CSV",
            data=csv_data,
            file_name=f"it_jobs_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
    else:
        st.info("Không tìm thấy kết quả phù hợp.")

    # Pagination controls
    col_prev, col_info, col_next = st.columns([1, 3, 1])
    with col_prev:
        if st.button("← Trước", disabled=st.session_state.list_page <= 1):
            st.session_state.list_page -= 1
            st.rerun()
    with col_next:
        if st.button("Sau →", disabled=st.session_state.list_page >= total_pages):
            st.session_state.list_page += 1
            st.rerun()


def _format_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame cho hiển thị."""
    rename_map = {
        "id": "ID",
        "source": "Nguồn",
        "title_clean": "Vị Trí",
        "company": "Công Ty",
        "salary_final_vnd": "Lương (VND)",
        "location_clean": "Địa Điểm",
        "job_level": "Cấp Độ",
        "yoe_extracted": "Năm KN",
        "skills_clean": "Kỹ Năng",
        "is_remote": "Remote",
        "url": "URL",
    }
    cols = [c for c in rename_map if c in df.columns]
    out = df[cols].rename(columns=rename_map)
    if "Remote" in out.columns:
        out["Remote"] = out["Remote"].apply(lambda x: bool(x) if pd.notna(x) else False)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# ADD NEW
# ═══════════════════════════════════════════════════════════════════════════════

def _render_add(insert_fn):
    st.subheader("➕ Thêm Việc Làm Mới")

    with st.form("add_job_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            title = st.text_input("Chức danh *", placeholder="vd: Senior Python Developer")
            company = st.text_input("Công ty *", placeholder="vd: FPT Software")
            source = st.selectbox("Nguồn", ["ITviec", "TopDev", "VietnamWorks",
                                             "LinkedIn", "Khác"])
            location = st.selectbox("Địa điểm", config.LOCATIONS + ["Khác"])
            job_level = st.selectbox("Cấp độ", config.JOB_LEVELS)
            is_remote = st.checkbox("Remote")

        with col2:
            salary_min = st.number_input("Lương tối thiểu (VND)", min_value=0,
                                         step=1_000_000, value=10_000_000)
            salary_max = st.number_input("Lương tối đa (VND)", min_value=0,
                                         step=1_000_000, value=20_000_000)
            yoe = st.number_input("Số năm kinh nghiệm", min_value=0.0,
                                  max_value=30.0, step=0.5)
            skills = st.text_area("Kỹ năng (phân cách bằng dấu phẩy)",
                                  placeholder="python, sql, spark, machine learning",
                                  height=80)
            url = st.text_input("Link tuyển dụng", placeholder="https://...")

        description = st.text_area("Mô tả công việc", height=120)

        submitted = st.form_submit_button("💾 Lưu", use_container_width=True,
                                          type="primary")

    if submitted:
        if not title or not company:
            st.error("⚠️ Chức danh và Công ty là bắt buộc!")
            return

        skill_list = [s.strip() for s in skills.split(",") if s.strip()]
        salary_avg = (salary_min + salary_max) / 2 if salary_max > 0 else salary_min

        record = {
            "source": source,
            "title_clean": title,
            "company": company,
            "salary": f"{salary_min/1e6:.0f}-{salary_max/1e6:.0f}M",
            "salary_clean": f"{salary_min/1e6:.0f}-{salary_max/1e6:.0f}M",
            "salary_min_vnd": float(salary_min),
            "salary_max_vnd": float(salary_max),
            "salary_final_vnd": float(salary_avg),
            "is_AI_predicted": 0,
            "impute_method": "manual",
            "location_clean": location,
            "is_remote": 1 if is_remote else 0,
            "job_level": job_level,
            "yoe_extracted": float(yoe),
            "skills_clean": ", ".join(skill_list),
            "skill_count": len(skill_list),
            "description_clean": description,
            "url": url,
        }

        with st.spinner("Đang lưu..."):
            try:
                new_id = insert_fn(record)
                st.success(f"✅ Đã thêm thành công! ID: {new_id}")
                st.balloons()
            except Exception as e:
                st.error(f"Lỗi: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# EDIT
# ═══════════════════════════════════════════════════════════════════════════════

def _render_edit(get_by_id_fn, update_fn):
    st.subheader("✏️ Chỉnh Sửa Record")

    job_id = st.number_input("Nhập ID cần sửa", min_value=1, step=1, value=1)

    if st.button("🔍 Tải Record"):
        with st.spinner("Đang tải..."):
            try:
                record = get_by_id_fn(int(job_id))
                if record:
                    st.session_state["edit_record"] = record
                    st.success(f"Đã tải record ID: {job_id}")
                else:
                    st.warning(f"Không tìm thấy record ID: {job_id}")
            except Exception as e:
                st.error(f"Lỗi: {e}")

    if "edit_record" in st.session_state:
        rec = st.session_state["edit_record"]

        st.markdown("---")
        st.markdown(f"**Đang sửa:** `{rec.get('title_clean', '')}` - `{rec.get('company', '')}`")

        with st.form("edit_form"):
            col1, col2 = st.columns(2)
            with col1:
                title = st.text_input("Chức danh", value=rec.get("title_clean", ""))
                company = st.text_input("Công ty", value=rec.get("company", ""))
                location = st.text_input("Địa điểm", value=rec.get("location_clean", ""))
                job_level = st.selectbox(
                    "Cấp độ",
                    config.JOB_LEVELS,
                    index=config.JOB_LEVELS.index(rec.get("job_level", "Undefined"))
                    if rec.get("job_level") in config.JOB_LEVELS else 0
                )
            with col2:
                sal_min = st.number_input("Lương tối thiểu",
                                          value=float(rec.get("salary_min_vnd") or 0),
                                          step=1_000_000.0)
                sal_max = st.number_input("Lương tối đa",
                                          value=float(rec.get("salary_max_vnd") or 0),
                                          step=1_000_000.0)
                yoe = st.number_input("Năm kinh nghiệm",
                                      value=float(rec.get("yoe_extracted") or 0),
                                      step=0.5)
                skills = st.text_area("Kỹ năng",
                                      value=rec.get("skills_clean", ""),
                                      height=80)

            url = st.text_input("URL", value=rec.get("url", ""))
            is_remote = st.checkbox("Remote", value=bool(rec.get("is_remote", 0)))
            description = st.text_area("Mô tả", value=rec.get("description_clean", ""),
                                       height=100)

            save_btn = st.form_submit_button("💾 Lưu thay đổi", type="primary",
                                             use_container_width=True)

        if save_btn:
            skill_list = [s.strip() for s in skills.split(",") if s.strip()]
            sal_avg = (sal_min + sal_max) / 2 if sal_max > 0 else sal_min

            updates = {
                "title_clean": title,
                "company": company,
                "location_clean": location,
                "job_level": job_level,
                "salary_min_vnd": sal_min,
                "salary_max_vnd": sal_max,
                "salary_final_vnd": sal_avg,
                "yoe_extracted": yoe,
                "skills_clean": ", ".join(skill_list),
                "skill_count": len(skill_list),
                "url": url,
                "is_remote": 1 if is_remote else 0,
                "description_clean": description,
            }

            with st.spinner("Đang cập nhật..."):
                try:
                    ok = update_fn(int(rec["id"]), updates)
                    if ok:
                        st.success("✅ Đã cập nhật thành công!")
                        del st.session_state["edit_record"]
                        st.rerun()
                    else:
                        st.error("Không thể cập nhật record.")
                except Exception as e:
                    st.error(f"Lỗi: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# DELETE
# ═══════════════════════════════════════════════════════════════════════════════

def _render_delete(read_fn, delete_fn, bulk_delete_fn):
    st.subheader("🗑️ Xóa Records")

    tab_single, tab_bulk = st.tabs(["Xóa đơn lẻ", "Xóa hàng loạt"])

    with tab_single:
        job_id = st.number_input("ID cần xóa", min_value=1, step=1, key="del_id")
        reason = st.text_input("Lý do xóa (tùy chọn)", key="del_reason")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Xóa mềm (có thể phục hồi)",
                         use_container_width=True, type="primary"):
                with st.spinner("Đang xóa..."):
                    try:
                        ok = delete_fn(int(job_id), reason)
                        if ok:
                            st.success(f"✅ Đã xóa mềm record ID: {job_id}")
                        else:
                            st.error("Không tìm thấy record")
                    except Exception as e:
                        st.error(f"Lỗi: {e}")

    with tab_bulk:
        st.warning("⚠️ Xóa hàng loạt dựa trên bộ lọc.")

        col1, col2 = st.columns(2)
        with col1:
            bulk_level = st.multiselect("Lọc theo cấp độ", config.JOB_LEVELS,
                                         key="bulk_level")
        with col2:
            bulk_loc = st.multiselect("Lọc theo địa điểm", config.LOCATIONS,
                                       key="bulk_loc")

        bulk_reason = st.text_input("Lý do", key="bulk_reason")

        # Preview
        if st.button("👁️ Xem trước records sẽ bị xóa"):
            filters = {}
            if bulk_level:
                filters["job_level"] = bulk_level
            if bulk_loc:
                filters["location"] = bulk_loc
            try:
                df, total = read_fn(filters=filters, page_size=50)
                st.markdown(f"**Sẽ xóa {total} records:**")
                if not df.empty:
                    st.dataframe(_format_display(df), hide_index=True)
                    st.session_state["bulk_delete_ids"] = df["id"].tolist()
            except Exception as e:
                st.error(f"Lỗi: {e}")

        if "bulk_delete_ids" in st.session_state:
            ids = st.session_state["bulk_delete_ids"]
            if st.button(f"🗑️ Xác nhận xóa {len(ids)} records",
                         type="primary", use_container_width=True):
                with st.spinner("Đang xóa..."):
                    try:
                        count = bulk_delete_fn(ids, bulk_reason)
                        st.success(f"✅ Đã xóa {count} records")
                        del st.session_state["bulk_delete_ids"]
                    except Exception as e:
                        st.error(f"Lỗi: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# DELETED RECORDS (UNDO)
# ═══════════════════════════════════════════════════════════════════════════════

def _render_deleted(get_deleted_fn, restore_fn):
    st.subheader("♻️ Records Đã Xóa")

    with st.spinner("Đang tải..."):
        try:
            df = get_deleted_fn()
        except Exception as e:
            st.error(f"Lỗi: {e}")
            return

    if df.empty:
        st.info("Chưa có records nào bị xóa.")
        return

    st.markdown(f"**{len(df)}** records đã bị xóa")

    cols_show = [c for c in ["id", "title_clean", "company",
                              "deleted_at", "deleted_reason"] if c in df.columns]
    st.dataframe(df[cols_show], hide_index=True, use_container_width=True)

    restore_id = st.number_input("ID cần phục hồi", min_value=1, step=1)
    if st.button("♻️ Phục hồi Record", type="primary"):
        with st.spinner("Đang phục hồi..."):
            try:
                ok = restore_fn(int(restore_id))
                if ok:
                    st.success(f"✅ Đã phục hồi record ID: {restore_id}")
                    st.rerun()
            except Exception as e:
                st.error(f"Lỗi: {e}")
