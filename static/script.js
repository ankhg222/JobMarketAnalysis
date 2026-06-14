document.addEventListener('DOMContentLoaded', () => {
    const topKInput = document.getElementById('top-k');
    const topKVal = document.getElementById('top-k-val');
    const btn = document.getElementById('recommend-btn');
    const profileInput = document.getElementById('user-profile');
    const loading = document.getElementById('loading');
    const resultsGrid = document.getElementById('results');

    // Update slider value
    topKInput.addEventListener('input', (e) => {
        topKVal.textContent = e.target.value;
    });

    // Handle recommend button click
    btn.addEventListener('click', async () => {
        const profile = profileInput.value.trim();
        if (!profile) {
            alert("Vui lòng nhập kỹ năng hoặc mô tả công việc!");
            return;
        }

        const top_k = parseInt(topKInput.value);

        // UI state changes
        resultsGrid.innerHTML = '';
        loading.classList.remove('hidden');
        btn.disabled = true;

        try {
            const response = await fetch('/api/recommend', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_profile: profile,
                    top_k: top_k
                })
            });

            const data = await response.json();

            if (response.ok) {
                renderResults(data.recommendations);
            } else {
                alert(`Lỗi: ${data.detail}`);
            }
        } catch (error) {
            console.error("Error:", error);
            alert("Không thể kết nối tới server. Vui lòng kiểm tra xem server đã chạy chưa.");
        } finally {
            loading.classList.add('hidden');
            btn.disabled = false;
        }
    });

    function renderResults(jobs) {
        if (jobs.length === 0) {
            resultsGrid.innerHTML = '<p style="text-align:center; width:100%; color:#94a3b8;">Không tìm thấy công việc phù hợp.</p>';
            return;
        }

        jobs.forEach((job, index) => {
            const delay = index * 0.1;
            const card = document.createElement('div');
            card.className = 'job-card';
            card.style.animationDelay = `${delay}s`;

            const scorePercent = (job.similarity_score * 100).toFixed(1);

            let salaryText = job.salary;
            // Handle nan from pandas
            if (salaryText === 'nan' || !salaryText) {
                salaryText = 'Thoả thuận';
            } else if (!isNaN(salaryText)) {
                // If it's a number, format it as VND
                salaryText = parseInt(salaryText).toLocaleString('vi-VN') + ' VNĐ';
            }

            card.innerHTML = `
                <div class="score-badge">Độ phù hợp: ${scorePercent}%</div>
                <h3>${job.title}</h3>
                <div class="company">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align: middle; margin-right: 5px;">
                        <rect x="4" y="2" width="16" height="20" rx="2" ry="2"></rect>
                        <path d="M9 22v-4h6v4"></path>
                        <path d="M8 6h.01"></path>
                        <path d="M16 6h.01"></path>
                        <path d="M12 6h.01"></path>
                        <path d="M12 10h.01"></path>
                        <path d="M12 14h.01"></path>
                        <path d="M16 10h.01"></path>
                        <path d="M16 14h.01"></path>
                        <path d="M8 10h.01"></path>
                        <path d="M8 14h.01"></path>
                    </svg> ${job.company}
                </div>
                <div class="skills"><strong>Kỹ năng:</strong><br/>${job.skills}</div>
                <div class="salary">💰 ${salaryText}</div>
                ${job.url && job.url !== '#' ? `
                <a href="${job.url}" target="_blank" class="apply-btn">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                        <polyline points="15 3 21 3 21 9"></polyline>
                        <line x1="10" y1="14" x2="21" y2="3"></line>
                    </svg>
                    Xem chi tiết & Ứng tuyển
                </a>` : ''}
            `;
            resultsGrid.appendChild(card);
        });
    }
});
