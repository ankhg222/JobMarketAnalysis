#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
# IT Job Market CRUD - Ubuntu Deployment Script
# Yêu cầu: Ubuntu 20.04/22.04 LTS, Java 8, Hadoop 3.3.6 đã cài
# ═══════════════════════════════════════════════════════════════════

set -e  # Dừng nếu có lỗi

APP_DIR="/opt/itjob_crud"
DATA_DIR="$APP_DIR/data"
VENV_DIR="$APP_DIR/.venv"

echo "╔════════════════════════════════════════════════╗"
echo "║   IT Job Market CRUD - Ubuntu Deployment       ║"
echo "╚════════════════════════════════════════════════╝"

# ── 1. Kiểm tra Java ─────────────────────────────────────────────
echo ""
echo "▶ [1/8] Kiểm tra Java..."
if ! java -version 2>&1 | grep -q "1.8\|11\|17"; then
    echo "❌ Java chưa cài. Cài Java 8:"
    echo "   sudo apt install openjdk-8-jdk -y"
    exit 1
fi
echo "✓ Java OK: $(java -version 2>&1 | head -1)"

# ── 2. Kiểm tra Hadoop/HDFS ──────────────────────────────────────
echo ""
echo "▶ [2/8] Kiểm tra Hadoop..."
if [ -z "$HADOOP_HOME" ]; then
    echo "❌ HADOOP_HOME không được set."
    echo "   export HADOOP_HOME=/opt/hadoop"
    echo "   export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin"
    exit 1
fi
echo "✓ HADOOP_HOME=$HADOOP_HOME"

# Kiểm tra HDFS đang chạy
if ! hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "⚠️  HDFS chưa chạy. Đang start..."
    "$HADOOP_HOME/sbin/start-dfs.sh"
    sleep 5
fi
echo "✓ HDFS đang chạy"

# ── 3. Thiết lập HDFS directories ────────────────────────────────
echo ""
echo "▶ [3/8] Tạo HDFS directories..."
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/backups/it_jobs
hdfs dfs -mkdir -p /user/backups/meta
hdfs dfs -mkdir -p /user/uploads
hdfs dfs -chmod -R 777 /user/hive
hdfs dfs -chmod -R 777 /user/backups
hdfs dfs -chmod -R 777 /user/uploads
echo "✓ HDFS directories OK"

# ── 4. Kiểm tra/cài Hive ──────────────────────────────────────────
echo ""
echo "▶ [4/8] Kiểm tra Hive..."
if [ -z "$HIVE_HOME" ]; then
    echo "⚠️  HIVE_HOME chưa set. App sẽ dùng embedded metastore (Derby)."
    echo "   Để dùng Hive đầy đủ:"
    echo "   export HIVE_HOME=/opt/hive"
    echo "   export PATH=\$PATH:\$HIVE_HOME/bin"
else
    echo "✓ HIVE_HOME=$HIVE_HOME"
    # Init schema nếu chưa có
    if [ ! -f "$HIVE_HOME/metastore_db/dbex.lck" ]; then
        echo "   Initializing Hive Derby schema..."
        "$HIVE_HOME/bin/schematool" -dbType derby -initSchema 2>/dev/null || true
    fi
fi

# ── 5. Tạo app directory ─────────────────────────────────────────
echo ""
echo "▶ [5/8] Chuẩn bị app directory..."
sudo mkdir -p "$APP_DIR"
sudo chown -R "$USER:$USER" "$APP_DIR"

# Copy files (nếu chưa ở đúng vị trí)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_SOURCE="$(dirname "$SCRIPT_DIR")"

if [ "$APP_SOURCE" != "$APP_DIR" ]; then
    echo "   Copy từ $APP_SOURCE → $APP_DIR"
    cp -r "$APP_SOURCE/." "$APP_DIR/"
fi

# Copy data file
mkdir -p "$DATA_DIR"
echo "   Đặt file CSV vào: $DATA_DIR/Data_ITJOB_Cleaned.csv"

# ── 6. Cài Python dependencies ───────────────────────────────────
echo ""
echo "▶ [6/8] Cài Python dependencies..."

# Kiểm tra Python 3.8+
PYTHON=$(which python3.11 || which python3.10 || which python3.9 || which python3.8 || which python3)
echo "   Python: $($PYTHON --version)"

# Tạo virtual environment
if [ ! -d "$VENV_DIR" ]; then
    "$PYTHON" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r "$APP_DIR/requirements.txt" -q
echo "✓ Dependencies đã cài"

# ── 7. Cấu hình environment variables ────────────────────────────
echo ""
echo "▶ [7/8] Cấu hình environment..."

ENV_FILE="$APP_DIR/.env"
cat > "$ENV_FILE" << EOF
# IT Job Market App - Environment Config
# Chỉnh các giá trị này theo hệ thống của bạn

HDFS_HOST=localhost
HDFS_PORT=9000
SPARK_MASTER=local[*]
HIVE_HOST=localhost
HIVE_PORT=10000
HIVE_DATABASE=itjobs_db

JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
SPARK_HOME=${SPARK_HOME:-/opt/spark}
HIVE_HOME=${HIVE_HOME:-/opt/hive}
EOF

echo "✓ .env file tạo tại $ENV_FILE"
echo "   ⚠️  Kiểm tra và chỉnh sửa $ENV_FILE nếu cần"

# ── 8. Tạo systemd service (tùy chọn) ────────────────────────────
echo ""
echo "▶ [8/8] Tạo systemd service..."

SERVICE_FILE="/etc/systemd/system/itjob-crud.service"
sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=IT Job Market CRUD - Streamlit App
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
Environment="PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"
EnvironmentFile=$ENV_FILE
ExecStart=$VENV_DIR/bin/streamlit run $APP_DIR/app.py \\
    --server.port=8501 \\
    --server.address=0.0.0.0 \\
    --server.headless=true \\
    --server.maxUploadSize=500
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable itjob-crud.service
echo "✓ Systemd service tạo: itjob-crud.service"

# ── Done ─────────────────────────────────────────────────────────
echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║               ✅ DEPLOYMENT HOÀN TẤT                  ║"
echo "╠════════════════════════════════════════════════════════╣"
echo "║  Các bước tiếp theo:                                   ║"
echo "║                                                         ║"
echo "║  1. Copy Data_ITJOB_Cleaned.csv vào:                  ║"
echo "║     $DATA_DIR/                                          ║"
echo "║                                                         ║"
echo "║  2. Chạy setup (tạo schema + import data):            ║"
echo "║     cd $APP_DIR                                         ║"
echo "║     source .venv/bin/activate                           ║"
echo "║     python scripts/setup.py                            ║"
echo "║                                                         ║"
echo "║  3. Start app:                                         ║"
echo "║     sudo systemctl start itjob-crud                   ║"
echo "║     hoặc: streamlit run app.py                        ║"
echo "║                                                         ║"
echo "║  4. Truy cập: http://localhost:8501                   ║"
echo "║     (hoặc http://YOUR_SERVER_IP:8501)                 ║"
echo "╚════════════════════════════════════════════════════════╝"
