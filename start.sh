#!/bin/bash

# ── Configuration ─────────────────────────────────────────────────────────────
PORT=9090
JAR_NAME="trading-agent-0.0.1-SNAPSHOT.jar"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"          # workspace/ — one level up
SOURCE_JAR="${SCRIPT_DIR}/target/${JAR_NAME}"
DEPLOY_JAR="${DEPLOY_DIR}/${JAR_NAME}"
APP_LOG="${SCRIPT_DIR}/app.log"
TUNNEL_LOG="${SCRIPT_DIR}/tunnel.log"
TUNNEL_NAME="jaineo-trading"
DOMAIN="trading-agent.jaineo.win"
CF_DIR="$HOME/.cloudflared"
CONFIG_FILE="$CF_DIR/$TUNNEL_NAME.yml"

# ── Step 1: System checks ─────────────────────────────────────────────────────
echo "⚙️  Step 1: Checking system requirements..."

if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed or not in PATH."
    exit 1
fi
echo "✅ Java: $(java -version 2>&1 | head -n 1)"

if ! command -v cloudflared &> /dev/null; then
    echo "📦 cloudflared not found — installing via Homebrew..."
    brew install cloudflare/cloudflare/cloudflared
fi
echo "✅ cloudflared is ready."

# ── Step 2: Build ─────────────────────────────────────────────────────────────
echo ""
echo "🔨 Step 2: Building project..."
cd "$SCRIPT_DIR"
mvn clean package -DskipTests -q
if [ $? -ne 0 ]; then
    echo "❌ Maven build failed — check output above."
    exit 1
fi
echo "✅ Build successful."

# ── Step 3: Copy JAR to parent directory ─────────────────────────────────────
echo ""
echo "📦 Step 3: Deploying JAR to ${DEPLOY_DIR} ..."

if [ ! -f "$SOURCE_JAR" ]; then
    echo "❌ Built JAR not found at ${SOURCE_JAR} — unexpected build error."
    exit 1
fi

# Remove old JAR from deploy directory
if [ -f "$DEPLOY_JAR" ]; then
    rm -f "$DEPLOY_JAR"
    echo "🗑️  Removed old JAR from ${DEPLOY_DIR}"
fi

cp "$SOURCE_JAR" "$DEPLOY_JAR"
echo "✅ JAR deployed → ${DEPLOY_JAR}"

# ── Step 4: Cloudflare Tunnel setup ──────────────────────────────────────────
echo ""
echo "☁️  Step 4: Cloudflare Tunnel setup for https://$DOMAIN ..."

mkdir -p "$CF_DIR"

TUNNEL_EXISTS=$(cloudflared tunnel list 2>/dev/null | grep -w "$TUNNEL_NAME")

if [ -z "$TUNNEL_EXISTS" ]; then
    echo "🔐 First run detected — authenticating with Cloudflare..."
    cloudflared login
    if ! cloudflared tunnel list &> /dev/null; then
        echo "❌ Authentication failed. Run: cloudflared tunnel login"
        exit 1
    fi

    echo "🔧 Creating named tunnel: $TUNNEL_NAME ..."
    cloudflared tunnel create "$TUNNEL_NAME"

    TUNNEL_ID=$(cloudflared tunnel list 2>/dev/null \
        | grep -w "$TUNNEL_NAME" \
        | awk '{print $1}')

    if [ -z "$TUNNEL_ID" ]; then
        echo "❌ Could not retrieve tunnel ID. Check Cloudflare dashboard."
        exit 1
    fi

    echo "📝 Writing config → $CONFIG_FILE ..."
    cat > "$CONFIG_FILE" <<EOF
tunnel: $TUNNEL_ID
credentials-file: $CF_DIR/$TUNNEL_ID.json
ingress:
  - hostname: $DOMAIN
    service: http://localhost:$PORT
  - service: http_status:404
EOF

    echo "🌐 Adding DNS CNAME record: $DOMAIN → $TUNNEL_NAME ..."
    cloudflared tunnel route dns "$TUNNEL_NAME" "$DOMAIN"
    echo "✅ First-run setup complete."
else
    echo "✅ Tunnel '$TUNNEL_NAME' already exists — skipping creation."
fi

# ── Step 5: Start Spring Boot ─────────────────────────────────────────────────
echo ""
echo "🚀 Step 5: Starting Spring Boot on port $PORT ..."
rm -f "$APP_LOG" "$TUNNEL_LOG"
java -Dserver.port=$PORT -jar "$DEPLOY_JAR" > "$APP_LOG" 2>&1 &
APP_PID=$!

echo "⏳ Step 6: Waiting for Spring Boot to be ready (up to 60 s)..."
MAX_ATTEMPTS=30
ATTEMPT=0
APP_READY=false

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s -o /dev/null http://127.0.0.1:$PORT; then
        APP_READY=true
        break
    fi
    echo -n "."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done
echo ""

if [ "$APP_READY" = false ]; then
    echo "❌ Spring Boot failed to start in 60 s — check $APP_LOG"
    kill $APP_PID 2>/dev/null
    exit 1
fi
echo "✅ Spring Boot is up on port $PORT."

# ── Step 7: Start Cloudflare Tunnel ──────────────────────────────────────────
echo ""
echo "☁️  Step 7: Starting tunnel $TUNNEL_NAME → https://$DOMAIN ..."
cloudflared tunnel --protocol http2 --config "$CONFIG_FILE" run "$TUNNEL_NAME" > "$TUNNEL_LOG" 2>&1 &
TUNNEL_PID=$!

sleep 4

if ! kill -0 $TUNNEL_PID 2>/dev/null; then
    echo "❌ Tunnel process exited — check $TUNNEL_LOG"
    kill $APP_PID 2>/dev/null
    exit 1
fi

echo "--------------------------------------------------------"
echo "🎉 Trading Agent is live!"
echo "🔗 Public URL  : https://$DOMAIN"
echo "🔗 Local URL   : http://localhost:$PORT"
echo "📝 App logs    : tail -f $APP_LOG"
echo "📝 Tunnel logs : tail -f $TUNNEL_LOG"
echo "🛑 To stop     : ./stop.sh"
echo "--------------------------------------------------------"

cat > "${SCRIPT_DIR}/stop.sh" <<STOP
#!/bin/bash
kill $APP_PID $TUNNEL_PID 2>/dev/null
echo "🛑 Trading Agent and Cloudflare Tunnel stopped."
STOP
chmod +x "${SCRIPT_DIR}/stop.sh"
