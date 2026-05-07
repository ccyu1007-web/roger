#!/bin/bash
# 自動部署腳本 - 由 Claude Code Stop hook 呼叫
# 功能：commit + push + 驗證 Render 部署

cd ~/Documents/AI機器人/stock_system || exit 0

LOG_DIR="logs"
LOG_FILE="$LOG_DIR/deploy.log"
mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# 檢查是否有變更
if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
    exit 0
fi

# 產生 commit message（列出變更的檔案）
CHANGED=$(git diff --name-only 2>/dev/null)
STAGED=$(git diff --cached --name-only 2>/dev/null)
UNTRACKED=$(git ls-files --others --exclude-standard 2>/dev/null)
ALL_CHANGED=$(echo -e "${CHANGED}\n${STAGED}\n${UNTRACKED}" | sort -u | grep -v '^$' | head -5)
FILE_COUNT=$(echo -e "${CHANGED}\n${STAGED}\n${UNTRACKED}" | sort -u | grep -v '^$' | wc -l | tr -d ' ')

if [ "$FILE_COUNT" -le 5 ]; then
    MSG="部署更新: $(echo "$ALL_CHANGED" | tr '\n' ', ' | sed 's/,$//')"
else
    MSG="部署更新: $(echo "$ALL_CHANGED" | tr '\n' ', ' | sed 's/,$//')... 等${FILE_COUNT}個檔案"
fi

# Stage + Commit
git add -A
if ! git commit -m "$MSG" 2>> "$LOG_FILE"; then
    log "ERROR: commit 失敗"
    exit 1
fi
log "commit 成功: $MSG"

# Push
if ! git push origin master 2>> "$LOG_FILE"; then
    log "ERROR: push 失敗"
    echo "部署失敗: git push 錯誤，詳見 logs/deploy.log" >&2
    exit 1
fi
log "push 成功"

# 驗證 Render 部署（等待最多 120 秒）
RENDER_URL="https://tock-system.onrender.com/api/health"
log "等待 Render 部署..."

# 先等 30 秒讓 Render 開始重新部署
sleep 30

RETRY=0
MAX_RETRY=6
while [ $RETRY -lt $MAX_RETRY ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$RENDER_URL" 2>/dev/null)
    if [ "$HTTP_CODE" = "200" ]; then
        log "Render 部署驗證成功 (HTTP $HTTP_CODE)"
        exit 0
    fi
    RETRY=$((RETRY + 1))
    log "Render 尚未就緒 (HTTP $HTTP_CODE)，第 ${RETRY}/${MAX_RETRY} 次重試..."
    sleep 15
done

log "WARNING: Render 部署驗證超時，請手動確認 $RENDER_URL"
exit 0
