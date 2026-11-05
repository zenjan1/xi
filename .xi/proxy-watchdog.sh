#!/usr/bin/env bash
# 模型池代理守护 — 每 20 秒检查代理是否在监听 8901, 掉了就拉起
# 用法: setsid nohup bash .xi/proxy-watchdog.sh >/dev/null 2>&1 &
REPO="/run/media/a/work/workbuddy-rs"
LOG="$REPO/.xi/proxy-watchdog.log"
cd "$REPO" || exit 1

log() { echo "[$(date '+%m-%d %H:%M:%S')] $1" >> "$LOG"; }

log "watchdog 启动 (pid $$)"
while true; do
  if ! ss -tlnp 2>/dev/null | grep -q ":8901 "; then
    log "代理未监听 8901, 拉起中..."
    setsid nohup python3 "$REPO/.xi/model-proxy.py" >/dev/null 2>&1 < /dev/null &
    sleep 3
    if ss -tlnp 2>/dev/null | grep -q ":8901 "; then
      log "代理已恢复"
    else
      log "⚠ 拉起失败, 20s 后重试"
    fi
  fi
  sleep 20
done
