#!/usr/bin/env bash
# xi selfcheck: 验证本地安装是否为最新版本且功能完整
# 用法: xi selfcheck 或 bash .xi/selfcheck.sh
# 退出码: 0=全部通过, 1=有异常
set -u
REPO="/run/media/a/work/workbuddy-rs"
BIN="$HOME/.local/bin/xi.real"
WRAPPER="$HOME/.local/bin/xi"
FAIL=0

echo "══════════════════════════════════════════"
echo "  xi 自检报告  $(date '+%Y-%m-%d %H:%M:%S')"
echo "══════════════════════════════════════════"

# 1. 二进制存在且可执行
if [ -x "$BIN" ]; then
  echo "✅ 二进制: $BIN ($(du -h "$BIN" | cut -f1))"
else
  echo "❌ 二进制不存在或不可执行: $BIN"
  FAIL=1
fi

# 2. 版本号一致性
TOML_VER=$(grep '^version' "$REPO/Cargo.toml" 2>/dev/null | cut -d'"' -f2)
BIN_VER=$("$BIN" version 2>/dev/null | grep -oP 'v[\d.]+')
if [ "$TOML_VER" = "${BIN_VER#v}" ]; then
  echo "✅ 版本一致: v$TOML_VER"
else
  echo "⚠️  版本不一致: Cargo.toml=$TOML_VER, 二进制=$BIN_VER"
  echo "   需要重新 build + install"
  FAIL=1
fi

# 3. xi 命令可用
if command -v xi &>/dev/null; then
  echo "✅ xi 命令: $(which xi)"
else
  echo "❌ xi 命令不在 PATH 中"
  FAIL=1
fi

# 4. 核心命令测试
for cmd in "version" "sysinfo" "status" "extensions"; do
  if "$BIN" "$cmd" &>/dev/null 2>&1; then
    echo "✅ xi $cmd"
  else
    echo "❌ xi $cmd 执行失败"
    FAIL=1
  fi
done

# 5. LLM 配置检查
LLM_ENV="$REPO/.xi/llm.env"
if [ -f "$LLM_ENV" ]; then
  MODEL=$(grep 'DASHSCOPE_MODEL' "$LLM_ENV" | cut -d'"' -f2)
  BASE=$(grep 'DASHSCOPE_BASE_URL' "$LLM_ENV" | cut -d'"' -f2)
  echo "✅ LLM 配置: model=$MODEL, base=$BASE"
else
  echo "❌ LLM 配置不存在: $LLM_ENV"
  FAIL=1
fi

# 6. dev 循环状态
PIDFILE="$REPO/.xi/dev.pid"
if [ -f "$PIDFILE" ]; then
  PID=$(cat "$PIDFILE")
  if kill -0 "$PID" 2>/dev/null; then
    echo "✅ dev 循环: 运行中 (pid $PID)"
  else
    echo "⚠️  dev 循环: pid $PID 已死(需重启)"
  fi
else
  echo "⚠️  dev 循环: 未运行"
fi

# 7. 自动监控状态
MON_PIDFILE="$REPO/.xi/auto-monitor.pid"
if [ -f "$MON_PIDFILE" ]; then
  MPID=$(cat "$MON_PIDFILE")
  if kill -0 "$MPID" 2>/dev/null; then
    echo "✅ 自动监控: 运行中 (pid $MPID)"
  else
    echo "⚠️  自动监控: pid $MPID 已死(需重启)"
    FAIL=1
  fi
else
  echo "⚠️  自动监控: 未运行"
  FAIL=1
fi

# 8. GitHub 同步状态
cd "$REPO"
UNPUSHED=$(git log --oneline github/main..main 2>/dev/null | wc -l)
if [ "$UNPUSHED" -eq 0 ]; then
  echo "✅ GitHub: 已同步"
else
  echo "⚠️  GitHub: $UNPUSHED 个提交未推送"
fi

# 9. 最近 feat(dev) 提交
LAST_FEAT=$(git log --oneline --grep="feat(dev)" -1 2>/dev/null)
FEAT_COUNT=$(git log --oneline --grep="feat(dev)" 2>/dev/null | wc -l)
echo "📊 feat(dev): $FEAT_COUNT 个 | 最近: $LAST_FEAT"

echo "══════════════════════════════════════════"
if [ $FAIL -eq 0 ]; then
  echo "  结论: ✅ 全部通过"
else
  echo "  结论: ❌ 有异常(见上方 ❌/⚠️)"
fi
echo "══════════════════════════════════════════"
exit $FAIL
