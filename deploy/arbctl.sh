#!/usr/bin/env bash
# arb-web systemd 管理：status / start / stop / restart / logs / env / health
set -euo pipefail

SERVICE=arb-web
ENV_FILE=/etc/arb-bot.env

cmd="${1:-status}"
shift || true

need_root() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "请用 sudo arbctl $cmd" >&2
    exit 1
  fi
}

case "$cmd" in
  -h|--help|help)
    cat <<'EOF'
用法：sudo arbctl <命令>

  status    服务状态
  start     启动
  stop      停止（会等正在进行的下单排空）
  restart   重启
  logs      看日志（Ctrl-C 退出）
  env       打开密钥文件 /etc/arb-bot.env
  health    访问本地健康检查
EOF
    ;;
  status)
    systemctl status "$SERVICE" --no-pager --full
    ;;
  start|stop|restart)
    need_root
    systemctl "$cmd" "$SERVICE"
    systemctl --no-pager --full status "$SERVICE" || true
    ;;
  logs)
    journalctl -u "$SERVICE" -f --no-hostname "$@"
    ;;
  env)
    need_root
    "${EDITOR:-nano}" "$ENV_FILE"
    echo "改完需要：sudo arbctl restart"
    ;;
  health)
    port="$(awk -F= '/^ARB_HTTP_PORT=/{print $2}' "$ENV_FILE" 2>/dev/null | tr -d '\r' | tail -n1)"
    port="${port:-8080}"
    curl -fsS --max-time 8 "http://127.0.0.1:${port}/healthz"
    echo
    ;;
  *)
    echo "未知命令：$cmd（arbctl help）" >&2
    exit 1
    ;;
esac
