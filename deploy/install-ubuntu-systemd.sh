#!/usr/bin/env bash
# 在全新 Ubuntu（x86_64）上一键安装 arb-bot 看板，并登记为 systemd 服务。
#
# 用法：先把本仓库放到机器上，然后：
#   sudo bash deploy/install-ubuntu-systemd.sh
#
# 会安装：编译依赖、Rust、Lighter 官方签名库、全部命令行程序，
# 然后把看板装到 /opt/arb-bot，数据目录 /var/lib/arb-bot。
# 不生成密钥文件：只把 .env.example 复制到 /opt/arb-bot/.env.example。
# 服务读 /etc/arb-bot.env，这份由你在服务器上自己生成和修改。
#
# 常用参数：
#   --deps-only     只装系统依赖、Rust 和签名库，不编译、不改服务
#   --no-start      装好并 enable，但不立刻启动
#   --skip-build    不重新编译，使用仓库里已有的 target/release 成品
#   --uninstall     停服务并删程序（保留密钥和台账）
#   --purge         在 --uninstall 基础上再删密钥和数据（不可恢复）

set -euo pipefail

SIGNER_URL="https://github.com/elliottech/lighter-go/releases/download/v1.0.10/lighter-signer-linux-amd64.so"
SIGNER_SHA256="1022a277cbb8fb9647886a611bc02151ad28cdbc5a02f7ac6416faa2898be392"
SIGNER_PATH="/opt/lighter/lighter-signer-linux-amd64.so"
OPT_DIR="/opt/arb-bot"
DATA_DIR="/var/lib/arb-bot"
ENV_FILE="/etc/arb-bot.env"
SERVICE_NAME="arb-web"
SERVICE_DST="/etc/systemd/system/${SERVICE_NAME}.service"
BINS=(arb-web arb-scan arb-paper arb-live)

DEPS_ONLY=0
NO_START=0
SKIP_BUILD=0
UNINSTALL=0
PURGE=0

log() { printf '[arb-install] %s\n' "$*"; }
die() { printf '[arb-install] 错误：%s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --deps-only) DEPS_ONLY=1 ;;
    --no-start) NO_START=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    --uninstall) UNINSTALL=1 ;;
    --purge) UNINSTALL=1; PURGE=1 ;;
    *) die "未知参数：$1（--help 看用法）" ;;
  esac
  shift
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICE_SRC="${SCRIPT_DIR}/${SERVICE_NAME}.service"

need_root() {
  [[ "$(id -u)" -eq 0 ]] || die "请用 root 运行：sudo bash deploy/install-ubuntu-systemd.sh"
}

assert_platform() {
  [[ "$(uname -s)" == Linux ]] || die "只支持 Linux"
  local arch
  arch="$(uname -m)"
  [[ "$arch" == x86_64 || "$arch" == amd64 ]] || die "Lighter 签名库只有 x86_64，当前是 ${arch}"
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    case "${ID:-}" in
      ubuntu|debian) ;;
      *) log "警告：未识别的发行版 ${ID:-unknown}，按 Debian 系继续" ;;
    esac
  fi
}

build_user() {
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != root ]]; then
    printf '%s\n' "$SUDO_USER"
  else
    printf 'root\n'
  fi
}

user_home() {
  getent passwd "$1" | cut -d: -f6
}

as_user() {
  local user="$1"
  shift
  if [[ "$user" == root ]]; then
    env "$@"
  else
    sudo -u "$user" -H "$@"
  fi
}

install_apt() {
  export DEBIAN_FRONTEND=noninteractive
  log "安装系统依赖"
  apt-get update -y
  apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    build-essential \
    pkg-config \
    openssl \
    xz-utils \
    jq
}

install_rust() {
  local user="$1"
  local home cargo
  home="$(user_home "$user")"
  cargo="${home}/.cargo/bin/cargo"
  if [[ -x "$cargo" ]]; then
    log "Rust 已在 ${user} 下安装，跳过 rustup"
    return
  fi
  log "为 ${user} 安装 Rust（rustup，minimal）"
  as_user "$user" bash -lc "curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable"
}

install_signer() {
  mkdir -p /opt/lighter
  if [[ -f "$SIGNER_PATH" ]]; then
    local have
    have="$(sha256sum "$SIGNER_PATH" | awk '{print $1}')"
    if [[ "$have" == "$SIGNER_SHA256" ]]; then
      log "Lighter 签名库已存在且校验通过"
      return
    fi
    log "已有签名库哈希不符，重新下载"
  fi
  local tmp
  tmp="$(mktemp)"
  log "下载 Lighter 官方签名库 v1.0.10"
  curl -fsSL --retry 5 --retry-delay 2 -o "$tmp" "$SIGNER_URL"
  local got
  got="$(sha256sum "$tmp" | awk '{print $1}')"
  [[ "$got" == "$SIGNER_SHA256" ]] || die "签名库 SHA-256 不符（得到 ${got}）"
  install -m 0755 "$tmp" "$SIGNER_PATH"
  rm -f "$tmp"
  log "签名库已装到 ${SIGNER_PATH}"
}

ensure_user() {
  if ! getent passwd arb >/dev/null; then
    log "创建系统用户 arb"
    useradd --system --home "$OPT_DIR" --shell /usr/sbin/nologin arb
  fi
  install -d -o arb -g arb -m 0750 "$OPT_DIR" "$DATA_DIR"
}

mem_kb() {
  awk '/MemTotal:/ {print $2}' /proc/meminfo
}

build_bins() {
  local user="$1"
  [[ -f "${REPO_ROOT}/Cargo.toml" ]] || die "找不到仓库根目录：${REPO_ROOT}"
  local cargo_bin
  cargo_bin="$(user_home "$user")/.cargo/bin"
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    log "跳过编译，使用已有成品"
    local bin
    for bin in "${BINS[@]}"; do
      [[ -x "${REPO_ROOT}/target/release/${bin}" ]] || die "没有 ${REPO_ROOT}/target/release/${bin}，去掉 --skip-build 再跑"
    done
    return
  fi

  local kb
  local -a build_env
  build_env=(PATH="${cargo_bin}:${PATH}")
  kb="$(mem_kb)"
  if [[ -n "$kb" && "$kb" -lt 3500000 ]]; then
    log "内存约 $((kb / 1024)) MB，关闭 fat LTO 以免编译时内存不够"
    build_env+=(CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=4)
  fi

  log "编译 release 成品（约几分钟到十几分钟）"
  as_user "$user" env "${build_env[@]}" \
    bash -lc "cd $(printf '%q' "$REPO_ROOT") && cargo build --release --locked --workspace"

  local bin
  for bin in "${BINS[@]}"; do
    [[ -x "${REPO_ROOT}/target/release/${bin}" ]] || die "编译结束但没有 ${bin}"
  done
}

install_bins() {
  local bin
  for bin in "${BINS[@]}"; do
    install -m 0755 "${REPO_ROOT}/target/release/${bin}" "${OPT_DIR}/${bin}"
  done
  install -m 0755 "${SCRIPT_DIR}/arbctl.sh" "${OPT_DIR}/arbctl"
  ln -sfn "${OPT_DIR}/arbctl" /usr/local/sbin/arbctl
  chown -R arb:arb "$OPT_DIR"
  log "程序已装到 ${OPT_DIR}：${BINS[*]} arbctl"
}

copy_env_example() {
  local example="${REPO_ROOT}/.env.example"
  local dest="${OPT_DIR}/.env.example"
  [[ -f "$example" ]] || die "找不到 ${example}"
  # 只复制模板。不写 /etc/arb-bot.env，也不生成 .env：密钥由你在服务器上自己放。
  tr -d '\r' < "$example" > "$dest"
  chown arb:arb "$dest"
  chmod 0644 "$dest"
  log "已复制模板到 ${dest}，没有生成密钥文件"
  if [[ ! -f "$ENV_FILE" ]]; then
    log "还没有 ${ENV_FILE}。服务只读这份，请在服务器上自己生成并修改后再启动"
  fi
}

install_service() {
  [[ -f "$SERVICE_SRC" ]] || die "找不到 ${SERVICE_SRC}"
  # Windows 拷过来的单元文件可能带 CR，systemd 会拒读。
  tr -d '' < "$SERVICE_SRC" > "$SERVICE_DST"
  chmod 0644 "$SERVICE_DST"
  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
  if [[ "$NO_START" -eq 1 ]]; then
    log "已登记服务 ${SERVICE_NAME}，按 --no-start 不启动"
    return
  fi
  systemctl restart "$SERVICE_NAME"
  sleep 2
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    log "服务已启动"
  else
    systemctl --no-pager --full status "$SERVICE_NAME" || true
    die "服务没起来，看上面的 status 和：journalctl -u ${SERVICE_NAME} -e"
  fi
  if command -v curl >/dev/null; then
    local port host
    port="$(awk -F= '/^ARB_HTTP_PORT=/{print $2}' "$ENV_FILE" 2>/dev/null | tr -d '\r' | tail -n1)"
    host="$(awk -F= '/^ARB_BIND_HOST=/{print $2}' "$ENV_FILE" 2>/dev/null | tr -d '\r' | tail -n1)"
    port="${port:-8080}"
    host="${host:-127.0.0.1}"
    if [[ "$host" == "0.0.0.0" ]]; then host=127.0.0.1; fi
    if curl -fsS --max-time 8 "http://${host}:${port}/healthz" >/dev/null; then
      log "健康检查通过：http://${host}:${port}/"
    else
      log "服务在跑，但健康检查还没通过（首次拉行情可能要一会儿）"
    fi
  fi
}

do_uninstall() {
  log "卸载 systemd 服务"
  systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
  rm -f "$SERVICE_DST" /usr/local/sbin/arbctl
  systemctl daemon-reload
  rm -rf "$OPT_DIR"
  if [[ "$PURGE" -eq 1 ]]; then
    log "按 --purge 删除 ${ENV_FILE} 与 ${DATA_DIR}"
    rm -f "$ENV_FILE"
    rm -rf "$DATA_DIR"
  else
    log "已保留 ${ENV_FILE} 与 ${DATA_DIR}（台账不要删）。彻底清掉请加 --purge"
  fi
  log "卸载完成"
}

print_done() {
  cat <<EOF

安装完成。

  看板：     见 ${ENV_FILE} 里的 ARB_BIND_HOST / ARB_HTTP_PORT（模板默认 127.0.0.1:8080）
  程序：     ${OPT_DIR}
  数据：     ${DATA_DIR}   （台账、价差历史；必须备份，不能删 jsonl）
  模板：     ${OPT_DIR}/.env.example   （只是模板，不含密钥）
  密钥：     ${ENV_FILE}   （安装不会生成或修改；请在服务器上自己生成。改完 sudo arbctl restart）
  签名库：   ${SIGNER_PATH}

常用命令：
  sudo arbctl status
  sudo arbctl logs
  sudo arbctl restart
  sudo arbctl env

配置只来自 .env.example / 你自己改的 ${ENV_FILE}。安装脚本不生成令牌、不开实盘。
EOF
}

main() {
  need_root
  assert_platform
  if [[ "$UNINSTALL" -eq 1 ]]; then
    do_uninstall
    exit 0
  fi
  [[ -f "$SERVICE_SRC" ]] || die "请从本仓库运行（找不到 deploy/${SERVICE_NAME}.service）"
  local user
  user="$(build_user)"
  install_apt
  install_rust "$user"
  install_signer
  if [[ "$DEPS_ONLY" -eq 1 ]]; then
    log "按 --deps-only 到此结束。Rust 用户：${user}；签名库：${SIGNER_PATH}"
    exit 0
  fi
  ensure_user
  build_bins "$user"
  install_bins
  copy_env_example
  if [[ ! -f "$ENV_FILE" ]]; then
    log "没有 ${ENV_FILE}，不登记、不启动服务。请在服务器上自己生成后再运行本脚本"
    exit 0
  fi
  install_service
  print_done
}

main "$@"
