# Ubuntu systemd 一键安装

在全新 Ubuntu（x86_64）上，把本仓库放到机器后执行：

```bash
sudo bash deploy/install-ubuntu-systemd.sh
```

脚本会安装编译依赖、Rust、Lighter 官方签名库，编译 `arb-web` / `arb-scan` / `arb-paper` / `arb-live`。它只把 `.env.example` 复制到 `/opt/arb-bot/.env.example`，不生成密钥文件。`/etc/arb-bot.env` 要在服务器上自己生成；有了这份文件，脚本才会登记并启动看板。

| 路径 | 用途 |
| --- | --- |
| `/opt/arb-bot/` | 程序（只读） |
| `/var/lib/arb-bot/` | 台账、价差历史等运行数据，必须备份 |
| `/opt/arb-bot/.env.example` | 配置模板。安装只复制它，不含密钥 |
| `/etc/arb-bot.env` | 密钥与配置。安装不生成、不修改，在服务器上自己放 |
| `/opt/lighter/lighter-signer-linux-amd64.so` | Lighter 官方签名库 v1.0.10 |

```bash
sudo arbctl status
sudo arbctl logs
sudo arbctl restart
```

默认不开实盘。模板按 **必填 / 条件必填 / 选填** 分组。
在服务器上生成 `/etc/arb-bot.env` 后，需要实盘时再把 `ARB_WEB_LIVE` 改成 `trade`，并设置 `ARB_WEB_MARKET_SLIPPAGE`。

可选参数：`--deps-only`、`--no-start`、`--skip-build`、`--uninstall`、`--purge`。
