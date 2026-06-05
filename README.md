<div align="center">

<img src="docs/screenshots/banner.svg" alt="qwen2api-rs — Qwen Web → OpenAI · Anthropic · Gemini" width="100%"/>

# qwen2api-rs

把通義千問（Qwen）Web 端能力轉換成 **OpenAI / Anthropic Claude / Gemini** 相容介面的自託管網關。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-dea584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![axum](https://img.shields.io/badge/axum-0.8-000000)](https://github.com/tokio-rs/axum)
[![tokio](https://img.shields.io/badge/tokio-1.x-3776ab)](https://tokio.rs)
[![Docker](https://img.shields.io/badge/docker-ready-2496ed?logo=docker&logoColor=white)](https://www.docker.com)

</div>

> **本專案是 [YuJunZhiXue/qwen2API](https://github.com/YuJunZhiXue/qwen2API)（Python + React）的 Rust 後端 + 純原生前端重寫版**，並非原作者，原始設計與協議分析歸功於上游。基準上游版本與同步流程見 [`dev/UPSTREAM.md`](dev/UPSTREAM.md)。

- **後端**：Rust（`axum` + `tokio` + `reqwest` + `serde`），單一靜態二進位，低記憶體、高並發。
- **前端**：純 `HTML + CSS + JS` 三檔（`web/`），零框架、零建置、可離線。
- **協議**：在同一個 binary 內同時提供 OpenAI / Anthropic / Gemini 三套 API 表面。

---

## 功能

- ✅ OpenAI Chat Completions（`/v1/chat/completions`）串流 + 非串流
- ✅ OpenAI Responses（`/v1/responses`）typed SSE events
- ✅ Anthropic Messages（`/v1/messages`、`/anthropic/v1/messages`）串流 + 非串流 + `count_tokens`
- ✅ Gemini `generateContent` / `streamGenerateContent`
- ✅ OpenAI Images（`/v1/images/generations`）— 驅動 Qwen 影像生成
- ✅ OpenAI Embeddings（佔位，確定性向量）
- ✅ 檔案上傳（`/v1/files`）+ 對話附件（自動阿里 OSS V4 上傳 / 小文字檔內聯）
- ✅ 工具/函式調用：工具定義注入 prompt + 從輸出解析 `tool_call`（Qwen Web 無原生工具）
- ✅ 思考模式（reasoning）串流，**usage 採上游真實 token 數**
- ✅ 帳號池：4 層並發控制、最少負載選號、限流指數退避、跨帳號重試
- ✅ chat_id 預熱池（規避上游 `/chats/new` 0.5~6s 握手；對上萬帳號有覆蓋數上限保護）
- ✅ 管理台 WebUI：運行狀態、帳號管理、API Key、接口測試、圖片生成、系統設置
- ✅ `/healthz`、`/readyz` 探針

## 介面預覽

<table>
  <tr>
    <td align="center" width="50%">
      <img src="docs/screenshots/stats.png" alt="統計面板：請求量、Tokens、TTFT、按模型/接口拆分"/>
      <sub><b>數據統計</b> · 請求 / Tokens / TTFT 分桶，按模型 + 接口拆分</sub>
    </td>
    <td align="center" width="50%">
      <img src="docs/screenshots/images.png" alt="圖片生成：批次提交與本地畫廊"/>
      <sub><b>圖片生成</b> · 批次提交、比例切換、本地永久保存</sub>
    </td>
  </tr>
  <tr>
    <td align="center" colspan="2">
      <img src="docs/screenshots/videos.png" alt="影片生成：異步任務佇列 + 智慧跳過無 t2v 權限帳號" width="70%"/>
      <br/><sub><b>影片生成</b> · 異步任務佇列 + 自動重試 + 智慧跳過無 t2v 權限帳號</sub>
    </td>
  </tr>
</table>

## 快速開始

需求：Rust 1.80+（已測 1.93）。

```bash
cp .env.example .env          # 設定 ADMIN_KEY、PORT 等
mkdir -p data
# 放入帳號：data/accounts.json = [{"email","token", ...}, ...]
#   token = 在 chat.qwen.ai 登入後，localStorage 裡的 token 原始值
#   範本見 dev/accounts.example.json
cargo run --release
```

啟動後：
- WebUI：`http://127.0.0.1:7860/`（系統設置頁貼上 `ADMIN_KEY` 或任一 API Key 作為會話金鑰）
- API Base：`http://127.0.0.1:7860`

呼叫範例（OpenAI 相容）：

```bash
curl http://127.0.0.1:7860/v1/chat/completions \
  -H "Authorization: Bearer <你的 API Key 或 ADMIN_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

Anthropic 相容：

```bash
curl http://127.0.0.1:7860/v1/messages \
  -H "x-api-key: <你的 API Key 或 ADMIN_KEY>" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":1024,"messages":[{"role":"user","content":"你好"}]}'
```

模型名可用任意 OpenAI / Claude / Gemini 名稱（自動映射到 Qwen，未知者回退 `DEFAULT_MODEL`），或直接用 `qwen3.7-plus`、`qwen3.7-plus-thinking` 等（`/v1/models` 可查全部）。

## 部署

兩種皆可（單一靜態 binary，rustls 無需系統 OpenSSL）。

### Docker（推薦，尤其從原 Python 版遷移者）

與原專案相同的 docker-compose 工作流；映像約 145MB（debian-slim 基底，比原 Python 版含 camoufox 小很多；可改 distroless/musl 再瘦身）。

```bash
# data/ 可直接沿用原版（放 accounts.json 等）
mkdir -p data
vim docker-compose.yml      # 修改 ADMIN_KEY 等
docker compose up -d --build
docker compose logs -f
```

- 資料持久化：`./data` 掛到容器 `/app/data`。
- 內建 `HEALTHCHECK`（打 `/healthz`）。
- 更新：`git pull && docker compose up -d --build`。

### Binary（最輕量，單機 / VPS）

```bash
cargo build --release          # 產出 target/release/qwen2api-rs
cp .env.example .env && vim .env
mkdir -p data                  # 放 accounts.json
WEB_DIR=web ./target/release/qwen2api-rs
```

建議用 systemd 常駐（`/etc/systemd/system/qwen2api-rs.service`）：

```ini
[Unit]
Description=qwen2api-rs gateway
After=network.target

[Service]
WorkingDirectory=/opt/qwen2api-rs
EnvironmentFile=/opt/qwen2api-rs/.env
ExecStart=/opt/qwen2api-rs/qwen2api-rs
Restart=always

[Install]
WantedBy=multi-user.target
```

> **Docker vs Binary 取捨**：Docker = 可重現、隔離、跨發行版可攜、與原版同流程、易更新 / 重啟；Binary = 啟動最快、佔用最小、無需 docker，但需自行用 systemd 常駐且跨機需注意 glibc 版本（或用 musl 靜態編譯）。

## 環境變數

完整清單見 [`.env.example`](.env.example)（含風控 / 帳號池 / 上下文等 20+ 項）。變數名與原 Python 版相容，可直接指向同一份 `data/`。

| 變數 | 用途 | 預設 |
|---|---|---|
| `PORT` | 服務埠 | `7860` |
| `ADMIN_KEY` | 管理台金鑰 | `change-me-now` |
| `MAX_INFLIGHT_PER_ACCOUNT` | 每帳號同時在途請求數 | `2` |
| `MAX_RETRIES` | 跨帳號重試次數 | `3` |
| `ACCOUNT_MIN_INTERVAL_MS` | 同帳號最小請求間隔（風控） | `3000` |
| `CHAT_ID_PREWARM_TARGET_PER_ACCOUNT` | chat_id 預熱池每帳號目標數 | `5` |
| `DEFAULT_MODEL` | 未知下游模型回退 | `qwen3.7-plus` |
| `DATA_DIR` | 資料目錄 | `./data` |

## 認證

- 下游請求：`Authorization: Bearer <key>`、`x-api-key`、或 `?key=`。
- 若 `data/api_keys.json` 有設定 key，則必須使用 `ADMIN_KEY` / 已建立的 key；否則放行任意 key。
- 管理台 `/api/admin/*`：`Bearer` 須等於 `ADMIN_KEY` 或已建立的 key。

## 架構

技術棧與 Python→Rust 模組對應見 [`dev/ARCHITECTURE.md`](dev/ARCHITECTURE.md)；實測捕捉的上游協議（含 SSE 格式）見 [`dev/PROTOCOL.md`](dev/PROTOCOL.md)。

```
src/
  main.rs            入口 / 路由
  config.rs state.rs db.rs error.rs util.rs auth.rs
  account/           帳號池（account.rs / pool.rs）
  upstream/          上游傳輸（client / payload / sse / executor / chat_id_pool）
  request/           標準請求構建（model_modes / prompt_builder / client_profiles / model_catalog）
  toolcall/          工具調用（注入 + 解析 + 名稱混淆）
  execution/         編排 + 串流翻譯（translator / presenter / formatters）
  context/           附件 / OSS V4 上傳 / 本地檔案庫
  api/               各協議端點（openai / anthropic / gemini / responses / images / videos / files / embeddings / admin / probes）
  stats.rs           SQLite 統計子系統
  media.rs           媒體任務佇列（圖片 / 影片）
web/                 純前端三檔（index.html / app.js / style.css）
dev/                 開發筆記（上游版本追蹤、架構、協議捕捉、部署）
```

## 與原專案 ([YuJunZhiXue/qwen2API](https://github.com/YuJunZhiXue/qwen2API)) 的刻意差異

1. **移除瀏覽器自動註冊**（camoufox / Playwright + 臨時郵箱）→ 僅手動貼 token；額外提供 `chat.qwen.ai` 純 HTTP signin + 自動 refresh worker。
2. **usage 改用上游真實 token 數**（原版用字元數估算）。
3. **預設旗艦模型**更新為 `qwen3.7-plus`。
4. **工具調用**採單一穩定文字格式（`<tool_call>{json}</tool_call>`）注入 + 解析（brace-balanced，無 regex 失誤）。
5. **媒體子系統**：圖片 / 影片改為非同步任務佇列（SQLite 持久化）+ 自動重試 + 智慧跳過無 t2v 權限帳號。
6. **觀測**：新增 `stats.rs` 統計面板（請求量 / Tokens / TTFT / 按模型 + 接口拆分）。

詳見 [`dev/UPSTREAM.md`](dev/UPSTREAM.md)。

## 致謝

- 上游原專案 [**YuJunZhiXue/qwen2API**](https://github.com/YuJunZhiXue/qwen2API) — 原始協議分析、多協議轉換思路、整套帳號池 / 風控設計皆源自此。
- Qwen / 通義千問 by 阿里巴巴 — 模型與 web 介面。

## 授權

[MIT License](LICENSE)

> 僅供學習與自託管研究。Qwen 為阿里巴巴商標，使用需遵守其服務條款；本專案不保證上游 web 介面行為穩定。
