//! 請求執行器：帳號層重試、預熱池快路徑、SSE framing、資源清理。
//! 對應 Python `upstream/qwen_executor.py` 的 chat_stream_events_with_retry。

use super::chat_id_pool::ChatIdPool;
use super::client::QwenClient;
use super::payload::{build_chat_payload, BuildPayloadArgs, ImageOptions};
use super::sse::{extract_upstream_error, parse_sse_chunk, QwenDelta};
use crate::account::AccountPool;
use crate::config::Settings;
use async_stream::stream;
use futures_util::Stream;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;

/// 執行器輸出的事件。
#[derive(Debug, Clone)]
pub enum UpstreamEvent {
    Meta { chat_id: String, email: String },
    Delta(QwenDelta),
    Done,
    Error(String),
}

#[derive(Clone)]
pub struct StreamParams {
    pub model: String,
    pub content: String,
    pub has_custom_tools: bool,
    pub files: Vec<serde_json::Value>,
    pub chat_type: String,
    pub image_options: Option<ImageOptions>,
    pub thinking_enabled: Option<bool>,
    pub enable_search: bool,
    pub fixed_account: Option<String>,
    pub existing_chat_id: Option<String>,
    pub delete_on_close: bool,
    pub use_prewarmed: bool,
    /// 本次請求的帳號層重試上限（None＝用 executor 預設）。影像/影片把重試交給應用層精準控制，故設 1。
    pub max_retries: Option<u32>,
    /// 取帳號時要繞過的 email 集合（如 t2v 已知無權限的帳號）。
    pub exclude: HashSet<String>,
}

impl Default for StreamParams {
    fn default() -> Self {
        StreamParams {
            model: String::new(),
            content: String::new(),
            has_custom_tools: false,
            files: Vec::new(),
            chat_type: "t2t".into(),
            image_options: None,
            thinking_enabled: None,
            enable_search: false,
            fixed_account: None,
            existing_chat_id: None,
            delete_on_close: true,
            use_prewarmed: true,
            max_retries: None,
            exclude: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct Executor {
    pub pool: Arc<AccountPool>,
    pub client: Arc<QwenClient>,
    pub chat_id_pool: Arc<ChatIdPool>,
    pub max_retries: u32,
    pub delete_attempts: u32,
    pub delete_delay_ms: u64,
}

/// 取消安全的資源 guard：drop 時（含 client 中途斷線導致 stream future 被丟棄）
/// 一定釋放帳號並刪除本次建立的上游會話，各一次。對應修復「斷線洩漏」。
struct StreamGuard {
    pool: Arc<AccountPool>,
    client: Arc<QwenClient>,
    /// Some = 由帳號池取得，需 release；fixed_account 則為 None。
    email: Option<String>,
    token: String,
    /// Some = 本次建立、需刪除的會話。
    chat_id: Option<String>,
    delete_attempts: u32,
    delete_delay_ms: u64,
    armed: bool,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let pool = self.pool.clone();
        let client = self.client.clone();
        let email = self.email.clone();
        let token = self.token.clone();
        let chat_id = self.chat_id.clone();
        let (attempts, delay) = (self.delete_attempts, self.delete_delay_ms);
        // Drop 不能 await → spawn detached 清理任務
        tokio::spawn(async move {
            if let Some(cid) = chat_id {
                client.delete_chat_reliable(&token, &cid, attempts, delay).await;
            }
            if let Some(em) = email {
                pool.release(&em).await;
            }
        });
    }
}

impl Executor {
    pub fn new(pool: Arc<AccountPool>, client: Arc<QwenClient>, chat_id_pool: Arc<ChatIdPool>, settings: &Settings) -> Self {
        Executor {
            pool,
            client,
            chat_id_pool,
            max_retries: settings.max_retries,
            delete_attempts: settings.chat_delete_retry_attempts,
            delete_delay_ms: settings.chat_delete_retry_delay_ms,
        }
    }

    /// 取得會話 id：優先用既有；否則嘗試預熱池；再否則建新會話。
    /// 回傳 (chat_id, owns)（owns=true 表示是本次建立、結束時可刪）。
    async fn obtain_chat_id(
        &self,
        token: &str,
        email: &str,
        model: &str,
        chat_type: &str,
        existing: &Option<String>,
        use_prewarmed: bool,
    ) -> Result<(String, bool), crate::error::AppError> {
        if let Some(cid) = existing {
            return Ok((cid.clone(), false));
        }
        if use_prewarmed && chat_type == "t2t" {
            // 傳入 token：回補與過期刪除都用它，避免在熱路徑查 pool（O(n) 鎖競爭）。
            if let Some(cid) = self.chat_id_pool.acquire(email, token, model).await {
                tracing::debug!("[執行器] 預熱池命中 email={email} chat={cid}");
                return Ok((cid, true));
            }
        }
        let cid = self.client.create_chat(token, model, chat_type).await?;
        Ok((cid, true))
    }

    /// 主串流：產生 UpstreamEvent 流。內含帳號層重試。
    pub fn run_stream(self: Arc<Self>, params: StreamParams) -> impl Stream<Item = UpstreamEvent> {
        stream! {
            // 初始 exclude：呼叫端傳入的「已知不可用」集合（如 t2v 無權限帳號）
            let mut exclude: HashSet<String> = params.exclude.clone();
            let mut last_error: Option<String> = None;
            let attempts = if params.fixed_account.is_some() {
                1
            } else {
                params.max_retries.unwrap_or(self.max_retries)
            };

            for attempt in 0..attempts {
                // 1) 取得帳號
                let handle = if let Some(email) = &params.fixed_account {
                    match self.pool.token_of(email).await {
                        Some(token) => crate::account::AccountHandle { email: email.clone(), token },
                        None => { yield UpstreamEvent::Error(format!("指定帳號不存在: {email}")); return; }
                    }
                } else {
                    match self.pool.acquire_wait(None, &exclude, 60.0).await {
                        Some(h) => h,
                        None => {
                            // 若先前嘗試已有真實錯誤，浮現它而非掩蓋成「無可用帳號」
                            let msg = last_error.clone().unwrap_or_else(|| {
                                "帳號池無可用帳號（全忙或限流）".into()
                            });
                            yield UpstreamEvent::Error(msg);
                            return;
                        }
                    }
                };
                let email = handle.email.clone();
                let token = handle.token.clone();
                let is_pool_acquired = params.fixed_account.is_none();

                // 取消安全 guard：取得帳號後立刻建立，所有離開路徑（成功/重試/錯誤/斷線）皆由它清理。
                let mut guard = StreamGuard {
                    pool: self.pool.clone(),
                    client: self.client.clone(),
                    email: if is_pool_acquired { Some(email.clone()) } else { None },
                    token: token.clone(),
                    chat_id: None,
                    delete_attempts: self.delete_attempts,
                    delete_delay_ms: self.delete_delay_ms,
                    armed: true,
                };

                // 2) 取得會話
                let (chat_id, owns) = match self
                    .obtain_chat_id(&token, &email, &params.model, &params.chat_type, &params.existing_chat_id, params.use_prewarmed)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        // guard 在 continue 時 drop → 釋放帳號
                        let msg = e.to_string();
                        last_error = Some(msg.clone());
                        self.classify_and_mark(&email, &msg).await;
                        exclude.insert(email.clone());
                        tracing::warn!("[執行器] 建會話失敗 第{}次 email={email} err={msg}", attempt + 1);
                        continue;
                    }
                };
                // 本次建立的會話交由 guard 負責刪除
                if owns && params.delete_on_close {
                    guard.chat_id = Some(chat_id.clone());
                }

                yield UpstreamEvent::Meta { chat_id: chat_id.clone(), email: email.clone() };

                // 3) 串流
                let payload = build_chat_payload(&BuildPayloadArgs {
                    chat_id: &chat_id,
                    model: &params.model,
                    content: &params.content,
                    has_custom_tools: params.has_custom_tools,
                    files: params.files.clone(),
                    chat_type: &params.chat_type,
                    image_options: params.image_options.clone(),
                    thinking_enabled: params.thinking_enabled,
                    enable_search: params.enable_search,
                });

                let resp = match self.client.start_stream(&token, &chat_id, &payload).await {
                    Ok(r) => r,
                    Err(e) => {
                        // 串流尚未開始 → 可重試；guard 在 continue 時 drop → 刪會話 + 釋放帳號
                        let msg = e.to_string();
                        last_error = Some(msg.clone());
                        self.classify_and_mark(&email, &msg).await;
                        exclude.insert(email.clone());
                        tracing::warn!("[執行器] 串流啟動失敗 第{}次 email={email} err={msg}", attempt + 1);
                        continue;
                    }
                };

                // 4) 消費 SSE bytes
                let mut byte_stream = resp.bytes_stream();
                let mut buffer: Vec<u8> = Vec::with_capacity(8192);
                let mut first_delta = false;
                let mut stream_error: Option<String> = None;
                let mut retryable = false;

                'consume: loop {
                    match byte_stream.next().await {
                        Some(Ok(chunk)) => {
                            buffer.extend_from_slice(&chunk);
                            // 以 b"\n\n" 切分完整訊息（\n 不會出現在 UTF-8 多位元組序列中）
                            loop {
                                let pos = find_subslice(&buffer, b"\n\n");
                                let Some(p) = pos else { break };
                                let msg_bytes: Vec<u8> = buffer.drain(..p + 2).collect();
                                let msg = String::from_utf8_lossy(&msg_bytes[..p]);
                                if let Some(err) = extract_upstream_error(&msg) {
                                    if first_delta {
                                        stream_error = Some(err);
                                        retryable = false;
                                    } else {
                                        stream_error = Some(err);
                                        retryable = true;
                                    }
                                    break 'consume;
                                }
                                for d in parse_sse_chunk(&msg) {
                                    first_delta = true;
                                    yield UpstreamEvent::Delta(d);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            stream_error = Some(format!("串流讀取錯誤: {e}"));
                            retryable = !first_delta;
                            break 'consume;
                        }
                        None => break 'consume,
                    }
                }
                // 處理殘餘 buffer
                if stream_error.is_none() && !buffer.is_empty() {
                    let msg = String::from_utf8_lossy(&buffer);
                    if let Some(err) = extract_upstream_error(&msg) {
                        stream_error = Some(err);
                        retryable = !first_delta;
                    } else {
                        for d in parse_sse_chunk(&msg) {
                            first_delta = true;
                            yield UpstreamEvent::Delta(d);
                        }
                    }
                }

                // 5) 結束（清理由 guard 在 drop 時統一處理：釋放帳號 + 刪會話，恰好一次）
                match stream_error {
                    None => {
                        if is_pool_acquired {
                            self.pool.mark_success(&email).await;
                        }
                        yield UpstreamEvent::Done;
                        return; // guard drop → 刪會話 + 釋放帳號
                    }
                    Some(err) => {
                        if !retryable || first_delta {
                            yield UpstreamEvent::Error(err);
                            return; // guard drop → 清理
                        }
                        last_error = Some(err.clone());
                        self.classify_and_mark(&email, &err).await;
                        exclude.insert(email.clone());
                        tracing::warn!("[執行器] 串流錯誤可重試 第{}次 email={email} err={err}", attempt + 1);
                        continue; // guard drop → 清理本次帳號/會話，下輪重新取得
                    }
                }
            }

            yield UpstreamEvent::Error(format!(
                "全部 {} 次嘗試失敗。最後錯誤: {}",
                attempts,
                last_error.unwrap_or_else(|| "未知".into())
            ));
        }
    }

    /// 依錯誤訊息分類並標記帳號狀態。
    async fn classify_and_mark(&self, email: &str, err: &str) {
        let lower = err.to_lowercase();
        // 含影片/影像每日額度上限（code=RateLimited / "upper limit for today's usage"）：
        // 視為限流並冷卻，使帳號池輪換到其他帳號（重試找有額度者）。
        if lower.contains("429")
            || lower.contains("rate limit")
            || lower.contains("ratelimited")
            || lower.contains("too many")
            || lower.contains("upper limit")
            || lower.contains("today's usage")
        {
            self.pool.mark_rate_limited(email).await;
        } else if lower.contains("unauthorized") || lower.contains("401") || lower.contains("403") {
            let reason = if lower.contains("activation") || lower.contains("pending") {
                "pending_activation"
            } else {
                "auth_error"
            };
            // 把原始上游錯誤訊息一併寫進 last_error；mark_invalid 對 auth_error 帶門檻，
            // 連續失敗 N 次（預設 3，AUTH_ERROR_FAIL_THRESHOLD）才永久 valid=false。
            self.pool.mark_invalid(email, reason, err).await;
        }
        // timeout / 其他：僅 exclude，不改帳號狀態
    }
}

/// 在 byte slice 中尋找子序列位置。
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}
