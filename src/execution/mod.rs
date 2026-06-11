//! 執行編排：七階段對話框架，驅動上游執行器串流，正規化成 OutEvent。
//! 對應 Python `runtime/execution.py` + `services/completion_bridge.py`。
//!
//! 七階段管線：
//!   Stage 1 — 請求攝取 (Request Intake)
//!   Stage 2 — 提示詞組裝 (Prompt Assembly)  [prompt_builder.rs]
//!   Stage 3 — 會話綁定   (Conversation Binding)
//!   Stage 4 — 上游串流   (Upstream Streaming)
//!   Stage 5 — 內容緩衝   (Content Buffering)
//!   Stage 6 — 工具解析   (Tool Resolution)
//!   Stage 7 — 輸出交付   (Output Delivery)

pub mod formatters;
pub mod presenter;
pub mod translator;

use crate::request::StandardRequest;
use crate::state::AppState;
use crate::stats::{RequestRecord, Stats};
use crate::toolcall::{parse_tool_calls, strip_tool_calls_with, ParsedToolCall};
use crate::upstream::{ImageOptions, StreamParams, UpstreamEvent};
use crate::util::{char_len, now_millis};
use async_stream::stream;
use futures_util::{Stream, StreamExt};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tiktoken_rs::CoreBPE;

static BPE: Lazy<CoreBPE> = Lazy::new(|| tiktoken_rs::cl100k_base().expect("cl100k_base"));

pub fn count_tokens(text: &str) -> usize {
    BPE.encode_ordinary(text).len()
}

#[derive(Debug, Clone, Default)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub reasoning_tokens: i64,
}

/// 正規化輸出事件。
#[derive(Debug, Clone)]
pub enum OutEvent {
    ReasoningDelta(String),
    ContentDelta(String),
    ToolCalls(Vec<ParsedToolCall>),
    Done { usage: Usage, finish_reason: String, email: Option<String> },
    Error(String),
}

// ═══════════════════════════════════════════════════════════════════════
// 七階段會話管線
// ═══════════════════════════════════════════════════════════════════════

/// Stage 1: 請求攝取 — 將 StandardRequest 轉為管線內部參數。
/// 此階段萃取上游所需全部欄位，並做早期驗證。
#[derive(Debug, Clone)]
pub struct PipelineParams {
    pub model: String,
    pub content: String,
    pub has_custom_tools: bool,
    pub files: Vec<Value>,
    pub chat_type: String,
    pub image_options: Option<ImageOptions>,
    pub thinking_enabled: Option<bool>,
    pub enable_search: bool,
    pub fixed_account: Option<String>,
    pub existing_chat_id: Option<String>,
    pub delete_on_close: bool,
    pub use_prewarmed: bool,
    pub max_retries: Option<u32>,
    pub exclude: HashSet<String>,
}

/// Stage 1 工廠：StandardRequest → PipelineParams
fn stage1_intake(std: &StandardRequest) -> PipelineParams {
    let bound = std.bound_account.clone();
    let use_prewarmed = std.chat_type == "t2t" && bound.is_none();
    let is_media = std.chat_type == "t2i" || std.chat_type == "t2v";
    PipelineParams {
        model: std.resolved_model.clone(),
        content: std.prompt.clone(),
        has_custom_tools: std.has_tools(),
        files: std.files.clone(),
        chat_type: std.chat_type.clone(),
        image_options: std.image_options.clone(),
        thinking_enabled: std.thinking_enabled,
        enable_search: std.enable_search,
        fixed_account: bound,
        existing_chat_id: None,
        delete_on_close: true,
        use_prewarmed,
        max_retries: if is_media { Some(1) } else { None },
        exclude: std.exclude_accounts.clone(),
    }
}

/// Stage 2: 提示詞組裝 — 在 prompt_builder.rs 中完成（messages_to_prompt）。
/// 此處僅為管線標記，實際呼叫在 request/mod.rs 層。

/// Stage 3: 會話綁定 — 將 PipelineParams 轉為 StreamParams（上游執行器用）。
fn stage3_bind(params: &PipelineParams) -> StreamParams {
    StreamParams {
        model: params.model.clone(),
        content: params.content.clone(),
        has_custom_tools: params.has_custom_tools,
        files: params.files.clone(),
        chat_type: params.chat_type.clone(),
        image_options: params.image_options.clone(),
        thinking_enabled: params.thinking_enabled,
        enable_search: params.enable_search,
        fixed_account: params.fixed_account.clone(),
        existing_chat_id: params.existing_chat_id.clone(),
        delete_on_close: params.delete_on_close,
        use_prewarmed: params.use_prewarmed,
        max_retries: params.max_retries,
        exclude: params.exclude.clone(),
    }
}

/// Stage 5: 內容緩衝器狀態。
/// 追蹤 reasoning 增量、answer 累積、phase 統計、usage 等。
struct ContentBuffer {
    tracker: ReasoningTracker,
    answer_buf: String,
    streamed_content: bool,
    last_out_tokens: i64,
    last_reasoning_tokens: i64,
    last_email: Option<String>,
    phase_counts: BTreeMap<String, usize>,
    skipped_phase_content_chars: usize,
    ttft_recorded: bool,
}

impl ContentBuffer {
    fn new() -> Self {
        ContentBuffer {
            tracker: ReasoningTracker::new(),
            answer_buf: String::new(),
            streamed_content: false,
            last_out_tokens: 0,
            last_reasoning_tokens: 0,
            last_email: None,
            phase_counts: BTreeMap::new(),
            skipped_phase_content_chars: 0,
            ttft_recorded: false,
        }
    }

    /// Stage 5 核心：處理一個上游 Delta，產出對應的 OutEvent（若有）。
    fn process_delta(
        &mut self,
        d: &crate::upstream::sse::QwenDelta,
        has_tools: bool,
        on_first_token: &mut dyn FnMut(),
    ) -> Vec<OutEvent> {
        let mut out = Vec::new();
        *self.phase_counts.entry(d.phase.clone()).or_insert(0) += 1;

        // reasoning 增量
        let rinc = self.tracker.delta(&d.reasoning_cumulative, &d.reasoning_incremental);
        if !rinc.is_empty() {
            if !self.ttft_recorded {
                on_first_token();
                self.ttft_recorded = true;
            }
            out.push(OutEvent::ReasoningDelta(rinc));
        }

        // content：收集非思考階段
        if !d.content.is_empty() {
            if d.phase != "think" && d.phase != "thinking_summary" {
                if !self.ttft_recorded {
                    on_first_token();
                    self.ttft_recorded = true;
                }
                self.answer_buf.push_str(&d.content);
                if !has_tools {
                    out.push(OutEvent::ContentDelta(d.content.clone()));
                    self.streamed_content = true;
                }
            } else {
                self.skipped_phase_content_chars += d.content.chars().count();
            }
        }
        out
    }

    /// Stage 5 完成：重置本輪累積（跨帳號重試用）。
    fn reset(&mut self) {
        let count = self.answer_buf.chars().count();
        if count > 0 {
            tracing::warn!("[Stage5] 清空本輪 answer_buf({count} chars) / reasoning state");
        }
        self.answer_buf.clear();
        self.streamed_content = false;
        self.tracker = ReasoningTracker::new();
        self.last_out_tokens = 0;
        self.last_reasoning_tokens = 0;
        self.phase_counts.clear();
        self.skipped_phase_content_chars = 0;
        self.ttft_recorded = false;
    }
}

/// Stage 6: 工具解析結果。
struct ToolResolution {
    tool_calls: Vec<ParsedToolCall>,
    yielded_content_at_end: bool,
    suspicious: bool,
}

/// Stage 6: 從緩衝的 answer_buf 解析工具呼叫，並產出清理後的文字。
fn stage6_resolve_tools(
    answer_buf: &str,
    has_tools: bool,
    registry: &HashMap<String, String>,
) -> ToolResolution {
    let mut tool_calls: Vec<ParsedToolCall> = Vec::new();
    if has_tools && !answer_buf.is_empty() {
        tool_calls = parse_tool_calls(answer_buf, registry);
        // 欄位別名 coercion：正規化 arguments 中的常見別名
        for tc in &mut tool_calls {
            coerce_argument_aliases(&mut tc.arguments);
        }
        // 去重：相同 name + 相同 arguments JSON → 只保留第一個
        tool_calls = dedup_tool_calls(tool_calls);
        // 路徑污染偵測：檢查所有 tool call arguments 中的 file/path 欄位
        if !tool_calls.is_empty() {
            let polluted: Vec<_> = tool_calls
                .iter()
                .filter(|tc| has_path_pollution(&tc.arguments))
                .collect();
            if !polluted.is_empty() {
                let names: Vec<_> = polluted.iter().map(|tc| &tc.name).collect();
                tracing::warn!("[Stage6] 路徑污染偵測 triggered by tools: {names:?}");
            }
        }
    }
    ToolResolution {
        tool_calls,
        yielded_content_at_end: false,
        suspicious: false,
    }
}

/// Stage 7: 輸出交付 — 將 Stage 5+6 結果轉為最終 OutEvent 序列。
/// 回傳 (events, usage, finish_reason, email)。
fn stage7_deliver(
    buffer: &mut ContentBuffer,
    resolution: &ToolResolution,
    has_tools: bool,
    registry: &HashMap<String, String>,
    prompt: &str,
) -> (Vec<OutEvent>, Usage, String, Option<String>) {
    let mut events = Vec::new();
    let mut yielded_content_at_end = resolution.yielded_content_at_end;

    // 有工具時，緩衝的可見文字在此一次性送出
    if has_tools && !buffer.streamed_content {
        let cleaned = strip_tool_calls_with(&buffer.answer_buf, registry);
        let cleaned = cleaned.trim();
        if !cleaned.is_empty() {
            events.push(OutEvent::ContentDelta(cleaned.to_string()));
            yielded_content_at_end = true;
        }
    }

    // 工具呼叫
    if !resolution.tool_calls.is_empty() {
        events.push(OutEvent::ToolCalls(resolution.tool_calls.clone()));
    }

    // 保險網 (fail-open)：buffer 非空卻被 strip 清光、又沒解析出 tool_calls
    if has_tools && !buffer.streamed_content && !yielded_content_at_end && resolution.tool_calls.is_empty()
        && !buffer.answer_buf.trim().is_empty()
    {
        events.push(OutEvent::ContentDelta(buffer.answer_buf.clone()));
        yielded_content_at_end = true;
    }

    // 診斷 log
    let cleaned_full = strip_tool_calls_with(&buffer.answer_buf, registry);
    let cleaned_chars = cleaned_full.chars().count();
    let client_saw_nothing = !buffer.streamed_content && !yielded_content_at_end && resolution.tool_calls.is_empty();
    let suspicious_short = has_tools
        && !client_saw_nothing
        && resolution.tool_calls.is_empty()
        && cleaned_chars > 0
        && cleaned_chars < 30;

    if client_saw_nothing && has_tools {
        tracing::warn!(
            "[Stage7] 客戶端零輸出 phases={:?} skipped_phase_chars={} answer_buf_chars={} cleaned_chars={} cleaned_is_empty={} last_email={:?} out_tokens={} reasoning_tokens={} answer_buf_full={:?}",
            buffer.phase_counts,
            buffer.skipped_phase_content_chars,
            buffer.answer_buf.chars().count(),
            cleaned_chars,
            cleaned_full.trim().is_empty(),
            buffer.last_email,
            buffer.last_out_tokens,
            buffer.last_reasoning_tokens,
            buffer.answer_buf,
        );
    } else if suspicious_short {
        tracing::warn!(
            "[Stage7] 客戶端極短輸出（< 30 chars） phases={:?} skipped_phase_chars={} answer_buf_chars={} cleaned_chars={} cleaned_full={:?} last_email={:?} out_tokens={} reasoning_tokens={}",
            buffer.phase_counts,
            buffer.skipped_phase_content_chars,
            buffer.answer_buf.chars().count(),
            cleaned_chars,
            cleaned_full,
            buffer.last_email,
            buffer.last_out_tokens,
            buffer.last_reasoning_tokens,
        );
    }

    // usage
    let visible = if has_tools {
        strip_tool_calls_with(&buffer.answer_buf, registry)
    } else {
        buffer.answer_buf.clone()
    };
    let prompt_tokens = count_tokens(prompt) as i64;
    let completion_tokens = if buffer.last_out_tokens > 0 {
        buffer.last_out_tokens
    } else {
        char_len(&visible) as i64
    };
    let usage = Usage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens + completion_tokens,
        reasoning_tokens: buffer.last_reasoning_tokens,
    };
    let finish_reason = if !resolution.tool_calls.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };

    (events, usage, finish_reason.to_string(), buffer.last_email.clone())
}

// ═══════════════════════════════════════════════════════════════════════
// 管線協調器
// ═══════════════════════════════════════════════════════════════════════

/// 計算 reasoning 增量（處理 qwen3.7 累積陣列）。
struct ReasoningTracker {
    emitted: String,
}
impl ReasoningTracker {
    fn new() -> Self {
        ReasoningTracker { emitted: String::new() }
    }
    fn delta(&mut self, cumulative: &Option<String>, incremental: &str) -> String {
        if let Some(full) = cumulative {
            let inc = if full.starts_with(&self.emitted) {
                full[self.emitted.len()..].to_string()
            } else {
                full.clone()
            };
            self.emitted = full.clone();
            inc
        } else if !incremental.is_empty() {
            self.emitted.push_str(incremental);
            incremental.to_string()
        } else {
            String::new()
        }
    }
}

/// 從上游 usage Value 萃取數值。
fn parse_upstream_usage(v: &Value) -> (i64, i64) {
    let output = v.get("output_tokens").and_then(|x| x.as_i64()).unwrap_or(0);
    let reasoning = v
        .get("output_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|x| x.as_i64())
        .unwrap_or(0);
    (output, reasoning)
}

/// 統計埋點所需的請求中介資料。
struct ProbeMeta {
    surface: String,
    model: String,
    resolved_model: String,
    chat_type: String,
    stream: bool,
    caller: Option<String>,
}

/// 取消安全的統計探針。
struct StatsProbe {
    stats: Arc<Stats>,
    meta: ProbeMeta,
    start: Instant,
    start_ms: i64,
    ttft_ms: Option<i64>,
    usage: Usage,
    success: bool,
    error: Option<String>,
}

impl StatsProbe {
    fn new(stats: Arc<Stats>, meta: ProbeMeta) -> Self {
        StatsProbe {
            stats,
            meta,
            start: Instant::now(),
            start_ms: now_millis(),
            ttft_ms: None,
            usage: Usage::default(),
            success: false,
            error: None,
        }
    }
    fn mark_first_token(&mut self) {
        if self.ttft_ms.is_none() {
            self.ttft_ms = Some(self.start.elapsed().as_millis() as i64);
        }
    }
}

impl Drop for StatsProbe {
    fn drop(&mut self) {
        let duration_ms = self.start.elapsed().as_millis() as i64;
        self.stats.record(RequestRecord {
            ts_ms: self.start_ms,
            surface: std::mem::take(&mut self.meta.surface),
            model: std::mem::take(&mut self.meta.model),
            resolved_model: std::mem::take(&mut self.meta.resolved_model),
            chat_type: std::mem::take(&mut self.meta.chat_type),
            stream: self.meta.stream,
            success: self.success,
            error: self.error.take(),
            prompt_tokens: self.usage.prompt_tokens,
            completion_tokens: self.usage.completion_tokens,
            reasoning_tokens: self.usage.reasoning_tokens,
            total_tokens: self.usage.total_tokens,
            ttft_ms: self.ttft_ms,
            duration_ms,
            caller: self.meta.caller.take(),
        });
    }
}

fn truncate_caller(c: &str) -> String {
    let n = 12;
    if c.chars().count() <= n {
        c.to_string()
    } else {
        format!("{}…", c.chars().take(n).collect::<String>())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 主入口：七階段管線
// ═══════════════════════════════════════════════════════════════════════

/// 主入口：回傳 OutEvent 串流。stream 與 non-stream 處理皆消費此串流。
///
/// 管線階段：
///   Stage 1: intake   → PipelineParams
///   Stage 2: assembly → (prompt_builder.rs, 上游)
///   Stage 3: bind     → StreamParams
///   Stage 4: stream   → executor.run_stream()
///   Stage 5: buffer   → ContentBuffer.process_delta()
///   Stage 6: resolve  → stage6_resolve_tools()
///   Stage 7: deliver  → stage7_deliver()
pub fn run_completion(
    state: AppState,
    std: StandardRequest,
    registry: HashMap<String, String>,
) -> impl Stream<Item = OutEvent> {
    let has_tools = std.has_tools();
    let prompt = std.prompt.clone();
    let executor = state.executor.clone();
    let stats = state.stats.clone();

    // Stage 1: 請求攝取
    let params = stage1_intake(&std);

    let probe_meta = ProbeMeta {
        surface: std.surface.clone(),
        model: std.response_model.clone(),
        resolved_model: std.resolved_model.clone(),
        chat_type: std.chat_type.clone(),
        stream: std.stream,
        caller: std.caller.as_deref().map(truncate_caller),
    };

    stream! {
        let mut probe = StatsProbe::new(stats, probe_meta);

        // Stage 3: 會話綁定
        let stream_params = stage3_bind(&params);

        // Stage 4: 上游串流
        let mut upstream = Box::pin(executor.clone().run_stream(stream_params));

        // Stage 5: 內容緩衝
        let mut buffer = ContentBuffer::new();
        let mut errored = false;

        while let Some(ev) = upstream.next().await {
            match ev {
                UpstreamEvent::Meta { email, .. } => {
                    buffer.last_email = Some(email);
                }
                UpstreamEvent::Delta(d) => {
                    // usage 追蹤
                    if let Some(u) = &d.usage {
                        let (o, r) = parse_upstream_usage(u);
                        if o > 0 { buffer.last_out_tokens = o; }
                        if r > 0 { buffer.last_reasoning_tokens = r; }
                    }
                    // Stage 5 核心處理
                    let events = buffer.process_delta(&d, has_tools, &mut || probe.mark_first_token());
                    for e in events {
                        yield e;
                    }
                }
                UpstreamEvent::Done => break,
                UpstreamEvent::Error(e) => {
                    probe.error = Some(e.clone());
                    probe.success = false;
                    yield OutEvent::Error(e);
                    errored = true;
                    break;
                }
                UpstreamEvent::Retrying => {
                    buffer.reset();
                }
            }
        }

        if errored {
            return;
        }

        // Stage 6: 工具解析
        let resolution = stage6_resolve_tools(&buffer.answer_buf, has_tools, &registry);

        // Stage 7: 輸出交付
        let (mut final_events, usage, finish_reason, last_email) =
            stage7_deliver(&mut buffer, &resolution, has_tools, &registry, &prompt);
        for e in final_events.drain(..) {
            yield e;
        }

        probe.usage = usage.clone();
        probe.success = true;
        yield OutEvent::Done { usage, finish_reason, email: last_email };
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 工具呼叫硬化
// ═══════════════════════════════════════════════════════════════════════

/// 去重：移除重複的 tool call（相同 name + 相同 arguments JSON 序列化）。
/// 保留第一次出現的順序。
pub fn dedup_tool_calls(calls: Vec<ParsedToolCall>) -> Vec<ParsedToolCall> {
    let total = calls.len();
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::with_capacity(total);
    for tc in calls {
        let args_str = serde_json::to_string(&tc.arguments).unwrap_or_default();
        let key = format!("{}|{}", tc.name, args_str);
        if seen.insert(key) {
            out.push(tc);
        }
    }
    if out.len() < total {
        tracing::info!(
            "[Dedup] 移除 {} 個重複 tool call（{} → {}）",
            total - out.len(),
            total,
            out.len()
        );
    }
    out
}

/// 路徑污染偵測：檢查 tool call 的 arguments 中是否含有可疑路徑模式。
/// 覆蓋：`../` 遍歷、絕對路徑 `/etc/`、`~` home、null byte 注入。
pub fn has_path_pollution(args: &Value) -> bool {
    match args {
        Value::Object(map) => {
            for (k, v) in map {
                // 只檢查與路徑/檔案相關的欄位
                let key_lower = k.to_lowercase();
                let is_path_field = key_lower.contains("path")
                    || key_lower.contains("file")
                    || key_lower.contains("dir")
                    || key_lower == "cwd"
                    || key_lower == "root"
                    || key_lower == "output"
                    || key_lower == "input";
                if is_path_field {
                    if let Value::String(s) = v {
                        if contains_path_danger(s) {
                            tracing::warn!(
                                "[PathPollution] 可疑路徑欄位 '{k}' = '{s}'"
                            );
                            return true;
                        }
                    }
                }
                // 遞迴檢查巢狀物件
                if has_path_pollution(v) {
                    return true;
                }
            }
            false
        }
        Value::Array(arr) => arr.iter().any(has_path_pollution),
        _ => false,
    }
}

fn contains_path_danger(s: &str) -> bool {
    // 路徑遍歷
    if s.contains("../") || s.contains("..\\") {
        return true;
    }
    // 絕對路徑（Unix + Windows）
    if s.starts_with('/') && (s.contains("/etc/") || s.contains("/var/") || s.contains("/root/")) {
        return true;
    }
    // Windows 絕對路徑
    let lower = s.to_lowercase();
    if lower.len() >= 2 && lower.as_bytes()[1] == b':' && lower.as_bytes().get(2) == Some(&b'\\') {
        return true;
    }
    // home 目錄
    if s.starts_with("~/") || s.starts_with("~\\") {
        return true;
    }
    // null byte 注入
    if s.contains('\0') {
        return true;
    }
    false
}

/// 欄位別名 coercion：將 arguments 中的常見別名正規化。
/// 例如：`cmd` / `command` / `shell` → 統一為 `command`
pub fn coerce_argument_aliases(args: &mut Value) {
    if let Value::Object(map) = args {
        let aliases: &[(&[&str], &str)] = &[
            (&["cmd", "shell", "exec", "run"], "command"),
            (&["filepath", "file_path", "filename", "path"], "file"),
            (&["directory", "folder"], "dir"),
            (&["regex", "pattern", "grep"], "query"),
            (&["output_file", "outfile", "out"], "output"),
            (&["input_file", "infile"], "input"),
        ];
        for (sources, target) in aliases {
            for src in *sources {
                if let Some(v) = map.remove(*src) {
                    if !map.contains_key(*target) {
                        map.insert(target.to_string(), v);
                    }
                    break; // 只取第一個匹配的別名
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 共用函式
// ═══════════════════════════════════════════════════════════════════════

/// 非串流：消費整個串流，回傳聚合結果。
pub struct CollectedResult {
    pub content: String,
    pub reasoning: String,
    pub tool_calls: Vec<ParsedToolCall>,
    pub usage: Usage,
    pub finish_reason: String,
    pub error: Option<String>,
    pub email: Option<String>,
}

pub async fn collect_completion(
    state: AppState,
    std: StandardRequest,
    registry: HashMap<String, String>,
) -> CollectedResult {
    let mut s = Box::pin(run_completion(state, std, registry));
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls = Vec::new();
    let mut usage = Usage::default();
    let mut finish_reason = "stop".to_string();
    let mut error = None;
    let mut email = None;
    while let Some(ev) = s.next().await {
        match ev {
            OutEvent::ReasoningDelta(r) => reasoning.push_str(&r),
            OutEvent::ContentDelta(c) => content.push_str(&c),
            OutEvent::ToolCalls(tc) => tool_calls = tc,
            OutEvent::Done { usage: u, finish_reason: fr, email: em } => {
                usage = u;
                finish_reason = fr;
                email = em;
            }
            OutEvent::Error(e) => error = Some(e),
        }
    }
    CollectedResult { content, reasoning, tool_calls, usage, finish_reason, error, email }
}

/// 共用：取得某請求的工具註冊表。
pub fn registry_for(std: &StandardRequest) -> HashMap<String, String> {
    let normalized = crate::toolcall::normalize_tools(&std.tools);
    crate::toolcall::build_registry(&normalized)
}

// 讓 Arc<Executor> 可被 clone 進 stream
type _AssertSend = Arc<crate::upstream::Executor>;

// ═══════════════════════════════════════════════════════════════════════
// 測試
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── 工具去重 ──

    #[test]
    fn dedup_removes_identical_calls() {
        let calls = vec![
            ParsedToolCall { id: "a".into(), name: "Bash".into(), arguments: json!({"cmd": "ls"}) },
            ParsedToolCall { id: "b".into(), name: "Bash".into(), arguments: json!({"cmd": "ls"}) },
            ParsedToolCall { id: "c".into(), name: "Bash".into(), arguments: json!({"cmd": "pwd"}) },
        ];
        let result = dedup_tool_calls(calls);
        assert_eq!(result.len(), 2, "應移除重複的 ls 呼叫");
        assert_eq!(result[0].id, "a");
        assert_eq!(result[1].id, "c");
    }

    #[test]
    fn dedup_keeps_unique_calls() {
        let calls = vec![
            ParsedToolCall { id: "a".into(), name: "Read".into(), arguments: json!({"file": "a.txt"}) },
            ParsedToolCall { id: "b".into(), name: "Read".into(), arguments: json!({"file": "b.txt"}) },
        ];
        let result = dedup_tool_calls(calls);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn dedup_empty_input() {
        assert!(dedup_tool_calls(vec![]).is_empty());
    }

    // ── 路徑污染偵測 ──

    #[test]
    fn detects_path_traversal() {
        assert!(has_path_pollution(&json!({"path": "../etc/passwd"})));
        assert!(has_path_pollution(&json!({"file": "..\\windows\\system32"})));
        assert!(has_path_pollution(&json!({"dir": "../../root"})));
    }

    #[test]
    fn detects_absolute_sensitive_paths() {
        assert!(has_path_pollution(&json!({"path": "/etc/passwd"})));
        assert!(has_path_pollution(&json!({"file": "/var/log/auth.log"})));
    }

    #[test]
    fn detects_home_directory() {
        assert!(has_path_pollution(&json!({"output": "~/.ssh/id_rsa"})));
    }

    #[test]
    fn detects_null_byte() {
        assert!(has_path_pollution(&json!({"file": "safe.txt\0.php"})));
    }

    #[test]
    fn allows_safe_paths() {
        assert!(!has_path_pollution(&json!({"path": "data/output.txt"})));
        assert!(!has_path_pollution(&json!({"file": "project/src/main.rs"})));
        assert!(!has_path_pollution(&json!({"dir": "tmp/build"})));
    }

    #[test]
    fn ignores_non_path_fields() {
        // name / description / query 不是路徑欄位，不觸發
        assert!(!has_path_pollution(&json!({"name": "../etc/passwd"})));
        assert!(!has_path_pollution(&json!({"query": "/etc/secret"})));
        assert!(!has_path_pollution(&json!({"description": "../../root"})));
    }

    #[test]
    fn nested_path_pollution() {
        assert!(has_path_pollution(&json!({
            "options": {"path": "../etc/shadow"}
        })));
    }

    // ── 欄位別名 coercion ──

    #[test]
    fn coerce_cmd_to_command() {
        let mut args = json!({"cmd": "ls -la"});
        coerce_argument_aliases(&mut args);
        assert_eq!(args["command"], "ls -la");
        assert!(args.get("cmd").is_none());
    }

    #[test]
    fn coerce_shell_to_command() {
        let mut args = json!({"shell": "bash script.sh"});
        coerce_argument_aliases(&mut args);
        assert_eq!(args["command"], "bash script.sh");
    }

    #[test]
    fn coerce_does_not_overwrite_existing() {
        // 已有 command，不該被 cmd 覆蓋
        let mut args = json!({"command": "existing", "cmd": "should_not_override"});
        coerce_argument_aliases(&mut args);
        assert_eq!(args["command"], "existing");
    }

    #[test]
    fn coerce_filepath_to_file() {
        let mut args = json!({"file_path": "/tmp/data.txt"});
        coerce_argument_aliases(&mut args);
        assert_eq!(args["file"], "/tmp/data.txt");
    }

    #[test]
    fn coerce_regex_to_query() {
        let mut args = json!({"pattern": "error.*"});
        coerce_argument_aliases(&mut args);
        assert_eq!(args["query"], "error.*");
    }

    // ── Stage 1: 請求攝取 ──

    #[test]
    fn stage1_intake_t2t_uses_prewarmed() {
        let std = StandardRequest {
            response_model: "gpt-4o".into(),
            resolved_model: "qwen3-max".into(),
            prompt: "hello".into(),
            stream: true,
            thinking_enabled: None,
            force_thinking: false,
            enable_search: false,
            chat_type: "t2t".into(),
            tools: vec![],
            tool_names: vec![],
            surface: "openai".into(),
            image_options: None,
            max_tokens: None,
            client_profile: "generic".into(),
            files: vec![],
            bound_account: None,
            caller: None,
            exclude_accounts: HashSet::new(),
        };
        let params = stage1_intake(&std);
        assert!(params.use_prewarmed);
        assert_eq!(params.chat_type, "t2t");
    }

    #[test]
    fn stage1_intake_bound_account_disables_prewarmed() {
        let std = StandardRequest {
            response_model: "gpt-4o".into(),
            resolved_model: "qwen3-max".into(),
            prompt: "hello".into(),
            stream: true,
            thinking_enabled: None,
            force_thinking: false,
            enable_search: false,
            chat_type: "t2t".into(),
            tools: vec![],
            tool_names: vec![],
            surface: "openai".into(),
            image_options: None,
            max_tokens: None,
            client_profile: "generic".into(),
            files: vec![],
            bound_account: Some("fixed@test.com".into()),
            caller: None,
            exclude_accounts: HashSet::new(),
        };
        let params = stage1_intake(&std);
        assert!(!params.use_prewarmed, "綁定帳號時不該用預熱池");
        assert_eq!(params.fixed_account, Some("fixed@test.com".into()));
    }

    #[test]
    fn stage1_intake_media_sets_max_retries_1() {
        let std = StandardRequest {
            response_model: "dall-e-3".into(),
            resolved_model: "qwen3-max".into(),
            prompt: "a cat".into(),
            stream: false,
            thinking_enabled: None,
            force_thinking: false,
            enable_search: false,
            chat_type: "t2i".into(),
            tools: vec![],
            tool_names: vec![],
            surface: "openai".into(),
            image_options: None,
            max_tokens: None,
            client_profile: "generic".into(),
            files: vec![],
            bound_account: None,
            caller: None,
            exclude_accounts: HashSet::new(),
        };
        let params = stage1_intake(&std);
        assert_eq!(params.max_retries, Some(1));
    }
}