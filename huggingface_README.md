---
title: qwen2API 企业网关
emoji: 🔑
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
license: mit
---

# qwen2API 企业网关

将 Qwen（通义千问）Web 端能力以 OpenAI / Anthropic Claude / Gemini 兼容 API 暴露的自托管网关。

## 功能特点

- OpenAI Chat Completions（流式 + 非流式）
- OpenAI Responses API（SSE 事件）
- Anthropic Messages API（流式 + 非流式）
- Gemini generateContent / streamGenerateContent
- 图片生成（`/v1/images/generations`）
- 视频生成（`/v1/videos/generations`）
- 文件上传（`/v1/files`）+ 对话附件
- Tool / Function Calling
- 账号池：4 层并发控制、最小负载选择、限流指数退避、跨账号重试
- chat_id 预热池
- 管理面板：运行状态、账号管理、API 密钥、接口测试、图片/视频生成、系统设置

## 快速开始

1. 部署到 HuggingFace Spaces 后打开 Space URL
2. 进入「系统设置」页面输入 ADMIN_KEY（或 API Key）作为会话密钥
3. 在「账号管理」页面注入 Qwen 账号
4. 使用生成的 API Key 调用接口

### 调用示例

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://your-space.hf.space/v1",
    api_key="your-api-key",
)

resp = client.chat.completions.create(
    model="qwen3.7-plus",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
)
for chunk in resp:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

## 环境变量配置

在 HuggingFace Spaces 的 **Settings → Variables and secrets** 中设置：

| 变量名 | 必填 | 说明 |
| --- | --- | --- |
| `ADMIN_KEY` | **是** | 管理面板密钥 |
| `PORT` | 否 | 服务端口，默认 7860 |
| `MAX_INFLIGHT_PER_ACCOUNT` | 否 | 每账号最大并发，默认 2 |
| `DEFAULT_MODEL` | 否 | 默认模型，默认 qwen3.7-plus |

## 注意事项

- 数据存储在容器的 `/app/data` 目录中，Space 重启后数据可能丢失
- 建议将 `ADMIN_KEY` 设置为 Space Secret
- 需要注入 Qwen 账号的 Token 才能正常使用

## 许可证

[MIT](./LICENSE)
