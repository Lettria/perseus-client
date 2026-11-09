from dotenv import load_dotenv

load_dotenv()
import logging

logging.getLogger("google_genai").setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

import json

import bleach
import markdown
from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    request,
    stream_with_context,
)
from simple_graph_retriever.client import GraphRetrievalClient
from simple_graph_retriever.models import RetrievalConfig
from google import genai
from google.genai import types as genai_types
from utils import make_tei_embed_fn, wait_for_embedder

MODEL_NAME = "gemini-3-flash-preview"

# Allow-list for the HTML that rendered markdown is allowed to produce,
# since the LLM reply (and anything it pulled from the graph) is untrusted
# input being inserted into the page.
ALLOWED_TAGS = [
    "p",
    "br",
    "strong",
    "em",
    "del",
    "ul",
    "ol",
    "li",
    "blockquote",
    "code",
    "pre",
    "h1",
    "h2",
    "h3",
    "h4",
    "a",
    "hr",
    "table",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
]
ALLOWED_ATTRIBUTES = {"a": ["href", "title", "rel", "target"]}


def render_markdown(text: str) -> str:
    html = markdown.markdown(
        text, extensions=["fenced_code", "tables", "sane_lists", "nl2br"]
    )
    return bleach.clean(
        html, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRIBUTES, strip=True
    )


app = Flask(__name__)

genai_client = genai.Client()
retrieval_client = None


def llm_fn(prompt: str) -> str:
    response = genai_client.models.generate_content(
        model="gemini-3-flash-preview", contents=prompt
    )
    return response.text or ""


def get_retrieval_client() -> GraphRetrievalClient:
    """Lazily builds the retrieval client so the embedder is only waited on
    once a chat request actually comes in, not at import time."""
    global retrieval_client
    if retrieval_client is None:
        wait_for_embedder()
        retrieval_client = GraphRetrievalClient(
            embed_fn=make_tei_embed_fn(), llm_fn=llm_fn
        )
    return retrieval_client


def history_to_contents(history: list[dict]) -> list[genai_types.Content]:
    """Converts a chat history of {role: "user" | "ai", content: str}
    messages into the genai SDK's Content objects, which use "model"
    rather than "ai" for the assistant role."""
    contents = []
    for turn in history:
        role = "model" if turn.get("role") == "ai" else "user"
        content = turn.get("content", "")
        if content:
            contents.append(
                genai_types.Content(role=role, parts=[genai_types.Part(text=content)])
            )
    return contents


def generate_chat_stream(message: str, history: list[dict]):
    """Runs retrieval + the final LLM call, yielding NDJSON-ready dicts:
    a "step" dict per RetrievalStepEvent as it happens, then one "answer"
    dict once the reply is ready."""
    config = RetrievalConfig(
        community_score_drop_off_pct=0.3,
        chunk_score_drop_off_pct=0.3,
        max_communities=5,
        max_chunks=20,
    )

    result = None
    for event in get_retrieval_client().iter_retrieve_graph(message, config):
        if event.key == "done":
            result = event.result
            continue
        yield {
            "type": "step",
            "key": event.key,
            "title": event.title,
            "output": event.output,
            "sub_events": [se.model_dump() for se in event.sub_events],
            "data": event.data,
        }

    context_markdown = (
        result.to_markdown() if result else "No relevant data found in the graph."
    )

    system_instruction = f"""
        You are a helpful assistant answering questions using a knowledge graph.
        Use the following retrieved graph context to answer the user's latest message.
        Only mention information that is related to the question directly or indirectly.
        Use tables when relevant to present data clearly.
        If the context does not contain the answer, say so instead of making things up.

        Retrieved context:
        {context_markdown}
    """

    contents = history_to_contents(history)
    contents.append(
        genai_types.Content(role="user", parts=[genai_types.Part(text=message)])
    )

    reply_chunks = []
    for chunk in genai_client.models.generate_content_stream(
        model=MODEL_NAME,
        contents=contents,
        config=genai_types.GenerateContentConfig(
            system_instruction=system_instruction,
            thinking_config=genai_types.ThinkingConfig(thinking_budget=128),
        ),
    ):
        if chunk.text:
            reply_chunks.append(chunk.text)
            yield {"type": "answer_chunk", "text": chunk.text}

    reply = "".join(reply_chunks) or "I don't have an answer for that."
    yield {"type": "answer", "reply": reply, "reply_html": render_markdown(reply)}


CHAT_PAGE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>graph rag</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap">
<style>
  :root {
    --color-bg: #000000;
    --color-surface: #1A1A1A;
    --color-surface-2: #2A2A2A;
    --color-text: #FFFFFF;
    --color-text-muted: #888888;
    --color-text-dim: #AAAAAA;
    --color-text-dim2: #DDDDDD;
    --color-avatar-ai-bg: #3A3A3A;
    --fg-rgb: 255, 255, 255;
    --accent-blue: #60A5FA;
    --accent-purple: #A78BFA;
  }
  :root[data-theme="light"] {
    --color-bg: #FFFFFF;
    --color-surface: #F0F0F0;
    --color-surface-2: #E6E6E6;
    --color-text: #111111;
    --color-text-muted: #666666;
    --color-text-dim: #555555;
    --color-text-dim2: #333333;
    --color-avatar-ai-bg: #DADADA;
    --fg-rgb: 0, 0, 0;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: 'Inter', -apple-system, 'Segoe UI', Roboto, sans-serif;
    background: var(--color-bg);
    color: var(--color-text);
    height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
  }
  button { font-family: inherit; }
  input::placeholder { color: var(--color-text-muted); }
  ::selection { background: rgba(var(--fg-rgb), 0.25); }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }
  @keyframes pulse-dot {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.35; transform: scale(0.7); }
  }
  @keyframes shimmer {
    0% { background-position: 150% 0; }
    100% { background-position: -50% 0; }
  }
  .shimmer-text {
    background-image: linear-gradient(100deg, var(--color-text-muted) 38%, var(--color-text) 50%, var(--color-text-muted) 62%);
    background-size: 250% 100%;
    -webkit-background-clip: text;
    background-clip: text;
    color: var(--color-text-muted);
    animation: shimmer 1.6s linear infinite;
  }

  .app {
    width: 100%;
    max-width: 920px;
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }

  header {
    flex: none;
    width: 100%;
    height: 36px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 20px;
    border-bottom: 1px solid rgba(var(--fg-rgb), 0.07);
    box-sizing: border-box;
  }
  header .title {
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.01em;
    color: var(--color-text-muted);
  }
  header .actions { display: flex; align-items: center; gap: 4px; }
  header .divider { width: 1px; height: 16px; background: rgba(var(--fg-rgb), 0.12); margin: 0 4px; }
  #reset-btn, #theme-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    background: transparent;
    border: none;
    color: var(--color-text-muted);
    cursor: pointer;
    border-radius: 6px;
  }
  #reset-btn:hover, #theme-btn:hover { color: var(--color-text); }

  #messages {
    flex: 1;
    overflow-y: auto;
    scroll-behavior: smooth;
    padding: 32px 24px 24px;
    display: flex;
    flex-direction: column;
    gap: 28px;
    scrollbar-width: thin;
    scrollbar-color: rgba(var(--fg-rgb), 0.18) transparent;
  }
  #messages::-webkit-scrollbar { width: 8px; }
  #messages::-webkit-scrollbar-track { background: transparent; }
  #messages::-webkit-scrollbar-thumb {
    background: rgba(var(--fg-rgb), 0.15);
    border-radius: 8px;
    border: 2px solid transparent;
    background-clip: content-box;
  }
  #messages::-webkit-scrollbar-thumb:hover { background: rgba(var(--fg-rgb), 0.28); background-clip: content-box; }
  .empty-state {
    margin: auto;
    text-align: center;
    color: var(--color-text-muted);
    font-size: 0.85rem;
    max-width: 300px;
  }

  .row { display: flex; gap: 0.6rem; max-width: 88%; }
  .row.user { align-self: flex-end; flex-direction: row-reverse; }
  .row.ai { align-self: flex-start; width: 100%; }

  .avatar {
    flex: none;
    width: 1.7rem;
    height: 1.7rem;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.68rem;
    font-weight: 600;
  }
  .row.user .avatar { background: var(--color-text); color: var(--color-bg); }
  .row.ai .avatar { background: var(--color-avatar-ai-bg); color: var(--color-text); border: 1px solid rgba(var(--fg-rgb), 0.3); }

  .user-bubble {
    background: var(--color-surface-2);
    color: var(--color-text);
    padding: 10px 16px;
    border-radius: 18px;
    font-size: 0.92rem;
    line-height: 1.55;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }

  .col { display: flex; flex-direction: column; gap: 10px; min-width: 0; flex: 1; }

  .thinking {
    border-radius: 12px;
    overflow: hidden;
  }
  .thinking-header {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;
    background: transparent;
    border: none;
    text-align: left;
    padding: 10px 14px;
    cursor: pointer;
    color: var(--color-text-muted);
    font-size: 0.8rem;
  }
  .thinking-steps { padding: 0 14px 10px; display: flex; flex-direction: column; }

  .step { border-top: 1px solid rgba(var(--fg-rgb), 0.06); padding-top: 4px; margin-top: 4px; }
  .step-header {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;
    background: transparent;
    border: none;
    text-align: left;
    padding: 8px 2px;
    cursor: pointer;
    color: var(--color-text);
    font-size: 0.83rem;
  }
  .step-content {
    margin: 0 2px 8px 23px;
    padding: 8px 11px;
    background: var(--color-bg);
    border: 1px solid rgba(var(--fg-rgb), 0.06);
    border-radius: 8px;
    font-size: 0.8rem;
    line-height: 1.5;
    color: var(--color-text-dim);
  }
  .substep { margin-top: 8px; padding-top: 8px; border-top: 1px dashed rgba(var(--fg-rgb), 0.14); }
  .substep-header {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 6px;
    background: transparent;
    border: none;
    text-align: left;
    padding: 3px 0;
    cursor: pointer;
    color: var(--color-text-dim);
    font-size: 0.75rem;
    font-family: ui-monospace, Menlo, Consolas, monospace;
  }
  .substep-output {
    margin: 4px 0 0 15px;
    padding: 6px 9px;
    background: var(--color-surface-2);
    border-radius: 6px;
    font-size: 0.72rem;
    font-family: ui-monospace, Menlo, Consolas, monospace;
    color: var(--color-text-dim2);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  .step-data {
    margin: 4px 0 0 15px;
    padding: 8px 10px;
    background: var(--color-bg);
    border-radius: 6px;
    font-size: 0.72rem;
    font-family: ui-monospace, Menlo, Consolas, monospace;
    color: var(--color-text-dim2);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    max-height: 220px;
    overflow-y: auto;
  }
  pre.step-data { font-size:0.62rem; }

  .answer { font-size: 0.95rem; line-height: 1.55; color: var(--color-text); overflow-wrap: anywhere; }
  .answer :first-child { margin-top: 0; }
  .answer :last-child { margin-bottom: 0; }
  .answer p { margin: 0.5em 0; }
  .answer ul, .answer ol { margin: 0.4em 0; padding-left: 1.3em; }
  .answer li { margin: 0.15em 0; }
  .answer li > p { margin: 0.15em 0; }
  .answer a { color: var(--color-text); text-decoration: underline; }
  .answer blockquote { margin: 0.5em 0; padding: 0.1em 0.8em; border-left: 3px solid var(--color-text); color: var(--color-text-dim); }
  .answer h1, .answer h2, .answer h3, .answer h4 { margin: 0.7em 0 0.35em; line-height: 1.3; }
  .answer code {
    background: var(--color-surface-2);
    padding: 0.1em 0.35em;
    border-radius: 0.3em;
    font-size: 0.85em;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  }
  .answer pre { background: var(--color-surface-2); padding: 0.65em 0.85em; border-radius: 0.6em; overflow-x: auto; margin: 0.5em 0; }
  .answer pre code { background: none; padding: 0; }
  .answer table { border-collapse: collapse; margin: 0.5em 0; font-size: 0.88em; width: 100%; }
  .answer th, .answer td { border: 1px solid rgba(var(--fg-rgb), 0.1); padding: 0.35em 0.6em; text-align: left; }
  .answer th { background: var(--color-surface-2); }
  .answer hr { border: none; border-top: 1px solid rgba(var(--fg-rgb), 0.1); margin: 0.6em 0; }

  .error-bubble {
    color: var(--color-text);
    background: rgba(var(--fg-rgb), 0.08);
    border: 1px solid rgba(var(--fg-rgb), 0.25);
    padding: 10px 14px;
    border-radius: 10px;
    font-size: 0.85rem;
  }

  form#chat-form { flex: none; padding: 14px 24px 22px; }
  .composer {
    max-width: 920px;
    margin: 0 auto;
    display: flex;
    align-items: center;
    background: var(--color-surface);
    border: 1px solid rgba(var(--fg-rgb), 0.12);
    border-radius: 22px;
    padding: 4px 18px;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
  }
  #chat-input {
    flex: 1;
    border: none;
    outline: none;
    font-size: 0.95rem;
    padding: 12px 0;
    background: transparent;
    color: var(--color-text);
  }
</style>
</head>
<body>
  <header>
    <div class="title">graph rag</div>
    <div class="actions">
      <button id="theme-btn" type="button" aria-label="Toggle light / dark theme"></button>
      <div class="divider"></div>
      <button id="reset-btn" type="button" aria-label="Start a new chat">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M12 5v14M5 12h14" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
      </button>
    </div>
  </header>

  <div class="app">
    <div id="messages"></div>

    <form id="chat-form">
      <div class="composer">
        <label for="chat-input" class="sr-only">Message</label>
        <input id="chat-input" type="text" placeholder="Message..." autocomplete="off" required>
      </div>
    </form>
  </div>

  <script>
    const messagesEl = document.getElementById("messages");
    const formEl = document.getElementById("chat-form");
    const inputEl = document.getElementById("chat-input");
    const resetBtn = document.getElementById("reset-btn");
    const themeBtn = document.getElementById("theme-btn");

    // Conversation history sent to the backend on every request so it can
    // be injected back into the LLM call, each turn tagged "user" | "ai".
    let history = [];
    let messages = [];
    let seed = 0;

    function escapeHtml(str) {
      const div = document.createElement("div");
      div.textContent = str;
      return div.innerHTML;
    }

    const ICONS = {
      chevron: (deg) => `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" aria-hidden="true" style="flex:none;color:var(--color-text-muted);transform:rotate(${deg});transition:transform .15s"><path d="M6 9l6 6 6-6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
      chevronSmall: (deg) => `<svg width="9" height="9" viewBox="0 0 24 24" fill="none" aria-hidden="true" style="flex:none;transform:rotate(${deg});transition:transform .15s"><path d="M6 9l6 6 6-6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
      done: `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true" style="flex:none;color:var(--accent-purple)"><circle cx="12" cy="12" r="10" fill="currentColor" opacity="0.18"/><path d="M7 12.5l3 3 7-7" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
      active: `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true" style="flex:none;color:var(--accent-blue)"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="1.6" opacity="0.4"/><circle cx="12" cy="12" r="4" fill="currentColor" style="animation: pulse-dot 1.1s ease-in-out infinite"/></svg>`,
      thinkingDot: (active) => `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true" style="flex:none;color:${active ? "var(--accent-blue)" : "var(--accent-purple)"}"><circle cx="12" cy="12" r="9" stroke="currentColor" stroke-width="1.6" opacity="0.4"/><circle cx="12" cy="12" r="3" fill="currentColor" ${active ? 'style="animation: pulse-dot 1.1s ease-in-out infinite"' : ""}/></svg>`,
      sun: `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true"><circle cx="12" cy="12" r="4" stroke="currentColor" stroke-width="2"/><path d="M12 2v3M12 19v3M4.2 4.2l2.1 2.1M17.7 17.7l2.1 2.1M2 12h3M19 12h3M4.2 19.8l2.1-2.1M17.7 6.3l2.1-2.1" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>`,
      moon: `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    };

    function applyTheme(theme) {
      document.documentElement.setAttribute("data-theme", theme);
      themeBtn.innerHTML = theme === "light" ? ICONS.moon : ICONS.sun;
      try { localStorage.setItem("theme", theme); } catch (e) {}
    }

    let theme;
    try { theme = localStorage.getItem("theme"); } catch (e) {}
    if (!theme) {
      theme = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
    }
    applyTheme(theme);

    themeBtn.addEventListener("click", () => {
      applyTheme(document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light");
    });

    function render() {
      if (!messages.length) {
        messagesEl.innerHTML = '<div class="empty-state">Start a conversation. Each message is answered using context retrieved live from the graph.</div>';
      } else {
        messagesEl.innerHTML = messages.map(renderMessage).join("");
      }
    }

    function isScrolledToBottom() {
      return messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < 80;
    }

    function scrollToBottom() {
      messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function renderMessage(m) {
      if (m.role === "user") {
        return `<div class="row user"><div class="avatar">U</div><div class="user-bubble">${escapeHtml(m.text)}</div></div>`;
      }

      const hasThinking = m.steps.length > 0;
      const activeStep = m.steps.find((s) => s.status === "active");
      const thinkingLabel = activeStep ? activeStep.title : (hasThinking ? "Context retrieved" : "");
      const thinkingChevron = m.thinkingOpen ? "90deg" : "0deg";

      let thinkingHtml = "";
      if (hasThinking) {
        const stepsHtml = m.thinkingOpen ? m.steps.map((s) => renderStep(m.id, s)).join("") : "";
        thinkingHtml = `
          <div class="thinking">
            <button class="thinking-header" data-action="toggle-thinking" data-msg="${m.id}">
              ${activeStep ? ICONS.thinkingDot(true) : ICONS.done}
              <span style="flex:1;font-weight:500" class="shimmer-text">${thinkingLabel}</span>
              ${ICONS.chevron(thinkingChevron)}
            </button>
            ${m.thinkingOpen ? `<div class="thinking-steps">${stepsHtml}</div>` : ""}
          </div>`;
      }

      const answerHtml = m.html
        ? `<div class="answer">${m.html}</div>`
        : (m.streamText ? `<div class="answer" style="white-space:pre-wrap">${escapeHtml(m.streamText)}</div>` : "");
      const errorHtml = m.error ? `<div class="error-bubble">Error: ${escapeHtml(m.error)}</div>` : "";

      return `<div class="row ai"><div class="avatar">AI</div><div class="col">${thinkingHtml}${answerHtml}${errorHtml}</div></div>`;
    }

    function renderStep(msgId, s) {
      const isActive = s.status === "active";
      const icon = isActive ? ICONS.active : ICONS.done;
      const titleHtml = isActive
        ? `<span class="shimmer-text">${escapeHtml(s.title)}</span>`
        : `<span>${escapeHtml(s.title)}</span>`;
      const chev = s.open ? "90deg" : "0deg";

      let contentHtml = "";
      if (s.open) {
        const subsHtml = (s.subEvents || []).map((sub, i) => renderSub(msgId, s.key, i, sub)).join("");
        const dataHtml = s.data != null ? renderStepData(msgId, s) : "";
        contentHtml = `<div class="step-content">${s.output ? escapeHtml(s.output) : ""}${subsHtml}${dataHtml}</div>`;
      }

      return `<div class="step">
        <button class="step-header" data-action="toggle-step" data-msg="${msgId}" data-step="${s.key}">
          ${icon}${titleHtml}${ICONS.chevron(chev)}
        </button>
        ${contentHtml}
      </div>`;
    }

    function renderStepData(msgId, s) {
      const chev = s.dataOpen ? "90deg" : "0deg";
      const body = s.dataOpen
        ? `<pre class="step-data">${escapeHtml(JSON.stringify(s.data, null, 2))}</pre>`
        : "";
      return `<div class="substep">
        <button class="substep-header" data-action="toggle-data" data-msg="${msgId}" data-step="${s.key}">
          ${ICONS.chevronSmall(chev)}<span style="flex:1">Data</span>
        </button>
        ${body}
      </div>`;
    }

    function renderSub(msgId, stepKey, idx, sub) {
      const chev = sub.open ? "90deg" : "0deg";
      const outputHtml = sub.open ? `<div class="substep-output">${escapeHtml(sub.output)}</div>` : "";
      return `<div class="substep">
        <button class="substep-header" data-action="toggle-sub" data-msg="${msgId}" data-step="${stepKey}" data-sub="${idx}">
          ${ICONS.chevronSmall(chev)}<span style="flex:1">${escapeHtml(sub.title)}</span>
        </button>
        ${outputHtml}
      </div>`;
    }

    messagesEl.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-action]");
      if (!btn) return;
      const msg = messages.find((m) => m.id === btn.dataset.msg);
      if (!msg) return;

      if (btn.dataset.action === "toggle-thinking") {
        msg.thinkingOpen = !msg.thinkingOpen;
      } else if (btn.dataset.action === "toggle-step") {
        const step = msg.steps.find((s) => s.key === btn.dataset.step);
        if (step) step.open = !step.open;
      } else if (btn.dataset.action === "toggle-sub") {
        const step = msg.steps.find((s) => s.key === btn.dataset.step);
        const sub = step && step.subEvents[Number(btn.dataset.sub)];
        if (sub) sub.open = !sub.open;
      } else if (btn.dataset.action === "toggle-data") {
        const step = msg.steps.find((s) => s.key === btn.dataset.step);
        if (step) step.dataOpen = !step.dataOpen;
      }
      render();
    });

    // Only the step that's currently running stays expanded; earlier steps
    // auto-collapse to a one-line summary (still clickable to reopen).
    function handleEvent(asstMsg, parsed) {
      const stick = isScrolledToBottom();
      if (parsed.type === "step") {
        asstMsg.steps.forEach((s) => {
          if (s.status === "active") {
            s.status = "done";
            s.open = false;
          }
        });
        asstMsg.steps.push({
          key: parsed.key,
          title: parsed.title,
          output: parsed.output,
          subEvents: (parsed.sub_events || []).map((se) => ({ title: se.title, output: se.output, open: false })),
          data: parsed.data ?? null,
          dataOpen: false,
          status: "active",
          open: true,
        });
      } else if (parsed.type === "answer_chunk") {
        asstMsg.streamText = (asstMsg.streamText || "") + parsed.text;
      } else if (parsed.type === "answer") {
        asstMsg.steps.forEach((s) => {
          s.status = "done";
        });
        asstMsg.text = parsed.reply;
        asstMsg.html = parsed.reply_html;
        asstMsg.streamText = "";
      }
      render();
      if (stick) scrollToBottom();
    }

    resetBtn.addEventListener("click", () => {
      messages = [];
      history = [];
      render();
    });

    render();

    formEl.addEventListener("submit", async (event) => {
      event.preventDefault();
      const message = inputEl.value.trim();
      if (!message) return;

      inputEl.value = "";
      inputEl.disabled = true;

      const asstMsg = { id: "a" + (++seed), role: "assistant", text: "", html: "", streamText: "", error: null, steps: [], thinkingOpen: false };
      messages.push({ id: "u" + (++seed), role: "user", text: message }, asstMsg);
      render();
      scrollToBottom();

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message, history }),
        });

        if (!response.ok || !response.body) {
          const data = await response.json().catch(() => ({}));
          throw new Error(data.error || "Request failed");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          let newlineIndex;
          while ((newlineIndex = buffer.indexOf("\\n")) !== -1) {
            const line = buffer.slice(0, newlineIndex);
            buffer = buffer.slice(newlineIndex + 1);
            if (!line.trim()) continue;

            const parsed = JSON.parse(line);
            if (parsed.type === "error") throw new Error(parsed.error || "Request failed");
            handleEvent(asstMsg, parsed);
          }
        }

        history.push({ role: "user", content: message });
        history.push({ role: "ai", content: asstMsg.text });
      } catch (err) {
        const stick = isScrolledToBottom();
        asstMsg.error = err.message;
        render();
        if (stick) scrollToBottom();
      } finally {
        inputEl.disabled = false;
        inputEl.focus();
      }
    });
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(CHAT_PAGE)


@app.route("/api/chat", methods=["POST"])
def chat():
    payload = request.get_json(silent=True) or {}
    message = (payload.get("message") or "").strip()
    history = payload.get("history") or []

    if not message:
        return jsonify({"error": "message is required"}), 400
    if not isinstance(history, list):
        return jsonify({"error": "history must be a list"}), 400

    def stream():
        try:
            for event in generate_chat_stream(message, history):
                # default=str guards against step "data" carrying a DB-driver
                # type json can't serialize natively (e.g. a temporal value).
                yield json.dumps(event, default=str) + "\n"
        except Exception as e:
            logging.error(f"Failed to answer chat message: {e}", exc_info=True)
            yield json.dumps(
                {"type": "error", "error": "Failed to generate a response"}
            ) + "\n"

    return Response(stream_with_context(stream()), mimetype="application/x-ndjson")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
