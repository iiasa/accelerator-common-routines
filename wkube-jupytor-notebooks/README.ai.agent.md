# 🧠 Jupyter AI Model Configuration & Usage Guide

This guide explains how to set up **Jupyter AI** to use a specific model and API, enable and configure the **Chat section**, set a default model, and use the `%%ai` magic commands with different output formats.

---

## 🌐 Model & API Details

To use Jupyter AI, make sure you have the correct **model name** and **base API URL**:

- **Model name:** `openai/gpt-oss-120b`  
- **Base API URL:** `https://api.groq.com/openai/v1`

You’ll need to provide your API key securely (e.g., via environment variables, `.env` file, or when prompted by Jupyter). No extra configuration files are required.

---

## 💬 Enable the Chat Section

Jupyter AI includes a **chat panel** that allows you to interact with the model conversationally.

1. Launch **Jupyter Notebook** or **JupyterLab**.  
2. Click the **AI Chat** icon in the left sidebar to open the chat panel.  
3. If the extension hasn’t been loaded yet, enable it by running:

```python
%load_ext jupyter_ai_magics
```

Once loaded, you can open the chat interface in the sidebar to start interacting with the model directly.

---

## 🧩 Configure the Chat Section Settings

To use the Chat feature properly, make sure the **chat provider** is set to the correct model interface:

1. Open the **AI Chat panel** in the Jupyter sidebar.  
2. Click on the **⚙️ Settings** (gear icon) in the chat panel.  
3. Under **Completion model**, select:  
   ```
   OpenAI (general interface) ::*
   ```
4. In the **Model ID** field, enter:
   ```
   openai/gpt-oss-120b
   ```
5. Make sure your API key is set in the **API Key** field (Groq or OpenAI compatible key).  
6. Save the settings.

> ✅ After this setup, your chat panel will be connected to the `openai/gpt-oss-120b` model using the Groq-compatible OpenAI API.

---

## ⚙️ Set a Default Model for Magics (Optional)

To avoid repeating the model name in every `%%ai` magic cell, set a **default model**:

```python
%config AiMagics.initial_language_model = "openai/gpt-oss-120b"
```

> Once set, you can use `%%ai` without specifying the model each time.

---

## ✨ Using the `%%ai` Magic Commands

The `%%ai` magic lets you send prompts to the model directly in notebook cells.

### 📌 **Basic Syntax**

```python
%%ai openai-chat-custom:openai/gpt-oss-120b -f <format>
<prompt here>
```

- `openai-chat-custom` → Custom chat interface  
- `openai/gpt-oss-120b` → Model name  
- `-f` → Output format (see list below)

> If you’ve set a default model, this can be shortened to:
>
> ```python
> %%ai -f code
> Write a Python function to reverse a string.
> ```

---

## 🧪 Example: Generate a Histogram

```python
%%ai openai-chat-custom:openai/gpt-oss-120b -f code
Generate me a histogram of the following dataset: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
```

The model will return Python code that you can execute in a new cell.

---

## 🧰 Supported Output Formats

The `-f` flag controls how the model’s response is formatted. Available formats:

| Format      | Description                                                                                      |
|------------|---------------------------------------------------------------------------------------------------|
| `code`     | Generates executable code (e.g., Python)                                                          |
| `image`    | For Hugging Face Hub’s text-to-image models only                                                  |
| `markdown` | Returns formatted Markdown text                                                                   |
| `math`     | Renders LaTeX-style math expressions                                                              |
| `html`     | Returns raw HTML output                                                                          |
| `json`     | Returns structured JSON data                                                                     |
| `text`     | Returns plain text (default for most models)                                                      |

> 💡 Choose the format based on your use case. For example, use `code` for scripts, `markdown` for explanations, or `json` for structured outputs.

---

## 🚀 Ready to Go

With the **chat panel** configured, **default model** set (optional), and the **AI magics** loaded, you’re ready to interact with powerful AI models inside Jupyter — for code generation, data analysis, documentation, and more.

P.S For more details see [here for magic command](https://jupyter-ai.readthedocs.io/en/latest/users/index.html#the-ai-and-ai-magic-commands)  and [here for chat](https://jupyter-ai.readthedocs.io/en/latest/users/index.html#the-chat-interface)