---
title: Cricket Players
emoji: "🏏"
colorFrom: blue
colorTo: indigo
sdk: streamlit
sdk_version: 1.33.0
app_file: app.py
pinned: false
---

# Cricket RAG Chatbot (Open Source)

Streamlit + LangChain + FAISS + HuggingFace local-model RAG chatbot for IPL/cricketer PDFs.

## Deploy on Hugging Face Spaces

1. Create a new Space (SDK: **Streamlit**).
2. Push this repo content to that Space.
3. In Space Settings -> Variables, optionally set:
   - `HF_MODEL_ID=TinyLlama/TinyLlama-1.1B-Chat-v1.0`
   - `DATA_DIR=data`
   - `INDEX_DIR=faiss_index`
   - `TOP_K=4`
   - `MAX_NEW_TOKENS=192`
4. Ensure `data/` contains PDFs and optional `player_metadata.json` + `images/`.
5. Space will auto-install from `requirements.txt` and start `app.py`.

## Notes

- Free CPU Spaces may be slow with larger models.
- Keep model small for free hosting.
- If `data/` changes, delete `faiss_index/` and restart Space to rebuild embeddings.

