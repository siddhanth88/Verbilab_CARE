"""
CARE Processing Pipeline v8
============================
- Sarvam STT + Sarvam-M scoring
- Agent-only transcript for scoring
- Parallel chunk processing via ffmpeg
- Bulletproof JSON extraction (handles <think> blocks)
- S3 / Drive / URL / direct upload
"""

import os, json, re, threading, tempfile, shutil, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests

CHUNK_SECONDS = 25


# ════════════════════════════════════════════════════════
#  SOURCE CONNECTORS
# ════════════════════════════════════════════════════════

def fetch_from_google_drive(url: str, dest_dir: str) -> str:
    """
    Download from Google Drive shareable link.
    Handles all URL formats including ?usp=drive_link suffix.
    """
    import re as _re
    # Extract file ID — handle all Drive URL formats
    file_id = None

    # Format: /file/d/FILE_ID/view or /file/d/FILE_ID?...
    m = _re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
    if m:
        file_id = m.group(1)
    # Format: ?id=FILE_ID or &id=FILE_ID
    elif "id=" in url:
        m = _re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
        if m: file_id = m.group(1)
    # Format: /open?id=FILE_ID
    elif "/open" in url:
        m = _re.search(r"id=([a-zA-Z0-9_-]+)", url)
        if m: file_id = m.group(1)
    else:
        # Assume raw file ID was passed
        file_id = url.strip().split("?")[0].split("/")[-1]

    if not file_id:
        raise RuntimeError(f"Could not extract Google Drive file ID from URL: {url}")

    print(f"[GDRIVE] File ID: {file_id}")

    # Try direct download URL
    dl = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})

    r = s.get(dl, stream=True, timeout=120)

    # Handle Google virus scan warning for large files
    for k, v in r.cookies.items():
        if "download_warning" in k or "confirm" in k.lower():
            dl2 = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={v}"
            r = s.get(dl2, stream=True, timeout=120)
            break

    # Detect filename from headers
    content_disp = r.headers.get("Content-Disposition", "")
    fname = f"gdrive_{file_id}.mp3"
    if "filename=" in content_disp:
        fname_match = _re.search(r"filename[*]?=[\"']?([^\"';]+)", content_disp)
        if fname_match:
            fname = fname_match.group(1).strip().strip('"').strip("'")

    dest = os.path.join(dest_dir, fname)
    total = 0
    with open(dest, "wb") as f:
        for chunk in r.iter_content(32768):
            if chunk:
                f.write(chunk)
                total += len(chunk)

    if total < 1000:
        raise RuntimeError(
            f"Google Drive download failed — only {total} bytes received. "
            f"Make sure the file is shared as 'Anyone with link can view'."
        )
    print(f"[GDRIVE] Done → {dest} ({total//1024}KB)")
    return dest


def fetch_from_url(url: str, dest_dir: str) -> str:
    fname = url.split("/")[-1].split("?")[0] or "audio.mp3"
    if not any(fname.lower().endswith(x) for x in [".mp3",".wav",".m4a",".ogg",".flac",".aac",".wma"]):
        fname += ".mp3"
    dest = os.path.join(dest_dir, fname)
    print(f"[URL] Downloading {url}...")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(32768):
            if chunk: f.write(chunk)
    print(f"[URL] Done → {os.path.getsize(dest)//1024}KB")
    return dest


def fetch_from_s3(s3_uri: str, dest_dir: str) -> str:
    try:
        import boto3
    except ImportError:
        raise ImportError("Run: pip install boto3")
    uri = s3_uri.replace("s3://", "")
    bucket, key = uri.split("/", 1)
    dest = os.path.join(dest_dir, os.path.basename(key))
    print(f"[S3] Downloading s3://{bucket}/{key}...")
    boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    ).download_file(bucket, key, dest)
    print(f"[S3] Done → {os.path.getsize(dest)//1024}KB")
    return dest


def resolve_audio_source(source: str, dest_dir: str) -> str:
    if source.startswith("s3://"):
        return fetch_from_s3(source, dest_dir)
    if "drive.google.com" in source:
        return fetch_from_google_drive(source, dest_dir)
    if source.startswith("http://") or source.startswith("https://"):
        return fetch_from_url(source, dest_dir)
    return source


# ════════════════════════════════════════════════════════
#  AUDIO CHUNKING
# ════════════════════════════════════════════════════════

def split_audio(path: str, chunk_sec: int = CHUNK_SECONDS):
    tmpdir = tempfile.mkdtemp(prefix="care_chunks_")
    pattern = os.path.join(tmpdir, "chunk_%04d.mp3")
    r = subprocess.run(
        ["ffmpeg", "-i", path, "-f", "segment",
         "-segment_time", str(chunk_sec), "-c:a", "libmp3lame",
         "-q:a", "4", "-y", pattern],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print("[CHUNK] ffmpeg unavailable — single file mode (install ffmpeg for speed)")
        shutil.rmtree(tmpdir, ignore_errors=True)
        return [path], None
    chunks = sorted([os.path.join(tmpdir, f) for f in os.listdir(tmpdir) if f.startswith("chunk_")])
    if not chunks:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return [path], None
    print(f"[CHUNK] {len(chunks)} chunks of {chunk_sec}s")
    return chunks, tmpdir


# ════════════════════════════════════════════════════════
#  TRANSCRIPTION
# ════════════════════════════════════════════════════════

def _transcribe_chunk(chunk_path: str, api_key: str, idx: int):
    with open(chunk_path, "rb") as f:
        data = f.read()
    r = requests.post(
        "https://api.sarvam.ai/speech-to-text-translate",
        headers={"api-subscription-key": api_key},
        files={"file": (os.path.basename(chunk_path), data, "audio/mpeg")},
        data={"model": "saaras:v3", "language_code": "unknown", "target_language_code": "en-IN"},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"[CHUNK {idx}] Error {r.status_code}: {r.text[:100]}")
        return idx, ""
    text = r.json().get("transcript", "").strip()
    print(f"[CHUNK {idx}] {len(text)} chars")
    return idx, text


def _extract_agent_only(full_transcript: str):
    """
    Returns (agent_only_for_scoring, full_for_display)
    Scoring uses agent lines only — display shows full transcript.
    """
    lines = full_transcript.split("\n")
    agent_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        upper = line.upper()
        # Explicitly agent lines
        if any(upper.startswith(p) for p in ["AGENT:", "AGENT :"]):
            # Strip the AGENT: prefix for cleaner scoring
            text = line.split(":", 1)[1].strip() if ":" in line else line
            agent_lines.append(f"AGENT: {text}")
        # Explicitly skip customer lines
        elif any(upper.startswith(p) for p in ["CUSTOMER:", "CUSTOMER :", "CALLER:", "CLIENT:", "BORROWER:"]):
            continue  # skip customer turns for scoring
        else:
            # Untagged — include (single speaker recording or unclear)
            agent_lines.append(line)

    agent = "\n".join(agent_lines) if agent_lines else full_transcript
    print(f"[BIFURCATION] Full: {len(full_transcript)} chars | Agent-only: {len(agent)} chars")
    return agent, full_transcript


def transcribe(audio_path: str):
    key = os.getenv("SARVAM_API_KEY")
    if not key: raise EnvironmentError("SARVAM_API_KEY not set")
    mb = os.path.getsize(audio_path) / 1024 / 1024
    print(f"[STT] {os.path.basename(audio_path)} ({mb:.1f} MB)")
    chunks, tmpdir = split_audio(audio_path)
    try:
        if len(chunks) == 1:
            _, text = _transcribe_chunk(chunks[0], key, 0)
        else:
            results = {}
            with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as ex:
                futs = {ex.submit(_transcribe_chunk, c, key, i): i for i, c in enumerate(chunks)}
                for f in as_completed(futs):
                    i, t = f.result(); results[i] = t
            text = " ".join(results[i] for i in sorted(results))
        print(f"[STT] Done — {len(text)} chars")
        return _extract_agent_only(text)
    finally:
        if tmpdir: shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════════════════
#  SCORING — BULLETPROOF JSON EXTRACTION
# ════════════════════════════════════════════════════════

SCORING_PROMPT = """You are a QA auditor for Company Finance collections call centre.
Score ONLY the AGENT (ignore customer). Output ONLY raw JSON starting with {{ - no thinking, no explanation.

FRAMEWORK (20 pts):
A1 Opening (0-2): disclaimer + company name + customer name + RPC confirmed
A2 Case Knowledge (0-2): exact amount + DPD days + loan details stated
A3 Probing (0-3) CRITICAL: deep follow-up, asked for proof if excuse given (death cert/medical cert/job proof)
A4 Negotiation (0-3) CRITICAL: urgency + consequences + part payment offered
A5 PTP Commitment (0-3) CRITICAL: specific amount + date + payment mode all confirmed
A6 Closing (0-2): summarised PTP + professional close
A7 Professionalism (0-3) CRITICAL: no threats, no abuse, calm and empathetic
A8 Call Handling (0-1): controlled conversation flow
A9 Troubleshooting (0-1): resolved payment technical issues

FLAGS: THREAT|ABUSE|FALSE_PROMISE|WRONG_DISCLOSURE|PTP_DETECTED|NO_PTP|NONE

AGENT TRANSCRIPT:
{transcript}

JSON (start immediately with {{, no other text):
{{"scores":{{"A1_opening":0,"A2_case_knowledge":0,"A3_probing":0,"A4_negotiation":0,"A5_commitment_ptp":0,"A6_closing":0,"A7_professionalism":0,"A8_call_handling":0,"A9_troubleshooting":0}},"total_score":0,"total_score_pct":0,"grade":"Poor","critical_fail":false,"ptp_detected":false,"ptp_amount":null,"ptp_date":null,"ptp_mode":null,"agent_sentiment":"neutral","sentiment_notes":"brief note","compliance_flags":["NONE"],"summary":"2-3 sentence summary","key_issues":["issue1"],"strengths":["strength1"],"coaching_tip":"one specific tip"}}"""


def _clean_json(raw: str) -> str:
    """
    Bulletproof JSON cleaner:
    1. Strip <think>...</think> blocks
    2. Strip markdown fences
    3. Extract { ... } 
    4. Fix trailing commas
    5. Fix control characters
    """
    # Remove <think> blocks — may span thousands of chars
    raw = re.sub(r"<think>[\s\S]*?</think>", "", raw, flags=re.IGNORECASE)

    # Remove markdown
    raw = re.sub(r"```json|```", "", raw)
    raw = raw.strip()

    # Find JSON boundaries
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return ""

    raw = raw[start:end + 1]

    # Fix trailing commas before } or ]
    raw = re.sub(r",(\s*[}\]])", r"\1", raw)

    # Remove control characters except newline/tab
    raw = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", raw)

    return raw.strip()


def score_transcript(agent_transcript: str) -> dict:
    key = os.getenv("SARVAM_API_KEY")
    if not key: raise EnvironmentError("SARVAM_API_KEY not set")

    prompt = SCORING_PROMPT.format(transcript=agent_transcript[:8000])

    def call_llm(messages: list, temp: float = 0.0) -> str:
        r = requests.post(
            "https://api.sarvam.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": "sarvam-m",
                "messages": messages,
                "temperature": temp,
                "max_tokens": 800,  # Shorter = less thinking
            },
            timeout=90,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Sarvam LLM {r.status_code}: {r.text}")
        return r.json()["choices"][0]["message"]["content"]

    # Attempt 1: Direct JSON prompt, temperature=0
    raw = call_llm([
        {"role": "system", "content": "You output ONLY raw JSON. No thinking tags. No explanation. Start with { immediately."},
        {"role": "user", "content": prompt}
    ], temp=0.0)

    print(f"[SCORE] Attempt 1 ({len(raw)} chars): {raw[:80]}")
    js = _clean_json(raw)

    # Attempt 2: Keep conversation — ask it to now output just the JSON
    if not js or not _is_valid_json(js):
        print("[SCORE] Attempt 2 — asking for JSON conversion...")
        raw2 = call_llm([
            {"role": "system", "content": "You output ONLY raw JSON starting with {. No thinking. No text before or after the JSON."},
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": raw},  # include its previous response
            {"role": "user", "content": "Now output ONLY the JSON object. Start with { right now:"}
        ], temp=0.0)
        print(f"[SCORE] Attempt 2 ({len(raw2)} chars): {raw2[:80]}")
        js = _clean_json(raw2)

    # Attempt 3: Completely fresh minimal prompt
    if not js or not _is_valid_json(js):
        print("[SCORE] Attempt 3 — minimal prompt...")
        mini = f"""Transcript: {agent_transcript[:3000]}

Fill in real scores and return ONLY this JSON:
{{"scores":{{"A1_opening":0,"A2_case_knowledge":0,"A3_probing":0,"A4_negotiation":0,"A5_commitment_ptp":0,"A6_closing":0,"A7_professionalism":0,"A8_call_handling":0,"A9_troubleshooting":0}},"total_score":0,"total_score_pct":0,"grade":"Poor","critical_fail":false,"ptp_detected":false,"ptp_amount":null,"ptp_date":null,"ptp_mode":null,"agent_sentiment":"neutral","sentiment_notes":"note","compliance_flags":["NONE"],"summary":"summary","key_issues":["issue"],"strengths":["strength"],"coaching_tip":"tip"}}"""

        raw3 = call_llm([
            {"role": "system", "content": "Return ONLY the filled JSON. Start with {{"},
            {"role": "user", "content": mini}
        ], temp=0.0)
        print(f"[SCORE] Attempt 3 ({len(raw3)} chars): {raw3[:80]}")
        js = _clean_json(raw3)

    if not js or not _is_valid_json(js):
        raise ValueError(f"Could not extract JSON after 3 attempts. Raw sample: {raw[:300]}")

    result = json.loads(js)

    # Auto-calculate grade
    total = result.get("total_score", 0)
    if total >= 18: result["grade"] = "Excellent"
    elif total >= 14: result["grade"] = "Good"
    elif total >= 8: result["grade"] = "Needs Improvement"
    else: result["grade"] = "Poor"

    # Check critical fail
    scores = result.get("scores", {})
    critical = ["A3_probing", "A4_negotiation", "A5_commitment_ptp", "A7_professionalism"]
    result["critical_fail"] = any(scores.get(k, 1) == 0 for k in critical)

    print(f"[SCORE] Done ✓ {total}/20 ({result['grade']})")
    return result


def _is_valid_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except Exception:
        return False


# ════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ════════════════════════════════════════════════════════

def process_call(call_id: str, audio_source: str, calls_db: dict, update_call_fn):
    tmp = tempfile.mkdtemp(prefix="care_dl_")
    try:
        # 1. Resolve source
        if not os.path.isfile(audio_source):
            update_call_fn(call_id, {"status": "fetching"})
            local = resolve_audio_source(audio_source, tmp)
        else:
            local = audio_source

        # 2. Transcribe
        update_call_fn(call_id, {"status": "transcribing"})
        print(f"[PIPELINE] {call_id} → transcribing...")
        agent_transcript, full_transcript = transcribe(local)

        if not agent_transcript.strip():
            update_call_fn(call_id, {"status": "failed", "error": "Empty transcript"}); return

        # 3. Score
        update_call_fn(call_id, {
            "transcript": full_transcript,
            "agent_transcript": agent_transcript,
            "status": "scoring"
        })
        print(f"[PIPELINE] {call_id} → scoring ({len(agent_transcript)} chars)...")
        s = score_transcript(agent_transcript)

        # 4. Save
        total = s.get("total_score", 0)
        pct = s.get("total_score_pct") or round((total / 20) * 100)
        flags = [f for f in s.get("compliance_flags", []) if f != "NONE"]

        update_call_fn(call_id, {
            "status": "processed",
            "score": total,
            "score_pct": pct,
            "grade": s.get("grade", "Poor"),
            "critical_fail": 1 if s.get("critical_fail") else 0,
            "scores_breakdown": s.get("scores", {}),
            "compliance_flags": flags,
            "ptp_detected": s.get("ptp_detected", False),
            "ptp_amount": s.get("ptp_amount"),
            "ptp_date": s.get("ptp_date"),
            "ptp_mode": s.get("ptp_mode"),
            "agent_sentiment": s.get("agent_sentiment", "neutral"),
            "sentiment_notes": s.get("sentiment_notes", ""),
            "summary": s.get("summary", ""),
            "key_issues": s.get("key_issues", []),
            "strengths": s.get("strengths", []),
            "coaching_tip": s.get("coaching_tip", ""),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        })
        ptp = f"PTP: {s.get('ptp_amount')} on {s.get('ptp_date')}" if s.get("ptp_detected") else "No PTP"
        cf = " ⚠ CRITICAL FAIL" if s.get("critical_fail") else ""
        print(f"[PIPELINE] {call_id} → DONE ✓ {total}/20 ({s.get('grade')}) | {ptp}{cf}")

    except json.JSONDecodeError as e:
        update_call_fn(call_id, {"status": "failed", "error": f"Score parse error: {e}"})
        print(f"[PIPELINE] {call_id} JSON error: {e}")
    except Exception as e:
        update_call_fn(call_id, {"status": "failed", "error": str(e)})
        print(f"[PIPELINE] {call_id} ERROR: {e}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def process_call_async(call_id, audio_source, calls_db, update_call_fn):
    t = threading.Thread(
        target=process_call,
        args=(call_id, audio_source, calls_db, update_call_fn),
        daemon=True
    )
    t.start()
    return t


# ════════════════════════════════════════════════════════
#  CSV EXPORT
# ════════════════════════════════════════════════════════

def export_calls_to_csv_bytes(calls: list) -> bytes:
    import io, csv
    output = io.StringIO()
    if not calls: return b""
    headers = [
        "id","filename","agent_id","loan_id","status","score","score_pct","grade",
        "critical_fail","ptp_detected","ptp_amount","ptp_date","ptp_mode",
        "compliance_flags","agent_sentiment",
        "A1_opening","A2_case_knowledge","A3_probing","A4_negotiation",
        "A5_commitment_ptp","A6_closing","A7_professionalism",
        "A8_call_handling","A9_troubleshooting",
        "summary","key_issues","strengths","coaching_tip",
        "uploaded_at","processed_at"
    ]
    writer = csv.DictWriter(output, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for c in calls:
        bd = c.get("scores_breakdown") or {}
        writer.writerow({
            "id": c.get("id",""), "filename": c.get("filename",""),
            "agent_id": c.get("agent_id",""), "loan_id": c.get("loan_id",""),
            "status": c.get("status",""), "score": c.get("score",""),
            "score_pct": c.get("score_pct",""), "grade": c.get("grade",""),
            "critical_fail": c.get("critical_fail",""),
            "ptp_detected": c.get("ptp_detected",""),
            "ptp_amount": c.get("ptp_amount",""), "ptp_date": c.get("ptp_date",""),
            "ptp_mode": c.get("ptp_mode",""),
            "compliance_flags": "; ".join(c.get("compliance_flags") or []),
            "agent_sentiment": c.get("agent_sentiment",""),
            "A1_opening": bd.get("A1_opening",""),
            "A2_case_knowledge": bd.get("A2_case_knowledge",""),
            "A3_probing": bd.get("A3_probing",""),
            "A4_negotiation": bd.get("A4_negotiation",""),
            "A5_commitment_ptp": bd.get("A5_commitment_ptp",""),
            "A6_closing": bd.get("A6_closing",""),
            "A7_professionalism": bd.get("A7_professionalism",""),
            "A8_call_handling": bd.get("A8_call_handling",""),
            "A9_troubleshooting": bd.get("A9_troubleshooting",""),
            "summary": c.get("summary",""),
            "key_issues": "; ".join(c.get("key_issues") or []),
            "strengths": "; ".join(c.get("strengths") or []),
            "coaching_tip": c.get("coaching_tip",""),
            "uploaded_at": c.get("uploaded_at",""),
            "processed_at": c.get("processed_at",""),
        })
    return output.getvalue().encode("utf-8")