// Use env OR fallback to Railway
const API_ROOT =
  import.meta.env.VITE_API_URL?.trim() ||
  "https://verbilabcare-production.up.railway.app";

console.log("🌐 API ROOT =", API_ROOT);

const BASE = API_ROOT;
const AUTH = `${API_ROOT}/api/auth`;

function getToken() {
  return localStorage.getItem("care_token") || "";
}

function authHeaders() {
  return {
    Authorization: `Bearer ${getToken()}`,
    "Content-Type": "application/json",
  };
}

// Safe fetch wrapper (THIS IS KEY 🔥)
async function safeFetch(url, options = {}) {
  try {
    const res = await fetch(url, options);

    let data;
    try {
      data = await res.json();
    } catch {
      data = null;
    }

    if (!res.ok) {
      throw new Error(data?.error || `HTTP ${res.status}`);
    }

    return data;
  } catch (err) {
    console.error("❌ API ERROR:", err);
    throw new Error("Cannot reach backend. Check API / CORS / URL.");
  }
}

// ─────────────────────────────────────────────────────────────
// AUTH
// ─────────────────────────────────────────────────────────────

export async function login(email, password) {
  return safeFetch(`${AUTH}/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
}

export async function getMe() {
  return safeFetch(`${AUTH}/me`, {
    headers: authHeaders(),
  });
}

export function logout() {
  localStorage.removeItem("care_token");
  localStorage.removeItem("care_user");
}

// ─────────────────────────────────────────────────────────────
// CALLS
// ─────────────────────────────────────────────────────────────

export async function getCalls(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return safeFetch(`${BASE}/calls${qs ? "?" + qs : ""}`, {
    headers: authHeaders(),
  });
}

export async function getCall(callId) {
  return safeFetch(`${BASE}/calls/${callId}`, {
    headers: authHeaders(),
  });
}

export async function getDashboard(params = {}) {
  const qs = new URLSearchParams(params).toString();
  return safeFetch(`${BASE}/reports/dashboard${qs ? "?" + qs : ""}`, {
    headers: authHeaders(),
  });
}

export async function getAgentKPIs() {
  return safeFetch(`${BASE}/agents/kpis`, {
    headers: authHeaders(),
  });
}

// ─────────────────────────────────────────────────────────────
// UPLOAD (XHR for progress)
// ─────────────────────────────────────────────────────────────

export function uploadCall(file, metadata = {}, onProgress) {
  return new Promise((resolve, reject) => {
    const formData = new FormData();
    formData.append("file", file);

    Object.entries(metadata).forEach(([k, v]) => {
      if (v) formData.append(k, v);
    });

    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${BASE}/calls/ingest`);

    xhr.setRequestHeader("Authorization", `Bearer ${getToken()}`);

    if (onProgress) {
      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
          onProgress(Math.round((e.loaded / e.total) * 100));
        }
      };
    }

    xhr.onload = () => {
      try {
        const data = JSON.parse(xhr.responseText);
        if (xhr.status === 200 || xhr.status === 201) {
          resolve(data);
        } else {
          reject(new Error(data.error || `Upload failed (${xhr.status})`));
        }
      } catch {
        reject(new Error("Invalid response from server"));
      }
    };

    xhr.onerror = () => {
      console.error("❌ XHR NETWORK ERROR");
      reject(new Error("Network error / CORS issue"));
    };

    xhr.send(formData);
  });
}

// ─────────────────────────────────────────────────────────────
// EXPORT
// ─────────────────────────────────────────────────────────────

export function downloadCSVExport(params = {}) {
  const qs = new URLSearchParams(params).toString();
  const url = `${BASE}/reports/export${qs ? "?" + qs : ""}`;

  const a = document.createElement("a");
  a.href = url;
  a.download = `CARE_Export_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
}

export async function syncGDrive(folderIdOrUrl = null) {
  return safeFetch(`${BASE}/connectors/gdrive/sync`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(folderIdOrUrl ? { folder_id: folderIdOrUrl } : {}),
  });
}

export async function saveGDriveConfig(folderUrl, autoSync = false) {
  return safeFetch(`${BASE}/connectors/gdrive/config`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({
      folder_url: folderUrl,
      auto_sync: autoSync,
    }),
  });
}

export async function ingestFromS3(s3Uri, metadata = {}) {
  return safeFetch(`${BASE}/calls/ingest-s3`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({ s3_uri: s3Uri, ...metadata }),
  });
}

export async function ingestFromUrl(url, metadata = {}) {
  return safeFetch(`${BASE}/calls/ingest-url`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({ url, ...metadata }),
  });
}
