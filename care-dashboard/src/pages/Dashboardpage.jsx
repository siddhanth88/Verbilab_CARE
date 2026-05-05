import DashboardPage from "./pages/dashboardPage";
import { useState, useEffect } from "react";
import { getDashboard } from "../services/api";
import { useNavigate } from "react-router-dom";


const DEFAULT_STATS = {
  calls_today: 0,
  processed: 0,
  processing_pct: 0,
  compliance_flags: 0,
  live_calls: 0,
  avg_score: 0,
  ingestion: { direct: 0, google_drive: 0, dialer_webhook: 0 },
};

export default function DashboardPage() {
  const [stats, setStats] = useState(DEFAULT_STATS);
  const [recentCalls, setRecentCalls] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    const fetchDashboard = async () => {
      try {
        setLoading(true);
        const data = await getDashboard();
        // Merge whatever the backend returns, fall back to defaults
        setStats((prev) => ({ ...prev, ...data }));
        setRecentCalls(data.recent_calls ?? data.calls ?? []);
      } catch (err) {
        console.error("Dashboard fetch failed:", err);
        setError("Could not load dashboard data. Is Flask running?");
      } finally {
        setLoading(false);
      }
    };
    fetchDashboard();
    // Auto-refresh every 30s
    const interval = setInterval(fetchDashboard, 30_000);
    return () => clearInterval(interval);
  }, []);

  const scoreColor = (score) => {
    if (score == null) return "text-gray-400";
    if (score >= 80) return "text-green-400";
    if (score >= 60) return "text-yellow-400";
    return "text-red-400";
  };

  const statusBadge = (status) => {
    const s = (status ?? "").toLowerCase();
    if (s === "processed" || s === "completed") return "bg-green-900/50 text-green-400";
    if (s === "processing") return "bg-yellow-900/50 text-yellow-400";
    if (s === "failed") return "bg-red-900/50 text-red-400";
    return "bg-gray-700 text-gray-400";
  };

  return (
    <div className="p-6 text-white">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Dashboard</h1>
        {loading && <span className="text-xs text-gray-400 animate-pulse">Refreshing…</span>}
      </div>

      {error && (
        <div className="mb-4 bg-red-900/40 border border-red-700 text-red-300 rounded-lg px-4 py-3 text-sm">
          ⚠ {error}
        </div>
      )}

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KpiCard label="Calls Today" value={stats.calls_today} />
        <KpiCard label="Processed" value={`${stats.processing_pct ?? 0}%`} sub={`${stats.processed} calls`} />
        <KpiCard label="Compliance Flags" value={stats.compliance_flags} accent="text-red-400" />
        <KpiCard label="Live Calls" value={stats.live_calls} accent="text-green-400" />
      </div>

      {/* Ingestion Breakdown */}
      {stats.ingestion && (
        <div className="bg-gray-800 rounded-xl p-5 mb-6">
          <h2 className="text-sm font-semibold text-gray-400 uppercase mb-4">Today's Ingestion</h2>
          <div className="space-y-3">
            {[
              { label: "Via Direct Upload", value: stats.ingestion.direct },
              { label: "Via Google Drive", value: stats.ingestion.google_drive },
              { label: "Via Dialer Webhook", value: stats.ingestion.dialer_webhook },
            ].map(({ label, value }) => (
              <div key={label} className="flex items-center justify-between">
                <span className="text-sm text-gray-300">{label}</span>
                <span className="font-semibold">{value}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Calls */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-400 uppercase mb-4">Recent Calls</h2>
        {recentCalls.length === 0 ? (
          <p className="text-gray-500 text-sm">No calls processed yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 text-xs uppercase border-b border-gray-700">
                  <th className="text-left pb-2 pr-4">File</th>
                  <th className="text-left pb-2 pr-4">Agent</th>
                  <th className="text-left pb-2 pr-4">Score</th>
                  <th className="text-left pb-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {recentCalls.map((call) => (
                  <tr
                    key={call.id ?? call.call_id ?? call.filename}
                    onClick={() => navigate(`/calls/${call.id}`)}
                    className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors"
                  >
                    <td className="py-2 pr-4 font-mono text-xs text-gray-300 max-w-[200px] truncate">
                      {call.filename ?? call.file_name ?? call.id}
                    </td>
                    <td className="py-2 pr-4 text-gray-300">{call.agent_id ?? "—"}</td>
                    <td className={`py-2 pr-4 font-bold ${scoreColor(call.score ?? call.compliance_score)}`}>
                      {call.score ?? call.compliance_score ?? "—"}
                    </td>
                    <td className="py-2">
                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusBadge(call.status)}`}>
                        {call.status ?? "Uploaded"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

function KpiCard({ label, value, sub, accent = "text-white" }) {
  return (
    <div className="bg-gray-800 rounded-xl p-4">
      <p className="text-xs text-gray-400 uppercase mb-1">{label}</p>
      <p className={`text-3xl font-bold ${accent}`}>{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}
