
"use client";
import { useState } from "react";

export default function ImpactPage() {
  const [address, setAddress] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<any>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    setResult(null);
    try {
      // Send to backend /api/geocode endpoint
      const response = await fetch("/api/geocode", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address }),
      });
      if (!response.ok) throw new Error("Failed to get prediction");
      const data = await response.json();
      setResult(data);
    } catch (err: any) {
      setError(err.message || "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <main style={{ padding: "80px", maxWidth: 600, margin: "0 auto" }}>
      <h1 style={{ fontSize: "2.5rem", fontWeight: 700, marginBottom: 16 }}>HydraX Impact Dashboard</h1>
      <p style={{ marginBottom: 32 }}>
        Here we’ll visualize charts, statistics, and district-level insights showing HydraX’s real-world impact on water savings and stormwater reduction.
      </p>
      <form onSubmit={handleSubmit} style={{ display: "flex", flexDirection: "column", gap: 16, marginBottom: 32 }}>
        <label htmlFor="address" style={{ fontWeight: 500 }}>Enter a London address:</label>
        <input
          id="address"
          type="text"
          value={address}
          onChange={e => setAddress(e.target.value)}
          placeholder="e.g. 10 Downing Street, London"
          style={{ padding: "12px", fontSize: "1rem", borderRadius: 8, border: "1px solid #ccc" }}
          required
        />
        <button
          type="submit"
          style={{ padding: "12px", fontSize: "1rem", borderRadius: 8, background: "#0070f3", color: "white", border: "none", fontWeight: 600, cursor: "pointer" }}
          disabled={loading}
        >
          {loading ? "Predicting..." : "Get Impact Prediction"}
        </button>
      </form>
      {error && <div style={{ color: "red", marginBottom: 16 }}>{error}</div>}
      {result && (
        <>
          {result.building_area && (
            <section style={{ background: "#222", color: "#fff", padding: 20, borderRadius: 12, marginBottom: 16 }}>
              <h2 style={{ fontSize: "1.2rem", fontWeight: 600, marginBottom: 8 }}>Rooftop Coverage</h2>
              <div style={{ fontSize: "1.1rem" }}>{Number(result.building_area).toFixed(2)} m²</div>
            </section>
          )}
          {result.predicted_rainfall && (
            <section style={{ background: "#222", color: "#fff", padding: 24, borderRadius: 12, marginBottom: 24 }}>
              <h2 style={{ fontSize: "1.5rem", fontWeight: 600, marginBottom: 12 }}>Yearly Rainfall & Captured Water</h2>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#333" }}>
                    <th style={{ padding: "8px", textAlign: "left", color: "#fff" }}>Year</th>
                    <th style={{ padding: "8px", textAlign: "left", color: "#fff" }}>Predicted Rainfall (mm)</th>
                    <th style={{ padding: "8px", textAlign: "left", color: "#fff" }}>Captured Water (L)</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(result.predicted_rainfall).map(([year, data]: any) => (
                    <tr key={year} style={{ background: "#222", borderBottom: "1px solid #444" }}>
                      <td style={{ padding: "8px", color: "#fff" }}>{year}</td>
                      <td style={{ padding: "8px", color: "#fff" }}>{Number(data.predicted_rainfall_mm).toFixed(2)}</td>
                      <td style={{ padding: "8px", color: "#fff" }}>{Number(data.predicted_collection_liters).toFixed(2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </>
      )}
    </main>
  );
}
