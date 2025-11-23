export default function Home() {
  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
        {/* Header */}
        <header className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold">HydraX Dashboard</h1>
            <p className="text-sm text-slate-400">
              Rainwater harvesting potential • London, UK
            </p>
          </div>
        </header>

        {/* Dashboard sections */}
        <div className="grid gap-6 md:grid-cols-[1.8fr,1fr]">
          {/* Map placeholder */}
          <section className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
            <h2 className="mb-2 text-sm font-medium text-slate-300">
              City Map (coming soon)
            </h2>
            <div className="flex h-72 items-center justify-center rounded-xl border border-dashed border-slate-700 text-slate-500 text-sm">
              Mapbox / rooftop layer will go here
            </div>
          </section>

          {/* Stats */}
          <aside className="space-y-4">
            <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                Estimated annual yield
              </h3>
              <p className="mt-2 text-2xl font-semibold">— m³ / year</p>
            </div>
            <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                Stormwater runoff reduction
              </h3>
              <p className="mt-2 text-2xl font-semibold">— %</p>
            </div>
          </aside>
        </div>
      </div>
    </main>
  );
} 
