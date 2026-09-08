export default function Home() {
  return (
    <main
      className="flex min-h-screen items-center justify-center p-6"
      id="main-content"
    >
      <section className="max-w-xl space-y-3 text-center">
        <h1 className="text-2xl font-bold">Saqi Operations</h1>
        <p className="text-muted-foreground">
          Collection and translation are managed by the restart-safe local
          service. This Worker exposes only the authenticated publication APIs.
        </p>
      </section>
    </main>
  );
}
