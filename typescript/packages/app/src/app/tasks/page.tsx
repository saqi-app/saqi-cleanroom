export default function TasksRetiredPage() {
  return (
    <main
      className="flex min-h-screen items-center justify-center p-6"
      id="main-content"
    >
      <section className="max-w-xl space-y-3 text-center">
        <h1 className="text-2xl font-bold">Task console retired</h1>
        <p className="text-muted-foreground">
          No cloud tasks remain. Collection and translation now run through the
          restart-safe local service.
        </p>
      </section>
    </main>
  );
}
