import Link from "next/link";

import { Button } from "@/components/ui/button";

export default function NotFound() {
  return (
    <div className="flex h-screen flex-col items-center justify-center gap-4 p-4 text-center">
      <h1 className="text-2xl font-bold">Not found</h1>
      <p className="text-muted-foreground">
        This operations page does not exist.
      </p>
      <Button asChild>
        <Link href="/">Back to operations</Link>
      </Button>
    </div>
  );
}
