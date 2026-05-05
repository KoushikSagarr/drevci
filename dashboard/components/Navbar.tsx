import Link from 'next/link';

export default function Navbar() {
  return (
    <nav className="fixed top-0 left-0 right-0 z-50 flex h-14 items-center justify-between border-b border-border bg-background px-6">
      <Link href="/runs" className="flex items-center gap-2 font-semibold text-white transition-opacity hover:opacity-80">
        <span className="text-xl">⚡</span>
        <span>Drev CI</span>
      </Link>
      <div className="text-sm font-medium text-text-secondary">
        v0.1.0
      </div>
    </nav>
  );
}
