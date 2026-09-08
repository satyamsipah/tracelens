import type { Metadata } from "next";
import "./globals.css";
import { Nav } from "@/components/nav";

export const metadata: Metadata = {
  title: "TraceLens",
  description: "Distributed tracing, logs, and metrics -- hand-built query engine and UI.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <head>
        {/* Applied before paint so the dark-mode preference in Nav's client
            state doesn't cause a flash of the light theme on first load. */}
        <script
          dangerouslySetInnerHTML={{
            __html: `try{document.documentElement.classList.toggle('dark', localStorage.getItem('theme') !== 'light')}catch(e){}`,
          }}
        />
      </head>
      <body className="flex min-h-screen">
        <Nav />
        <main className="flex-1 overflow-x-hidden p-6">{children}</main>
      </body>
    </html>
  );
}
