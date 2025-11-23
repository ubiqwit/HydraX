// app/layout.tsx
import "./globals.css";

export const metadata = {
  title: "HydraX – AI for Sustainable Cities",
  description:
    "HydraX is an AI-powered sustainability tool that helps London measure and visualize its rainwater harvesting potential.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="hx-body">{children}</body>
    </html>
  );
}
