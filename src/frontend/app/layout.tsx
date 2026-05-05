import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'WhiteChristmas',
  description: 'Real-time Crime Analytics & Intelligent Alert System',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-gray-900 text-gray-100">{children}</body>
    </html>
  );
}
