import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'WhiteChristmas — Real-Time Crime Analytics',
  description:
    'Real-time Crime Analytics & Intelligent Alert System powered by Rust, Kafka, Spark, Go, and Next.js',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
