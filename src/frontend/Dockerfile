# ── Stage 1: Install dependencies ────────────────────────────────────────────
FROM oven/bun:1 AS deps

WORKDIR /app
COPY src/frontend/package.json src/frontend/bun.lock ./
RUN bun install --frozen-lockfile

# ── Stage 2: Build Next.js ────────────────────────────────────────────────────
FROM oven/bun:1 AS builder

WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY src/frontend/ ./

# next.config.js has output: "standalone"
RUN bun run build

# ── Stage 3: Minimal runtime ──────────────────────────────────────────────────
# Use Node (not Bun) for the Next.js production server — it's the tested path
FROM node:24-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000

RUN addgroup --system --gid 1001 nodejs \
 && adduser  --system --uid 1001 nextjs

COPY --from=builder /app/public                          ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static     ./.next/static

USER nextjs
EXPOSE 3000

CMD ["node", "server.js"]
