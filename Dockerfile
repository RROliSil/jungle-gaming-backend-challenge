FROM oven/bun:1.1.42-alpine AS base
WORKDIR /app

COPY package.json ./
RUN bun install

COPY tsconfig.json tsconfig.build.json mikro-orm.config.ts ./
COPY src ./src
COPY scripts ./scripts

EXPOSE 3001
CMD ["bun", "run", "src/main.ts"]
