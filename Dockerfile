FROM node:22-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci

COPY tsconfig.json drizzle.config.ts ./
COPY src ./src
COPY drizzle ./drizzle

RUN npm run build

CMD ["node", "--max-old-space-size=210", "--max-semi-space-size=16", "--initial-old-space-size=64", "dist/server.cjs"]
