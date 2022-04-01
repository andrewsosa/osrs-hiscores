FROM node:17-alpine
WORKDIR /app

ADD package.json package-lock.json ./
RUN npm ci

ADD index.js ./
CMD ["node", "index.js"]
