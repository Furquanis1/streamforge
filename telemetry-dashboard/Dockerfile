# syntax=docker/dockerfile:1
FROM node:20-alpine AS build
WORKDIR /app

ARG VITE_EDGE_API_URL=http://localhost:8083
ARG VITE_FOG_API_URL=http://localhost:8082
ARG VITE_CLOUD_API_URL=http://localhost:8081

ENV VITE_EDGE_API_URL=${VITE_EDGE_API_URL}
ENV VITE_FOG_API_URL=${VITE_FOG_API_URL}
ENV VITE_CLOUD_API_URL=${VITE_CLOUD_API_URL}

COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine AS runtime
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 5173
