# Ace Cadence Bridge Deployment

This branch runs the existing Render bridge as a separate AWS service/container for the Ace Cadence app.

## Why WSS matters

Render currently gives this app a public secure WebSocket URL automatically, like:

```text
wss://cadence-bridge.onrender.com/media-stream
```

Twilio media streams should use a public WebSocket URL. In production that should be TLS-backed `wss://...`, usually through nginx with an SSL certificate. For local/POC on the EC2 public IP, the container can still run on `ws://host:3001`, but Twilio/browser compatibility is better once nginx exposes it as `wss://your-domain/media-stream`.

## Same EC2 Deployment

Clone this repo beside `Ace-Cadence` on the EC2 instance, then run it as its own container:

```bash
cd /home/ec2-user/cadence/cadence-bridge
cp .env.aws.example .env
# fill ELEVENLABS_API_KEY and ELEVENLABS_AGENT_ID
sudo docker network ls | grep ace-cadence
sudo docker compose -f docker-compose.aws.yml up -d --build
sudo docker compose -f docker-compose.aws.yml exec cadence-bridge wget -qO- http://127.0.0.1:3001/health
```

Use these env vars:

```text
ELEVENLABS_API_KEY=...
ELEVENLABS_AGENT_ID=...
CADENCE_API_BASE_URL=http://ace-cadence-nginx-1
```

`CADENCE_API_BASE_URL` replaces the old `CONVEX_SITE_URL` name. `CONVEX_SITE_URL` still works as a fallback, so Render is not broken.

## Ace-Cadence Env

Set Ace-Cadence to point Twilio streams at the public nginx URL that proxies to this bridge:

```text
BRIDGE_SERVER_URL=wss://cadence-pro.acelive.ai
```

Do not use `ws://cadence-bridge:3001` for `BRIDGE_SERVER_URL`; that hostname is only reachable inside Docker, while Twilio connects from the public internet. Until SSL/domain routing works on AWS, keep using the existing Render bridge URL for live Twilio tests.

## Optional nginx routes

If nginx is routing the bridge on the same domain, proxy these to `http://cadence-bridge:3001` from the Ace-Cadence nginx container:

```nginx
location /media-stream {
    proxy_pass http://cadence-bridge:3001/media-stream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}

location /monitor {
    proxy_pass http://cadence-bridge:3001/monitor;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}

location /listen/ {
    proxy_pass http://cadence-bridge:3001;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}

location /start-monitor {
    proxy_pass http://cadence-bridge:3001/start-monitor;
    proxy_set_header Host $host;
}
```


