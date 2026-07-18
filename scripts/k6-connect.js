import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const API_KEY = __ENV.API_KEY || "dev-key";
const MODEL = __ENV.MODEL || "mlx-community/SmolLM2-135M-Instruct";
const generatedTokens = new Counter("generated_tokens");
const generateTokens = new Counter("generate_tokens");
const streamTokens = new Counter("stream_tokens");
const generateRequests = new Counter("generate_requests");
const streamRequests = new Counter("stream_requests");
const generateLatency = new Trend("generate_latency", true);
const streamLatency = new Trend("stream_latency", true);

export const options = {
  scenarios: {
    generate: {
      executor: "constant-vus",
      exec: "generate",
      vus: Number(__ENV.GENERATE_VUS || 2),
      duration: __ENV.DURATION || "20s",
      gracefulStop: "30s",
    },
    stream_generate: {
      executor: "constant-vus",
      exec: "streamGenerate",
      vus: Number(__ENV.STREAM_VUS || 1),
      duration: __ENV.DURATION || "20s",
      gracefulStop: "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    checks: ["rate>0.99"],
  },
};

export function generate() {
  const body = generateRequest("Explain token budgeting in one sentence.", 32);
  const res = http.post(`${BASE_URL}/inference.v1.InferenceService/Generate`, body, params());
  const tokens = readVarintField(new Uint8Array(res.body || new ArrayBuffer(0)), 5);
  generatedTokens.add(tokens);
  generateTokens.add(tokens);
  generateRequests.add(1);
  generateLatency.add(res.timings.duration);
  check(res, {
    "Generate status 200": (r) => r.status === 200,
    "Generate protobuf body": (r) => r.body && r.body.byteLength > 8,
  });
  sleep(0.05);
}

export function streamGenerate() {
  const body = generateRequest("Count from one to five.", 24);
  const res = http.post(`${BASE_URL}/inference.v1.InferenceService/StreamGenerate`, body, params());
  const tokens = readStreamGeneratedTokens(new Uint8Array(res.body || new ArrayBuffer(0)));
  generatedTokens.add(tokens);
  streamTokens.add(tokens);
  streamRequests.add(1);
  streamLatency.add(res.timings.duration);
  check(res, {
    "StreamGenerate status 200": (r) => r.status === 200,
    "StreamGenerate connect envelopes": (r) => hasConnectEnvelope(r.body),
  });
  sleep(0.05);
}

function params() {
  return {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      "Content-Type": "application/proto",
    },
    responseType: "binary",
    timeout: __ENV.REQUEST_TIMEOUT || "30s",
  };
}

function generateRequest(content, maxTokens) {
  const message = concat([
    fieldString(1, "user"),
    fieldString(2, content),
  ]);
  return concat([
    fieldString(1, MODEL),
    fieldBytes(2, message),
    fieldVarint(4, maxTokens),
  ]).buffer;
}

function hasConnectEnvelope(body) {
  if (!body || body.byteLength < 5) return false;
  const bytes = new Uint8Array(body);
  const len = (bytes[1] << 24) | (bytes[2] << 16) | (bytes[3] << 8) | bytes[4];
  return len > 0 && bytes.byteLength >= 5 + len;
}

function readStreamGeneratedTokens(bytes) {
  let offset = 0;
  let latest = 0;
  while (offset + 5 <= bytes.length) {
    const len = (bytes[offset + 1] << 24) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 8) | bytes[offset + 4];
    offset += 5;
    if (len <= 0 || offset + len > bytes.length) {
      break;
    }
    latest = Math.max(latest, readVarintField(bytes.slice(offset, offset + len), 4));
    offset += len;
  }
  return latest;
}

function readVarintField(bytes, field) {
  let offset = 0;
  while (offset < bytes.length) {
    const key = readVarint(bytes, offset);
    if (!key) {
      return 0;
    }
    offset = key.next;
    const fieldNumber = key.value >>> 3;
    const wireType = key.value & 7;
    if (fieldNumber === field && wireType === 0) {
      const value = readVarint(bytes, offset);
      return value ? value.value : 0;
    }
    offset = skipField(bytes, offset, wireType);
  }
  return 0;
}

function skipField(bytes, offset, wireType) {
  if (wireType === 0) {
    const value = readVarint(bytes, offset);
    return value ? value.next : bytes.length;
  }
  if (wireType === 1) {
    return Math.min(offset + 8, bytes.length);
  }
  if (wireType === 2) {
    const len = readVarint(bytes, offset);
    return len ? Math.min(len.next + len.value, bytes.length) : bytes.length;
  }
  if (wireType === 5) {
    return Math.min(offset + 4, bytes.length);
  }
  return bytes.length;
}

function readVarint(bytes, offset) {
  let value = 0;
  let shift = 0;
  for (let i = offset; i < bytes.length && shift < 64; i++) {
    const byte = bytes[i];
    value += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) {
      return { value, next: i + 1 };
    }
    shift += 7;
  }
  return null;
}

function fieldString(field, value) {
  return fieldBytes(field, utf8(value));
}

function fieldBytes(field, value) {
  return concat([varint((field << 3) | 2), varint(value.length), value]);
}

function fieldVarint(field, value) {
  return concat([varint(field << 3), varint(value)]);
}

function varint(value) {
  const out = [];
  let current = value >>> 0;
  while (current >= 0x80) {
    out.push((current & 0x7f) | 0x80);
    current >>>= 7;
  }
  out.push(current);
  return new Uint8Array(out);
}

function utf8(value) {
  const encoded = unescape(encodeURIComponent(value));
  const out = new Uint8Array(encoded.length);
  for (let i = 0; i < encoded.length; i++) {
    out[i] = encoded.charCodeAt(i);
  }
  return out;
}

function concat(parts) {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}
