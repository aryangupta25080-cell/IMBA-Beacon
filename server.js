const http = require("node:http");
const fs = require("node:fs/promises");
const path = require("node:path");
const { createReadStream, existsSync } = require("node:fs");
const { URL } = require("node:url");

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 3000);
const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, "data");
const WAITLIST_FILE = path.join(DATA_DIR, "waitlist.json");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".jpeg": "image/jpeg",
  ".jpg": "image/jpeg",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".webp": "image/webp"
};

async function ensureDataStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });

  if (!existsSync(WAITLIST_FILE)) {
    await fs.writeFile(WAITLIST_FILE, "[]\n", "utf8");
  }
}

async function readWaitlist() {
  await ensureDataStore();
  const file = await fs.readFile(WAITLIST_FILE, "utf8");
  return JSON.parse(file);
}

async function writeWaitlist(entries) {
  await fs.writeFile(WAITLIST_FILE, JSON.stringify(entries, null, 2) + "\n", "utf8");
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(JSON.stringify(payload));
}

function sendNotFound(response) {
  sendJson(response, 404, { message: "Resource not found." });
}

function serveFile(response, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || "application/octet-stream";

  response.writeHead(200, {
    "Content-Type": contentType
  });

  createReadStream(filePath).pipe(response);
}

function normalizePathname(pathname) {
  if (pathname === "/") return "/index.html";
  return pathname;
}

async function parseRequestBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";

    request.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1_000_000) {
        reject(new Error("Payload too large."));
        request.destroy();
      }
    });

    request.on("end", () => {
      if (!body) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(new Error("Invalid JSON payload."));
      }
    });

    request.on("error", reject);
  });
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

async function handleWaitlistSubmission(request, response) {
  try {
    const body = await parseRequestBody(request);
    const email = String(body.email || "").trim().toLowerCase();

    if (!isValidEmail(email)) {
      sendJson(response, 400, { message: "Please enter a valid email address." });
      return;
    }

    const waitlist = await readWaitlist();
    const alreadyExists = waitlist.some((entry) => entry.email === email);

    if (alreadyExists) {
      sendJson(response, 409, { message: "This email is already on the waitlist." });
      return;
    }

    waitlist.push({
      email,
      submittedAt: new Date().toISOString()
    });

    await writeWaitlist(waitlist);

    sendJson(response, 201, {
      message: "Thanks! You're on the IMBA Beacon waitlist now."
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to process request." });
  }
}

async function handleWaitlistList(response) {
  try {
    const waitlist = await readWaitlist();
    sendJson(response, 200, {
      count: waitlist.length,
      entries: waitlist
    });
  } catch (error) {
    sendJson(response, 500, { message: "Unable to read waitlist data." });
  }
}

async function requestHandler(request, response) {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = normalizePathname(url.pathname);

  if (request.method === "POST" && pathname === "/api/waitlist") {
    await handleWaitlistSubmission(request, response);
    return;
  }

  if (request.method === "GET" && pathname === "/api/waitlist") {
    await handleWaitlistList(response);
    return;
  }

  if (request.method !== "GET" && request.method !== "HEAD") {
    sendJson(response, 405, { message: "Method not allowed." });
    return;
  }

  const safePath = path.normalize(path.join(ROOT_DIR, pathname));

  if (!safePath.startsWith(ROOT_DIR)) {
    sendNotFound(response);
    return;
  }

  if (!existsSync(safePath)) {
    sendNotFound(response);
    return;
  }

  if (request.method === "HEAD") {
    response.writeHead(200);
    response.end();
    return;
  }

  serveFile(response, safePath);
}

async function startServer() {
  await ensureDataStore();

  const server = http.createServer((request, response) => {
    requestHandler(request, response).catch((error) => {
      sendJson(response, 500, { message: error.message || "Internal server error." });
    });
  });

  server.listen(PORT, HOST, () => {
    console.log(`IMBA Beacon server running at http://${HOST}:${PORT}`);
  });
}

startServer().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});
