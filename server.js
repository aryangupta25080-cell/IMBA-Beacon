const http = require("node:http");
const fs = require("node:fs/promises");
const { createReadStream, existsSync } = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { URL } = require("node:url");
const { OAuth2Client } = require("google-auth-library");

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 3000);
const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, "data");
const WAITLIST_FILE = path.join(DATA_DIR, "waitlist.json");
const PAYMENTS_FILE = path.join(DATA_DIR, "payments.json");
const SESSION_COOKIE = "imba_session";
const ENV_FILE = path.join(ROOT_DIR, ".env");

if (existsSync(ENV_FILE)) {
  const envLines = require("node:fs").readFileSync(ENV_FILE, "utf8").split("\n");
  envLines.forEach((line) => {
    const trimmedLine = line.trim();
    if (!trimmedLine || trimmedLine.startsWith("#")) return;

    const separatorIndex = trimmedLine.indexOf("=");
    if (separatorIndex === -1) return;

    const key = trimmedLine.slice(0, separatorIndex).trim();
    const value = trimmedLine.slice(separatorIndex + 1).trim();

    if (key && !process.env[key]) {
      process.env[key] = value;
    }
  });
}

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const PHONEPE_CLIENT_ID = process.env.PHONEPE_CLIENT_ID || "";
const PHONEPE_CLIENT_SECRET = process.env.PHONEPE_CLIENT_SECRET || "";
const PHONEPE_CLIENT_VERSION = process.env.PHONEPE_CLIENT_VERSION || "";
const PHONEPE_ENV = process.env.PHONEPE_ENV || "sandbox";
const PAYMENTS_ENABLED = process.env.PAYMENTS_ENABLED === "true";
const ADMIN_ACCESS_KEY = process.env.ADMIN_ACCESS_KEY || "";
const APP_BASE_URL = process.env.APP_BASE_URL || `http://127.0.0.1:${PORT}`;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "";

const googleClient = GOOGLE_CLIENT_ID ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;
const sessionStore = new Map();

const PLAN_CATALOG = {
  basic: {
    id: "basic",
    label: "Basic",
    amount: 150000,
    currency: "INR",
    description: "IMBA Beacon Basic preparation plan"
  },
  pro: {
    id: "pro",
    label: "Pro",
    amount: 200000,
    currency: "INR",
    description: "IMBA Beacon Pro preparation plan"
  }
};

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

  if (!existsSync(PAYMENTS_FILE)) {
    await fs.writeFile(PAYMENTS_FILE, "[]\n", "utf8");
  }
}

async function readJsonFile(filePath) {
  await ensureDataStore();
  const content = await fs.readFile(filePath, "utf8");
  return JSON.parse(content);
}

async function writeJsonFile(filePath, data) {
  await fs.writeFile(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}

function sendJson(response, statusCode, payload, extraHeaders = {}) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...extraHeaders
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

function getAllowedOrigin(request) {
  const requestOrigin = request.headers.origin || "";
  if (!requestOrigin) return "";
  if (FRONTEND_ORIGIN && requestOrigin === FRONTEND_ORIGIN) return requestOrigin;
  if (!FRONTEND_ORIGIN && (requestOrigin === "http://127.0.0.1:3000" || requestOrigin === "http://localhost:3000")) {
    return requestOrigin;
  }
  return "";
}

function getCorsHeaders(request) {
  const allowedOrigin = getAllowedOrigin(request);
  if (!allowedOrigin) return {};

  return {
    "Access-Control-Allow-Origin": allowedOrigin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
  };
}

function parseCookies(request) {
  const header = request.headers.cookie || "";
  return header.split(";").reduce((accumulator, part) => {
    const [rawKey, ...rest] = part.trim().split("=");
    if (!rawKey) return accumulator;
    accumulator[rawKey] = decodeURIComponent(rest.join("="));
    return accumulator;
  }, {});
}

function buildCookie(name, value, options = {}) {
  const segments = [`${name}=${encodeURIComponent(value)}`];

  if (options.httpOnly) segments.push("HttpOnly");
  if (options.sameSite) segments.push(`SameSite=${options.sameSite}`);
  if (options.path) segments.push(`Path=${options.path}`);
  if (options.maxAge !== undefined) segments.push(`Max-Age=${options.maxAge}`);
  if (options.secure) segments.push("Secure");

  return segments.join("; ");
}

function isProductionRequest(request) {
  const forwardedProto = request.headers["x-forwarded-proto"];
  return forwardedProto === "https";
}

function createSessionCookie(request, sessionId) {
  const crossOrigin = Boolean(FRONTEND_ORIGIN && FRONTEND_ORIGIN !== APP_BASE_URL);
  return buildCookie(SESSION_COOKIE, sessionId, {
    httpOnly: true,
    sameSite: crossOrigin ? "None" : "Lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 7,
    secure: crossOrigin || isProductionRequest(request)
  });
}

function clearSessionCookie(request) {
  const crossOrigin = Boolean(FRONTEND_ORIGIN && FRONTEND_ORIGIN !== APP_BASE_URL);
  return buildCookie(SESSION_COOKIE, "", {
    httpOnly: true,
    sameSite: crossOrigin ? "None" : "Lax",
    path: "/",
    maxAge: 0,
    secure: crossOrigin || isProductionRequest(request)
  });
}

function getSessionUser(request) {
  const cookies = parseCookies(request);
  const sessionId = cookies[SESSION_COOKIE];
  if (!sessionId) return null;
  return sessionStore.get(sessionId) || null;
}

function createSession(userProfile) {
  const sessionId = crypto.randomUUID();
  sessionStore.set(sessionId, userProfile);
  return sessionId;
}

function destroySession(request) {
  const cookies = parseCookies(request);
  const sessionId = cookies[SESSION_COOKIE];
  if (sessionId) {
    sessionStore.delete(sessionId);
  }
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

function isValidPhone(value) {
  return /^[6-9]\d{9}$/.test(value);
}

function isValidPercentile(value) {
  const numericValue = Number(value);
  return Number.isFinite(numericValue) && numericValue >= 0 && numericValue <= 100;
}

function getPublicUser(user) {
  if (!user) return null;
  return {
    name: user.name,
    email: user.email,
    picture: user.picture
  };
}

function getPhonePeHost() {
  return PHONEPE_ENV === "production"
    ? "https://api.phonepe.com/apis/pg"
    : "https://api-preprod.phonepe.com/apis/pg-sandbox";
}

function getAdminAccessKey(request, url) {
  const authHeader = request.headers.authorization || "";
  if (authHeader.startsWith("Bearer ")) {
    return authHeader.slice("Bearer ".length).trim();
  }

  const headerKey = request.headers["x-admin-key"];
  if (headerKey) {
    return String(headerKey).trim();
  }

  return String(url.searchParams.get("accessKey") || "").trim();
}

function requireAdminAccess(request, response, url, corsHeaders = {}) {
  if (!ADMIN_ACCESS_KEY) {
    sendJson(response, 503, { message: "Admin access is not configured yet." }, corsHeaders);
    return false;
  }

  const providedKey = getAdminAccessKey(request, url);
  if (!providedKey || providedKey !== ADMIN_ACCESS_KEY) {
    sendJson(response, 401, { message: "Invalid admin access key." }, corsHeaders);
    return false;
  }

  return true;
}

function escapeCsvValue(value) {
  const stringValue = String(value ?? "");
  if (/[",\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, "\"\"")}"`;
  }
  return stringValue;
}

function createWaitlistCsv(entries) {
  const columns = ["name", "email", "phone", "schoolPercentile", "category", "submittedAt"];
  const rows = entries.map((entry) => columns.map((column) => escapeCsvValue(entry[column])).join(","));
  return [columns.join(","), ...rows].join("\n");
}

async function handleWaitlistSubmission(request, response) {
  try {
    const body = await parseRequestBody(request);
    const name = String(body.name || "").trim();
    const email = String(body.email || "").trim().toLowerCase();
    const phone = String(body.phone || "").trim();
    const schoolPercentile = String(body.schoolPercentile || "").trim();
    const category = String(body.category || "").trim();

    if (name.length < 2) {
      sendJson(response, 400, { message: "Please enter your full name." });
      return;
    }

    if (!isValidEmail(email)) {
      sendJson(response, 400, { message: "Please enter a valid email address." });
      return;
    }

    if (!isValidPhone(phone)) {
      sendJson(response, 400, { message: "Please enter a valid 10-digit phone number." });
      return;
    }

    if (!isValidPercentile(schoolPercentile)) {
      sendJson(response, 400, { message: "Please enter a valid school percentile between 0 and 100." });
      return;
    }

    if (!category) {
      sendJson(response, 400, { message: "Please select your candidate category." });
      return;
    }

    const waitlist = await readJsonFile(WAITLIST_FILE);
    const alreadyExists = waitlist.some((entry) => entry.email === email);

    if (alreadyExists) {
      sendJson(response, 409, { message: "This email is already on the waitlist." });
      return;
    }

    waitlist.push({
      name,
      email,
      phone,
      schoolPercentile: Number(schoolPercentile),
      category,
      submittedAt: new Date().toISOString()
    });

    await writeJsonFile(WAITLIST_FILE, waitlist);

    sendJson(response, 201, {
      message: "Thanks! Your details have been received and you're on the IMBA Beacon waitlist now."
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to process request." });
  }
}

async function handleWaitlistList(response) {
  try {
    const waitlist = await readJsonFile(WAITLIST_FILE);
    sendJson(response, 200, {
      count: waitlist.length,
      entries: waitlist
    });
  } catch (error) {
    sendJson(response, 500, { message: "Unable to read waitlist data." });
  }
}

async function handleAdminWaitlist(request, response, url, corsHeaders) {
  if (!requireAdminAccess(request, response, url, corsHeaders)) return;

  try {
    const waitlist = await readJsonFile(WAITLIST_FILE);
    sendJson(response, 200, {
      count: waitlist.length,
      entries: waitlist
    }, corsHeaders);
  } catch (error) {
    sendJson(response, 500, { message: "Unable to read waitlist data." }, corsHeaders);
  }
}

async function handleAdminWaitlistCsv(request, response, url, corsHeaders) {
  if (!requireAdminAccess(request, response, url, corsHeaders)) return;

  try {
    const waitlist = await readJsonFile(WAITLIST_FILE);
    const csv = createWaitlistCsv(waitlist);
    response.writeHead(200, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": 'attachment; filename="imba-beacon-waitlist.csv"',
      "Cache-Control": "no-store",
      ...corsHeaders
    });
    response.end(csv);
  } catch (error) {
    sendJson(response, 500, { message: "Unable to export waitlist CSV." }, corsHeaders);
  }
}

function handleConfig(response) {
  sendJson(response, 200, {
    appBaseUrl: APP_BASE_URL,
    frontendOrigin: FRONTEND_ORIGIN,
    googleClientId: GOOGLE_CLIENT_ID,
    paymentsEnabled: PAYMENTS_ENABLED,
    paymentProvider: "phonepe",
    phonepeClientId: PHONEPE_CLIENT_ID,
    phonepeHost: getPhonePeHost(),
    plans: Object.values(PLAN_CATALOG).map((plan) => ({
      id: plan.id,
      label: plan.label,
      amount: plan.amount,
      currency: plan.currency,
      description: plan.description
    }))
  });
}

async function handleGoogleLogin(request, response) {
  if (!googleClient || !GOOGLE_CLIENT_ID) {
    sendJson(response, 503, {
      message: "Google login is not configured yet. Add GOOGLE_CLIENT_ID on the server."
    });
    return;
  }

  try {
    const body = await parseRequestBody(request);
    const credential = String(body.credential || "");

    if (!credential) {
      sendJson(response, 400, { message: "Missing Google credential." });
      return;
    }

    const ticket = await googleClient.verifyIdToken({
      idToken: credential,
      audience: GOOGLE_CLIENT_ID
    });

    const payload = ticket.getPayload();

    if (!payload || !payload.email || !payload.email_verified) {
      sendJson(response, 401, { message: "Google account could not be verified." });
      return;
    }

    const sessionId = createSession({
      googleId: payload.sub,
      name: payload.name || payload.email,
      email: payload.email,
      picture: payload.picture || ""
    });

    sendJson(
      response,
      200,
      {
        message: "Google sign-in successful.",
        user: getPublicUser(sessionStore.get(sessionId))
      },
      {
        "Set-Cookie": createSessionCookie(request, sessionId)
      }
    );
  } catch (error) {
    sendJson(response, 401, { message: "Unable to verify Google sign-in." });
  }
}

function handleSession(request, response) {
  sendJson(response, 200, {
    user: getPublicUser(getSessionUser(request))
  });
}

function handleLogout(request, response) {
  destroySession(request);
  sendJson(
    response,
    200,
    { message: "Logged out successfully." },
    { "Set-Cookie": clearSessionCookie(request) }
  );
}

function requireAuthenticatedUser(request, response) {
  const user = getSessionUser(request);
  if (!user) {
    sendJson(response, 401, { message: "Please sign in with Google first." });
    return null;
  }
  return user;
}

async function createPhonePeOrder() {
  throw new Error("PhonePe checkout is not fully configured yet. Add merchant credentials and complete the hosted checkout integration.");
}

async function handleCreateOrder(request, response) {
  const user = requireAuthenticatedUser(request, response);
  if (!user) return;

  if (!PAYMENTS_ENABLED) {
    sendJson(response, 503, {
      message: "Online payments are temporarily disabled. Please join the waitlist and we will share enrollment details directly."
    });
    return;
  }

  if (!PHONEPE_CLIENT_ID || !PHONEPE_CLIENT_SECRET || !PHONEPE_CLIENT_VERSION) {
    sendJson(response, 503, {
      message: "PhonePe is not configured yet. Add PHONEPE_CLIENT_ID, PHONEPE_CLIENT_SECRET, and PHONEPE_CLIENT_VERSION."
    });
    return;
  }

  try {
    const body = await parseRequestBody(request);
    const planId = String(body.planId || "").toLowerCase();
    const plan = PLAN_CATALOG[planId];

    if (!plan) {
      sendJson(response, 400, { message: "Invalid plan selected." });
      return;
    }

    const orderId = `IMBA_${plan.id}_${Date.now()}`.slice(0, 40);
    const transaction = await createPhonePeOrder(plan, orderId, user);

    sendJson(response, 201, {
      checkoutUrl: transaction.checkoutUrl,
      orderId,
      plan: {
        id: plan.id,
        label: plan.label,
        amount: plan.amount,
        currency: plan.currency,
        amountDisplay: (plan.amount / 100).toFixed(2)
      },
      user: getPublicUser(user)
    });
  } catch (error) {
    sendJson(response, 500, { message: error.message || "Unable to create PhonePe order." });
  }
}

async function fetchPhonePeOrderStatus() {
  throw new Error("PhonePe status verification is not configured yet.");
}

async function handleVerifyPayment(request, response) {
  const user = requireAuthenticatedUser(request, response);
  if (!user) return;

  if (!PAYMENTS_ENABLED) {
    sendJson(response, 503, {
      message: "Online payments are temporarily disabled."
    });
    return;
  }

  if (!PHONEPE_CLIENT_ID || !PHONEPE_CLIENT_SECRET || !PHONEPE_CLIENT_VERSION) {
    sendJson(response, 503, { message: "PhonePe verification is not configured yet." });
    return;
  }

  try {
    const body = await parseRequestBody(request);
    const { planId, orderId } = body;

    const plan = PLAN_CATALOG[String(planId || "").toLowerCase()];

    if (!plan || !orderId) {
      sendJson(response, 400, { message: "Missing payment verification details." });
      return;
    }

    const statusPayload = await fetchPhonePeOrderStatus(orderId);
    const txnInfo = statusPayload.data || {};

    if (!statusPayload.success) {
      sendJson(response, 400, {
        message: statusPayload.message || "Payment has not been completed successfully yet."
      });
      return;
    }

    const payments = await readJsonFile(PAYMENTS_FILE);
    const existingPayment = payments.find((payment) => payment.orderId === orderId);

    if (!existingPayment) {
      payments.push({
        planId: plan.id,
        planLabel: plan.label,
        amount: plan.amount,
        currency: plan.currency,
        orderId,
        paymentId: txnInfo.transactionId || "",
        paymentMode: txnInfo.paymentInstrument || "",
        bankTxnId: txnInfo.providerReferenceId || "",
        email: user.email,
        name: user.name,
        verifiedAt: new Date().toISOString()
      });

      await writeJsonFile(PAYMENTS_FILE, payments);
    }

    sendJson(response, 200, {
      message: `Payment verified successfully for the ${plan.label} plan via PhonePe.`
    });
  } catch (error) {
    sendJson(response, 500, { message: error.message || "Unable to verify payment." });
  }
}

async function requestHandler(request, response) {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = normalizePathname(url.pathname);
  const corsHeaders = getCorsHeaders(request);

  if (request.method === "OPTIONS") {
    response.writeHead(204, corsHeaders);
    response.end();
    return;
  }

  if (request.method === "GET" && pathname === "/api/config") {
    sendJson(response, 200, {
      appBaseUrl: APP_BASE_URL,
      frontendOrigin: FRONTEND_ORIGIN,
      googleClientId: GOOGLE_CLIENT_ID,
      paymentsEnabled: PAYMENTS_ENABLED,
      paymentProvider: "phonepe",
      phonepeClientId: PHONEPE_CLIENT_ID,
      phonepeHost: getPhonePeHost(),
      plans: Object.values(PLAN_CATALOG).map((plan) => ({
        id: plan.id,
        label: plan.label,
        amount: plan.amount,
        currency: plan.currency,
        description: plan.description
      }))
    }, corsHeaders);
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/google") {
    await handleGoogleLogin(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "GET" && pathname === "/api/auth/session") {
    sendJson(response, 200, {
      user: getPublicUser(getSessionUser(request))
    }, corsHeaders);
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/logout") {
    sendJson(
      response,
      200,
      { message: "Logged out successfully." },
      {
        ...corsHeaders,
        "Set-Cookie": clearSessionCookie(request)
      }
    );
    return;
  }

  if (request.method === "POST" && pathname === "/api/payments/order") {
    await handleCreateOrder(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/payments/verify") {
    await handleVerifyPayment(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/waitlist") {
    await handleWaitlistSubmission(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "GET" && pathname === "/api/waitlist") {
    const waitlist = await readJsonFile(WAITLIST_FILE);
    sendJson(response, 200, {
      count: waitlist.length,
      entries: waitlist
    }, corsHeaders);
    return;
  }

  if (request.method === "GET" && pathname === "/api/admin/waitlist") {
    await handleAdminWaitlist(request, response, url, corsHeaders);
    return;
  }

  if (request.method === "GET" && pathname === "/api/admin/waitlist.csv") {
    await handleAdminWaitlistCsv(request, response, url, corsHeaders);
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
    console.log(`IMBA Beacon server running at ${APP_BASE_URL}`);
  });
}

startServer().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});
