const http = require("node:http");
const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const { existsSync } = fsSync;
const path = require("node:path");
const crypto = require("node:crypto");
const { URL } = require("node:url");
const { promisify } = require("node:util");
const { OAuth2Client } = require("google-auth-library");
const nodemailer = require("nodemailer");
const { Pool } = require("pg");
const Razorpay = require("razorpay");

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 3000);
const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, "data");
const PROTECTED_CONTENT_DIR = path.join(ROOT_DIR, "protected-content");
const WAITLIST_FILE = path.join(DATA_DIR, "waitlist.json");
const PAYMENTS_FILE = path.join(DATA_DIR, "payments.json");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const OTP_FILE = path.join(DATA_DIR, "otp-codes.json");
const SESSIONS_FILE = path.join(DATA_DIR, "sessions.json");
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
    const rawValue = trimmedLine.slice(separatorIndex + 1).trim();
    const value = (
      (rawValue.startsWith('"') && rawValue.endsWith('"')) ||
      (rawValue.startsWith("'") && rawValue.endsWith("'"))
    )
      ? rawValue.slice(1, -1)
      : rawValue;

    if (key && !process.env[key]) {
      process.env[key] = value;
    }
  });
}

const DATABASE_URL = process.env.DATABASE_URL || "";
const DATABASE_SSL = process.env.DATABASE_SSL !== "false";
const DATABASE_POOL_MAX = Number(process.env.DATABASE_POOL_MAX || 10);
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const RAZORPAY_KEY_ID = String(process.env.RAZORPAY_KEY_ID || "").trim();
const RAZORPAY_KEY_SECRET = String(process.env.RAZORPAY_KEY_SECRET || "").trim();
const PAYMENTS_ENABLED = process.env.PAYMENTS_ENABLED === "true";
const ADMIN_ACCESS_KEY = process.env.ADMIN_ACCESS_KEY || "";
const EMAIL_OTP_EXPIRY_MINUTES = Number(process.env.EMAIL_OTP_EXPIRY_MINUTES || 10);
const RESEND_API_KEY = process.env.RESEND_API_KEY || "";
const RESEND_FROM = process.env.RESEND_FROM || "";
const SMTP_HOST = process.env.SMTP_HOST || "";
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_SECURE = process.env.SMTP_SECURE === "true";
const SMTP_USER = process.env.SMTP_USER || "";
const SMTP_PASS = process.env.SMTP_PASS || "";
const EMAIL_FROM = process.env.EMAIL_FROM || RESEND_FROM || SMTP_USER || "";
const APP_BASE_URL = process.env.APP_BASE_URL || `http://127.0.0.1:${PORT}`;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "";

const googleClient = GOOGLE_CLIENT_ID ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;
const sessionStore = new Map();
let database = null;
let razorpayClient = null;
const PREMIUM_FEATURE_KEYS = [
  "liveClasses",
  "mockInterviews",
  "practiceZone",
  "mentorFeedback",
  "leaderboard",
  "progressTracker",
  "newsBriefs"
];
const scryptAsync = promisify(crypto.scrypt);
const otpTransporter = SMTP_HOST && SMTP_USER && SMTP_PASS
  ? nodemailer.createTransport({
      host: SMTP_HOST,
      port: SMTP_PORT,
      secure: SMTP_SECURE,
      family: 4,
      connectionTimeout: 15000,
      greetingTimeout: 15000,
      socketTimeout: 20000,
      auth: {
        user: SMTP_USER,
        pass: SMTP_PASS
      }
    })
  : null;

const PLAN_CATALOG = {
  basic: {
    id: "basic",
    label: "Basic",
    amount: 119900,
    currency: "INR",
    description: "IMBA Beacon Basic preparation plan"
  },
  pro: {
    id: "pro",
    label: "Pro",
    amount: 179900,
    currency: "INR",
    description: "IMBA Beacon Pro preparation plan"
  }
};

const DEFAULT_COUPON_CODE = String(process.env.IMBA_COUPON_CODE || "IMDS10").trim().toUpperCase();
const DEFAULT_COUPON_DISCOUNT_PERCENT = 10;
const FREE_SESSION_TYPES = {
  interview: "Free Interview Session",
  batch: "Free Batch Interaction Session"
};

const PROTECTED_CONTENT = {
  "daily-18-may": { file: "articles/daily-18-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-19-may": { file: "articles/daily-19-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-20-may": { file: "articles/daily-20-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-21-may": { file: "articles/daily-21-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-22-may": { file: "articles/daily-22-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-23-may": { file: "articles/daily-23-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-25-may": { file: "articles/daily-25-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-26-may": { file: "articles/daily-26-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-27-may": { file: "articles/daily-27-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-28-may": { file: "articles/daily-28-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "daily-29-may": { file: "articles/daily-29-may.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "weekly-business-brief-week-1": { file: "articles/weekly-business-brief-week-1.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "bengal-elections": { file: "articles/Bengal_Elections.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "tamil-nadu-elections": { file: "articles/Tamil_nadu_elections.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "pm-modi-visit": { file: "articles/PM_modi_visit_.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "neet-paper-leak-report": { file: "articles/IIT-Mandi-interview-prep-PDF__1_.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "master-question-bank": { file: "sheets/master-question-bank-final.html", minPlan: "pro", type: "text/html; charset=utf-8" },
  "economic-terms": { file: "sheets/economic-terms-final.html", minPlan: "pro", type: "text/html; charset=utf-8" },
  "model-answers": { file: "sheets/model-answers.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "previous-year-questions": {
    file: "sheets/previous-year-questions.xlsx",
    minPlan: "basic",
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    downloadName: "previous-year-questions.xlsx"
  },
  "sets-relation-functions-notes": {
    file: "sheets/sets-relation-functions-notes.pdf",
    minPlan: "basic",
    type: "application/pdf",
    downloadName: "sets-relation-functions-notes.pdf"
  },
  "trigonometry-inverse-trigonometry-notes": {
    file: "sheets/trigonometry-inverse-trigonometry-notes.pdf",
    minPlan: "basic",
    type: "application/pdf",
    downloadName: "trigonometry-inverse-trigonometry-notes.pdf"
  },
  "lecture-2-practise-2-2-trigonometry": {
    file: "sheets/lecture-2-practise-2-2-trigonometry.pdf",
    minPlan: "basic",
    type: "application/pdf",
    downloadName: "lecture-2-practise-2-2-trigonometry.pdf"
  },
  "quiz-1": { file: "quizzes/quiz-1.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-2": { file: "quizzes/quiz-2.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-3": { file: "quizzes/quiz-3.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-4": { file: "quizzes/quiz-4.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-5": { file: "quizzes/quiz-5.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-6": { file: "quizzes/quiz-6.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-7": { file: "quizzes/quiz-7.html", minPlan: "basic", type: "text/html; charset=utf-8" },
  "quiz-8": { file: "quizzes/quiz-8.html", minPlan: "basic", type: "text/html; charset=utf-8" }
};

function normalizeCouponCode(value) {
  return String(value || "").trim().toUpperCase().replace(/\s+/g, "");
}

function normalizeFreeSessionType(value) {
  const sessionType = String(value || "").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(FREE_SESSION_TYPES, sessionType) ? sessionType : "";
}

function formatAmountDisplay(amount, currency = "INR") {
  const safeAmount = Number.isFinite(amount) ? amount : 0;
  const prefix = currency === "INR" ? "₹" : `${currency} `;
  return `${prefix}${(safeAmount / 100).toFixed(2)}`;
}

async function resolveCoupon(plan, rawCouponCode) {
  const couponCode = normalizeCouponCode(rawCouponCode);

  if (!plan || !couponCode) {
    return { applied: false, code: couponCode };
  }

  if (!DEFAULT_COUPON_CODE || couponCode !== DEFAULT_COUPON_CODE) {
    return {
      applied: false,
      code: couponCode,
      message: "Invalid coupon code. Please check the code and try again."
    };
  }

  if (plan.id !== "pro") {
    return {
      applied: false,
      code: couponCode,
      message: "This coupon is available only for the Pro plan."
    };
  }

  const db = await getDatabase();
  await db.run(`
    INSERT INTO coupons (code, used_count, max_uses)
    VALUES (?, 0, 10)
    ON CONFLICT(code) DO NOTHING
  `, [couponCode]);

  const row = await db.get("SELECT used_count, max_uses FROM coupons WHERE code = ?", couponCode);
  const usedCount = row?.used_count || 0;
  const maxUses = row?.max_uses || 10;

  if (usedCount >= maxUses) {
    return {
      applied: false,
      code: couponCode,
      message: `Coupon limit reached. This code was valid for the first ${maxUses} students only.`
    };
  }

  const originalAmount = plan.amount;
  const discountPercent = DEFAULT_COUPON_DISCOUNT_PERCENT;
  const discountAmount = Math.round((originalAmount * discountPercent) / 100);
  const finalAmount = Math.max(100, originalAmount - discountAmount);

  return {
    applied: true,
    code: couponCode,
    discountPercent,
    originalAmount,
    discountAmount,
    finalAmount,
    remaining: maxUses - usedCount
  };
}




async function ensureDataStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });

  if (!existsSync(WAITLIST_FILE)) {
    await fs.writeFile(WAITLIST_FILE, "[]\n", "utf8");
  }

  if (!existsSync(PAYMENTS_FILE)) {
    await fs.writeFile(PAYMENTS_FILE, "[]\n", "utf8");
  }

  if (!existsSync(USERS_FILE)) {
    await fs.writeFile(USERS_FILE, "[]\n", "utf8");
  }

  if (!existsSync(OTP_FILE)) {
    await fs.writeFile(OTP_FILE, "[]\n", "utf8");
  }

  if (!existsSync(SESSIONS_FILE)) {
    await fs.writeFile(SESSIONS_FILE, "{}\n", "utf8");
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

function safeParseJson(value, fallback) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
}

function normalizeTimestamp(value, fallback = null) {
  if (value === null || value === undefined) {
    return fallback;
  }

  const stringValue = String(value).trim();
  if (!stringValue) {
    return fallback;
  }

  const parsedTime = Date.parse(stringValue);
  if (Number.isNaN(parsedTime)) {
    return fallback;
  }

  return new Date(parsedTime).toISOString();
}

function normalizeSqlParams(args) {
  if (args.length === 1 && Array.isArray(args[0])) {
    return args[0];
  }

  return args;
}

function toPostgresPlaceholders(sql) {
  let placeholderIndex = 0;
  return sql.replace(/\?/g, () => `$${++placeholderIndex}`);
}

function createDatabaseAdapter(pool) {
  return {
    async exec(sql) {
      await pool.query(sql);
    },
    async get(sql, ...args) {
      const params = normalizeSqlParams(args);
      const result = await pool.query(toPostgresPlaceholders(sql), params);
      return result.rows[0] || null;
    },
    async all(sql, ...args) {
      const params = normalizeSqlParams(args);
      const result = await pool.query(toPostgresPlaceholders(sql), params);
      return result.rows;
    },
    async run(sql, ...args) {
      const params = normalizeSqlParams(args);
      return pool.query(toPostgresPlaceholders(sql), params);
    }
  };
}

async function getDatabase() {
  if (database) return database;

  await ensureDataStore();
  if (!DATABASE_URL) {
    throw new Error("DATABASE_URL is required. Add your Supabase Postgres connection string before starting the server.");
  }

  const pool = new Pool({
    connectionString: DATABASE_URL,
    max: Number.isFinite(DATABASE_POOL_MAX) && DATABASE_POOL_MAX > 0 ? DATABASE_POOL_MAX : 10,
    ssl: DATABASE_SSL ? { rejectUnauthorized: false } : false
  });

  await pool.query("SELECT 1");
  database = createDatabaseAdapter(pool);

  await database.exec(`
    CREATE TABLE IF NOT EXISTS users (
      email TEXT PRIMARY KEY,
      google_id TEXT,
      name TEXT NOT NULL,
      password_hash TEXT,
      phone TEXT,
      school_percentile DOUBLE PRECISION,
      category TEXT,
      picture TEXT,
      provider TEXT NOT NULL DEFAULT 'beacon',
      email_verified BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ,
      last_login_at TIMESTAMPTZ,
      course_access_json JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE TABLE IF NOT EXISTS otp_codes (
      email TEXT PRIMARY KEY,
      purpose TEXT NOT NULL,
      otp_hash TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      user_email TEXT NOT NULL,
      user_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS waitlist (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      phone TEXT NOT NULL,
      school_percentile DOUBLE PRECISION NOT NULL,
      category TEXT NOT NULL,
      submitted_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS payments (
      order_id TEXT PRIMARY KEY,
      plan_id TEXT NOT NULL,
      plan_label TEXT NOT NULL,
      amount INTEGER NOT NULL,
      currency TEXT NOT NULL,
      payment_id TEXT,
      payment_mode TEXT,
      bank_txn_id TEXT,
      email TEXT NOT NULL,
      name TEXT NOT NULL,
      verified_at TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS coupons (
      code TEXT PRIMARY KEY,
      used_count INTEGER NOT NULL DEFAULT 0,
      max_uses INTEGER NOT NULL DEFAULT 10
    );

    CREATE TABLE IF NOT EXISTS free_session_registrations (
      id BIGSERIAL PRIMARY KEY,
      session_type TEXT NOT NULL,
      session_label TEXT NOT NULL,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      phone TEXT NOT NULL,
      school_percentile DOUBLE PRECISION NOT NULL,
      registered_at TIMESTAMPTZ NOT NULL,
      UNIQUE (session_type, email)
    );

    CREATE TABLE IF NOT EXISTS quiz_attempts (
      id BIGSERIAL PRIMARY KEY,
      email TEXT NOT NULL,
      quiz_id TEXT NOT NULL,
      quiz_title TEXT NOT NULL,
      set_id TEXT NOT NULL,
      score INTEGER NOT NULL,
      total INTEGER NOT NULL,
      percent INTEGER NOT NULL,
      answers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      submitted_at TIMESTAMPTZ NOT NULL,
      UNIQUE (email, quiz_id, set_id)
    );
  `);

  await migrateLegacyData(database);
  return database;
}

async function loadSessionStore() {
  const db = await getDatabase();
  const sessionRows = await db.all("SELECT session_id, user_json FROM sessions");
  sessionStore.clear();
  sessionRows.forEach((row) => {
    const sessionUser = safeParseJson(row.user_json, null);
    if (row.session_id && sessionUser && typeof sessionUser === "object") {
      sessionStore.set(row.session_id, sessionUser);
    }
  });
}

async function saveSessionStore() {
  const db = await getDatabase();
  const createdAt = new Date().toISOString();
  for (const [sessionId, sessionUser] of sessionStore.entries()) {
    await db.run(`
      INSERT INTO sessions (session_id, user_email, user_json, created_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT (session_id) DO UPDATE SET
        user_email = EXCLUDED.user_email,
        user_json = EXCLUDED.user_json
    `, [
      sessionId,
      normalizeEmail(sessionUser.email),
      JSON.stringify(sessionUser),
      createdAt
    ]);
  }
}

async function saveSession(sessionId, sessionUser) {
  const db = await getDatabase();
  await db.run(`
    INSERT INTO sessions (session_id, user_email, user_json, created_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT (session_id) DO UPDATE SET
      user_email = EXCLUDED.user_email,
      user_json = EXCLUDED.user_json
  `, [
    sessionId,
    normalizeEmail(sessionUser.email),
    JSON.stringify(sessionUser),
    new Date().toISOString()
  ]);
}

async function deleteSession(sessionId) {
  const db = await getDatabase();
  await db.run("DELETE FROM sessions WHERE session_id = ?", sessionId);
}

function sendJson(response, statusCode, payload, extraHeaders = {}) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...extraHeaders
  });
  response.end(JSON.stringify(payload));
}


function normalizePathname(pathname) {
  if (pathname === "/") return "/index.html";
  return pathname;
}

const ALLOWED_ORIGINS = [
  "https://imba-beacon.in",
  "https://www.imba-beacon.in",
  "https://imba-beacon.vercel.app",
  "http://127.0.0.1:3000",
  "http://localhost:3000"
];

function getAllowedOrigin(request) {
  const requestOrigin = request.headers.origin || "";
  if (!requestOrigin) return "";
  if (FRONTEND_ORIGIN && requestOrigin === FRONTEND_ORIGIN) return requestOrigin;
  if (ALLOWED_ORIGINS.includes(requestOrigin)) return requestOrigin;
  if (requestOrigin.endsWith(".vercel.app")) return requestOrigin;
  return "";
}

function getCorsHeaders(request) {
  const allowedOrigin = getAllowedOrigin(request);
  if (!allowedOrigin) return {};

  return {
    "Access-Control-Allow-Origin": allowedOrigin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
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

function isCrossOriginRequest(request) {
  const requestOrigin = request.headers.origin || "";
  if (!requestOrigin) return false;

  try {
    const originHost = new URL(requestOrigin).host;
    const appHost = new URL(APP_BASE_URL).host;
    return originHost !== appHost;
  } catch (error) {
    return Boolean(FRONTEND_ORIGIN && requestOrigin === FRONTEND_ORIGIN);
  }
}

function createSessionCookie(request, sessionId) {
  const crossOrigin = isCrossOriginRequest(request);
  return buildCookie(SESSION_COOKIE, sessionId, {
    httpOnly: true,
    sameSite: crossOrigin ? "None" : "Lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 7,
    secure: crossOrigin || isProductionRequest(request)
  });
}

function clearSessionCookie(request) {
  const crossOrigin = isCrossOriginRequest(request);
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
  const authHeader = request.headers.authorization || "";
  const bearerSessionId = authHeader.startsWith("Bearer ") ? authHeader.slice("Bearer ".length).trim() : "";
  const sessionId = bearerSessionId || cookies[SESSION_COOKIE];
  if (!sessionId) return null;
  return sessionStore.get(sessionId) || null;
}

async function createSession(userProfile) {
  const sessionId = crypto.randomUUID();
  sessionStore.set(sessionId, userProfile);
  await saveSession(sessionId, userProfile);
  return sessionId;
}

async function destroySession(request) {
  const cookies = parseCookies(request);
  const authHeader = request.headers.authorization || "";
  const bearerSessionId = authHeader.startsWith("Bearer ") ? authHeader.slice("Bearer ".length).trim() : "";
  const sessionId = bearerSessionId || cookies[SESSION_COOKIE];
  if (sessionId) {
    sessionStore.delete(sessionId);
    await deleteSession(sessionId);
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
  const courseAccess = normalizeCourseAccess(user);
  return {
    name: user.name,
    email: user.email,
    picture: user.picture,
    phone: user.phone || "",
    schoolPercentile: user.schoolPercentile ?? null,
    category: user.category || "",
    provider: user.provider || "beacon",
    emailVerified: Boolean(user.emailVerified),
    createdAt: user.createdAt || "",
    lastLoginAt: user.lastLoginAt || "",
    courseAccess
  };
}

function normalizeFeatureAccess(value, purchased = false) {
  const source = value && typeof value === "object" ? value : {};
  return PREMIUM_FEATURE_KEYS.reduce((accumulator, key) => {
    accumulator[key] = typeof source[key] === "boolean" ? source[key] : purchased;
    return accumulator;
  }, {});
}

function normalizeCourseAccess(user) {
  const source = user?.courseAccess && typeof user.courseAccess === "object" ? user.courseAccess : {};
  const purchased = Boolean(source.purchased || user?.purchased);
  const planId = source.planId === "basic" || source.planId === "pro" ? source.planId : null;
  return {
    purchased,
    planId,
    grantedAt: source.grantedAt || "",
    featureAccess: normalizeFeatureAccess(source.featureAccess, purchased)
  };
}

function getUserPlan(user) {
  const courseAccess = normalizeCourseAccess(user);
  return courseAccess.purchased && (courseAccess.planId === "basic" || courseAccess.planId === "pro")
    ? courseAccess.planId
    : "none";
}

function canAccessPlan(user, minPlan) {
  const plan = getUserPlan(user);
  if (minPlan === "pro") return plan === "pro";
  if (minPlan === "basic") return plan === "basic" || plan === "pro";
  return Boolean(user);
}

function getFrontendUrl(pathname) {
  const baseUrl = FRONTEND_ORIGIN || "https://www.imba-beacon.in";
  try {
    return new URL(pathname, baseUrl).toString();
  } catch (error) {
    return pathname;
  }
}

function redirectToLogin(request, response, url) {
  const nextPath = `${url.pathname}${url.search || ""}`;
  const loginUrl = getFrontendUrl(`/login.html?next=${encodeURIComponent(nextPath)}`);
  response.writeHead(302, {
    Location: loginUrl,
    "Cache-Control": "no-store"
  });
  response.end();
}

function sendAccessDeniedPage(response, contentMeta) {
  response.writeHead(403, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Access denied | IMBA Beacon</title>
  <style>
    :root{--maroon:#4a1830;--maroon-deep:#2f1022;--gold:#f3bd6a;--cream:#fffaf4;--ink:#3b2230}
    *{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(circle at top,#fff7ed,var(--cream));font-family:Manrope,Arial,sans-serif;color:var(--ink);padding:24px}
    .card{width:min(560px,100%);border:1px solid #eadfd6;border-radius:28px;background:#fff;box-shadow:0 24px 70px rgba(74,24,48,.14);padding:34px}
    .badge{display:inline-flex;align-items:center;border-radius:999px;background:#fff0d2;color:var(--maroon);font-weight:800;padding:8px 14px;margin-bottom:18px}
    h1{font-family:Georgia,serif;font-size:clamp(2rem,6vw,3.3rem);line-height:1;margin:0 0 12px;color:var(--maroon)}
    p{font-size:1rem;line-height:1.65;color:#7d6874;margin:0 0 22px}
    a{display:inline-flex;text-decoration:none;background:linear-gradient(135deg,var(--gold),#e8993f);color:var(--maroon-deep);font-weight:800;border-radius:16px;padding:13px 18px}
  </style>
</head>
<body>
  <main class="card">
    <span class="badge">${contentMeta.minPlan === "pro" ? "Pro only" : "Paid members only"}</span>
    <h1>Access denied</h1>
    <p>This content is protected for ${contentMeta.minPlan === "pro" ? "Pro plan" : "Basic and Pro plan"} students. Please upgrade or switch to an eligible account to continue.</p>
    <a href="${getFrontendUrl("/student.html#section-upgrade")}">View plans</a>
  </main>
</body>
</html>`);
}

function resolveProtectedContent(slug) {
  const normalizedSlug = String(slug || "").trim().toLowerCase().replace(/\.html$/i, "");
  const contentMeta = PROTECTED_CONTENT[normalizedSlug];
  if (!contentMeta) return null;

  const filePath = path.resolve(PROTECTED_CONTENT_DIR, contentMeta.file);
  if (!filePath.startsWith(PROTECTED_CONTENT_DIR + path.sep)) return null;
  return { ...contentMeta, slug: normalizedSlug, filePath };
}

async function serveProtectedContent(request, response, url) {
  const slug = decodeURIComponent(url.pathname.replace(/^\/protected\//, ""));
  const contentMeta = resolveProtectedContent(slug);

  if (!contentMeta || !existsSync(contentMeta.filePath)) {
    sendJson(response, 404, { message: "Protected content not found." });
    return;
  }

  const user = getSessionUser(request);
  if (!user) {
    redirectToLogin(request, response, url);
    return;
  }

  if (!canAccessPlan(user, contentMeta.minPlan)) {
    sendAccessDeniedPage(response, contentMeta);
    return;
  }

  const headers = {
    "Content-Type": contentMeta.type || "application/octet-stream",
    "Cache-Control": "private, no-store",
    "X-Content-Type-Options": "nosniff"
  };

  if (contentMeta.downloadName) {
    const disposition = url.searchParams.get("download") === "1" ? "attachment" : "inline";
    headers["Content-Disposition"] = `${disposition}; filename="${contentMeta.downloadName}"`;
  }

  if ((contentMeta.type || "").startsWith("text/html")) {
    const html = await fs.readFile(contentMeta.filePath, "utf8");
    const htmlWithBase = html.includes("<base ")
      ? html
      : html.replace(/<head([^>]*)>/i, `<head$1>\n<base href="${getFrontendUrl("/")}">`);
    response.writeHead(200, headers);
    response.end(htmlWithBase);
    return;
  }

  response.writeHead(200, headers);
  fsSync.createReadStream(contentMeta.filePath).pipe(response);
}

function buildPurchasedCourseAccess(user, planId) {
  const currentCourseAccess = normalizeCourseAccess(user);
  return {
    purchased: true,
    planId,
    grantedAt: currentCourseAccess.grantedAt || new Date().toISOString(),
    featureAccess: PREMIUM_FEATURE_KEYS.reduce((accumulator, key) => {
      accumulator[key] = true;
      return accumulator;
    }, {})
  };
}

async function syncUserSessions(updatedUser) {
  const email = normalizeEmail(updatedUser?.email);
  if (!email) return;

  Array.from(sessionStore.entries()).forEach(([sessionId, sessionUser]) => {
    if (normalizeEmail(sessionUser?.email) === email) {
      sessionStore.set(sessionId, updatedUser);
    }
  });

  await saveSessionStore();
}

async function activatePlanForUser(user, planId) {
  const persistedUser = await upsertUser({
    ...user,
    courseAccess: buildPurchasedCourseAccess(user, planId)
  });

  await syncUserSessions(persistedUser);
  return persistedUser;
}

function mapUserRow(row) {
  if (!row) return null;

  return {
    googleId: row.google_id || "",
    name: row.name || "",
    email: normalizeEmail(row.email),
    passwordHash: row.password_hash || "",
    phone: row.phone || "",
    schoolPercentile: row.school_percentile === null || row.school_percentile === undefined
      ? null
      : Number(row.school_percentile),
    category: row.category || "",
    picture: row.picture || "",
    provider: row.provider || "beacon",
    emailVerified: Boolean(row.email_verified),
    createdAt: row.created_at || "",
    lastLoginAt: row.last_login_at || "",
    courseAccess: normalizeCourseAccess({
      courseAccess: safeParseJson(row.course_access_json, {})
    })
  };
}

function mapWaitlistRow(row) {
  return {
    name: row.name,
    email: normalizeEmail(row.email),
    phone: row.phone,
    schoolPercentile: Number(row.school_percentile),
    category: row.category,
    submittedAt: row.submitted_at
  };
}

async function upsertUserInDatabase(db, nextUser) {
  const email = normalizeEmail(nextUser.email);
  const existingRow = await db.get("SELECT * FROM users WHERE email = ?", email);
  const existingUser = existingRow ? mapUserRow(existingRow) : null;
  const mergedUser = {
    ...(existingUser || {}),
    ...nextUser,
    email
  };
  const normalizedCourseAccess = normalizeCourseAccess(mergedUser);
  const createdAt = existingUser?.createdAt
    ? normalizeTimestamp(existingUser.createdAt, new Date().toISOString())
    : normalizeTimestamp(mergedUser.createdAt, new Date().toISOString());
  const lastLoginAt = normalizeTimestamp(
    mergedUser.lastLoginAt,
    existingUser?.lastLoginAt ? normalizeTimestamp(existingUser.lastLoginAt, null) : null
  );

  await db.run(`
    INSERT INTO users (
      email,
      google_id,
      name,
      password_hash,
      phone,
      school_percentile,
      category,
      picture,
      provider,
      email_verified,
      created_at,
      last_login_at,
      course_access_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(email) DO UPDATE SET
      google_id = excluded.google_id,
      name = excluded.name,
      password_hash = excluded.password_hash,
      phone = excluded.phone,
      school_percentile = excluded.school_percentile,
      category = excluded.category,
      picture = excluded.picture,
      provider = excluded.provider,
      email_verified = excluded.email_verified,
      created_at = excluded.created_at,
      last_login_at = excluded.last_login_at,
      course_access_json = excluded.course_access_json
  `, [
    email,
    mergedUser.googleId || "",
    mergedUser.name || email,
    mergedUser.passwordHash || "",
    mergedUser.phone || "",
    mergedUser.schoolPercentile === null || mergedUser.schoolPercentile === undefined || mergedUser.schoolPercentile === ""
      ? null
      : Number(mergedUser.schoolPercentile),
    mergedUser.category || "",
    mergedUser.picture || "",
    mergedUser.provider || "beacon",
    Boolean(mergedUser.emailVerified),
    createdAt,
    lastLoginAt,
    JSON.stringify(normalizedCourseAccess)
  ]);

  const storedRow = await db.get("SELECT * FROM users WHERE email = ?", email);
  return mapUserRow(storedRow);
}

async function migrateLegacyData(db) {
  const usersCount = await db.get("SELECT COUNT(*) AS count FROM users");
  if (Number(usersCount?.count || 0) === 0) {
    const users = await readJsonFile(USERS_FILE);
    for (const user of users) {
      await upsertUserInDatabase(db, user);
    }
  }

  const otpCount = await db.get("SELECT COUNT(*) AS count FROM otp_codes");
  if (Number(otpCount?.count || 0) === 0) {
    const otpRecords = await readJsonFile(OTP_FILE);
    for (const record of otpRecords) {
      await db.run(`
        INSERT INTO otp_codes (email, purpose, otp_hash, expires_at, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (email) DO UPDATE SET
          purpose = EXCLUDED.purpose,
          otp_hash = EXCLUDED.otp_hash,
          expires_at = EXCLUDED.expires_at,
          created_at = EXCLUDED.created_at
      `, [
        normalizeEmail(record.email),
        record.purpose || "verify-email",
        record.otpHash || "",
        normalizeTimestamp(record.expiresAt, new Date().toISOString()),
        normalizeTimestamp(record.createdAt, new Date().toISOString())
      ]);
    }
  }

  const sessionCount = await db.get("SELECT COUNT(*) AS count FROM sessions");
  if (Number(sessionCount?.count || 0) === 0) {
    const storedSessions = await readJsonFile(SESSIONS_FILE);
    for (const [sessionId, sessionUser] of Object.entries(storedSessions || {})) {
      if (!sessionId || !sessionUser || typeof sessionUser !== "object") continue;
      await db.run(`
        INSERT INTO sessions (session_id, user_email, user_json, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (session_id) DO UPDATE SET
          user_email = EXCLUDED.user_email,
          user_json = EXCLUDED.user_json,
          created_at = EXCLUDED.created_at
      `, [
        sessionId,
        normalizeEmail(sessionUser.email),
        JSON.stringify(sessionUser),
        normalizeTimestamp(sessionUser.createdAt, new Date().toISOString())
      ]);
    }
  }

  const waitlistCount = await db.get("SELECT COUNT(*) AS count FROM waitlist");
  if (Number(waitlistCount?.count || 0) === 0) {
    const waitlistEntries = await readJsonFile(WAITLIST_FILE);
    for (const entry of waitlistEntries) {
      await db.run(`
        INSERT INTO waitlist (name, email, phone, school_percentile, category, submitted_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (email) DO NOTHING
      `, [
        entry.name || "",
        normalizeEmail(entry.email),
        entry.phone || "",
        Number(entry.schoolPercentile || 0),
        entry.category || "",
        normalizeTimestamp(entry.submittedAt, new Date().toISOString())
      ]);
    }
  }

  const paymentsCount = await db.get("SELECT COUNT(*) AS count FROM payments");
  if (Number(paymentsCount?.count || 0) === 0) {
    const payments = await readJsonFile(PAYMENTS_FILE);
    for (const payment of payments) {
      await db.run(`
        INSERT INTO payments (
          order_id,
          plan_id,
          plan_label,
          amount,
          currency,
          payment_id,
          payment_mode,
          bank_txn_id,
          email,
          name,
          verified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO NOTHING
      `, [
        payment.orderId || "",
        payment.planId || "",
        payment.planLabel || "",
        Number(payment.amount || 0),
        payment.currency || "INR",
        payment.paymentId || "",
        payment.paymentMode || "",
        payment.bankTxnId || "",
        normalizeEmail(payment.email),
        payment.name || "",
        normalizeTimestamp(payment.verifiedAt, new Date().toISOString())
      ]);
    }
  }
}

function summarizeProfileStatus(user) {
  const fields = [user?.phone, user?.schoolPercentile, user?.category].filter((value) => value !== null && value !== undefined && String(value).trim() !== "");
  return {
    completedFields: fields.length,
    isComplete: fields.length === 3
  };
}

function getAdminUserRecord(user) {
  const publicUser = getPublicUser(user);
  const profileStatus = summarizeProfileStatus(user);
  return {
    ...publicUser,
    profileStatus
  };
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isValidPassword(value) {
  return typeof value === "string" && value.length >= 8;
}

function generateOtpCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

async function hashSecret(value) {
  const salt = crypto.randomBytes(16).toString("hex");
  const derivedKey = await scryptAsync(value, salt, 64);
  return `${salt}:${derivedKey.toString("hex")}`;
}

async function verifySecret(value, storedHash) {
  const [salt, key] = String(storedHash || "").split(":");
  if (!salt || !key) return false;
  const derivedKey = await scryptAsync(value, salt, 64);
  return crypto.timingSafeEqual(Buffer.from(key, "hex"), derivedKey);
}

async function findUserByEmail(email) {
  const db = await getDatabase();
  const row = await db.get("SELECT * FROM users WHERE email = ?", normalizeEmail(email));
  return mapUserRow(row);
}

async function upsertUser(nextUser) {
  const db = await getDatabase();
  return upsertUserInDatabase(db, nextUser);
}

async function createOtpRecord(email, purpose = "verify-email") {
  const otpCode = generateOtpCode();
  const otpHash = await hashSecret(otpCode);
  const normalizedEmail = normalizeEmail(email);
  const db = await getDatabase();
  await db.run(`
    INSERT INTO otp_codes (email, purpose, otp_hash, expires_at, created_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(email) DO UPDATE SET
      purpose = excluded.purpose,
      otp_hash = excluded.otp_hash,
      expires_at = excluded.expires_at,
      created_at = excluded.created_at
  `, [
    normalizedEmail,
    purpose,
    otpHash,
    new Date(Date.now() + EMAIL_OTP_EXPIRY_MINUTES * 60 * 1000).toISOString(),
    new Date().toISOString()
  ]);
  return otpCode;
}

async function verifyOtpRecord(email, otp) {
  const normalizedEmail = normalizeEmail(email);
  const db = await getDatabase();
  const existingRecord = await db.get("SELECT * FROM otp_codes WHERE email = ?", normalizedEmail);

  if (!existingRecord) {
    return { ok: false, message: "No OTP request found for this email." };
  }

  if (new Date(existingRecord.expires_at).getTime() < Date.now()) {
    await db.run("DELETE FROM otp_codes WHERE email = ?", normalizedEmail);
    return { ok: false, message: "This OTP has expired. Please request a fresh one." };
  }

  const isValid = await verifySecret(String(otp || ""), existingRecord.otp_hash);
  if (!isValid) {
    return { ok: false, message: "Invalid OTP. Please try again." };
  }

  await db.run("DELETE FROM otp_codes WHERE email = ?", normalizedEmail);
  return { ok: true, purpose: existingRecord.purpose };
}

async function sendOtpEmail(email, otpCode) {
  const subject = "Your IMBA Beacon verification code";
  const text = `Your IMBA Beacon verification code is ${otpCode}. It will expire in ${EMAIL_OTP_EXPIRY_MINUTES} minutes.`;
  const html = `<p>Your IMBA Beacon verification code is <strong>${otpCode}</strong>.</p><p>It will expire in ${EMAIL_OTP_EXPIRY_MINUTES} minutes.</p>`;

  if (RESEND_API_KEY && (RESEND_FROM || EMAIL_FROM)) {
    const resendResponse = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from: RESEND_FROM || EMAIL_FROM,
        to: [email],
        subject,
        text,
        html
      })
    });

    const resendPayload = await resendResponse.json().catch(() => null);
    if (!resendResponse.ok) {
      const resendMessage = resendPayload?.message || resendPayload?.error || "Unable to send OTP using Resend.";
      throw new Error(resendMessage);
    }

    return;
  }

  if (!otpTransporter || !EMAIL_FROM) {
    throw new Error("Email OTP is not configured yet. Add RESEND_API_KEY and RESEND_FROM, or valid SMTP settings on the server.");
  }

  await otpTransporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject,
    text,
    html
  });
}

function getRazorpayClient() {
  if (!RAZORPAY_KEY_ID || !RAZORPAY_KEY_SECRET) {
    return null;
  }

  if (!razorpayClient) {
    razorpayClient = new Razorpay({
      key_id: RAZORPAY_KEY_ID,
      key_secret: RAZORPAY_KEY_SECRET
    });
  }

  return razorpayClient;
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

function buildClientConfig() {
  return {
    appBaseUrl: APP_BASE_URL,
    frontendOrigin: FRONTEND_ORIGIN,
    googleClientId: GOOGLE_CLIENT_ID,
    authProviders: {
      google: Boolean(GOOGLE_CLIENT_ID),
      beaconPassword: true,
      emailOtp: true
    },
    paymentsEnabled: PAYMENTS_ENABLED,
    paymentProvider: "razorpay",
    razorpayKeyId: RAZORPAY_KEY_ID,
    plans: Object.values(PLAN_CATALOG).map((plan) => ({
      id: plan.id,
      label: plan.label,
      amount: plan.amount,
      currency: plan.currency,
      description: plan.description
    }))
  };
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

    const db = await getDatabase();
    const existingEntry = await db.get("SELECT email FROM waitlist WHERE email = ?", email);

    if (existingEntry) {
      sendJson(response, 409, { message: "This email is already on the waitlist." });
      return;
    }

    await db.run(`
      INSERT INTO waitlist (name, email, phone, school_percentile, category, submitted_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `, [
      name,
      email,
      phone,
      Number(schoolPercentile),
      category,
      new Date().toISOString()
    ]);

    sendJson(response, 201, {
      message: "Thanks! Your details have been received and you're on the IMBA Beacon waitlist now."
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to process request." });
  }
}

async function handleWaitlistList(response) {
  try {
    const db = await getDatabase();
    const waitlist = (await db.all(`
      SELECT name, email, phone, school_percentile, category, submitted_at
      FROM waitlist
      ORDER BY submitted_at DESC
    `)).map(mapWaitlistRow);
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
    const db = await getDatabase();
    const waitlist = (await db.all(`
      SELECT name, email, phone, school_percentile, category, submitted_at
      FROM waitlist
      ORDER BY submitted_at DESC
    `)).map(mapWaitlistRow);
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
    const db = await getDatabase();
    const waitlist = (await db.all(`
      SELECT name, email, phone, school_percentile, category, submitted_at
      FROM waitlist
      ORDER BY submitted_at DESC
    `)).map(mapWaitlistRow);
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

async function handleAdminUsers(request, response, url, corsHeaders) {
  if (!requireAdminAccess(request, response, url, corsHeaders)) return;

  try {
    const db = await getDatabase();
    const users = await db.all("SELECT * FROM users");
    const entries = users
      .map((row) => getAdminUserRecord(mapUserRow(row)))
      .sort((left, right) => new Date(right.createdAt || 0).getTime() - new Date(left.createdAt || 0).getTime());

    sendJson(response, 200, {
      count: entries.length,
      entries
    }, corsHeaders);
  } catch (error) {
    sendJson(response, 500, { message: "Unable to read user access data." }, corsHeaders);
  }
}

async function handleAdminUserAccessUpdate(request, response, url, corsHeaders) {
  if (!requireAdminAccess(request, response, url, corsHeaders)) return;

  try {
    const body = await parseRequestBody(request);
    const email = normalizeEmail(body.email);
    const purchased = Boolean(body.purchased);
    const planId = body.planId === "basic" || body.planId === "pro" ? body.planId : null;
    const requestedFeatureAccess = body.featureAccess && typeof body.featureAccess === "object" ? body.featureAccess : {};

    if (!isValidEmail(email)) {
      sendJson(response, 400, { message: "Provide a valid user email." }, corsHeaders);
      return;
    }

    if (purchased && !planId) {
      sendJson(response, 400, { message: "Select Basic or Pro before granting paid course access." }, corsHeaders);
      return;
    }

    const existingUser = await findUserByEmail(email);
    if (!existingUser) {
      sendJson(response, 404, { message: "No Beacon user found for this email." }, corsHeaders);
      return;
    }

    const nextCourseAccess = {
      purchased,
      planId,
      grantedAt: purchased ? (existingUser.courseAccess?.grantedAt || new Date().toISOString()) : "",
      featureAccess: PREMIUM_FEATURE_KEYS.reduce((accumulator, key) => {
        accumulator[key] = typeof requestedFeatureAccess[key] === "boolean"
          ? requestedFeatureAccess[key]
          : purchased;
        return accumulator;
      }, {})
    };

    const persistedUser = await upsertUser({
      ...existingUser,
      courseAccess: nextCourseAccess
    });

    const sessionEntries = Array.from(sessionStore.entries());
    sessionEntries.forEach(([sessionId, sessionUser]) => {
      if (normalizeEmail(sessionUser.email) === email) {
        sessionStore.set(sessionId, persistedUser);
      }
    });
    await saveSessionStore();

    sendJson(response, 200, {
      message: "User access updated successfully.",
      user: getAdminUserRecord(persistedUser)
    }, corsHeaders);
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to update user access." }, corsHeaders);
  }
}

async function handleAdminQuizAttempts(request, response, url, corsHeaders) {
  if (!requireAdminAccess(request, response, url, corsHeaders)) return;

  try {
    const rows = await database.all(
      `SELECT email, quiz_id, quiz_title, set_id, score, total, percent, submitted_at
       FROM quiz_attempts
       ORDER BY submitted_at DESC`
    );

    sendJson(response, 200, {
      attempts: rows.map((row) => ({
        email: row.email,
        quizId: row.quiz_id,
        quizTitle: row.quiz_title,
        setId: row.set_id,
        score: Number(row.score || 0),
        total: Number(row.total || 0),
        percent: Number(row.percent || 0),
        submittedAt: row.submitted_at
      }))
    }, corsHeaders);
  } catch (error) {
    console.error("Admin quiz attempts failed:", error);
    sendJson(response, 500, { message: "Unable to read quiz attempts." }, corsHeaders);
  }
}

function handleConfig(response) {
  sendJson(response, 200, buildClientConfig());
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

    const persistedUser = await upsertUser({
      googleId: payload.sub,
      name: payload.name || payload.email,
      email: payload.email,
      picture: payload.picture || "",
      provider: "google",
      emailVerified: true,
      createdAt: new Date().toISOString(),
      lastLoginAt: new Date().toISOString()
    });

    const sessionId = await createSession(persistedUser);

    sendJson(
      response,
      200,
      {
        message: "Google sign-in successful.",
        user: getPublicUser(sessionStore.get(sessionId)),
        sessionToken: sessionId
      },
      {
        "Set-Cookie": createSessionCookie(request, sessionId)
      }
    );
  } catch (error) {
    sendJson(response, 401, { message: "Unable to verify Google sign-in." });
  }
}

async function handleBeaconSignup(request, response) {
  try {
    const body = await parseRequestBody(request);
    const name = String(body.name || "").trim();
    const email = normalizeEmail(body.email);
    const password = String(body.password || "");
    const confirmPassword = String(body.confirmPassword || "");
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

    if (!isValidPassword(password)) {
      sendJson(response, 400, { message: "Create a password with at least 8 characters." });
      return;
    }

    if (password !== confirmPassword) {
      sendJson(response, 400, { message: "Passwords do not match." });
      return;
    }

    if (phone && !isValidPhone(phone)) {
      sendJson(response, 400, { message: "Please enter a valid 10-digit phone number." });
      return;
    }

    if (schoolPercentile && !isValidPercentile(schoolPercentile)) {
      sendJson(response, 400, { message: "Please enter a valid school percentile between 0 and 100." });
      return;
    }

    const existingUser = await findUserByEmail(email);
    if (existingUser && existingUser.emailVerified) {
      sendJson(response, 409, { message: "An account already exists for this email. Please sign in instead." });
      return;
    }

    const passwordHash = await hashSecret(password);
    const now = new Date().toISOString();
    await upsertUser({
      ...(existingUser || {}),
      name,
      email,
      passwordHash,
      phone,
      schoolPercentile: schoolPercentile ? Number(schoolPercentile) : null,
      category,
      picture: existingUser?.picture || "",
      provider: "beacon",
      emailVerified: false,
      createdAt: existingUser?.createdAt || now,
      lastLoginAt: existingUser?.lastLoginAt || null
    });

    const otpCode = await createOtpRecord(email, "verify-email");
    await sendOtpEmail(email, otpCode);

    sendJson(response, 201, {
      message: existingUser
        ? "A fresh verification OTP has been sent to your email. Verify it to activate your Beacon account."
        : "Beacon account created. Please verify the OTP sent to your email before signing in."
    });
  } catch (error) {
    console.error("Beacon signup failed:", error);
    sendJson(response, 400, {
      message: error && error.message ? error.message : "Unable to create Beacon account."
    });
  }
}

async function handlePasswordLogin(request, response) {
  try {
    const body = await parseRequestBody(request);
    const email = normalizeEmail(body.email);
    const password = String(body.password || "");

    if (!isValidEmail(email) || !password) {
      sendJson(response, 400, { message: "Please enter your email and password." });
      return;
    }

    const user = await findUserByEmail(email);
    if (!user || !user.passwordHash) {
      sendJson(response, 401, { message: "No Beacon account found for this email." });
      return;
    }

    const passwordOk = await verifySecret(password, user.passwordHash);
    if (!passwordOk) {
      sendJson(response, 401, { message: "Incorrect password." });
      return;
    }

    if (!user.emailVerified) {
      sendJson(response, 403, { message: "Your Beacon account is not verified yet. Request an email OTP to continue." });
      return;
    }

    const persistedUser = await upsertUser({
      ...user,
      lastLoginAt: new Date().toISOString()
    });

    const sessionId = await createSession(persistedUser);
    sendJson(response, 200, {
      message: "Logged in successfully.",
      user: getPublicUser(persistedUser),
      sessionToken: sessionId
    }, {
      "Set-Cookie": createSessionCookie(request, sessionId)
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to log in." });
  }
}

async function handleRequestOtp(request, response) {
  try {
    const body = await parseRequestBody(request);
    const email = normalizeEmail(body.email);

    if (!isValidEmail(email)) {
      sendJson(response, 400, { message: "Please enter a valid email address." });
      return;
    }

    const user = await findUserByEmail(email);
    if (!user) {
      sendJson(response, 404, { message: "No Beacon account found for this email. Please sign up first." });
      return;
    }

    const otpCode = await createOtpRecord(email, user.emailVerified ? "login" : "verify-email");
    await sendOtpEmail(email, otpCode);

    sendJson(response, 200, {
      message: "A verification OTP has been sent to your email."
    });
  } catch (error) {
    console.error("OTP request failed:", error);
    sendJson(response, 400, {
      message: error && error.message ? error.message : "Unable to send OTP right now."
    });
  }
}

async function handleVerifyOtp(request, response) {
  try {
    const body = await parseRequestBody(request);
    const email = normalizeEmail(body.email);
    const otp = String(body.otp || "").trim();

    if (!isValidEmail(email) || otp.length < 4) {
      sendJson(response, 400, { message: "Enter a valid email and OTP." });
      return;
    }

    const user = await findUserByEmail(email);
    if (!user) {
      sendJson(response, 404, { message: "No Beacon account found for this email." });
      return;
    }

    const otpResult = await verifyOtpRecord(email, otp);
    if (!otpResult.ok) {
      sendJson(response, 401, { message: otpResult.message });
      return;
    }

    const persistedUser = await upsertUser({
      ...user,
      emailVerified: true,
      lastLoginAt: new Date().toISOString()
    });

    const sessionId = await createSession(persistedUser);
    sendJson(response, 200, {
      message: otpResult.purpose === "verify-email"
        ? "Email verified successfully. Your Beacon account is now active."
        : "OTP verified successfully. You are now signed in.",
      user: getPublicUser(persistedUser),
      sessionToken: sessionId
    }, {
      "Set-Cookie": createSessionCookie(request, sessionId)
    });
  } catch (error) {
    console.error("OTP verification failed:", error);
    sendJson(response, 400, {
      message: error && error.message ? error.message : "Unable to verify OTP."
    });
  }
}

function handleSession(request, response) {
  sendJson(response, 200, {
    user: getPublicUser(getSessionUser(request))
  });
}

async function handleLogout(request, response) {
  await destroySession(request);
  sendJson(
    response,
    200,
    { message: "Logged out successfully." },
    { "Set-Cookie": clearSessionCookie(request) }
  );
}

function requireAuthenticatedUser(request, response, corsHeaders = {}) {
  const user = getSessionUser(request);
  if (!user) {
    sendJson(response, 401, { message: "Please sign in to your Beacon account first." }, corsHeaders);
    return null;
  }
  return user;
}

async function handleProfileUpdate(request, response) {
  const sessionUser = requireAuthenticatedUser(request, response);
  if (!sessionUser) return;

  try {
    const body = await parseRequestBody(request);
    const name = String(body.name || "").trim();
    const phone = String(body.phone || "").trim();
    const schoolPercentile = String(body.schoolPercentile || "").trim();
    const category = String(body.category || "").trim();

    if (name.length < 2) {
      sendJson(response, 400, { message: "Please enter your full name." });
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

    const persistedUser = await upsertUser({
      ...sessionUser,
      name,
      phone,
      schoolPercentile: Number(schoolPercentile),
      category
    });

    const cookies = parseCookies(request);
    const authHeader = request.headers.authorization || "";
    const bearerSessionId = authHeader.startsWith("Bearer ") ? authHeader.slice("Bearer ".length).trim() : "";
    const sessionId = bearerSessionId || cookies[SESSION_COOKIE];
    if (sessionId) {
      sessionStore.set(sessionId, persistedUser);
      await saveSession(sessionId, persistedUser);
    }

    sendJson(response, 200, {
      message: "Profile updated successfully.",
      user: getPublicUser(persistedUser)
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to update profile." });
  }
}

async function handleFreeSessionRegistration(request, response) {
  const sessionUser = requireAuthenticatedUser(request, response);
  if (!sessionUser) return;

  try {
    const body = await parseRequestBody(request);
    const sessionType = normalizeFreeSessionType(body.sessionType);
    const name = String(body.name || "").trim();
    const phone = String(body.phone || "").trim();
    const schoolPercentile = String(body.schoolPercentile || "").trim();
    const email = normalizeEmail(sessionUser.email);

    if (!sessionType) {
      sendJson(response, 400, { message: "Please select a valid free session." });
      return;
    }

    if (name.length < 2) {
      sendJson(response, 400, { message: "Please enter your full name." });
      return;
    }

    if (!isValidPhone(phone)) {
      sendJson(response, 400, { message: "Please enter a valid 10-digit phone number." });
      return;
    }

    if (!isValidPercentile(schoolPercentile)) {
      sendJson(response, 400, { message: "Please enter a valid JEE percentile between 0 and 100." });
      return;
    }

    const db = await getDatabase();
    const sessionLabel = FREE_SESSION_TYPES[sessionType];
    const registeredAt = new Date().toISOString();

    await db.run(`
      INSERT INTO free_session_registrations (
        session_type,
        session_label,
        name,
        email,
        phone,
        school_percentile,
        registered_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (session_type, email) DO UPDATE SET
        session_label = excluded.session_label,
        name = excluded.name,
        phone = excluded.phone,
        school_percentile = excluded.school_percentile,
        registered_at = excluded.registered_at
    `, [
      sessionType,
      sessionLabel,
      name,
      email,
      phone,
      Number(schoolPercentile),
      registeredAt
    ]);

    const persistedUser = await upsertUser({
      ...sessionUser,
      name,
      phone,
      schoolPercentile: Number(schoolPercentile)
    });

    const cookies = parseCookies(request);
    const sessionId = cookies[SESSION_COOKIE];
    if (sessionId) {
      sessionStore.set(sessionId, persistedUser);
      await saveSessionStore();
    }

    sendJson(response, 200, {
      message: `Registration confirmed for ${sessionLabel}. The session link will be uploaded one hour prior to the session on the website and will also be sent through email.`,
      registration: {
        sessionType,
        sessionLabel,
        registeredAt
      },
      user: getPublicUser(persistedUser)
    });
  } catch (error) {
    sendJson(response, 400, { message: error.message || "Unable to complete registration." });
  }
}

async function handleQuizAttemptSave(request, response, corsHeaders = {}) {
  const sessionUser = requireAuthenticatedUser(request, response, corsHeaders);
  if (!sessionUser) return;

  try {
    const body = await parseRequestBody(request);
    const quizId = String(body.quizId || "").trim();
    const quizTitle = String(body.quizTitle || "").trim();
    const setId = String(body.setId || "").trim().toUpperCase();
    const score = Number(body.score);
    const total = Number(body.total);
    const percent = Number(body.percent);
    const answers = body.answers && typeof body.answers === "object" ? body.answers : {};

    if (!quizId || !quizTitle || !setId) {
      sendJson(response, 400, { message: "Missing quiz attempt details." }, corsHeaders);
      return;
    }

    if (!Number.isInteger(score) || !Number.isInteger(total) || total <= 0 || score < 0 || score > total) {
      sendJson(response, 400, { message: "Invalid quiz score." }, corsHeaders);
      return;
    }

    const boundedPercent = Number.isFinite(percent)
      ? Math.max(0, Math.min(100, Math.round(percent)))
      : Math.round((score / total) * 100);
    const submittedAt = new Date().toISOString();

    await database.run(
      `INSERT INTO quiz_attempts (
        email,
        quiz_id,
        quiz_title,
        set_id,
        score,
        total,
        percent,
        answers_json,
        submitted_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
      ON CONFLICT (email, quiz_id, set_id)
      DO UPDATE SET
        quiz_title = EXCLUDED.quiz_title,
        score = EXCLUDED.score,
        total = EXCLUDED.total,
        percent = EXCLUDED.percent,
        answers_json = EXCLUDED.answers_json,
        submitted_at = EXCLUDED.submitted_at`,
      normalizeEmail(sessionUser.email),
      quizId,
      quizTitle,
      setId,
      score,
      total,
      boundedPercent,
      JSON.stringify(answers),
      submittedAt
    );

    sendJson(response, 200, {
      message: "Quiz attempt saved.",
      attempt: {
        quizId,
        quizTitle,
        setId,
        score,
        total,
        percent: boundedPercent,
        submittedAt
      }
    }, corsHeaders);
  } catch (error) {
    console.error("Quiz attempt save failed:", error);
    sendJson(response, 500, { message: "Unable to save quiz attempt." }, corsHeaders);
  }
}

async function handleQuizAttemptsList(request, response, corsHeaders = {}) {
  const sessionUser = requireAuthenticatedUser(request, response, corsHeaders);
  if (!sessionUser) return;

  try {
    const rows = await database.all(
      `SELECT quiz_id, quiz_title, set_id, score, total, percent, submitted_at
       FROM quiz_attempts
       WHERE email = ?
       ORDER BY submitted_at DESC`,
      normalizeEmail(sessionUser.email)
    );

    sendJson(response, 200, {
      attempts: rows.map((row) => ({
        quizId: row.quiz_id,
        quizTitle: row.quiz_title,
        setId: row.set_id,
        score: Number(row.score || 0),
        total: Number(row.total || 0),
        percent: Number(row.percent || 0),
        submittedAt: row.submitted_at
      }))
    }, corsHeaders);
  } catch (error) {
    console.error("Quiz attempts list failed:", error);
    sendJson(response, 500, { message: "Unable to load quiz progress." }, corsHeaders);
  }
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

  const razorpay = getRazorpayClient();
  if (!razorpay) {
    sendJson(response, 503, {
      message: "Razorpay is not configured yet. Add RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET."
    });
    return;
  }

  try {
    const body = await parseRequestBody(request);
    const planId = String(body.planId || "").toLowerCase();
    const plan = planId ? PLAN_CATALOG[planId] : null;
    const coupon = await resolveCoupon(plan, body.couponCode);

    if (planId && !plan) {
      sendJson(response, 400, { message: "Invalid plan selected." });
      return;
    }

    if (coupon.code && !coupon.applied) {
      sendJson(response, 400, { message: coupon.message || "Invalid coupon code." });
      return;
    }

    const requestedAmount = plan
      ? (coupon.applied ? coupon.finalAmount : plan.amount)
      : Number.parseInt(String(body.amount || ""), 10);
    const amount = Number.isFinite(requestedAmount) ? requestedAmount : NaN;
    const currency = String(plan?.currency || body.currency || "INR").trim().toUpperCase() || "INR";
    const fallbackReceipt = plan ? `IMBA_${plan.id}_${Date.now()}` : `IMBA_${Date.now()}`;
    const receipt = String(body.receipt || fallbackReceipt)
      .trim()
      .replace(/[^a-zA-Z0-9_-]/g, "")
      .slice(0, 40) || fallbackReceipt.slice(0, 40);

    if (!Number.isInteger(amount) || amount < 100) {
      sendJson(response, 400, { message: "Amount must be at least 100 paise." });
      return;
    }

    const order = await razorpay.orders.create({
      amount,
      currency,
      receipt,
      notes: {
        email: normalizeEmail(user.email),
        name: user.name || "",
        planId: plan?.id || "",
        couponCode: coupon.applied ? coupon.code : "",
        discountPercent: coupon.applied ? String(coupon.discountPercent) : "",
        originalAmount: coupon.applied ? String(coupon.originalAmount) : "",
        discountAmount: coupon.applied ? String(coupon.discountAmount) : "",
        source: "imba-beacon"
      }
    });

    sendJson(response, 201, {
      order_id: order.id,
      amount: order.amount,
      currency: order.currency,
      receipt: order.receipt,
      plan: {
        id: plan?.id || "",
        label: plan?.label || "",
        amount: plan?.amount || amount,
        currency: plan?.currency || currency,
        amountDisplay: ((plan?.amount || amount) / 100).toFixed(2)
      },
      coupon: coupon.applied
        ? {
            applied: true,
            code: coupon.code,
            discountPercent: coupon.discountPercent,
            originalAmount: coupon.originalAmount,
            discountAmount: coupon.discountAmount,
            finalAmount: coupon.finalAmount,
            originalAmountDisplay: formatAmountDisplay(coupon.originalAmount, currency),
            finalAmountDisplay: formatAmountDisplay(coupon.finalAmount, currency)
          }
        : {
            applied: false
          }
    });
  } catch (error) {
    console.error("Razorpay order creation failed:", error);
    const statusCode = error?.statusCode === 401 ? 401 : 500;
    sendJson(response, statusCode, {
      message: statusCode === 401
        ? "Razorpay authentication failed. Check the configured key ID and key secret."
        : (error.message || "Unable to create Razorpay order.")
    });
  }
}

function verifyRazorpaySignature(orderId, paymentId, signature) {
  if (!orderId || !paymentId || !signature || !RAZORPAY_KEY_SECRET) {
    return false;
  }

  const expectedSignature = crypto
    .createHmac("sha256", RAZORPAY_KEY_SECRET)
    .update(`${orderId}|${paymentId}`)
    .digest("hex");

  if (expectedSignature.length !== signature.length) {
    return false;
  }

  return crypto.timingSafeEqual(
    Buffer.from(expectedSignature, "utf8"),
    Buffer.from(signature, "utf8")
  );
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

  const razorpay = getRazorpayClient();
  if (!razorpay) {
    sendJson(response, 503, { message: "Razorpay verification is not configured yet." });
    return;
  }

  try {
    const body = await parseRequestBody(request);
    const orderId = String(body.razorpay_order_id || body.order_id || "").trim();
    const paymentId = String(body.razorpay_payment_id || body.payment_id || "").trim();
    const signature = String(body.razorpay_signature || body.signature || "").trim();
    const hintedPlanId = String(body.planId || "").toLowerCase();

    if (!orderId || !paymentId || !signature) {
      sendJson(response, 400, { message: "Missing payment verification details." });
      return;
    }

    if (!verifyRazorpaySignature(orderId, paymentId, signature)) {
      sendJson(response, 400, { message: "Payment signature mismatch. Do not mark this order as paid." });
      return;
    }

    const razorpayOrder = await razorpay.orders.fetch(orderId);
    const resolvedPlanId = PLAN_CATALOG[hintedPlanId]
      ? hintedPlanId
      : String(razorpayOrder?.notes?.planId || "").toLowerCase();
    const plan = PLAN_CATALOG[resolvedPlanId] || null;
    const amount = Number(razorpayOrder?.amount || plan?.amount || 0);
    const currency = String(razorpayOrder?.currency || plan?.currency || "INR");

    const db = await getDatabase();
    const existingPayment = await db.get("SELECT order_id FROM payments WHERE order_id = ?", orderId);

    if (!existingPayment) {
      await db.run(`
        INSERT INTO payments (
          order_id,
          plan_id,
          plan_label,
          amount,
          currency,
          payment_id,
          payment_mode,
          bank_txn_id,
          email,
          name,
          verified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        orderId,
        plan?.id || "",
        plan?.label || "Custom",
        amount,
        currency,
        paymentId,
        "Razorpay Standard Checkout",
        "",
        normalizeEmail(user.email),
        user.name,
        new Date().toISOString()
      ]);
    }

    const updatedUser = plan ? await activatePlanForUser(user, plan.id) : user;

    // Increment coupon usage if one was applied
    const usedCouponCode = normalizeCouponCode(String(razorpayOrder?.notes?.couponCode || ""));
    if (usedCouponCode && usedCouponCode === DEFAULT_COUPON_CODE) {
      try {
        await db.run(
          "UPDATE coupons SET used_count = used_count + 1 WHERE code = ?",
          [usedCouponCode]
        );
      } catch (couponError) {
        console.error("Failed to increment coupon usage:", couponError);
      }
    }

    sendJson(response, 200, {
      message: plan
        ? `Payment verified successfully. Your ${plan.label} plan is now active.`
        : "Payment verified successfully.",
      user: getPublicUser(updatedUser),
      plan: plan ? {
        id: plan.id,
        label: plan.label,
        amount: plan.amount,
        currency: plan.currency
      } : null
    });
  } catch (error) {
    console.error("Razorpay payment verification failed:", error);
    const statusCode = error?.statusCode === 401 ? 401 : 500;
    sendJson(response, statusCode, {
      message: statusCode === 401
        ? "Razorpay authentication failed. Check the configured key ID and key secret."
        : (error.message || "Unable to verify payment.")
    });
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
    sendJson(response, 200, buildClientConfig(), corsHeaders);
    return;
  }

  if (request.method === "GET" && pathname.startsWith("/protected/")) {
    await serveProtectedContent(request, response, url);
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/google") {
    await handleGoogleLogin(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/signup") {
    await handleBeaconSignup(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/login/password") {
    await handlePasswordLogin(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/profile") {
    await handleProfileUpdate(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/free-session/register") {
    await handleFreeSessionRegistration(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/otp/request") {
    await handleRequestOtp(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && pathname === "/api/auth/otp/verify") {
    await handleVerifyOtp(request, {
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
    await destroySession(request);
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

  if (request.method === "POST" && pathname === "/api/apply-coupon") {
    const user = requireAuthenticatedUser(request, response);
    if (!user) return;
    try {
      const body = await parseRequestBody(request);
      const planId = String(body.planId || "pro").toLowerCase();
      const plan = PLAN_CATALOG[planId] || PLAN_CATALOG.pro;
      const coupon = await resolveCoupon(plan, body.code);
      if (!coupon.applied) {
        sendJson(response, 400, { valid: false, message: coupon.message || "Invalid coupon code." }, corsHeaders);
      } else {
        sendJson(response, 200, {
          valid: true,
          discountPercent: coupon.discountPercent,
          discountAmount: coupon.discountAmount,
          finalAmount: coupon.finalAmount,
          remaining: coupon.remaining
        }, corsHeaders);
      }
    } catch (error) {
      sendJson(response, 500, { valid: false, message: "Could not validate coupon." }, corsHeaders);
    }
    return;
  }

  if (request.method === "GET" && pathname === "/api/quiz-attempts") {
    await handleQuizAttemptsList(request, response, corsHeaders);
    return;
  }

  if (request.method === "POST" && pathname === "/api/quiz-attempts") {
    await handleQuizAttemptSave(request, response, corsHeaders);
    return;
  }

  if (request.method === "POST" && (pathname === "/api/create-order" || pathname === "/api/payments/order")) {
    await handleCreateOrder(request, {
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
    return;
  }

  if (request.method === "POST" && (pathname === "/api/verify-payment" || pathname === "/api/payments/verify")) {
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
    await handleWaitlistList({
      writeHead: (...args) => response.writeHead(args[0], { ...args[1], ...corsHeaders }),
      end: (...args) => response.end(...args)
    });
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

  if (request.method === "GET" && pathname === "/api/admin/users") {
    await handleAdminUsers(request, response, url, corsHeaders);
    return;
  }

  if (request.method === "POST" && pathname === "/api/admin/users/access") {
    await handleAdminUserAccessUpdate(request, response, url, corsHeaders);
    return;
  }

  if (request.method === "GET" && pathname === "/api/admin/quiz-attempts") {
    await handleAdminQuizAttempts(request, response, url, corsHeaders);
    return;
  }

  if (request.method !== "GET" && request.method !== "HEAD") {
    sendJson(response, 405, { message: "Method not allowed." }, corsHeaders);
    return;
  }

  sendJson(response, 404, { message: "API route not found." }, corsHeaders);
}

async function startServer() {
  await ensureDataStore();
  await getDatabase();
  await loadSessionStore();

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
