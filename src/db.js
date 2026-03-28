const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, '..', 'data');
const DATA_FILE = path.join(DATA_DIR, 'va_records.enc');
const WD_FILE = path.join(DATA_DIR, 'withdrawals.enc');
const USERS_FILE = path.join(DATA_DIR, 'users.enc');
const CONFIG_FILE = path.join(DATA_DIR, 'config.enc');
const BALANCE_HISTORY_FILE = path.join(DATA_DIR, 'balance_history.enc');
const USER_BALANCE_HISTORY_FILE = path.join(DATA_DIR, 'user_balance_history.enc');
const IBFT_HISTORY_FILE = path.join(DATA_DIR, 'ibft_history.enc');
const KEY_FILE = path.join(DATA_DIR, 'db.key');

function deriveKey(secret) {
  return crypto.createHash('sha256').update(String(secret)).digest('base64').substr(0, 32);
}

function readKeyFile() {
  try {
    if (!fs.existsSync(KEY_FILE)) return '';
    return String(fs.readFileSync(KEY_FILE, 'utf8') || '').trim();
  } catch (_) {
    return '';
  }
}

function writeKeyFile(secret) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(KEY_FILE, String(secret || '').trim(), 'utf8');
  } catch (_) {}
}

function shouldAssumeExistingStore() {
  try {
    return fs.existsSync(USERS_FILE) || fs.existsSync(DATA_FILE) || fs.existsSync(WD_FILE) || fs.existsSync(CONFIG_FILE);
  } catch (_) {
    return false;
  }
}

function selectEncryptionSecret() {
  const envSecret = String(process.env.DB_ENCRYPTION_KEY || '').trim();
  if (envSecret) return envSecret;
  const fileSecret = readKeyFile();
  if (fileSecret) return fileSecret;
  if (shouldAssumeExistingStore()) return 'DEFAULT_BOT_SECRET_KEY_123';
  const generated = crypto.randomBytes(32).toString('hex');
  writeKeyFile(generated);
  return generated;
}

const ENCRYPTION_SECRET = selectEncryptionSecret();
if (!String(process.env.DB_ENCRYPTION_KEY || '').trim()) {
  const fileSecret = readKeyFile();
  if (!fileSecret) writeKeyFile(ENCRYPTION_SECRET);
}

const ENCRYPTION_KEY = deriveKey(ENCRYPTION_SECRET);

function getDecryptKeys() {
  const keys = [];
  const envSecret = String(process.env.DB_ENCRYPTION_KEY || '').trim();
  const fileSecret = readKeyFile();
  if (envSecret) keys.push(deriveKey(envSecret));
  if (fileSecret) keys.push(deriveKey(fileSecret));
  keys.push(deriveKey('DEFAULT_BOT_SECRET_KEY_123'));
  return Array.from(new Set(keys));
}

const IV_LENGTH = 16;

function encrypt(text) {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decryptWithKey(text, key) {
  try {
    const textParts = text.split(':');
    const iv = Buffer.from(textParts.shift(), 'hex');
    const encryptedText = Buffer.from(textParts.join(':'), 'hex');
    const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(key), iv);
    let decrypted = decipher.update(encryptedText);
    decrypted = Buffer.concat([decrypted, decipher.final()]);
    return decrypted.toString();
  } catch (e) {
    return null;
  }
}

function readEncryptedFile(filePath, defaultData) {
  if (!fs.existsSync(filePath)) return defaultData;
  try {
    const encData = fs.readFileSync(filePath, 'utf8');
    if (!encData.includes(':')) {
      // Migrate from unencrypted json (if any legacy data exists)
      return JSON.parse(encData);
    }
    for (const key of getDecryptKeys()) {
      const decData = decryptWithKey(encData, key);
      if (!decData) continue;
      try {
        const parsed = JSON.parse(decData);
        if (key !== ENCRYPTION_KEY) {
          try {
            writeEncryptedFile(filePath, parsed);
          } catch (_) {}
        }
        return parsed;
      } catch (_) {}
    }
    return defaultData;
  } catch (_) {
    return defaultData;
  }
}

function writeEncryptedFile(filePath, data) {
  const text = JSON.stringify(data);
  const encData = encrypt(text);
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tmp = `${filePath}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, encData, 'utf8');
  fs.renameSync(tmp, filePath);
}

function ensureStore() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(KEY_FILE)) writeKeyFile(ENCRYPTION_SECRET);
  if (!fs.existsSync(DATA_FILE)) writeEncryptedFile(DATA_FILE, []);
  if (!fs.existsSync(WD_FILE)) writeEncryptedFile(WD_FILE, []);
  if (!fs.existsSync(USERS_FILE)) writeEncryptedFile(USERS_FILE, {});
  if (!fs.existsSync(CONFIG_FILE)) writeEncryptedFile(CONFIG_FILE, { globalFeePercent: 0 });
  if (!fs.existsSync(BALANCE_HISTORY_FILE)) writeEncryptedFile(BALANCE_HISTORY_FILE, []);
  if (!fs.existsSync(USER_BALANCE_HISTORY_FILE)) writeEncryptedFile(USER_BALANCE_HISTORY_FILE, []);
  if (!fs.existsSync(IBFT_HISTORY_FILE)) writeEncryptedFile(IBFT_HISTORY_FILE, []);
}

ensureStore();

function loadAll() {
  return readEncryptedFile(DATA_FILE, []);
}

function saveAll(arr) {
  writeEncryptedFile(DATA_FILE, arr);
}

function upsert(record) {
  const arr = loadAll();
  const idx = arr.findIndex((r) => r.requestId === record.requestId);
  if (idx >= 0) {
    arr[idx] = { ...arr[idx], ...record };
  } else {
    arr.push(record);
  }
  saveAll(arr);
  return record;
}

function getByRequestId(requestId) {
  const arr = loadAll();
  return arr.find((r) => r.requestId === requestId) || null;
}

function loadWithdrawals() {
  return readEncryptedFile(WD_FILE, []);
}

function saveWithdrawals(arr) {
  writeEncryptedFile(WD_FILE, arr);
}

function addWithdrawal(record) {
  const arr = loadWithdrawals();
  arr.push(record);
  saveWithdrawals(arr);
  return record;
}

function getWithdrawals({ status, limit } = {}) {
  let arr = loadWithdrawals();
  if (status) arr = arr.filter((w) => w.status === status);
  arr.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  if (limit && Number(limit) > 0) arr = arr.slice(0, Number(limit));
  return arr;
}

function updateWithdrawalStatus(id, status, extra) {
  const arr = loadWithdrawals();
  const idx = arr.findIndex((w) => w.id === id);
  if (idx < 0) return null;
  const patch = extra && typeof extra === 'object' ? extra : {};
  arr[idx] = { ...arr[idx], status, updatedAt: Date.now(), ...patch };
  saveWithdrawals(arr);
  return arr[idx];
}

function getWithdrawalById(id) {
  const arr = loadWithdrawals();
  return arr.find((w) => w.id === id) || null;
}

function loadUsers() {
  return readEncryptedFile(USERS_FILE, {});
}

function saveUsers(data) {
  writeEncryptedFile(USERS_FILE, data);
}

function getUser(id) {
  const users = loadUsers();
  const key = String(id);
  if (!users[key]) {
    users[key] = {
      id: key,
      username: '',
      isActive: false,
      feePercent: null,
      ipnFeeFlat: null,
      withdrawFeeFlat: null,
      vaLimit: null,
      balance: 0,
      createdVA: 0,
    };
    saveUsers(users);
  }
  return users[key];
}

function findUser(id) {
  const users = loadUsers();
  const key = String(id);
  return users[key] || null;
}

function updateUser(id, data) {
  const users = loadUsers();
  const key = String(id);
  if (!users[key]) {
    users[key] = {
      id: key,
      username: '',
      isActive: false,
      feePercent: null,
      ipnFeeFlat: null,
      withdrawFeeFlat: null,
      vaLimit: null,
      balance: 0,
      createdVA: 0,
    };
  }
  users[key] = { ...users[key], ...data };
  if (users[key].balance < 0) users[key].balance = 0;
  saveUsers(users);
  return users[key];
}

function getAllUsers() {
  return Object.values(loadUsers());
}

function getConfig() {
  return readEncryptedFile(CONFIG_FILE, { globalFeePercent: 0, ipnFeeFlat: 4000, withdrawFeeFlat: 4000 });
}

function updateConfig(data) {
  const conf = getConfig();
  writeEncryptedFile(CONFIG_FILE, { ...conf, ...data });
  return getConfig();
}

function loadIbftHistory() {
  return readEncryptedFile(IBFT_HISTORY_FILE, []);
}

function saveIbftHistory(arr) {
  writeEncryptedFile(IBFT_HISTORY_FILE, arr);
}

function addIbftHistory(entry) {
  const arr = loadIbftHistory();
  const e = entry && typeof entry === 'object' ? entry : {};
  arr.push({
    ts: Number(e.ts) || Date.now(),
    adminId: e.adminId ? String(e.adminId) : '',
    merchant: e.merchant ? String(e.merchant) : '',
    bankCode: e.bankCode ? String(e.bankCode) : '',
    accountNumber: e.accountNumber ? String(e.accountNumber) : '',
    accountName: e.accountName ? String(e.accountName) : '',
    amount: Number(e.amount) || 0,
    remark: e.remark ? String(e.remark) : '',
    orderId: e.orderId ? String(e.orderId) : '',
    tranStatus: e.tranStatus ? String(e.tranStatus) : '',
    errorCode: e.errorCode ? String(e.errorCode) : '',
    errorMessage: e.errorMessage ? String(e.errorMessage) : '',
  });
  const max = 500;
  if (arr.length > max) arr.splice(0, arr.length - max);
  saveIbftHistory(arr);
  return arr[arr.length - 1];
}

function getIbftHistory(limit = 20) {
  const arr = loadIbftHistory();
  const n = Math.max(1, Math.min(200, Number(limit) || 20));
  return arr.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0)).slice(0, n);
}

function getAllIbftHistory() {
  const arr = loadIbftHistory();
  return arr.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0));
}

function loadBalanceHistory() {
  return readEncryptedFile(BALANCE_HISTORY_FILE, []);
}

function saveBalanceHistory(arr) {
  writeEncryptedFile(BALANCE_HISTORY_FILE, arr);
}

function addBalanceHistory(entry) {
  const arr = loadBalanceHistory();
  const e = entry && typeof entry === 'object' ? entry : {};
  arr.push({
    ts: Number(e.ts) || Date.now(),
    balance: Number(e.balance) || 0,
    balanceRaw: e.balanceRaw ? String(e.balanceRaw) : '',
    source: e.source ? String(e.source) : '',
    adminId: e.adminId ? String(e.adminId) : '',
  });
  const max = 500;
  if (arr.length > max) arr.splice(0, arr.length - max);
  saveBalanceHistory(arr);
  return arr[arr.length - 1];
}

function getBalanceHistory(limit = 50) {
  const arr = loadBalanceHistory();
  const n = Math.max(1, Math.min(200, Number(limit) || 50));
  return arr.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0)).slice(0, n);
}

function loadUserBalanceHistory() {
  return readEncryptedFile(USER_BALANCE_HISTORY_FILE, []);
}

function saveUserBalanceHistory(arr) {
  writeEncryptedFile(USER_BALANCE_HISTORY_FILE, arr);
}

function addUserBalanceHistory(entry) {
  const arr = loadUserBalanceHistory();
  const e = entry && typeof entry === 'object' ? entry : {};
  arr.push({
    ts: Number(e.ts) || Date.now(),
    userId: e.userId ? String(e.userId) : '',
    delta: Number(e.delta) || 0,
    balanceAfter: Number(e.balanceAfter) || 0,
    reason: e.reason ? String(e.reason) : '',
    ref: e.ref ? String(e.ref) : '',
  });
  const max = 3000;
  if (arr.length > max) arr.splice(0, arr.length - max);
  saveUserBalanceHistory(arr);
  return arr[arr.length - 1];
}

function getUserBalanceHistory(userId, limit = 30) {
  const uid = String(userId || '').trim();
  const n = Math.max(1, Math.min(200, Number(limit) || 30));
  const arr = loadUserBalanceHistory().filter((x) => String(x.userId) === uid);
  return arr.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0)).slice(0, n);
}

function getVAsByUser(userId, limit = 10) {
  const arr = loadAll();
  const uid = String(userId);
  return arr.filter((r) => String(r.userId) === uid).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)).slice(0, limit);
}

module.exports = {
  upsert,
  getByRequestId,
  addWithdrawal,
  getWithdrawals,
  updateWithdrawalStatus,
  getWithdrawalById,
  getUser,
  findUser,
  updateUser,
  getAllUsers,
  getConfig,
  updateConfig,
  addBalanceHistory,
  getBalanceHistory,
  addUserBalanceHistory,
  getUserBalanceHistory,
  addIbftHistory,
  getIbftHistory,
  getAllIbftHistory,
  getVAsByUser,
  // expose internal methods for migration script
  loadAll,
  saveAll,
  loadWithdrawals,
  saveWithdrawals,
  loadUsers,
  saveUsers
};
