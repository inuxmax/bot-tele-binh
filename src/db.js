const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, '..', 'data');
const DATA_FILE = path.join(DATA_DIR, 'va_records.enc');
const WD_FILE = path.join(DATA_DIR, 'withdrawals.enc');
const USERS_FILE = path.join(DATA_DIR, 'users.enc');
const CONFIG_FILE = path.join(DATA_DIR, 'config.enc');

// Lấy secret key từ môi trường hoặc sinh một chuỗi mặc định (để dev)
const ENCRYPTION_KEY = process.env.DB_ENCRYPTION_KEY 
  ? crypto.createHash('sha256').update(String(process.env.DB_ENCRYPTION_KEY)).digest('base64').substr(0, 32)
  : crypto.createHash('sha256').update('DEFAULT_BOT_SECRET_KEY_123').digest('base64').substr(0, 32);

const IV_LENGTH = 16;

function encrypt(text) {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(text) {
  try {
    const textParts = text.split(':');
    const iv = Buffer.from(textParts.shift(), 'hex');
    const encryptedText = Buffer.from(textParts.join(':'), 'hex');
    const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY), iv);
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
    const decData = decrypt(encData);
    if (!decData) return defaultData;
    return JSON.parse(decData);
  } catch (_) {
    return defaultData;
  }
}

function writeEncryptedFile(filePath, data) {
  const text = JSON.stringify(data);
  const encData = encrypt(text);
  fs.writeFileSync(filePath, encData, 'utf8');
}

function ensureStore() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(DATA_FILE)) writeEncryptedFile(DATA_FILE, []);
  if (!fs.existsSync(WD_FILE)) writeEncryptedFile(WD_FILE, []);
  if (!fs.existsSync(USERS_FILE)) writeEncryptedFile(USERS_FILE, {});
  if (!fs.existsSync(CONFIG_FILE)) writeEncryptedFile(CONFIG_FILE, { globalFeePercent: 0 });
}

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

function updateWithdrawalStatus(id, status) {
  const arr = loadWithdrawals();
  const idx = arr.findIndex((w) => w.id === id);
  if (idx < 0) return null;
  arr[idx] = { ...arr[idx], status, updatedAt: Date.now() };
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
    users[key] = { id: key, isActive: false, feePercent: null, vaLimit: null, balance: 0, createdVA: 0 };
    saveUsers(users);
  }
  return users[key];
}

function updateUser(id, data) {
  const users = loadUsers();
  const key = String(id);
  if (!users[key]) {
    users[key] = { id: key, isActive: false, feePercent: null, vaLimit: null, balance: 0, createdVA: 0 };
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
  return readEncryptedFile(CONFIG_FILE, { globalFeePercent: 0 });
}

function updateConfig(data) {
  const conf = getConfig();
  writeEncryptedFile(CONFIG_FILE, { ...conf, ...data });
  return getConfig();
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
  updateUser,
  getAllUsers,
  getConfig,
  updateConfig,
  getVAsByUser,
  // expose internal methods for migration script
  loadAll,
  saveAll,
  loadWithdrawals,
  saveWithdrawals,
  loadUsers,
  saveUsers
};
