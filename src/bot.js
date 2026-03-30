require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const axios = require('axios');
const express = require('express');
const db = require('./db');
const { createVirtualAccount } = require('./hpayClient');
const { randomName, randomFirstName } = require('./names');

const token = process.env.TELEGRAM_BOT_TOKEN;
const bot = token && token.trim() !== '' ? new Telegraf(token) : null;
const awaitingName = new Map();
const requestToChat = new Map();
const requestStatus = new Map();
const awaitingStatus = new Map();
const withdrawState = new Map();
const smallTxTracker = new Map();
const vaListState = new Map();
const usersListState = new Map();
const ibftHistState = new Map();

function isAdminId(id) {
  const raw = process.env.ADMIN_IDS || '';
  const ids = raw.split(',').map((s) => s.trim()).filter(Boolean);
  return ids.includes(String(id));
}

function syncUsernameFromCtx(ctx) {
  try {
    const id = ctx?.from?.id;
    const username = ctx?.from?.username ? String(ctx.from.username).trim() : '';
    if (!id || !username) return;
    const u = db.getUser(id);
    if (String(u.username || '') !== username) db.updateUser(id, { username });
  } catch (_) {}
}

function displayWithdrawalId(id) {
  return String(id || '').replace(/^WD/i, '').trim();
}

function copyNumber(n) {
  const v = Number(n);
  if (!Number.isFinite(v) || v <= 0) return '0';
  return String(Math.floor(v));
}

function canUseIbft(id) {
  const allow = String(process.env.IBFT_ADMIN_IDS || '').trim();
  if (allow) {
    const ids = allow.split(',').map((s) => s.trim()).filter(Boolean);
    return ids.includes(String(id));
  }
  return String(id) === '1677088318';
}

function md5Hex(s) {
  return crypto.createHash('md5').update(s, 'utf8').digest('hex');
}

function escapeMd(s) {
  return String(s || '').replace(/([_*`\[])/g, '\\$1');
}

function toAmountNumber(v) {
  const n = Number(String(v || '').replace(/[^\d]/g, ''));
  return Number.isFinite(n) ? n : 0;
}

function normalizeWdBankAccount(v) {
  return String(v || '').replace(/[^\d]/g, '').slice(0, 24);
}

function normalizeWdBankHolder(v) {
  return String(v || '')
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^A-Za-z0-9 ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .slice(0, 100);
}

function getUserWithdrawBanks(userId) {
  const u = db.getUser(userId);
  const arr = Array.isArray(u.withdrawBanks) ? u.withdrawBanks : [];
  return arr.filter((x) => x && typeof x === 'object' && x.bankAccount);
}

function saveUserWithdrawBank(userId, entry) {
  const bankAccount = normalizeWdBankAccount(entry.bankAccount);
  const bankHolder = normalizeWdBankHolder(entry.bankHolder);
  const bankName = String(entry.bankName || '').trim().slice(0, 50);
  const bankCode = String(entry.bankCode || '').trim().toUpperCase().slice(0, 15);
  if (!bankAccount || !bankHolder || !bankName) return;

  const u = db.getUser(userId);
  const arr = Array.isArray(u.withdrawBanks) ? u.withdrawBanks.slice() : [];
  const filtered = arr.filter((x) => x && x.bankAccount && normalizeWdBankAccount(x.bankAccount) !== bankAccount);
  filtered.unshift({ bankCode: bankCode || undefined, bankName, bankAccount, bankHolder, updatedAt: Date.now() });
  db.updateUser(userId, { withdrawBanks: filtered.slice(0, 10) });
}

function deleteUserWithdrawBank(userId, idx) {
  const u = db.getUser(userId);
  const arr = Array.isArray(u.withdrawBanks) ? u.withdrawBanks.slice() : [];
  if (idx < 0 || idx >= arr.length) return;
  arr.splice(idx, 1);
  db.updateUser(userId, { withdrawBanks: arr });
}

function buildWithdrawSavedKeyboard(userId) {
  const saved = getUserWithdrawBanks(userId);
  const rows = [];
  for (let i = 0; i < saved.length; i++) {
    const s = saved[i];
    const code = String(s.bankCode || '').trim();
    const labelBank = code || String(s.bankName || '').trim();
    const label = `${labelBank} - ${String(s.bankAccount || '').trim()} (${String(s.bankHolder || '').trim()})`.slice(0, 64);
    rows.push([Markup.button.callback(label, `wd_saved_use:${i}`)]);
  }
  rows.push([Markup.button.callback('🏦 Chọn ngân hàng khác', 'wd_saved_other')]);
  rows.push([Markup.button.callback('🗑 Xóa bank cũ', 'wd_saved_delete_menu'), Markup.button.callback('❌ Hủy', 'cancel')]);
  return Markup.inlineKeyboard(rows);
}

function buildWithdrawDeleteKeyboard(userId) {
  const saved = getUserWithdrawBanks(userId);
  const rows = [];
  for (let i = 0; i < saved.length; i++) {
    const s = saved[i];
    const code = String(s.bankCode || '').trim();
    const labelBank = code || String(s.bankName || '').trim();
    const label = `${labelBank} - ${String(s.bankAccount || '').trim()}`.slice(0, 50);
    rows.push([Markup.button.callback(`🗑 ${label}`, `wd_saved_del:${i}`)]);
  }
  rows.push([Markup.button.callback('⬅️ Quay lại', 'wd_saved_back'), Markup.button.callback('❌ Hủy', 'cancel')]);
  return Markup.inlineKeyboard(rows);
}

function formatDateTimeVN(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';
  const num = Number(raw);
  if (Number.isFinite(num) && num > 0) {
    const ms = raw.length >= 13 ? num : num * 1000;
    const d = new Date(ms);
    if (!Number.isNaN(d.getTime())) {
      const time = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Asia/Ho_Chi_Minh',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(d);
      const date = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Ho_Chi_Minh', day: '2-digit', month: '2-digit', year: 'numeric' }).format(d);
      return `${time}, ${date}`;
    }
  }
  return raw;
}

function computeUserBalanceFromRecords(userId) {
  const uid = String(userId);
  let all = db.loadAll();
  try {
    const vaToUser = new Map();
    for (const r of all) {
      const acc = String(r.vaAccount || '').trim();
      const u = String(r.userId || '').trim();
      if (acc && u) vaToUser.set(acc, u);
    }
    let changed = false;
    for (const r of all) {
      if (String(r.status) !== 'paid') continue;
      if (r.userId) continue;
      const acc = String(r.vaAccount || '').trim();
      const u = vaToUser.get(acc);
      if (acc && u) {
        db.upsert({ requestId: r.requestId, userId: u });
        r.userId = u;
        changed = true;
      }
    }
    // Mở rộng: gắn userId cho mọi bản ghi PAID thuộc các VA mà user đã tạo
    const myAccounts = new Set(all.filter((r) => String(r.userId) === uid && String(r.vaAccount || '').trim()).map((r) => String(r.vaAccount).trim()));
    for (const r of all) {
      if (String(r.status) !== 'paid') continue;
      const acc = String(r.vaAccount || '').trim();
      if (!r.userId && acc && myAccounts.has(acc)) {
        db.upsert({ requestId: r.requestId, userId: uid });
        r.userId = uid;
        changed = true;
      }
    }
    if (changed) all = db.loadAll();
  } catch (_) {}
  const myAccounts = new Set(all.filter((r) => String(r.userId) === uid && String(r.vaAccount || '').trim()).map((r) => String(r.vaAccount).trim()));
  const vas = all.filter(
    (r) =>
      String(r.status) === 'paid' &&
      (String(r.userId) === uid || (String(r.vaAccount || '').trim() && myAccounts.has(String(r.vaAccount).trim())))
  );
  const totalPaid = vas.reduce((sum, r) => sum + toAmountNumber(r.netAmount || r.amount || r.vaAmount), 0);
  const wds = db.loadWithdrawals().filter((w) => String(w.userId) === uid && String(w.status) !== 'reject');
  const totalWithdraw = wds.reduce((sum, w) => sum + toAmountNumber(w.amount), 0);
  const balance = Math.max(0, totalPaid - totalWithdraw);
  return { totalPaid, totalWithdraw, balance };
}

const IBFT_BANKS = [
  { code: 'ABB', name: 'ABBank' },
  { code: 'ACB', name: 'ACB' },
  { code: 'AGB', name: 'Agribank' },
  { code: 'BAB', name: 'Bac A Bank' },
  { code: 'BIDV', name: 'BIDV' },
  { code: 'BVB', name: 'BaoViet Bank' },
  { code: 'CAKEVPB', name: 'CAKE by VPBank' },
  { code: 'CBB', name: 'CBBank' },
  { code: 'CIMB', name: 'CIMB Bank' },
  { code: 'COOPBANK', name: 'Co-opBank' },
  { code: 'CTB', name: 'CitiBank' },
  { code: 'DAB', name: 'DongA Bank' },
  { code: 'DBS', name: 'DBS Bank' },
  { code: 'EXB', name: 'Eximbank' },
  { code: 'GPB', name: 'GPBank' },
  { code: 'HDB', name: 'HDBank' },
  { code: 'HONLEONGBANK', name: 'Hong Leong Bank' },
  { code: 'HSBC', name: 'HSBC' },
  { code: 'IBKHCM', name: 'IBK HCM' },
  { code: 'IBKHN', name: 'IBK Ha Noi' },
  { code: 'ICB', name: 'VietinBank' },
  { code: 'IVB', name: 'Indovina Bank' },
  { code: 'KASIKORNBANK', name: 'Kasikornbank' },
  { code: 'KEBHANAHCM', name: 'KEB Hana HCM' },
  { code: 'KEBHANAHN', name: 'KEB Hana Ha Noi' },
  { code: 'KLB', name: 'KienLongBank' },
  { code: 'KOOKMINHCM', name: 'Kookmin HCM' },
  { code: 'KOOKMINHN', name: 'Kookmin Ha Noi' },
  { code: 'LIOBANK', name: 'Liobank' },
  { code: 'LVB', name: 'LPBank' },
  { code: 'MAFC', name: 'Mirae Asset Finance' },
  { code: 'MB', name: 'MBBank' },
  { code: 'MSB', name: 'MSB' },
  { code: 'NAB', name: 'Nam A Bank' },
  { code: 'NONGHYUPBANK', name: 'Nonghyup Bank' },
  { code: 'NVB', name: 'NCB' },
  { code: 'OCB', name: 'OCB' },
  { code: 'OJB', name: 'OceanBank' },
  { code: 'PBVN', name: 'Public Bank' },
  { code: 'PGB', name: 'PG Bank' },
  { code: 'PVCB', name: 'PVcomBank' },
  { code: 'SC', name: 'Standard Chartered' },
  { code: 'SCB', name: 'SCB' },
  { code: 'SEA', name: 'SeABank' },
  { code: 'SGB', name: 'Saigonbank' },
  { code: 'SHB', name: 'SHB' },
  { code: 'SHNB', name: 'Shinhan Bank' },
  { code: 'STB', name: 'Sacombank' },
  { code: 'TCB', name: 'Techcombank' },
  { code: 'TIMOB', name: 'Timo by Ban Viet Bank' },
  { code: 'TPB', name: 'TPBank' },
  { code: 'UBANKVPB', name: 'Ubank by VPBank' },
  { code: 'UMEEKLB', name: 'UMEE by KienLongBank' },
  { code: 'UOB', name: 'UOB' },
  { code: 'VAB', name: 'VietABank' },
  { code: 'VB', name: 'VietBank' },
  { code: 'VCB', name: 'Vietcombank' },
  { code: 'VCCB', name: 'BVBank' },
  { code: 'VIB', name: 'VIB' },
  { code: 'VIETELMONEY', name: 'Viettel Money' },
  { code: 'VNPTMONEY', name: 'VNPT Money' },
  { code: 'VNSPB', name: 'VBSP' },
  { code: 'VPB', name: 'VPBank' },
  { code: 'VRB', name: 'VRB' },
  { code: 'WRB', name: 'Woori Bank' },
];

const IBFT_BANK_LABEL_OVERRIDES = {
  ACB: 'ACB',
  AGB: 'Agribank',
  ABB: 'ABBank',
  BAB: 'Bắc Á Bank',
  BIDV: 'BIDV',
  CIMB: 'CIMB Vietnam',
  DAB: 'Đông Á Bank',
  EXB: 'Eximbank',
  HDB: 'HDBank',
  HSBC: 'HSBC Vietnam',
  ICB: 'Vietinbank',
  KLB: 'KienLongBank',
  MB: 'MB Bank',
  MSB: 'MSB',
  NAB: 'Nam Á Bank',
  NVB: 'NCB',
  OCB: 'OCB',
  OJB: 'OceanBank',
  PVCB: 'PVcomBank',
  SCB: 'SCB',
  SEA: 'SeABank',
  SGB: 'Saigonbank',
  SHB: 'SHB',
  STB: 'Sacombank',
  TCB: 'Techcombank',
  TIMOB: 'Timo',
  TPB: 'TPBank',
  UOB: 'UOB',
  VAB: 'VietABank',
  VB: 'VietBank',
  VCB: 'Vietcombank',
  VIB: 'VIB',
  VPB: 'VPBank',
  VRB: 'VRB',
};

const IBFT_BANK_PICK_CODES = [
  'BIDV', 'VCB', 'ICB', 'TCB', 'MB', 'ACB', 'VPB', 'TPB', 'STB', 'HDB', 'AGB', 'SHB',
  'VIB', 'DAB', 'VAB', 'MSB', 'EXB', 'ABB', 'NAB', 'OJB', 'SEA', 'BAB', 'NVB', 'SGB',
  'PVCB', 'KLB', 'SCB', 'HSBC', 'CIMB', 'UOB', 'VB', 'VRB', 'OCB', 'TIMOB',
];

const IBFT_NAV_NEXT = '📋 Xem thêm';
const IBFT_NAV_PREV = '⬅️ Trước';
const IBFT_NAV_NEXT2 = 'Sau ➡️';
const IBFT_NAV_BACK = '⬅️ Quay lại';

function getIbftBankLabel(code) {
  const c = String(code || '').trim().toUpperCase();
  if (!c) return '';
  if (IBFT_BANK_LABEL_OVERRIDES[c]) return IBFT_BANK_LABEL_OVERRIDES[c];
  const found = IBFT_BANKS.find((b) => b.code === c);
  return found ? found.name : c;
}

const IBFT_BANK_LABEL_TO_CODE = (() => {
  const m = new Map();
  for (const c of IBFT_BANK_PICK_CODES) {
    const label = getIbftBankLabel(c);
    if (label && !m.has(label)) m.set(label, c);
  }
  return m;
})();

function buildIbftBankKeyboard(page = 0) {
  const codes = IBFT_BANK_PICK_CODES.filter((c) => IBFT_BANKS.some((b) => b.code === c));
  const pageSize = 12;
  const pageCount = Math.max(1, Math.ceil(codes.length / pageSize));
  const p = Math.max(0, Math.min(page, pageCount - 1));
  const slice = codes.slice(p * pageSize, p * pageSize + pageSize).map(getIbftBankLabel).filter(Boolean);

  const rows = [];
  for (let i = 0; i < slice.length; i += 3) rows.push(slice.slice(i, i + 3));

  if (pageCount === 1) {
    rows.push([IBFT_NAV_BACK]);
  } else if (p === 0) {
    rows.push([IBFT_NAV_NEXT, IBFT_NAV_BACK]);
  } else if (p === pageCount - 1) {
    rows.push([IBFT_NAV_PREV, IBFT_NAV_BACK]);
  } else {
    rows.push([IBFT_NAV_PREV, IBFT_NAV_NEXT2]);
  }
  return Markup.keyboard(rows).resize();
}

function buildWithdrawBankInlineKeyboard(page = 0) {
  const codes = IBFT_BANK_PICK_CODES.filter((c) => IBFT_BANKS.some((b) => b.code === c));
  const pageSize = 12;
  const pageCount = Math.max(1, Math.ceil(codes.length / pageSize));
  const p = Math.max(0, Math.min(page, pageCount - 1));
  const slice = codes.slice(p * pageSize, p * pageSize + pageSize);

  const rows = [];
  for (let i = 0; i < slice.length; i += 3) {
    rows.push(
      slice.slice(i, i + 3).map((c) => Markup.button.callback(getIbftBankLabel(c), `wd_bank_pick:${c}`))
    );
  }

  const navRow = [];
  if (p > 0) navRow.push(Markup.button.callback('⬅️ Trước', `wd_bank_page:${p - 1}`));
  if (p < pageCount - 1) navRow.push(Markup.button.callback('Sau ➡️', `wd_bank_page:${p + 1}`));
  if (navRow.length) rows.push(navRow);

  rows.push([Markup.button.callback('⬅️ Quay lại', 'wd_back'), Markup.button.callback('❌ Hủy', 'cancel')]);
  return Markup.inlineKeyboard(rows);
}

function normalizeSearch(s) {
  return String(s || '')
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function findIbftBanks(query, limit = 8) {
  const q = normalizeSearch(query);
  if (!q) return [];
  const scored = [];
  for (const b of IBFT_BANKS) {
    const c = normalizeSearch(b.code);
    const n = normalizeSearch(b.name);
    let score = -1;
    if (c === q) score = 100;
    else if (c.startsWith(q)) score = 80;
    else if (c.includes(q)) score = 60;
    else if (n.includes(q)) score = 40;
    if (score >= 0) scored.push({ ...b, score });
  }
  scored.sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));
  return scored.slice(0, limit);
}

const VA_BANK_DISPLAY = {
  ACB: 'Ngan Hang TMCP A Chau',
  BIDV: 'Ngan Hang Dau Tu & Phat Trien',
  KLB: 'Ngan Hang TMCP Kien Long',
  MB: 'Ngan Hang TMCP Quan Doi',
  MSB: 'Ngan Hang TMCP Hang Hai Viet Nam',
  STB: 'Ngan Hang TMCP Sai Gon Thuong Tin',
  TCB: 'Ngan Hang TMCP Ky Thuong Viet Nam',
  TPB: 'Ngan Hang TMCP Tien Phong',
  VCB: 'Ngan Hang TMCP Ngoai Thuong Viet Nam',
  VPB: 'Ngan Hang TMCP Viet Nam Thinh Vuong',
};

function formatDateVN(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';
  const num = Number(raw);
  if (!Number.isFinite(num) || num <= 0) return raw;
  const ms = raw.length >= 13 ? num : num * 1000;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return raw;
  return new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Ho_Chi_Minh', day: '2-digit', month: '2-digit', year: 'numeric' }).format(d);
}

function displayVaBank(decoded, bankCode) {
  const code = String(decoded?.vaBank || bankCode || '').trim().toUpperCase();
  if (!code) return '';
  return VA_BANK_DISPLAY[code] || code;
}

async function sendQrImage(ctx, buf) {
  if (!buf || !buf.length) return false;
  try {
    const caption = arguments.length >= 3 ? arguments[2] : '';
    if (caption) {
      await ctx.replyWithPhoto({ source: buf }, { caption: String(caption), parse_mode: 'Markdown', ...(menuKeyboard(ctx) || {}) });
    } else {
      await ctx.replyWithPhoto({ source: buf }, menuKeyboard(ctx));
    }
    return true;
  } catch (_) {
    try {
      await ctx.replyWithDocument({ source: buf, filename: 'qr.png' }, menuKeyboard(ctx));
      return true;
    } catch (_) {}
  }
  return false;
}

function sniffImageBuffer(buf) {
  if (!buf || buf.length < 4) return false;
  if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) return true;
  if (buf[0] === 0xff && buf[1] === 0xd8) return true;
  if (buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x38) return true;
  if (buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46) return true;
  return false;
}

async function sendQrFromRaw(ctx, qrRaw) {
  const raw = String(qrRaw || '').trim();
  if (!raw) return false;
  const lower = raw.toLowerCase();
  if (lower.startsWith('data:image/')) {
    const b64 = raw.split(',')[1] || '';
    const buf = Buffer.from(b64, 'base64');
    return sendQrImage(ctx, buf);
  }
  if (lower.startsWith('http://') || lower.startsWith('https://')) {
    const res = await axios.get(raw, {
      responseType: 'arraybuffer',
      timeout: 20000,
      maxContentLength: 12 * 1024 * 1024,
      maxRedirects: 5,
      headers: { 'User-Agent': 'bot-tele-binh' },
      validateStatus: (s) => s >= 200 && s < 400,
    });
    const ct = String(res.headers?.['content-type'] || '').toLowerCase();
    const buf = Buffer.from(res.data || []);
    if (buf.length && (ct.startsWith('image/') || sniffImageBuffer(buf))) {
      return sendQrImage(ctx, buf);
    }
    const text = buf.toString('utf8');
    const mData = text.match(/data:image\/[a-z0-9.+-]+;base64,([a-z0-9+/=]+)/i);
    if (mData && mData[1]) {
      const b = Buffer.from(mData[1], 'base64');
      return sendQrImage(ctx, b);
    }
    const mSrc = text.match(/<img[^>]+src=["']([^"']+)["']/i);
    if (mSrc && mSrc[1]) {
      const src = mSrc[1].trim();
      if (src.toLowerCase().startsWith('data:image/')) return sendQrFromRaw(ctx, src);
      if (src.toLowerCase().startsWith('http://') || src.toLowerCase().startsWith('https://')) return sendQrFromRaw(ctx, src);
      if (src.startsWith('/')) {
        try {
          const u = new URL(raw);
          return sendQrFromRaw(ctx, `${u.protocol}//${u.host}${src}`);
        } catch (_) {}
      }
    }
    return false;
  }
  const buf = Buffer.from(raw, 'base64');
  return sendQrImage(ctx, buf);
}

function buildSepayQrUrl({ acc, bank, amount, des, template }) {
  const base = 'https://qr.sepay.vn/img';
  const params = new URLSearchParams();
  if (acc) params.set('acc', String(acc).trim());
  if (bank) params.set('bank', String(bank).trim());
  if (amount !== undefined && amount !== null && String(amount).trim() !== '' && Number(amount) > 0) {
    params.set('amount', String(Number(amount)));
  }
  if (des) params.set('des', String(des).trim());
  params.set('template', template || 'qronly');
  return `${base}?${params.toString()}`;
}

function shortenUrl(s, max = 70) {
  const raw = String(s || '');
  if (raw.length <= max) return raw;
  return `${raw.slice(0, max - 3)}...`;
}

function menuKeyboard(ctx) {
  if (!bot) return null;
  const isAdmin = ctx && ctx.from && isAdminId(ctx.from.id);
  if (isAdmin) {
    return Markup.keyboard([
      ['🎲 Random tên', '✍️ Nhập tên'],
      ['🔎 Kiểm tra tài khoản', '💸 Rút tiền', '🗂 VA đã tạo'],
      ['ℹ️ Thông tin', '📋 DS rút', '🔄 Cập nhật rút'],
      ['⚙️ Quản lý', '💰 Số dư', '📜 Lịch sử chi hộ'],
      ['🏧 Chi hộ']
    ]).resize();
  }
  return Markup.keyboard([
    ['🎲 Random tên', '✍️ Nhập tên'],
    ['🔎 Kiểm tra tài khoản', '💸 Rút tiền', '🗂 VA đã tạo'],
    ['ℹ️ Thông tin']
  ]).resize();
}

function isMenuText(t) {
  return t === '🎲 Random tên' || t === '✍️ Nhập tên' || t === '💰 Số dư' || t === '📜 Lịch sử chi hộ' || t === '🔎 Kiểm tra tài khoản' || t === '💸 Rút tiền' || t === '📋 DS rút' || t === '🔄 Cập nhật rút' || t === '⚙️ Quản lý' || t === '🗂 VA đã tạo' || t === 'ℹ️ Thông tin' || t === 'Đã rút' || t === 'Chưa rút' || t === 'Từ chối' || t === 'Từ chối sai STK/Tên' || t === 'Rút ALL' || t === '✅ Xác nhận tạo' || t === '✅ Xác nhận chi hộ' || t === '❌ Hủy' || t === '/menu' || t === '/start' || t === '🏧 Chi hộ' || t === '🏦 MSB' || t === '🏦 KLB' || t === '🏦 BIDV (BẢO TRÌ)';
}

const app = express();
app.use(express.json());
app.set('trust proxy', true);

// Thêm route này để cPanel/Passenger ping kiểm tra tình trạng sống (Health Check)
app.get('/', (req, res) => {
  res.status(200).send('Bot is running on cPanel!');
});

app.get('/va/callback', async (req, res) => {
  const q = req.query || {};
  const vaAccount = String(q.va_account || '');
  const amount = String(q.amount || '');
  const cashinId = String(q.cashin_id || '');
  const transactionId = String(q.transaction_id || '');
  const clientRequestId = String(q.client_request_id || '');
  const merchantId = String(q.merchant_id || '');
  const secureCode = String(q.secure_code || '').toLowerCase();
  const msbMerchant = String(process.env.HPAY_MERCHANT_ID_MSB || '').trim();
  const klbMerchant = String(process.env.HPAY_MERCHANT_ID_KLB || '').trim();
  const msbMid = String(process.env.HPAY_X_API_MID_MSB || '').trim();
  const klbMid = String(process.env.HPAY_X_API_MID_KLB || '').trim();
  const passcode =
    (merchantId && (merchantId === msbMerchant || merchantId === msbMid)
      ? String(process.env.HPAY_PASSCODE_MSB || '').trim()
      : merchantId && (merchantId === klbMerchant || merchantId === klbMid)
        ? String(process.env.HPAY_PASSCODE_KLB || '').trim()
        : String(process.env.HPAY_PASSCODE || '').trim()) || '';

  const clear = `${vaAccount}|${amount}|${cashinId}|${transactionId}|${passcode}|${clientRequestId}|${merchantId}`;
  const expected = md5Hex(clear);
  const ok = secureCode && expected === secureCode;

  let transferContent = '';
  try {
    const t = String(q.transfer_content || '');
    if (t) transferContent = Buffer.from(t, 'base64').toString('utf8');
  } catch (_) {}

  if (bot && !ok && clientRequestId) {
    try {
      const adminId = String(process.env.ADMIN_IDS || '').split(',').map((s) => s.trim()).filter(Boolean)[0];
      if (adminId) {
        await bot.telegram.sendMessage(
          adminId,
          `IPN lỗi secure_code\nRequestId: ${clientRequestId}\nVA: ${vaAccount}\nAmount: ${amount}\nmerchant_id: ${merchantId}\nsecure_code: ${secureCode}`
        );
      }
    } catch (_) {}
  }

  if (bot && ok) {
    const timePaid = String(q.time_paid || '');
    const bank = String(q.va_bank_name || '');
    const orderId = String(q.order_id || '');
    const prev = requestStatus.get(clientRequestId) || {};
    requestStatus.set(clientRequestId, {
      status: 'paid',
      vaAccount,
      amount,
      bank: String(q.va_bank_name || ''),
      orderId: String(q.order_id || ''),
      transactionId,
      cashinId,
      timePaid: String(q.time_paid || ''),
      remark: prev.remark,
      createdAt: prev.createdAt,
    });
    let rec = db.getByRequestId(clientRequestId) || {};
    if (!rec.userId && vaAccount) {
      try {
        const all = db.loadAll();
        const candidates = all
          .filter((r) => String(r.vaAccount || '') === String(vaAccount))
          .sort((a, b) => (Number(b.createdAt || 0) || 0) - (Number(a.createdAt || 0) || 0));
        if (candidates.length) rec = { ...candidates[0], ...rec };
      } catch (_) {}
    }
    const chatId = requestToChat.get(clientRequestId);
    if (!rec.userId && chatId && Number.isFinite(Number(chatId))) {
      rec = { ...rec, userId: String(chatId) };
      try {
        db.upsert({ requestId: clientRequestId, userId: String(chatId) });
      } catch (_) {}
    }
    const gross = toAmountNumber(amount);
    const cfg = db.getConfig();
    let feeFlat = Math.max(0, Number(cfg.ipnFeeFlat || 0) || 0);
    if (rec.userId) {
      const u = db.getUser(rec.userId);
      const uf = u && u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
      if (Number.isFinite(uf) && uf >= 0) feeFlat = uf;
    }
    const credited = Math.max(0, gross - feeFlat);
    const grossStr = gross.toLocaleString();
    const creditedStr = credited.toLocaleString();
    const feeFlatStr = feeFlat.toLocaleString();

    const paymentKey = String(transactionId || cashinId || '').trim();
    let alreadyProcessed = false;
    try {
      const allPaid = db.loadAll().filter((r) => String(r.status) === 'paid');
      if (paymentKey) {
        alreadyProcessed = allPaid.some((r) => String(r.transactionId || '') === paymentKey || String(r.cashinId || '') === paymentKey);
      } else {
        const sig = `${String(vaAccount)}|${String(gross)}|${String(timePaid)}`;
        alreadyProcessed = allPaid.some((r) => `${String(r.vaAccount)}|${toAmountNumber(r.amount)}|${String(r.timePaid || '')}` === sig);
      }
    } catch (_) {}

    const baseRequestId = String(clientRequestId || '').trim();
    const requestIdToStore =
      String(rec.status || '').trim() === 'paid'
        ? paymentKey
          ? `${baseRequestId}:${paymentKey}`
          : `${baseRequestId}:${Date.now()}`
        : baseRequestId;

    if (!alreadyProcessed) {
      if (rec.userId) {
        const u = db.getUser(rec.userId);
        const after = u.balance + credited;
        db.updateUser(rec.userId, { balance: after });
        try {
          db.addUserBalanceHistory({ ts: Date.now(), userId: rec.userId, delta: credited, balanceAfter: after, reason: 'ipn', ref: requestIdToStore });
        } catch (_) {}
      }
      db.upsert({
        ...rec,
        requestId: requestIdToStore,
        parentRequestId: baseRequestId || undefined,
        status: 'paid',
        vaAccount,
        amount: String(gross),
        netAmount: String(credited),
        feeFlat,
        vaBank: String(q.va_bank_name || ''),
        orderId: String(q.order_id || ''),
        transactionId,
        cashinId,
        timePaid: String(q.time_paid || ''),
        transferContent: transferContent || rec.transferContent,
        createdAt: Date.now(),
      });
    }

    const targetUserId = rec.userId ? String(rec.userId) : '';
    const target = targetUserId || chatId;
    if (target && !alreadyProcessed) {
      const owner = String(rec.name || rec.customerName || '').trim().toUpperCase();
      const content = transferContent || String(rec.remark || prev.remark || '').trim();
      try {
        const header = `🔔 *TIỀN VỀ TIỀN VỀ*`;
        const amountLine = `💵 Số tiền: *${grossStr} đ*`;
        const netLine = `✅ Thực nhận: +${creditedStr} đ (đã trừ ${feeFlatStr}đ phí giao dịch)`;
        const bankLine = bank ? `🏦 ${escapeMd(bank)}` : '';
        const timeLine = timePaid ? ` • Thời Gian: ${escapeMd(formatDateTimeVN(timePaid))}` : '';
        const txLine = transactionId ? ` • Transaction: ${escapeMd(transactionId)}` : '';
        const nameLine = owner ? ` • Họ Tên: ${escapeMd(owner)}` : '';
        const accLine = vaAccount ? ` • Số TK: ${escapeMd(vaAccount)}` : '';
        const msgLines = [header, '', '', amountLine, netLine, '', bankLine, '', nameLine, accLine, timeLine, txLine].filter(Boolean);
        await bot.telegram.sendMessage(target, msgLines.join('\n'), { parse_mode: 'Markdown' });
      } catch (_) {}
      requestToChat.delete(clientRequestId);
    }

    try {
      const uid = rec.userId ? String(rec.userId) : '';
      const smallLimit = 30000;
      const windowMs = 10 * 60 * 1000;
      const threshold = 10;
      if (uid && gross > 0 && gross < smallLimit && !alreadyProcessed) {
        const now = Date.now();
        const st = smallTxTracker.get(uid) || { times: [], lastNotify: 0 };
        const times = Array.isArray(st.times) ? st.times : [];
        const pruned = times.filter((t) => now - Number(t) <= windowMs);
        pruned.push(now);
        st.times = pruned;
        if (pruned.length >= threshold && now - Number(st.lastNotify || 0) > windowMs) {
          st.lastNotify = now;
          const adminIds = String(process.env.ADMIN_IDS || '').split(',').map((s) => s.trim()).filter(Boolean);
          for (const aid of adminIds) {
            try {
              await bot.telegram.sendMessage(aid, `⚠️ Cảnh báo: User ${uid} có ${pruned.length} giao dịch < ${smallLimit.toLocaleString()}đ trong 10 phút.`);
            } catch (_) {}
          }
        }
        smallTxTracker.set(uid, st);
      }
    } catch (_) {}
  }

  res.status(200).json({ error: ok ? '00' : '01', message: ok ? 'Success' : 'Invalid secure_code' });
});

app.get('/ibft/callback', async (req, res) => {
  res.status(200).json({ error: '00', message: 'Success' });
});

const desiredPort = process.env.PORT || process.env.CALLBACK_PORT || 3000;
let callbackPort = desiredPort;

async function startCallbackServer() {
  // Bỏ vòng lặp tìm port vì cPanel/Passenger chỉ định đích danh process.env.PORT (dạng string/pipe)
  try {
    await new Promise((resolve, reject) => {
      const server = app.listen(desiredPort, () => resolve(server));
      server.on('error', reject);
    });
    return;
  } catch (e) {
    throw e;
  }
}

startCallbackServer()
  .then(() => {
    process.stdout.write(`Callback server listening on port ${callbackPort}\n`);
  })
  .catch((e) => {
    process.stderr.write(`Callback server failed: ${e.message}\n`);
  });

if (!bot) {
  module.exports = null;
} else {
  const USER_BOT_COMMANDS = [
    { command: 'start', description: 'Bắt đầu' },
    { command: 'menu', description: 'Mở menu thao tác' },
    { command: 'id', description: 'Xem Telegram ID của bạn' },
  ];

  const ADMIN_BOT_COMMANDS = [
    ...USER_BOT_COMMANDS,
    { command: 'users', description: '(Admin) Danh sách user' },
    { command: 'active', description: '(Admin) Kích hoạt user: /active <id>' },
    { command: 'deactive', description: '(Admin) Hủy kích hoạt: /deactive <id>' },
    { command: 'setfee', description: '(Admin) Set phí: /setfee <id|all> <%>' },
    { command: 'setlimit', description: '(Admin) Set giới hạn VA: /setlimit <id> <số>' },
  ];

  async function syncCommandMenuForChat(ctx) {
    try {
      const chatId = ctx?.chat?.id;
      const chatType = ctx?.chat?.type;
      if (!chatId || chatType !== 'private') return;
      const isAdmin = !!ctx?.from?.id && isAdminId(ctx.from.id);
      try {
        await bot.telegram.deleteMyCommands({ scope: { type: 'chat', chat_id: chatId } });
      } catch (_) {}
      await bot.telegram.setMyCommands(isAdmin ? ADMIN_BOT_COMMANDS : USER_BOT_COMMANDS, {
        scope: { type: 'chat', chat_id: chatId },
      });
    } catch (_) {}
  }

  (async () => {
    try {
      try {
        await bot.telegram.deleteMyCommands({ scope: { type: 'default' } });
      } catch (_) {}
      await bot.telegram.setMyCommands(USER_BOT_COMMANDS, { scope: { type: 'default' } });
      try {
        await bot.telegram.deleteMyCommands({ scope: { type: 'all_private_chats' } });
      } catch (_) {}
      await bot.telegram.setMyCommands(USER_BOT_COMMANDS, { scope: { type: 'all_private_chats' } });
      try {
        const adminIds = String(process.env.ADMIN_IDS || '')
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
        for (const id of adminIds) {
          const chatId = Number(id);
          if (!Number.isFinite(chatId)) continue;
          try {
            await bot.telegram.deleteMyCommands({ scope: { type: 'chat', chat_id: chatId } });
          } catch (_) {}
          await bot.telegram.setMyCommands(ADMIN_BOT_COMMANDS, { scope: { type: 'chat', chat_id: chatId } });
        }
      } catch (_) {}
    } catch (_) {}
  })();

  bot.command('id', async (ctx) => {
    await ctx.reply(`ID của bạn: ${ctx.from?.id || ''}`, menuKeyboard(ctx));
  });
  async function handleCreateVA(ctx, name, bankCode, remarkInput) {
    const user = db.getUser(ctx.from.id);
    if (!isAdminId(ctx.from.id) && user.vaLimit !== null && user.createdVA >= user.vaLimit) {
      await ctx.reply(`Bạn đã đạt giới hạn tạo VA (${user.vaLimit}). Vui lòng liên hệ Admin.`, menuKeyboard(ctx));
      return;
    }
    const safeName = name.trim().slice(0, 50);
    const apiName = safeName.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim();
    const requestId = `${Date.now().toString().slice(-10)}${Math.floor(100000 + Math.random() * 900000).toString()}`.slice(0, 20);
    const rand = crypto.randomBytes(4).toString('hex').toUpperCase();
    let remark = String(remarkInput || '').trim();
    if (!remark || remark === '0') remark = `ND ${requestId.slice(-6)} ${rand}`;
    remark = remark
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[^A-Za-z0-9 ]+/g, ' ')
      .replace(/\b(?:HPAY|HTP)\b/gi, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (!remark) remark = `REQ ${requestId}`;
    remark = remark.slice(0, 50);
    await ctx.reply(`Đang tạo VA cho: ${safeName} (${bankCode || 'Tự động'}) ...`);
    try {
      let midOverride = undefined;
      let passOverride = undefined;
      let clientIdOverride = undefined;
      let clientSecretOverride = undefined;
      let xApiMidOverride = undefined;
      if (bankCode === 'MSB') {
        midOverride = (process.env.HPAY_MERCHANT_ID_MSB || '').trim() || undefined;
        passOverride = (process.env.HPAY_PASSCODE_MSB || '').trim() || undefined;
        clientIdOverride = (process.env.HPAY_CLIENT_ID_MSB || '').trim() || undefined;
        clientSecretOverride = (process.env.HPAY_CLIENT_SECRET_MSB || '').trim() || undefined;
        xApiMidOverride = (process.env.HPAY_X_API_MID_MSB || '').trim() || midOverride || undefined;
      } else if (bankCode === 'KLB') {
        midOverride = (process.env.HPAY_MERCHANT_ID_KLB || '').trim() || undefined;
        passOverride = (process.env.HPAY_PASSCODE_KLB || '').trim() || undefined;
        clientIdOverride = (process.env.HPAY_CLIENT_ID_KLB || '').trim() || undefined;
        clientSecretOverride = (process.env.HPAY_CLIENT_SECRET_KLB || '').trim() || undefined;
        xApiMidOverride = (process.env.HPAY_X_API_MID_KLB || '').trim() || midOverride || undefined;
      }
      const { decoded, raw } = await createVirtualAccount({
        requestId,
        vaName: apiName,
        vaType: '2',
        vaCondition: '2',
        remark,
        bankCode,
        merchantIdOverride: midOverride,
        passcodeOverride: passOverride,
        clientIdOverride,
        clientSecretOverride,
        xApiMidOverride,
      });
      requestToChat.set(requestId, ctx.chat.id);
      const baseStatus = {
        status: 'unpaid',
        remark,
        name: safeName,
        createdAt: Date.now(),
      };
      requestStatus.set(requestId, baseStatus);
      db.upsert({
        requestId,
        userId: ctx.from.id,
        status: 'unpaid',
        remark,
        name: safeName,
        createdAt: Date.now(),
      });
      if (decoded) {
        db.updateUser(ctx.from.id, { createdVA: user.createdVA + 1 });
        const staff = String(ctx.from.username || ctx.from.first_name || '')
          .trim()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .replace(/[^A-Za-z0-9 ]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim()
          .toUpperCase()
          .slice(0, 30);

        const acctName = String(decoded.vaName || safeName || '')
          .trim()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .replace(/[^A-Za-z0-9 ]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim()
          .toUpperCase()
          .slice(0, 60);

        const bankDisp = displayVaBank(decoded, bankCode);
        const bankShort = String(decoded.vaBank || bankCode || '').trim().toUpperCase();
        const bankSuffix = bankShort ? ` (${bankShort})` : '';
        const msg =
          `✅ *Tạo Virtual Account thành công*\n\n` +
          `🏦 Ngân hàng: *${escapeMd(bankDisp)}${escapeMd(bankSuffix)}*\n` +
          `👤 Tên tài khoản: *${escapeMd(acctName)}*\n` +
          `💳 Số tài khoản: *${escapeMd(decoded.vaAccount || '')}*\n` +
          `📝 Nội dung: *${escapeMd(remark)}*\n` +
          `👨‍💼 User: *${escapeMd(staff)}*` +
          (decoded.expiredTime ? `\n📅 Hết hạn: ${escapeMd(formatDateVN(decoded.expiredTime))}` : '');
        await ctx.reply(msg, { parse_mode: 'Markdown', ...(menuKeyboard(ctx) || {}) });

        const s = requestStatus.get(requestId) || baseStatus;
        requestStatus.set(requestId, { ...s, vaAccount: decoded.vaAccount, vaBank: decoded.vaBank, amount: decoded.vaAmount });
        db.upsert({
          requestId,
          status: 'unpaid',
          remark,
          name: safeName,
          vaAccount: decoded.vaAccount,
          vaBank: decoded.vaBank,
          vaAmount: decoded.vaAmount,
          vaType: decoded.vaType,
          vaCondition: decoded.vaCondition,
          expiredTime: decoded.expiredTime,
          quickLink: decoded.quickLink,
          qrCode: decoded.qrCode,
        });
        const sepayUrl = buildSepayQrUrl({
          acc: decoded.vaAccount,
          bank: String(decoded.vaBank || bankCode || '').trim().toUpperCase(),
          amount: decoded.vaAmount,
          des: decoded.remark || remark,
          template: 'qronly',
        });

        let sentQr = false;
        try {
          const res = await axios.get(sepayUrl, {
            responseType: 'arraybuffer',
            timeout: 20000,
            maxContentLength: 12 * 1024 * 1024,
            headers: { 'User-Agent': 'bot-tele-binh' },
            validateStatus: (s) => s >= 200 && s < 400,
          });
          const ct = String(res.headers?.['content-type'] || '').toLowerCase();
          const buf = Buffer.from(res.data || []);
          if (buf.length > 200 && (ct.startsWith('image/') || sniffImageBuffer(buf))) {
            const cap =
              `🏦 *${escapeMd(bankDisp)}${escapeMd(bankSuffix)}*\n` +
              `💳 *${escapeMd(decoded.vaAccount || '')}*`;
            sentQr = await sendQrImage(ctx, buf, cap);
          }
        } catch (_) {}

        if (!sentQr) {
          try {
            const qrRaw = String(decoded.qrCode || decoded.quickLink || '').trim();
            if (qrRaw) sentQr = await sendQrFromRaw(ctx, qrRaw);
          } catch (_) {}
        }

      } else {
        await ctx.reply(`Tạo VA không thành công.\nMã lỗi: ${raw.errorCode || 'N/A'}\nThông tin: ${raw.errorMessage || 'N/A'}`, menuKeyboard(ctx));
      }
    } catch (e) {
      await ctx.reply(`Lỗi tạo VA: ${e.response?.data?.errorMessage || e.message}`, menuKeyboard(ctx));
    }
  }

  bot.use(async (ctx, next) => {
    syncUsernameFromCtx(ctx);
    if (ctx.from && !isAdminId(ctx.from.id)) {
      const u = db.getUser(ctx.from.id);
      if (!u.isActive) {
        // Nếu là /start thì để bot.start xử lý, không chặn ở middleware
        if (ctx.message && ctx.message.text && ctx.message.text.startsWith('/start')) {
          return next();
        }
        if (ctx.message && ctx.message.text) {
          const name = ctx.from.first_name || ctx.from.username || 'User';
          const msg = `🔒 TÀI KHOẢN CHƯA ĐƯỢC DUYỆT!\n\n━━━━━━━━━━━━━━━━━━━━\n🆔 User ID của bạn:\n\`${ctx.from.id}\`\n👤 Tên: ${name}\n━━━━━━━━━━━━━━━━━━━━\n\n📋 Hướng dẫn:\n1️⃣ Copy ID ở trên (nhấn vào số ID)\n2️⃣ Gửi ID cho Admin *@lieunhuyenbet* để được duyệt\n3️⃣ Đợi Admin duyệt, bot sẽ thông báo\n\n💡 Gõ /start để xem thông tin bot.`;
          await ctx.replyWithMarkdown(msg, Markup.removeKeyboard());
        }
        return;
      }
    }
    return next();
  });

  const MAIN_MENU_CMDS = [
    '🎲 Random tên', '✍️ Nhập tên', '💰 Số dư', '🔎 Kiểm tra tài khoản', 
    '💸 Rút tiền', '📋 DS rút', '🔄 Cập nhật rút', '⚙️ Quản lý', '🗂 VA đã tạo', 
    'ℹ️ Thông tin', '🏧 Chi hộ', '📜 Lịch sử chi hộ', '/menu', '/start'
  ];

  bot.use(async (ctx, next) => {
    if (ctx.message && ctx.message.text) {
      syncUsernameFromCtx(ctx);
      const text = ctx.message.text.split('@')[0];
      if (MAIN_MENU_CMDS.includes(text)) {
        const id = ctx.from?.id;
        if (id) {
          awaitingName.delete(id);
          awaitingStatus.delete(id);
          withdrawState.delete(id);
          confirmCreateState.delete(id);
          bankSelectionState.delete(id);
          vaContentState.delete(id);
          randomNameState.delete(id);
          awaitingWdUpdate.delete(id);
        }
      }
    }
    return next();
  });

  bot.start(async (ctx) => {
    await syncCommandMenuForChat(ctx);
    syncUsernameFromCtx(ctx);
    const sendGuide = async () => {
      const guideMsg =
        `📌 HƯỚNG DẪN LẤY BANK (Cách sử dụng)\n\n` +
        `1️⃣ Nhấn "🎲 Random tên" để lấy nhanh một VA ngẫu nhiên.\n` +
        `2️⃣ Nhấn "✍️ Nhập tên" để tạo VA theo tên khách hàng mong muốn.\n` +
        `3️⃣ Nhấn "🔎 Kiểm tra tài khoản" để kiểm tra giao dịch của VA.\n` +
        `4️⃣ Nhấn "💸 Rút tiền" để yêu cầu rút số dư khả dụng.\n\n` +
        `(Liên hệ admin để kích hoạt ID @lieunhuyenbet)\n\n` +
        `Chào bạn! Vui lòng chọn thao tác bên dưới:`;
      await ctx.reply(guideMsg, menuKeyboard(ctx));
    };
    if (!isAdminId(ctx.from.id)) {
      const u = db.getUser(ctx.from.id);
      if (!u.isActive) {
         const name = ctx.from.first_name || ctx.from.username || 'User';
         const msg = `🔒 TÀI KHOẢN CHƯA ĐƯỢC DUYỆT!\n\n━━━━━━━━━━━━━━━━━━━━\n🆔 User ID của bạn:\n\`${ctx.from.id}\`\n👤 Tên: ${name}\n━━━━━━━━━━━━━━━━━━━━\n\n📋 Hướng dẫn:\n1️⃣ Copy ID ở trên (nhấn vào số ID)\n2️⃣ Gửi ID cho Admin *@lieunhuyenbet* để được duyệt\n3️⃣ Đợi Admin duyệt, bot sẽ thông báo\n\n💡 Gõ /start để xem thông tin bot.`;
         return ctx.replyWithMarkdown(msg, Markup.removeKeyboard());
      }
    }
    await sendGuide();
  });

const confirmCreateState = new Map();
const bankSelectionState = new Map();
const vaContentState = new Map();
const randomNameState = new Map();
const ibftState = new Map();

  function clearUserStates(id) {
    withdrawState.delete(id);
    awaitingName.delete(id);
    confirmCreateState.delete(id);
    bankSelectionState.delete(id);
    vaContentState.delete(id);
    randomNameState.delete(id);
    ibftState.delete(id);
    awaitingWdUpdate.delete(id);
    awaitingStatus.delete(id);
  }

  bot.action('cancel', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    clearUserStates(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('Đã hủy thao tác.', menuKeyboard(ctx));
  });

  bot.action('wd_back', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    clearUserStates(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('Menu:', menuKeyboard(ctx));
  });

  bot.action(/^wd_bank_page:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_bank') return;
    const page = Math.max(0, Number(ctx.match[1] || 0));
    withdrawState.set(ctx.from.id, { ...st, page });
    try {
      await ctx.editMessageReplyMarkup(buildWithdrawBankInlineKeyboard(page).reply_markup);
    } catch (_) {}
  });

  bot.action(/^wd_bank_pick:([A-Z0-9]{2,15})$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_bank') return;
    const code = String(ctx.match[1] || '').trim().toUpperCase();
    const b = IBFT_BANKS.find((x) => x.code === code);
    if (!b) return;
    withdrawState.set(ctx.from.id, { ...st, stage: 'bank_account', method: 'bank', bankCode: code, bankName: getIbftBankLabel(code) });
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply(`Đã chọn ngân hàng: ${getIbftBankLabel(code)}\nNhập số tài khoản:`, {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.action(/^wd_saved_use:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_saved') return;
    const idx = Number(ctx.match[1] || -1);
    const saved = getUserWithdrawBanks(ctx.from.id);
    const s = saved[idx];
    if (!s) return;
    withdrawState.set(ctx.from.id, {
      ...st,
      stage: 'amount',
      method: 'bank',
      bankCode: String(s.bankCode || '').trim().toUpperCase() || undefined,
      bankName: String(s.bankName || '').trim(),
      bankAccount: normalizeWdBankAccount(s.bankAccount),
      bankHolder: normalizeWdBankHolder(s.bankHolder),
    });
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('Nhập số tiền rút:', Markup.keyboard([['Rút ALL'], ['❌ Hủy']]).resize());
  });

  bot.action('wd_saved_other', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_saved') return;
    withdrawState.set(ctx.from.id, { stage: 'choose_bank', page: 0, balance: st.balance, method: 'bank' });
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('🏦 Chọn ngân hàng:', { reply_markup: buildWithdrawBankInlineKeyboard(0).reply_markup });
  });

  bot.action('wd_saved_delete_menu', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_saved') return;
    withdrawState.set(ctx.from.id, { ...st, stage: 'delete_saved' });
    try {
      await ctx.editMessageReplyMarkup(buildWithdrawDeleteKeyboard(ctx.from.id).reply_markup);
    } catch (_) {}
  });

  bot.action('wd_saved_back', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'delete_saved') return;
    withdrawState.set(ctx.from.id, { ...st, stage: 'choose_saved' });
    try {
      await ctx.editMessageReplyMarkup(buildWithdrawSavedKeyboard(ctx.from.id).reply_markup);
    } catch (_) {}
  });

  bot.action(/^wd_saved_del:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = withdrawState.get(ctx.from.id);
    if (!st || st.stage !== 'delete_saved') return;
    const idx = Number(ctx.match[1] || -1);
    deleteUserWithdrawBank(ctx.from.id, idx);
    try {
      await ctx.editMessageReplyMarkup(buildWithdrawDeleteKeyboard(ctx.from.id).reply_markup);
    } catch (_) {}
  });

  bot.action(/^rn_pick:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = randomNameState.get(ctx.from.id);
    if (!st || st.stage !== 'choose_option') return;
    const idx = Number(ctx.match[1]);
    const chosen = Array.isArray(st.options) ? st.options[idx] : '';
    if (!chosen) return;
    randomNameState.delete(ctx.from.id);
    confirmCreateState.set(ctx.from.id, chosen);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply(`Bạn chuẩn bị tạo VA với tên: *${chosen}*\n\nVui lòng xác nhận:`, {
      parse_mode: 'Markdown',
      reply_markup: Markup.inlineKeyboard([
        [Markup.button.callback('✅ Xác nhận tạo', 'va_confirm')],
        [Markup.button.callback('❌ Hủy', 'cancel')],
      ]).reply_markup,
    });
  });

  bot.action('va_confirm', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const name = confirmCreateState.get(ctx.from.id);
    if (!name) {
      await ctx.reply('Không tìm thấy thông tin xác nhận.', menuKeyboard(ctx));
      return;
    }
    bankSelectionState.set(ctx.from.id, { name });
    confirmCreateState.delete(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('Vui lòng chọn Ngân hàng để tiếp tục tạo:', {
      reply_markup: Markup.inlineKeyboard([
        [Markup.button.callback('🏦 MSB', 'va_bank:MSB'), Markup.button.callback('🏦 KLB', 'va_bank:KLB')],
        [Markup.button.callback('🏦 BIDV (BẢO TRÌ)', 'va_bank:BIDV')],
        [Markup.button.callback('❌ Hủy', 'cancel')],
      ]).reply_markup,
    });
  });

  bot.action(/^va_bank:(MSB|KLB|BIDV)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const bank = String(ctx.match[1] || '').toUpperCase();
    const st = bankSelectionState.get(ctx.from.id);
    if (!st || !st.name) return;
    if (bank === 'BIDV') {
      await ctx.reply('Ngân hàng BIDV hiện đang bảo trì. Vui lòng chọn ngân hàng khác.', {
        reply_markup: Markup.inlineKeyboard([
          [Markup.button.callback('🏦 MSB', 'va_bank:MSB'), Markup.button.callback('🏦 KLB', 'va_bank:KLB')],
          [Markup.button.callback('❌ Hủy', 'cancel')],
        ]).reply_markup,
      });
      return;
    }
    bankSelectionState.delete(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    vaContentState.set(ctx.from.id, { name: st.name, bank });
    await ctx.reply('Nhập nội dung chuyển khoản (ví dụ: ND 123456 ABC):', {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.hears('🎲 Random tên', async (ctx) => {
    randomNameState.set(ctx.from.id, { stage: 'enter_prefix' });
    await ctx.reply('Nhập Họ và Tên đệm (Ví dụ: LE VAN):', {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.hears('✍️ Nhập tên', async (ctx) => {
    awaitingName.set(ctx.from.id, true);
    await ctx.reply('Vui lòng nhập họ và tên:', {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.hears('✅ Xác nhận tạo', async (ctx) => {
    const name = confirmCreateState.get(ctx.from.id);
    if (!name) {
      await ctx.reply('Không tìm thấy thông tin xác nhận.', menuKeyboard(ctx));
      return;
    }
    bankSelectionState.set(ctx.from.id, { name });
    confirmCreateState.delete(ctx.from.id);
    await ctx.reply('Vui lòng chọn Ngân hàng để tiếp tục tạo:', {
      reply_markup: Markup.inlineKeyboard([
        [Markup.button.callback('🏦 MSB', 'va_bank:MSB'), Markup.button.callback('🏦 KLB', 'va_bank:KLB')],
        [Markup.button.callback('🏦 BIDV (BẢO TRÌ)', 'va_bank:BIDV')],
        [Markup.button.callback('❌ Hủy', 'cancel')],
      ]).reply_markup,
    });
  });

  bot.hears('🏦 MSB', async (ctx) => {
    const state = bankSelectionState.get(ctx.from.id);
    if (!state) return;
    bankSelectionState.delete(ctx.from.id);
    vaContentState.set(ctx.from.id, { name: state.name, bank: 'MSB' });
    await ctx.reply('Nhập nội dung chuyển khoản (ví dụ: ND 123456 ABC):', {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.hears('🏦 KLB', async (ctx) => {
    const state = bankSelectionState.get(ctx.from.id);
    if (!state) return;
    bankSelectionState.delete(ctx.from.id);
    vaContentState.set(ctx.from.id, { name: state.name, bank: 'KLB' });
    await ctx.reply('Nhập nội dung chuyển khoản (ví dụ: ND 123456 ABC):', {
      reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
    });
  });

  bot.hears('🏦 BIDV (BẢO TRÌ)', async (ctx) => {
    const state = bankSelectionState.get(ctx.from.id);
    if (!state) return;
    await ctx.reply('Ngân hàng BIDV hiện đang bảo trì. Vui lòng chọn ngân hàng khác:', Markup.keyboard([
      ['🏦 MSB', '🏦 KLB'],
      ['🏦 BIDV (BẢO TRÌ)'],
      ['❌ Hủy']
    ]).resize());
  });

  bot.command('menu', async (ctx) => {
    awaitingName.delete(ctx.from.id);
    await syncCommandMenuForChat(ctx);
    await ctx.reply('Menu:', menuKeyboard(ctx));
  });

  const { getAccountBalance } = require('./hpayClient');
  bot.hears('💰 Số dư', async (ctx) => {
    if (!isAdminId(ctx.from.id)) {
      await ctx.reply('Bạn không có quyền dùng chức năng này.', menuKeyboard(ctx));
      return;
    }
    try {
      await ctx.reply('Đang kiểm tra số dư...', menuKeyboard(ctx));
      const msb = {
        merchantIdOverride: (process.env.HPAY_MERCHANT_ID_MSB || '').trim() || undefined,
        passcodeOverride: (process.env.HPAY_PASSCODE_MSB || '').trim() || undefined,
        clientIdOverride: (process.env.HPAY_CLIENT_ID_MSB || '').trim() || undefined,
        clientSecretOverride: (process.env.HPAY_CLIENT_SECRET_MSB || '').trim() || undefined,
        xApiMidOverride: (process.env.HPAY_X_API_MID_MSB || '').trim() || undefined,
      };
      const hasMsb = msb.merchantIdOverride && msb.passcodeOverride;
      const cfg = hasMsb ? msb : {};
      const { decoded } = await getAccountBalance(cfg);
      const balStr = String(decoded?.balance || '').trim();
      const balNum = toAmountNumber(balStr);
      db.updateUser(ctx.from.id, { balance: balNum });
      try {
        db.addBalanceHistory({ ts: Date.now(), balance: balNum, balanceRaw: balStr, source: 'MSB', adminId: ctx.from.id });
      } catch (_) {}
      await ctx.reply(`Số Dư: ${balStr || balNum.toLocaleString()}`, menuKeyboard(ctx));
    } catch (e) {
      const msg = e.response?.data?.errorMessage || e.message;
      await ctx.reply(`Lỗi lấy số dư: ${msg}`, menuKeyboard(ctx));
    }
  });

  const { createIBFT } = require('./hpayClient');
  bot.hears('🏧 Chi hộ', async (ctx) => {
    if (!canUseIbft(ctx.from.id)) {
      await ctx.reply('Bạn không có quyền dùng chức năng này.', menuKeyboard(ctx));
      return;
    }
    const hasMsb = String(process.env.HPAY_MERCHANT_ID_MSB || '').trim() && String(process.env.HPAY_PASSCODE_MSB || '').trim();
    const hasKlb = String(process.env.HPAY_MERCHANT_ID_KLB || '').trim() && String(process.env.HPAY_PASSCODE_KLB || '').trim();
    if (hasMsb && !hasKlb) {
      ibftState.set(ctx.from.id, { stage: 'enter_bank', page: 0, merchant: 'MSB' });
      await ctx.reply('🏦 Chọn ngân hàng:', buildIbftBankKeyboard(0));
      return;
    }
    if (hasKlb && !hasMsb) {
      ibftState.set(ctx.from.id, { stage: 'enter_bank', page: 0, merchant: 'KLB' });
      await ctx.reply('🏦 Chọn ngân hàng:', buildIbftBankKeyboard(0));
      return;
    }
    ibftState.set(ctx.from.id, { stage: 'pick_source' });
    await ctx.reply('Chọn nguồn chi hộ:', {
      reply_markup: Markup.inlineKeyboard([
        [Markup.button.callback('🏦 MSB', 'ibft_src:MSB'), Markup.button.callback('🏦 KLB', 'ibft_src:KLB')],
        [Markup.button.callback('❌ Hủy', 'cancel')],
      ]).reply_markup,
    });
  });

  bot.action(/^ibft_src:(MSB|KLB)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!canUseIbft(ctx.from.id)) return;
    const st = ibftState.get(ctx.from.id);
    if (!st || st.stage !== 'pick_source') return;
    const merchant = String(ctx.match[1] || '').trim().toUpperCase();
    ibftState.set(ctx.from.id, { stage: 'enter_bank', page: 0, merchant });
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await ctx.reply('🏦 Chọn ngân hàng:', buildIbftBankKeyboard(0));
  });

  function getIbftMerchantConfig(merchantOverride) {
    const pref = String(merchantOverride || process.env.IBFT_MERCHANT || '').trim().toUpperCase();
    const hasMsb = String(process.env.HPAY_MERCHANT_ID_MSB || '').trim() && String(process.env.HPAY_PASSCODE_MSB || '').trim();
    const hasKlb = String(process.env.HPAY_MERCHANT_ID_KLB || '').trim() && String(process.env.HPAY_PASSCODE_KLB || '').trim();
    const pick = pref === 'KLB' ? 'KLB' : pref === 'MSB' ? 'MSB' : hasMsb ? 'MSB' : hasKlb ? 'KLB' : '';
    if (pick === 'MSB') {
      const merchantIdOverride = (process.env.HPAY_MERCHANT_ID_MSB || '').trim() || undefined;
      const passcodeOverride = (process.env.HPAY_PASSCODE_MSB || '').trim() || undefined;
      const clientIdOverride = (process.env.HPAY_CLIENT_ID_MSB || '').trim() || undefined;
      const clientSecretOverride = (process.env.HPAY_CLIENT_SECRET_MSB || '').trim() || undefined;
      const xApiMidOverride = (process.env.HPAY_X_API_MID_MSB || '').trim() || merchantIdOverride || undefined;
      return { merchantIdOverride, passcodeOverride, clientIdOverride, clientSecretOverride, xApiMidOverride, merchantLabel: 'MSB' };
    }
    if (pick === 'KLB') {
      const merchantIdOverride = (process.env.HPAY_MERCHANT_ID_KLB || '').trim() || undefined;
      const passcodeOverride = (process.env.HPAY_PASSCODE_KLB || '').trim() || undefined;
      const clientIdOverride = (process.env.HPAY_CLIENT_ID_KLB || '').trim() || undefined;
      const clientSecretOverride = (process.env.HPAY_CLIENT_SECRET_KLB || '').trim() || undefined;
      const xApiMidOverride = (process.env.HPAY_X_API_MID_KLB || '').trim() || merchantIdOverride || undefined;
      return { merchantIdOverride, passcodeOverride, clientIdOverride, clientSecretOverride, xApiMidOverride, merchantLabel: 'KLB' };
    }
    return { merchantIdOverride: undefined, passcodeOverride: undefined, clientIdOverride: undefined, clientSecretOverride: undefined, xApiMidOverride: undefined, merchantLabel: '' };
  }

  async function runIbftConfirm(ctx) {
    if (!canUseIbft(ctx.from.id)) {
      await ctx.reply('Bạn không có quyền dùng chức năng này.', menuKeyboard(ctx));
      return;
    }
    const st = ibftState.get(ctx.from.id);
    if (!st || st.stage !== 'confirm') {
      await ctx.reply('Phiên chi hộ không còn hợp lệ. Vui lòng thao tác lại.', menuKeyboard(ctx));
      return;
    }
    const bankCode = st.bankCode;
    const accountNumber = st.accountNumber;
    const accountName = st.accountName;
    const amount = st.amount;
    if (!bankCode || !accountNumber || !accountName || !amount) {
      ibftState.delete(ctx.from.id);
      await ctx.reply('Thiếu thông tin chi hộ. Vui lòng thao tác lại.', menuKeyboard(ctx));
      return;
    }
    try {
      await ctx.reply('Đang xử lý chi hộ...', menuKeyboard(ctx));
    } catch (_) {}

    try {
      const remark = st.remark || `CH ${Date.now().toString().slice(-8)}`;
      const callbackUrl = (process.env.IBFT_CALLBACK_URL || '').trim();
      const cfg = getIbftMerchantConfig(st.merchant);

      const { decoded, raw } = await createIBFT({
        bankCode,
        bankName: bankCode,
        accountNumber,
        accountName,
        amount,
        remark,
        callbackUrl,
        merchantIdOverride: cfg.merchantIdOverride,
        passcodeOverride: cfg.passcodeOverride,
        clientIdOverride: cfg.clientIdOverride,
        clientSecretOverride: cfg.clientSecretOverride,
        xApiMidOverride: cfg.xApiMidOverride,
      });

      ibftState.delete(ctx.from.id);
      const lines = [];
      if (cfg.merchantLabel) lines.push(`Nguồn: ${cfg.merchantLabel}`);
      if (decoded && typeof decoded === 'object') {
        if (decoded.orderId) lines.push(`OrderId: ${decoded.orderId}`);
        if (decoded.tranStatus) lines.push(`Trạng thái: ${decoded.tranStatus}`);
      }
      const rawErrCode = raw ? String(raw.errorCode || raw.error_code || '') : '';
      const rawErrMsg = raw ? String(raw.errorMessage || raw.error_message || raw.message || '') : '';
      const isOk = rawErrCode === '' || rawErrCode === '00' || String(decoded?.tranStatus || '').toLowerCase() === 'success';
      try {
        db.addIbftHistory({
          ts: Date.now(),
          adminId: ctx.from.id,
          merchant: cfg.merchantLabel,
          bankCode,
          accountNumber,
          accountName,
          amount,
          remark,
          orderId: decoded?.orderId || '',
          tranStatus: decoded?.tranStatus || '',
          errorCode: isOk ? '' : rawErrCode,
          errorMessage: isOk ? '' : rawErrMsg,
        });
      } catch (_) {}
      if (!isOk) {
        lines.push('Kết quả: Sai thông tin tài khoản hoặc ngân hàng không hợp lệ.');
        if (rawErrCode) lines.push(`Mã lỗi: ${rawErrCode}`);
        if (rawErrMsg) lines.push(`Thông tin: ${rawErrMsg}`);
      } else if (!lines.length) {
        lines.push('Đã tạo yêu cầu chi hộ.');
      }
      await ctx.reply(lines.join('\n') || 'Đã tạo yêu cầu chi hộ.', menuKeyboard(ctx));
    } catch (e) {
      const stNow = ibftState.get(ctx.from.id) || {};
      ibftState.delete(ctx.from.id);
      const data = e.response?.data || {};
      const code = String(data.errorCode || data.error_code || '');
      const msg = String(data.errorMessage || data.error_message || data.message || e.message || '');
      try {
        db.addIbftHistory({
          ts: Date.now(),
          adminId: ctx.from.id,
          merchant: String(stNow.merchant || ''),
          bankCode: String(stNow.bankCode || ''),
          accountNumber: String(stNow.accountNumber || ''),
          accountName: String(stNow.accountName || ''),
          amount: Number(stNow.amount) || 0,
          remark: String(stNow.remark || ''),
          orderId: '',
          tranStatus: '',
          errorCode: code,
          errorMessage: msg,
        });
      } catch (_) {}
      if (code || msg) {
        await ctx.reply(`Sai thông tin tài khoản hoặc ngân hàng không hợp lệ.\n${code ? `Mã lỗi: ${code}\n` : ''}${msg ? `Thông tin: ${msg}` : ''}`.trim(), menuKeyboard(ctx));
      } else {
        await ctx.reply(`Lỗi chi hộ: ${e.message}`, menuKeyboard(ctx));
      }
    }
  }

  bot.action('ibft_confirm', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
    await runIbftConfirm(ctx);
  });

  bot.hears('✅ Xác nhận chi hộ', async (ctx) => {
    await runIbftConfirm(ctx);
  });

  bot.hears('ℹ️ Thông tin', async (ctx) => {
    const computed = computeUserBalanceFromRecords(ctx.from.id);
    db.updateUser(ctx.from.id, { balance: computed.balance });
    const user = db.getUser(ctx.from.id);
    const config = db.getConfig();
    const ipnFeeFlatBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
    const wdFeeFlatBase = Math.max(0, Number(config.withdrawFeeFlat ?? 0) || 0);
    const ipnFeeUser = user.ipnFeeFlat !== null && user.ipnFeeFlat !== undefined ? Number(user.ipnFeeFlat) : NaN;
    const wdFeeUser = user.withdrawFeeFlat !== null && user.withdrawFeeFlat !== undefined ? Number(user.withdrawFeeFlat) : NaN;
    const ipnFeeFlat = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeFlatBase;
    const wdFeeFlat = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeFlatBase;
    const feePercent = user.feePercent !== null ? user.feePercent : config.globalFeePercent;
    
    const msg = `ℹ️ THÔNG TIN TÀI KHOẢN\n\n` +
      `👤 ID: \`${user.id}\`\n` +
      `💰 Tổng số dư: ${user.balance.toLocaleString()}đ\n` +
      `💸 Phí chuyển: ${wdFeeFlat.toLocaleString()}đ\n` +
      `🧾 Phí giao dịch: ${ipnFeeFlat.toLocaleString()}đ\n` +
      `📉 Phí rút: ${feePercent}%\n` +
      `📊 Tổng số VA đã tạo: ${user.createdVA}\n` +
      `🚫 Giới hạn tạo VA: ${user.vaLimit !== null ? user.vaLimit : 'Không giới hạn'}`;
      
    await ctx.replyWithMarkdown(msg, menuKeyboard(ctx));
  });

  bot.hears('🗂 VA đã tạo', async (ctx) => {
    const all = db.getVAsByUser(ctx.from.id, 1000);
    if (all.length === 0) return ctx.reply('Bạn chưa tạo VA nào.', menuKeyboard(ctx));
    const pageSize = 8;
    const pageCount = Math.max(1, Math.ceil(all.length / pageSize));
    const page = 0;
    vaListState.set(ctx.from.id, { page });
    const slice = all.slice(page * pageSize, page * pageSize + pageSize);
    const lines = [];
    lines.push(`🗂 VA đã tạo (${page + 1}/${pageCount})`);
    lines.push('');
    for (const v of slice) {
      const statusRaw = String(v.status || '').trim().toLowerCase();
      const statusLabel = statusRaw === 'paid' ? '✅ paid' : statusRaw === 'unpaid' ? '⏳ unpaid' : statusRaw || 'N/A';
      const amt = statusRaw === 'paid' ? (v.netAmount || v.amount || '0') : (v.vaAmount || '0');
      const amtNum = toAmountNumber(amt);
      const amtStr = amtNum ? `${amtNum.toLocaleString()}đ` : String(amt || '0');
      const name = String(v.name || v.customerName || 'N/A').trim();
      const bank = String(v.vaBank || 'N/A').trim();
      const acc = String(v.vaAccount || 'N/A').trim();
      const rid = String(v.requestId || '').trim();
      lines.push(`• ${statusLabel} | ${amtStr}`);
      lines.push(`  🏦 ${bank} | 💳 ${acc}`);
      lines.push(`  👤 ${name}`);
      if (rid) lines.push(`  🆔 ${rid}`);
      lines.push('');
    }
    const nav = [];
    if (page > 0) nav.push(Markup.button.callback('⬅️ Trước', `va_list:${page - 1}`));
    if (page < pageCount - 1) nav.push(Markup.button.callback('Sau ➡️', `va_list:${page + 1}`));
    const kbRows = [];
    if (nav.length) kbRows.push(nav);
    kbRows.push([Markup.button.callback('❌ Đóng', 'va_list_close')]);
    await ctx.replyWithMarkdown(lines.join('\n').trim(), { reply_markup: Markup.inlineKeyboard(kbRows).reply_markup });
  });

  bot.action(/^va_list:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    const st = vaListState.get(ctx.from.id);
    if (!st) return;
    const all = db.getVAsByUser(ctx.from.id, 1000);
    const pageSize = 8;
    const pageCount = Math.max(1, Math.ceil(all.length / pageSize));
    const page = Math.max(0, Math.min(Number(ctx.match[1] || 0), pageCount - 1));
    vaListState.set(ctx.from.id, { page });
    const slice = all.slice(page * pageSize, page * pageSize + pageSize);
    const lines = [];
    lines.push(`🗂 VA đã tạo (${page + 1}/${pageCount})`);
    lines.push('');
    for (const v of slice) {
      const statusRaw = String(v.status || '').trim().toLowerCase();
      const statusLabel = statusRaw === 'paid' ? '✅ paid' : statusRaw === 'unpaid' ? '⏳ unpaid' : statusRaw || 'N/A';
      const amt = statusRaw === 'paid' ? (v.netAmount || v.amount || '0') : (v.vaAmount || '0');
      const amtNum = toAmountNumber(amt);
      const amtStr = amtNum ? `${amtNum.toLocaleString()}đ` : String(amt || '0');
      const name = String(v.name || v.customerName || 'N/A').trim();
      const bank = String(v.vaBank || 'N/A').trim();
      const acc = String(v.vaAccount || 'N/A').trim();
      const rid = String(v.requestId || '').trim();
      lines.push(`• ${statusLabel} | ${amtStr}`);
      lines.push(`  🏦 ${bank} | 💳 ${acc}`);
      lines.push(`  👤 ${name}`);
      if (rid) lines.push(`  🆔 ${rid}`);
      lines.push('');
    }
    const nav = [];
    if (page > 0) nav.push(Markup.button.callback('⬅️ Trước', `va_list:${page - 1}`));
    if (page < pageCount - 1) nav.push(Markup.button.callback('Sau ➡️', `va_list:${page + 1}`));
    const kbRows = [];
    if (nav.length) kbRows.push(nav);
    kbRows.push([Markup.button.callback('❌ Đóng', 'va_list_close')]);
    try {
      await ctx.editMessageText(lines.join('\n').trim(), { parse_mode: 'Markdown', reply_markup: Markup.inlineKeyboard(kbRows).reply_markup });
    } catch (_) {}
  });

  bot.action('va_list_close', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    vaListState.delete(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
  });

  bot.hears('⚙️ Quản lý', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const msg = `Lệnh quản lý (gõ trực tiếp):\n\n` +
      `/active <id> : Kích hoạt user\n` +
      `/deactive <id> : Hủy kích hoạt\n` +
      `/setfee <id> <%> : Set % phí cho user\n` +
      `/setfee all <%> : Set % phí chung\n` +
      `/setipnfee <số> : Set phí chuyển rút tiền (VNĐ)\n` +
      `/setipnfeeall <số> : Set phí giao dịch tiền về (VNĐ) cho tất cả\n` +
      `/setwdfeeall <số> : Set phí chuyển rút tiền (VNĐ) cho tất cả\n` +
      `/setipnfeeuser <id> <số> : Set phí giao dịch tiền về (VNĐ) theo user\n` +
      `/setwdfeeuser <id> <số> : Set phí chuyển rút tiền (VNĐ) theo user\n` +
      `/ibfthist [n] : Lịch sử chi hộ\n` +
      `/ibftexport ... : Xuất excel chi hộ\n` +
      `/balhist [n] : Xem lịch sử số dư admin\n` +
      `/user <id> : Xem thông tin user\n` +
      `/uhist <id> [n] : Xem lịch sử số dư user\n` +
      `/setlimit <id> <số> : Set giới hạn VA cho user\n` +
      `/users : Xem danh sách user\n` +
      `/usersexport : Xuất excel user\n` +
      `/admins : Xem danh sách admin`;
    await ctx.reply(msg, menuKeyboard(ctx));
  });

  bot.command('active', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const id = ctx.message.text.split(' ')[1];
    if (!id) return ctx.reply('Cú pháp: /active <id>');
    db.updateUser(id, { isActive: true });
    await ctx.reply(`Đã kích hoạt user ${id}`);
    try { await bot.telegram.sendMessage(id, 'Tài khoản của bạn đã được kích hoạt!', menuKeyboard({from: {id}})); } catch(_) {}
  });

  bot.command('deactive', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const id = ctx.message.text.split(' ')[1];
    if (!id) return ctx.reply('Cú pháp: /deactive <id>');
    db.updateUser(id, { isActive: false });
    await ctx.reply(`Đã hủy kích hoạt user ${id}`);
  });

  bot.command('setfee', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = ctx.message.text.split(' ');
    const id = parts[1];
    const fee = parseFloat(parts[2]);
    if (!id || isNaN(fee)) return ctx.reply('Cú pháp: /setfee <id|all> <%>');
    if (id === 'all') {
      db.updateConfig({ globalFeePercent: fee });
      await ctx.reply(`Đã set phí chung: ${fee}%`);
    } else {
      db.updateUser(id, { feePercent: fee });
      await ctx.reply(`Đã set phí cho user ${id}: ${fee}%`);
    }
  });

  bot.command('setipnfee', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = ctx.message.text.split(' ');
    const fee = Number(String(parts[1] || '').replace(/[^\d]/g, ''));
    if (!Number.isFinite(fee) || fee < 0) return ctx.reply('Cú pháp: /setipnfee <số>');
    db.updateConfig({ withdrawFeeFlat: fee, ipnFeeFlat: fee });
    await ctx.reply(`Đã set phí chuyển rút tiền: ${fee.toLocaleString()}đ`);
  });

  bot.command('setipnfeeall', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const fee = Number(String(parts[1] || '').replace(/[^\d]/g, ''));
    if (!Number.isFinite(fee) || fee < 0) {
      await ctx.reply('Cú pháp: /setipnfeeall <số>', menuKeyboard(ctx));
      return;
    }
    db.updateConfig({ ipnFeeFlat: fee });
    await ctx.reply(`Đã set phí giao dịch tiền về (tất cả): ${fee.toLocaleString()}đ`, menuKeyboard(ctx));
  });

  bot.command('setwdfeeall', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const fee = Number(String(parts[1] || '').replace(/[^\d]/g, ''));
    if (!Number.isFinite(fee) || fee < 0) {
      await ctx.reply('Cú pháp: /setwdfeeall <số>', menuKeyboard(ctx));
      return;
    }
    db.updateConfig({ withdrawFeeFlat: fee });
    await ctx.reply(`Đã set phí chuyển rút tiền (tất cả): ${fee.toLocaleString()}đ`, menuKeyboard(ctx));
  });

  bot.command('setipnfeeuser', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const id = String(parts[1] || '').trim();
    const fee = Number(String(parts[2] || '').replace(/[^\d]/g, ''));
    if (!id || !Number.isFinite(fee) || fee < 0) {
      await ctx.reply('Cú pháp: /setipnfeeuser <id> <số>', menuKeyboard(ctx));
      return;
    }
    db.updateUser(id, { ipnFeeFlat: fee });
    await ctx.reply(`Đã set phí giao dịch tiền về cho user ${id}: ${fee.toLocaleString()}đ`, menuKeyboard(ctx));
  });

  bot.command('setwdfeeuser', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const id = String(parts[1] || '').trim();
    const fee = Number(String(parts[2] || '').replace(/[^\d]/g, ''));
    if (!id || !Number.isFinite(fee) || fee < 0) {
      await ctx.reply('Cú pháp: /setwdfeeuser <id> <số>', menuKeyboard(ctx));
      return;
    }
    db.updateUser(id, { withdrawFeeFlat: fee });
    await ctx.reply(`Đã set phí chuyển rút tiền cho user ${id}: ${fee.toLocaleString()}đ`, menuKeyboard(ctx));
  });

  bot.command('setlimit', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = ctx.message.text.split(' ');
    const id = parts[1];
    const limit = parseInt(parts[2]);
    if (!id || isNaN(limit)) return ctx.reply('Cú pháp: /setlimit <id> <số>');
    db.updateUser(id, { vaLimit: limit });
    await ctx.reply(`Đã set giới hạn VA cho user ${id}: ${limit}`);
  });

  bot.command('users', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const arg = String(parts[1] || '').trim();
    if (arg) {
      const numeric = arg.replace(/[^\d]/g, '');
      if (numeric.length >= 8) {
        const u = db.findUser(numeric);
        if (!u) {
          await ctx.reply('Không tìm thấy user này.', menuKeyboard(ctx));
          return;
        }
        const config = db.getConfig();
        const ipnFeeBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
        const wdFeeBase = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);
        const feePercentBase = Number(config.globalFeePercent || 0);
        const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
        const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
        const feePercentUser = u.feePercent !== null && u.feePercent !== undefined ? Number(u.feePercent) : NaN;
        const ipnFee = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeBase;
        const wdFee = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeBase;
        const feePercent = Number.isFinite(feePercentUser) ? feePercentUser : feePercentBase;
        const lines = [];
        lines.push('👤 *THÔNG TIN USER*');
        lines.push('');
      const uname = String(u.username || '').trim();
      if (uname) lines.push(`👤 Username: *@${escapeMd(uname)}*`);
        lines.push(`🆔 ID: \`${u.id}\``);
        lines.push(`📌 Trạng thái: *${u.isActive ? 'ACTIVE' : 'INACTIVE'}*`);
        lines.push(`💰 Số dư: *${Number(u.balance || 0).toLocaleString()}đ*`);
        lines.push('');
        lines.push(`🧾 Phí giao dịch (IPN): *${ipnFee.toLocaleString()}đ*${Number.isFinite(ipnFeeUser) ? ' (user)' : ''}`);
        lines.push(`💸 Phí chuyển (rút): *${wdFee.toLocaleString()}đ*${Number.isFinite(wdFeeUser) ? ' (user)' : ''}`);
        lines.push(`📉 Phí rút (%): *${feePercent}%*${Number.isFinite(feePercentUser) ? ' (user)' : ''}`);
        lines.push('');
        lines.push(`📊 VA: ${u.createdVA}/${u.vaLimit !== null ? u.vaLimit : '∞'}`);
        await ctx.replyWithMarkdown(lines.join('\n'), menuKeyboard(ctx));
        return;
      }
    }

    const page = Math.max(1, Number(arg || 1) || 1);
    const config = db.getConfig();
    const users = db.getAllUsers().slice().sort((a, b) => String(a.id).localeCompare(String(b.id)));
    if (users.length === 0) return ctx.reply('Chưa có user nào.');
    const pageSize = 10;
    const pageCount = Math.max(1, Math.ceil(users.length / pageSize));
    const p = Math.max(1, Math.min(page, pageCount));
    usersListState.set(ctx.from.id, { page: p });

    const ipnFeeBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
    const wdFeeBase = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);

    const slice = users.slice((p - 1) * pageSize, (p - 1) * pageSize + pageSize);
    const lines = [];
    lines.push(`👥 Danh sách User (${p}/${pageCount})`);
    lines.push('');
    for (const u of slice) {
      const uname = String(u.username || '').trim();
      const unameStr = uname ? `@${uname}` : '-';
      const feePercent = u.feePercent !== null ? u.feePercent : config.globalFeePercent;
      const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
      const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
      const ipnFee = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeBase;
      const wdFee = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeBase;
      lines.push(
        `${unameStr} | ${u.id} | ${u.isActive ? 'Active' : 'Inactive'} | Dư: ${Number(u.balance || 0).toLocaleString()}đ | VA: ${u.createdVA}/${u.vaLimit !== null ? u.vaLimit : '∞'}`
      );
      lines.push(`%: ${feePercent}% | IPN: ${ipnFee.toLocaleString()}đ | WD: ${wdFee.toLocaleString()}đ`);
      lines.push('');
    }

    const nav = [];
    if (p > 1) nav.push(Markup.button.callback('⬅️ Trước', `users_list:${p - 1}`));
    if (p < pageCount) nav.push(Markup.button.callback('Sau ➡️', `users_list:${p + 1}`));
    const rows = [];
    if (nav.length) rows.push(nav);
    rows.push([Markup.button.callback('📤 Xuất Excel', 'users_export')]);
    rows.push([Markup.button.callback('❌ Đóng', 'users_list_close')]);
    await ctx.reply(lines.join('\n').trim(), { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
  });

  bot.action(/^users_list:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    const config = db.getConfig();
    const users = db.getAllUsers().slice().sort((a, b) => String(a.id).localeCompare(String(b.id)));
    if (!users.length) return;
    const pageSize = 10;
    const pageCount = Math.max(1, Math.ceil(users.length / pageSize));
    const p = Math.max(1, Math.min(Number(ctx.match[1] || 1), pageCount));
    usersListState.set(ctx.from.id, { page: p });

    const ipnFeeBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
    const wdFeeBase = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);

    const slice = users.slice((p - 1) * pageSize, (p - 1) * pageSize + pageSize);
    const lines = [];
    lines.push(`👥 Danh sách User (${p}/${pageCount})`);
    lines.push('');
    for (const u of slice) {
      const uname = String(u.username || '').trim();
      const unameStr = uname ? `@${uname}` : '-';
      const feePercent = u.feePercent !== null ? u.feePercent : config.globalFeePercent;
      const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
      const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
      const ipnFee = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeBase;
      const wdFee = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeBase;
      lines.push(
        `${unameStr} | ${u.id} | ${u.isActive ? 'Active' : 'Inactive'} | Dư: ${Number(u.balance || 0).toLocaleString()}đ | VA: ${u.createdVA}/${u.vaLimit !== null ? u.vaLimit : '∞'}`
      );
      lines.push(`%: ${feePercent}% | IPN: ${ipnFee.toLocaleString()}đ | WD: ${wdFee.toLocaleString()}đ`);
      lines.push('');
    }

    const nav = [];
    if (p > 1) nav.push(Markup.button.callback('⬅️ Trước', `users_list:${p - 1}`));
    if (p < pageCount) nav.push(Markup.button.callback('Sau ➡️', `users_list:${p + 1}`));
    const rows = [];
    if (nav.length) rows.push(nav);
    rows.push([Markup.button.callback('📤 Xuất Excel', 'users_export')]);
    rows.push([Markup.button.callback('❌ Đóng', 'users_list_close')]);
    try {
      await ctx.editMessageText(lines.join('\n').trim(), { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
    } catch (_) {}
  });

  bot.action('users_list_close', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    usersListState.delete(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
  });

  bot.action('users_export', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    try {
      const config = db.getConfig();
      const ipnFeeBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
      const wdFeeBase = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);
      const feePercentBase = Number(config.globalFeePercent || 0);

      const users = db.getAllUsers().slice().sort((a, b) => String(a.id).localeCompare(String(b.id)));
      const headers = [
        'id',
        'username',
        'isActive',
        'balance',
        'createdVA',
        'vaLimit',
        'feePercent_effective',
        'feePercent_user',
        'ipnFee_effective',
        'ipnFee_user',
        'withdrawFee_effective',
        'withdrawFee_user',
      ];
      const rows = users.map((u) => {
        const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
        const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
        const feePercentUser = u.feePercent !== null && u.feePercent !== undefined ? Number(u.feePercent) : NaN;
        const ipnFee = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeBase;
        const wdFee = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeBase;
        const feePercent = Number.isFinite(feePercentUser) ? feePercentUser : feePercentBase;
        return [
          String(u.id),
          String(u.username || ''),
          u.isActive ? '1' : '0',
          String(Number(u.balance || 0)),
          String(Number(u.createdVA || 0)),
          u.vaLimit === null || u.vaLimit === undefined ? '' : String(u.vaLimit),
          String(feePercent),
          Number.isFinite(feePercentUser) ? String(feePercentUser) : '',
          String(ipnFee),
          Number.isFinite(ipnFeeUser) ? String(ipnFeeUser) : '',
          String(wdFee),
          Number.isFinite(wdFeeUser) ? String(wdFeeUser) : '',
        ];
      });
      const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
      const filename = `users_${stamp}.csv`;
      const filePath = writeCsvFile(filename, headers, rows);
      await bot.telegram.sendDocument(ctx.chat.id, { source: fs.createReadStream(filePath), filename }, { caption: `Xuất user: ${users.length} dòng` });
      try {
        fs.unlinkSync(filePath);
      } catch (_) {}
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel user: ${e.message}`, menuKeyboard(ctx));
    }
  });

  bot.command('usersexport', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    try {
      const config = db.getConfig();
      const ipnFeeBase = Math.max(0, Number(config.ipnFeeFlat ?? 0) || 0);
      const wdFeeBase = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);
      const feePercentBase = Number(config.globalFeePercent || 0);

      const users = db.getAllUsers().slice().sort((a, b) => String(a.id).localeCompare(String(b.id)));
      const headers = [
        'id',
        'username',
        'isActive',
        'balance',
        'createdVA',
        'vaLimit',
        'feePercent_effective',
        'feePercent_user',
        'ipnFee_effective',
        'ipnFee_user',
        'withdrawFee_effective',
        'withdrawFee_user',
      ];
      const rows = users.map((u) => {
        const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
        const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
        const feePercentUser = u.feePercent !== null && u.feePercent !== undefined ? Number(u.feePercent) : NaN;
        const ipnFee = Number.isFinite(ipnFeeUser) && ipnFeeUser >= 0 ? ipnFeeUser : ipnFeeBase;
        const wdFee = Number.isFinite(wdFeeUser) && wdFeeUser >= 0 ? wdFeeUser : wdFeeBase;
        const feePercent = Number.isFinite(feePercentUser) ? feePercentUser : feePercentBase;
        return [
          String(u.id),
          String(u.username || ''),
          u.isActive ? '1' : '0',
          String(Number(u.balance || 0)),
          String(Number(u.createdVA || 0)),
          u.vaLimit === null || u.vaLimit === undefined ? '' : String(u.vaLimit),
          String(feePercent),
          Number.isFinite(feePercentUser) ? String(feePercentUser) : '',
          String(ipnFee),
          Number.isFinite(ipnFeeUser) ? String(ipnFeeUser) : '',
          String(wdFee),
          Number.isFinite(wdFeeUser) ? String(wdFeeUser) : '',
        ];
      });
      const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
      const filename = `users_${stamp}.csv`;
      const filePath = writeCsvFile(filename, headers, rows);
      await ctx.reply('Đang tạo file...', menuKeyboard(ctx));
      await bot.telegram.sendDocument(ctx.chat.id, { source: fs.createReadStream(filePath), filename }, { caption: `Xuất user: ${users.length} dòng` });
      try {
        fs.unlinkSync(filePath);
      } catch (_) {}
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel user: ${e.message}`, menuKeyboard(ctx));
    }
  });

  bot.command('admins', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const adminIds = String(process.env.ADMIN_IDS || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    const ibftIds = String(process.env.IBFT_ADMIN_IDS || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    const lines = [];
    lines.push(`ADMIN_IDS: ${adminIds.length ? adminIds.join(', ') : '(trống)'}`);
    lines.push(`IBFT_ADMIN_IDS: ${ibftIds.length ? ibftIds.join(', ') : '(trống)'}`);
    await ctx.reply(lines.join('\n'), menuKeyboard(ctx));
  });

  bot.command('balhist', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const limit = Math.max(1, Math.min(50, Number(parts[1] || 20) || 20));
    const items = db.getBalanceHistory(limit);
    if (!items.length) {
      await ctx.reply('Chưa có lịch sử số dư.', menuKeyboard(ctx));
      return;
    }
    const lines = items.map((it) => {
      const ts = formatDateTimeVN(it.ts);
      const bal = Number(it.balance) || 0;
      return `${ts} | ${bal.toLocaleString()}đ`;
    });
    await ctx.reply(`Lịch sử số dư (mới nhất):\n${lines.join('\n')}`, menuKeyboard(ctx));
  });

  function maskAccountNumber(s) {
    const acc = String(s || '').trim();
    if (acc.length <= 6) return acc;
    return `${acc.slice(0, 3)}xxxx${acc.slice(-3)}`;
  }

  function normalizeKeyText(s) {
    return String(s || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();
  }

  function classifyIbft(it) {
    const ok = !it.errorCode && !it.errorMessage;
    if (ok) return 'success';
    return 'other_error';
  }

  function startOfVnDayTs(nowTs) {
    const offsetMs = 7 * 60 * 60 * 1000;
    const d = new Date((Number(nowTs) || Date.now()) + offsetMs);
    const y = d.getUTCFullYear();
    const m = d.getUTCMonth();
    const day = d.getUTCDate();
    return Date.UTC(y, m, day) - offsetMs;
  }

  function rangeToTs(rangeKey) {
    const end = Date.now();
    const startOfDay = startOfVnDayTs(end);
    if (rangeKey === 'today') return { fromTs: startOfDay, toTs: end };
    if (rangeKey === '7d') return { fromTs: startOfDay - 6 * 24 * 60 * 60 * 1000, toTs: end };
    return { fromTs: 0, toTs: end };
  }

  function filterIbft(items, typeKey, rangeKey) {
    const { fromTs, toTs } = rangeToTs(rangeKey);
    const type = String(typeKey || 'all');
    return items.filter((it) => {
      const ts = Number(it.ts) || 0;
      if (fromTs && ts < fromTs) return false;
      if (toTs && ts > toTs) return false;
      if (type === 'all') return true;
      return classifyIbft(it) === type;
    });
  }

  function fmtIbftItem(it) {
    const ts = formatDateTimeVN(it.ts);
    const cls = classifyIbft(it);
    const icon = cls === 'success' ? '✅' : '❌';
    const amt = Number(it.amount) || 0;
    const bank = escapeMd(String(it.bankCode || '').trim().toUpperCase());
    const acc = escapeMd(maskAccountNumber(it.accountNumber));
    const name = escapeMd(String(it.accountName || '').trim().toUpperCase());
    const merchant = escapeMd(String(it.merchant || '').trim().toUpperCase());
    const orderId = escapeMd(String(it.orderId || '').trim());
    const status = escapeMd(String(it.tranStatus || '').trim());
    const lines = [];
    lines.push(`${icon} ${escapeMd(ts)} | ${amt.toLocaleString()}đ${merchant ? ` | ${merchant}` : ''}`);
    lines.push(`🏦 ${bank} | 💳 ${acc}`);
    if (name) lines.push(`👤 ${name}`);
    if (orderId) lines.push(`🆔 ${orderId}`);
    if (status) lines.push(`📌 ${status}`);
    if (cls !== 'success') {
      if (it.errorCode) lines.push(`⚠️ Mã lỗi: ${escapeMd(String(it.errorCode))}`);
      if (it.errorMessage) lines.push(`⚠️ ${escapeMd(String(it.errorMessage))}`);
    }
    return lines.join('\n');
  }

  function formatIbftHistoryPage(allItems, typeKey, rangeKey, page) {
    const filtered = filterIbft(allItems, typeKey, rangeKey);
    const counts = { success: 0, other_error: 0 };
    for (const it of filterIbft(allItems, 'all', rangeKey)) counts[classifyIbft(it)]++;
    const pageSize = 10;
    const pageCount = Math.max(1, Math.ceil(filtered.length / pageSize));
    const p = Math.max(1, Math.min(Number(page) || 1, pageCount));
    const slice = filtered.slice((p - 1) * pageSize, (p - 1) * pageSize + pageSize);

    const typeLabel =
      typeKey === 'success'
        ? '✅ Thành công'
        : typeKey === 'other_error'
          ? '❌ Lỗi'
          : '📦 Tất cả';
    const rangeLabel = rangeKey === 'today' ? 'Hôm nay' : rangeKey === '7d' ? '7 ngày' : 'All';

    const out = [];
    out.push(`📜 *LỊCH SỬ CHI HỘ* (${p}/${pageCount})`);
    out.push(`📌 Loại: *${typeLabel}* | 📅 ${rangeLabel}`);
    out.push(`✅ ${counts.success} | ❌ ${counts.other_error}`);
    out.push('');
    if (!slice.length) {
      out.push('Không có dữ liệu.');
      return { text: out.join('\n'), page: p, pageCount, total: filtered.length };
    }
    for (const it of slice) {
      out.push(fmtIbftItem(it));
      out.push('');
    }
    return { text: out.join('\n').trim(), page: p, pageCount, total: filtered.length };
  }

  function csvEscape(v) {
    const s = String(v ?? '');
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  function ensureExportDir() {
    const dir = path.join(__dirname, '..', 'data', 'exports');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  function writeCsvFile(filename, headers, rows) {
    const dir = ensureExportDir();
    const filePath = path.join(dir, filename);
    const lines = [];
    lines.push(headers.map(csvEscape).join(','));
    for (const r of rows) lines.push(r.map(csvEscape).join(','));
    fs.writeFileSync(filePath, lines.join('\n'), 'utf8');
    return filePath;
  }

  bot.hears('📜 Lịch sử chi hộ', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const all = db.getAllIbftHistory();
    if (!all.length) {
      await ctx.reply('Chưa có lịch sử chi hộ.', menuKeyboard(ctx));
      return;
    }
    const st = { type: 'all', range: 'all', page: 1 };
    ibftHistState.set(ctx.from.id, st);
    const pageData = formatIbftHistoryPage(all, st.type, st.range, st.page);
    const nav = [];
    if (pageData.page > 1) nav.push(Markup.button.callback('⬅️', `ibft_hist_page:${pageData.page - 1}`));
    if (pageData.page < pageData.pageCount) nav.push(Markup.button.callback('➡️', `ibft_hist_page:${pageData.page + 1}`));
    const rows = [
      [
        Markup.button.callback('✅ Thành công', 'ibft_hist_type:success'),
        Markup.button.callback('❌ Lỗi', 'ibft_hist_type:other_error'),
        Markup.button.callback('📦 Tất cả', 'ibft_hist_type:all'),
      ],
      [Markup.button.callback('📅 Hôm nay', 'ibft_hist_range:today'), Markup.button.callback('📅 7 ngày', 'ibft_hist_range:7d'), Markup.button.callback('📅 All', 'ibft_hist_range:all')],
    ];
    if (nav.length) rows.push(nav);
    rows.push([Markup.button.callback('📤 Xuất Excel', 'ibft_export'), Markup.button.callback('📤 Xuất ALL', 'ibft_export_all')]);
    rows.push([Markup.button.callback('❌ Đóng', 'ibft_hist_close')]);
    await ctx.replyWithMarkdown(pageData.text, { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
  });

  bot.command('ibfthist', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const all = db.getAllIbftHistory();
    if (!all.length) {
      await ctx.reply('Chưa có lịch sử chi hộ.', menuKeyboard(ctx));
      return;
    }
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const page = Math.max(1, Number(parts[1] || 1) || 1);
    const type = String(parts[2] || 'all').trim();
    const range = String(parts[3] || 'all').trim();
    const st = { type, range, page };
    ibftHistState.set(ctx.from.id, st);
    const pageData = formatIbftHistoryPage(all, st.type, st.range, st.page);
    await ctx.replyWithMarkdown(pageData.text, menuKeyboard(ctx));
  });

  async function updateIbftHistMessage(ctx, nextState) {
    const all = db.getAllIbftHistory();
    const st = { type: nextState.type || 'all', range: nextState.range || 'all', page: Number(nextState.page) || 1 };
    ibftHistState.set(ctx.from.id, st);
    const pageData = formatIbftHistoryPage(all, st.type, st.range, st.page);
    const nav = [];
    if (pageData.page > 1) nav.push(Markup.button.callback('⬅️', `ibft_hist_page:${pageData.page - 1}`));
    if (pageData.page < pageData.pageCount) nav.push(Markup.button.callback('➡️', `ibft_hist_page:${pageData.page + 1}`));
    const rows = [
      [
        Markup.button.callback('✅ Thành công', 'ibft_hist_type:success'),
        Markup.button.callback('❌ Lỗi', 'ibft_hist_type:other_error'),
        Markup.button.callback('📦 Tất cả', 'ibft_hist_type:all'),
      ],
      [Markup.button.callback('📅 Hôm nay', 'ibft_hist_range:today'), Markup.button.callback('📅 7 ngày', 'ibft_hist_range:7d'), Markup.button.callback('📅 All', 'ibft_hist_range:all')],
    ];
    if (nav.length) rows.push(nav);
    rows.push([Markup.button.callback('📤 Xuất Excel', 'ibft_export'), Markup.button.callback('📤 Xuất ALL', 'ibft_export_all')]);
    rows.push([Markup.button.callback('❌ Đóng', 'ibft_hist_close')]);
    try {
      await ctx.editMessageText(pageData.text, { parse_mode: 'Markdown', reply_markup: Markup.inlineKeyboard(rows).reply_markup });
    } catch (_) {}
  }

  bot.action(/^ibft_hist_page:(\d+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    const st = ibftHistState.get(ctx.from.id) || { type: 'all', range: 'all', page: 1 };
    await updateIbftHistMessage(ctx, { ...st, page: Number(ctx.match[1] || 1) });
  });

  bot.action(/^ibft_hist_type:(all|success|other_error)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    const st = ibftHistState.get(ctx.from.id) || { type: 'all', range: 'all', page: 1 };
    await updateIbftHistMessage(ctx, { ...st, type: String(ctx.match[1] || 'all'), page: 1 });
  });

  bot.action(/^ibft_hist_range:(all|today|7d)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    const st = ibftHistState.get(ctx.from.id) || { type: 'all', range: 'all', page: 1 };
    await updateIbftHistMessage(ctx, { ...st, range: String(ctx.match[1] || 'all'), page: 1 });
  });

  bot.action('ibft_hist_close', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    ibftHistState.delete(ctx.from.id);
    try {
      await ctx.editMessageReplyMarkup(undefined);
    } catch (_) {}
  });

  async function exportIbftCsv(ctx, state, forceAll) {
    const all = db.getAllIbftHistory();
    const st = state || { type: 'all', range: 'all', page: 1 };
    const type = forceAll ? 'all' : String(st.type || 'all');
    const range = forceAll ? 'all' : String(st.range || 'all');
    const filtered = filterIbft(all, type, range);
    const headers = [
      'ts',
      'datetime_vn',
      'type',
      'adminId',
      'merchant',
      'bankCode',
      'accountNumber',
      'accountName',
      'amount',
      'remark',
      'orderId',
      'tranStatus',
      'errorCode',
      'errorMessage',
    ];
    const rows = filtered.map((it) => [
      String(Number(it.ts) || 0),
      formatDateTimeVN(it.ts),
      classifyIbft(it),
      String(it.adminId || ''),
      String(it.merchant || ''),
      String(it.bankCode || ''),
      String(it.accountNumber || ''),
      String(it.accountName || ''),
      String(Number(it.amount) || 0),
      String(it.remark || ''),
      String(it.orderId || ''),
      String(it.tranStatus || ''),
      String(it.errorCode || ''),
      String(it.errorMessage || ''),
    ]);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    const filename = `ibft_${range}_${type}_${stamp}.csv`.replace(/[^\w.-]/g, '_');
    const filePath = writeCsvFile(filename, headers, rows);
    await bot.telegram.sendDocument(ctx.chat.id, { source: fs.createReadStream(filePath), filename }, { caption: `Xuất chi hộ: ${rows.length} dòng` });
    try {
      fs.unlinkSync(filePath);
    } catch (_) {}
  }

  function parseDateYmdToTs(s) {
    const m = String(s || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);
    if (!y || mo < 1 || mo > 12 || d < 1 || d > 31) return null;
    const dt = new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00+07:00`);
    if (Number.isNaN(dt.getTime())) return null;
    return dt.getTime();
  }

  bot.command('ibftexport', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const arg1 = String(parts[1] || '').trim().toLowerCase();
    if (!arg1) {
      await ctx.reply('Cú pháp: /ibftexport all | /ibftexport <from YYYY-MM-DD> <to YYYY-MM-DD> [all|success|other_error]', menuKeyboard(ctx));
      return;
    }
    if (arg1 === 'all') {
      await exportIbftCsv(ctx, { type: 'all', range: 'all', page: 1 }, true);
      return;
    }
    const fromTs = parseDateYmdToTs(parts[1]);
    const toStart = parseDateYmdToTs(parts[2]);
    const type = String(parts[3] || 'all').trim();
    if (!fromTs || !toStart) {
      await ctx.reply('Sai ngày. Dùng dạng YYYY-MM-DD. Ví dụ: /ibftexport 2026-03-01 2026-03-31 success', menuKeyboard(ctx));
      return;
    }
    const toTs = toStart + 24 * 60 * 60 * 1000 - 1;
    const all = db.getAllIbftHistory();
    const filtered = all.filter((it) => {
      const ts = Number(it.ts) || 0;
      if (ts < fromTs || ts > toTs) return false;
      if (type === 'all') return true;
      return classifyIbft(it) === type;
    });
    const headers = [
      'ts',
      'datetime_vn',
      'type',
      'adminId',
      'merchant',
      'bankCode',
      'accountNumber',
      'accountName',
      'amount',
      'remark',
      'orderId',
      'tranStatus',
      'errorCode',
      'errorMessage',
    ];
    const rows = filtered.map((it) => [
      String(Number(it.ts) || 0),
      formatDateTimeVN(it.ts),
      classifyIbft(it),
      String(it.adminId || ''),
      String(it.merchant || ''),
      String(it.bankCode || ''),
      String(it.accountNumber || ''),
      String(it.accountName || ''),
      String(Number(it.amount) || 0),
      String(it.remark || ''),
      String(it.orderId || ''),
      String(it.tranStatus || ''),
      String(it.errorCode || ''),
      String(it.errorMessage || ''),
    ]);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    const filename = `ibft_${parts[1]}_${parts[2]}_${type}_${stamp}.csv`.replace(/[^\w.-]/g, '_');
    const filePath = writeCsvFile(filename, headers, rows);
    await bot.telegram.sendDocument(ctx.chat.id, { source: fs.createReadStream(filePath), filename }, { caption: `Xuất chi hộ: ${rows.length} dòng` });
    try {
      fs.unlinkSync(filePath);
    } catch (_) {}
  });

  bot.action('ibft_export', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    try {
      const st = ibftHistState.get(ctx.from.id) || { type: 'all', range: 'all', page: 1 };
      await exportIbftCsv(ctx, st, false);
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel chi hộ: ${e.message}`, menuKeyboard(ctx));
    }
  });

  bot.action('ibft_export_all', async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    try {
      await exportIbftCsv(ctx, { type: 'all', range: 'all', page: 1 }, true);
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel chi hộ: ${e.message}`, menuKeyboard(ctx));
    }
  });

  bot.command('user', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const id = String(parts[1] || '').trim();
    if (!id) {
      await ctx.reply('Cú pháp: /user <id>', menuKeyboard(ctx));
      return;
    }
    const u = db.getUser(id);
    const config = db.getConfig();
    const feeFlat = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);
    const feePercent = u.feePercent !== null ? u.feePercent : config.globalFeePercent;
    const ipnFeeUser = u.ipnFeeFlat !== null && u.ipnFeeFlat !== undefined ? Number(u.ipnFeeFlat) : NaN;
    const wdFeeUser = u.withdrawFeeFlat !== null && u.withdrawFeeFlat !== undefined ? Number(u.withdrawFeeFlat) : NaN;
    const lines = [];
    lines.push(`ID: ${u.id}`);
    lines.push(`Trạng thái: ${u.isActive ? 'Active' : 'Inactive'}`);
    lines.push(`Số dư: ${Number(u.balance || 0).toLocaleString()}đ`);
    lines.push(`Phí chuyển: ${feeFlat.toLocaleString()}đ`);
    lines.push(`Phí rút: ${feePercent}%`);
    if (Number.isFinite(ipnFeeUser)) lines.push(`Phí tiền về (user): ${Math.max(0, ipnFeeUser).toLocaleString()}đ`);
    if (Number.isFinite(wdFeeUser)) lines.push(`Phí chuyển rút (user): ${Math.max(0, wdFeeUser).toLocaleString()}đ`);
    lines.push(`VA: ${u.createdVA}/${u.vaLimit !== null ? u.vaLimit : '∞'}`);
    const saved = Array.isArray(u.withdrawBanks) ? u.withdrawBanks : [];
    if (saved.length) lines.push(`Bank đã lưu: ${saved.length}`);
    await ctx.reply(lines.join('\n'), menuKeyboard(ctx));
  });

  bot.command('uhist', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const id = String(parts[1] || '').trim();
    const limit = Math.max(1, Math.min(50, Number(parts[2] || 20) || 20));
    if (!id) {
      await ctx.reply('Cú pháp: /uhist <id> [n]', menuKeyboard(ctx));
      return;
    }
    const items = db.getUserBalanceHistory(id, limit);
    if (!items.length) {
      await ctx.reply('Chưa có lịch sử số dư user này.', menuKeyboard(ctx));
      return;
    }
    const lines = items.map((it) => {
      const ts = formatDateTimeVN(it.ts);
      const delta = Number(it.delta) || 0;
      const sign = delta >= 0 ? '+' : '';
      const bal = Number(it.balanceAfter) || 0;
      const reason = String(it.reason || '');
      const ref = String(it.ref || '');
      return `${ts} | ${sign}${delta.toLocaleString()}đ | ${bal.toLocaleString()}đ${reason ? ` | ${reason}` : ''}${ref ? ` | ${ref}` : ''}`;
    });
    await ctx.reply(`Lịch sử số dư user ${id} (mới nhất):\n${lines.join('\n')}`, menuKeyboard(ctx));
  });



  bot.hears('💸 Rút tiền', async (ctx) => {
    const computed = computeUserBalanceFromRecords(ctx.from.id);
    db.updateUser(ctx.from.id, { balance: computed.balance });
    const user = db.getUser(ctx.from.id);
    const saved = getUserWithdrawBanks(ctx.from.id);
    if (saved.length) {
      withdrawState.set(ctx.from.id, { stage: 'choose_saved', balance: user.balance, method: 'bank' });
      await ctx.reply(`Số dư khả dụng: ${user.balance.toLocaleString()}đ\nChọn tài khoản nhận tiền:`, {
        reply_markup: buildWithdrawSavedKeyboard(ctx.from.id).reply_markup,
      });
      return;
    }
    withdrawState.set(ctx.from.id, { stage: 'choose_bank', page: 0, balance: user.balance, method: 'bank' });
    await ctx.reply(`Số dư khả dụng: ${user.balance.toLocaleString()}đ\n🏦 Chọn ngân hàng:`, {
      reply_markup: buildWithdrawBankInlineKeyboard(0).reply_markup,
    });
  });

  bot.hears('❌ Hủy', async (ctx) => {
    const id = ctx.from.id;
    clearUserStates(id);
    await ctx.reply('Đã hủy thao tác.', menuKeyboard(ctx));
  });

  bot.hears('📋 DS rút', async (ctx) => {
    if (!isAdminId(ctx.from.id)) {
      await ctx.reply('Bạn không có quyền dùng chức năng này.', menuKeyboard(ctx));
      return;
    }
    const pending = db.getWithdrawals({ status: 'pending', limit: 10 });
    if (!pending.length) {
      await ctx.reply('Chưa có yêu cầu rút tiền nào.', menuKeyboard(ctx));
      return;
    }
    const fmt = (w) => {
      const status = String(w.status || '').trim().toLowerCase();
      const rejectReason = String(w.rejectReason || '').trim().toLowerCase();
      const title =
        status === 'pending'
          ? '🆕 Yêu cầu rút tiền mới'
          : status === 'done'
            ? '✅ Rút tiền thành công'
            : status === 'reject' && rejectReason === 'wrong_info'
              ? '❌ Từ chối (sai STK/Tên)'
              : status === 'reject'
                ? '❌ Rút tiền bị từ chối'
                : '📌 Yêu cầu rút tiền';
      const userLabel = w.username ? `@${String(w.username).trim()}` : String(w.userId || '').trim();
      const bal = w.balanceBefore !== null && w.balanceBefore !== undefined ? Number(w.balanceBefore) || 0 : w.userId ? Number(db.getUser(w.userId).balance || 0) : 0;
      const amt = Number(w.amount) || toAmountNumber(w.amount);
      const feeFlat = Number(w.feeFlat) || 0;
      const feePercent = w.feePercent === null || w.feePercent === undefined ? null : Number(w.feePercent) || 0;
      const feeByPercent = w.feeByPercent !== null && w.feeByPercent !== undefined ? Number(w.feeByPercent) || 0 : feePercent === null ? 0 : Math.floor((amt * feePercent) / 100);
      const actual = Number(w.actualReceive) || Math.max(0, amt - feeFlat - feeByPercent);
      const lines = [];
      lines.push(`*${escapeMd(title)}*`);
      lines.push(`🆔 ID: *${escapeMd(displayWithdrawalId(w.id))}*`);
      lines.push(`👤 User: ${escapeMd(userLabel)}`);
      if (w.userId) lines.push(`🆔 User ID: ${escapeMd(String(w.userId))}`);
      lines.push(`💰 Số dư user: ${bal.toLocaleString()}đ`);
      lines.push('');

      const method = String(w.method || '').toLowerCase();
      if (method === 'bank') {
        lines.push(`🏦 Ngân hàng: ${escapeMd(String(w.bankName || '').trim())}`);
        lines.push(`💳 STK: ${escapeMd(String(w.bankAccount || '').trim())}`);
        lines.push(`👤 Chủ TK: ${escapeMd(String(w.bankHolder || '').trim().toUpperCase())}`);
      } else {
        lines.push(`🌐 Network: ${escapeMd(String(w.network || '').trim().toUpperCase())}`);
        lines.push(`👛 Ví: ${escapeMd(String(w.wallet || '').trim())}`);
      }
      lines.push('');
      lines.push(`💵 Số tiền trừ: ${amt.toLocaleString()}đ`);
      lines.push(`💸 Phí chuyển: ${feeFlat.toLocaleString()}đ`);
      if (feePercent !== null) lines.push(`📉 Phí rút: ${feePercent}%`);
      lines.push(`💰 Thực nhận: ${actual.toLocaleString()}đ`);
      return lines.join('\n');
    };
    const lines = [];
    lines.push('📋 *DANH SÁCH RÚT TIỀN*');
    lines.push('');
    if (pending.length) {
      lines.push('⏳ *Đang chờ*');
      for (const w of pending) lines.push(fmt(w), '');
    }
    await ctx.replyWithMarkdown(lines.join('\n').trim(), menuKeyboard(ctx));
    await ctx.reply('📤 Xuất danh sách rút tiền:', {
      reply_markup: Markup.inlineKeyboard([
        [
          Markup.button.callback('⏳ Pending', 'wd_export:pending'),
          Markup.button.callback('✅ Done', 'wd_export:done'),
          Markup.button.callback('❌ Reject', 'wd_export:reject'),
        ],
        [Markup.button.callback('📦 Xuất ALL', 'wd_export:all')],
      ]).reply_markup,
    });
  });

  async function exportWithdrawalsCsv(ctx, statusKey, fromTs, toTs) {
    const status = String(statusKey || 'all').trim().toLowerCase();
    const all = db.getWithdrawals({});
    const filtered = all.filter((w) => {
      const st = String(w.status || '').trim().toLowerCase();
      if (status !== 'all' && st !== status) return false;
      const ts = Number(w.createdAt || w.updatedAt || 0) || 0;
      if (fromTs && ts < fromTs) return false;
      if (toTs && ts > toTs) return false;
      return true;
    });
    const headers = [
      'id',
      'status',
      'rejectReason',
      'rejectNote',
      'createdAt',
      'createdAt_vn',
      'updatedAt',
      'updatedAt_vn',
      'userId',
      'username',
      'method',
      'bankName',
      'bankAccount',
      'bankHolder',
      'network',
      'wallet',
      'amount',
      'feeFlat',
      'feePercent',
      'feeByPercent',
      'actualReceive',
      'balanceBefore',
      'balanceAfter',
    ];
    const rows = filtered.map((w) => [
      String(w.id || ''),
      String(w.status || ''),
      String(w.rejectReason || ''),
      String(w.rejectNote || ''),
      String(Number(w.createdAt || 0) || 0),
      formatDateTimeVN(w.createdAt || ''),
      String(Number(w.updatedAt || 0) || 0),
      formatDateTimeVN(w.updatedAt || ''),
      String(w.userId || ''),
      String(w.username || ''),
      String(w.method || ''),
      String(w.bankName || ''),
      String(w.bankAccount || ''),
      String(w.bankHolder || ''),
      String(w.network || ''),
      String(w.wallet || ''),
      String(Number(w.amount) || 0),
      String(Number(w.feeFlat) || 0),
      w.feePercent === null || w.feePercent === undefined ? '' : String(Number(w.feePercent) || 0),
      String(Number(w.feeByPercent) || 0),
      String(Number(w.actualReceive) || 0),
      String(Number(w.balanceBefore) || 0),
      String(Number(w.balanceAfter) || 0),
    ]);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    const namePart = `${status}_${fromTs ? 'range' : 'all'}`.replace(/[^\w.-]/g, '_');
    const filename = `withdrawals_${namePart}_${stamp}.csv`;
    const filePath = writeCsvFile(filename, headers, rows);
    await bot.telegram.sendDocument(ctx.chat.id, { source: fs.createReadStream(filePath), filename }, { caption: `Xuất rút tiền (${status}): ${rows.length} dòng` });
    try {
      fs.unlinkSync(filePath);
    } catch (_) {}
  }

  bot.action(/^wd_export:(pending|done|reject|all)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    try {
      await exportWithdrawalsCsv(ctx, String(ctx.match[1] || 'all'), 0, 0);
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel rút tiền: ${e.message}`, menuKeyboard(ctx));
    }
  });

  bot.command('wdexport', async (ctx) => {
    if (!isAdminId(ctx.from.id)) return;
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const status = String(parts[1] || 'all').trim().toLowerCase();
    const fromTs = parseDateYmdToTs(parts[2]);
    const toStart = parseDateYmdToTs(parts[3]);
    if (parts[2] && (!fromTs || !toStart)) {
      await ctx.reply('Cú pháp: /wdexport [all|pending|done|reject] [from YYYY-MM-DD] [to YYYY-MM-DD]', menuKeyboard(ctx));
      return;
    }
    const toTs = toStart ? toStart + 24 * 60 * 60 * 1000 - 1 : 0;
    try {
      await exportWithdrawalsCsv(ctx, status, fromTs || 0, toTs || 0);
    } catch (e) {
      await ctx.reply(`Lỗi xuất excel rút tiền: ${e.message}`, menuKeyboard(ctx));
    }
  });

  const awaitingWdUpdate = new Map();
  bot.hears('🔄 Cập nhật rút', async (ctx) => {
    if (!isAdminId(ctx.from.id)) {
      await ctx.reply('Bạn không có quyền dùng chức năng này.', menuKeyboard(ctx));
      return;
    }
    awaitingWdUpdate.set(ctx.from.id, { stage: 'enter_id' });
    await ctx.reply('Nhập ID rút tiền (ví dụ: 4845250540424981). (ID cũ có thể bắt đầu bằng WD)', Markup.keyboard([['❌ Hủy']]).resize());
  });

  bot.action(/^wd_copy:(net|acc|id):(.+)$/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
    } catch (_) {}
    if (!isAdminId(ctx.from.id)) return;
    const kind = String(ctx.match[1] || '');
    const id = String(ctx.match[2] || '').trim();
    const w = db.getWithdrawalById(id) || db.getWithdrawalById(`WD${id}`) || db.getWithdrawalById(String(id).replace(/^WD/i, ''));
    if (!w) {
      try {
        await ctx.reply('Không tìm thấy lệnh rút này.', menuKeyboard(ctx));
      } catch (_) {}
      return;
    }
    if (kind === 'id') {
      await ctx.reply(displayWithdrawalId(w.id), menuKeyboard(ctx));
      return;
    }
    if (kind === 'acc') {
      const acc = String(w.bankAccount || '').trim();
      await ctx.reply(acc || 'N/A', menuKeyboard(ctx));
      return;
    }
    const amount = Number(w.amount) || 0;
    const feeFlat = Number(w.feeFlat) || 0;
    const feePercent = w.feePercent === null || w.feePercent === undefined ? null : Number(w.feePercent) || 0;
    const feeByPercent =
      w.feeByPercent !== null && w.feeByPercent !== undefined ? Number(w.feeByPercent) || 0 : feePercent === null ? 0 : Math.floor((amount * feePercent) / 100);
    const net = Number(w.actualReceive) || Math.max(0, amount - feeFlat - feeByPercent);
    await ctx.reply(copyNumber(net), menuKeyboard(ctx));
  });

  bot.hears('Đã rút', async (ctx, next) => {
    const st = awaitingWdUpdate.get(ctx.from.id);
    if (!st || st.stage !== 'choose_status') return next();
    const w = db.getWithdrawalById(st.id);
    if (!w) return;
    if (w.status !== 'done') {
       if (w.status === 'reject') {
         const u = db.getUser(w.userId);
         const delta = -(Number(w.amount) || 0);
         const after = u.balance + delta;
         db.updateUser(w.userId, { balance: after }); // deduct back
         try {
           db.addUserBalanceHistory({ ts: Date.now(), userId: w.userId, delta, balanceAfter: after, reason: 'withdraw_deduct_back', ref: w.id });
         } catch (_) {}
       }
       db.updateWithdrawalStatus(st.id, 'done');
    }
    awaitingWdUpdate.delete(ctx.from.id);
    await ctx.reply(`Đã cập nhật ${displayWithdrawalId(st.id)} → đã rút.`, menuKeyboard(ctx));
    if (w.userId) {
      try {
        const amount = Number(w.amount) || 0;
        const feeFlat = Number(w.feeFlat) || 0;
        const feePercent = w.feePercent === null || w.feePercent === undefined ? null : Number(w.feePercent) || 0;
        const feeByPercent = feePercent === null ? 0 : Math.floor((amount * feePercent) / 100);
        const net = Number(w.actualReceive) || Math.max(0, amount - feeFlat - feeByPercent);
        const lines = [];
        lines.push('✅ Rút tiền thành công!');
        lines.push(`🆔 ID: ${displayWithdrawalId(w.id)}`);
        lines.push(`💵 Số tiền rút: ${amount.toLocaleString()} đ`);
        lines.push(`💸 Phí chuyển: ${feeFlat.toLocaleString()} đ`);
        if (feePercent !== null) lines.push(`📉 Phí rút: ${feePercent}%`);
        lines.push(`💰 Thực nhận: ${net.toLocaleString()} đ (copy: ${copyNumber(net)})`);
        lines.push('');
        const method = String(w.method || '').toLowerCase();
        if (method === 'bank') {
          lines.push(`🏦 Ngân hàng: ${String(w.bankName || '').trim()}`);
          lines.push(`💳 STK: ${String(w.bankAccount || '').trim()}`);
          lines.push(`👤 Chủ TK: ${String(w.bankHolder || '').trim().toUpperCase()}`);
          lines.push('🏦 Đã chuyển vào tài khoản ngân hàng của bạn');
        } else {
          lines.push(`🌐 Network: ${String(w.network || '').trim().toUpperCase()}`);
          lines.push(`👛 Ví: ${String(w.wallet || '').trim()}`);
          lines.push('✅ Đã chuyển vào ví của bạn');
        }
        const msg = lines.join('\n').trim();
        await bot.telegram.sendMessage(w.userId, msg);
      } catch (_) {}
    }
  });

  bot.hears('Chưa rút', async (ctx, next) => {
    const st = awaitingWdUpdate.get(ctx.from.id);
    if (!st || st.stage !== 'choose_status') return next();
    const w = db.getWithdrawalById(st.id);
    if (!w) return;
    if (w.status === 'reject') {
       const u = db.getUser(w.userId);
       const delta = -(Number(w.amount) || 0);
       const after = u.balance + delta;
       db.updateUser(w.userId, { balance: after }); // deduct back
       try {
         db.addUserBalanceHistory({ ts: Date.now(), userId: w.userId, delta, balanceAfter: after, reason: 'withdraw_deduct_back', ref: w.id });
       } catch (_) {}
    }
    db.updateWithdrawalStatus(st.id, 'pending');
    awaitingWdUpdate.delete(ctx.from.id);
    await ctx.reply(`Đã cập nhật ${displayWithdrawalId(st.id)} → chưa rút (pending).`, menuKeyboard(ctx));
    if (w.userId) {
      try {
        await bot.telegram.sendMessage(w.userId, `Yêu cầu rút tiền ${displayWithdrawalId(w.id)} đang chờ xử lý.`);
      } catch (_) {}
    }
  });

  bot.hears('Từ chối', async (ctx, next) => {
    const st = awaitingWdUpdate.get(ctx.from.id);
    if (!st || st.stage !== 'choose_status') return next();
    const w = db.getWithdrawalById(st.id);
    if (!w) return;
    awaitingWdUpdate.set(ctx.from.id, { stage: 'enter_reject_reason', id: st.id });
    await ctx.reply('Nhập lý do từ chối (ví dụ: sai STK/tên, ngân hàng lỗi...).', Markup.keyboard([['❌ Hủy']]).resize());
  });

  bot.hears('Từ chối sai STK/Tên', async (ctx, next) => {
    const st = awaitingWdUpdate.get(ctx.from.id);
    if (!st || st.stage !== 'choose_status') return next();
    const w = db.getWithdrawalById(st.id);
    if (!w) return;
    if (w.status !== 'reject') {
      const u = db.getUser(w.userId);
      const delta = Number(w.amount) || 0;
      const after = u.balance + delta;
      db.updateUser(w.userId, { balance: after });
      try {
        db.addUserBalanceHistory({ ts: Date.now(), userId: w.userId, delta, balanceAfter: after, reason: 'withdraw_refund_wrong_info', ref: w.id });
      } catch (_) {}
    }
    db.updateWithdrawalStatus(st.id, 'reject', { rejectReason: 'wrong_info', rejectNote: 'Sai STK/Tên người nhận' });
    awaitingWdUpdate.delete(ctx.from.id);
    await ctx.reply(`Đã cập nhật ${displayWithdrawalId(st.id)} → Từ chối (sai STK/Tên) (đã hoàn tiền).`, menuKeyboard(ctx));
    if (w.userId) {
      try {
        await bot.telegram.sendMessage(w.userId, `Yêu cầu rút tiền ${displayWithdrawalId(w.id)} bị từ chối do sai STK hoặc tên người nhận và đã hoàn tiền. Vui lòng tạo lại yêu cầu rút.`);
      } catch (_) {}
    }
  });

  function formatStatus(id) {
    const s = requestStatus.get(id) || db.getByRequestId(id);
    if (!s) return 'Không tìm thấy requestId này.';
    const lines = [];
    lines.push(`RequestId: ${id}`);
    lines.push(`Trạng thái: ${s.status}`);
    if (s.name) lines.push(`Tên: ${s.name}`);
    if (s.customerName) lines.push(`KH: ${s.customerName}`);
    if (s.vaAccount) lines.push(`STK: ${s.vaAccount}`);
    if (s.amount) lines.push(`Số tiền: ${s.amount}`);
    if (s.vaBank) lines.push(`Ngân hàng: ${s.vaBank}`);
    if (s.remark) lines.push(`Remark: ${s.remark}`);
    if (s.transactionId) lines.push(`Transaction: ${s.transactionId}`);
    if (s.cashinId) lines.push(`CASHIN: ${s.cashinId}`);
    if (s.timePaid) lines.push(`TimePaid: ${s.timePaid}`);
    return lines.join('\n');
  }

  function findLatestByVaAccount(vaAccount) {
    const acc = String(vaAccount || '').replace(/[^\d]/g, '');
    if (!acc) return null;
    const candidates = [];
    try {
      for (const [rid, s] of requestStatus.entries()) {
        if (String(s?.vaAccount || '') === acc) candidates.push({ requestId: rid, ...s });
      }
    } catch (_) {}
    try {
      const all = db.loadAll();
      for (const r of all) {
        if (String(r?.vaAccount || '') === acc) candidates.push(r);
      }
    } catch (_) {}
    if (!candidates.length) return null;
    candidates.sort((a, b) => {
      const ap = String(a.status || '') === 'paid' ? 1 : 0;
      const bp = String(b.status || '') === 'paid' ? 1 : 0;
      if (ap !== bp) return bp - ap;
      return (Number(b.timePaid || b.createdAt || 0) || 0) - (Number(a.timePaid || a.createdAt || 0) || 0);
    });
    return candidates[0];
  }

  function formatStatusByVaAccount(vaAccount) {
    const acc = String(vaAccount || '').replace(/[^\d]/g, '');
    const s = findLatestByVaAccount(acc);
    if (!s) return 'Không tìm thấy VA này.';
    const statusRaw = String(s.status || '').trim();
    const statusKey = statusRaw.toLowerCase();
    const statusLabel =
      statusKey === 'paid' ? '✅ PAID' : statusKey === 'unpaid' ? '⏳ UNPAID' : statusRaw ? statusRaw.toUpperCase() : 'N/A';

    const amountValue = s.amount || s.vaAmount || '';
    const amountNum = toAmountNumber(amountValue);
    const amountStr = amountNum ? `${amountNum.toLocaleString()} đ` : escapeMd(String(amountValue || '0'));

    const bankStr = String(s.vaBank || s.bank || '').trim();
    const nameStr = String(s.name || s.customerName || '').trim();
    const remarkStr = String(s.transferContent || s.remark || '').trim();
    const timeStr = s.timePaid ? formatDateTimeVN(s.timePaid) : '';
    const txStr = String(s.transactionId || '').trim();
    const cashinStr = String(s.cashinId || '').trim();
    const reqIdStr = String(s.requestId || '').trim();

    const out = [];
    out.push('*🔎 KẾT QUẢ KIỂM TRA*');
    out.push('');
    out.push(`💳 STK: \`${escapeMd(acc)}\``);
    if (reqIdStr) out.push(`🆔 RequestId: \`${escapeMd(reqIdStr)}\``);
    out.push(`📌 Trạng thái: *${escapeMd(statusLabel)}*`);
    out.push(`💵 Số tiền: *${escapeMd(amountStr)}*`);
    if (bankStr) out.push(`🏦 Ngân hàng: *${escapeMd(bankStr)}*`);
    if (nameStr) out.push(`👤 Tên: *${escapeMd(nameStr)}*`);
    if (remarkStr) out.push(`📝 Nội dung: ${escapeMd(remarkStr)}`);
    if (timeStr) out.push(`🕒 Thời gian: ${escapeMd(timeStr)}`);
    if (txStr) out.push(`🔁 Transaction: ${escapeMd(txStr)}`);
    if (cashinStr) out.push(`🧾 CASHIN: ${escapeMd(cashinStr)}`);
    return out.join('\n');
  }

  bot.hears('🔎 Kiểm tra tài khoản', async (ctx) => {
    awaitingStatus.set(ctx.from.id, true);
    await ctx.reply('Nhập Số tài khoản (vaAccount) để kiểm tra:');
  });

  bot.on('text', async (ctx, next) => {
    const text = ctx.message?.text || '';
    
    const rnSt = randomNameState.get(ctx.from.id);
    if (rnSt) {
      if (isMenuText(text)) return next();
      if (rnSt.stage === 'enter_prefix') {
        const prefix = text
          .trim()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .replace(/[^A-Za-z0-9 ]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim()
          .toUpperCase();
        if (prefix.length === 0) {
          await ctx.reply('Họ và Tên đệm không được để trống. Nhập lại:', {
            reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
          });
          return;
        }

        const options = [];
        const seen = new Set();
        while (options.length < 3) {
          const firstName = randomFirstName()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^A-Za-z0-9 ]+/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .toUpperCase();
          const fullName = `${prefix} ${firstName}`.replace(/\s+/g, ' ').trim();
          if (!seen.has(fullName)) {
            seen.add(fullName);
            options.push(fullName);
          }
        }

        randomNameState.set(ctx.from.id, { stage: 'choose_option', prefix, options });
        await ctx.reply(
          `Bạn muốn tạo VA với tên gốc *${prefix}*.\nHãy chọn một trong số các tên mở rộng dưới đây:`,
          {
            parse_mode: 'Markdown',
            reply_markup: Markup.inlineKeyboard([
              [Markup.button.callback(`✅ ${options[0]}`, 'rn_pick:0')],
              [Markup.button.callback(`✅ ${options[1]}`, 'rn_pick:1')],
              [Markup.button.callback(`✅ ${options[2]}`, 'rn_pick:2')],
              [Markup.button.callback('❌ Hủy', 'cancel')],
            ]).reply_markup,
          }
        );
        return;
      }
      if (rnSt.stage === 'choose_option') {
        await ctx.reply('Vui lòng bấm chọn 1 tên ở các nút bên dưới tin nhắn.', menuKeyboard(ctx));
        return;
      }
    }
    const ibft = ibftState.get(ctx.from.id);
    if (ibft) {
      const allowBankPick = (ibft.stage === 'enter_bank' || ibft.stage === 'pick_bank') && text.trim().startsWith('🏦');
      if (isMenuText(text) && !allowBankPick) return next();
      if (ibft.stage === 'enter_bank') {
        const raw = text.trim();

        const pageCount = Math.max(1, Math.ceil(IBFT_BANK_PICK_CODES.length / 12));
        const curPage = Number.isFinite(Number(ibft.page)) ? Number(ibft.page) : 0;
        if (raw === IBFT_NAV_BACK) {
          clearUserStates(ctx.from.id);
          await ctx.reply('Menu:', menuKeyboard(ctx));
          return;
        }
        if (raw === IBFT_NAV_NEXT || raw === IBFT_NAV_NEXT2) {
          const nextPage = Math.min(pageCount - 1, curPage + 1);
          ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_bank', page: nextPage });
          await ctx.reply('🏦 Chọn ngân hàng:', buildIbftBankKeyboard(nextPage));
          return;
        }
        if (raw === IBFT_NAV_PREV) {
          const prevPage = Math.max(0, curPage - 1);
          ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_bank', page: prevPage });
          await ctx.reply('🏦 Chọn ngân hàng:', buildIbftBankKeyboard(prevPage));
          return;
        }

        const directCode = IBFT_BANK_LABEL_TO_CODE.get(raw);
        const normalized = normalizeSearch(raw);

        if (!normalized) {
          await ctx.reply(
            'Mã/tên ngân hàng không hợp lệ. Vui lòng chọn trong danh sách hoặc gõ tên để tìm:',
            buildIbftBankKeyboard(curPage)
          );
          return;
        }

        const exact = directCode ? IBFT_BANKS.find((b) => b.code === directCode) : IBFT_BANKS.find((b) => normalizeSearch(b.code) === normalized);
        if (exact) {
          ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_account', bankCode: exact.code });
          await ctx.reply(`Đã chọn ngân hàng: ${getIbftBankLabel(exact.code)}\nNhập số tài khoản nhận:`, {
            reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
          });
          return;
        }

        const matches = findIbftBanks(raw, 8);
        if (!matches.length) {
          await ctx.reply(
            'Không tìm thấy ngân hàng. Vui lòng chọn trong danh sách hoặc gõ tên để tìm:',
            buildIbftBankKeyboard(curPage)
          );
          return;
        }

        if (matches.length === 1) {
          const b = matches[0];
          ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_account', bankCode: b.code });
          await ctx.reply(`Đã chọn ngân hàng: ${getIbftBankLabel(b.code)}\nNhập số tài khoản nhận:`, {
            reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
          });
          return;
        }

        const buttons = matches.map((b) => `🏦 ${b.code} - ${b.name}`);
        const rows = [];
        for (let i = 0; i < buttons.length; i += 2) rows.push(buttons.slice(i, i + 2));
        rows.push(['❌ Hủy']);
        ibftState.set(ctx.from.id, { ...ibft, stage: 'pick_bank', bankMatches: matches.map((b) => b.code) });
        await ctx.reply('Tìm thấy nhiều ngân hàng, vui lòng chọn:', Markup.keyboard(rows).resize());
        return;
      }

      if (ibft.stage === 'pick_bank') {
        const m = text.trim().match(/^🏦\s*([A-Z0-9]+)\b/);
        const code = m ? m[1].trim().toUpperCase() : '';
        const ok = code && Array.isArray(ibft.bankMatches) && ibft.bankMatches.includes(code);
        const b = IBFT_BANKS.find((x) => x.code === code);
        if (!ok || !b) {
          ibftState.set(ctx.from.id, { stage: 'enter_bank', page: 0 });
          await ctx.reply(
            'Lựa chọn không hợp lệ. Vui lòng chọn trong danh sách hoặc gõ tên để tìm:',
            buildIbftBankKeyboard(0)
          );
          return;
        }
        ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_account', bankCode: b.code, bankMatches: undefined });
        await ctx.reply(`Đã chọn ngân hàng: ${getIbftBankLabel(b.code)}\nNhập số tài khoản nhận:`, {
          reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
        });
        return;
      }

      if (ibft.stage === 'enter_account') {
        const accountNumber = String(text).replace(/[^\d]/g, '');
        if (!accountNumber || accountNumber.length < 6 || accountNumber.length > 24) {
          await ctx.reply('Số tài khoản không hợp lệ. Nhập lại:', Markup.keyboard([['❌ Hủy']]).resize());
          return;
        }
        ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_name', accountNumber });
        await ctx.reply('Nhập tên chủ tài khoản:', Markup.keyboard([['❌ Hủy']]).resize());
        return;
      }

      if (ibft.stage === 'enter_name') {
        const accountName = text
          .trim()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .replace(/[^A-Za-z0-9 ]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim()
          .toUpperCase()
          .slice(0, 100);

        if (!accountName) {
          await ctx.reply('Tên không hợp lệ. Nhập lại:', Markup.keyboard([['❌ Hủy']]).resize());
          return;
        }
        ibftState.set(ctx.from.id, { ...ibft, stage: 'enter_amount', accountName });
        await ctx.reply('Nhập số tiền (VND):', Markup.keyboard([['❌ Hủy']]).resize());
        return;
      }

      if (ibft.stage === 'enter_amount') {
        const amount = Number(String(text).replace(/[^\d]/g, ''));
        if (!amount || amount <= 0) {
          await ctx.reply('Số tiền không hợp lệ. Nhập lại:', Markup.keyboard([['❌ Hủy']]).resize());
          return;
        }

        const rand = crypto.randomBytes(4).toString('hex').toUpperCase();
        const remark = `CH ${String(amount).slice(-4)} ${rand}`.slice(0, 50);
        ibftState.set(ctx.from.id, { ...ibft, stage: 'confirm', amount, remark });

        await ctx.reply(
          `Xác nhận chi hộ:\nNgân hàng: ${ibft.bankCode}\nSTK: ${ibft.accountNumber}\nTên: ${ibft.accountName}\nSố tiền: ${amount.toLocaleString()}đ`,
          {
            reply_markup: Markup.inlineKeyboard([
              [Markup.button.callback('✅ Xác nhận chi hộ', 'ibft_confirm')],
              [Markup.button.callback('❌ Hủy', 'cancel')],
            ]).reply_markup,
          }
        );
        return;
      }

      if (ibft.stage === 'confirm') {
        await ctx.reply('Vui lòng bấm các nút bên dưới tin nhắn xác nhận.', menuKeyboard(ctx));
        return;
      }
    }

    const upd = (id) => awaitingWdUpdate.set(ctx.from.id, { stage: 'choose_status', id });
    const wdU = awaitingWdUpdate.get(ctx.from.id);
    if (wdU) {
      if (isMenuText(text)) return next();
      if (wdU.stage === 'enter_id') {
        const raw = String(text || '').trim();
        const cleaned = raw.replace(/\s+/g, '');
        const candidates = [];
        if (cleaned) candidates.push(cleaned);
        if (cleaned && cleaned.toUpperCase().startsWith('WD')) candidates.push(cleaned.slice(2));
        if (cleaned && !cleaned.toUpperCase().startsWith('WD')) candidates.push(`WD${cleaned}`);
        let w = null;
        let foundId = '';
        for (const cid of candidates) {
          const ww = db.getWithdrawalById(cid);
          if (ww) {
            w = ww;
            foundId = cid;
            break;
          }
        }
        if (!w) {
          await ctx.reply('ID không hợp lệ hoặc không tồn tại. Nhập lại:', Markup.keyboard([['❌ Hủy']]).resize());
          return;
        }
        const id = foundId || w.id;
        const amt = Number(w.amount) || toAmountNumber(w.amount);
        const feeFlat = Number(w.feeFlat) || 0;
        const feePercent = w.feePercent === null || w.feePercent === undefined ? null : Number(w.feePercent) || 0;
        const feeByPercent =
          w.feeByPercent !== null && w.feeByPercent !== undefined ? Number(w.feeByPercent) || 0 : feePercent === null ? 0 : Math.floor((amt * feePercent) / 100);
        const net = Number(w.actualReceive) || Math.max(0, amt - feeFlat - feeByPercent);
        await ctx.reply(
          `🆔 ID: ${displayWithdrawalId(w.id)}\n` +
            `👤 User ID: ${w.userId || ''}\n` +
            `📌 Phương thức: ${w.method}\n` +
            (w.method === 'bank'
              ? `Ngân hàng: ${w.bankName}\nSTK: ${w.bankAccount}\nChủ TK: ${w.bankHolder}\n`
              : `Network: ${w.network}\nVí: ${w.wallet}\n`) +
            `💵 Số tiền trừ: ${amt.toLocaleString()}đ\n` +
            `💸 Phí chuyển: ${feeFlat.toLocaleString()}đ\n` +
            (feePercent !== null ? `📉 Phí rút: ${feePercent}% (${feeByPercent.toLocaleString()}đ)\n` : '') +
            `✅ Số tiền chi hộ (thực nhận): ${net.toLocaleString()}đ (copy: ${copyNumber(net)})\n` +
            `Trạng thái hiện tại: ${w.status}\nChọn trạng thái mới:`,
          {
            reply_markup: Markup.keyboard([['Đã rút', 'Chưa rút'], ['Từ chối', 'Từ chối sai STK/Tên'], ['❌ Hủy']]).resize().reply_markup,
          }
        );
        try {
          await ctx.reply('📋 Copy nhanh:', {
            reply_markup: Markup.inlineKeyboard([
              [Markup.button.callback('📋 Copy Thực nhận', `wd_copy:net:${w.id}`)],
              [
                Markup.button.callback('📋 Copy STK', `wd_copy:acc:${w.id}`),
                Markup.button.callback('📋 Copy ID', `wd_copy:id:${w.id}`),
              ],
            ]).reply_markup,
          });
        } catch (_) {}
        upd(w.id);
        return;
      }
      if (wdU.stage === 'enter_reject_reason') {
        const id = wdU.id;
        const w = db.getWithdrawalById(id);
        if (!w) {
          awaitingWdUpdate.delete(ctx.from.id);
          await ctx.reply('ID không hợp lệ hoặc không tồn tại.', menuKeyboard(ctx));
          return;
        }
        const reason = String(text || '').trim().replace(/\s+/g, ' ');
        if (!reason) {
          await ctx.reply('Lý do trống. Nhập lại lý do từ chối:', Markup.keyboard([['❌ Hủy']]).resize());
          return;
        }
        if (w.status !== 'reject') {
          const u = db.getUser(w.userId);
          const delta = Number(w.amount) || 0;
          const after = u.balance + delta;
          db.updateUser(w.userId, { balance: after });
          try {
            db.addUserBalanceHistory({ ts: Date.now(), userId: w.userId, delta, balanceAfter: after, reason: 'withdraw_refund_reason', ref: w.id });
          } catch (_) {}
        }
        db.updateWithdrawalStatus(id, 'reject', { rejectReason: 'admin_reject', rejectNote: reason });
        awaitingWdUpdate.delete(ctx.from.id);
        await ctx.reply(`Đã cập nhật ${displayWithdrawalId(id)} → Từ chối (đã hoàn tiền).\nLý do: ${reason}`, menuKeyboard(ctx));
        if (w.userId) {
          try {
            await bot.telegram.sendMessage(w.userId, `Yêu cầu rút tiền ${displayWithdrawalId(w.id)} đã bị từ chối và hoàn tiền.\nLý do: ${reason}`);
          } catch (_) {}
        }
        return;
      }
    }
    const vaSt = vaContentState.get(ctx.from.id);
    if (vaSt) {
      if (isMenuText(text)) {
        vaContentState.delete(ctx.from.id);
        return next();
      }
      vaContentState.delete(ctx.from.id);
      const content = String(text || '').trim().replace(/\s+/g, ' ');
      await handleCreateVA(ctx, vaSt.name, vaSt.bank, content);
      return;
    }
    const wst = withdrawState.get(ctx.from.id);
    if (wst) {
      if (isMenuText(text) && !(wst.stage === 'amount' && text === 'Rút ALL')) return next();
      if (wst.stage === 'choose_saved' || wst.stage === 'delete_saved') {
        await ctx.reply('Vui lòng chọn bằng các nút bên dưới tin nhắn.', menuKeyboard(ctx));
        return;
      }
      if (wst.stage === 'choose_bank') {
        await ctx.reply('Vui lòng chọn ngân hàng bằng các nút bên dưới tin nhắn “🏦 Chọn ngân hàng”.', menuKeyboard(ctx));
        return;
      }

      if (wst.stage === 'bank_name') {
        withdrawState.set(ctx.from.id, { ...wst, stage: 'bank_account', bankName: text.trim().slice(0, 50) });
        await ctx.reply('Nhập số tài khoản:', Markup.keyboard([['❌ Hủy']]).resize());
        return;
      }
      if (wst.stage === 'bank_account') {
        const bankAccount = normalizeWdBankAccount(text);
        if (!bankAccount || bankAccount.length < 6) {
          await ctx.reply('Số tài khoản không hợp lệ. Nhập lại:', {
            reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
          });
          return;
        }
        withdrawState.set(ctx.from.id, { ...wst, stage: 'bank_holder', bankAccount });
        await ctx.reply('Nhập tên chủ tài khoản:', {
          reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
        });
        return;
      }
      if (wst.stage === 'bank_holder') {
        const bankHolder = normalizeWdBankHolder(text);
        if (!bankHolder) {
          await ctx.reply('Tên chủ tài khoản không hợp lệ. Nhập lại:', {
            reply_markup: Markup.inlineKeyboard([[Markup.button.callback('❌ Hủy', 'cancel')]]).reply_markup,
          });
          return;
        }
        saveUserWithdrawBank(ctx.from.id, {
          bankCode: wst.bankCode,
          bankName: wst.bankName,
          bankAccount: wst.bankAccount,
          bankHolder,
        });
        withdrawState.set(ctx.from.id, { ...wst, stage: 'amount', bankHolder });
        await ctx.reply('Nhập số tiền rút:', Markup.keyboard([['Rút ALL'], ['❌ Hủy']]).resize());
        return;
      }
      if (wst.stage === 'amount') {
        const user = db.getUser(ctx.from.id);
        const balanceBefore = Number(user.balance) || 0;
        let amount;
        if (text === 'Rút ALL') {
          amount = balanceBefore;
        } else {
          amount = Number(text.replace(/[^\d]/g, ''));
        }

        if (!amount || amount <= 0) {
          await ctx.reply('Số tiền không hợp lệ. Nhập lại:', Markup.keyboard([['Rút ALL'], ['❌ Hủy']]).resize());
          return;
        }
        
        // Cập nhật lại số dư mới nhất từ DB để tránh lỗi đồng bộ
        if (amount > balanceBefore) {
          await ctx.reply(`Số dư không đủ. Bạn chỉ có ${balanceBefore.toLocaleString()}đ. Nhập lại:`, Markup.keyboard([['Rút ALL'], ['❌ Hủy']]).resize());
          return;
        }
        
        const config = db.getConfig();
        let feeFlat = Math.max(0, Number(config.withdrawFeeFlat ?? config.ipnFeeFlat ?? 0) || 0);
        const userFeeFlat = user.withdrawFeeFlat !== null && user.withdrawFeeFlat !== undefined ? Number(user.withdrawFeeFlat) : NaN;
        if (Number.isFinite(userFeeFlat) && userFeeFlat >= 0) feeFlat = userFeeFlat;
        const feePercent = user.feePercent !== null ? user.feePercent : config.globalFeePercent;
        const feeByPercent = Math.floor((amount * Number(feePercent || 0)) / 100);
        const actualReceive = Math.max(0, amount - feeFlat - feeByPercent);

        const id = `${Date.now().toString().slice(-10)}${Math.floor(100000 + Math.random() * 900000)}`;
        const balanceAfter = balanceBefore - amount;
        db.updateUser(ctx.from.id, { balance: balanceAfter });
        try {
          db.addUserBalanceHistory({ ts: Date.now(), userId: ctx.from.id, delta: -amount, balanceAfter, reason: 'withdraw_create', ref: id });
        } catch (_) {}

        const rec = {
          id,
          userId: ctx.from.id,
          username: ctx.from.username || '',
          method: wst.method,
          bankName: wst.bankName,
          bankAccount: wst.bankAccount,
          bankHolder: wst.bankHolder,
          network: wst.network,
          wallet: wst.wallet,
          amount,
          feeFlat,
          feePercent,
          feeByPercent,
          actualReceive,
          balanceBefore,
          balanceAfter,
          createdAt: Date.now(),
          status: 'pending',
        };
        db.addWithdrawal(rec);
        withdrawState.delete(ctx.from.id);
        const userAfter = db.getUser(ctx.from.id);
        const msgUser =
          `✅ Yêu cầu rút tiền đã được tạo\n\n` +
          `💵 Số tiền rút: ${amount.toLocaleString()} đ\n` +
          `💸 Phí chuyển: ${feeFlat.toLocaleString()} đ\n` +
          `📉 Phí rút: ${feePercent}%\n` +
          `✅ Thực nhận: ${actualReceive.toLocaleString()} đ\n` +
          `🏦 Ngân hàng: ${rec.bankName}\n` +
          `💳 STK: ${rec.bankAccount}\n` +
          `👤 Chủ TK: ${rec.bankHolder}\n` +
          `💰 Số dư còn lại: ${Number(userAfter.balance || 0).toLocaleString()} đ\n\n` +
          `⏳ Đang chờ duyệt...`;
        await ctx.reply(msgUser, menuKeyboard(ctx));
        const adminRaw = process.env.ADMIN_IDS || '';
        const adminIds = adminRaw.split(',').map((s) => s.trim()).filter(Boolean);
        for (const aid of adminIds) {
          try {
            const userLabel = ctx.from.username ? `@${ctx.from.username}` : String(ctx.from.id);
            const msgAdmin =
              `🆕 Yêu cầu rút tiền mới\n\n` +
              `🆔 ID: ${displayWithdrawalId(id)}\n` +
              `👤 User: ${userLabel}\n` +
              `🆔 User ID: ${ctx.from.id}\n` +
              `💰 Số dư user: ${balanceBefore.toLocaleString()}đ\n\n` +
              `🏦 Ngân hàng: ${rec.bankName}\n` +
              `💳 STK: ${rec.bankAccount}\n` +
              `👤 Chủ TK: ${rec.bankHolder}\n\n` +
              `💵 Số tiền trừ: ${amount.toLocaleString()}đ\n` +
              `💸 Phí chuyển: ${feeFlat.toLocaleString()}đ\n` +
              `📉 Phí rút: ${feePercent}%\n` +
              `💰 Thực nhận: ${actualReceive.toLocaleString()}đ (copy: ${copyNumber(actualReceive)})`;
            await bot.telegram.sendMessage(aid, msgAdmin, {
              reply_markup: Markup.inlineKeyboard([
                [Markup.button.callback('📋 Copy Thực nhận', `wd_copy:net:${id}`)],
                [
                  Markup.button.callback('📋 Copy STK', `wd_copy:acc:${id}`),
                  Markup.button.callback('📋 Copy ID', `wd_copy:id:${id}`),
                ],
              ]).reply_markup,
            });
          } catch (_) {}
        }
        return;
      }
    }
    if (awaitingName.get(ctx.from.id)) {
      if (isMenuText(text)) {
        awaitingName.delete(ctx.from.id);
        return next();
      }
      awaitingName.delete(ctx.from.id);
      const rawName = String(text || '').trim().replace(/\s+/g, ' ');
      const endsOk = /[0-9A-Za-zÀ-ỹ]$/u.test(rawName);
      const wordCount = rawName
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^A-Za-z0-9 ]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .split(' ')
        .filter(Boolean).length;
      if (!rawName || !endsOk || wordCount < 2) {
        await ctx.reply('Vui lòng nhập đầy đủ Họ và Tên (ví dụ: Nguyen Van A). Không được kết thúc bằng dấu chấm.', menuKeyboard(ctx));
        return;
      }
      
      confirmCreateState.set(ctx.from.id, rawName);
      await ctx.reply(`Bạn chuẩn bị tạo VA với tên: *${rawName}*\n\nVui lòng xác nhận:`, {
        parse_mode: 'Markdown',
        reply_markup: Markup.inlineKeyboard([
          [Markup.button.callback('✅ Xác nhận tạo', 'va_confirm')],
          [Markup.button.callback('❌ Hủy', 'cancel')],
        ]).reply_markup
      });
      return;
    }
    if (awaitingStatus.get(ctx.from.id)) {
      if (isMenuText(text)) {
        awaitingStatus.delete(ctx.from.id);
        return next();
      }
      awaitingStatus.delete(ctx.from.id);
      const acc = String(text || '').replace(/[^\d]/g, '');
      if (!acc) {
        await ctx.reply('Số tài khoản (vaAccount) trống.', menuKeyboard(ctx));
        return;
      }
      await ctx.replyWithMarkdown(formatStatusByVaAccount(acc), menuKeyboard(ctx));
      return;
    }
    return next();
  });

  bot.launch();

  process.once('SIGINT', () => bot.stop('SIGINT'));
  process.once('SIGTERM', () => bot.stop('SIGTERM'));
}
