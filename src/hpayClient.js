const axios = require('axios');
const fs = require('fs');
const crypto = require('crypto');
const { getAccessToken } = require('./hpayAuth');

function readPrivateKey() {
  const envKeyB64 = process.env.HPAY_PRIVATE_KEY_B64;
  if (envKeyB64 && envKeyB64.trim().length > 0) {
    try {
      return Buffer.from(envKeyB64, 'base64').toString('utf8');
    } catch (_) {
      return null;
    }
  }
  const envKey = process.env.HPAY_PRIVATE_KEY;
  if (envKey && envKey.trim().length > 0) {
    return envKey.replace(/\\n/g, '\n');
  }
  const keyFile = process.env.HPAY_PRIVATE_KEY_FILE;
  if (keyFile && fs.existsSync(keyFile)) {
    return fs.readFileSync(keyFile, 'utf8');
  }
  return null;
}

function makeRequestId() {
  const t = Date.now().toString().slice(-10);
  const r = Math.floor(100000 + Math.random() * 900000).toString();
  return (t + r).slice(0, 20);
}

function signRSASHA256(message, privateKey) {
  const signer = crypto.createSign('RSA-SHA256');
  signer.update(message);
  signer.end();
  return signer.sign(privateKey, 'base64');
}

function buildHeaders(authHeader) {
  const mid = process.env.HPAY_X_API_MID || '';
  const rawAuth = typeof authHeader === 'string' ? authHeader : process.env.HPAY_AUTH_TOKEN || '';
  const auth =
    rawAuth.toLowerCase().startsWith('bearer ') ? rawAuth : rawAuth ? `Bearer ${rawAuth}` : '';
  return {
    'x-api-mid': mid,
    Authorization: auth,
    'Content-Type': 'application/json',
  };
}

async function createVirtualAccount({ requestId, vaName, vaType = '1', vaCondition = '2', vaAmount, remark, vaExpirationTime }) {
  const baseUrl = process.env.HPAY_BASE_URL || 'https://openapi-sandbox.htpgroup.com.vn';
  const url = `${baseUrl}/service/va/v1/create`;

  const merchantId = process.env.HPAY_MERCHANT_ID || '';
  const passcode = process.env.HPAY_PASSCODE || '';
  const privateKey = readPrivateKey();
  if (!privateKey) {
    throw new Error('Không tìm thấy private key: cấu hình HPAY_PRIVATE_KEY_B64 hoặc HPAY_PRIVATE_KEY (một dòng với \\n) hoặc HPAY_PRIVATE_KEY_FILE');
  }
  try {
    crypto.createPrivateKey({ key: privateKey, format: 'pem' });
  } catch (e) {
    throw new Error('Private key không hợp lệ (PEM). Kiểm tra định dạng: BEGIN ... PRIVATE KEY ... END; nếu dùng .env, dùng 1 dòng với \\n hoặc HPAY_PRIVATE_KEY_B64');
  }

  const payload = {
    requestId: requestId || makeRequestId(),
    merchantId,
    vaType,
    vaName,
    vaCondition,
  };
  if (vaAmount && String(vaAmount).trim() !== '') payload.vaAmount = String(vaAmount);
  if (remark && remark.trim() !== '') payload.remark = remark;
  if (vaExpirationTime) payload.vaExpirationTime = Number(vaExpirationTime);

  const data = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64');
  let signature = '';
  if (privateKey && passcode) {
    const message = `${data}|${passcode}`;
    signature = signRSASHA256(message, privateKey);
  }
  if (!signature) {
    throw new Error('Thiếu chữ ký: cần cấu hình HPAY_PRIVATE_KEY hoặc HPAY_PRIVATE_KEY_FILE (private key RSA 2048) và HPAY_PASSCODE');
  }

  let authHeader = '';
  try {
    const tokenResp = await getAccessToken(process.env.HPAY_TOKEN_SCOPE || 'va');
    if (tokenResp && tokenResp.access_token) {
      authHeader = `Bearer ${tokenResp.access_token}`;
    }
  } catch (_) {}
  
  if (!authHeader) {
    const rawAuth = process.env.HPAY_AUTH_TOKEN || '';
    authHeader = rawAuth.toLowerCase().startsWith('bearer ') ? rawAuth : rawAuth ? `Bearer ${rawAuth}` : '';
  }
  const headers = buildHeaders(authHeader);
  const body = { data, signature };

  const res = await axios.post(url, body, { headers, timeout: 20000 });
  const resData = res.data || {};
  let decoded = null;
  try {
    if (resData.data) {
      const buf = Buffer.from(resData.data, 'base64');
      decoded = JSON.parse(buf.toString('utf8'));
    }
  } catch (_) {}
  return { raw: resData, decoded, requestId: payload.requestId };
}

async function getAccountBalance({ requestId } = {}) {
  const baseUrl = process.env.HPAY_BASE_URL || 'https://openapi-sandbox.htpgroup.com.vn';
  const url = `${baseUrl}/service/account/v1/get-balance`;
  const merchantId = process.env.HPAY_MERCHANT_ID || '';
  const passcode = process.env.HPAY_PASSCODE || '';
  const privateKey = readPrivateKey();
  if (!privateKey) {
    throw new Error('Không tìm thấy private key: cấu hình HPAY_PRIVATE_KEY_B64 hoặc HPAY_PRIVATE_KEY (một dòng với \\n) hoặc HPAY_PRIVATE_KEY_FILE');
  }
  try {
    crypto.createPrivateKey({ key: privateKey, format: 'pem' });
  } catch (_) {
    throw new Error('Private key không hợp lệ (PEM). Kiểm tra định dạng');
  }
  const payload = {
    requestId: requestId || makeRequestId(),
    merchantId,
  };
  const data = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64');
  let signature = '';
  if (privateKey && passcode) {
    const message = `${data}|${passcode}`;
    signature = signRSASHA256(message, privateKey);
  }
  if (!signature) {
    throw new Error('Thiếu chữ ký: cần cấu hình private key và passcode');
  }
  // Luôn dùng token theo scope 'account' để tránh dùng nhầm token 'va' từ .env
  let authHeader = '';
  try {
    const tokenResp = await getAccessToken('account');
    if (tokenResp && tokenResp.access_token) {
      authHeader = `Bearer ${tokenResp.access_token}`;
    }
  } catch (_) {}
  // Fallback (ít khả năng cần) nếu lấy token thất bại
  if (!authHeader) {
    const rawAuth = process.env.HPAY_AUTH_TOKEN || '';
    authHeader = rawAuth.toLowerCase().startsWith('bearer ') ? rawAuth : rawAuth ? `Bearer ${rawAuth}` : '';
  }
  const headers = buildHeaders(authHeader);
  const body = { data, signature };
  const res = await axios.post(url, body, { headers, timeout: 20000 });
  const resData = res.data || {};
  let decoded = null;
  try {
    if (resData.data) {
      const buf = Buffer.from(resData.data, 'base64');
      decoded = JSON.parse(buf.toString('utf8'));
    }
  } catch (_) {}
  return { raw: resData, decoded, requestId: payload.requestId };
}

module.exports = {
  createVirtualAccount,
  getAccountBalance,
}
