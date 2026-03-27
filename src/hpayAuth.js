const axios = require('axios');

let cache = {
  token: '',
  expiresAt: 0,
  scope: '',
  clientId: '',
  mid: '',
};

function isValid() {
  return cache.token && Date.now() < cache.expiresAt - 30000;
}

async function getAccessToken(scope = 'va', { clientId, clientSecret, mid } = {}) {
  const resolvedBaseUrl = process.env.HPAY_BASE_URL || 'https://openapi-sandbox.htpgroup.com.vn';
  const resolvedClientId = (clientId || process.env.HPAY_CLIENT_ID || '').trim();
  const resolvedClientSecret = (clientSecret || process.env.HPAY_CLIENT_SECRET || '').trim();
  const resolvedMid = (mid || process.env.HPAY_X_API_MID || '').trim();

  if (isValid() && cache.scope === scope && cache.clientId === resolvedClientId && cache.mid === resolvedMid) {
    return { access_token: cache.token, expires_in: Math.floor((cache.expiresAt - Date.now()) / 1000) };
  }
  const url = `${resolvedBaseUrl}/service/${scope}/v1/oauth2/token`;
  const params = new URLSearchParams();
  params.set('client_id', resolvedClientId);
  params.set('client_secret', resolvedClientSecret);
  params.set('grant_type', 'client_credentials');
  params.set('scope', scope);
  const headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
  };
  if (resolvedMid) headers['x-api-mid'] = resolvedMid;
  const res = await axios.post(url, params.toString(), { headers, timeout: 15000 });
  const token = res.data?.access_token || '';
  const expiresIn = res.data?.expires_in || 0;
  if (token && expiresIn) {
    cache = {
      token,
      expiresAt: Date.now() + expiresIn * 1000,
      scope,
      clientId: resolvedClientId,
      mid: resolvedMid,
    };
  }
  return { access_token: token, expires_in: expiresIn };
}

module.exports = { getAccessToken };
