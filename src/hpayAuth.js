const axios = require('axios');

let cache = {
  token: '',
  expiresAt: 0,
  scope: '',
};

function isValid() {
  return cache.token && Date.now() < cache.expiresAt - 30000;
}

async function getAccessToken(scope = 'va') {
  if (isValid() && cache.scope === scope) {
    return { access_token: cache.token, expires_in: Math.floor((cache.expiresAt - Date.now()) / 1000) };
  }
  const baseUrl = process.env.HPAY_BASE_URL || 'https://openapi-sandbox.htpgroup.com.vn';
  const clientId = process.env.HPAY_CLIENT_ID || '';
  const clientSecret = process.env.HPAY_CLIENT_SECRET || '';
  const url = `${baseUrl}/service/${scope}/v1/oauth2/token`;
  const params = new URLSearchParams();
  params.set('client_id', clientId);
  params.set('client_secret', clientSecret);
  params.set('grant_type', 'client_credentials');
  params.set('scope', scope);
  const mid = process.env.HPAY_X_API_MID || '';
  const headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
  };
  if (mid) headers['x-api-mid'] = mid;
  const res = await axios.post(url, params.toString(), { headers, timeout: 15000 });
  const token = res.data?.access_token || '';
  const expiresIn = res.data?.expires_in || 0;
  if (token && expiresIn) {
    cache = { token, expiresAt: Date.now() + expiresIn * 1000, scope };
  }
  return { access_token: token, expires_in: expiresIn };
}

module.exports = { getAccessToken };
