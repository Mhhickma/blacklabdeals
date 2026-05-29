const crypto = require('crypto');

const DEFAULT_SPREADSHEET_ID = '1egThCqGYkEoAGF7JEVHlvn6ks1Fus3vrq1R1WxKakr4';
const DEFAULT_RANGE = 'A:C';

function json(res, status, payload) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(payload));
}

function b64url(input) {
  return Buffer.from(input)
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function normalizePrivateKey(key) {
  return String(key || '').replace(/\\n/g, '\n');
}

function clean(value, maxLength) {
  return String(value || '').trim().slice(0, maxLength || 200);
}

function isValidEmail(value) {
  if (!value) return true;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function isValidPhone(value) {
  if (!value) return true;
  const digits = String(value).replace(/\D/g, '');
  return digits.length >= 7 && digits.length <= 15;
}

async function getAccessToken() {
  const clientEmail = process.env.GOOGLE_CLIENT_EMAIL;
  const privateKey = normalizePrivateKey(process.env.GOOGLE_PRIVATE_KEY);

  if (!clientEmail || !privateKey) {
    throw new Error('Missing Google service account environment variables');
  }

  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const claim = {
    iss: clientEmail,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now
  };

  const unsigned = b64url(JSON.stringify(header)) + '.' + b64url(JSON.stringify(claim));
  const signature = crypto
    .createSign('RSA-SHA256')
    .update(unsigned)
    .sign(privateKey, 'base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

  const assertion = unsigned + '.' + signature;
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion
    }).toString()
  });

  const data = await response.json();
  if (!response.ok || !data.access_token) {
    throw new Error(data.error_description || data.error || 'Unable to get Google access token');
  }

  return data.access_token;
}

async function appendToSheet(row) {
  const spreadsheetId = process.env.GOOGLE_SHEET_ID || DEFAULT_SPREADSHEET_ID;
  const range = process.env.GOOGLE_SHEET_RANGE || DEFAULT_RANGE;
  const token = await getAccessToken();
  const url = 'https://sheets.googleapis.com/v4/spreadsheets/'
    + encodeURIComponent(spreadsheetId)
    + '/values/'
    + encodeURIComponent(range)
    + ':append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS';

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ values: [row] })
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error && data.error.message ? data.error.message : 'Unable to append to sheet');
  }
  return data;
}

module.exports = async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.end();
    return;
  }

  if (req.method !== 'POST') {
    json(res, 405, { ok: false, error: 'Method not allowed' });
    return;
  }

  try {
    const body = typeof req.body === 'object' && req.body ? req.body : JSON.parse(req.body || '{}');
    const email = clean(body.email, 200);
    const phone = clean(body.phone, 40);

    if (!email && !phone) {
      json(res, 400, { ok: false, error: 'Email or phone is required' });
      return;
    }

    if (!isValidEmail(email)) {
      json(res, 400, { ok: false, error: 'Enter a valid email address' });
      return;
    }

    if (!isValidPhone(phone)) {
      json(res, 400, { ok: false, error: 'Enter a valid phone number' });
      return;
    }

    const timestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
    await appendToSheet([timestamp, email, phone]);
    json(res, 200, { ok: true });
  } catch (error) {
    console.error('Subscribe error:', error && error.message ? error.message : error);
    json(res, 500, { ok: false, error: 'Unable to save signup' });
  }
};
