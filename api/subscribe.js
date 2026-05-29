function json(res, status, payload) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(payload));
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

async function sendToAppsScript(email, phone) {
  const endpoint = process.env.GOOGLE_APPS_SCRIPT_URL || process.env.SUBSCRIBER_SCRIPT_URL;

  if (!endpoint) {
    throw new Error('Missing GOOGLE_APPS_SCRIPT_URL environment variable');
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ email, phone }).toString()
  });

  const text = await response.text();
  let data = {};
  try {
    data = JSON.parse(text);
  } catch (error) {
    throw new Error('Apps Script did not return JSON');
  }

  if (!response.ok || data.success !== true) {
    throw new Error(data.error || 'Unable to save signup');
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

    await sendToAppsScript(email, phone);
    json(res, 200, { ok: true });
  } catch (error) {
    console.error('Subscribe error:', error && error.message ? error.message : error);
    json(res, 500, { ok: false, error: error && error.message ? error.message : 'Unable to save signup' });
  }
};
