"""
DealDrop — Deal Notifier
-------------------------
Reads deals.json, finds new hot deals (≥50% off) that haven't
been announced yet, then fires email via Mailchimp and SMS via Twilio.

Run automatically by fetch_deals.py after every refresh.

Requirements:
    pip install requests mailchimp-marketing twilio

Setup:
    Fill in the CONFIG section below with your API keys.
"""

import json
import os
import hashlib
from datetime import datetime

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────

# Mailchimp
MAILCHIMP_API_KEY   = os.environ.get("MAILCHIMP_API_KEY",   "YOUR_MAILCHIMP_API_KEY")
MAILCHIMP_SERVER    = os.environ.get("MAILCHIMP_SERVER",    "us1")   # e.g. us1, us2
MAILCHIMP_LIST_ID   = os.environ.get("MAILCHIMP_LIST_ID",   "YOUR_LIST_ID")
MAILCHIMP_FROM_NAME = "DealDrop"
MAILCHIMP_FROM_EMAIL= "deals@yourdomain.com"   # Must be verified in Mailchimp

# Twilio
TWILIO_ACCOUNT_SID  = os.environ.get("TWILIO_ACCOUNT_SID",  "YOUR_TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN   = os.environ.get("TWILIO_AUTH_TOKEN",   "YOUR_TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER  = os.environ.get("TWILIO_FROM_NUMBER",  "+1XXXXXXXXXX")  # Your Twilio number
TWILIO_LIST_FILE    = "sms_subscribers.json"   # Local file storing SMS subscribers

# Site
SITE_URL            = "https://yourdomain.com"

# State file — tracks which deals have already been announced
NOTIFIED_FILE       = "notified_deals.json"

# Minimum discount to trigger a notification
HOT_DEAL_PCT        = 50

# ─── LOAD STATE ────────────────────────────────────────────────────────────────

def load_notified():
    """Load set of already-notified deal IDs."""
    try:
        with open(NOTIFIED_FILE) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def save_notified(notified_set):
    """Persist notified deal IDs so we don't re-notify."""
    with open(NOTIFIED_FILE, "w") as f:
        json.dump(list(notified_set), f)

def deal_fingerprint(deal):
    """Unique ID for a deal — use ASIN if available, else hash title+price."""
    if deal.get("asin"):
        return deal["asin"]
    raw = f"{deal.get('title','')}-{deal.get('price','')}"
    return hashlib.md5(raw.encode()).hexdigest()

# ─── LOAD DEALS ────────────────────────────────────────────────────────────────

def load_hot_deals():
    """Read deals.json and return deals that qualify as hot."""
    try:
        with open("deals.json") as f:
            data = json.load(f)
        return [d for d in data.get("deals", []) if d.get("pct", 0) >= HOT_DEAL_PCT]
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  Could not load deals.json: {e}")
        return []

# ─── EMAIL VIA MAILCHIMP ───────────────────────────────────────────────────────

def build_email_html(new_deals):
    """Build a clean HTML email showing the new hot deals."""
    items_html = ""
    for d in new_deals:
        img_html = (
            f'<img src="{d["image"]}" width="80" height="80" '
            f'style="object-fit:contain;border-radius:6px;background:#f5f3f0;" />'
            if d.get("image") else
            f'<div style="width:80px;height:80px;background:#f5f3f0;border-radius:6px;'
            f'display:flex;align-items:center;justify-content:center;font-size:32px;">'
            f'{d.get("emoji","🛒")}</div>'
        )
        items_html += f"""
        <tr>
          <td style="padding:16px 0;border-bottom:1px solid #e8e6e1;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td width="90" valign="top">{img_html}</td>
                <td valign="top" style="padding-left:14px;">
                  <p style="margin:0 0 4px;font-size:14px;font-weight:600;color:#1a1a18;line-height:1.4;">{d.get('title','')}</p>
                  <p style="margin:0 0 8px;font-size:12px;color:#6b6b65;">{d.get('cat','')} &nbsp;·&nbsp; {d.get('desc','')}</p>
                  <span style="font-size:20px;font-weight:700;color:#c94040;">{d.get('price','')}</span>
                  &nbsp;
                  <span style="font-size:13px;color:#9e9e97;text-decoration:line-through;">{d.get('was','')}</span>
                  &nbsp;
                  <span style="background:#fff3d4;color:#8a5c00;font-size:11px;font-weight:700;padding:3px 8px;border-radius:100px;">{d.get('pct',0)}% off</span>
                  <br/><br/>
                  <a href="{d.get('link', SITE_URL)}" style="display:inline-block;padding:8px 18px;background:#c94040;color:white;text-decoration:none;border-radius:7px;font-size:13px;font-weight:600;">Grab Deal →</a>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f9f8f5;font-family:'DM Sans',Arial,sans-serif;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#f9f8f5;padding:32px 16px;">
        <tr><td align="center">
          <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #e8e6e1;">

            <!-- Header -->
            <tr>
              <td style="background:#2a6041;padding:24px 32px;">
                <h1 style="margin:0;font-size:24px;color:white;font-weight:700;">🔥 Hot Deals Alert</h1>
                <p style="margin:6px 0 0;font-size:14px;color:rgba(255,255,255,0.8);">
                  {len(new_deals)} new deal{'s' if len(new_deals) != 1 else ''} just dropped — {datetime.now().strftime('%b %d, %Y')}
                </p>
              </td>
            </tr>

            <!-- Deals -->
            <tr>
              <td style="padding:8px 32px 0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  {items_html}
                </table>
              </td>
            </tr>

            <!-- CTA -->
            <tr>
              <td style="padding:24px 32px;text-align:center;">
                <a href="{SITE_URL}" style="display:inline-block;padding:12px 32px;background:#2a6041;color:white;text-decoration:none;border-radius:8px;font-size:14px;font-weight:600;">
                  See All Deals →
                </a>
              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="padding:16px 32px 24px;border-top:1px solid #e8e6e1;text-align:center;">
                <p style="margin:0;font-size:11px;color:#9e9e97;line-height:1.6;">
                  You're receiving this because you subscribed at {SITE_URL}.<br/>
                  Affiliate links — we may earn a commission at no extra cost to you.<br/>
                  <a href="*|UNSUB|*" style="color:#9e9e97;">Unsubscribe</a>
                </p>
              </td>
            </tr>

          </table>
        </td></tr>
      </table>
    </body>
    </html>
    """

def send_mailchimp_campaign(new_deals):
    """Create and send a Mailchimp campaign to all subscribers."""
    if not MAILCHIMP_API_KEY or MAILCHIMP_API_KEY == "YOUR_MAILCHIMP_API_KEY":
        print("  [Email] Mailchimp not configured — skipping.")
        return False

    base_url = f"https://{MAILCHIMP_SERVER}.api.mailchimp.com/3.0"
    headers = {"Authorization": f"Bearer {MAILCHIMP_API_KEY}", "Content-Type": "application/json"}

    subject = f"🔥 {len(new_deals)} Hot Deal{'s' if len(new_deals) != 1 else ''} — Up to {max(d.get('pct',0) for d in new_deals)}% Off"

    try:
        # Create campaign
        campaign_data = {
            "type": "regular",
            "recipients": {"list_id": MAILCHIMP_LIST_ID},
            "settings": {
                "subject_line": subject,
                "from_name": MAILCHIMP_FROM_NAME,
                "reply_to": MAILCHIMP_FROM_EMAIL,
                "title": f"DealDrop Alert {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            }
        }
        r = requests.post(f"{base_url}/campaigns", json=campaign_data, headers=headers, timeout=15)
        r.raise_for_status()
        campaign_id = r.json()["id"]

        # Set content
        content_data = {"html": build_email_html(new_deals)}
        r = requests.put(f"{base_url}/campaigns/{campaign_id}/content", json=content_data, headers=headers, timeout=15)
        r.raise_for_status()

        # Send
        r = requests.post(f"{base_url}/campaigns/{campaign_id}/actions/send", headers=headers, timeout=15)
        r.raise_for_status()

        print(f"  [Email] ✓ Sent campaign '{subject}' to list {MAILCHIMP_LIST_ID}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"  [Email] ERROR sending Mailchimp campaign: {e}")
        return False

# ─── SMS VIA TWILIO ────────────────────────────────────────────────────────────

def load_sms_subscribers():
    """Load list of SMS subscriber phone numbers."""
    try:
        with open(TWILIO_LIST_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def build_sms_message(new_deals):
    """Build a short SMS message for hot deals."""
    top = new_deals[0]
    msg = f"🔥 DealDrop Hot Deal: {top.get('title','')[:50]}... {top.get('pct',0)}% off — {top.get('price','')} (was {top.get('was','')})\n{SITE_URL}"
    if len(new_deals) > 1:
        msg += f"\n+{len(new_deals)-1} more hot deal{'s' if len(new_deals)-1 != 1 else ''} live now."
    return msg

def send_twilio_sms(new_deals):
    """Send SMS to all subscribers via Twilio."""
    if not TWILIO_ACCOUNT_SID or TWILIO_ACCOUNT_SID == "YOUR_TWILIO_ACCOUNT_SID":
        print("  [SMS] Twilio not configured — skipping.")
        return False

    subscribers = load_sms_subscribers()
    if not subscribers:
        print("  [SMS] No SMS subscribers yet.")
        return False

    message = build_sms_message(new_deals)
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    auth = (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

    sent = 0
    failed = 0
    for number in subscribers:
        try:
            r = requests.post(url, auth=auth, data={
                "From": TWILIO_FROM_NUMBER,
                "To": number,
                "Body": message,
            }, timeout=15)
            r.raise_for_status()
            sent += 1
        except requests.exceptions.RequestException as e:
            print(f"  [SMS] Failed to send to {number}: {e}")
            failed += 1

    print(f"  [SMS] ✓ Sent to {sent} subscribers ({failed} failed)")
    return sent > 0

# ─── MAIN ──────────────────────────────────────────────────────────────────────

def run_notifications():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking for new hot deals to notify...")

    hot_deals   = load_hot_deals()
    notified    = load_notified()

    # Find deals we haven't notified about yet
    new_deals = [d for d in hot_deals if deal_fingerprint(d) not in notified]

    if not new_deals:
        print("  No new hot deals to notify about.")
        return

    print(f"  Found {len(new_deals)} new hot deal(s) — sending notifications...")

    email_ok = send_mailchimp_campaign(new_deals)
    sms_ok   = send_twilio_sms(new_deals)

    if email_ok or sms_ok:
        # Mark these deals as notified so we don't send again
        for d in new_deals:
            notified.add(deal_fingerprint(d))
        save_notified(notified)
        print(f"  ✓ Notifications sent and {len(new_deals)} deals marked as notified.")
    else:
        print("  No notifications were sent successfully.")

if __name__ == "__main__":
    run_notifications()
