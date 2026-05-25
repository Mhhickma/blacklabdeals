# Amazon Compliant Product Card Test

This folder is for testing a compliant Black Lab Deals product-card workflow before changing the live public pages.

## Goal

Create product cards where the public card data comes from the Amazon Creator API / Product Advertising API, not from Keepa price data.

## Rules for this test

- Keepa may be used only to identify ASINs privately.
- Keepa should pass forward only ASIN and category.
- Public cards should not display Keepa-derived price data.
- Public cards should not show Hot Deal labels, price-drop labels, discount percentages, was prices, lowest prices, highest prices, price history, price alerts, or average discount data.
- Public cards may show current Amazon price only if it comes from the Amazon Creator API / PA API.
- Every displayed price must include a date/time stamp.
- Every displayed price must expire after 23 hours.
- If a price is expired and cannot be refreshed, the public card should hide the price and show: Check current price on Amazon.

## Compliant card fields

The public card should only include:

- Product title from approved Amazon API data
- Product image from approved Amazon API data, if allowed by API rules
- Category
- Current Amazon price from approved Amazon API data
- Price shown as of date/time
- Required price and availability disclaimer
- View on Amazon button with the proper affiliate tag

## Required disclaimer

Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.

## Example public data structure

```json
{
  "asin": "B000000000",
  "title": "Product title from Amazon API",
  "category": "Tools & Home Improvement",
  "image": "Amazon API image URL",
  "currentPrice": "$31.00",
  "priceFetchedAt": "2026-05-25T20:15:00Z",
  "priceExpiresAt": "2026-05-26T19:15:00Z",
  "amazonUrl": "https://www.amazon.com/dp/B000000000?tag=simplewoodsho-20"
}
```

## Do not include

```json
{
  "wasPrice": "$63.23",
  "discountPercent": "51%",
  "hotDeal": true,
  "priceDrop": true,
  "lowestSeenPrice": "$29.99",
  "highestSeenPrice": "$79.99",
  "priceHistory": [],
  "lastKeepaPrice": "$31.00"
}
```

## Next test files to add

- `test-asins.json` for ASIN-only input
- `products-public.json` for Creator API / PA API public output
- `index.html` for a noindex card preview page
- test script for expiration behavior
