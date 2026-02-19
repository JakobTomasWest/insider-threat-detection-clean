# features_v2 (daily_user)

- Input: `out/<REL>/features_v1/daily_user/daily_user.parquet`
- Output: `out/<REL>/features_v2/daily_user/daily_user.parquet`
- Keys: `(user_key, day)` — identical to v1

## Windows
- Detector window when day = D: `[D-13, D]`
- Trend (per-row): `[d-6, d]` (includes today)
- Baseline (per-row): `[d-37, d-8]` (excludes last 7)

## Columns added
- `ah_rate_1d`, `ah_rate_trend`, `ah_rate_baseline`
- `usb_count_1d`, `usb_count_trend`, `usb_count_baseline`
- `ah_novel`, `usb_novel`

## Notes
- WL/Dropbox evidence comes from v1: `http_n_wikileaks`, `http_n_dropbox`
- Dedupe per user/day, use `asfreq("D")` for rolling math, then filter back to v1 keys
