# Lessons Learned

## 2026-03-10
- When planning client-side behavior around backend defaults, verify the exact public response schema first.
- Do not assume internal backend fields are exposed via API contracts.
- If product direction changes during planning, record the updated fallback behavior explicitly before implementation.
- After auth flow changes, confirm local config initialization reflects server-created defaults (e.g., default organization) instead of relying only on runtime fallback.
