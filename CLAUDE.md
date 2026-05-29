# Behaviral guidelines for this project
# See: D:\hb\CLAUDE.md for global rules

## Project: hb-trading

Cryptocurrency semi-automated trading system. All code must be production-quality:
type hints required, edge cases handled, errors logged not swallowed.

### Architecture rules
- Each module in src/ is independently testable
- Data flows one direction: collector → storage → analysis → signals → dashboard
- Never trade with real money unless the user explicitly confirms a live trade
- All API calls must have timeout + retry

### Price Action module (src/price_action.py)
Reference the price-action skill for Al Brooks methodology.
Key principles: 80% rule, H2/L2 as gold standard, wedge = exhaustion.
