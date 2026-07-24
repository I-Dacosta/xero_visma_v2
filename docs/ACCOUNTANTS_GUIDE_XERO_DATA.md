# Guide to the Xero Data — For the Accounting Team

_This explains what's in the data warehouse for our 7 Xero companies, in plain terms. It's meant to be read before you start reviewing the numbers — a few things need context, or they'll look like errors when they're actually expected._

_For a full list of every table and what each one holds, see the companion document `ACCOUNTANTS_TABLE_CATALOG.md`._

---

## 1. What's covered

Data for all **7 Xero-connected companies**:

| Company | Country | Currency |
|---|---|---|
| Aqua Pharma Australia Pty Ltd | Australia | AUD |
| Aquatiq Pty Ltd | Australia | AUD |
| Monagold Prawn Farm | Australia | AUD |
| Aquatiq New Zealand Limited | New Zealand | NZD |
| Aqua Pharma Limited | UK | GBP |
| Aquatiq Ltd | UK | GBP |
| Aqua Pharma Inc | Canada | CAD |

**This is Xero data only, for now.** The Visma-side companies are on a separate system and aren't part of this data yet — there's no single combined view across all group companies yet, and nothing here has been consolidated or had intercompany transactions eliminated. Each of the 7 companies stands alone.

The data is a genuine copy of what's in Xero — invoices, bills, payments, the general ledger, etc. — reorganized to be easier to query and compare, but not restated or adjusted in any way. If a number looks odd, it's either genuinely worth flagging, or one of the known items below explains it — that's exactly what this guide is for.

---

## 2. Two versions of the same data — which one to use

You'll see everything twice, in two forms:

- **Level 0** — the data organized close to how Xero itself structures it, using Xero's own field names. Think of this as the audit-trail copy: if you ever need to trace a number back to exactly what Xero shows, this is the version that matches.
- **Level 1** — the *same numbers*, renamed and lightly reshaped into more standard, consistent terminology. **This is the version you'll want to work with day to day.** Nothing is recalculated between the two — Level 1 is just Level 0 relabeled for readability.

Nothing described below changes between the two levels — the caveats apply equally to both.

---

## 3. What's in the data

**About the companies themselves:**
- Chart of accounts (all ~874 accounts across the 7 companies)
- Customers and suppliers (one combined contact list — see the caveat in section 6)
- Currencies, tax rates, departments/tracking categories, users

**Transactions:**
- Sales invoices and supplier bills (~29,100 lines)
- Credit notes (~660 lines)
- Payments made and received (~16,400)
- Other bank account activity not tied to an invoice/bill (~5,700 lines)
- Manual journal entries (~7,000 lines)
- Purchase orders and quotes
- Transfers between a company's own bank accounts
- Overpayments (money received/paid before being matched to an invoice)
- **The general ledger itself** — every posting Xero has made, in full (~227,000 lines, oldest dated 2017)
- Copies of Xero's own built-in reports (Trial Balance, P&L, Balance Sheet, etc.), saved as a reference point — more on this in section 8

---

## 4. A few conventions worth knowing before you look at amounts

- **Line-level amounts exclude tax by default.** Every invoice/bill/credit-note line shows the **net** amount (before tax). Tax is available separately alongside it. This is deliberate and consistent — don't add tax back in unless you're specifically looking at the tax column.
- **Two currency views, side by side.** Each transaction carries both the amount in the currency it was actually transacted in (e.g. USD for a foreign supplier), and the amount converted to the company's own reporting currency. Make sure you're reading the one you mean to.
- **The general ledger uses signed amounts** (debits and credits, one column, one sign convention) — a normal ledger view. **The invoice/bill/payment tables show plain positive amounts instead** — the sale or purchase value, not a debit/credit. If you're ever comparing the two, keep in mind they're presented differently by design.
- **Watch the status field on invoices, bills, and credit notes.** Voided and deleted documents are *kept* in the data (for audit purposes) with their original amount still showing — they are **not** automatically excluded from a raw total. Always filter to exclude `VOIDED` and `DELETED` before summing anything, or ask IT to do it for you.

---

## 5. The general ledger — what it is, and its one real limitation

The general ledger table is a genuine, line-by-line copy of Xero's own ledger — not something reconstructed from invoices/bills. It's been checked and every single posting balances (debits = credits) with no exceptions worth mentioning.

**We compared our ledger totals against Xero's own Trial Balance and Profit & Loss reports, account by account, to make sure they agree.** Results:

- **Income Statement (P&L) accounts: excellent agreement** — 98% of revenue/expense accounts tied out exactly against Xero's Trial Balance, and a separate check against Xero's own current-month P&L report also matched almost perfectly (94% exact, and the small remainder was immaterial amounts Xero's own report doesn't even display).
- **Balance Sheet accounts: this is the one thing to really know about.** Only around 15% of balance sheet accounts (equity, fixed assets, payables control accounts, etc.) tied out to Xero's Trial Balance. **This is expected, not a data error.** Here's why: balance sheet accounts carry a running balance from day one of the company, including whatever **opening/conversion balance** was entered when the company first moved onto Xero. That one-time opening entry isn't retrievable through the normal ledger feed we sync from — we checked this thoroughly, including re-confirming against a full, from-scratch re-download of the entire ledger history, and the conversion balances simply aren't there. It's a gap in what Xero exposes for this purpose, not something missing from our sync.

**Practical takeaway:** trust the ledger for **income statement** figures — they tie out very well. For **balance sheet** figures, treat our cumulative/running totals as incomplete until we can source the original conversion balances separately (e.g. from Xero's Balance Sheet report itself, or from whoever set up each company in Xero originally). Don't spend time chasing a balance sheet mismatch back to a "bug" — flag it if the *size* of the gap looks surprising for a specific account, but the existence of a gap on balance sheet accounts is expected.

---

## 6. Postings you'll see with no invoice or bill behind them (also expected)

A meaningful slice of the ledger (about 13% of all postings) has **no source document at all** — no invoice, bill, or any transaction reference. We looked into this specifically: these are **Xero's own automatic inventory costing entries** — whenever stock is bought or moved, Xero silently posts a cost-of-goods entry in the background. There's no separate "document" behind these; that's just how Xero's inventory feature works. Nothing to reconcile these against.

Similarly, if a company uses Xero's built-in **payroll** feature, payroll-related postings (wages, super/pension, tax withholding) come through the ledger from a completely separate part of Xero that we don't currently pull data from directly. These postings are genuine and correct — they just won't ever have a matching "bill" in our data.

---

## 7. Two companies with an open, unresolved question

Comparing transaction-level detail (invoices, bills) against the ledger works very well for most companies and most transaction types. Two specific companies stand out and are still being looked into:

- **Aqua Pharma Inc** (the newest company added) — one account shows a large amount of ledger activity ($7.7M) with **no matching invoices at all** in our data. Best guess so far: some historical data may have been entered directly as journal entries during setup, rather than through normal invoicing — not yet confirmed.
- **Aquatiq Ltd** — supplier bills and bill payments don't tie out as well here as for the other companies. The amounts on the bills consistently differ from what's in the ledger, by inconsistent amounts (so it's not a simple exchange-rate issue). Best guess: some bills may have had their account coding changed after the original ledger entry was posted (once posted, ledger entries don't get rewritten, even if the bill is later re-coded) — also not yet confirmed.

If you notice anything unusual for these two companies specifically, it may well be related to this — flag it anyway, but know that it's already on our radar.

*(One more minor, unrelated oddity: a handful of ledger entries for Aqua Pharma Inc are dated as far forward as September 2027 — likely placeholder/future fiscal-year-end entries, not investigated in depth yet.)*

---

## 8. Things that are **not** built yet — so don't expect to find them

- **No custom account grouping yet.** Right now, accounts are classified only using Xero's own built-in categories (Asset/Liability/Equity/Revenue/Expense, plus Xero's own more detailed groupings). If you have a specific chart-of-accounts rollup or reporting-line structure you use today, it hasn't been mapped in yet — **we'll need your input to define that mapping** when we get to it, since it's a judgment call only the accounting team can really make.
- **No separate "customers" list vs "suppliers" list yet.** All contacts (customers and suppliers) are currently in one combined list, with a flag indicating which side(s) they're used on. A clean split is planned but not done.
- **No group/consolidated view.** Each of the 7 companies' numbers are independent; nothing has been combined, and no intercompany eliminations have been applied.
- **Xero's own built-in reports are saved alongside the data** (Trial Balance, P&L, Balance Sheet, etc., as of whenever they were last pulled) — useful as a cross-check/reference point, but they're a copy of Xero's report output, not something we've built ourselves.

---

## 9. What would help most from you

- **Sanity-check the transaction-level detail** (invoices, bills, payments, credit notes) for a company or period you know well — this is the part of the data we're most confident in, and your eye on it is the best test.
- **Flag anything odd that isn't explained above** — that's exactly the point of this review.
- **When we're ready to build the account-grouping/reporting-line mapping** (section 8), we'll need you to define how our chart of accounts should roll up — that's not something IT can decide alone.

---

_Questions about anything above, or need a specific number pulled/explained — just ask._
