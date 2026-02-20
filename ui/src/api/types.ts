// ── Transactions ──

export interface TransactionItem {
  id: string
  source: string
  institution: string
  account_ref: string
  posted_at: string
  amount: string
  currency: string
  raw_merchant: string | null
  raw_memo: string | null
  cleaned_merchant: string | null
  canonical_merchant_id: string | null
  canonical_merchant_name: string | null
  merchant_match_type: string | null
  category_path: string | null
  category_name: string | null
  category_type: string | null
}

export interface TransactionList {
  items: TransactionItem[]
  next_cursor: string | null
  has_more: boolean
}

export interface DedupMember {
  raw_transaction_id: string
  source: string
  is_preferred: boolean
}

export interface DedupGroupInfo {
  group_id: string
  match_rule: string
  confidence: string
  members: DedupMember[]
}

export interface EconomicEventLeg {
  raw_transaction_id: string
  leg_type: string
  amount: string
  currency: string
}

export interface EconomicEventInfo {
  event_id: string
  event_type: string
  initiated_at: string | null
  description: string | null
  legs: EconomicEventLeg[]
}

export interface TransactionDetail extends TransactionItem {
  raw_data: Record<string, unknown> | null
  dedup_group: DedupGroupInfo | null
  economic_event: EconomicEventInfo | null
}

// ── Accounts ──

export interface AccountItem {
  institution: string
  account_ref: string
  currency: string
  transaction_count: number
  earliest_date: string | null
  latest_date: string | null
  balance: string | null
  account_id: string | null
  account_name: string | null
  account_type: string | null
  is_active: boolean | null
}

export interface AccountList {
  items: AccountItem[]
}

export interface AccountDetailResponse {
  institution: string
  account_ref: string
  summary: {
    transaction_count: number
    earliest_date: string
    latest_date: string
    balance: number
    currency: string
  }
  recent_transactions: TransactionItem[]
}

// ── Merchants ──

export interface MerchantItem {
  id: string
  name: string
  category_hint: string | null
  mapping_count: number
}

export interface MerchantList {
  items: MerchantItem[]
  next_cursor: string | null
  has_more: boolean
}

// ── Categories ──

export interface CategoryItem {
  id: string
  name: string
  full_path: string
  category_type: string | null
  is_active: boolean
  parent_id: string | null
  children: CategoryItem[]
}

export interface CategoryTree {
  items: CategoryItem[]
}

export interface SpendingByCategory {
  category_path: string
  category_name: string
  category_type: string | null
  total: string
  transaction_count: number
}

export interface SpendingReport {
  items: SpendingByCategory[]
  date_from: string
  date_to: string
  total_income: string
  total_expense: string
}

// ── Stats ──

export interface MonthlyTotal {
  month: string
  income: string
  expense: string
  net: string
  transaction_count: number
}

export interface MonthlyReport {
  items: MonthlyTotal[]
  currency: string
}

export interface OverviewStats {
  total_accounts: number
  active_accounts: number
  total_raw_transactions: number
  active_transactions: number
  dedup_groups: number
  removed_by_dedup: number
  category_coverage_pct: string
  institutions: string[]
  date_range_from: string | null
  date_range_to: string | null
}
