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
  category_is_override: boolean
  note: string | null
  tags: string[]
}

export interface TagItem {
  tag: string
  source: string
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
  note: string | null
  note_source: string | null
  tags: TagItem[]
  dedup_group: DedupGroupInfo | null
  economic_event: EconomicEventInfo | null
}

// ── Bulk Operations ──

export interface BulkOperationResult {
  ok: boolean
  affected: number
}

export interface BulkMerchantNameResult extends BulkOperationResult {
  merchant_ids: string[]
}

export interface BulkTagReplaceResult extends BulkOperationResult {
  removed: number
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
  name: string | null
  display_name: string | null
  account_type: string | null
  is_active: boolean | null
  is_archived: boolean
  exclude_from_reports: boolean
  scope: string
}

export interface AccountList {
  items: AccountItem[]
}

export interface AccountUpdate {
  display_name?: string | null
  is_archived?: boolean
  exclude_from_reports?: boolean
  scope?: string
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
    account_name: string | null
    display_name: string | null
    account_type: string | null
    is_active: boolean | null
    is_archived: boolean
    exclude_from_reports: boolean
    scope: string
  }
  recent_transactions: TransactionItem[]
}

// ── Merchants ──

export interface MerchantItem {
  id: string
  name: string
  display_name: string | null
  category_hint: string | null
  category_method: string | null
  category_confidence: string | null
  mapping_count: number
}

export interface MerchantList {
  items: MerchantItem[]
  next_cursor: string | null
  has_more: boolean
}

export interface MerchantTransaction {
  id: string
  posted_at: string
  amount: string
  currency: string
  raw_merchant: string | null
  source: string
  institution: string
  account_ref: string
}

export interface MerchantDetail {
  id: string
  name: string
  display_name: string | null
  category_hint: string | null
  category_method: string | null
  category_confidence: string | null
  mapping_count: number
  aliases: string[]
  recent_transactions: MerchantTransaction[]
}

export interface DisplayRule {
  id: number
  pattern: string
  display_name: string
  merge_group: boolean
  category_hint: string | null
  priority: number
}

export interface DisplayRuleList {
  items: DisplayRule[]
}

export interface CategorySuggestionItem {
  id: number
  canonical_merchant_id: string
  merchant_name: string
  suggested_category_id: string
  suggested_category_path: string
  method: string
  confidence: string
  reasoning: string | null
  status: string
  created_at: string
}

export interface CategorySuggestionList {
  items: CategorySuggestionItem[]
  total: number
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

export interface AccountOption {
  institution: string
  account_ref: string
  label: string
}

export interface OverviewStats {
  total_accounts: number
  active_accounts: number
  total_raw_transactions: number
  active_transactions: number
  dedup_groups: number
  removed_by_dedup: number
  category_coverage_pct: string
  accounts: AccountOption[]
  date_range_from: string | null
  date_range_to: string | null
}
