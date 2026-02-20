"""Pydantic response models for the Finance API."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


# ── Transactions ──────────────────────────────────────────────────────────────


class TransactionItem(BaseModel):
    """Transaction in list view."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    source: str
    institution: str
    account_ref: str
    posted_at: date
    amount: Decimal
    currency: str
    raw_merchant: str | None = None
    raw_memo: str | None = None
    cleaned_merchant: str | None = None
    canonical_merchant_id: UUID | None = None
    canonical_merchant_name: str | None = None
    merchant_match_type: str | None = None
    category_path: str | None = None
    category_name: str | None = None
    category_type: str | None = None


class TransactionList(BaseModel):
    items: list[TransactionItem]
    next_cursor: str | None = None
    has_more: bool = False


class DedupMember(BaseModel):
    raw_transaction_id: UUID
    source: str
    is_preferred: bool


class DedupGroupInfo(BaseModel):
    group_id: UUID
    match_rule: str
    confidence: Decimal
    members: list[DedupMember]


class EconomicEventLeg(BaseModel):
    raw_transaction_id: UUID
    leg_type: str
    amount: Decimal
    currency: str


class EconomicEventInfo(BaseModel):
    event_id: UUID
    event_type: str
    initiated_at: date | None = None
    description: str | None = None
    legs: list[EconomicEventLeg]


class TransactionDetail(TransactionItem):
    """Full transaction detail including dedup and economic event info."""

    raw_data: dict | None = None
    dedup_group: DedupGroupInfo | None = None
    economic_event: EconomicEventInfo | None = None


# ── Accounts ──────────────────────────────────────────────────────────────────


class AccountItem(BaseModel):
    id: UUID
    institution: str
    name: str
    currency: str
    account_type: str
    is_active: bool
    transaction_count: int = 0
    earliest_date: date | None = None
    latest_date: date | None = None
    balance: Decimal | None = None


class AccountList(BaseModel):
    items: list[AccountItem]


# ── Merchants ─────────────────────────────────────────────────────────────────


class MerchantItem(BaseModel):
    id: UUID
    name: str
    category_hint: str | None = None
    mapping_count: int = 0


class MerchantList(BaseModel):
    items: list[MerchantItem]
    next_cursor: str | None = None
    has_more: bool = False


class MerchantMappingUpdate(BaseModel):
    """Request body for updating a merchant's category hint."""

    category_hint: str | None = None


# ── Categories ────────────────────────────────────────────────────────────────


class CategoryItem(BaseModel):
    id: UUID
    name: str
    full_path: str
    category_type: str | None = None
    is_active: bool = True
    parent_id: UUID | None = None
    children: list["CategoryItem"] = []


class CategoryTree(BaseModel):
    items: list[CategoryItem]


class SpendingByCategory(BaseModel):
    category_path: str
    category_name: str
    category_type: str | None = None
    total: Decimal
    transaction_count: int


class SpendingReport(BaseModel):
    items: list[SpendingByCategory]
    date_from: date
    date_to: date
    total_income: Decimal
    total_expense: Decimal


# ── Stats ─────────────────────────────────────────────────────────────────────


class MonthlyTotal(BaseModel):
    month: str  # YYYY-MM
    income: Decimal
    expense: Decimal
    net: Decimal
    transaction_count: int


class MonthlyReport(BaseModel):
    items: list[MonthlyTotal]
    currency: str


class OverviewStats(BaseModel):
    total_accounts: int
    active_accounts: int
    total_raw_transactions: int
    active_transactions: int
    dedup_groups: int
    removed_by_dedup: int
    category_coverage_pct: Decimal
    institutions: list[str]
    date_range_from: date | None = None
    date_range_to: date | None = None
