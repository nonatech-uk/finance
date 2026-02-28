import { useState, useEffect, useMemo, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { useCgt, useTaxYears, useUpdateTaxYear } from '../hooks/useStocks'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CgtSummary } from '../api/types'

interface HoldingMeta {
  holding_id: string
  symbol: string
  max_shares: string
  current_price: string | null
}

function currentTaxYear(): string {
  const now = new Date()
  const month = now.getMonth() + 1
  const day = now.getDate()
  const year = (month > 4 || (month === 4 && day >= 6)) ? now.getFullYear() : now.getFullYear() - 1
  return `${year}/${String(year + 1).slice(-2)}`
}

export default function TaxSummary() {
  const [selectedYear, setSelectedYear] = useState(currentTaxYear())
  const { data: taxYearsData } = useTaxYears()
  const updateTaxYear = useUpdateTaxYear()

  // Quantity overrides: holding_id -> qty string
  const [qtyOverrides, setQtyOverrides] = useState<Record<string, string>>({})
  // Debounced overrides sent to API
  const [debouncedOverrides, setDebouncedOverrides] = useState<Record<string, string>>({})

  const { data: cgtData, isLoading: cgtLoading } = useCgt(selectedYear, debouncedOverrides)

  // Debounce quantity changes (300ms)
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedOverrides(qtyOverrides), 300)
    return () => clearTimeout(timer)
  }, [qtyOverrides])

  // Income form state
  const [incomeForm, setIncomeForm] = useState({ gross_income: '', personal_allowance: '12570' })
  const [incomeSaved, setIncomeSaved] = useState(false)

  // Load existing income when tax years data changes
  useEffect(() => {
    if (taxYearsData?.items) {
      const entry = taxYearsData.items.find(i => i.tax_year === selectedYear)
      if (entry) {
        setIncomeForm({
          gross_income: entry.gross_income,
          personal_allowance: entry.personal_allowance,
        })
      } else {
        setIncomeForm({ gross_income: '', personal_allowance: '12570' })
      }
    }
  }, [taxYearsData, selectedYear])

  // Init qty overrides from holdings metadata (default to max)
  const holdings: HoldingMeta[] = useMemo(() => {
    if (!cgtData || !('holdings' in cgtData)) return []
    return (cgtData as any).holdings ?? []
  }, [cgtData])

  // Initialise overrides to max_shares on first load
  useEffect(() => {
    if (holdings.length > 0 && Object.keys(qtyOverrides).length === 0) {
      const initial: Record<string, string> = {}
      for (const h of holdings) {
        initial[h.holding_id] = h.max_shares
      }
      setQtyOverrides(initial)
    }
  }, [holdings]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleQtyChange = useCallback((holdingId: string, value: string) => {
    setQtyOverrides(prev => ({ ...prev, [holdingId]: value }))
  }, [])

  const handleSaveIncome = (e: React.FormEvent) => {
    e.preventDefault()
    updateTaxYear.mutate(
      { taxYear: selectedYear, ...incomeForm },
      {
        onSuccess: () => {
          setIncomeSaved(true)
          setTimeout(() => setIncomeSaved(false), 2000)
        },
      },
    )
  }

  // Build list of available tax years
  const availableYears = useMemo(() => {
    const years = new Set<string>()
    years.add(currentTaxYear())
    if (taxYearsData?.items) {
      taxYearsData.items.forEach(i => years.add(i.tax_year))
    }
    return [...years].sort().reverse()
  }, [taxYearsData])

  // Type-narrow the CGT response
  const cgt: CgtSummary | null = useMemo(() => {
    if (!cgtData) return null
    if ('tax_year' in cgtData) return cgtData as CgtSummary
    return null
  }, [cgtData])

  return (
    <div className="space-y-6">
      <div>
        <Link to="/stocks" className="text-sm text-text-secondary hover:text-text-primary">&larr; Back to Portfolio</Link>
      </div>

      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Capital Gains Tax Summary</h2>
        <select
          value={selectedYear}
          onChange={e => setSelectedYear(e.target.value)}
          className="px-3 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
        >
          {availableYears.map(y => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
      </div>

      {/* Income Input */}
      <form onSubmit={handleSaveIncome} className="bg-bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-medium text-text-secondary mb-3">Your Income ({selectedYear})</h3>
        <div className="flex gap-4 items-end">
          <div>
            <label className="block text-xs text-text-secondary mb-1">Gross Income</label>
            <div className="flex items-center">
              <span className="text-text-secondary mr-1">&pound;</span>
              <input
                type="number"
                step="1"
                value={incomeForm.gross_income}
                onChange={e => setIncomeForm({ ...incomeForm, gross_income: e.target.value })}
                placeholder="50000"
                className="w-32 px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">Personal Allowance</label>
            <div className="flex items-center">
              <span className="text-text-secondary mr-1">&pound;</span>
              <input
                type="number"
                step="1"
                value={incomeForm.personal_allowance}
                onChange={e => setIncomeForm({ ...incomeForm, personal_allowance: e.target.value })}
                className="w-28 px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>
          <button
            type="submit"
            disabled={updateTaxYear.isPending || !incomeForm.gross_income}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
          >
            {updateTaxYear.isPending ? 'Saving...' : incomeSaved ? 'Saved!' : 'Save'}
          </button>
        </div>
        {incomeForm.gross_income && (
          <div className="mt-2 text-xs text-text-secondary">
            Taxable income: &pound;{Math.max(0, parseFloat(incomeForm.gross_income) - parseFloat(incomeForm.personal_allowance || '12570')).toLocaleString()}
            &ensp;&middot;&ensp;
            Basic rate band remaining: &pound;{Math.max(0, 37700 - Math.max(0, parseFloat(incomeForm.gross_income) - parseFloat(incomeForm.personal_allowance || '12570'))).toLocaleString()}
          </div>
        )}
      </form>

      {/* Hypothetical Disposal Planner */}
      {holdings.length > 0 && (
        <div className="bg-bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-medium text-text-secondary mb-3">Shares to Sell (Planning)</h3>
          <div className="space-y-3">
            {holdings.map(h => {
              const max = parseFloat(h.max_shares)
              const current = parseFloat(qtyOverrides[h.holding_id] ?? h.max_shares)
              const price = h.current_price ? parseFloat(h.current_price) : null
              return (
                <div key={h.holding_id} className="flex items-center gap-4">
                  <span className="font-medium w-16">{h.symbol}</span>
                  <input
                    type="range"
                    min="0"
                    max={max}
                    step={max > 100 ? 1 : 0.1}
                    value={current}
                    onChange={e => handleQtyChange(h.holding_id, e.target.value)}
                    className="flex-1 accent-accent"
                  />
                  <input
                    type="number"
                    min="0"
                    max={max}
                    step="1"
                    value={qtyOverrides[h.holding_id] ?? h.max_shares}
                    onChange={e => handleQtyChange(h.holding_id, e.target.value)}
                    className="w-20 px-2 py-1 text-sm text-right rounded border border-border bg-bg-primary text-text-primary tabular-nums"
                  />
                  <span className="text-xs text-text-secondary w-20 text-right">/ {max}</span>
                  {price !== null && (
                    <span className="text-xs text-text-secondary w-28 text-right">
                      ${(current * price).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* CGT Calculation */}
      {cgtLoading ? (
        <LoadingSpinner />
      ) : cgt ? (
        <div className="bg-bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-medium text-text-secondary mb-4">CGT Calculation</h3>
          <div className="space-y-2 text-sm">
            <Row label="Total Gains" value={`£${fmt(cgt.total_gains)}`} className="text-income" />
            <Row label="Total Losses" value={`-£${fmt(cgt.total_losses)}`} className="text-expense" />
            <div className="border-t border-border pt-2">
              <Row label="Net Gains" value={`£${fmt(cgt.net_gains)}`} bold />
            </div>
            <Row label="Annual Exempt Amount" value={`-£${fmt(cgt.exempt_amount)}`} className="text-text-secondary" />
            <div className="border-t border-border pt-2">
              <Row label="Taxable Gains" value={`£${fmt(cgt.taxable_gains)}`} bold />
            </div>
            <div className="border-t border-border pt-2 space-y-1">
              <Row label="Basic Rate (18%)" value={`£${fmt(cgt.basic_rate_tax)}`} subtitle={`on £${fmt(cgt.basic_rate_amount)}`} />
              <Row label="Higher Rate (24%)" value={`£${fmt(cgt.higher_rate_tax)}`} subtitle={`on £${fmt(cgt.higher_rate_amount)}`} />
            </div>
            <div className="border-t border-border pt-2">
              <Row label="Total CGT Liability" value={`£${fmt(cgt.total_tax)}`} bold className="text-lg" />
            </div>
            {!cgt.gross_income && (
              <p className="text-xs text-text-secondary mt-2">
                Enter your income above to calculate the correct tax band split.
              </p>
            )}
          </div>
        </div>
      ) : (
        <div className="bg-bg-card border border-border rounded-lg p-8 text-center text-text-secondary">
          No disposals in {selectedYear}.
        </div>
      )}

      {/* Disposals Table */}
      {cgt && cgt.disposals.length > 0 && (
        <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
          <div className="px-4 py-3 border-b border-border">
            <h3 className="text-sm font-medium text-text-secondary">Disposals</h3>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Symbol</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Date</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Qty</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Proceeds</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Cost</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Gain/Loss</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Match</th>
              </tr>
            </thead>
            <tbody>
              {cgt.disposals.map((d, i) => {
                const gl = parseFloat(d.gain_loss)
                return (
                  <tr key={i} className="border-b border-border last:border-0 hover:bg-bg-hover">
                    <td className="px-4 py-2 font-medium">{d.symbol}</td>
                    <td className="px-4 py-2 tabular-nums">{d.trade_date}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{parseFloat(d.quantity).toLocaleString()}</td>
                    <td className="px-4 py-2 text-right tabular-nums">${fmt(d.proceeds)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">${fmt(d.cost_basis)}</td>
                    <td className={`px-4 py-2 text-right tabular-nums font-medium ${gl >= 0 ? 'text-income' : 'text-expense'}`}>
                      {gl >= 0 ? '+' : ''}${fmt(d.gain_loss)}
                    </td>
                    <td className="px-4 py-2 text-text-secondary text-xs">
                      {d.match_type === 'same_day' ? 'Same-day' : d.match_type === 'hypothetical' ? 'If sold today' : 'S.104 Pool'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="bg-bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-medium text-text-secondary mb-2">Dividend Tax</h3>
        <p className="text-text-secondary text-sm">Coming in a future update.</p>
      </div>
    </div>
  )
}

function fmt(v: string): string {
  return parseFloat(v).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function Row({ label, value, subtitle, bold, className = '' }: {
  label: string
  value: string
  subtitle?: string
  bold?: boolean
  className?: string
}) {
  return (
    <div className={`flex justify-between items-baseline ${className}`}>
      <span className={`text-text-secondary ${bold ? 'font-medium text-text-primary' : ''}`}>{label}</span>
      <div className="text-right">
        <span className={`tabular-nums ${bold ? 'font-semibold' : ''}`}>{value}</span>
        {subtitle && <span className="text-xs text-text-secondary ml-1.5">{subtitle}</span>}
      </div>
    </div>
  )
}
