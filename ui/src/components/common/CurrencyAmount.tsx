interface Props {
  amount: string | number
  currency?: string
  className?: string
  showSign?: boolean
}

const CURRENCY_SYMBOLS: Record<string, string> = {
  GBP: '£', USD: '$', EUR: '€', CHF: 'CHF ', PLN: 'zł', NOK: 'kr',
}

export default function CurrencyAmount({ amount, currency = 'GBP', className = '', showSign = true }: Props) {
  const num = typeof amount === 'string' ? parseFloat(amount) : amount
  const isNegative = num < 0
  const abs = Math.abs(num)
  const symbol = CURRENCY_SYMBOLS[currency] || currency + ' '
  const formatted = abs.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  const sign = showSign && !isNegative && num > 0 ? '+' : ''

  return (
    <span className={`tabular-nums font-medium ${isNegative ? 'text-expense' : 'text-income'} ${className}`}>
      {sign}{isNegative ? '-' : ''}{symbol}{formatted}
    </span>
  )
}
