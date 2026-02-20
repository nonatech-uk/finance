interface Props {
  children: React.ReactNode
  variant?: 'default' | 'income' | 'expense' | 'warning' | 'accent'
  className?: string
}

const VARIANT_CLASSES = {
  default: 'bg-bg-hover text-text-secondary',
  income: 'bg-income/15 text-income',
  expense: 'bg-expense/15 text-expense',
  warning: 'bg-warning/15 text-warning',
  accent: 'bg-accent/15 text-accent',
}

export default function Badge({ children, variant = 'default', className = '' }: Props) {
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${VARIANT_CLASSES[variant]} ${className}`}>
      {children}
    </span>
  )
}
