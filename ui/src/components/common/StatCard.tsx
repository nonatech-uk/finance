interface Props {
  label: string
  value: string | number
  subtitle?: string
  className?: string
}

export default function StatCard({ label, value, subtitle, className = '' }: Props) {
  return (
    <div className={`bg-bg-card border border-border rounded-lg p-5 ${className}`}>
      <div className="text-text-secondary text-sm mb-1">{label}</div>
      <div className="text-2xl font-semibold tabular-nums">{value}</div>
      {subtitle && <div className="text-text-secondary text-xs mt-1">{subtitle}</div>}
    </div>
  )
}
