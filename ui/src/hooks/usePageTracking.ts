import { useEffect } from 'react'
import { useLocation } from 'react-router-dom'

export function usePageTracking() {
  const { pathname } = useLocation()
  useEffect(() => {
    fetch('/api/v1/usage/pageview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: pathname }),
    }).catch(() => {})
  }, [pathname])
}
