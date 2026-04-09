import { useState } from 'react'
import Sidebar from './Sidebar'
import AppSwitcher from './AppSwitcher'

export default function Shell({ children }: { children: React.ReactNode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false)

  return (
    <div className="flex w-full min-h-screen">
      {/* Mobile header */}
      <div className="fixed top-0 left-0 right-0 h-12 bg-bg-secondary border-b border-border flex items-center px-4 z-30 md:hidden">
        <button
          onClick={() => setSidebarOpen(true)}
          className="p-1 -ml-1 text-text-secondary hover:text-text-primary"
        >
          <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
            <rect x="2" y="4" width="16" height="2" rx="1" />
            <rect x="2" y="9" width="16" height="2" rx="1" />
            <rect x="2" y="14" width="16" height="2" rx="1" />
          </svg>
        </button>
        <span className="ml-3 text-lg font-bold text-accent">Finance</span>
        <div className="ml-auto">
          <AppSwitcher currentApp="Finance" />
        </div>
      </div>

      {/* Desktop sidebar */}
      <Sidebar className="hidden md:flex" />

      {/* Mobile drawer */}
      {sidebarOpen && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-40 md:hidden"
            onClick={() => setSidebarOpen(false)}
          />
          <Sidebar
            className="fixed inset-y-0 left-0 z-50 md:hidden"
            onNavigate={() => setSidebarOpen(false)}
          />
        </>
      )}

      <main className="flex-1 overflow-auto p-4 pt-16 md:p-6">
        {children}
      </main>
    </div>
  )
}
