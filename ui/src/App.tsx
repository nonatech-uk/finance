import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Shell from './components/layout/Shell'
import Dashboard from './pages/Dashboard'
import Transactions from './pages/Transactions'
import Accounts from './pages/Accounts'
import AccountDetail from './pages/AccountDetail'
import Categories from './pages/Categories'
import Merchants from './pages/Merchants'

export default function App() {
  return (
    <BrowserRouter>
      <Shell>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/transactions" element={<Transactions />} />
          <Route path="/accounts" element={<Accounts />} />
          <Route path="/accounts/:institution/:accountRef" element={<AccountDetail />} />
          <Route path="/categories" element={<Categories />} />
          <Route path="/merchants" element={<Merchants />} />
          <Route path="*" element={<div className="text-text-secondary">Page not found</div>} />
        </Routes>
      </Shell>
    </BrowserRouter>
  )
}
