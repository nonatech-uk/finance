import { Shell as SharedShell } from '@mees/shared-ui'
import Sidebar from './Sidebar'

export default function Shell({ children }: { children: React.ReactNode }) {
  return (
    <SharedShell appName="Finance" sidebar={Sidebar}>
      {children}
    </SharedShell>
  )
}
