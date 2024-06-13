import { AppBar, Toolbar } from '@mui/material'
import NamespaceDropdown from './NamespaceDropdown'

interface Props {
  namespace: string
  namespaces: string[]
}
const NavigationBar = ({ namespace, namespaces }: Props) => {
  return (
    <>
      <Toolbar />
      <AppBar sx={{ backgroundColor: 'white' }}>
        <Toolbar
          sx={{
            ml: '100px',
            display: 'flex',
            justifyContent: 'flex-end',
          }}
        >
          <NamespaceDropdown namespace={namespace} namespaces={namespaces} />
        </Toolbar>
      </AppBar>
    </>
  )
}

export default NavigationBar
