import { Alert, Box } from '@mui/material'
import { useLoaderData } from 'react-router-dom'
import { SandboxesCard } from '../../components/cards/SandboxesCard'
import { SandboxesListLoaderData } from './types'

const SandboxesListPage = () => {
  const { sandboxes, namespace } = useLoaderData() as SandboxesListLoaderData

  if (!sandboxes || !namespace) {
    return (
      <Box>
        <Alert severity="error">
          Failed to load sandboxes data. Please try again.
        </Alert>
      </Box>
    )
  }

  return (
    <Box>
      <SandboxesCard sandboxes={sandboxes} namespace={namespace} />
    </Box>
  )
}

export default SandboxesListPage
