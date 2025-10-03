import { Box, Alert } from '@mui/material'
import { useLoaderData } from 'react-router-dom'
import { ApplicationsCard } from '../../components/cards/ApplicationsCard'
import { ApplicationsListLoaderData } from './types'

const ApplicationsListPage = () => {
  const { client, applications, namespace } =
    useLoaderData() as ApplicationsListLoaderData

  if (!client || !applications || !namespace) {
    return (
      <Box>
        <Alert severity="error">
          Failed to load compute graphs data. Please try again.
        </Alert>
      </Box>
    )
  }

  return (
    <Box>
      <ApplicationsCard
        applications={applications}
        client={client}
        namespace={namespace}
      />
    </Box>
  )
}

export default ApplicationsListPage
