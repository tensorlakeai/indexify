import DeleteIcon from '@mui/icons-material/Delete'
import {
  Alert,
  Card,
  CardContent,
  Grid,
  IconButton,
  Typography,
} from '@mui/material'
import { Box, Stack } from '@mui/system'
import { IndexifyClient } from 'getindexify'
import { Cpu, InfoCircle } from 'iconsax-react'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Application, ApplicationsList } from '../../types/types'
import { deleteApplication } from '../../utils/delete'
import CopyText from '../CopyText'
import TruncatedText from '../TruncatedText'

interface ApplicationsProps {
  client: IndexifyClient
  applications: ApplicationsList
  namespace: string
}

export function ApplicationsCard({
  client,
  applications,
  namespace,
}: ApplicationsProps) {
  const [localApplications, setLocalApplications] = useState<Application[]>(
    applications.applications || []
  )
  const [error, setError] = useState<string | null>(null)

  async function handleDeleteApplication(applicationName: string) {
    try {
      const result = await deleteApplication({
        namespace,
        application: applicationName,
      })
      if (result) {
        setLocalApplications((prevGraphs) =>
          prevGraphs.filter((graph) => graph.name !== applicationName)
        )
      }
    } catch (err) {
      console.error('Error deleting compute graph:', err)
      setError('Failed to delete compute graph. Please try again.')
    }
  }

  function renderGraphCard(application: Application) {
    return (
      <Grid item xs={12} sm={6} md={4} key={application.name} mb={2}>
        <Card
          sx={{
            minWidth: 275,
            height: '100%',
            boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
          }}
        >
          <CardContent>
            <Box display="flex" justifyContent="space-between">
              <Link to={`/${namespace}/applications/${application.name}`}>
                <TruncatedText text={application.name} maxLength={20} />
              </Link>
              <Box display="flex" flexDirection="row" alignItems="center">
                <CopyText text={application.name} />
                <IconButton
                  onClick={() => handleDeleteApplication(application.name)}
                  aria-label="delete application"
                >
                  <DeleteIcon color="error" />
                </IconButton>
              </Box>
            </Box>

            <Typography
              variant="subtitle2"
              color="text.secondary"
              sx={{ mb: 1 }}
            >
              {application.description}
            </Typography>

            <Typography variant="subtitle2" color="text.secondary">
              <Typography sx={{ mb: 0 }}>Functions:</Typography>
              {Object.keys(application.functions).length > 0 ? (
                <>
                  <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                    {Object.values(application.functions).map((func, index) => (
                      <li key={func.name + '_' + index}>{func.name} </li>
                    ))}
                  </ul>
                </>
              ) : (
                <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                  <li>N/A</li>
                </ul>
              )}
            </Typography>

            <Typography variant="subtitle2" color="text.secondary">
              <Typography sx={{ mb: 0 }}>Tags:</Typography>
              {Object.keys(application.tags).length > 0 ? (
                <>
                  <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                    {Object.values(application.tags).map((tag, index) => (
                      <li key={tag + '_' + index}>{tag} </li>
                    ))}
                  </ul>
                </>
              ) : (
                <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                  <li>N/A</li>
                </ul>
              )}
            </Typography>

            <Typography variant="subtitle2" color="text.secondary">
              <Typography sx={{ mb: 0 }}>Entrypoint:</Typography>
              {Object.keys(application.entrypoint).length > 0 ? (
                <>
                  <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                    {Object.values(application.entrypoint).map(
                      (entry, index) => (
                        <li key={entry + '_' + index}>{entry} </li>
                      )
                    )}
                  </ul>
                </>
              ) : (
                <ul style={{ margin: '0 0 6px 0', paddingLeft: '20px' }}>
                  <li>N/A</li>
                </ul>
              )}
            </Typography>

            {application.created_at !== undefined &&
              application.created_at > 0 && (
                <Typography variant="subtitle2" color="text.secondary">
                  Created At:{' '}
                  {new Date(application.created_at).toLocaleString()}
                </Typography>
              )}
            <Typography variant="subtitle2" color="text.secondary">
              Version: {application.version || 'unknown'}
            </Typography>
            <Typography variant="subtitle2" color="text.secondary">
              Tombstoned: {application.tombstoned ? 'true' : 'false'}
            </Typography>
          </CardContent>
        </Card>
      </Grid>
    )
  }

  function renderContent() {
    if (error)
      return (
        <Alert variant="outlined" severity="error" sx={{ my: 2 }}>
          {error}
        </Alert>
      )

    if (!localApplications.length)
      return (
        <Alert variant="outlined" severity="info" sx={{ my: 2 }}>
          No Applications Found
        </Alert>
      )

    const sortedGraphs = [...localApplications].sort((a, b) =>
      a.name.localeCompare(b.name)
    )

    return (
      <Box sx={{ width: '100%', overflow: 'auto', borderRadius: '5px' }} mt={2}>
        <Grid container spacing={1}>
          {sortedGraphs.map(renderGraphCard)}
        </Grid>
      </Box>
    )
  }

  return (
    <>
      <Stack direction="row" alignItems="center" spacing={2}>
        <div className="heading-icon-container">
          <Cpu size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Applications
          <IconButton
            href="https://docs.tensorlake.ai/applications/quickstart"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  )
}
