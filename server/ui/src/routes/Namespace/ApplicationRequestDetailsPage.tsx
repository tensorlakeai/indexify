import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import {
  Box,
  Breadcrumbs,
  Card,
  CardContent,
  Chip,
  Grid,
  Stack,
  Typography,
} from '@mui/material'
import { TableDocument } from 'iconsax-react'
import { Link, useLoaderData } from 'react-router-dom'
import CopyText from '../../components/CopyText'
import CopyTextPopover from '../../components/CopyTextPopover'
import FunctionRunsTable from '../../components/tables/FunctionRunsTable'
import { Request, RequestOutcome } from '../../types/types'
import { formatTimestamp } from '../../utils/helpers'

const ApplicationRequestDetailsPage = () => {
  const { namespace, application, requestId, applicationRequest } =
    useLoaderData() as {
      indexifyServiceURL: string
      requestId: string
      application: string
      namespace: string
      applicationRequest: Request
    }

  const renderOutcome = (outcome: RequestOutcome | null | undefined) => {
    if (!outcome) {
      return <Chip label="Unknown" size="small" color="default" />
    }

    if (outcome === 'success') {
      return <Chip label="Success" size="small" color="success" />
    }

    if (outcome === 'undefined') {
      return <Chip label="Pending" size="small" color="default" />
    }

    if (typeof outcome === 'object' && outcome.failure) {
      return (
        <Chip
          label={`Failed: ${outcome.failure}`}
          size="small"
          color="error"
          title={`Failure reason: ${outcome.failure}`}
        />
      )
    }

    return <Chip label="Unknown" size="small" color="default" />
  }

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <CopyTextPopover text={namespace}>
          <Typography color="text.primary">{namespace}</Typography>
        </CopyTextPopover>
        <Link to={`/${namespace}/applications`}>
          <CopyTextPopover text="Applications">
            <Typography color="text.primary">Applications</Typography>
          </CopyTextPopover>
        </Link>
        <CopyTextPopover text={application}>
          <Typography color="text.primary">{application}</Typography>
        </CopyTextPopover>
        <Link color="inherit" to={`/${namespace}/applications/${application}`}>
          <CopyTextPopover text={application}>
            <Typography color="text.primary">Requests</Typography>
          </CopyTextPopover>
        </Link>
        <CopyTextPopover text={requestId}>
          <Typography color="text.primary">{requestId}</Typography>
        </CopyTextPopover>
      </Breadcrumbs>
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument
                size="25"
                className="heading-icons"
                variant="Outline"
              />
            </div>
            <Typography variant="h4" display={'flex'} flexDirection={'row'}>
              Application Requests - {requestId} <CopyText text={requestId} />
            </Typography>
          </div>
        </Box>

        {/* Request Details */}
        <Card
          sx={{
            mb: 3,
            boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
          }}
        >
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Details
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Request ID
                </Typography>
                <Box display="flex" alignItems="center" gap={1}>
                  <Typography variant="body1">
                    {applicationRequest.id}
                  </Typography>
                  <CopyText text={applicationRequest.id} />
                </Box>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Application Version
                </Typography>
                <Typography variant="body1">
                  {applicationRequest.application_version}
                </Typography>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Created At
                </Typography>
                <Typography variant="body1">
                  {formatTimestamp(applicationRequest.created_at)}
                </Typography>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Outcome
                </Typography>
                {renderOutcome(applicationRequest.outcome)}
              </Grid>

              {applicationRequest.request_error && (
                <Grid item xs={12}>
                  <Typography
                    variant="subtitle2"
                    color="text.secondary"
                    gutterBottom
                  >
                    Request Error
                  </Typography>
                  <Card
                    variant="outlined"
                    sx={{
                      backgroundColor: 'error.main',
                      color: 'error.contrastText',
                    }}
                  >
                    <CardContent>
                      <Typography variant="subtitle2" gutterBottom>
                        Function:{' '}
                        {applicationRequest.request_error.function_name}
                      </Typography>
                      <Typography variant="body2">
                        {applicationRequest.request_error.message}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              )}
            </Grid>
          </CardContent>
        </Card>

        <FunctionRunsTable
          namespace={namespace}
          applicationName={application}
          functionRuns={applicationRequest.function_runs}
        />
      </Box>
    </Stack>
  )
}

export default ApplicationRequestDetailsPage
