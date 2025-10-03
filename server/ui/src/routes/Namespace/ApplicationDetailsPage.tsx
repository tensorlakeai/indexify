import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import {
  Box,
  Breadcrumbs,
  Button,
  Chip,
  CircularProgress,
  Stack,
  Typography,
} from '@mui/material'
import axios from 'axios'
import { TableDocument } from 'iconsax-react'
import { useCallback, useState } from 'react'
import { Link, useLoaderData } from 'react-router-dom'
import CopyText from '../../components/CopyText'
import CopyTextPopover from '../../components/CopyTextPopover'
import ApplicationFunctionsTable from '../../components/tables/ApplicationFunctionsTable'
import { GraphRequestsTable } from '../../components/tables/ShallowGraphRequestsTable'
import type { ShallowGraphRequest } from '../../types/types'
import { getIndexifyServiceURL } from '../../utils/helpers'
import { ApplicationDetailsLoaderData } from './types'

const ApplicationDetailsPage = () => {
  const {
    namespace,
    application,
    graphRequests: graphRequestsPayload,
  } = useLoaderData() as ApplicationDetailsLoaderData

  const [shallowGraphRequests, setShallowGraphRequests] = useState<
    ShallowGraphRequest[]
  >(graphRequestsPayload.requests)
  const [isLoading, setIsLoading] = useState(false)
  const [prevCursor, setPrevCursor] = useState<string | null>(
    graphRequestsPayload.prev_cursor ? graphRequestsPayload.prev_cursor : null
  )
  const [nextCursor, setNextCursor] = useState<string | null>(
    graphRequestsPayload.next_cursor ? graphRequestsPayload.next_cursor : null
  )

  // const handleDelete = useCallback((updatedList: ShallowGraphRequest[]) => {
  //   const sortedList = [...updatedList].sort(
  //     (a, b) => (b.created_at ?? 0) - (a.created_at ?? 0)
  //   )
  //   setShallowGraphRequests(sortedList)
  // }, [])

  const fetchInvocations = useCallback(
    async (cursor: string | null, direction: 'forward' | 'backward') => {
      setIsLoading(true)
      try {
        const serviceURL = getIndexifyServiceURL()
        const limit = 20
        const url = `${serviceURL}/v1/namespaces/${namespace}/applications/${
          application.name
        }/requests?limit=${limit}${
          cursor ? `&cursor=${cursor}` : ''
        }&direction=${direction}`

        const response = await axios.get(url)
        const data = response.data

        setShallowGraphRequests([...data.invocations])

        setPrevCursor(data.prev_cursor)
        setNextCursor(data.next_cursor)
      } catch (error) {
        console.error('Error fetching invocations:', error)
      } finally {
        setIsLoading(false)
      }
    },
    [namespace, application.name]
  )

  const handleNextPage = useCallback(() => {
    if (nextCursor) {
      fetchInvocations(nextCursor, 'forward')
    }
  }, [nextCursor, fetchInvocations])

  const handlePreviousPage = useCallback(() => {
    if (prevCursor) {
      fetchInvocations(prevCursor, 'backward')
    }
  }, [prevCursor, fetchInvocations])

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <CopyTextPopover text={namespace}>
          <Typography color="text.primary">{namespace}</Typography>
        </CopyTextPopover>
        <Link to={`/${namespace}/compute-graphs`}>
          <CopyTextPopover text="Compute Graphs">
            <Typography color="text.primary">Compute Graphs</Typography>
          </CopyTextPopover>
        </Link>
        <CopyTextPopover text={application.name}>
          <Typography color="text.primary">{application.name}</Typography>
        </CopyTextPopover>
      </Breadcrumbs>
      <Box>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument
                size="25"
                className="heading-icons"
                variant="Outline"
              />
            </div>
            <Typography
              variant="h4"
              sx={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
                gap: 1,
              }}
            >
              {application.name}
              <Chip label={`Version ${application.version}`} size="small" />
              <CopyText text={application.name} />
            </Typography>
          </div>

          <ApplicationFunctionsTable
            namespace={namespace}
            applicationFunctions={application.functions}
          />
        </Box>

        <GraphRequestsTable
          namespace={namespace}
          applicationName={application.name}
          shallowGraphRequests={shallowGraphRequests}
          // onDelete={handleDelete}
        />

        <Box
          sx={{
            display: 'flex',
            justifyContent: 'space-between',
            mt: 2,
            alignItems: 'center',
          }}
        >
          <Button
            startIcon={<NavigateBeforeIcon />}
            onClick={handlePreviousPage}
            disabled={!prevCursor || isLoading}
            variant="outlined"
          >
            Previous
          </Button>

          {isLoading && <CircularProgress size={24} />}

          <Button
            endIcon={<NavigateNextIcon />}
            onClick={handleNextPage}
            disabled={!nextCursor || isLoading}
            variant="outlined"
          >
            Next
          </Button>
        </Box>
      </Box>
    </Stack>
  )
}

export default ApplicationDetailsPage
