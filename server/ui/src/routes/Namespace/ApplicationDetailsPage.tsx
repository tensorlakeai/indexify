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
import ApplicationEntrypointTable from '../../components/tables/ApplicationEntrypointTable'
import ApplicationFunctionsTable from '../../components/tables/ApplicationFunctionsTable'
import ApplicationTagsTable from '../../components/tables/ApplicationTagsTable'
import { ShallowRequestsTable } from '../../components/tables/ShallowRequestsTable'
import type { ShallowRequest } from '../../types/types'
import { getIndexifyServiceURL } from '../../utils/helpers'
import { ApplicationDetailsLoaderData } from './types'

const ApplicationDetailsPage = () => {
  const { namespace, application, applicationRequests } =
    useLoaderData() as ApplicationDetailsLoaderData

  const [shallowGraphRequests, setShallowGraphRequests] = useState<
    ShallowRequest[]
  >(applicationRequests.requests)
  const [isLoading, setIsLoading] = useState(false)
  const [prevCursor, setPrevCursor] = useState<string | null>(
    applicationRequests.prev_cursor ? applicationRequests.prev_cursor : null
  )
  const [nextCursor, setNextCursor] = useState<string | null>(
    applicationRequests.next_cursor ? applicationRequests.next_cursor : null
  )

  const retrieveApplicationRequests = useCallback(
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

        setShallowGraphRequests([...data.requests])

        setPrevCursor(data.prev_cursor)
        setNextCursor(data.next_cursor)
      } catch (error) {
        console.error('Error fetching requests:', error)
      } finally {
        setIsLoading(false)
      }
    },
    [namespace, application.name]
  )

  const handleNextPage = useCallback(() => {
    if (nextCursor) {
      retrieveApplicationRequests(nextCursor, 'forward')
    }
  }, [nextCursor, retrieveApplicationRequests])

  const handlePreviousPage = useCallback(() => {
    if (prevCursor) {
      retrieveApplicationRequests(prevCursor, 'backward')
    }
  }, [prevCursor, retrieveApplicationRequests])

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

          <ApplicationEntrypointTable
            namespace={namespace}
            entrypoint={application.entrypoint}
          />

          <ApplicationTagsTable namespace={namespace} tags={application.tags} />
        </Box>

        <ShallowRequestsTable
          namespace={namespace}
          applicationName={application.name}
          shallowRequests={shallowGraphRequests}
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
