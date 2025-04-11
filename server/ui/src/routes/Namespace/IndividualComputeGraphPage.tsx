import {
  Box,
  Breadcrumbs,
  Typography,
  Stack,
  Chip,
  Button,
  CircularProgress,
} from '@mui/material'
import { TableDocument } from 'iconsax-react'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore'
import { Link, useLoaderData } from 'react-router-dom'
import { useState, useCallback } from 'react'
import type { Invocation } from '../../types'
import type { IndividualComputeGraphLoaderData } from './types'
import ComputeGraphTable from '../../components/tables/ComputeGraphTable'
import CopyText from '../../components/CopyText'
import InvocationsTable from '../../components/tables/InvocationsTable'
import CopyTextPopover from '../../components/CopyTextPopover'
import axios from 'axios'
import { getIndexifyServiceURL } from '../../utils/helpers'

const IndividualComputeGraphPage = () => {
  const {
    invocationsList,
    computeGraph,
    namespace,
    prevCursor: prevCursorLoader,
    nextCursor: nextCursorLoader,
  } = useLoaderData() as IndividualComputeGraphLoaderData

  const [invocations, setInvocations] = useState<Invocation[]>(invocationsList)
  const [isLoading, setIsLoading] = useState(false)
  const [prevCursor, setPrevCursor] = useState<string | null>(prevCursorLoader)
  const [nextCursor, setNextCursor] = useState<string | null>(nextCursorLoader)

  const handleDelete = useCallback((updatedList: Invocation[]) => {
    const sortedList = [...updatedList].sort(
      (a, b) => (b.created_at ?? 0) - (a.created_at ?? 0)
    )
    setInvocations(sortedList)
  }, [])

  const fetchInvocations = useCallback(
    async (cursor: string | null, direction: 'forward' | 'backward') => {
      setIsLoading(true)
      try {
        const serviceURL = getIndexifyServiceURL()
        const limit = 20
        const url = `${serviceURL}/namespaces/${namespace}/compute_graphs/${
          computeGraph.name
        }/invocations?limit=${limit}${
          cursor ? `&cursor=${cursor}` : ''
        }&direction=${direction}`

        const response = await axios.get(url)
        const data = response.data

        setInvocations([...data.invocations])

        setPrevCursor(data.prev_cursor)
        setNextCursor(data.next_cursor)
        console.log(direction, {
          prevCursor: data.prev_cursor,
          nextCursor: data.next_cursor,
        })
      } catch (error) {
        console.error('Error fetching invocations:', error)
      } finally {
        setIsLoading(false)
      }
    },
    [namespace, computeGraph.name]
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
        <CopyTextPopover text={computeGraph.name}>
          <Typography color="text.primary">{computeGraph.name}</Typography>
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
              {computeGraph.name}
              <Chip label={`Version ${computeGraph.version}`} size="small" />
              <CopyText text={computeGraph.name} />
            </Typography>
          </div>

          <ComputeGraphTable namespace={namespace} graphData={computeGraph} />
        </Box>

        <InvocationsTable
          invocationsList={invocations}
          namespace={namespace}
          computeGraph={computeGraph.name}
          onDelete={handleDelete}
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

export default IndividualComputeGraphPage
