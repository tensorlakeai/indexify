import { Box, Breadcrumbs, Typography, Stack, Chip, Button } from '@mui/material'
import { TableDocument } from 'iconsax-react'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData, useNavigate } from 'react-router-dom'
import { useState, useCallback, useEffect } from 'react'
import type { Invocation } from '../../types'
import type { IndividualComputeGraphLoaderData } from './types'
import ComputeGraphTable from '../../components/tables/ComputeGraphTable'
import CopyText from '../../components/CopyText'
import InvocationsTable from '../../components/tables/InvocationsTable'
import CopyTextPopover from '../../components/CopyTextPopover'

const IndividualComputeGraphPage = () => {
  const { invocationsList, computeGraph, namespace, nextCursor, currentCursor } =
    useLoaderData() as IndividualComputeGraphLoaderData
  
  const navigate = useNavigate()

  const [invocations, setInvocations] = useState<Invocation[]>(
    [...invocationsList].sort(
      (a, b) => (b.created_at ?? 0) - (a.created_at ?? 0)
    )
  )

  useEffect(() => {
    setInvocations(
      [...invocationsList].sort(
        (a, b) => (b.created_at ?? 0) - (a.created_at ?? 0)
      )
    )
  }, [invocationsList])

  const handleDelete = useCallback((updatedList: Invocation[]) => {
    const sortedList = [...updatedList].sort(
      (a, b) => (b.created_at ?? 0) - (a.created_at ?? 0)
    )
    setInvocations(sortedList)
  }, [])

  const handleNextPage = () => {
    if (nextCursor) {
      navigate(`/${namespace}/compute-graphs/${computeGraph.name}?cursor=${nextCursor}`)
    }
  }

  const handlePreviousPage = () => {
    navigate(`/${namespace}/compute-graphs/${computeGraph.name}`)
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
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 2 }}>
          <Button 
            onClick={handlePreviousPage}
            disabled={!currentCursor}
          >
            Previous
          </Button>
          <Button 
            onClick={handleNextPage}
            disabled={!nextCursor}
          >
            Next
          </Button>
        </Box>
      </Box>
    </Stack>
  )
}

export default IndividualComputeGraphPage
