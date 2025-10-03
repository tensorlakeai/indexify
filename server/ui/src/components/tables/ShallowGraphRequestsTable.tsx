import {
  Box,
  Chip,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import { Link } from 'react-router-dom'
import { RequestOutcome, ShallowGraphRequest } from '../../types/types'
import { formatTimestamp } from '../../utils/helpers'
import CopyText from '../CopyText'

interface GraphRequestTableProps {
  namespace: string
  applicationName: string
  shallowGraphRequests: ShallowGraphRequest[]
  // onDelete: (updatedList: ShallowGraphRequest[]) => void
}

export function GraphRequestsTable({
  namespace,
  applicationName,
  shallowGraphRequests,
}: GraphRequestTableProps) {
  console.log('shallowGraphRequests', shallowGraphRequests)

  // TODO: update this
  // const handleDelete = async (invocationId: string) => {
  //   try {
  //     await axios.delete(
  //       `${getIndexifyServiceURL()}/namespaces/${namespace}/compute_graphs/${computeGraph}/requests/${invocationId}`,
  //       { headers: { accept: '*/*' } }
  //     )

  //     onDelete(
  //       shallowGraphRequests.filter(
  //         (invocation) => invocation.id !== invocationId
  //       )
  //     )
  //   } catch (error) {
  //     console.error(`Error deleting invocation ${invocationId}:`, error)
  //   }
  // }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Graph Requests
      </Typography>
      <TableContainer
        component={Paper}
        sx={{ boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset' }}
      >
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Created At</TableCell>
              <TableCell>Outcome</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {shallowGraphRequests.map((request) => (
              <TableRow key={request.id}>
                <TableCell>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Link
                      to={`/${namespace}/applications/${applicationName}/requests/${request.id}`}
                    >
                      {request.id}
                    </Link>
                    <CopyText text={request.id} />
                  </Box>
                </TableCell>
                <TableCell>{formatTimestamp(request.created_at)}</TableCell>
                <TableCell>{renderOutcome(request.outcome)}</TableCell>
                {/* <TableCell>
                  <IconButton
                    onClick={() => handleDelete(request.id)}
                    color="error"
                    size="small"
                  >
                    <DeleteIcon />
                  </IconButton>
                </TableCell> */}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default GraphRequestsTable

const renderOutcome = (outcome: RequestOutcome | undefined) => {
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
