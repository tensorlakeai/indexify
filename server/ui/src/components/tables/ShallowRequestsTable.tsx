import DeleteIcon from '@mui/icons-material/Delete'
import {
  Box,
  Chip,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { RequestOutcome, ShallowRequest } from '../../types/types'
import { deleteApplicationRequest } from '../../utils/delete'
import { formatTimestamp } from '../../utils/helpers'
import CopyText from '../CopyText'

interface ShallowRequestTableProps {
  namespace: string
  applicationName: string
  shallowRequests: ShallowRequest[]
}

export function ShallowRequestsTable({
  namespace,
  applicationName,
  shallowRequests,
}: ShallowRequestTableProps) {
  const [localRequests, setLocalRequests] = useState<ShallowRequest[]>(
    shallowRequests || []
  )
  const [error, setError] = useState<string | null>(null)

  async function handleDeleteRequest(requestId: string) {
    try {
      const result = await deleteApplicationRequest({
        namespace,
        application: applicationName,
        requestId,
      })
      if (result.success) {
        setLocalRequests((prevRequests) =>
          prevRequests.filter((request) => request.id !== requestId)
        )
      } else {
        setError(result.message)
      }
    } catch (err) {
      console.error('Error deleting application request:', err)
      setError('Failed to delete application request. Please try again.')
    }
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Application Requests
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
              <TableCell>Delete</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {localRequests.map((request) => (
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
                <TableCell>
                  <IconButton
                    onClick={() => handleDeleteRequest(request.id)}
                    color="error"
                    size="small"
                  >
                    <DeleteIcon />
                  </IconButton>
                  {error && (
                    <>
                      <Chip label="Error" size="small" color="error" />
                      <p style={{ color: 'red' }}>{error}</p>
                    </>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default ShallowRequestsTable

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
