import StopIcon from '@mui/icons-material/Stop'
import {
  Box,
  Chip,
  CircularProgress,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material'
import axios from 'axios'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { SandboxInfo } from '../../types/types'
import { getIndexifyServiceURL } from '../../utils/helpers'
import CopyText from '../CopyText'

interface SandboxesTableProps {
  sandboxes: SandboxInfo[]
  namespace: string
  onSandboxDeleted?: () => void
}

const CELL_STYLES = { fontSize: 14, pt: 1, border: 'none' } as const

const TABLE_CONTAINER_STYLES = {
  borderRadius: '8px',
  mt: 2,
  boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
} as const

function getStatusColor(status: string): 'success' | 'warning' | 'error' | 'default' {
  switch (status.toLowerCase()) {
    case 'running':
      return 'success'
    case 'pending':
      return 'warning'
    case 'terminated':
      return 'error'
    default:
      return 'default'
  }
}

function SandboxesTable({ sandboxes, namespace, onSandboxDeleted }: SandboxesTableProps) {
  const [stoppingIds, setStoppingIds] = useState<Set<string>>(new Set())

  const handleStopSandbox = async (sandboxId: string) => {
    setStoppingIds((prev) => new Set(prev).add(sandboxId))
    try {
      const serviceURL = getIndexifyServiceURL()
      await axios.delete(
        `${serviceURL}/v1/namespaces/${namespace}/sandboxes/${sandboxId}`
      )
      onSandboxDeleted?.()
    } catch (error) {
      console.error('Failed to stop sandbox:', error)
    } finally {
      setStoppingIds((prev) => {
        const next = new Set(prev)
        next.delete(sandboxId)
        return next
      })
    }
  }

  // Don't render if no sandboxes
  if (!sandboxes || sandboxes.length === 0) {
    return null
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Sandboxes
      </Typography>
      <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
        <Table sx={{ minWidth: 650 }} aria-label="sandboxes table">
          <TableHead>
            <TableRow>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                ID
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Image
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Status
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Resources
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Timeout
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Created At
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Container ID
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Actions
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {sandboxes.map((sandbox) => (
              <TableRow
                key={sandbox.id}
                sx={{
                  '& td': {
                    border: 'none',
                    borderTop: '1px solid rgba(224, 224, 224, 1)',
                  },
                }}
              >
                <TableCell sx={CELL_STYLES}>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Link
                      to={`/${namespace}/sandboxes/${sandbox.id}`}
                      style={{ textDecoration: 'none' }}
                    >
                      <Typography
                        variant="body2"
                        sx={{
                          fontFamily: 'monospace',
                          fontSize: '0.75rem',
                          color: 'primary.main',
                          '&:hover': {
                            textDecoration: 'underline',
                          },
                        }}
                      >
                        {sandbox.id.substring(0, 12)}...
                      </Typography>
                    </Link>
                    <CopyText text={sandbox.id} />
                  </Box>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Typography
                      variant="body2"
                      sx={{
                        wordBreak: 'break-all',
                        overflowWrap: 'break-word',
                        maxWidth: 200,
                      }}
                    >
                      {sandbox.image}
                    </Typography>
                    <CopyText text={sandbox.image} />
                  </Box>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Box display="flex" flexDirection="column" gap={0.5}>
                    <Chip
                      label={sandbox.status}
                      size="small"
                      color={getStatusColor(sandbox.status)}
                      variant="outlined"
                    />
                    {sandbox.outcome && (
                      <Typography variant="caption" color="text.secondary">
                        {sandbox.outcome}
                      </Typography>
                    )}
                  </Box>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2" sx={{ fontSize: '0.75rem' }}>
                    {sandbox.resources.cpus} CPU, {sandbox.resources.memory_mb}MB RAM
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2">
                    {sandbox.timeout_secs === 0 ? 'No timeout' : `${sandbox.timeout_secs}s`}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2">
                    {new Date(sandbox.created_at).toLocaleString()}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  {sandbox.container_id ? (
                    <Box display="flex" flexDirection="row" alignItems="center">
                      <Typography
                        variant="body2"
                        sx={{ fontFamily: 'monospace', fontSize: '0.75rem' }}
                      >
                        {sandbox.container_id.substring(0, 12)}...
                      </Typography>
                      <CopyText text={sandbox.container_id} />
                    </Box>
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      -
                    </Typography>
                  )}
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  {sandbox.status.toLowerCase() !== 'terminated' && (
                    <Tooltip title="Stop Sandbox">
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => handleStopSandbox(sandbox.id)}
                        disabled={stoppingIds.has(sandbox.id)}
                      >
                        {stoppingIds.has(sandbox.id) ? (
                          <CircularProgress size={20} />
                        ) : (
                          <StopIcon />
                        )}
                      </IconButton>
                    </Tooltip>
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

export default SandboxesTable
