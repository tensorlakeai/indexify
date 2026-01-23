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
import { ProcessInfo } from '../../types/types'

interface ProcessesTableProps {
  processes: ProcessInfo[]
  selectedPid: number | null
  onSelectProcess: (pid: number) => void
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
    case 'exited':
      return 'warning'
    case 'signaled':
      return 'error'
    default:
      return 'default'
  }
}

function ProcessesTable({ processes, selectedPid, onSelectProcess }: ProcessesTableProps) {
  if (!processes || processes.length === 0) {
    return (
      <Box sx={{ width: '100%', mt: 2 }}>
        <Typography variant="h6" gutterBottom>
          Processes
        </Typography>
        <Typography variant="body2" color="text.secondary">
          No processes found. The sandbox may not have started any processes yet.
        </Typography>
      </Box>
    )
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Processes
      </Typography>
      <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
        <Table sx={{ minWidth: 650 }} aria-label="processes table">
          <TableHead>
            <TableRow>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                PID
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Command
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Status
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Exit Code
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Started At
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Ended At
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {processes.map((process) => (
              <TableRow
                key={process.pid}
                onClick={() => onSelectProcess(process.pid)}
                sx={{
                  cursor: 'pointer',
                  backgroundColor: selectedPid === process.pid ? 'action.selected' : 'inherit',
                  '&:hover': {
                    backgroundColor: 'action.hover',
                  },
                  '& td': {
                    border: 'none',
                    borderTop: '1px solid rgba(224, 224, 224, 1)',
                  },
                }}
              >
                <TableCell sx={CELL_STYLES}>
                  <Typography
                    variant="body2"
                    sx={{ fontFamily: 'monospace', fontWeight: 'bold' }}
                  >
                    {process.pid}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography
                    variant="body2"
                    sx={{
                      fontFamily: 'monospace',
                      wordBreak: 'break-all',
                      overflowWrap: 'break-word',
                      maxWidth: 300,
                    }}
                  >
                    {process.command} {process.args.join(' ')}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Chip
                    label={process.status}
                    size="small"
                    color={getStatusColor(process.status)}
                    variant="outlined"
                  />
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2">
                    {process.exit_code !== null && process.exit_code !== undefined
                      ? process.exit_code
                      : process.signal !== null && process.signal !== undefined
                        ? `Signal ${process.signal}`
                        : '-'}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2">
                    {new Date(process.started_at).toLocaleString()}
                  </Typography>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Typography variant="body2">
                    {process.ended_at
                      ? new Date(process.ended_at).toLocaleString()
                      : '-'}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default ProcessesTable
