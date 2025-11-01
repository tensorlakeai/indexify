import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp'
import {
  Box,
  Chip,
  Collapse,
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
import {
  Allocation,
  FunctionRun,
  FunctionRunOutcome,
  FunctionRunStatus,
} from '../../types/types'
import { formatTimestamp, nanoSecondsToDate } from '../../utils/helpers'
import CopyText from '../CopyText'

interface FunctionRunsTableProps {
  namespace: string
  applicationName: string
  functionRuns: FunctionRun[]
}

interface FunctionRunRowProps {
  namespace: string
  applicationName: string
  functionRun: FunctionRun
}

const renderStatus = (status: FunctionRunStatus) => {
  const statusConfig: Record<
    FunctionRunStatus,
    { label: string; color: 'default' | 'info' | 'success' }
  > = {
    pending: { label: 'pending', color: 'default' },
    running: { label: 'running', color: 'info' },
    completed: { label: 'completed', color: 'success' },
  }

  const config = statusConfig[status] || {
    label: 'Unknown',
    color: 'default' as const,
  }
  return <Chip label={config.label} size="small" color={config.color} />
}

const renderOutcome = (outcome?: FunctionRunOutcome | null) => {
  if (!outcome) {
    return <Chip label="Unknown" size="small" color="default" />
  }

  const outcomeConfig: Record<
    FunctionRunOutcome,
    { label: string; color: 'success' | 'error' | 'default' }
  > = {
    success: { label: 'success', color: 'success' },
    failure: { label: 'failure', color: 'error' },
    undefined: { label: 'undefined', color: 'default' },
  }

  const config = outcomeConfig[outcome] || {
    label: 'Unknown',
    color: 'default' as const,
  }
  return <Chip label={config.label} size="small" color={config.color} />
}

const formatDuration = (durationMs?: number | null): string => {
  if (!durationMs) return 'N/A'
  if (durationMs < 1000) return `${durationMs}ms`
  return `${(durationMs / 1000).toFixed(2)}s`
}

function FunctionRunRow({
  namespace,
  applicationName,
  functionRun,
}: FunctionRunRowProps) {
  const [open, setOpen] = useState(false)
  const hasAllocations =
    functionRun.allocations && functionRun.allocations.length > 0

  return (
    <>
      <TableRow>
        <TableCell>
          {hasAllocations && (
            <IconButton
              aria-label="expand row"
              size="small"
              onClick={() => setOpen(!open)}
            >
              {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
            </IconButton>
          )}
        </TableCell>
        <TableCell>
          <Box display="flex" flexDirection="row" alignItems="center">
            {functionRun.id}
            <CopyText text={functionRun.id} />
          </Box>
        </TableCell>
        <TableCell>{functionRun.name}</TableCell>
        <TableCell>{renderStatus(functionRun.status)}</TableCell>
        <TableCell>{renderOutcome(functionRun.outcome)}</TableCell>
        <TableCell>{nanoSecondsToDate(functionRun.created_at)}</TableCell>
        <TableCell>
          <Box display="flex" flexDirection="row" alignItems="center">
            {functionRun.application_version}
            <CopyText text={functionRun.application_version} />
          </Box>
        </TableCell>
      </TableRow>
      {hasAllocations && (
        <TableRow>
          <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={7}>
            <Collapse in={open} timeout="auto" unmountOnExit>
              <Box sx={{ margin: 1 }}>
                <Typography gutterBottom component="div">
                  Allocations
                </Typography>
                <Table size="small" aria-label="allocations">
                  <TableHead>
                    <TableRow>
                      <TableCell>ID</TableCell>
                      <TableCell>Executor ID</TableCell>
                      <TableCell>Function Executor ID</TableCell>
                      <TableCell>Outcome</TableCell>
                      <TableCell>Attempt</TableCell>
                      <TableCell>Duration</TableCell>
                      <TableCell>Created At</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {functionRun.allocations!.map((allocation: Allocation) => (
                      <TableRow key={allocation.id}>
                        <TableCell>
                          <Box
                            display="flex"
                            flexDirection="row"
                            alignItems="center"
                          >
                            {allocation.id}
                            <CopyText text={allocation.id} />
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box
                            display="flex"
                            flexDirection="row"
                            alignItems="center"
                          >
                            {allocation.executor_id}
                            <CopyText text={allocation.executor_id} />
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box
                            display="flex"
                            flexDirection="row"
                            alignItems="center"
                          >
                            {allocation.function_executor_id}
                            <CopyText text={allocation.function_executor_id} />
                          </Box>
                        </TableCell>
                        <TableCell>
                          {renderOutcome(allocation.outcome)}
                        </TableCell>
                        <TableCell>{allocation.attempt_number}</TableCell>
                        <TableCell>
                          {formatDuration(allocation.execution_duration_ms)}
                        </TableCell>
                        <TableCell>
                          {formatTimestamp(allocation.created_at)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </Box>
            </Collapse>
          </TableCell>
        </TableRow>
      )}
    </>
  )
}

export function FunctionRunsTable({
  namespace,
  applicationName,
  functionRuns,
}: FunctionRunsTableProps) {
  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Function Runs
      </Typography>
      <TableContainer
        component={Paper}
        sx={{ boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset' }}
      >
        <Table>
          <TableHead>
            <TableRow>
              <TableCell />
              <TableCell>ID</TableCell>
              <TableCell>Function Name</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Outcome</TableCell>
              <TableCell>Created At</TableCell>
              <TableCell>Application Version</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {functionRuns.map((functionRun) => (
              <FunctionRunRow
                key={functionRun.id}
                namespace={namespace}
                applicationName={applicationName}
                functionRun={functionRun}
              />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default FunctionRunsTable
