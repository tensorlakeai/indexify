import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import SearchIcon from '@mui/icons-material/Search'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Chip,
  InputAdornment,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import axios from 'axios'
import React, { useCallback, useEffect, useState } from 'react'
import { toast, ToastContainer } from 'react-toastify'
import 'react-toastify/dist/ReactToastify.css'
import { formatTimestamp } from '../../utils/helpers'
import CopyText from '../CopyText'
import CopyTextPopover from '../CopyTextPopover'

interface Output {
  compute_fn: string
  id: string
  created_at: string
}

interface Task {
  id: string
  namespace: string
  compute_fn: string
  compute_graph: string
  invocation_id: string
  input_key: string
  outcome: string
  status: string
  allocations: Allocation[]
}

export interface Allocation {
  id: string
  namespace: string
  compute_graph: string
  compute_fn: string
  executor_id: string
  task_id: string
  invocation_id: string
  created_at: number
  outcome: string
  attempt_number: number
}

interface InvocationTasksTableProps {
  indexifyServiceURL: string
  invocationId: string
  namespace: string
  computeGraph: string
}

const OUTCOME_STYLES = {
  success: {
    color: '#008000b8',
    backgroundColor: 'rgb(0 200 0 / 19%)',
  },
  failed: {
    color: 'red',
    backgroundColor: 'rgba(255, 0, 0, 0.1)',
  },
  failure: {
    color: 'red',
    backgroundColor: 'rgba(255, 0, 0, 0.1)',
  },
  completed: {
    color: '#008000b8',
    backgroundColor: 'rgb(0 200 0 / 19%)',
  },
  default: {
    color: 'blue',
    backgroundColor: 'rgba(0, 0, 255, 0.1)',
  },
} as const

export function InvocationTasksTable({
  indexifyServiceURL,
  invocationId,
  namespace,
  computeGraph,
}: InvocationTasksTableProps) {
  const [tasks, setTasks] = useState<Task[]>([])
  const [outputs, setOutputs] = useState<Output[]>([])
  const [searchTerms, setSearchTerms] = useState<Record<string, string>>({})
  const [filteredTasks, setFilteredTasks] = useState<Record<string, Task[]>>({})
  const [expandedPanels, setExpandedPanels] = useState<Record<string, boolean>>(
    {}
  )

  const fetchTasks = async () => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/tasks`
      const response = await axios.get<{ tasks: Task[] }>(url, {
        headers: { accept: 'application/json' },
      })
      setTasks(response.data.tasks)
      const outputUrl = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/outputs`
      const outputResponse = await axios.get<{ outputs: Output[] }>(outputUrl, {
        headers: { accept: 'application/json' },
      })
      setOutputs(outputResponse.data.outputs)
    } catch (error) {
      console.error('Error fetching tasks:', error)
      toast.error('Failed to fetch tasks. Please try again later.')
    }
  }

  const fetchFunctionOutputPayload = useCallback(
    async (function_name: string, output_id: string) => {
      try {
        const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/fn/${function_name}/output/${output_id}`
        const response = await axios.get(url)
        const content_type = response.headers['content-type']
        const fileExtension =
          content_type === 'application/json'
            ? 'json'
            : content_type === 'application/octet-stream'
            ? 'bin'
            : 'txt'
        const blob = new Blob([response.data], { type: content_type })
        const downloadLink = document.createElement('a')
        downloadLink.href = URL.createObjectURL(blob)
        downloadLink.download = `${function_name}_${output_id}_output.${fileExtension}`
        document.body.appendChild(downloadLink)
        downloadLink.click()
        document.body.removeChild(downloadLink)
      } catch (error) {
        toast.error(
          `Failed to fetch output for function: ${function_name} for output id: ${output_id}`
        )
      }
    },
    [indexifyServiceURL, invocationId, namespace, computeGraph]
  )

  useEffect(() => {
    fetchTasks()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [indexifyServiceURL, invocationId, namespace, computeGraph])

  useEffect(() => {
    const grouped = tasks.reduce(
      (acc, task) => ({
        ...acc,
        [task.compute_fn]: [...(acc[task.compute_fn] || []), task],
      }),
      {} as Record<string, Task[]>
    )

    setFilteredTasks(grouped)
    setExpandedPanels(
      Object.keys(grouped).reduce((acc, key) => ({ ...acc, [key]: true }), {})
    )
  }, [tasks])

  useEffect(() => {
    const filtered = tasks.reduce((acc, task) => {
      const searchTerm = searchTerms[task.compute_fn]?.toLowerCase()
      if (!searchTerm || task.id.toLowerCase().includes(searchTerm)) {
        return {
          ...acc,
          [task.compute_fn]: [...(acc[task.compute_fn] || []), task],
        }
      }
      return acc
    }, {} as Record<string, Task[]>)

    setFilteredTasks(filtered)
  }, [tasks, searchTerms])

  const handleAccordionChange =
    (panel: string) => (event: React.SyntheticEvent, isExpanded: boolean) => {
      const target = event.target as HTMLElement
      if (
        target.classList.contains('MuiAccordionSummary-expandIconWrapper') ||
        target.closest('.MuiAccordionSummary-expandIconWrapper')
      ) {
        setExpandedPanels((prev) => ({ ...prev, [panel]: isExpanded }))
      }
    }

  const viewLogs = async (
    allocation: Allocation,
    logType: 'stdout' | 'stderr'
  ) => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/allocations/${allocation.id}/logs/${logType}`
      const { data: logContent } = await axios.get(url, {
        responseType: 'text',
        headers: { accept: 'text/plain' },
      })

      if (!logContent?.trim()) {
        toast.info(`No ${logType} logs found for allocation ${allocation.id}.`)
        return
      }

      const newWindow = window.open('', '_blank')
      if (!newWindow) {
        toast.error(
          'Unable to open new window. Please check your browser settings.'
        )
        return
      }

      newWindow.document.write(`
        <html>
          <head>
            <title>Allocation ${allocation.id} - ${logType} Log</title>
            <style>body { font-family: monospace; white-space: pre-wrap; word-wrap: break-word; }</style>
          </head>
          <body>${logContent}</body>
        </html>
      `)
      newWindow.document.close()
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        toast.info(`No ${logType} logs found for allocation ${allocation.id}.`)
      } else {
        toast.error(
          `Failed to fetch ${logType} logs for allocation ${allocation.id}. Please try again later.`
        )
        console.error(`Error fetching ${logType} logs:`, error)
      }
    }
  }

  const getChipStyles = (outcome: string) => ({
    borderRadius: '16px',
    fontWeight: 'bold',
    ...(OUTCOME_STYLES[outcome.toLowerCase() as keyof typeof OUTCOME_STYLES] ||
      OUTCOME_STYLES.default),
  })

  if (!tasks?.length) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Tasks Found
        </Alert>
        <ToastContainer position="top-right" />
      </Box>
    )
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h4" gutterBottom>
        Tasks for Invocation
      </Typography>
      {Object.entries(filteredTasks).map(([computeFn, tasks], index) => (
        <Accordion
          key={computeFn}
          expanded={expandedPanels[computeFn]}
          onChange={handleAccordionChange(computeFn)}
        >
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls={`panel${index}-content`}
            id={`panel${index}-header`}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                width: '100%',
                justifyContent: 'space-between',
              }}
            >
              <CopyTextPopover text={computeFn}>
                <Typography variant="h4">{computeFn}</Typography>
              </CopyTextPopover>
              <Box
                sx={{ display: 'flex', alignItems: 'center' }}
                onClick={(e) => e.stopPropagation()}
              >
                <TextField
                  size="small"
                  placeholder="Search by ID"
                  value={searchTerms[computeFn] || ''}
                  onChange={(e) =>
                    setSearchTerms((prev) => ({
                      ...prev,
                      [computeFn]: e.target.value,
                    }))
                  }
                  InputProps={{
                    endAdornment: (
                      <InputAdornment position="end">
                        <SearchIcon />
                      </InputAdornment>
                    ),
                  }}
                />
              </Box>
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            <Typography sx={{ mb: 2 }}>Tasks</Typography>
            <TableContainer
              component={Paper}
              sx={{
                boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
              }}
              elevation={0}
            >
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Outcome</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {tasks.map((task) => (
                    <TableRow key={task.id}>
                      <TableCell>
                        <Box
                          sx={{ display: 'flex', alignItems: 'center', gap: 1 }}
                        >
                          {task.id}
                          <CopyText text={task.id} />
                        </Box>
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={task.outcome}
                          sx={getChipStyles(task.outcome)}
                        />
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={task.status}
                          sx={getChipStyles(task.status)}
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>

            <Typography sx={{ mt: 3, mb: 2 }}>Allocations</Typography>
            <TableContainer
              component={Paper}
              sx={{
                boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
              }}
              elevation={0}
            >
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Executor ID</TableCell>
                    <TableCell>Retries</TableCell>
                    <TableCell>Outcome</TableCell>
                    <TableCell>Logs</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {tasks.map((task) =>
                    task.allocations.map((allocation) => (
                      <TableRow key={allocation.id}>
                        <TableCell>
                          <Box
                            sx={{
                              display: 'flex',
                              alignItems: 'center',
                              gap: 1,
                            }}
                          >
                            {allocation.id}
                            <CopyText text={allocation.id} />
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box
                            sx={{
                              display: 'inline-flex',
                              alignItems: 'center',
                              gap: 1,
                            }}
                          >
                            {allocation.executor_id}
                            <CopyText text={allocation.executor_id} />
                          </Box>
                        </TableCell>
                        <TableCell>{allocation.attempt_number}</TableCell>
                        <TableCell>
                          <Chip
                            label={allocation.outcome}
                            sx={getChipStyles(allocation.outcome)}
                          />
                        </TableCell>
                        <TableCell>
                          <Button
                            onClick={() => viewLogs(allocation, 'stdout')}
                            sx={{ mr: 1 }}
                          >
                            View stdout
                          </Button>
                          <Button
                            onClick={() => viewLogs(allocation, 'stderr')}
                          >
                            View stderr
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>

            <Typography sx={{ mt: 3, mb: 2 }}>Outputs</Typography>
            <TableContainer
              component={Paper}
              sx={{
                boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
              }}
              elevation={0}
            >
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Created At</TableCell>
                    <TableCell>Output</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {outputs
                    .filter((output) => output.compute_fn === computeFn)
                    .map((output, idx) => (
                      <TableRow key={idx}>
                        <TableCell>
                          <Box
                            sx={{
                              display: 'flex',
                              alignItems: 'center',
                              gap: 1,
                            }}
                          >
                            {output.id}
                            <CopyText text={output.id} />
                          </Box>
                        </TableCell>
                        <TableCell>
                          {formatTimestamp(output.created_at)}
                        </TableCell>
                        <TableCell>
                          <Button
                            onClick={() =>
                              fetchFunctionOutputPayload(computeFn, output.id)
                            }
                          >
                            Download output
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      ))}
      <ToastContainer position="top-right" />
    </Box>
  )
}

export default InvocationTasksTable
