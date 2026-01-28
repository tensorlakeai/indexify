import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import RefreshIcon from '@mui/icons-material/Refresh'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Breadcrumbs,
  Card,
  CardContent,
  Chip,
  Grid,
  IconButton,
  Paper,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material'
import axios from 'axios'
import { TableDocument } from 'iconsax-react'
import { useCallback, useEffect, useState } from 'react'
import { Link, useLoaderData } from 'react-router-dom'
import CopyText from '../../components/CopyText'
import CopyTextPopover from '../../components/CopyTextPopover'
import ProcessLogsPanel from '../../components/ProcessLogsPanel'
import ProcessesTable from '../../components/tables/ProcessesTable'
import { ListProcessesResponse, ProcessInfo, SandboxInfo } from '../../types/types'

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

interface CodeBlockProps {
  code: string
  label: string
}

function CodeBlock({ code, label }: CodeBlockProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="subtitle2" color="text.secondary" gutterBottom>
        {label}
      </Typography>
      <Paper
        sx={{
          p: 1.5,
          backgroundColor: '#1e1e1e',
          position: 'relative',
          overflow: 'auto',
        }}
      >
        <Tooltip title={copied ? 'Copied!' : 'Copy to clipboard'}>
          <IconButton
            size="small"
            onClick={handleCopy}
            sx={{
              position: 'absolute',
              top: 8,
              right: 8,
              color: 'grey.500',
              '&:hover': { color: 'grey.300' },
            }}
          >
            <ContentCopyIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Typography
          component="pre"
          sx={{
            fontFamily: 'monospace',
            fontSize: '0.8rem',
            color: '#e0e0e0',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
            m: 0,
            pr: 4,
          }}
        >
          {code}
        </Typography>
      </Paper>
    </Box>
  )
}

const SandboxDetailsPage = () => {
  const { namespace, application, sandboxId, sandbox } = useLoaderData() as {
    namespace: string
    application: string
    sandboxId: string
    sandbox: SandboxInfo | null
  }

  const [processes, setProcesses] = useState<ProcessInfo[]>([])
  const [selectedPid, setSelectedPid] = useState<number | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Use sandbox_url for routing via sandbox-proxy or direct dataplane access
  const daemonUrl = sandbox?.sandbox_url ?? null
  const isRunning = sandbox?.status?.toLowerCase() === 'running'

  // Check if we're accessing dataplane directly (local dev)
  const isLocalDataplane = daemonUrl?.includes('127.0.0.1') || daemonUrl?.includes('localhost')

  const fetchProcesses = useCallback(async () => {
    if (!daemonUrl || !isRunning) return

    setLoading(true)
    setError(null)

    try {
      // For local dataplane access, add sandbox_id as query param (or header for axios)
      const headers = isLocalDataplane ? { 'Tensorlake-Sandbox-Id': sandboxId } : {}
      const response = await axios.get<ListProcessesResponse>(
        `${daemonUrl}/api/v1/processes`,
        { headers }
      )
      setProcesses(response.data.processes)
      // Auto-select first process if none selected
      if (response.data.processes.length > 0 && selectedPid === null) {
        setSelectedPid(response.data.processes[0].pid)
      }
    } catch (err) {
      console.error('Failed to fetch processes:', err)
      setError('Failed to fetch processes from sandbox')
    } finally {
      setLoading(false)
    }
  }, [daemonUrl, isRunning, selectedPid, isLocalDataplane, sandboxId])

  // Poll for processes periodically (only when sandbox is running)
  useEffect(() => {
    if (!isRunning) return

    fetchProcesses()

    const interval = setInterval(fetchProcesses, 5000)
    return () => clearInterval(interval)
  }, [fetchProcesses, isRunning])

  if (!sandbox) {
    return (
      <Stack direction="column" spacing={3}>
        <Alert severity="error">Sandbox not found</Alert>
      </Stack>
    )
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
        <Link to={`/${namespace}/applications`}>
          <CopyTextPopover text="Applications">
            <Typography color="text.primary">Applications</Typography>
          </CopyTextPopover>
        </Link>
        <Link to={`/${namespace}/applications/${application}`}>
          <CopyTextPopover text={application}>
            <Typography color="text.primary">{application}</Typography>
          </CopyTextPopover>
        </Link>
        <Typography color="text.primary">Sandboxes</Typography>
        <CopyTextPopover text={sandboxId}>
          <Typography color="text.primary">
            {sandboxId.substring(0, 12)}...
          </Typography>
        </CopyTextPopover>
      </Breadcrumbs>

      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument
                size="25"
                className="heading-icons"
                variant="Outline"
              />
            </div>
            <Typography variant="h4" display={'flex'} flexDirection={'row'}>
              Sandbox - {sandboxId.substring(0, 12)}... <CopyText text={sandboxId} />
            </Typography>
          </div>
        </Box>

        {/* Sandbox Details */}
        <Card
          sx={{
            mb: 3,
            boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
          }}
        >
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Details
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Sandbox ID
                </Typography>
                <Box display="flex" alignItems="center" gap={1}>
                  <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                    {sandbox.id}
                  </Typography>
                  <CopyText text={sandbox.id} />
                </Box>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Status
                </Typography>
                <Box display="flex" alignItems="center" gap={1}>
                  <Chip
                    label={sandbox.status}
                    size="small"
                    color={getStatusColor(sandbox.status)}
                  />
                  {sandbox.outcome && (
                    <Typography variant="body2" color="text.secondary">
                      ({sandbox.outcome})
                    </Typography>
                  )}
                </Box>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Image
                </Typography>
                <Box display="flex" alignItems="center" gap={1}>
                  <Typography variant="body1">{sandbox.image}</Typography>
                  <CopyText text={sandbox.image} />
                </Box>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Created At
                </Typography>
                <Typography variant="body1">
                  {new Date(sandbox.created_at).toLocaleString()}
                </Typography>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Resources
                </Typography>
                <Typography variant="body1">
                  {sandbox.resources.cpus} CPU, {sandbox.resources.memory_mb}MB RAM,{' '}
                  {sandbox.resources.ephemeral_disk_mb}MB Disk
                </Typography>
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography
                  variant="subtitle2"
                  color="text.secondary"
                  gutterBottom
                >
                  Timeout
                </Typography>
                <Typography variant="body1">
                  {sandbox.timeout_secs === 0 ? 'No timeout' : `${sandbox.timeout_secs}s`}
                </Typography>
              </Grid>

              {sandbox.container_id && (
                <Grid item xs={12} md={6}>
                  <Typography
                    variant="subtitle2"
                    color="text.secondary"
                    gutterBottom
                  >
                    Container ID
                  </Typography>
                  <Box display="flex" alignItems="center" gap={1}>
                    <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                      {sandbox.container_id.substring(0, 12)}...
                    </Typography>
                    <CopyText text={sandbox.container_id} />
                  </Box>
                </Grid>
              )}

              {sandbox.executor_id && (
                <Grid item xs={12} md={6}>
                  <Typography
                    variant="subtitle2"
                    color="text.secondary"
                    gutterBottom
                  >
                    Executor ID
                  </Typography>
                  <Box display="flex" alignItems="center" gap={1}>
                    <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                      {sandbox.executor_id.substring(0, 12)}...
                    </Typography>
                    <CopyText text={sandbox.executor_id} />
                  </Box>
                </Grid>
              )}

              {sandbox.sandbox_url && (
                <Grid item xs={12} md={6}>
                  <Typography
                    variant="subtitle2"
                    color="text.secondary"
                    gutterBottom
                  >
                    Sandbox URL
                  </Typography>
                  <Box display="flex" alignItems="center" gap={1}>
                    <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                      {sandbox.sandbox_url}
                    </Typography>
                    <CopyText text={sandbox.sandbox_url} />
                  </Box>
                </Grid>
              )}
            </Grid>
          </CardContent>
        </Card>

        {/* API Examples Section */}
        {daemonUrl && (
          <Accordion
            sx={{
              mb: 3,
              boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
              '&:before': { display: 'none' },
            }}
          >
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Typography variant="h6">API Examples</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Use these curl commands to interact with the sandbox daemon directly.
              </Typography>

              <CodeBlock
                label="Run a simple command"
                code={`curl -X POST ${daemonUrl}/api/v1/processes \\
  -H "Content-Type: application/json" \\
  -d '{"command": "echo", "args": ["Hello, World!"]}'`}
              />

              <CodeBlock
                label="Run a shell command with environment variables"
                code={`curl -X POST ${daemonUrl}/api/v1/processes \\
  -H "Content-Type: application/json" \\
  -d '{
    "command": "sh",
    "args": ["-c", "echo $MY_VAR"],
    "env": {"MY_VAR": "test value"}
  }'`}
              />

              <CodeBlock
                label="Run bash script"
                code={`curl -X POST ${daemonUrl}/api/v1/processes \\
  -H "Content-Type: application/json" \\
  -d '{
    "command": "bash",
    "args": ["-c", "for i in 1 2 3; do echo Line $i; sleep 1; done"]
  }'`}
              />

              <CodeBlock
                label="List all processes"
                code={`curl ${daemonUrl}/api/v1/processes`}
              />

              <CodeBlock
                label="Get process stdout (replace <pid> with actual PID)"
                code={`curl ${daemonUrl}/api/v1/processes/<pid>/stdout`}
              />

              <CodeBlock
                label="Follow process output in real-time (SSE)"
                code={`curl ${daemonUrl}/api/v1/processes/<pid>/output/follow`}
              />

              <CodeBlock
                label="Write a file"
                code={`curl -X PUT "${daemonUrl}/api/v1/files?path=/tmp/hello.txt" \\
  -H "Content-Type: application/octet-stream" \\
  -d "Hello from sandbox!"`}
              />

              <CodeBlock
                label="Read a file"
                code={`curl "${daemonUrl}/api/v1/files?path=/tmp/hello.txt"`}
              />

              <CodeBlock
                label="List directory contents"
                code={`curl "${daemonUrl}/api/v1/files/list?path=/tmp"`}
              />
            </AccordionDetails>
          </Accordion>
        )}

        {/* Processes Section */}
        {!isRunning ? (
          <Alert severity="info" sx={{ mb: 3 }}>
            {sandbox.status === 'Terminated'
              ? 'Sandbox has been terminated. Process information is no longer available.'
              : 'Sandbox is not yet running. Process information will be available once the sandbox starts.'}
          </Alert>
        ) : !daemonUrl ? (
          <Alert severity="warning" sx={{ mb: 3 }}>
            Sandbox is running but URL is not available.
          </Alert>
        ) : (
          <>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
              <Typography variant="h6">Processes</Typography>
              <Tooltip title="Refresh processes">
                <IconButton onClick={fetchProcesses} disabled={loading} size="small">
                  <RefreshIcon />
                </IconButton>
              </Tooltip>
            </Box>

            {error && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {error}
              </Alert>
            )}

            <ProcessesTable
              processes={processes}
              selectedPid={selectedPid}
              onSelectProcess={setSelectedPid}
            />

            {/* Process Logs */}
            {selectedPid !== null && daemonUrl && (
              <ProcessLogsPanel daemonUrl={daemonUrl} pid={selectedPid} sandboxId={isLocalDataplane ? sandboxId : undefined} />
            )}
          </>
        )}
      </Box>
    </Stack>
  )
}

export default SandboxDetailsPage
