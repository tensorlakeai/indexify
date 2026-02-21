import StopIcon from '@mui/icons-material/Stop'
import {
  Alert,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Grid,
  IconButton,
  Typography,
} from '@mui/material'
import { Box, Stack } from '@mui/system'
import axios from 'axios'
import { Box as BoxIcon, InfoCircle } from 'iconsax-react'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { ListSandboxesResponse, SandboxInfo } from '../../types/types'
import { getIndexifyServiceURL } from '../../utils/helpers'
import CopyText from '../CopyText'
import TruncatedText from '../TruncatedText'

interface SandboxesCardProps {
  sandboxes: ListSandboxesResponse
  namespace: string
}

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

export function SandboxesCard({ sandboxes, namespace }: SandboxesCardProps) {
  const [localSandboxes, setLocalSandboxes] = useState<SandboxInfo[]>(
    sandboxes.sandboxes || []
  )
  const [error, setError] = useState<string | null>(null)
  const [stoppingIds, setStoppingIds] = useState<Set<string>>(new Set())

  async function handleStopSandbox(sandboxId: string) {
    setStoppingIds((prev) => new Set(prev).add(sandboxId))
    try {
      const serviceURL = getIndexifyServiceURL()
      await axios.delete(
        `${serviceURL}/v1/namespaces/${namespace}/sandboxes/${sandboxId}`
      )
      // Update the sandbox status locally
      setLocalSandboxes((prevSandboxes) =>
        prevSandboxes.map((s) =>
          s.id === sandboxId ? { ...s, status: 'terminated' } : s
        )
      )
    } catch (err) {
      console.error('Error stopping sandbox:', err)
      setError('Failed to stop sandbox. Please try again.')
    } finally {
      setStoppingIds((prev) => {
        const next = new Set(prev)
        next.delete(sandboxId)
        return next
      })
    }
  }

  function renderSandboxCard(sandbox: SandboxInfo) {
    return (
      <Grid item xs={12} sm={6} md={4} key={sandbox.id} mb={2}>
        <Card
          sx={{
            minWidth: 275,
            height: '100%',
            boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
          }}
        >
          <CardContent>
            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Link to={`/${namespace}/sandboxes/${sandbox.id}`}>
                <TruncatedText text={sandbox.id} maxLength={16} />
              </Link>
              <Box display="flex" flexDirection="row" alignItems="center">
                <CopyText text={sandbox.id} />
                {sandbox.status !== 'terminated' && (
                  <IconButton
                    onClick={() => handleStopSandbox(sandbox.id)}
                    aria-label="stop sandbox"
                    disabled={stoppingIds.has(sandbox.id)}
                  >
                    {stoppingIds.has(sandbox.id) ? (
                      <CircularProgress size={20} />
                    ) : (
                      <StopIcon color="error" />
                    )}
                  </IconButton>
                )}
              </Box>
            </Box>

            <Box sx={{ my: 1 }}>
              <Chip
                label={sandbox.status}
                size="small"
                color={getStatusColor(sandbox.status)}
                variant="outlined"
              />
              {sandbox.outcome && (
                <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                  {sandbox.outcome}
                </Typography>
              )}
            </Box>

            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 0.5 }}>
              Image: {sandbox.image}
            </Typography>

            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 0.5 }}>
              Resources: {sandbox.resources.cpus} CPU, {sandbox.resources.memory_mb}MB RAM
            </Typography>

            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 0.5 }}>
              Timeout: {sandbox.timeout_secs === 0 ? 'No timeout' : `${sandbox.timeout_secs}s`}
            </Typography>

            {sandbox.created_at > 0 && (
              <Typography variant="subtitle2" color="text.secondary">
                Created: {new Date(sandbox.created_at).toLocaleString()}
              </Typography>
            )}

            {sandbox.executor_id && (
              <Typography variant="subtitle2" color="text.secondary" sx={{ fontSize: '0.75rem' }}>
                Executor: {sandbox.executor_id.substring(0, 12)}...
              </Typography>
            )}
          </CardContent>
        </Card>
      </Grid>
    )
  }

  function renderContent() {
    if (error)
      return (
        <Alert variant="outlined" severity="error" sx={{ my: 2 }}>
          {error}
        </Alert>
      )

    if (!localSandboxes.length)
      return (
        <Alert variant="outlined" severity="info" sx={{ my: 2 }}>
          No Sandboxes Found
        </Alert>
      )

    // Sort by creation time (newest first)
    const sortedSandboxes = [...localSandboxes].sort(
      (a, b) => b.created_at - a.created_at
    )

    return (
      <Box sx={{ width: '100%', overflow: 'auto', borderRadius: '5px' }} mt={2}>
        <Grid container spacing={1}>
          {sortedSandboxes.map(renderSandboxCard)}
        </Grid>
      </Box>
    )
  }

  return (
    <>
      <Stack direction="row" alignItems="center" spacing={2}>
        <div className="heading-icon-container">
          <BoxIcon size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Sandboxes
          <IconButton
            href="https://docs.tensorlake.ai/sandboxes"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  )
}
