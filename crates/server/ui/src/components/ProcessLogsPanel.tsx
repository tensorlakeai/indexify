import { Box, Paper, Tab, Tabs, Typography } from '@mui/material'
import { useCallback, useEffect, useRef, useState } from 'react'
import { OutputEvent } from '../types/types'

interface ProcessLogsPanelProps {
  daemonUrl: string
  pid: number
}

type OutputTab = 'combined' | 'stdout' | 'stderr'

interface LogLine {
  line: string
  stream?: string
  timestamp: number
}

function ProcessLogsPanel({ daemonUrl, pid }: ProcessLogsPanelProps) {
  const [activeTab, setActiveTab] = useState<OutputTab>('combined')
  const [combinedLogs, setCombinedLogs] = useState<LogLine[]>([])
  const [stdoutLogs, setStdoutLogs] = useState<LogLine[]>([])
  const [stderrLogs, setStderrLogs] = useState<LogLine[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const eventSourceRef = useRef<EventSource | null>(null)
  const logsContainerRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = useCallback(() => {
    if (logsContainerRef.current) {
      logsContainerRef.current.scrollTop = logsContainerRef.current.scrollHeight
    }
  }, [])

  // Scroll to bottom when logs change
  useEffect(() => {
    scrollToBottom()
  }, [combinedLogs, stdoutLogs, stderrLogs, activeTab, scrollToBottom])

  // Connect to SSE stream
  useEffect(() => {
    // Clean up previous connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
    }

    // Reset state
    setCombinedLogs([])
    setStdoutLogs([])
    setStderrLogs([])
    setError(null)

    // Determine which endpoint to use based on active tab
    const endpoint =
      activeTab === 'combined'
        ? `${daemonUrl}/api/v1/processes/${pid}/output/follow`
        : activeTab === 'stdout'
          ? `${daemonUrl}/api/v1/processes/${pid}/stdout/follow`
          : `${daemonUrl}/api/v1/processes/${pid}/stderr/follow`

    const eventSource = new EventSource(endpoint)
    eventSourceRef.current = eventSource

    eventSource.onopen = () => {
      setIsConnected(true)
      setError(null)
    }

    eventSource.addEventListener('output', (event) => {
      try {
        const data: OutputEvent = JSON.parse(event.data)
        const logLine: LogLine = {
          line: data.line,
          stream: data.stream,
          timestamp: data.timestamp,
        }

        if (activeTab === 'combined') {
          setCombinedLogs((prev) => [...prev, logLine])
        } else if (activeTab === 'stdout') {
          setStdoutLogs((prev) => [...prev, logLine])
        } else {
          setStderrLogs((prev) => [...prev, logLine])
        }
      } catch (e) {
        console.error('Failed to parse SSE event:', e)
      }
    })

    eventSource.addEventListener('eof', () => {
      eventSource.close()
      setIsConnected(false)
    })

    eventSource.onerror = () => {
      setError('Failed to connect to log stream')
      setIsConnected(false)
      eventSource.close()
    }

    return () => {
      eventSource.close()
    }
  }, [daemonUrl, pid, activeTab])

  const handleTabChange = (_: React.SyntheticEvent, newValue: OutputTab) => {
    setActiveTab(newValue)
  }

  const currentLogs =
    activeTab === 'combined'
      ? combinedLogs
      : activeTab === 'stdout'
        ? stdoutLogs
        : stderrLogs

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          mb: 1,
        }}
      >
        <Typography variant="h6">
          Process Output (PID: {pid})
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            sx={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              backgroundColor: isConnected ? 'success.main' : 'grey.500',
            }}
          />
          <Typography variant="caption" color="text.secondary">
            {isConnected ? 'Connected' : 'Disconnected'}
          </Typography>
        </Box>
      </Box>

      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={activeTab} onChange={handleTabChange}>
          <Tab label="Combined" value="combined" />
          <Tab label="Stdout" value="stdout" />
          <Tab label="Stderr" value="stderr" />
        </Tabs>
      </Box>

      {error && (
        <Typography color="error" sx={{ mt: 1 }}>
          {error}
        </Typography>
      )}

      <Paper
        ref={logsContainerRef}
        sx={{
          mt: 1,
          p: 2,
          height: 400,
          overflow: 'auto',
          backgroundColor: '#1e1e1e',
          fontFamily: 'monospace',
          fontSize: '0.85rem',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
        }}
      >
        {currentLogs.length === 0 ? (
          <Typography color="grey.500" sx={{ fontFamily: 'monospace' }}>
            {isConnected ? 'Waiting for output...' : 'No output available'}
          </Typography>
        ) : (
          currentLogs.map((log, index) => (
            <Box
              key={index}
              sx={{
                color: log.stream === 'stderr' ? '#ff6b6b' : '#e0e0e0',
                lineHeight: 1.4,
              }}
            >
              {activeTab === 'combined' && log.stream && (
                <Typography
                  component="span"
                  sx={{
                    color: log.stream === 'stderr' ? '#ff6b6b' : '#6bbf6b',
                    fontWeight: 'bold',
                    mr: 1,
                    fontSize: '0.75rem',
                  }}
                >
                  [{log.stream}]
                </Typography>
              )}
              {log.line}
            </Box>
          ))
        )}
      </Paper>
    </Box>
  )
}

export default ProcessLogsPanel
