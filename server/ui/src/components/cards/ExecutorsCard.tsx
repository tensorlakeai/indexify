import {
  Alert,
  Box,
  Chip,
  Collapse,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import { ArrowDown2, ArrowUp2, InfoCircle, Setting4 } from 'iconsax-react'
import { useState } from 'react'
import { stateBackgroundColorMap, stateColorMap } from '../../theme'
import { ExecutorMetadata } from '../../types/types'
import DisplayResourceContent from './DisplayResourceContent'
import { FunctionExecutorsContent } from './FunctionExecutorsContent'

interface ExecutorsCardProps {
  executors: ExecutorMetadata[]
}

function ExecutorsContent({ executors }: ExecutorsCardProps) {
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({})

  const toggleRow = (id: string) =>
    setExpandedRows((prev) => ({ ...prev, [id]: !prev[id] }))

  if (!executors?.length)
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Executors Found
        </Alert>
      </Box>
    )

  return (
    <>
      {executors.map((executor) => (
        <TableContainer
          component={Paper}
          sx={{
            boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
            mt: 3,
            mb: 6,
          }}
          elevation={0}
          key={executor.id}
        >
          <Table sx={{ minWidth: 650 }} aria-label="executors table">
            <TableHead>
              <TableRow
                sx={{
                  backgroundColor: stateBackgroundColorMap[executor.state],
                  border: '1px solid #ccc',
                }}
              >
                <TableCell sx={{ fontWeight: 'bold' }} colSpan={2}>
                  Executor Details
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <TableRow>
                <TableCell
                  colSpan={1}
                  sx={{
                    verticalAlign: 'top',
                    fontSize: '0.90rem',
                    width: '50%',
                  }}
                >
                  <p>
                    <strong>ID:</strong> {executor.id}
                  </p>
                  <p>
                    <strong>Address:</strong> {executor.addr}
                  </p>
                  <p>
                    <strong>Version:</strong> {executor.executor_version}
                  </p>
                  <p>
                    <strong>Clock:</strong> {executor.clock}
                  </p>
                  <p>
                    <strong>Tombstoned:</strong>{' '}
                    {executor.tombstoned ? 'Yes' : 'No'}
                  </p>
                </TableCell>
                <TableCell
                  colSpan={1}
                  sx={{
                    verticalAlign: 'top',
                    fontSize: '0.90rem',
                    width: '50%',
                  }}
                >
                  <p>
                    <strong>State:</strong>{' '}
                    <span
                      style={{
                        color: stateColorMap[executor.state],
                        fontWeight: 'bold',
                      }}
                    >
                      {executor.state}
                    </span>
                  </p>
                  <p>
                    <strong>State Hash:</strong>
                    <br />
                    <span
                      style={{ fontSize: '0.85rem', wordBreak: 'break-all' }}
                    >
                      {executor.state_hash}
                    </span>
                  </p>
                  <p>
                    <strong>Labels:</strong>
                  </p>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {Object.entries(executor.labels).map(([key, value]) => (
                      <Chip
                        key={key}
                        label={`${key}: ${value}`}
                        variant="outlined"
                        size="small"
                        sx={{
                          height: '20px',
                          '& .MuiChip-label': {
                            padding: '0 6px',
                            fontSize: '0.75rem',
                          },
                          backgroundColor: 'rgba(51, 132, 252, 0.1)',
                          color: 'rgb(51, 132, 252)',
                          borderColor: 'rgba(51, 132, 252, 0.3)',
                        }}
                      />
                    ))}
                  </Box>
                </TableCell>
              </TableRow>

              <TableRow
                sx={{ backgroundColor: '#eeeeee', border: '1px solid #ccc' }}
              >
                <TableCell colSpan={1} sx={{ fontWeight: 'bold' }}>
                  Host Resources
                </TableCell>
                <TableCell colSpan={1} sx={{ fontWeight: 'bold' }}>
                  Free Resources
                </TableCell>
              </TableRow>
              <TableRow key={executor.id + '-resources'}>
                <TableCell
                  colSpan={1}
                  sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}
                >
                  <DisplayResourceContent
                    resourceValue={executor.host_resources}
                  />
                </TableCell>
                <TableCell
                  colSpan={1}
                  sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}
                >
                  <DisplayResourceContent
                    resourceValue={executor.free_resources}
                  />
                </TableCell>
              </TableRow>

              {Array.isArray(executor.function_allowlist) &&
                executor.function_allowlist.length > 0 && (
                  <>
                    <TableRow
                      sx={{
                        backgroundColor: '#eeeeee',
                        border: '1px solid #ccc',
                      }}
                    >
                      <TableCell colSpan={1} sx={{ fontWeight: 'bold' }}>
                        Function Allowlist
                      </TableCell>
                      <TableCell
                        colSpan={1}
                        sx={{ fontWeight: 'bold', textAlign: 'right' }}
                      >
                        <IconButton
                          size="small"
                          onClick={() => toggleRow(executor.id + '-allowlist')}
                        >
                          {expandedRows[executor.id + '-allowlist'] ? (
                            <ArrowUp2 size={16} />
                          ) : (
                            <ArrowDown2 size={16} />
                          )}
                        </IconButton>
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell colSpan={2} sx={{ padding: 0 }}>
                        <Collapse
                          in={expandedRows[executor.id + '-allowlist']}
                          timeout="auto"
                          unmountOnExit
                        >
                          {executor.function_allowlist.map(
                            (functionAllowListEntry) => (
                              <TableRow
                                key={
                                  functionAllowListEntry.compute_fn +
                                  functionAllowListEntry.compute_graph +
                                  functionAllowListEntry.namespace +
                                  '-allowlist'
                                }
                              >
                                <TableCell
                                  colSpan={1}
                                  sx={{
                                    verticalAlign: 'top',
                                    fontSize: '0.90rem',
                                  }}
                                >
                                  <p>
                                    <strong>Namespace:</strong>{' '}
                                    {functionAllowListEntry.namespace}
                                  </p>
                                  <p>
                                    <strong>Compute Function:</strong>{' '}
                                    {functionAllowListEntry.compute_fn}
                                  </p>
                                </TableCell>
                                <TableCell
                                  colSpan={1}
                                  sx={{
                                    verticalAlign: 'top',
                                    fontSize: '0.90rem',
                                  }}
                                >
                                  <p>
                                    <strong>Compute Graph:</strong>{' '}
                                    {functionAllowListEntry.compute_graph}
                                  </p>
                                  <p>
                                    <strong>Version:</strong>{' '}
                                    {functionAllowListEntry.version
                                      ? functionAllowListEntry.version
                                      : '-'}
                                  </p>
                                </TableCell>
                              </TableRow>
                            )
                          )}
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </>
                )}

              {executor.function_executors.length > 0 ? (
                <>
                  <TableRow
                    sx={{
                      backgroundColor: '#eeeeee',
                      border: '1px solid #ccc',
                    }}
                  >
                    <TableCell colSpan={1} sx={{ fontWeight: 'bold' }}>
                      {executor.function_executors.length} Function Executors
                    </TableCell>
                    <TableCell
                      colSpan={1}
                      sx={{ fontWeight: 'bold', textAlign: 'right' }}
                    >
                      <IconButton
                        size="small"
                        onClick={() => toggleRow(executor.id)}
                      >
                        {expandedRows[executor.id] ? (
                          <ArrowUp2 size={16} />
                        ) : (
                          <ArrowDown2 size={16} />
                        )}
                      </IconButton>
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell colSpan={2} sx={{ padding: 0 }}>
                      <Collapse
                        in={expandedRows[executor.id]}
                        timeout="auto"
                        unmountOnExit
                        sx={{ width: '100%' }}
                      >
                        {executor.function_executors.map((fnExecutor) => (
                          <FunctionExecutorsContent
                            key={fnExecutor.id}
                            functionExecutor={fnExecutor}
                          />
                        ))}
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </>
              ) : (
                <TableRow
                  sx={{
                    backgroundColor: '#eeeeee',
                    border: '1px solid #ccc',
                  }}
                >
                  <TableCell colSpan={2} sx={{ fontWeight: 'bold' }}>
                    0 Function Executors
                  </TableCell>
                </TableRow>
              )}

              {executor.server_only_function_executors.length > 0 ? (
                <>
                  <TableRow
                    sx={{
                      backgroundColor: '#eeeeee',
                      border: '1px solid #ccc',
                    }}
                  >
                    <TableCell colSpan={1} sx={{ fontWeight: 'bold' }}>
                      {executor.server_only_function_executors.length} Server
                      Only Function Executors
                    </TableCell>
                    <TableCell
                      colSpan={1}
                      sx={{ fontWeight: 'bold', textAlign: 'right' }}
                    >
                      <IconButton
                        size="small"
                        onClick={() => toggleRow(executor.id + '-server')}
                      >
                        {expandedRows[executor.id + '-server'] ? (
                          <ArrowUp2 size={16} />
                        ) : (
                          <ArrowDown2 size={16} />
                        )}
                      </IconButton>
                    </TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell colSpan={2} sx={{ padding: 0 }}>
                      <Collapse
                        in={expandedRows[executor.id + '-server']}
                        timeout="auto"
                        unmountOnExit
                      >
                        {executor.server_only_function_executors.map(
                          (fnExecutor) => (
                            <FunctionExecutorsContent
                              key={fnExecutor.id}
                              functionExecutor={fnExecutor}
                            />
                          )
                        )}
                      </Collapse>
                    </TableCell>
                  </TableRow>
                </>
              ) : (
                <TableRow
                  sx={{
                    backgroundColor: '#eeeeee',
                    border: '1px solid #ccc',
                  }}
                >
                  <TableCell colSpan={2} sx={{ fontWeight: 'bold' }}>
                    0 Server Only Function Executors
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      ))}
    </>
  )
}

export function ExecutorsCard({ executors }: ExecutorsCardProps) {
  return (
    <>
      <Stack direction="row" alignItems="center" spacing={2}>
        <div className="heading-icon-container">
          <Setting4 size={25} className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Executors
          <IconButton
            href="https://docs.tensorlake.ai/architecture#executors"
            target="_blank"
            rel="noopener noreferrer"
            sx={{ ml: 1 }}
          >
            <InfoCircle size={20} variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      <ExecutorsContent executors={executors} />
    </>
  )
}
