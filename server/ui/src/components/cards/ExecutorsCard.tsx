import {
  Alert,
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
import { Box, Stack } from '@mui/system'
import { InfoCircle, Setting4 } from 'iconsax-react'
import { stateBackgroundColorMap, stateColorMap } from '../../theme'
import { ExecutorMetadata } from '../../types'
import DisplayResourceContent from './DisplayResourceContent'
import { FunctionExecutorsContent } from './FunctionExecutorsContent'

interface ExecutorsCardProps {
  executors: ExecutorMetadata[]
}

function ExecutorsContent({ executors }: ExecutorsCardProps) {
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
                  {Object.entries(executor.host_resources).map(
                    ([key, value]) => (
                      <DisplayResourceContent keyName={key} value={value} />
                    )
                  )}
                </TableCell>
                <TableCell
                  colSpan={1}
                  sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}
                >
                  {Object.entries(executor.free_resources).map(
                    ([key, value]) => (
                      <DisplayResourceContent keyName={key} value={value} />
                    )
                  )}
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
                      <TableCell colSpan={2} sx={{ fontWeight: 'bold' }}>
                        Function Allowlist
                      </TableCell>
                    </TableRow>
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
                            sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}
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
                            sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}
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
                  </>
                )}

              {executor.function_executors.length > 0 && (
                <>
                  <TableRow
                    sx={{
                      backgroundColor: '#eeeeee',
                      border: '1px solid #ccc',
                    }}
                  >
                    <TableCell colSpan={2} sx={{ fontWeight: 'bold' }}>
                      Function Executors
                    </TableCell>
                  </TableRow>
                  {executor.function_executors.map((fnExecutor) => (
                    <FunctionExecutorsContent
                      key={fnExecutor.id}
                      functionExecutor={fnExecutor}
                    />
                  ))}
                </>
              )}

              {executor.server_only_function_executors.length > 0 && (
                <>
                  <TableRow
                    sx={{
                      backgroundColor: '#eeeeee',
                      border: '1px solid #ccc',
                    }}
                  >
                    <TableCell colSpan={2} sx={{ fontWeight: 'bold' }}>
                      Server Only Function Executors
                    </TableCell>
                  </TableRow>
                  {executor.server_only_function_executors.map((fnExecutor) => (
                    <FunctionExecutorsContent
                      key={fnExecutor.id}
                      functionExecutor={fnExecutor}
                    />
                  ))}
                </>
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
