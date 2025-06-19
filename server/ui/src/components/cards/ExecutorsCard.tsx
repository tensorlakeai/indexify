import {
  Alert,
  Chip,
  IconButton,
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
import { Box, Stack } from '@mui/system'
import { InfoCircle, SearchNormal1, Setting4 } from 'iconsax-react'
import { useState } from 'react'
import { ExecutorMetadata } from '../../types'
import { FunctionExecutorsContent } from './FunctionExecutorsContent'

interface ExecutorsCardProps {
  executors: ExecutorMetadata[]
}

interface FunctionAllowlistEntry {
  compute_fn: string
  compute_graph: string
  namespace: string
  version: string | null
}

function ExecutorsContent({ executors }: ExecutorsCardProps) {
  const [searchTerms, setSearchTerms] = useState<Record<string, string>>({})

  const filterFunctions = (
    functions: FunctionAllowlistEntry[],
    searchTerm: string
  ) =>
    functions.filter((fn) =>
      Object.values(fn).some((val) =>
        val?.toString().toLowerCase().includes(searchTerm.toLowerCase())
      )
    )

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
            mt: 2,
          }}
          elevation={0}
        >
          <Table sx={{ minWidth: 650 }} aria-label="executors table">
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: 'bold' }}>ID</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Address</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Labels</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Version</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <>
                <TableRow key={executor.id}>
                  <TableCell component="th" scope="row">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {executor.id}
                    </Box>
                  </TableCell>
                  <TableCell>{executor.addr}</TableCell>
                  <TableCell>
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
                  <TableCell>{executor.executor_version}</TableCell>
                </TableRow>
                {executor.function_allowlist &&
                  executor.function_allowlist.length > 0 && (
                    <TableRow>
                      <TableCell
                        style={{ paddingBottom: 0, paddingTop: 0 }}
                        colSpan={5}
                      >
                        <Box sx={{ margin: 2 }}>
                          <Box
                            sx={{
                              mb: 2,
                              display: 'flex',
                              justifyContent: 'flex-end',
                            }}
                          >
                            <TextField
                              size="small"
                              placeholder="Search functions..."
                              value={searchTerms[executor.id] || ''}
                              onChange={(e) =>
                                setSearchTerms((prev) => ({
                                  ...prev,
                                  [executor.id]: e.target.value,
                                }))
                              }
                              InputProps={{
                                startAdornment: (
                                  <InputAdornment position="start">
                                    <SearchNormal1 size={20} />
                                  </InputAdornment>
                                ),
                              }}
                            />
                          </Box>
                          <Table
                            size="small"
                            sx={{
                              border: 1,
                              borderColor: 'divider',
                              borderRadius: 10,
                              '& td, & th': {
                                borderBottom: 1,
                                borderColor: 'divider',
                              },
                            }}
                          >
                            <TableHead>
                              <TableRow>
                                <TableCell>Compute Function</TableCell>
                                <TableCell>Compute Graph</TableCell>
                                <TableCell>Namespace</TableCell>
                                <TableCell>Version</TableCell>
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {filterFunctions(
                                executor.function_allowlist,
                                searchTerms[executor.id] || ''
                              ).map((fn, idx) => (
                                <TableRow key={idx}>
                                  <TableCell>{fn.compute_fn}</TableCell>
                                  <TableCell>{fn.compute_graph}</TableCell>
                                  <TableCell>{fn.namespace}</TableCell>
                                  <TableCell>{fn.version || '-'}</TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </Box>
                      </TableCell>
                    </TableRow>
                  )}

                {executor.function_executors.length > 0 && (
                  <>
                    <TableRow>
                      <TableCell colSpan={4} sx={{ fontWeight: 'bold' }}>
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
                    <TableRow>
                      <TableCell colSpan={4} sx={{ fontWeight: 'bold' }}>
                        Server Only Function Executors
                      </TableCell>
                    </TableRow>
                    {executor.server_only_function_executors.map(
                      (fnExecutor) => (
                        <FunctionExecutorsContent
                          key={fnExecutor.id}
                          functionExecutor={fnExecutor}
                        />
                      )
                    )}
                  </>
                )}
              </>
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
