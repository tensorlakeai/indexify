import { useState } from 'react'
import {
  Alert,
  IconButton,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Collapse,
  TextField,
  InputAdornment,
} from '@mui/material'
import { Box, Stack } from '@mui/system'
import {
  Setting4,
  InfoCircle,
  ArrowDown2,
  ArrowUp2,
  SearchNormal1,
} from 'iconsax-react'
import { ExecutorMetadata } from '../../types'

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
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({})
  const [searchTerms, setSearchTerms] = useState<Record<string, string>>({})

  const toggleRow = (id: string) =>
    setExpandedRows((prev) => ({ ...prev, [id]: !prev[id] }))

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
    <TableContainer component={Paper} sx={{ mt: 2 }} elevation={0}>
      <Table sx={{ minWidth: 650 }} aria-label="executors table">
        <TableHead>
          <TableRow>
            <TableCell padding="checkbox" />
            <TableCell>ID</TableCell>
            <TableCell>Address</TableCell>
            <TableCell>Functions</TableCell>
            <TableCell>Labels</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {executors.map((executor) => (
            <>
              <TableRow key={executor.id}>
                <TableCell padding="checkbox">
                  {executor.function_allowlist &&
                  executor.function_allowlist.length > 0 ? (
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
                  ) : null}
                </TableCell>
                <TableCell component="th" scope="row">
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {executor.id}
                  </Box>
                </TableCell>
                <TableCell>{executor.addr}</TableCell>
                <TableCell>
                  <Typography noWrap>
                    {executor.function_allowlist
                      ? executor.function_allowlist.length
                      : 0}{' '}
                    functions
                  </Typography>
                </TableCell>
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
              </TableRow>
              {executor.function_allowlist &&
                executor.function_allowlist.length > 0 && (
                  <TableRow>
                    <TableCell
                      style={{ paddingBottom: 0, paddingTop: 0 }}
                      colSpan={5}
                    >
                      <Collapse
                        in={expandedRows[executor.id]}
                        timeout="auto"
                        unmountOnExit
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
                      </Collapse>
                    </TableCell>
                  </TableRow>
                )}
            </>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
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
