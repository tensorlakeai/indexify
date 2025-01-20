import { Alert, IconButton, Typography, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Chip } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Setting4, InfoCircle } from 'iconsax-react'
import { ExecutorMetadata } from '../../types'

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
    <TableContainer component={Paper} sx={{ mt: 2 }} elevation={0}>
      <Table sx={{ minWidth: 650 }} aria-label="executors table">
        <TableHead>
          <TableRow>
            <TableCell>ID</TableCell>
            <TableCell>Version</TableCell>
            <TableCell>Address</TableCell>
            <TableCell>Allowed Functions</TableCell>
            <TableCell>Labels</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {executors.map((executor) => (
            <TableRow key={executor.id}>
              <TableCell component="th" scope="row">
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Setting4 size={16} variant="Outline" />
                  {executor.id}
                </Box>
              </TableCell>
              <TableCell>{executor.executor_version}</TableCell>
              <TableCell>{executor.addr}</TableCell>
              <TableCell>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {executor.function_allowlist.map((fn, index) => (
                    <Chip
                      key={index}
                      label={`${fn.compute_fn}@${fn.compute_graph}`}
                      variant="outlined"
                      size="small"
                      sx={{
                        backgroundColor: 'rgba(51, 132, 252, 0.1)',
                        color: 'rgb(51, 132, 252)',
                        borderColor: 'rgba(51, 132, 252, 0.3)',
                      }}
                    />
                  ))}
                </Box>
              </TableCell>
              <TableCell>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {Object.entries(executor.labels).map(([key, value]) => (
                    <Chip
                      key={key}
                      label={`${key}: ${value}`}
                      variant="outlined"
                      size="small"
                      sx={{
                        backgroundColor: 'rgba(51, 132, 252, 0.1)',
                        color: 'rgb(51, 132, 252)',
                        borderColor: 'rgba(51, 132, 252, 0.3)',
                      }}
                    />
                  ))}
                </Box>
              </TableCell>
            </TableRow>
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
