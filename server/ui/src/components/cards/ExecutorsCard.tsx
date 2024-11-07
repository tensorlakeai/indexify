import { Alert, IconButton, Typography, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Chip } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Setting4, InfoCircle } from 'iconsax-react'

interface ExecutorMetadata {
  image_name: string
  id: string
  executor_version: string
  addr: string
  labels: Record<string, string>
}

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
            <TableCell>Image Name</TableCell>
            <TableCell>ID</TableCell>
            <TableCell>Version</TableCell>
            <TableCell>Address</TableCell>
            <TableCell>Labels</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {executors.map((executor) => (
            <TableRow key={executor.image_name}>
              <TableCell component="th" scope="row">
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Setting4 size={16} variant="Outline" />
                  {executor.image_name}
                </Box>
              </TableCell>
              <TableCell>{executor.id}</TableCell>
              <TableCell>{executor.executor_version}</TableCell>
              <TableCell>{executor.addr}</TableCell>
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
