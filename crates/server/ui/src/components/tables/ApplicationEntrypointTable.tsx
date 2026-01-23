import {
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import { EntryPointManifest } from '../../types/types'
import CopyText from '../CopyText'

interface ApplicationEntrypointTableProps {
  entrypoint?: EntryPointManifest | null
  namespace: string
}

const CELL_STYLES = { fontSize: 14, pt: 1, border: 'none' } as const

const TABLE_CONTAINER_STYLES = {
  borderRadius: '8px',
  mt: 2,
  boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
} as const

function ApplicationEntrypointTable({
  entrypoint,
}: ApplicationEntrypointTableProps) {
  // Don't render if no entrypoint
  if (!entrypoint) {
    return null
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Application Entrypoint
      </Typography>
      <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
        <Table sx={{ minWidth: 650 }} aria-label="application entrypoint table">
          <TableHead>
            <TableRow>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Function Name
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Input Serializer
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Output Serializer
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Output Type Hints
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <TableRow
              sx={{
                '& td': {
                  border: 'none',
                  borderTop: '1px solid rgba(224, 224, 224, 1)',
                },
              }}
            >
              <TableCell sx={CELL_STYLES}>
                <Box display="flex" flexDirection="row" alignItems="center">
                  {entrypoint.function_name}
                  <CopyText text={entrypoint.function_name} />
                </Box>
              </TableCell>
              <TableCell sx={CELL_STYLES}>
                <Box display="flex" flexDirection="row" alignItems="center">
                  {entrypoint.input_serializer}
                </Box>
              </TableCell>
              <TableCell sx={CELL_STYLES}>
                <Box display="flex" flexDirection="row" alignItems="center">
                  {entrypoint.output_serializer}
                </Box>
              </TableCell>
              <TableCell sx={CELL_STYLES}>
                <Box display="flex" flexDirection="row" alignItems="center">
                  <Typography
                    variant="body2"
                    sx={{
                      fontFamily: 'monospace',
                      wordBreak: 'break-all',
                      overflowWrap: 'break-word',
                      fontSize: '0.75rem',
                      flex: 1,
                    }}
                  >
                    {entrypoint.output_type_hints_base64
                      ? `${entrypoint.output_type_hints_base64.substring(
                          0,
                          32
                        )}...`
                      : 'N/A'}
                  </Typography>
                  {entrypoint.output_type_hints_base64 && (
                    <CopyText text={entrypoint.output_type_hints_base64} />
                  )}
                </Box>
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default ApplicationEntrypointTable
