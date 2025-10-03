import {
  Box,
  Chip,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import CopyText from '../CopyText'

interface ApplicationTagsTableProps {
  tags: Record<string, string>
  namespace: string
}

const CELL_STYLES = { fontSize: 14, pt: 1, border: 'none' } as const

const TABLE_CONTAINER_STYLES = {
  borderRadius: '8px',
  mt: 2,
  boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
} as const

function ApplicationTagsTable({ tags }: ApplicationTagsTableProps) {
  const tagEntries = Object.entries(tags)

  // Don't render if no tags
  if (tagEntries.length === 0) {
    return null
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Application Tags
      </Typography>
      <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
        <Table sx={{ minWidth: 650 }} aria-label="application tags table">
          <TableHead>
            <TableRow>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Key
              </TableCell>
              <TableCell sx={{ ...CELL_STYLES, fontWeight: 'bold' }}>
                Value
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tagEntries.map(([key, value]) => (
              <TableRow
                key={key}
                sx={{
                  '& td': {
                    border: 'none',
                    borderTop: '1px solid rgba(224, 224, 224, 1)',
                  },
                }}
              >
                <TableCell sx={CELL_STYLES}>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Chip
                      label={key}
                      size="small"
                      variant="outlined"
                      color="primary"
                    />
                    <CopyText text={key} />
                  </Box>
                </TableCell>
                <TableCell sx={CELL_STYLES}>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Typography
                      variant="body2"
                      sx={{
                        wordBreak: 'break-all',
                        overflowWrap: 'break-word',
                        flex: 1,
                      }}
                    >
                      {value}
                    </Typography>
                    <CopyText text={value} />
                  </Box>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default ApplicationTagsTable
