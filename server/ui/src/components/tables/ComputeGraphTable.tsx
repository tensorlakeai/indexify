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
} from '@mui/material'
import { ComputeGraph } from '../../types'
import CopyText from '../CopyText'

interface ComputeGraphTableProps {
  graphData: ComputeGraph
  namespace: string
}

interface RowData {
  name: string
  type: 'compute_fn' | 'dynamic_router'
  fn_name: string
  description: string
  dependencies: string[]
}

const TYPE_COLORS = {
  compute_fn: 'primary',
  dynamic_router: 'secondary',
} satisfies Record<RowData['type'], 'primary' | 'secondary'>

const CELL_STYLES = { fontSize: 14, pt: 1 } as const
const CHIP_STYLES = {
  height: '16px',
  width: 'fit-content',
  fontSize: '0.625rem',
  '& .MuiChip-label': { padding: '0 6px' },
} as const

const TABLE_CONTAINER_STYLES = {
  borderRadius: '8px',
  mt: 2,
  boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
} as const

const TABLE_HEADERS = ['Node Name', 'Out Edges', 'Description'] as const

function ComputeGraphTable({ graphData, namespace }: ComputeGraphTableProps) {
  // each node will be rendered as a row in the table
  const nodes = Object.entries(graphData.nodes).map(([nodeName, node]) => ({
    name: nodeName,
    type: 'compute_fn',
    fn_name: node.fn_name,
    description: node.description,
    dependencies: graphData.edges[nodeName] || [],
  }))

  return (
    <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
      <Table sx={{ minWidth: 650 }} aria-label="compute graph table">
        <TableHead>
          <TableRow>
            {TABLE_HEADERS.map((header) => (
              <TableCell key={header} sx={CELL_STYLES}>
                {header}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {nodes
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((row) => (
              <TableRow key={row.name}>
                <TableCell sx={{ pt: 2 }}>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'left',
                      flexDirection: 'column',
                    }}
                  >
                    <Box display="flex" flexDirection="row" alignItems="center">
                      {row.name}
                      <CopyText text={row.name} />
                    </Box>
                    <Chip
                      label={row.type}
                      color={
                        TYPE_COLORS[row.type as keyof typeof TYPE_COLORS] ??
                        'default'
                      }
                      size="small"
                      sx={CHIP_STYLES}
                    />
                  </Box>
                </TableCell>
                <TableCell sx={{ pt: 2 }}>
                  {row.dependencies.map((dep) => (
                    <Chip
                      key={`${row.name}-${dep}`}
                      label={dep}
                      size="small"
                      sx={{ mr: 0.5 }}
                    />
                  ))}
                </TableCell>
                <TableCell sx={{ pt: 2 }}>{row.description}</TableCell>
              </TableRow>
            ))}
        </TableBody>
      </Table>
    </TableContainer>
  )
}

export default ComputeGraphTable
