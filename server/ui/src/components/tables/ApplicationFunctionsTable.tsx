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
import {
  ApplicationFunction,
  NodeResources,
  NodeRetryPolicy,
  Parameter,
  PlacementConstraints,
} from '../../types/types'
import CopyText from '../CopyText'

interface ApplicationFunctionsTableProps {
  applicationFunctions: Record<string, ApplicationFunction>
  namespace: string
}

interface RowData {
  name: string
  description: string
  cache_key?: string | null
  initialization_timeout_sec: number
  max_concurrency: number
  return_type?: string
  timeout_sec: number
  parameters: Parameter[]
  placement_constraints: PlacementConstraints
  resources: NodeResources
  retry_policy: NodeRetryPolicy
}

const CELL_STYLES = { fontSize: 14, pt: 1, border: 'none' } as const

const DETAILS_CELL_STYLES = {
  fontSize: 14,
  pt: 1,
  verticalAlign: 'top' as const,
  border: 'none',
} as const

const TABLE_CONTAINER_STYLES = {
  borderRadius: '8px',
  mt: 2,
  boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset',
} as const

const TABLE_HEADERS = [
  { key: 'name', label: 'Name' },
  { key: 'description', label: 'Description' },
  { key: 'cache_key', label: 'Cache Key' },
  { key: 'initialization_timeout_sec', label: 'Init Timeout (sec)' },
  { key: 'max_concurrency', label: 'Max Concurrency' },
  { key: 'return_type', label: 'Return Type' },
  { key: 'timeout_sec', label: 'Timeout (sec)' },
] as const

const formatDataType = (dataType: any): string => {
  if (typeof dataType === 'string') {
    return dataType
  }

  if (typeof dataType === 'object' && dataType !== null) {
    if (dataType.type) {
      return String(dataType.type)
    }
    return JSON.stringify(dataType)
  }

  return String(dataType)
}

function ApplicationFunctionsTable({
  applicationFunctions,
}: ApplicationFunctionsTableProps) {
  const renderParameters = (parameters: Parameter[]) => {
    if (!parameters || parameters.length === 0) return 'None'
    return (
      <Box>
        {parameters.map((param, index) => (
          <Chip
            key={index}
            label={`${param.name}: ${formatDataType(param.data_type)}${
              param.required ? ' *' : ''
            }`}
            size="small"
            sx={{ mr: 0.5, mb: 0.5 }}
            title={param.description}
          />
        ))}
      </Box>
    )
  }

  const renderPlacementConstraints = (constraints: PlacementConstraints) => {
    if (
      !constraints.filter_expressions ||
      constraints.filter_expressions.length === 0
    ) {
      return 'None'
    }
    return (
      <Box>
        {constraints.filter_expressions.map((expr, index) => (
          <Chip
            key={index}
            label={expr}
            size="small"
            sx={{ mr: 0.5, mb: 0.5 }}
          />
        ))}
      </Box>
    )
  }

  const renderResources = (resources: NodeResources) => {
    return (
      <Box>
        <Typography variant="caption" display="block">
          CPU: {resources.cpus}
        </Typography>
        <Typography variant="caption" display="block">
          Memory: {resources.memory_mb} MB
        </Typography>
        <Typography variant="caption" display="block">
          Disk: {resources.ephemeral_disk_mb} MB
        </Typography>
        {resources.gpus && resources.gpus.length > 0 && (
          <Typography variant="caption" display="block">
            GPU:{' '}
            {resources.gpus
              .map((gpu) => `${gpu.count}x ${gpu.model}`)
              .join(', ')}
          </Typography>
        )}
      </Box>
    )
  }

  const renderRetryPolicy = (policy: NodeRetryPolicy) => {
    return (
      <Box>
        <Typography variant="caption" display="block">
          Max Retries: {policy.max_retries}
        </Typography>
        <Typography variant="caption" display="block">
          Initial Delay: {policy.initial_delay_sec}s
        </Typography>
        <Typography variant="caption" display="block">
          Max Delay: {policy.max_delay_sec}s
        </Typography>
        <Typography variant="caption" display="block">
          Multiplier: {policy.delay_multiplier}
        </Typography>
      </Box>
    )
  }
  const applicationFunctionRows: RowData[] = Object.entries(
    applicationFunctions
  ).map(([functionName, func]) => ({
    name: String(functionName),
    description: String(func.description || ''),
    cache_key: func.cache_key ? String(func.cache_key) : null,
    initialization_timeout_sec: Number(func.initialization_timeout_sec),
    max_concurrency: Number(func.max_concurrency),
    return_type: func.return_type
      ? formatDataType(func.return_type)
      : undefined,
    timeout_sec: Number(func.timeout_sec),
    parameters: func.parameters,
    placement_constraints: func.placement_constraints,
    resources: func.resources,
    retry_policy: func.retry_policy,
  }))

  return (
    <TableContainer component={Paper} sx={TABLE_CONTAINER_STYLES}>
      <Table sx={{ minWidth: 650 }} aria-label="compute graph table">
        <TableHead>
          <TableRow>
            {TABLE_HEADERS.map((header) => (
              <TableCell
                key={header.key}
                sx={{ ...CELL_STYLES, fontWeight: 'bold' }}
              >
                {header.label}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {applicationFunctionRows
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((row) => (
              <>
                <TableRow
                  key={row.name}
                  sx={{
                    '& td': {
                      border: 'none',
                      borderTop: '1px solid rgba(224, 224, 224, 1)',
                    },
                  }}
                >
                  <TableCell sx={CELL_STYLES}>
                    <Box display="flex" flexDirection="row" alignItems="center">
                      {row.name}
                      <CopyText text={row.name} />
                    </Box>
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.description || 'N/A')}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.cache_key || 'N/A')}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.initialization_timeout_sec)}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.max_concurrency)}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.return_type || 'N/A')}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}>
                    {String(row.timeout_sec)}
                  </TableCell>
                </TableRow>

                <TableRow
                  key={row.name + '-details'}
                  sx={{ '& td': { border: 'none' } }}
                >
                  <TableCell sx={CELL_STYLES}></TableCell>
                  <TableCell sx={DETAILS_CELL_STYLES}>
                    <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                      Parameters: required*
                    </Typography>
                    <br />
                    {renderParameters(row.parameters)}
                  </TableCell>
                  <TableCell sx={DETAILS_CELL_STYLES}>
                    <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                      Placement Constraints:
                    </Typography>
                    <br />
                    {renderPlacementConstraints(row.placement_constraints)}
                  </TableCell>
                  <TableCell sx={DETAILS_CELL_STYLES}>
                    <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                      Resources:
                    </Typography>
                    <br />
                    {renderResources(row.resources)}
                  </TableCell>
                  <TableCell sx={DETAILS_CELL_STYLES}>
                    <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                      Retry Policy:
                    </Typography>
                    <br />
                    {renderRetryPolicy(row.retry_policy)}
                  </TableCell>
                  <TableCell sx={CELL_STYLES}></TableCell>
                  <TableCell sx={CELL_STYLES}></TableCell>
                </TableRow>
              </>
            ))}
        </TableBody>
      </Table>
    </TableContainer>
  )
}

export default ApplicationFunctionsTable
