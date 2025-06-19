import { TableCell, TableRow } from '@mui/material'
import { stateColorMap } from '../../theme'
import { FunctionExecutorMetadata } from '../../types'

export function FunctionExecutorsContent({
  functionExecutor,
}: {
  functionExecutor: FunctionExecutorMetadata
}) {
  return (
    <TableRow>
      <TableCell colSpan={1} sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}>
        <p>
          <strong>ID:</strong> {functionExecutor.id}
        </p>
        <p>
          <strong>Namespace:</strong> {functionExecutor.namespace}
        </p>
        <p>
          <strong>Compute Graph:</strong> {functionExecutor.compute_graph_name}
        </p>
      </TableCell>
      <TableCell colSpan={1} sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}>
        <p>
          <strong>Version:</strong> {functionExecutor.version}
        </p>
        <p>
          <strong>State:</strong>{' '}
          <span
            style={{
              color: stateColorMap[functionExecutor.state],
              fontWeight: 'bold',
            }}
          >
            {functionExecutor.state}
          </span>
        </p>
        <p>
          <strong>Desired State:</strong>{' '}
          <span
            style={{
              color: stateColorMap[functionExecutor.desired_state],
              fontWeight: 'bold',
            }}
          >
            {functionExecutor.desired_state}
          </span>
        </p>
      </TableCell>
    </TableRow>
  )
}
