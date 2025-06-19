import { TableCell, TableRow } from '@mui/material'
import { FunctionExecutorMetadata } from '../../types'

export function FunctionExecutorsContent({
  functionExecutor,
}: {
  functionExecutor: FunctionExecutorMetadata
}) {
  return (
    <TableRow>
      <TableCell colSpan={2} sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}>
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
      <TableCell colSpan={2} sx={{ verticalAlign: 'top', fontSize: '0.90rem' }}>
        <p>
          <strong>Version:</strong> {functionExecutor.version}
        </p>
        <p>
          {/* TODO: running green pill */}
          <strong>State:</strong> {functionExecutor.state}
        </p>
        <p>
          <strong>Desired State:</strong> {functionExecutor.desired_state}
        </p>
      </TableCell>
    </TableRow>
  )
}
