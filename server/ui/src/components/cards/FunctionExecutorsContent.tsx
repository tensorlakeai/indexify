import { Table, TableCell, TableRow } from '@mui/material'
import { stateColorMap } from '../../theme'
import { FunctionExecutorMetadata } from '../../types/types'

export function FunctionExecutorsContent({
  functionExecutor,
}: {
  functionExecutor: FunctionExecutorMetadata
}) {
  return (
    <Table>
      <TableRow sx={{ width: '100%' }}>
        <TableCell
          sx={{ verticalAlign: 'top', fontSize: '0.90rem', width: '50%' }}
        >
          <p>
            <strong>ID:</strong> {functionExecutor.id}
          </p>
          <p>
            <strong>Namespace:</strong> {functionExecutor.namespace}
          </p>
          <p>
            <strong>Compute Graph:</strong>{' '}
            {functionExecutor.compute_graph_name}
          </p>
          <p>
            <strong>Compute Function Name:</strong>{' '}
            {functionExecutor.compute_fn_name}
          </p>
        </TableCell>
        <TableCell
          sx={{ verticalAlign: 'top', fontSize: '0.90rem', width: '50%' }}
        >
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
    </Table>
  )
}
