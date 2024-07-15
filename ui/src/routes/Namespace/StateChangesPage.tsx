import React, { useMemo } from 'react';
import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material';
import { TableVirtuoso, TableComponents } from 'react-virtuoso';
import { useLoaderData } from 'react-router-dom';
import { StateChange } from '../../types';

const StateChangesPage = () => {
  const { stateChanges } = useLoaderData() as {
    stateChanges: StateChange[];
  };

  const columns = useMemo(() => [
    { key: 'id', label: 'ID' },
    { key: 'change_type', label: 'Change Type' },
    { key: 'object_id', label: 'Object ID' },
    { key: 'created_at', label: 'Created At' },
    { key: 'processed_at', label: 'Processed At' },
    // { key: 'refcnt_object_id', label: 'Refcnt Object ID' },
  ], []);

  const rows = useMemo(() => stateChanges, [stateChanges]);

  const VirtuosoTableComponents: TableComponents<StateChange> = {
    Scroller: React.forwardRef<HTMLDivElement>((props, ref) => (
      <TableContainer component={Paper} {...props} ref={ref} />
    )),
    Table: (props) => <Table {...props} style={{ borderCollapse: 'separate', tableLayout: 'fixed' }} />,
    TableHead,
    TableRow: ({ item: _item, ...props }) => <TableRow {...props} />,
    TableBody: React.forwardRef<HTMLTableSectionElement>((props, ref) => <TableBody {...props} ref={ref} />),
  };

  return (
    <Paper style={{ height: '100vh', width: '100%' }}>
      <TableVirtuoso
        data={rows}
        components={VirtuosoTableComponents}
        fixedHeaderContent={() => (
          <TableRow>
            {columns.map((column) => (
              <TableCell key={column.key} style={{ fontWeight: 'bold' }}>
                {column.label}
              </TableCell>
            ))}
          </TableRow>
        )}
        itemContent={(_index, row) => (
          <>
            {columns.map((column) => (
              <TableCell key={column.key}>
                {row[column.key as keyof StateChange]?.toString() ?? 'N/A'}
              </TableCell>
            ))}
          </>
        )}
      />
    </Paper>
  );
};

export default StateChangesPage;
