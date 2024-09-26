import React, { useMemo } from 'react';
import { Paper, Stack, Table as MuiTable, TableBody as MuiTableBody, TableCell as MuiTableCell, TableContainer as MuiTableContainer, TableHead as MuiTableHead, TableRow as MuiTableRow, Typography } from '@mui/material';
import { TableVirtuoso, TableComponents } from 'react-virtuoso';
import { useLoaderData } from 'react-router-dom';
import { StateChange } from '../../types';
import { Cpu } from 'iconsax-react';
import { formatTimestamp } from '../../utils/helpers';

const StateChangesPage = () => {
  const { stateChanges } = useLoaderData() as {
    stateChanges: StateChange[];
  };

  const columns = useMemo(() => [
    { key: 'id', label: 'ID' },
    { key: 'change_type', label: 'Change Type' },
    { key: 'object_id', label: 'Object ID' },
    { 
      key: 'created_at', 
      label: 'Created At',
      format: formatTimestamp
    },
    { 
      key: 'processed_at', 
      label: 'Processed At',
      format: formatTimestamp
    },
  ], []);

  const rows = useMemo(() => stateChanges, [stateChanges]);

  const VirtuosoTableComponents: TableComponents<StateChange> = {
    Scroller: React.forwardRef<HTMLDivElement>((props, ref) => (
      <MuiTableContainer component={Paper} {...props} ref={ref} />
    )),
    Table: (props) => <MuiTable {...props} style={{ borderCollapse: 'separate', tableLayout: 'fixed' }} />,
    TableHead: MuiTableHead,
    TableRow: ({ item: _item, ...props }) => <MuiTableRow {...props} />,
    TableBody: React.forwardRef<HTMLTableSectionElement>((props, ref) => <MuiTableBody {...props} ref={ref} />),
  };

  return (
    <>
      <Stack
        display={'flex'}
        direction={'row'}
        alignItems={'center'}
        spacing={2}
      >
        <div className="heading-icon-container">
          <Cpu size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          System Events
        </Typography>
      </Stack>
      <Paper style={{ height: '100vh', width: '100%', marginTop: 10, boxShadow: "0px 0px 3px -1px #6FA8EA inset" }} elevation={0}>
        <TableVirtuoso
          data={rows}
          components={VirtuosoTableComponents}
          fixedHeaderContent={() => (
            <MuiTableRow>
              {columns.map((column) => (
                <MuiTableCell key={column.key} style={{ fontWeight: 500 }}>
                  {column.label}
                </MuiTableCell>
              ))}
            </MuiTableRow>
          )}
          itemContent={(_index, row) => (
            <>
              {columns.map((column) => (
                <MuiTableCell key={column.key}>
                  {column.format
                    ? column.format(row[column.key as keyof StateChange])
                    : row[column.key as keyof StateChange]?.toString() ?? 'N/A'}
                </MuiTableCell>
              ))}
            </>
          )}
        />
      </Paper>
    </>
  );
};

export default StateChangesPage;
