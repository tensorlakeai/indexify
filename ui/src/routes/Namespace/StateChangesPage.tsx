import React, { useMemo } from 'react';
import { Paper, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from '@mui/material';
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
      <TableContainer component={Paper} {...props} ref={ref} />
    )),
    Table: (props) => <Table {...props} style={{ borderCollapse: 'separate', tableLayout: 'fixed' }} />,
    TableHead,
    TableRow: ({ item: _item, ...props }) => <TableRow {...props} />,
    TableBody: React.forwardRef<HTMLTableSectionElement>((props, ref) => <TableBody {...props} ref={ref} />),
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
          System Changes
        </Typography>
      </Stack>
      <Paper style={{ height: '100vh', width: '100%', marginTop: 10, boxShadow: "0px 0px 3px -1px #6FA8EA inset" }} elevation={0}>
        <TableVirtuoso
          data={rows}
          components={VirtuosoTableComponents}
          fixedHeaderContent={() => (
            <TableRow>
              {columns.map((column) => (
                <TableCell key={column.key} style={{ fontWeight: 500 }}>
                  {column.label}
                </TableCell>
              ))}
            </TableRow>
          )}
          itemContent={(_index, row) => (
            <>
              {columns.map((column) => (
                <TableCell key={column.key}>
                  {column.format
                    ? column.format(row[column.key as keyof StateChange])
                    : row[column.key as keyof StateChange]?.toString() ?? 'N/A'}
                </TableCell>
              ))}
            </>
          )}
        />
      </Paper>
    </>
  );
};

export default StateChangesPage;
