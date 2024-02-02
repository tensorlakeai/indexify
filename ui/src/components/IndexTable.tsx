import { DataGrid, GridColDef, GridRenderCellParams } from "@mui/x-data-grid";
import { IIndex } from "../lib/Indexify/types";
import { Alert } from "@mui/material";

const IndexTable = ({ indexes }: { indexes: IIndex[] }) => {
  const columns: GridColDef[] = [
    {
      field: "name",
      headerName: "Name",
      width: 200,
    },
    {
      field: "schema",
      headerName: "Schema",
      width: 300,
      valueGetter: (params) => {
        return JSON.stringify(params.value);
      },
    },
  ];

  const getRowId = (row: IIndex) => {
    return row.name;
  };

  if (indexes.length == 0) {
    return (
      <Alert variant="outlined" severity="info">
        No Indexes Found
      </Alert>
    );
  }

  return (
    <DataGrid
      getRowId={getRowId}
      rows={indexes}
      columns={columns}
      initialState={{
        pagination: {
          paginationModel: { page: 0, pageSize: 5 },
        },
      }}
      pageSizeOptions={[5, 10]}
    />
  );
};

export default IndexTable;
