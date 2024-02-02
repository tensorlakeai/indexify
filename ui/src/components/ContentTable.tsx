import { DataGrid, GridColDef, GridRenderCellParams } from "@mui/x-data-grid";
import { IIndex } from "../lib/Indexify/types";
import { Alert, Typography } from "@mui/material";
import { Box } from "@mui/system";

const ContentTable = ({ indexes }: { indexes: IIndex[] }) => {
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

  const renderContent = () => {
    if (indexes.length == 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Content Found
          </Alert>
        </Box>
      );
    }
    return (
      <>
        <div style={{ height: 400, width: "100%" }}>
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
        </div>
      </>
    );
  };

  return (
    <>
      <Typography variant="h4">Content</Typography>
      {renderContent()}
    </>
  );
};

export default ContentTable;
