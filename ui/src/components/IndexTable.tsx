import { DataGrid, GridColDef, GridRenderCellParams } from "@mui/x-data-grid";
import { IIndex } from "../lib/Indexify/types";
import { Alert, Typography } from "@mui/material";
import { Box } from "@mui/system";
import React from "react";

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

  const renderContent = () => {
    if (indexes.length == 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Indexes Found
          </Alert>
        </Box>
      );
    }
    return (
      <>
        <div
          style={{
            width: "100%",
          }}
        >
          <DataGrid
            autoHeight
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
      <Typography variant="h4">Indexes</Typography>
      {renderContent()}
    </>
  );
};

export default IndexTable;
