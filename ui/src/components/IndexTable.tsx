import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IIndex } from "getindexify";
import { Alert, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import ManageSearchIcon from "@mui/icons-material/ManageSearch";
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
    if (indexes.length === 0) {
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
            sx={{ backgroundColor: "white" }}
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
      <Stack
        display={"flex"}
        direction={"row"}
        alignItems={"center"}
        spacing={2}
      >
        <ManageSearchIcon />
        <Typography variant="h3">Indexes</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default IndexTable;
