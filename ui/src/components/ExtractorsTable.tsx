import React from "react";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Alert, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import MemoryIcon from "@mui/icons-material/MemoryOutlined";
import Extractor from "../lib/Indexify/extractor";

const ExtractorsTable = ({ extractors }: { extractors: Extractor[] }) => {
  const columns: GridColDef[] = [
    { field: "name", headerName: "Name", width: 300 },
    { field: "description", headerName: "Description", width: 300 },
    {
      field: "input_params",
      headerName: "Input Parameters",
      width: 300,
      valueGetter: (params) => {
        return JSON.stringify(params.value);
      },
    },
    {
      field: "outputs",
      headerName: "Outputs",
      width: 300,
      valueGetter: (params) => {
        return JSON.stringify(params.row.input_params);
      },
    },
  ];

  const getRowId = (row: Extractor) => {
    return row.name;
  };

  const renderContent = () => {
    if (extractors.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Extractors Found
          </Alert>
        </Box>
      );
    }
    return (
      <Box
        sx={{
          width: "100%",
        }}
      >
        <DataGrid
          sx={{ backgroundColor: "white" }}
          autoHeight
          getRowId={getRowId}
          rows={extractors}
          columns={columns}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 5 },
            },
          }}
          pageSizeOptions={[5, 10]}
        />
      </Box>
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
        <MemoryIcon />
        <Typography variant="h3">Extractor Bindings</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractorsTable;
