import React from "react";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Alert, Chip, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import MemoryIcon from "@mui/icons-material/MemoryOutlined";
import { Extractor } from "getindexify";

const ExtractorsTable = ({ extractors }: { extractors: Extractor[] }) => {
  const columns: GridColDef[] = [
    { field: "name", headerName: "Name", width: 300 },
    { field: "description", headerName: "Description", width: 300 },
    // {
    //   field: "input_mime_types",
    //   headerName: "Input MimeTypes",
    //   width: 300,
    //   renderCell: (params) => {
    //     return (
    //       <Box sx={{ overflowX: "scroll" }}>
    //         <Stack gap={1} direction="row">
    //           {(params.value ?? []).map((val: string) => {
    //             return (
    //               <Chip
    //                 key={val}
    //                 label={val}
    //                 sx={{ backgroundColor: "#4AA4F4", color: "white" }}
    //               />
    //             );
    //           })}
    //         </Stack>
    //       </Box>
    //     );
    //   },
    // },
    {
      field: "input_params",
      headerName: "Input Parameters",
      width: 300,
      valueGetter: (params) => {
        return params.value?.properties;
      },
      renderCell: (params) => {
        if (!params.value) {
          return <Typography variant="body1">None</Typography>;
        }
        return (
          <Box sx={{ overflowX: "scroll" }}>
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val: string) => {
                return (
                  <Chip key={val} label={`${val}:${params.value[val].type}`} />
                );
              })}
            </Stack>
          </Box>
        );
      },
    },
    {
      field: "outputs",
      headerName: "Outputs",
      width: 300,
      valueGetter: (params) => {
        return params.value ?? {};
      },
      renderCell: (params) => {
        if (!params.value) {
          return <Typography variant="body1">None</Typography>;
        }
        return (
          <Box overflow="scroll">
            <Stack gap={1} direction="row" overflow="scroll">
              {Object.keys(params.value).map((val: string) => {
                return <Chip key={val} label={val} />;
              })}
            </Stack>
          </Box>
        );
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
        <Typography variant="h3">Extractors</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractorsTable;
