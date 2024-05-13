import React from "react";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Alert, Chip, IconButton, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import StorageIcon from "@mui/icons-material/Storage";
import InfoIcon from "@mui/icons-material/Info";
import { ISchema } from "getindexify";

const SchemasTable = ({ schemas }: { schemas: ISchema[] }) => {
  const getRowId = (row: ISchema) => {
    return row.id;
  };

  const columns: GridColDef[] = [
    { field: "namespace", headerName: "namespace", width: 200 },
    {
      field: "extraction_graph_name",
      headerName: "Extraction Graph",
      width: 250,
    },
    {
      field: "columns",
      headerName: "Columns",
      width: 500,
      renderCell: (params) => {
        if (!params.value) {
          return <Typography variant="body1">None</Typography>;
        }
        return (
          <Box sx={{ overflowX: "scroll" }}>
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val) => (
                <Chip
                  key={val}
                  sx={{ backgroundColor: "#060D3F", color: "white" }}
                  label={`${val}: ${params.value[val].type}`}
                />
              ))}
            </Stack>
          </Box>
        );
      },
    },
  ];

  const renderContent = () => {
    const filteredSchemas = schemas.filter(
      (schema) => Object.keys(schema.columns).length > 0
    );
    if (filteredSchemas.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Schemas Found
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
          rows={filteredSchemas}
          columns={columns}
          getRowId={getRowId}
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
        <StorageIcon />
        <Typography variant="h3">
          SQL Tables
          <IconButton
            href="https://getindexify.ai/concepts/#vector-index-and-retreival-apis"
            target="_blank"
          >
            <InfoIcon fontSize="small" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default SchemasTable;
