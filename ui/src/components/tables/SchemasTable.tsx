import React from "react";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Alert, Chip, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import StorageIcon from "@mui/icons-material/Storage";
import { ISchema } from "getindexify";

const SchemasTable = ({ schemas }: { schemas: ISchema[] }) => {
  const columns: GridColDef[] = [
    { field: "content_source", headerName: "Content Source", width: 300 },
    {
      field: "columns",
      headerName: "Columns",
      width: 250,
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
                  label={`${val}: ${params.value[val]}`}
                />
              ))}
            </Stack>
          </Box>
        );
      },
    },
  ];

  const getRowId = (row: ISchema) => {
    return row.content_source;
  };

  const renderContent = () => {
    const filteredSchemas = schemas.filter(
      (schema) => Object.keys(schema.columns).length > 0
    );
    if (filteredSchemas.length === 0) {
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
          rows={filteredSchemas}
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
        <StorageIcon />
        <Typography variant="h3">SQL Tables</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default SchemasTable;
