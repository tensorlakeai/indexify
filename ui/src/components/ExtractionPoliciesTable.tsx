import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IExtractionPolicy } from "getindexify";
import { Alert, Chip, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import React from "react";
import GavelIcon from '@mui/icons-material/Gavel';
import { Link } from "react-router-dom";

const getRowId = (row: IExtractionPolicy) => {
  return row.name;
};

const ExtractionPoliciesTable = ({
  extractionPolicies,
  namespace,
}: {
  extractionPolicies: IExtractionPolicy[];
  namespace: string;
}) => {
  const columns: GridColDef[] = [
    {
      field: "name",
      headerName: "Name",
      width: 200,
      renderCell: (params) => {
        return (
          <Link color="inherit" to={`/${namespace}/extraction-policies/${params.value}`} target="_blank">
            {params.value}
          </Link>
        );
      },
    },
    {
      field: "extractor",
      headerName: "Extractor",
      width: 200,
    },
    {
      field: "content_source",
      headerName: "Content Source",
      width: 150,
    },
    {
      field: "filters_eq",
      headerName: "Filters",
      width: 100,
      valueGetter: (params) => {
        return JSON.stringify(params.value);
      },
    },
    {
      field: "input_params",
      headerName: "Input Params",
      width: 300,
      renderCell: (params) => {
        if (!params.value) {
          return <Typography variant="body1">None</Typography>;
        }
        return (
          <Box sx={{ overflowX: "scroll" }}>
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val: string) => {
                return (
                  <Chip key={val} label={`${val}:${params.value[val]}`} />
                );
              })}
            </Stack>
          </Box>
        );
      },
    },
  ];

  const renderContent = () => {
    if (extractionPolicies.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Policies Found
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
          rows={extractionPolicies}
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
        <GavelIcon />
        <Typography variant="h3">Extraction Policies</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractionPoliciesTable;
