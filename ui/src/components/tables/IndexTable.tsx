import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IExtractionPolicy, IIndex } from "getindexify";
import { Alert, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import ManageSearchIcon from "@mui/icons-material/ManageSearch";
import React from "react";
import { Link } from "react-router-dom";

const IndexTable = ({
  indexes,
  namespace,
  extractionPolicies
}: {
  indexes: IIndex[];
  namespace: string;
  extractionPolicies: IExtractionPolicy[]
}) => {
  const getPolicyFromIndexname = (indexName:string):IExtractionPolicy | undefined => {
    return extractionPolicies.find(policy => String(indexName).startsWith(policy.name))
  }

  const columns: GridColDef[] = [
    {
      field: "name",
      headerName: "Name",
      width: 300,
      renderCell: (params) => {
        return (
          <Link to={`/${namespace}/indexes/${params.value}`}>
            {params.value}
          </Link>
        );
      },
    },
    {
      field: "policy_name",
      headerName: "Policy Name",
      width: 300,
      renderCell: (params) => {
        const policy = getPolicyFromIndexname(params.row.name)
        if (!policy) {
          return null
        }
        return (
          <Link to={`/${namespace}/extraction-policies/${policy.name}`}>{policy.name}</Link>
        );
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
