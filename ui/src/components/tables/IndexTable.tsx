import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IIndex } from "getindexify";
import { Alert, Chip, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import ManageSearchIcon from "@mui/icons-material/ManageSearch";
import React from "react";
import { Link } from "react-router-dom";

const IndexTable = ({
  indexes,
  namespace,
}: {
  indexes: IIndex[];
  namespace: string;
}) => {
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
      field: "schema",
      headerName: "Schema",
      width: 300,
      renderCell: (params) => {
        if (!params.value) {
          return <Typography variant="body1">None</Typography>;
        }
        return (
          <Box sx={{ overflowX: "scroll" }}>
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val: string) => {
                return <Chip key={val} label={`${val}:${params.value[val]}`} />;
              })}
            </Stack>
          </Box>
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
