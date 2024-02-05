import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import { DataGrid, GridColDef, GridRenderCellParams } from "@mui/x-data-grid";
import { Typography } from "@mui/material";
import React from "react";
import CircleIcon from "@mui/icons-material/Circle";
import { stringToColor } from "../utils/helpers";
import { Stack } from "@mui/system";
import { LoaderFunctionArgs, useLoaderData } from "react-router-dom";

export async function loader(args: LoaderFunctionArgs) {
  const client = new IndexifyClient();
  const repositories = await client.repositories();
  return { repositories };
}

const RepositoriesPage = () => {
  const { repositories } = useLoaderData() as { repositories: Repository[] };

  const columns: GridColDef[] = [
    {
      field: "name",
      headerName: "Name",
      width: 400,
      renderCell: (params: GridRenderCellParams<Repository>) => (
        <a href={`/repositories/${params.value}`}>
          <Stack
            display={"flex"}
            direction={"row"}
            alignItems={"center"}
            spacing={1}
          >
            <CircleIcon
              sx={{ width: "15px", color: stringToColor(params.value) }}
            />
            <Typography variant="body2">{params.value}</Typography>
          </Stack>
        </a>
      ),
    },
    {
      field: "extractorBindings",
      headerName: "Extractor Bindings",
      width: 200,
      valueGetter: (params) => {
        return params.value.length;
      },
    },
  ];

  const getRowId = (row: Repository) => {
    return row.name;
  };

  return (
    <div>
      <Typography mb={3} variant="h3" component="h1">
        Namespaces
      </Typography>
      <DataGrid
        sx={{ backgroundColor: "white" }}
        autoHeight
        getRowId={getRowId}
        rows={repositories}
        columns={columns}
        initialState={{
          pagination: {
            paginationModel: { page: 0, pageSize: 20 },
          },
        }}
        pageSizeOptions={[5, 10, 20]}
      />
    </div>
  );
};

export default RepositoriesPage;
