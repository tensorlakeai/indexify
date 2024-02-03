import { useEffect, useState } from "react";
import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import {
  DataGrid,
  GridColDef,
  GridRenderCellParams,
  GridValueGetterParams,
} from "@mui/x-data-grid";
import { Typography } from "@mui/material";
import React from "react";

const RepositoriesPage = () => {
  const client = new IndexifyClient();

  const [repositories, setRepositories] = useState<Repository[]>([]);
  useEffect(() => {
    client.repositories().then((repos) => {
      setRepositories(repos);
    });
  }, []);

  const columns: GridColDef[] = [
    {
      field: "name",
      headerName: "Name",
      width: 400,
      renderCell: (params: GridRenderCellParams<Repository>) => (
        <a href={`/repositories/${params.value}`}>{params.value}</a>
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
        Repositories
      </Typography>
      <DataGrid
        autoHeight
        getRowId={getRowId}
        rows={repositories}
        columns={columns}
        initialState={{
          pagination: {
            paginationModel: { page: 0, pageSize: 5 },
          },
        }}
        pageSizeOptions={[5, 10]}
      />
    </div>
  );
};

export default RepositoriesPage;
