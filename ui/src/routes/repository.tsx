import { useEffect, useState } from "react";
import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import {
  DataGrid,
  GridColDef,
  GridRenderCellParams,
  GridValueGetterParams,
} from "@mui/x-data-grid";
import { Typography } from "@mui/material";
import { IIndex, IRepository } from "../lib/Indexify/types";
import IndexTable from "../components/IndexTable";

export async function loader({ params }: LoaderFunctionArgs) {
  const name = params.repositoryname;
  const client = new IndexifyClient();
  if (name === undefined) return null;

  const repository = await client.getRepository(name);
  const indexes = await repository.indexes();
  return { repository, indexes };
}

const RepositoryPage = () => {
  const { repository, indexes } = useLoaderData() as {
    repository: Repository;
    indexes: IIndex[];
  };

  return (
    <div>
      <Typography mb={3} variant="h3" component="h1">
        {repository.name}
      </Typography>
      <div style={{ height: 400, width: "100%" }}>
        <Typography variant="h4">Indexes</Typography>
        <IndexTable indexes={indexes} />
      </div>
    </div>
  );
};

export default RepositoryPage;
