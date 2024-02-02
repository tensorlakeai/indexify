import { useEffect, useState } from "react";
import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Typography } from "@mui/material";
import { IContent, IIndex, IRepository } from "../lib/Indexify/types";
import IndexTable from "../components/IndexTable";
import ContentTable from "../components/ContentTable";

export async function loader({ params }: LoaderFunctionArgs) {
  const name = params.repositoryname;
  const client = new IndexifyClient();
  if (name === undefined) return null;

  const repository = await client.getRepository(name);
  const [indexes, contentList] = await Promise.all([
    repository.indexes(),
    repository.getContent(),
  ]);
  return { repository, indexes, contentList };
}

const RepositoryPage = () => {
  const { repository, indexes } = useLoaderData() as {
    repository: Repository;
    indexes: IIndex[];
    contentList: IContent[];
  };

  return (
    <div>
      <Typography mb={3} variant="h3" component="h1">
        {repository.name}
      </Typography>
      <IndexTable indexes={indexes} />
      <ContentTable indexes={indexes} />
    </div>
  );
};

export default RepositoryPage;
