import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Typography } from "@mui/material";
import { IContent, IIndex } from "../lib/Indexify/types";
import IndexTable from "../components/IndexTable";
import ContentTable from "../components/ContentTable";
import React from "react";
import { Stack } from "@mui/system";
import ExtractorBindingsTable from "../components/ExtractorBindingsTable";

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
  const { repository, indexes, contentList } = useLoaderData() as {
    repository: Repository;
    indexes: IIndex[];
    contentList: IContent[];
  };

  return (
    <Stack direction="column" spacing={3}>
      <Typography variant="h2" component="h1">
        {repository.name}
      </Typography>
      <ExtractorBindingsTable bindings={repository.extractorBindings} />
      <IndexTable indexes={indexes} />
      <ContentTable content={contentList} />
    </Stack>
  );
};

export default RepositoryPage;
