import IndexifyClient from "../lib/Indexify/client";
import Repository from "../lib/Indexify/repository";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Box, Typography, Stack } from "@mui/material";
import { IContent, IIndex } from "../lib/Indexify/types";
import IndexTable from "../components/IndexTable";
import ContentTable from "../components/ContentTable";
import React from "react";
import ExtractorBindingsTable from "../components/ExtractorBindingsTable";
import CircleIcon from "@mui/icons-material/Circle";
import { stringToColor } from "../utils/helpers";

export async function loader({ params }: LoaderFunctionArgs) {
  const name = params.namespace;
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
      <Box display={"flex"} alignItems={"center"}>
        <CircleIcon
          sx={{
            width: "30px",
            color: stringToColor(repository.name),
            mr: 1,
          }}
        />
        <Typography variant="h2" component="h1">
          {repository.name}
        </Typography>
      </Box>
      <ExtractorBindingsTable bindings={repository.extractorBindings} />
      <IndexTable indexes={indexes} />
      <ContentTable content={contentList} />
    </Stack>
  );
};

export default RepositoryPage;
