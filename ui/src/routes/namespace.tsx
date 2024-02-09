import IndexifyClient from "../lib/Indexify/client";
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
  const { namespace } = params;
  const client = await IndexifyClient.createClient({ namespace });
  if (namespace === undefined) return null;

  const [indexes, contentList] = await Promise.all([
    client.indexes(),
    client.getContent(),
  ]);
  return { client, indexes, contentList };
}

const NamespacePage = () => {
  const { client, indexes, contentList } = useLoaderData() as {
    client: IndexifyClient;
    indexes: IIndex[];
    contentList: IContent[];
  };

  return (
    <Stack direction="column" spacing={3}>
      <Box display={"flex"} alignItems={"center"}>
        <CircleIcon
          sx={{
            width: "30px",
            color: stringToColor(client.namespace),
            mr: 1,
          }}
        />
        <Typography variant="h2" component="h1">
          {client.namespace}
        </Typography>
      </Box>
      <ExtractorBindingsTable bindings={client.extractorBindings} />
      <IndexTable indexes={indexes} />
      <ContentTable content={contentList} />
    </Stack>
  );
};

export default NamespacePage;
