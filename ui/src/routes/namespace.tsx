import IndexifyClient from "../lib/Indexify/client";
import Namespace from "../lib/Indexify/namespace";
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

  const namespace = await client.getNamespace(name);
  const [indexes, contentList] = await Promise.all([
    namespace.indexes(),
    namespace.getContent(),
  ]);
  return { namespace, indexes, contentList };
}

const NamespacePage = () => {
  const { namespace, indexes, contentList } = useLoaderData() as {
    namespace: Namespace;
    indexes: IIndex[];
    contentList: IContent[];
  };

  return (
    <Stack direction="column" spacing={3}>
      <Box display={"flex"} alignItems={"center"}>
        <CircleIcon
          sx={{
            width: "30px",
            color: stringToColor(namespace.name),
            mr: 1,
          }}
        />
        <Typography variant="h2" component="h1">
          {namespace.name}
        </Typography>
      </Box>
      <ExtractorBindingsTable bindings={namespace.extractorBindings} />
      <IndexTable indexes={indexes} />
      <ContentTable content={contentList} />
    </Stack>
  );
};

export default NamespacePage;
