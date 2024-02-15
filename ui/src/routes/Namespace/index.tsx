import IndexifyClient from "../../lib/Indexify/client";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Box, Typography, Stack } from "@mui/material";
import { IContentMetadata, IIndex } from "../../lib/Indexify/types";
import IndexTable from "../../components/IndexTable";
import ContentTable from "../../components/ContentTable";
import React from "react";
import ExtractorBindingsTable from "../../components/ExtractorBindingsTable";
import CircleIcon from "@mui/icons-material/Circle";
import { stringToColor } from "../../utils/helpers";
import ExtractorsTable from "../../components/ExtractorsTable";
import Extractor from "../../lib/Indexify/extractor";

export async function loader({ params }: LoaderFunctionArgs) {
  const { namespace } = params;
  const client = await IndexifyClient.createClient({ namespace });
  const [extractors, indexes, contentList] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getContent(),
  ]);
  return { client, extractors, indexes, contentList };
}

const NamespacePage = () => {
  const { client, extractors, indexes, contentList } = useLoaderData() as {
    client: IndexifyClient;
    extractors: Extractor[];
    indexes: IIndex[];
    contentList: IContentMetadata[];
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
      <ExtractorBindingsTable
        namespace={client.namespace}
        extractorBindings={client.extractorBindings}
      />
      <IndexTable indexes={indexes} />
      <ContentTable content={contentList} />
      <ExtractorsTable extractors={extractors} />
    </Stack>
  );
};

export default NamespacePage;
