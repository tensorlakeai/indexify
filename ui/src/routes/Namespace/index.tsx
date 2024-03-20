import {
  IndexifyClient,
  Extractor,
  IContentMetadata,
  IIndex,
} from "getindexify";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Stack } from "@mui/material";
import ContentTable from "../../components/ContentTable";
import React from "react";
import ExtractionGraphs from "../../components/ExtractionGraphs";
import ExtractorsTable from "../../components/ExtractorsTable";

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
      <ExtractionGraphs
        indexes={indexes}
        namespace={client.namespace}
        extractionPolicies={client.extractionPolicies}
      />
      <ContentTable
        extractionPolicies={client.extractionPolicies}
        content={contentList}
      />
      <ExtractorsTable extractors={extractors} />
    </Stack>
  );
};

export default NamespacePage;
