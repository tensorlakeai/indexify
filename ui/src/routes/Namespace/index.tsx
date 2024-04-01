import {
  IndexifyClient,
  Extractor,
  IContentMetadata,
  IIndex,
  ISchema,
} from "getindexify";
import { useLoaderData, LoaderFunctionArgs } from "react-router-dom";
import { Stack } from "@mui/material";
import ContentTable from "../../components/tables/ContentTable";
import React from "react";
import ExtractionGraphs from "../../components/ExtractionGraphs";
import ExtractorsTable from "../../components/tables/ExtractorsTable";
import { getIndexifyServiceURL } from "../../utils/helpers";
import SchemasTable from "../../components/tables/SchemasTable";
import IndexTable from "../../components/tables/IndexTable";

export async function loader({ params }: LoaderFunctionArgs) {
  const { namespace } = params;
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  });
  const [extractors, indexes, contentList, schemas] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getContent(),
    client.getSchemas(),
  ]);
  return { client, extractors, indexes, contentList, schemas, namespace };
}

const NamespacePage = () => {
  const { client, extractors, indexes, contentList, schemas, namespace } =
    useLoaderData() as {
      client: IndexifyClient;
      extractors: Extractor[];
      indexes: IIndex[];
      contentList: IContentMetadata[];
      schemas: ISchema[];
      namespace: string;
    };

  return (
    <Stack direction="column" spacing={3}>
      <ExtractionGraphs
        namespace={client.namespace}
        extractionPolicies={client.extractionPolicies}
        extractors={extractors}
      />
      <IndexTable namespace={namespace} indexes={indexes} extractionPolicies={client.extractionPolicies}/>
      <SchemasTable schemas={schemas} />
      <ContentTable
        extractionPolicies={client.extractionPolicies}
        content={contentList}
      />
      <ExtractorsTable extractors={extractors} />
    </Stack>
  );
};

export default NamespacePage;
