import {
  IndexifyClient,
  Extractor,
  IContentMetadata,
  IIndex,
  ISchema,
  ITask,
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
  const [extractors, indexes, contentList, schemas, tasks] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getExtractedContent(),
    client.getSchemas(),
    client.getTasks(),
  ]);
  return {
    client,
    extractors,
    indexes,
    contentList,
    schemas,
    tasks,
    namespace,
  };
}

const NamespacePage = () => {
  const { client, extractors, indexes, contentList, schemas, tasks, namespace } =
    useLoaderData() as {
      client: IndexifyClient;
      extractors: Extractor[];
      indexes: IIndex[];
      contentList: IContentMetadata[];
      schemas: ISchema[];
      tasks: ITask[];
      namespace: string;
    };

  return (
    <Stack direction="column" spacing={3}>
      <ExtractionGraphs
        namespace={client.namespace}
        extractionGraphs={client.extractionGraphs}
        extractors={extractors}
        tasks={tasks}
      />
      <IndexTable
        namespace={namespace}
        indexes={indexes}
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
      />
      <SchemasTable schemas={schemas} />
      <ContentTable
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
        content={contentList}
      />
      <ExtractorsTable extractors={extractors} />
    </Stack>
  );
};

export default NamespacePage;
