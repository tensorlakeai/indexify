import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Box, Typography, Stack, Breadcrumbs } from "@mui/material";
import { IExtractionPolicy, IndexifyClient, ITask } from "getindexify";
import React from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";
import { getIndexifyServiceURL } from "../../utils/helpers";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const policyname = params.policyname;
  const graphname = params.graphname;
  if (!namespace || !policyname) return redirect("/");

  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  });

  const policy = client.extractionGraphs
    .map((graph) => graph.extraction_policies)
    .flat()
    .find((policy) => policy.name === policyname && policy.graph_name === graphname);
  const tasks = (await client.getTasks()).filter(
    (task) => task.extraction_policy_id === policy?.id
  );

  return { tasks, policy, namespace, client };
}

const ExtractionPolicyPage = () => {
  const { tasks, policy, namespace, client } = useLoaderData() as {
    tasks: ITask[];
    policy: IExtractionPolicy;
    namespace: string;
    client: IndexifyClient;
  };

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" to={`/${namespace}`}>
          {namespace}
        </Link>
        <Typography color="text.primary">Extraction Policies</Typography>
        <Typography color="text.primary">{policy.graph_name}</Typography>
        <Typography color="text.primary">{policy.name}</Typography>
      </Breadcrumbs>
      <Box display={"flex"} alignItems={"center"}>
        <Typography variant="h2" component="h1">
          Extraction Policy - {policy.name}
        </Typography>
      </Box>
      <TasksTable
        policies={client.extractionGraphs.map(graph => graph.extraction_policies).flat()}
        namespace={namespace}
        tasks={tasks}
        hideExtractionPolicy
      />
    </Stack>
  );
};

export default ExtractionPolicyPage;
