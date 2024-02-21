import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Box, Typography, Stack, Breadcrumbs } from "@mui/material";
import { IndexifyClient, ITask } from "getindexify";
import React from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const policyname = params.policyname;
  if (!namespace || !policyname) return redirect("/");

  const client = await IndexifyClient.createClient();
  const tasks = (await client.getTasks(policyname)).filter(
    (task) => task.extraction_policy === policyname
  );

  return { tasks, policyname, namespace };
}

const ExtractionPolicyPage = () => {
  const { tasks, policyname, namespace } = useLoaderData() as {
    tasks: ITask[];
    policyname: string;
    namespace: string;
  };

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" to={`/${namespace}`}>
          {namespace}
        </Link>
        <Typography color="text.primary">Extraction Policies</Typography>
        <Typography color="text.primary">{policyname}</Typography>
      </Breadcrumbs>
      <Box display={"flex"} alignItems={"center"}>
        <Typography variant="h2" component="h1">
          Extraction Policy - {policyname}
        </Typography>
      </Box>
      <TasksTable namespace={namespace} tasks={tasks} hideExtractionPolicy />
    </Stack>
  );
};

export default ExtractionPolicyPage;
