import IndexifyClient from "../../lib/Indexify/client";
import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Typography, Stack, Breadcrumbs } from "@mui/material";
import { ITask } from "../../lib/Indexify/types";
import React from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const contentId = params.contentId;
  if (!namespace || !contentId) return redirect("/");
  const client = await IndexifyClient.createClient();
  // get content from contentId
  const tasks = await client
    .getTasks()
    .then((tasks) => tasks.filter((t) => t.content_metadata.id === contentId));

  return { namespace, tasks, contentId };
}

const ContentPage = () => {
  const { namespace, tasks, contentId } = useLoaderData() as {
    namespace: string;
    tasks: ITask[];
    contentId: string;
  };

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" to={`/${namespace}`}>
          {namespace}
        </Link>
        <Typography color="text.primary">Content</Typography>
        <Typography color="text.primary">{contentId}</Typography>
      </Breadcrumbs>
      <Typography variant="h2" component="h2">
        Content - {contentId}
      </Typography>
      <TasksTable namespace={namespace} tasks={tasks} hideContentId />
    </Stack>
  );
};

export default ContentPage;
