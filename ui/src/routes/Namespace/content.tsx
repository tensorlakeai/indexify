import IndexifyClient from "../../lib/Indexify/client";
import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Box, Typography, Stack, Breadcrumbs } from "@mui/material";
import { IContent, ITask } from "../../lib/Indexify/types";
import React from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const parentId = params.parentId;
  if (!namespace || !parentId) return redirect("/");
  const client = await IndexifyClient.createClient();
  // get content from parentId
  const content = await client.getContent(parentId).then((res) => res[0]);
  // get filter tasks from content source that match parent_id
  const tasks = await client
    .getTasks(content.source)
    .then((tasks) =>
      tasks.filter((t) => t.content_metadata.id === content.parent_id)
    );

  return { namespace, tasks, content };
}

const ContentPage = () => {
  const { namespace, tasks, content } = useLoaderData() as {
    namespace: string;
    tasks: ITask[];
    content: IContent;
  };

  const displayValues = [
    { title: "ID", value: content.id },
    {
      title: "Parent ID",
      value: content.parent_id,
    },
    {
      title: "Name",
      value: content.name,
    },
    {
      title: "Source",
      value: content.source,
    },
    {
      title: "Content Type",
      value: content.content_type,
    },
    {
      title: "Storage URL",
      value: content.storage_url,
    },
  ] as { title: string; value: string }[];

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" to={`/${namespace}`}>
          {namespace}
        </Link>
        <Typography color="text.primary">Content</Typography>
        <Typography color="text.primary">{content.parent_id}</Typography>
      </Breadcrumbs>

      {displayValues.map(({ title, value }) => (
        <Box>
          <Typography variant="h3" gutterBottom>
            {title}
          </Typography>
          <Typography variant="body2">{value}</Typography>
        </Box>
      ))}

      <TasksTable namespace={namespace} tasks={tasks} />
    </Stack>
  );
};

export default ContentPage;
