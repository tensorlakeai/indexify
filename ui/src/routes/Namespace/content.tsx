import IndexifyClient from "../../lib/Indexify/client";
import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Typography, Stack, Breadcrumbs, Box } from "@mui/material";
import { IContent, ITask } from "../../lib/Indexify/types";
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
  const content = await client.getContentById(contentId);
  return { namespace, tasks, contentId, content };
}

const ContentPage = () => {
  const { namespace, tasks, contentId, content } = useLoaderData() as {
    namespace: string;
    tasks: ITask[];
    contentId: string;
    content: IContent;
  };

  function byteArrayToBlob(byteArray: number[]): Blob {
    const arrayBuffer = new ArrayBuffer(byteArray.length);
    const uint8Array = new Uint8Array(arrayBuffer);
    for (let i = 0; i < byteArray.length; i++) {
      uint8Array[i] = byteArray[i];
    }
    return new Blob([uint8Array]);
  }

  function bytesToString(bytes: number[]): string {
    const chunkSize = 10000; // Size of each chunk, adjust based on your environment
    let result = "";

    for (let i = 0; i < bytes.length; i += chunkSize) {
      const chunk = bytes.slice(i, i + chunkSize);
      result += String.fromCharCode.apply(null, chunk);
    }

    return result;
  }

  const renderContent = () => {
    console.log(content.content_type);
    if (content.content_type.startsWith("image")) {
      const blob = byteArrayToBlob(content.bytes);
      return (
        <img
          alt="content"
          src={URL.createObjectURL(blob)}
          width="100%"
          style={{ maxWidth: "200px" }}
          height="auto"
        />
      );
    } else if (content.content_type.startsWith("text")) {
      return (
        <Box
          sx={{
            maxHeight: "500px",
            overflow: "scroll",
          }}
        >
          <Typography variant="body2">
            {bytesToString(content.bytes)}
          </Typography>
        </Box>
      );
    }
    return null;
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
      <Typography variant="h2">Content - {contentId}</Typography>
      <Typography variant="body1">MimeType: {content.content_type}</Typography>
      {/* display content */}
      {renderContent()}
      {/* tasks */}
      <TasksTable namespace={namespace} tasks={tasks} hideContentId />
    </Stack>
  );
};

export default ContentPage;
