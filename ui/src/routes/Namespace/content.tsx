import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Typography, Stack, Breadcrumbs, Box } from "@mui/material";
import { IContentMetadata, IndexifyClient, ITask } from "getindexify";
import React, { useState } from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";
import axios from "axios";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const contentId = params.contentId;
  if (!namespace || !contentId) return redirect("/");
  const client = await IndexifyClient.createClient();
  // get content from contentId
  const tasks = await client
    .getTasks()
    .then((tasks) => tasks.filter((t) => t.content_metadata.id === contentId));
  const contentMetadata = await client.getContentById(contentId);
  return { client, namespace, tasks, contentId, contentMetadata };
}

const ContentPage = () => {
  const { client, namespace, tasks, contentId, contentMetadata } = useLoaderData() as {
    namespace: string;
    tasks: ITask[];
    contentId: string;
    contentMetadata: IContentMetadata;
    client: IndexifyClient
  };

  const [textContent, setTextContent] = useState("")

  const renderContent = () => {
    const contentURL = `${client.serviceUrl}/namespaces/${namespace}/content/${contentId}/download`
    if (contentMetadata.mime_type.startsWith("image")) {
      return (
        <img
          alt="content"
          src={contentURL}
          width="100%"
          style={{ maxWidth: "200px" }}
          height="auto"
        />
      );
    } else if (contentMetadata.mime_type.startsWith("audio")) {
      return (
        <audio controls>
          <source src={contentURL} type={contentMetadata.mime_type} />
          Your browser does not support the audio element.
        </audio>
      );
    } else if (contentMetadata.mime_type.startsWith("video")) {
      return (
        <video
          src={contentURL}
          controls
          style={{ width: "100%", maxWidth: "400px", height: "auto" }}
        />
      );
    } else if (contentMetadata.mime_type.startsWith("text")) {
      axios.get(contentURL).then(res => {
        setTextContent(String(res.data))
      })
      return (
        <Box
          sx={{
            maxHeight: "500px",
            overflow: "scroll",
          }}
        >
          <Typography variant="body2">
            {textContent}
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
      <Typography variant="body1">MimeType: {contentMetadata.mime_type}</Typography>
      {/* display content */}
      {renderContent()}
      {/* tasks */}
      <TasksTable namespace={namespace} tasks={tasks} hideContentId />
    </Stack>
  );
};

export default ContentPage;
