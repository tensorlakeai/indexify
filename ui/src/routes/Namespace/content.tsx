import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Typography, Stack, Breadcrumbs, Box } from "@mui/material";
import {
  IContentMetadata,
  IExtractedMetadata,
  IndexifyClient,
  ITask,
} from "getindexify";
import React, { useEffect, useState } from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";
import ExtractedMetadataTable from "../../components/ExtractedMetaDataTable";
import { isAxiosError } from "axios";
import Errors from "../../components/Errors";
import PdfDisplay from "../../components/PdfViewer";
import { groupMetadataByExtractor } from "../../utils/helpers";

export async function loader({ params }: LoaderFunctionArgs) {
  const errors: string[] = [];
  const namespace = params.namespace;
  const contentId = params.contentId;
  if (!namespace || !contentId) return redirect("/");
  const client = await IndexifyClient.createClient();
  // get content from contentId
  const tasks = await client
    .getTasks()
    .then((tasks) => tasks.filter((t) => t.content_metadata.id === contentId))
    .catch((e) => {
      if (isAxiosError(e)) {
        errors.push(`getTasks: ${e.message}`);
      }
      return null;
    });
  const contentMetadata = await client.getContentById(contentId);
  const extractedMetadataList = await client
    .getExtractedMetadata(contentId)
    .catch((e) => {
      if (isAxiosError(e)) {
        errors.push(
          `getExtractedMetadata: ${e.message} - ${
            e.response?.data || "unknown"
          }`
        );
      }
      return [];
    });
  return {
    client,
    namespace,
    tasks,
    contentId,
    contentMetadata,
    groupedExtractedMetadata:groupMetadataByExtractor(extractedMetadataList),
    errors,
  };
}

const ContentPage = () => {
  const {
    client,
    namespace,
    tasks,
    contentId,
    contentMetadata,
    groupedExtractedMetadata,
    errors,
  } = useLoaderData() as {
    namespace: string;
    tasks: ITask[];
    contentId: string;
    contentMetadata: IContentMetadata;
    groupedExtractedMetadata: Record<string,IExtractedMetadata[]>;
    client: IndexifyClient;
    errors: string[];
  };

  const [textContent, setTextContent] = useState("");
  useEffect(() => {
    client.downloadContent<string>(contentId).then((data) => {
      setTextContent(data);
    });
  }, [client, contentId]);

  const renderContent = () => {
    if (contentMetadata.mime_type.startsWith("application/pdf")) {
      return <PdfDisplay url={contentMetadata.content_url} />;
    } else if (contentMetadata.mime_type.startsWith("image")) {
      return (
        <img
          alt="content"
          src={contentMetadata.content_url}
          width="100%"
          style={{ maxWidth: "200px" }}
          height="auto"
        />
      );
    } else if (contentMetadata.mime_type.startsWith("audio")) {
      return (
        <audio controls>
          <source
            src={contentMetadata.content_url}
            type={contentMetadata.mime_type}
          />
          Your browser does not support the audio element.
        </audio>
      );
    } else if (contentMetadata.mime_type.startsWith("video")) {
      return (
        <video
          src={contentMetadata.content_url}
          controls
          style={{ width: "100%", maxWidth: "400px", height: "auto" }}
        />
      );
    } else if (contentMetadata.mime_type.startsWith("text")) {
      return (
        <Box
          sx={{
            maxHeight: "500px",
            overflow: "scroll",
          }}
        >
          <Typography variant="body2">{textContent}</Typography>
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
      <Errors errors={errors} />
      <Typography variant="h2">Content - {contentId}</Typography>
      <Typography variant="body1">
        MimeType: {contentMetadata.mime_type}
      </Typography>
      {/* display content */}
      {renderContent()}
      {/* tasks */}
      {Object.keys(groupedExtractedMetadata).map(key => {
        const extractedMetadata = groupedExtractedMetadata[key]
        return <ExtractedMetadataTable extractedMetadata={extractedMetadata} />
      })}
      <TasksTable namespace={namespace} tasks={tasks} hideContentId />
    </Stack>
  );
};

export default ContentPage;
