import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import { Typography, Stack, Breadcrumbs, Box } from "@mui/material";
import {
  IContentMetadata,
  IExtractedMetadata,
  IndexifyClient,
  ITask,
} from "getindexify";
import React, { ReactElement, useEffect, useState } from "react";
import TasksTable from "../../components/TasksTable";
import { Link } from "react-router-dom";
import ExtractedMetadataTable from "../../components/tables/ExtractedMetaDataTable";
import { isAxiosError } from "axios";
import Errors from "../../components/Errors";
import PdfDisplay from "../../components/PdfViewer";
import {
  getIndexifyServiceURL,
  groupMetadataByExtractor,
  formatBytes,
} from "../../utils/helpers";
import moment from "moment";
import ReactJson from "@microlink/react-json-view";

export async function loader({ params }: LoaderFunctionArgs) {
  const errors: string[] = [];
  const namespace = params.namespace;
  const contentId = params.contentId;
  if (!namespace || !contentId) return redirect("/");
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  });
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
    groupedExtractedMetadata: groupMetadataByExtractor(extractedMetadataList),
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
    groupedExtractedMetadata: Record<string, IExtractedMetadata[]>;
    client: IndexifyClient;
    errors: string[];
  };

  const renderMetadataEntry = (label: string, value: ReactElement | string) => {
    return (
      <Box display="flex">
        <Typography variant="label">{label}:</Typography>
        <Typography sx={{ ml: 1 }} variant="body1">
          {value}
        </Typography>
      </Box>
    );
  };

  const [textContent, setTextContent] = useState("");

  useEffect(() => {
    if (
      contentMetadata.mime_type.startsWith("application/json") ||
      contentMetadata.mime_type.startsWith("text")
    ) {
      client.downloadContent<string | object>(contentId).then((data) => {
        if (typeof data === "object") {
          setTextContent(JSON.stringify(data));
        } else {
          setTextContent(data);
        }
      });
    }
  }, [client, contentId, contentMetadata.mime_type]);

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
    } else if (contentMetadata.mime_type.startsWith("application/json")) {
      return (
        <Box
          sx={{
            maxHeight: "500px",
            overflow: "scroll",
          }}
        >
          {textContent && (
            <ReactJson name={null} src={JSON.parse(textContent)}></ReactJson>
          )}
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
      <Typography variant="h2">{contentId}</Typography>
      <Stack direction={"column"} gap={1}>
        {renderMetadataEntry("Filename", contentMetadata.name)}
        {contentMetadata.parent_id
          ? renderMetadataEntry(
              "ParentID",
              <Link
                to={`/${namespace}/content/${contentMetadata.parent_id}`}
                target="_blank"
              >
                {contentMetadata.parent_id}
              </Link>
            )
          : null}
        {renderMetadataEntry(
          "Created At",
          moment(contentMetadata.created_at * 1000).format()
        )}
        {renderMetadataEntry("MimeType", contentMetadata.mime_type)}
        {renderMetadataEntry("Source", contentMetadata.source)}
        {renderMetadataEntry("Storage Url", contentMetadata.storage_url)}
        {renderMetadataEntry("Size", formatBytes(contentMetadata.size))}
      </Stack>
      {/* display content */}
      {renderContent()}
      {/* tasks */}
      {Object.keys(groupedExtractedMetadata).map((key) => {
        const extractedMetadata = groupedExtractedMetadata[key];
        return (
          <ExtractedMetadataTable
            key={key}
            extractedMetadata={extractedMetadata}
          />
        );
      })}
      <TasksTable
        policies={client.extractionPolicies}
        namespace={namespace}
        tasks={tasks}
        hideContentId
      />
    </Stack>
  );
};

export default ContentPage;
