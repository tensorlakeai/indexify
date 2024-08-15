import React from "react";
import { Paper, Grid, Typography, Box, Stack, Chip } from "@mui/material";
import { styled } from "@mui/system";
import { Link } from "react-router-dom";
import PdfDisplay from "./PdfViewer";
import ReactJson from "@microlink/react-json-view";
import InfoBox from "./InfoBox";
import { formatTimestamp } from "../utils/helpers";

interface DetailedContentProps {
  filename: string;
  source: string;
  size: string;
  createdAt: string;
  storageURL: string;
  parentID: string;
  mimeType: string;
  contentUrl: string;
  namespace: string;
  textContent?: string;
  extractionGraph?: string
}

const StyledPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  margin: theme.spacing(2)
}));

const DetailedContent: React.FC<DetailedContentProps> = ({
  filename,
  source,
  size,
  createdAt,
  storageURL,
  parentID,
  mimeType,
  contentUrl,
  namespace,
  textContent,
  extractionGraph
}) => {
  const renderContent = () => {
    if (mimeType.startsWith("application/pdf")) {
      return <PdfDisplay url={contentUrl} />;
    } else if (mimeType.startsWith("image")) {
      return (
        <img
          alt="content"
          src={contentUrl}
          width="100%"
          style={{ maxWidth: "200px" }}
          height="auto"
        />
      );
    } else if (mimeType.startsWith("audio")) {
      return (
        <audio controls>
          <source src={contentUrl} type={mimeType} />
          Your browser does not support the audio element.
        </audio>
      );
    } else if (mimeType.startsWith("video")) {
      return (
        <video
          src={contentUrl}
          controls
          style={{ width: "100%", maxWidth: "400px", height: "auto" }}
        />
      );
    } else if (mimeType.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto" }}>
           <code lang="html">{textContent}</code>
        </Box>
      );
    } else if (mimeType.startsWith("text/plain") && !mimeType.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto" }}>
           <InfoBox text={textContent} />
        </Box>
      );
    } else if (mimeType.startsWith("application/json")) {
      return (
        <Box sx={{ maxHeight: "500px", overflow: "auto" }}>
          {textContent &&
            <ReactJson name={null} src={JSON.parse(textContent)} />}
        </Box>
      );
    }
    return null;
  };
  return (
    <StyledPaper
      elevation={0}
      sx={{ boxShadow: "0px 0px 2px 0px #D0D6DE", borderRadius: "12px" }}
    >
      <Grid container spacing={1} paddingLeft={2} paddingRight={2}>
        <Grid item xs={12} md={6} rowSpacing={4} marginTop={1}>
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              Filename:
            </Typography>
            <Typography variant="subtitle2">
              {filename}
            </Typography>
          </Stack>
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              Source:
            </Typography>
            <Typography variant="subtitle2">
              {source}
            </Typography>
          </Stack>
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              Size:
            </Typography>
            <Typography variant="subtitle2">
              {size}
            </Typography>
          </Stack>
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              Created at:
            </Typography>
            <Typography variant="subtitle2">
              {formatTimestamp(createdAt)}
            </Typography>
          </Stack>
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              Storage URL:
            </Typography>
            <Typography variant="subtitle2">
              {storageURL}
            </Typography>
          </Stack>
          {parentID &&
            <Stack
              direction="row"
              spacing={1}
              alignItems={"center"}
              sx={{ marginBottom: 1 }}
            >
              <Typography variant="caption" sx={{ color: "#757A82" }}>
                Parent ID:
              </Typography>
              <Typography variant="subtitle2">
                <Link
                  to={`/${namespace}/extraction-graphs/${extractionGraph}/content/${parentID}`}
                  target="_blank"
                  style={{ color: "inherit", textDecoration: "underline" }}
                >
                  {parentID}
                </Link>
              </Typography>
            </Stack>}
          <Stack
            direction="row"
            spacing={1}
            alignItems={"center"}
            sx={{ marginBottom: 1 }}
          >
            <Typography variant="caption" sx={{ color: "#757A82" }}>
              MimeType:
            </Typography>
            <Chip label={mimeType} sx={{ backgroundColor: "#E5EFFB" }} />
          </Stack>
        </Grid>
        <Grid item xs={12} md={6} marginTop={1}>
          {renderContent()}
        </Grid>
      </Grid>
    </StyledPaper>
  );
};

export default DetailedContent;
