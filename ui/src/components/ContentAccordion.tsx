import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Grid,
  Paper,
  Stack,
  Chip,
  styled
} from '@mui/material'
import { IContentMetadata, IndexifyClient } from 'getindexify';
import PdfDisplay from "./PdfViewer";
import ReactJson from "@microlink/react-json-view";
import InfoBox from "./InfoBox";
import { formatBytes, formatTimestamp } from '../utils/helpers';

const StyledPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  boxShadow: "0px 0px 2px 0px #D0D6DE",
  borderRadius: "12px",
  width: '100%'
}));

interface ContentAccordionProps {
  content: IContentMetadata;
  client: IndexifyClient;
  namespace: string;
}

const ContentAccordion: React.FC<ContentAccordionProps> = ({ content, client, namespace }) => {
  const [textContent, setTextContent] = useState<string>('');
  const [downloadContent, setDownloadContent] = useState<string | Blob | undefined>(undefined);

  useEffect(() => {
    if (content) {
      client.downloadContent<string | Blob>(content.id).then((data) => {
        if (typeof data === 'object' && !(data instanceof Blob)) {
          setTextContent(JSON.stringify(data));
          setDownloadContent(JSON.stringify(data));
        } else if (typeof data === 'string') {
          setTextContent(data);
          setDownloadContent(data);
        } else {
          setDownloadContent(data);
        }
      }).catch(error => {
        console.error('Error downloading content:', error);
        setTextContent('');
        setDownloadContent(undefined);
      });
    } else {
      setTextContent('');
      setDownloadContent(undefined);
    }
  }, [client, content]);

  const renderContent = () => {
    if (!content) return null;

    const contentUrl = `${client.serviceUrl}/namespaces/${namespace}/content/${content.id}/download`;

    if (content.mime_type.startsWith("application/pdf")) {
      return <PdfDisplay url={contentUrl} />;
    } else if (content.mime_type.startsWith("image")) {
      return (
        <img
          alt="content"
          src={contentUrl}
          style={{ maxWidth: "100%", height: "auto" }}
        />
      );
    } else if (content.mime_type.startsWith("audio")) {
      return (
        <audio controls style={{ width: "100%" }}>
          <source src={contentUrl} type={content.mime_type} />
          Your browser does not support the audio element.
        </audio>
      );
    } else if (content.mime_type.startsWith("video")) {
      return (
        <video
          src={contentUrl}
          controls
          style={{ width: "100%", height: "auto" }}
        />
      );
    } else if (content.mime_type.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto", width: "100%" }}>
          <code lang="html">{textContent}</code>
        </Box>
      );
    } else if (content.mime_type.startsWith("text/plain") && !content.mime_type.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto", width: "100%" }}>
          <InfoBox text={textContent} />
        </Box>
      );
    } else if (content.mime_type.startsWith("application/json")) {
      return (
        <Box sx={{ maxHeight: "500px", overflow: "auto", width: "100%" }}>
          {textContent &&
            <ReactJson name={null} src={JSON.parse(textContent)} />}
        </Box>
      );
    }
    return null;
  };

  const handleDownload = () => {
    if (!content || !downloadContent) return;

    const blob = downloadContent instanceof Blob 
      ? downloadContent 
      : new Blob([downloadContent], { type: content.mime_type });

    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = content.name || content.id;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
  };

  return (
    <StyledPaper elevation={0}>
      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <Stack spacing={1}>
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Filename:</Typography>
              <Typography variant="subtitle2">{content.name}</Typography>
            </Box>
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Source:</Typography>
              <Typography variant="subtitle2">{content.source ? content.source : "Ingestion"}</Typography>
            </Box>
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Size:</Typography>
              <Typography variant="subtitle2">{formatBytes(content.size)}</Typography>
            </Box>
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Created at:</Typography>
              <Typography variant="subtitle2">{formatTimestamp(content.created_at)}</Typography>
            </Box>
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Storage URL:</Typography>
              <Typography variant="subtitle2">{content.storage_url}</Typography>
            </Box>
            {content.parent_id && (
              <Box display="flex" alignItems="center">
                <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>Parent ID:</Typography>
                <Typography variant="subtitle2">{content.parent_id}</Typography>
              </Box>
            )}
            <Box display="flex" alignItems="center">
              <Typography variant="caption" sx={{ color: "#757A82", marginRight: 1 }}>MimeType:</Typography>
              <Chip label={content.mime_type} sx={{ backgroundColor: "#E5EFFB" }} />
            </Box>
          </Stack>
        </Grid>
        <Grid item xs={12} md={6}>
          {renderContent()}
        </Grid>
      </Grid>
      <Box sx={{ display: 'flex', justifyContent: 'flex-start' }}>
        <Button 
          variant="contained" 
          color="primary" 
          onClick={handleDownload}
          disabled={!downloadContent}
        >
          Download
        </Button>
      </Box>
    </StyledPaper>
  );
};

export default ContentAccordion;