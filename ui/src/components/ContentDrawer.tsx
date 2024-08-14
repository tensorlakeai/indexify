import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import {
  Drawer,
  Box,
  Typography,
  IconButton,
  styled,
  Button,
  Grid,
  Paper,
  Stack,
  Chip
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { IContentMetadata, IndexifyClient } from 'getindexify';
import PdfDisplay from "./PdfViewer";
import ReactJson from "@microlink/react-json-view";
import InfoBox from "./InfoBox";
import { formatBytes, formatTimestamp } from '../utils/helpers';
import CopyText from './CopyText';

const StyledPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  margin: theme.spacing(2),
  boxShadow: "0px 0px 2px 0px #D0D6DE",
  borderRadius: "12px"
}));

interface ContentDrawerProps {
  open: boolean;
  onClose: () => void;
  content: IContentMetadata | undefined;
  client: IndexifyClient;
}

const ContentDrawer: React.FC<ContentDrawerProps> = ({ open, onClose, content, client }) => {
  const [textContent, setTextContent] = useState<string>('');
  const [downloadContent, setDownloadContent] = useState<string | Blob | undefined>(undefined);
  const { namespace = 'default' } = useParams<{ namespace: string }>();

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
          width="100%"
          style={{ maxWidth: "200px" }}
          height="auto"
        />
      );
    } else if (content.mime_type.startsWith("audio")) {
      return (
        <audio controls>
          <source src={contentUrl} type={content.mime_type} />
          Your browser does not support the audio element.
        </audio>
      );
    } else if (content.mime_type.startsWith("video")) {
      return (
        <video
          src={contentUrl}
          controls
          style={{ width: "100%", maxWidth: "400px", height: "auto" }}
        />
      );
    } else if (content.mime_type.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto" }}>
          <code lang="html">{textContent}</code>
        </Box>
      );
    } else if (content.mime_type.startsWith("text/plain") && !content.mime_type.startsWith("text/html")) {
      return (
        <Box sx={{ maxHeight: "300px", overflow: "auto" }}>
          <InfoBox text={textContent} />
        </Box>
      );
    } else if (content.mime_type.startsWith("application/json")) {
      return (
        <Box sx={{ maxHeight: "500px", overflow: "auto" }}>
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

  if (!content) return null;

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      sx={{ '& .MuiDrawer-paper': { width: '700px' } }}
    >
      <Box sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Box display="flex" flexDirection="row">
            <Typography variant="h6">{content.id}</Typography>
            <CopyText text={content.id}/>
          </Box>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
        
        <StyledPaper elevation={0} sx={{marginLeft: 0}}>
          <Grid container spacing={1} paddingLeft={2} paddingRight={2}>
            <Grid item xs={12} md={6} rowSpacing={4} marginTop={1}>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>Filename:</Typography>
                <Typography variant="subtitle2">{content.name}</Typography>
              </Stack>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>Source:</Typography>
                <Typography variant="subtitle2">{content.source ? content.source : "Ingestion"}</Typography>
              </Stack>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>Size:</Typography>
                <Typography variant="subtitle2">{formatBytes(content.size)}</Typography>
              </Stack>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>Created at:</Typography>
                <Typography variant="subtitle2">{formatTimestamp(content.created_at)}</Typography>
              </Stack>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>Storage URL:</Typography>
                <Typography variant="subtitle2">{content.storage_url}</Typography>
              </Stack>
              {content.parent_id && (
                <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                  <Typography variant="caption" sx={{ color: "#757A82" }}>Parent ID:</Typography>
                  <Typography variant="subtitle2">{content.parent_id}</Typography>
                </Stack>
              )}
              <Stack direction="row" spacing={1} alignItems="center" sx={{ marginBottom: 1 }}>
                <Typography variant="caption" sx={{ color: "#757A82" }}>MimeType:</Typography>
                <Chip label={content.mime_type} sx={{ backgroundColor: "#E5EFFB" }} />
              </Stack>
            </Grid>
          </Grid>
          {renderContent()}
        </StyledPaper>

         <Box sx={{ display: 'flex', justifyContent: 'flex-start', mt: 2 }}>
          <Button variant="outlined" onClick={onClose} sx={{ mr: 1, color: '#3296FE' }}>
            Cancel
          </Button>
          <Button 
            variant="contained" 
            color="primary" 
            onClick={handleDownload}
            disabled={!downloadContent}
          >
            Download
          </Button>
        </Box>
      </Box>
    </Drawer>
  );
};

export default ContentDrawer;