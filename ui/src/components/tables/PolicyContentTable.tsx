import React, { useEffect, useState } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  TableContainer, 
  Table, 
  TableHead, 
  TableRow, 
  TableCell, 
  TableBody,
  Alert
} from '@mui/material'
import {
  IContentMetadata,
  IndexifyClient,
} from 'getindexify'
import CopyText from '../../components/CopyText'

interface PolicyContentTableProps {
  client: IndexifyClient;
  namespace: string;
  contentId: string;
  extractorName: string;
  policyName: string;
  onContentClick: (content: IContentMetadata) => void;
}

const PolicyContentTable: React.FC<PolicyContentTableProps> = ({ 
  client, 
  namespace, 
  contentId, 
  extractorName, 
  policyName,
  onContentClick 
}) => {
  const [content, setContent] = useState<IContentMetadata[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const loadContent = async () => {
      setIsLoading(true);
      try {
        const result = await client.getExtractionPolicyContent({
          contentId,
          graphName: extractorName,
          policyName,
        });
        setContent(result);
      } catch (error) {
        console.error(`Error loading content for policy ${policyName}:`, error);
        setContent([]);
      } finally {
        setIsLoading(false);
      }
    };

    loadContent();
  }, [client, contentId, extractorName, policyName]);

  if (isLoading) {
    return <Typography>Loading...</Typography>;
  }

  if (content.length === 0) {
    return (
      <Box mt={1} mb={2}>
        <Alert variant="standard" severity="info">
          No Content Found
        </Alert>
      </Box>
    );
  }

  return (
    <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",}}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Content ID</TableCell>
            <TableCell>Mime Type</TableCell>
            <TableCell>Source</TableCell>
            <TableCell>Parent ID</TableCell>
            <TableCell>Labels</TableCell>
            <TableCell>Created At</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {content.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <Box sx={{ display: "flex", alignItems: "center" }}>
                  <Typography
                    onClick={() => onContentClick(row)}
                    sx={{ cursor: 'pointer', '&:hover': { textDecoration: 'underline' } }}
                  >
                    {row.id}
                  </Typography>
                  <CopyText text={row.id}/>
                </Box>
              </TableCell>
              <TableCell>{row.mime_type}</TableCell>
              <TableCell>{row.source}</TableCell>
              <TableCell>{row.parent_id}</TableCell>
              <TableCell>
                {typeof row.labels === 'object' && row.labels !== null
                  ? Object.entries(row.labels)
                      .map(([key, value]) => `${key}: ${value}`)
                      .join(', ')
                  : String(row.labels)}
              </TableCell>
              <TableCell>
                {row.created_at ? new Date(row.created_at * 1000).toLocaleString() : ''}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default PolicyContentTable;
