import React, { useEffect, useState } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  Table as MuiTable, 
  TableContainer as MuiTableContainer, 
  TableHead as MuiTableHead, 
  TableRow as MuiTableRow, 
  TableCell as MuiTableCell, 
  TableBody as MuiTableBody,
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
    <MuiTableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",}}>
      <MuiTable>
        <MuiTableHead>
          <MuiTableRow>
            <MuiTableCell>Content ID</MuiTableCell>
            <MuiTableCell>Mime Type</MuiTableCell>
            <MuiTableCell>Source</MuiTableCell>
            <MuiTableCell>Parent ID</MuiTableCell>
            <MuiTableCell>Labels</MuiTableCell>
            <MuiTableCell>Created At</MuiTableCell>
          </MuiTableRow>
        </MuiTableHead>
        <MuiTableBody>
          {content.map((row) => (
            <MuiTableRow key={row.id}>
              <MuiTableCell>
                <Box sx={{ display: "flex", alignItems: "center" }}>
                  <Typography
                    onClick={() => onContentClick(row)}
                    sx={{ cursor: 'pointer', '&:hover': { textDecoration: 'underline' } }}
                  >
                    {row.id}
                  </Typography>
                  <CopyText text={row.id}/>
                </Box>
              </MuiTableCell>
              <MuiTableCell>{row.mime_type}</MuiTableCell>
              <MuiTableCell>{row.source}</MuiTableCell>
              <MuiTableCell>{row.parent_id}</MuiTableCell>
              <MuiTableCell>
                {typeof row.labels === 'object' && row.labels !== null
                  ? Object.entries(row.labels)
                      .map(([key, value]) => `${key}: ${value}`)
                      .join(', ')
                  : String(row.labels)}
              </MuiTableCell>
              <MuiTableCell>
                {row.created_at ? new Date(row.created_at * 1000).toLocaleString() : ''}
              </MuiTableCell>
            </MuiTableRow>
          ))}
        </MuiTableBody>
      </MuiTable>
    </MuiTableContainer>
  );
};

export default PolicyContentTable;
