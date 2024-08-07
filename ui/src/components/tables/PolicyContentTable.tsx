import React, { useEffect, useState } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  TableContainer as MuiTableContainer, 
  TableHead as MuiTableHead, 
  TableRow as MuiTableRow, 
  TableCell as MuiTableCell,
  Alert,
  TextField,
  Button
} from '@mui/material'
import { TableVirtuoso } from 'react-virtuoso';
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
  const [searchContentId, setSearchContentId] = useState("");
  const [filteredContent, setFilteredContent] = useState<IContentMetadata[]>([]);

  const loadContent = async () => {
    setIsLoading(true);
    try {
      const result = await client.getExtractionPolicyContent({
        contentId: searchContentId ? searchContentId : contentId,
        graphName: extractorName,
        policyName: policyName
      })
      
      setContent(result);
      setFilteredContent(result);
    } catch (error) {
      console.error(`Error loading content for policy ${policyName}:`, error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadContent();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, searchContentId, extractorName, namespace, policyName]);

  const handleSearch = () => {
    loadContent();
  };

  const handleOnEnter = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      handleSearch();
    }
  };

  if (isLoading && content.length === 0) {
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

  const TableComponents = {
    Scroller: React.forwardRef<HTMLDivElement>((props, ref) => (
      <MuiTableContainer component={Paper} {...props} ref={ref} sx={{boxShadow: "0px 0px 2px -1px rgba(51, 132, 252, 0.5) inset"}} />
    )),
    Table: (props: React.HTMLAttributes<HTMLTableElement>) => (
      <table {...props} style={{ borderCollapse: 'separate', tableLayout: 'fixed' }} />
    ),
    TableHead: MuiTableHead,
    TableRow: MuiTableRow,
    TableBody: React.forwardRef<HTMLTableSectionElement>((props, ref) => (
      <tbody {...props} ref={ref} />
    )),
  };

  const fixedHeaderContent = () => (
    <MuiTableRow>
      <MuiTableCell 
        style={{ 
          width: '20%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Content ID
      </MuiTableCell>
      <MuiTableCell 
        style={{ 
          width: '15%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Mime Type
      </MuiTableCell>
      <MuiTableCell 
        style={{ 
          width: '15%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Source
      </MuiTableCell>
      <MuiTableCell 
        style={{ 
          width: '15%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Parent ID
      </MuiTableCell>
      <MuiTableCell 
        style={{ 
          width: '20%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Labels
      </MuiTableCell>
      <MuiTableCell 
        style={{ 
          width: '15%', 
          backgroundColor: '#fff', 
          position: 'sticky', 
          top: 0, 
          zIndex: 1 
        }}
      >
        Created At
      </MuiTableCell>
    </MuiTableRow>
  );

  const rowContent = (_index: number, row: IContentMetadata) => (
    <React.Fragment>
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
      <MuiTableCell>{row.source ? row.source : "Ingestion"}</MuiTableCell>
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
    </React.Fragment>
  );

  return (
    <>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
        <TextField
          placeholder="Search by Content Id"
          variant="outlined"
          size="small"
          value={searchContentId}
          onChange={(e) => setSearchContentId(e.target.value)}
          onKeyDown={handleOnEnter}
          sx={{ flexGrow: 1 }}
        />
        <Button variant="contained" onClick={handleSearch} disabled={isLoading || !searchContentId}>
          Search
        </Button>
      </Box>
      <Paper style={{ height: 500, width: '100%', overflow: 'hidden'}}>
        <TableVirtuoso
          style={{ height: '100%' }}
          data={filteredContent}
          components={TableComponents}
          fixedHeaderContent={fixedHeaderContent}
          itemContent={rowContent}
        />
      </Paper>
    </>
  );
};

export default PolicyContentTable;
