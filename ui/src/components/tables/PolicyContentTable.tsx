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
  TablePagination
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
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const loadContent = async () => {
    setIsLoading(true);
    try {
      const result = await client.getExtractionPolicyContent({
        contentId: contentId,
        graphName: extractorName,
        policyName: policyName
      })
      
      setContent(result);
    } catch (error) {
      console.error(`Error loading content for policy ${policyName}:`, error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadContent();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, contentId, extractorName, namespace, policyName]);

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
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
      <MuiTableContainer component={Paper} {...props} ref={ref} />
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
    <Box>
      <Paper style={{ height: 400, width: '100%', overflow: 'hidden' }}>
        <TableVirtuoso
          style={{ height: '100%' }}
          data={content.slice(0, rowsPerPage)}
          components={TableComponents}
          fixedHeaderContent={fixedHeaderContent}
          itemContent={rowContent}
        />
      </Paper>
      <TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={content.length}
        rowsPerPage={rowsPerPage}
        page={0}
        onPageChange={() => {}}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Box>
  );
};

export default PolicyContentTable;