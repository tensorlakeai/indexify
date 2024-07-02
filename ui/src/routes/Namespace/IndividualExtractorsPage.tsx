import React from 'react';
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  Chip,
  IconButton,
} from '@mui/material';
import ExtendedContentTable from '../../components/ExtendedContentTable';
import { InfoCircle, TableDocument } from 'iconsax-react';
import CopyText from '../../components/CopyText';

const ExtractorTable = () => {
  const rows = [
    { id: 1, name: 'moondreamextractor', extractor: 'tensorlake/moondream', inputTypes: ['image/bmp', 'image/gif'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 2, name: 'minilm-moondream-caption', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 3, name: 'minilm-moondream-caption-efefegeef...', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 4, name: 'minilm-moondream-wordsege-plentyofroom', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
  ];

  return (
    <TableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: 'none'}}>
      <Table sx={{ minWidth: 650 }} aria-label="extractor table">
        <TableHead>
          <Typography variant="h4" sx={{ display: 'flex', alignItems: 'center', ml: 2, mt: 2 }}>
            moondreamcaptionkb
            <CopyText text='moondreamcaptionkb' />
          </Typography>
          <TableRow>
            <TableCell sx={{ fontSize: 14}}>Name</TableCell>
            <TableCell sx={{ fontSize: 14}}>Extractor</TableCell>
            <TableCell sx={{ fontSize: 14}}>Input Types</TableCell>
            <TableCell sx={{ fontSize: 14}}>Input Parameters</TableCell>
            <TableCell sx={{ fontSize: 14}}>Pending</TableCell>
            <TableCell sx={{ fontSize: 14}}>Failed</TableCell>
            <TableCell sx={{ fontSize: 14}}>Completed</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <Box sx={{ display: 'flex', alignItems: 'left', flexDirection: 'column' }}>
                  <Box sx={{ mr: 1, display: 'flex' }}>
                    {Array(row.id).fill(0).map((_, i) => (
                      <Box key={i} sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#6FA8EA', mr: 0.5 }} />
                    ))}
                  </Box>
                  {row.name}
                </Box>
              </TableCell>
              <TableCell>{row.extractor}</TableCell>
              <TableCell>
                {row.inputTypes.map((type, index) => (
                  <Chip key={index} label={type} size="small" sx={{ mr: 0.5 }} />
                ))}
              </TableCell>
              <TableCell>{row.inputParameters}</TableCell>
              <TableCell><Chip label={row.pending} sx={{ backgroundColor: '#E5EFFB' }} /></TableCell>
              <TableCell><Chip label={row.failed} sx={{ backgroundColor: '#FBE5E5' }} /></TableCell>
              <TableCell><Chip label={row.completed} sx={{ backgroundColor: '#E5FBE6' }} /></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

const IndividualExtractorsPage = () => {
  return (
    <Box sx={{ p: 0 }}>
      <Box sx={{ mb: 3 }}>
        <div className="content-table-header">
          <div className="heading-icon-container">
            <TableDocument size="25" className="heading-icons" variant="Outline"/>
          </div>
          <Typography variant="h4">
            tensorlake/moondream
            <IconButton
              href="https://docs.getindexify.ai/concepts/#content"
              target="_blank"
            >
              <InfoCircle size="20" variant="Outline"/>
            </IconButton>
          </Typography>
        </div>
        <ExtractorTable />
      </Box>
      <ExtendedContentTable />
    </Box>
  );
};

export default IndividualExtractorsPage;