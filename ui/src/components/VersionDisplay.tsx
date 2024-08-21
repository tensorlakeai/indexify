import React, { useState, useEffect } from 'react';
import { Alert, CircularProgress, Typography } from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';

const VersionDisplay = ({ owner, repo }: { owner: string; repo: string }) => {
  const [version, setVersion] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);


  const alertStyle = {
    padding: '0px 16px',
    display: 'flex',
    alignItems: 'center',
    '& .MuiAlert-icon': {
      padding: '0px',
      marginRight: '8px',
    },
    '& .MuiAlert-message': {
      padding: '8px 0',
    },
  };

  const textStyle = {
    fontSize: '12px',
    lineHeight: 1.5,
  };

  if (loading) {
    return (
      <Alert severity="info" icon={<CircularProgress size={16} />} sx={alertStyle}>
        <Typography sx={textStyle}>Loading version...</Typography>
      </Alert>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={alertStyle}>
        <Typography sx={textStyle}>Error: {error}</Typography>
      </Alert>
    );
  }

  return (
    <Alert severity="info" icon={<InfoIcon fontSize="small" />} sx={alertStyle}>
      <Typography sx={textStyle}>Indexify Version: {version}</Typography>
    </Alert>
  );
};

export default VersionDisplay;
