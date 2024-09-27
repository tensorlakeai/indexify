import React, { useState, useEffect } from 'react';
import { Alert, Typography, IconButton, Box } from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';
import WarningIcon from '@mui/icons-material/Warning';
import ErrorIcon from '@mui/icons-material/Error';
import CloseIcon from '@mui/icons-material/Close';
import Link from '@mui/material/Link';

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
    flex: 1,
    display: 'flex',
    justifyContent: 'center',
  },
  '& .MuiAlert-action': {
    padding: '0px',
    marginRight: '0px',
  },
};

const textStyle = {
  fontSize: '14px',
  lineHeight: 1.5,
  textAlign: 'center',
};

interface VersionDisplayProps {
  owner: string;
  repo: string;
  variant?: 'announcement' | 'sidebar';
  serviceUrl: string;
  drawerWidth: number;
}

const VersionDisplay: React.FC<VersionDisplayProps> = ({ owner, repo, variant = 'sidebar', serviceUrl, drawerWidth }) => {
  const [openApiVersion, setOpenApiVersion] = useState<string | null>(null);
  const [githubVersion, setGithubVersion] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [dismissed, setDismissed] = useState<boolean>(false);

  useEffect(() => {
    const fetchOpenApiVersion = async () => {
      try {
        const response = await fetch(`${serviceUrl}/docs/openapi.json`);
        if (!response.ok) {
          throw new Error('Failed to fetch OpenAPI version');
        }
        const data = await response.json();
        setOpenApiVersion(data.info.version);
      } catch (error) {
        console.error('Error fetching OpenAPI version:', error);
        setError('Failed to fetch OpenAPI version');
      }
    };

    const fetchGithubVersion = async () => {
      try {
        const response = await fetch(`https://api.github.com/repos/${owner}/${repo}/releases/latest`);
        if (!response.ok) {
          throw new Error('Failed to fetch GitHub version');
        }
        const data = await response.json();
        setGithubVersion(data.tag_name);
      } catch (error) {
        console.error('Error fetching GitHub version:', error);
        setError('Failed to fetch GitHub version');
      }
    };

    Promise.all([fetchOpenApiVersion(), fetchGithubVersion()])
      .then(() => setLoading(false))
      .catch(() => setLoading(false));

    // Check if the announcement has been dismissed
    const dismissedData = localStorage.getItem('versionAnnouncementDismissed');
    if (dismissedData) {
      const { timestamp, dismissed } = JSON.parse(dismissedData);
      const today = new Date().toDateString();
      if (new Date(timestamp).toDateString() === today && dismissed) {
        setDismissed(true);
      } else {
        localStorage.removeItem('versionAnnouncementDismissed');
      }
    }
  }, [owner, repo, serviceUrl]);

  const compareVersions = (v1: string, v2: string): number => {
    const v1Parts = v1.replace('v', '').split('.').map(Number);
    const v2Parts = v2.replace('v', '').split('.').map(Number);
    
    for (let i = 0; i < Math.max(v1Parts.length, v2Parts.length); i++) {
      const part1 = v1Parts[i] || 0;
      const part2 = v2Parts[i] || 0;
      if (part1 > part2) return 1;
      if (part1 < part2) return -1;
    }
    return 0;
  };

  const getAlertSeverity = (): 'info' | 'warning' | 'error' => {
    if (!openApiVersion || !githubVersion) return 'info';
    const comparison = compareVersions(githubVersion, openApiVersion);
    if (comparison === 0) return 'info';
    if (comparison === 1) return 'warning';
    return 'error';
  };

  const getAlertIcon = (severity: 'info' | 'warning' | 'error') => {
    switch (severity) {
      case 'warning':
        return <WarningIcon fontSize="small" />;
      case 'error':
        return <ErrorIcon fontSize="small" />;
      default:
        return <InfoIcon fontSize="small" />;
    }
  };

  const handleDismiss = () => {
    setDismissed(true);
    const dismissData = {
      timestamp: new Date().toISOString(),
      dismissed: true
    };
    localStorage.setItem('versionAnnouncementDismissed', JSON.stringify(dismissData));
  };

  if (loading) {
    return <Typography variant="caption">Loading version...</Typography>;
  }

  if (error) {
    return <Typography variant="caption">Error: {error}</Typography>;
  }

  const severity = getAlertSeverity();

  if (variant === 'announcement') {
    if (severity === 'info' || dismissed) return null;
    
    return (
      <Box sx={{ marginLeft: `${drawerWidth}px`, width: `calc(100% - ${drawerWidth}px)` }}>
        <Alert 
          severity={severity} 
          icon={getAlertIcon(severity)}
          action={
            <IconButton
              aria-label="close"
              color="inherit"
              size="small"
              onClick={handleDismiss}
            >
              <CloseIcon fontSize="inherit" />
            </IconButton>
          }
          sx={{
            ...alertStyle,
            width: '100%',
          }}
        >
          <Typography sx={textStyle}>
            A new version of Indexify <Link href={`https://github.com/${owner}/${repo}/releases`} target="_blank" rel="noopener noreferrer">({githubVersion})</Link> is available. Your current version: v{openApiVersion}
          </Typography>
        </Alert>
      </Box>
    );
  }

  // Sidebar version
  return (
    <Alert severity="info" icon={<InfoIcon fontSize="small" />} sx={alertStyle}>
      <Typography sx={textStyle}>Indexify Version: {openApiVersion}</Typography>
    </Alert>
  );
};

export default VersionDisplay;
