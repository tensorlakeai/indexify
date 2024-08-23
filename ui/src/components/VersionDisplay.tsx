import React, { useState, useEffect } from 'react';
import { Alert, Typography } from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';
import WarningIcon from '@mui/icons-material/Warning';
import ErrorIcon from '@mui/icons-material/Error';

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
    fontSize: '14px',
    lineHeight: 1.5,
  };


const VersionDisplay = ({ owner, repo, variant = 'sidebar' }: { owner: string; repo: string; variant?: 'announcement' | 'sidebar' }) => {
  const [openApiVersion, setOpenApiVersion] = useState<string | null>(null);
  const [githubVersion, setGithubVersion] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchOpenApiVersion = async () => {
      try {
        const response = await fetch('http://localhost:8900/api-docs/openapi.json');
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
  }, [owner, repo]);

  const compareVersions = (v1: string, v2: string) => {
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

  const getAlertSeverity = () => {
    if (!openApiVersion || !githubVersion) return 'info';
    const comparison = compareVersions(githubVersion, openApiVersion);
    if (comparison === 0) return 'info';
    if (comparison === 1) return 'warning';
    return 'error';
  };

  const getAlertIcon = (severity: string) => {
    switch (severity) {
      case 'warning':
        return <WarningIcon fontSize="small" />;
      case 'error':
        return <ErrorIcon fontSize="small" />;
      default:
        return <InfoIcon fontSize="small" />;
    }
  };

  if (loading) {
    return <Typography variant="caption">Loading version...</Typography>;
  }

  if (error) {
    return <Typography variant="caption">Error: {error}</Typography>;
  }

  const severity = getAlertSeverity();

  if (variant === 'announcement') {
    if (severity === 'info') return null;
    
    return (
      <Alert 
        severity={severity} 
        icon={getAlertIcon(severity)}
        sx={{
          width: '100%',
          justifyContent: 'center',
          '& .MuiAlert-message': {
            flex: 'none',
          },
        }}
      >
        <Typography>
          {severity === 'warning' 
            ? `A new version (${githubVersion}) is available on GitHub. Your current version: ${openApiVersion}`
            : `A major update (${githubVersion}) is available on GitHub. Your current version: ${openApiVersion}`
          }
        </Typography>
      </Alert>
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
