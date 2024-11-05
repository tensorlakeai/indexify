import { useState, useEffect } from 'react';
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

interface VersionState {
  openApi: string | null;
  github: string | null;
  error: string | null;
  isLoading: boolean;
  isDismissed: boolean;
}

export function VersionDisplay({ owner, repo, variant = 'sidebar', serviceUrl, drawerWidth }: VersionDisplayProps) {
  const [state, setState] = useState<VersionState>({
    openApi: null,
    github: null,
    error: null,
    isLoading: true,
    isDismissed: false,
  });

  useEffect(() => {
    async function fetchVersions() {
      try {
        const [openApiResponse, githubResponse] = await Promise.all([
          fetch(`${serviceUrl}/docs/openapi.json`),
          fetch(`https://api.github.com/repos/${owner}/${repo}/releases/latest`)
        ]);

        if (!openApiResponse.ok || !githubResponse.ok) 
          throw new Error('Failed to fetch versions');

        const [openApiData, githubData] = await Promise.all([
          openApiResponse.json(),
          githubResponse.json()
        ]);

        setState(prev => ({
          ...prev,
          openApi: openApiData.info.version,
          github: githubData.tag_name,
          isLoading: false
        }));
      } catch (error) {
        setState(prev => ({
          ...prev,
          error: 'Failed to fetch version information',
          isLoading: false
        }));
      }
    }

    const dismissedData = localStorage.getItem('versionAnnouncementDismissed');
    if (dismissedData) {
      const { timestamp, dismissed } = JSON.parse(dismissedData);
      const today = new Date().toDateString();
      if (new Date(timestamp).toDateString() === today && dismissed) {
        setState(prev => ({ ...prev, isDismissed: true }));
      } else {
        localStorage.removeItem('versionAnnouncementDismissed');
      }
    }

    void fetchVersions();
  }, [owner, repo, serviceUrl]);

  function compareVersions(v1: string, v2: string): number {
    const normalize = (v: string) => v.replace('v', '').split('.').map(Number);
    const [parts1, parts2] = [normalize(v1), normalize(v2)];
    
    for (let i = 0; i < Math.max(parts1.length, parts2.length); i++) {
      const diff = (parts1[i] || 0) - (parts2[i] || 0);
      if (diff !== 0) return diff > 0 ? 1 : -1;
    }
    return 0;
  }

  function getAlertSeverity(): 'info' | 'warning' | 'error' {
    if (!state.openApi || !state.github) return 'info';
    const comparison = compareVersions(state.github, state.openApi);
    return comparison === 0 ? 'info' : comparison === 1 ? 'warning' : 'error';
  }

  const alertIcons = {
    warning: <WarningIcon fontSize="small" />,
    error: <ErrorIcon fontSize="small" />,
    info: <InfoIcon fontSize="small" />
  };

  function handleDismiss() {
    setState(prev => ({ ...prev, isDismissed: true }));
    localStorage.setItem('versionAnnouncementDismissed', JSON.stringify({
      timestamp: new Date().toISOString(),
      dismissed: true
    }));
  }

  if (state.isLoading) return <Typography variant="caption">Loading version...</Typography>;
  if (state.error) return <Typography variant="caption">Error: {state.error}</Typography>;

  const severity = getAlertSeverity();

  if (variant === 'announcement') {
    if (severity === 'info' || state.isDismissed) return null;
    
    return (
      <Box sx={{ marginLeft: `${drawerWidth}px`, width: `calc(100% - ${drawerWidth}px)` }}>
        <Alert 
          severity={severity}
          icon={alertIcons[severity]}
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
          sx={{ ...alertStyle, width: '100%' }}
        >
          <Typography sx={textStyle}>
            A new version of Indexify <Link href={`https://github.com/${owner}/${repo}/releases`} target="_blank" rel="noopener noreferrer">({state.github})</Link> is available. Your current version: v{state.openApi}
          </Typography>
        </Alert>
      </Box>
    );
  }

  return (
    <Alert severity="info" icon={<InfoIcon fontSize="small" />} sx={alertStyle}>
      <Typography sx={textStyle}>Indexify Version: {state.openApi}</Typography>
    </Alert>
  );
}

export default VersionDisplay;
