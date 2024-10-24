import React from 'react';
import { useNavigate, useParams, useLocation, useLoaderData } from 'react-router-dom';
import { 
  FormControl,
  Select,
  MenuItem,
  SelectChangeEvent,
  Box,
  InputLabel
} from '@mui/material';

interface Namespace {
  name: string;
  created_at: number;
}

interface LoaderData {
  namespaces: Namespace[];
  namespace: string;
}

const NamespaceSelector = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { namespace } = useParams();
  const { namespaces } = useLoaderData() as LoaderData;

  const handleNamespaceChange = (event: SelectChangeEvent) => {
    const value = event.target.value;
    const pathSegments = location.pathname.split('/').filter(Boolean);
    
    if (pathSegments[0] === 'namespaces') {
      navigate(`/${value}/compute-graphs`);
      return;
    }

    if (pathSegments.length >= 2) {
      const currentRoute = pathSegments[1];
      const remainingPath = pathSegments.slice(2).join('/');
      const newPath = `/${value}/${currentRoute}${remainingPath ? '/' + remainingPath : ''}`;
      navigate(newPath);
    } else {
      navigate(`/${value}/compute-graphs`);
    }
  };

  const currentNamespace = namespace || 'default';

  return (
    <Box sx={{ p: 2 }}>
      <FormControl fullWidth size="small">
        <InputLabel id="namespace-select-label">Select your namespace</InputLabel>
        <Select
          labelId="namespace-select-label"
          value={currentNamespace}
          onChange={handleNamespaceChange}
          label="Select your namespace"
          sx={{
            '& .MuiSelect-select': {
              py: 1,
            }
          }}
        >
          {namespaces?.map((ns) => (
            <MenuItem key={ns.name} value={ns.name}>
              {ns.name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
};

export default NamespaceSelector;
