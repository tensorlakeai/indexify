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

export function NamespaceSelector() {
  const navigate = useNavigate();
  const location = useLocation();
  const { namespace } = useParams();
  const { namespaces } = useLoaderData() as LoaderData;

  const handleNamespaceChange = (event: SelectChangeEvent) => {
    const value = event.target.value;
    const [firstSegment, secondSegment, ...rest] = location.pathname
      .split('/')
      .filter(Boolean);

    if (firstSegment === 'namespaces' || !secondSegment) {
      navigate(`/${value}/compute-graphs`);
      return;
    }

    const remainingPath = rest.length ? `/${rest.join('/')}` : '';
    navigate(`/${value}/${secondSegment}${remainingPath}`);
  };

  return (
    <Box sx={{ p: 2 }}>
      <FormControl fullWidth size="small">
        <InputLabel id="namespace-select-label">Select your namespace</InputLabel>
        <Select
          labelId="namespace-select-label"
          value={namespace || 'default'}
          onChange={handleNamespaceChange}
          label="Select your namespace"
          sx={{ '& .MuiSelect-select': { py: 1 } }}
        >
          {namespaces?.map(({ name }) => (
            <MenuItem key={name} value={name}>
              {name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}

export default NamespaceSelector;
