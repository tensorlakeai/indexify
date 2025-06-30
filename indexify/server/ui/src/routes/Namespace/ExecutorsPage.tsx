import { Box } from '@mui/material';
import { useLoaderData } from 'react-router-dom';
import { ExecutorsCard } from '../../components/cards/ExecutorsCard';
import type { ExecutorsLoaderData } from './types';

const ExecutorsPage = () => {
  const { executors } = useLoaderData() as ExecutorsLoaderData;
  return (
    <Box>
      <ExecutorsCard executors={executors} />
    </Box>
  );
};

export default ExecutorsPage;
