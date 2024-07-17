import React from 'react';
import { Breadcrumbs, Stack, Typography } from '@mui/material';
import { Link, useLoaderData } from 'react-router-dom';
import { StateChange } from '../../types';
import { Cpu } from 'iconsax-react';

const ExtractionPoliciesContentPage = () => {
  const { stateChanges } = useLoaderData() as {
    stateChanges: StateChange[];
  };

  return (
    <>
      <Stack
        display={'flex'}
        direction={'row'}
        alignItems={'center'}
        spacing={1}
      >
        {/* <Breadcrumbs
            aria-label="breadcrumb"
            separator={<NavigateNextIcon fontSize="small" />}
        >
            <Typography color="text.primary">{namespace}</Typography>
            <Link color="inherit" to={`/${namespace}/extraction-graphs`}>
                <Typography color="text.primary">Extraction Graph</Typography>
            </Link>
            <Link color="inherit" to={`/${namespace}/extraction-graphs/${extractorName}`}>
                <Typography color="text.primary">{extractorName}</Typography>
            </Link>
            <Link color="inherit" to={`/${namespace}/extraction-graphs/${extractorName}/content`}>
                <Typography color="text.primary">{extractorName}</Typography>
            </Link>
            <Typography color="text.primary">{contentId}</Typography>
        </Breadcrumbs> */}
        <div className="heading-icon-container">
          <Cpu size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Ingested Contents
        </Typography>
      </Stack>
    </>
  );
};

export default ExtractionPoliciesContentPage;
