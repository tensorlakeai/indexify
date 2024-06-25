import React from 'react';
import { Box, Typography } from '@mui/material';
import AccessTimeIcon from '@mui/icons-material/AccessTime';

interface InfoBoxProps {
    text: string;
}

const InfoBox: React.FC<InfoBoxProps> = ({text}) => {
  return (
    <Box
      sx={{
        border: '1px dashed #B0D1F7',
        borderRadius: '8px',
        padding: '32px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        maxWidth: '600px',
        margin: '20px',
        boxShadow: "0px 1px 2px 0px ##00000040 inset",
      }}
    >
      <AccessTimeIcon
        sx={{
          color: '#2196f3',
          fontSize: '24px',
          marginRight: '16px',
          marginTop: '4px',
        }}
      />
      <Typography variant="subtitle2" color="text.secondary">
        {text}
      </Typography>
    </Box>
  );
};

export default InfoBox;
