import { Box, Typography } from '@mui/material';
import AccessTimeIcon from '@mui/icons-material/AccessTime';

interface InfoBoxProps {
    text?: string;
}

export function InfoBox({ text }: InfoBoxProps) {
  return (
    <Box
      sx={{
        border: '1px dashed #B0D1F7',
        borderRadius: '8px',
        padding: '26px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        maxWidth: '500px',
        margin: '10px',
        boxShadow: '0px 1px 2px 0px #00000040 inset',
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
}

export default InfoBox;
