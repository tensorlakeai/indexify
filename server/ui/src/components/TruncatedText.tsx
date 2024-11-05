import { Tooltip, Typography } from '@mui/material';

interface TruncatedTextProps {
  text: string;
  maxLength?: number;
}

export function TruncatedText({ text, maxLength = 25 }: TruncatedTextProps) {
  const truncatedText = text.length > maxLength 
    ? `${text.slice(0, maxLength)}...` 
    : text;

  return (
    <Tooltip title={text}>
      <Typography
        variant="h6"
        component="div"
        className="cursor-default"
      >
        {truncatedText}
      </Typography>
    </Tooltip>
  );
}

export default TruncatedText;
