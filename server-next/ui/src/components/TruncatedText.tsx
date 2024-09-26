import { Tooltip, Typography } from '@mui/material';

const TruncatedText = ({ text, maxLength = 25 }: { text: string, maxLength: number}) => {

  const truncatedText = text.length > maxLength 
    ? `${text.slice(0, maxLength)}...` 
    : text;

  return (
    <Tooltip content={text} title={text}>
      <Typography
        variant="h6"
        component="div"
        className="cursor-default"
      >
        {truncatedText}
      </Typography>
    </Tooltip>
  );
};

export default TruncatedText;