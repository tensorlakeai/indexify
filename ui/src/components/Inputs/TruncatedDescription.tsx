import { useEffect, useRef, useState } from 'react';
import { Typography, Tooltip } from '@mui/material';

export const TruncatedDescription = ({ description }: { description: string }) => {
  const [isOverflowing, setIsOverflowing] = useState(false);
  const textRef = useRef<HTMLParagraphElement>(null);

  useEffect(() => {
    const checkOverflow = () => {
      if (textRef.current) {
        setIsOverflowing(textRef.current.scrollHeight > textRef.current.clientHeight);
      }
    };
    checkOverflow();
    window.addEventListener('resize', checkOverflow);
    return () => window.removeEventListener('resize', checkOverflow);
  }, [description]);

  return (
    <Tooltip title={isOverflowing ? description : ''} arrow>
      <Typography
        ref={textRef}
        variant="subtitle2"
        paragraph
        sx={{
          display: '-webkit-box',
          WebkitLineClamp: 2,
          WebkitBoxOrient: 'vertical',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          marginBottom: 0,
          marginLeft: { xs: 0, lg: 1 },
        }}
      >
        {description}
      </Typography>
    </Tooltip>
  );
};
