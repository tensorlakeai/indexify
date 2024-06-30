import React, { useRef, useState, useEffect } from 'react';
import { Box, Stack, Chip, IconButton } from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';

export const ScrollableChips = ({ inputParams }: { inputParams: any }) => {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [showLeftArrow, setShowLeftArrow] = useState(false);
  const [showRightArrow, setShowRightArrow] = useState(false);

  const checkScroll = () => {
    if (scrollContainerRef.current) {
      const { scrollLeft, scrollWidth, clientWidth } = scrollContainerRef.current;
      setShowLeftArrow(scrollLeft > 0);
      setShowRightArrow(scrollLeft < scrollWidth - clientWidth - 1);
    }
  };

  useEffect(() => {
    checkScroll();
    window.addEventListener('resize', checkScroll);
    return () => window.removeEventListener('resize', checkScroll);
  }, []);

  const scroll = (scrollOffset: number) => {
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollBy({ left: scrollOffset, behavior: 'smooth' });
      setTimeout(checkScroll, 100);
    }
  };

  if (!inputParams || Object.keys(inputParams).length === 0) {
    return <Chip label="None" sx={{ backgroundColor: '#E9EDF1', color: '#757A82' }} />;
  }

    const chipCount = Object.keys(inputParams).length;

  return (
    <Box sx={{ position: 'relative', width: '100%' }}>
      {showLeftArrow && (
        <IconButton
          onClick={() => scroll(-100)}
          sx={{
            position: 'absolute',
            left: -20, // Adjust this value as needed
            top: '50%',
            transform: 'translateY(-50%)',
            zIndex: 1,
            '&:hover': { backgroundColor: '#3296fe4d' },
          }}
        >
          <ChevronLeftIcon fontSize={'small'} />
        </IconButton>
      )}
      <Box
        ref={scrollContainerRef}
        sx={{
          overflowX: 'auto',
          scrollbarWidth: 'none',
          '&::-webkit-scrollbar': { display: 'none' },
          px: chipCount > 1 ? 3 : 0
        }}
        onScroll={checkScroll}
      >
        <Stack gap={1} direction="row" sx={{ py: 1 }}>
          {Object.keys(inputParams).map((val: string) => (
            <Chip
              key={val}
              label={`${val}:${inputParams[val].type}`}
              sx={{ backgroundColor: '#E5EFFB', color: '#1C2026', flexShrink: 0 }}
            />
          ))}
        </Stack>
      </Box>
      {showRightArrow && (
        <IconButton
          onClick={() => scroll(100)}
          sx={{
            position: 'absolute',
            right: -20, // Adjust this value as needed
            top: '50%',
            transform: 'translateY(-50%)',
            zIndex: 1,
            '&:hover': { backgroundColor: '#3296fe4d' },
          }}
        >
          <ChevronRightIcon fontSize={'small'} />
        </IconButton>
      )}
    </Box>
  );
};