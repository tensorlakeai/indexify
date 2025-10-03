import { ContentCopy } from '@mui/icons-material'
import { Popover, Typography } from '@mui/material'
import { useState, type ReactNode } from 'react'

interface CopyTextPopoverProps {
  text: string
  children: ReactNode
}

function CopyTextPopover({ text, children }: CopyTextPopoverProps) {
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null)
  const [isCopied, setIsCopied] = useState(false)

  const handlePopoverOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget)
  }

  const handlePopoverClose = () => {
    setAnchorEl(null)
  }

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setIsCopied(true)
      setTimeout(() => {
        setIsCopied(false)
        handlePopoverClose()
      }, 1000)
    } catch (err) {
      console.error('Failed to copy text:', err)
    }
  }

  const open = Boolean(anchorEl)

  return (
    <div
      onMouseEnter={handlePopoverOpen}
      onMouseLeave={handlePopoverClose}
      style={{ display: 'inline-block' }}
    >
      {children}
      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handlePopoverClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
        sx={{
          pointerEvents: 'none',
          '& .MuiPopover-paper': {
            pointerEvents: 'auto',
          },
        }}
      >
        <Typography
          sx={{
            p: 1,
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            cursor: 'pointer',
            '&:hover': {
              bgcolor: 'action.hover',
            },
            fontSize: 12,
          }}
          onClick={handleCopy}
        >
          <ContentCopy sx={{ fontSize: 12 }} />
          {isCopied ? 'Copied!' : 'Click to copy'}
        </Typography>
      </Popover>
    </div>
  )
}

export default CopyTextPopover
