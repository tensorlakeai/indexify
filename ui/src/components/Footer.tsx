import { Box, Typography } from '@mui/material'
import Link from '@mui/material/Link'

const Footer = () => {
  return (
    <Box my={5}>
      <Typography variant="body2" color="text.secondary" align="center">
        {'Copyright © '}
        <Link color="inherit" href="https://tensorlake.ai/">
          Tensorlake
        </Link>{' '}
        {new Date().getFullYear()}
        {'.'}
      </Typography>
    </Box>
  )
}

export default Footer
