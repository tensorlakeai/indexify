import { Box, Typography } from "@mui/material";
import Link from "@mui/material/Link";

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <Box py={2} textAlign="center">
      <Typography variant="caption" color="CaptionText" align="center">
        Copyright © 
        <Link color="inherit" href="https://tensorlake.ai/">
          Tensorlake
        </Link>
        {` ${currentYear}.`}
      </Typography>
    </Box>
  );
}

export default Footer;
