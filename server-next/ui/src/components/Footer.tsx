import { Box, Typography } from "@mui/material";
import Link from "@mui/material/Link";

const Footer = () => {
  return (
    <Box py={2} textAlign={"center"}>
      <Typography variant="caption" color="CaptionText" align="center">
        {"Copyright © "}
        <Link color="inherit" href="https://tensorlake.ai/">
          Tensorlake
        </Link>{" "}
        {new Date().getFullYear()}
        {"."}
      </Typography>
    </Box>
  );
};

export default Footer;
