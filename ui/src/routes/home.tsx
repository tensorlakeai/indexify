import { Typography } from "@mui/material";
import { Box } from "@mui/system";
import React from "react";

const HomePage = () => {
  return (
    <Box sx={{ height: "100%" }}>
      <Typography variant="h2" component="h1">
        Welcome to Indexify
      </Typography>

      <Typography variant="body1">
        <a href="https://getindexify.ai" target="_blank">
          Read documentation
        </a>
      </Typography>
    </Box>
  );
};

export default HomePage;
